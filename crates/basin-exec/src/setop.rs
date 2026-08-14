//! `Values`, `Empty`, `Distinct` and `SetOp` — the operators that sit between
//! "a scan/filter/project/join/sort plan runs" and "most `SELECT`s run".
//! `SELECT 1` bottoms out on [`Empty`], `INSERT ... VALUES (...)` bottoms out
//! on [`Values`], and `UNION`/`INTERSECT`/`EXCEPT`/`DISTINCT` are common
//! enough in real queries that leaving them as [`crate::build::BuildError::Unsupported`]
//! would fail a large share of ordinary `SELECT`s, not just exotic ones.
//!
//! # NULL equals NULL here — the opposite of a join key
//!
//! [`crate::join`]'s module doc explains why a join key with a NULL never
//! matches, including another all-NULL key: two NULL-keyed rows must never be
//! treated as "the same join partner". Set membership and row identity are a
//! different question with the opposite answer. Postgres defines `DISTINCT`,
//! `UNION`, `INTERSECT` and `EXCEPT` in terms of row *equality for grouping
//! purposes*, and for that purpose two NULLs in the same column ARE
//! considered the same value — `SELECT NULL UNION SELECT NULL` yields one
//! row, not two, and `SELECT DISTINCT` over two all-NULL rows yields one row.
//! Getting this backwards (treating it like a join key) is the classic bug
//! this module's tests are built to catch; see `null_equals_null_in_distinct`
//! and `null_equals_null_in_union` below.
//!
//! This module gets the rule for free from [`arrow::row::RowConverter`]:
//! [`crate::join`]'s doc explains that the row encoding gives every NULL
//! value in a column the *same* sentinel bytes regardless of what non-null
//! value it might otherwise have carried — which is exactly why `join.rs` has
//! to mask NULL-keyed rows out by hand before inserting them into its hash
//! table. Here, that same property is exactly what is wanted, so this module
//! does the opposite of `join.rs`: it inserts every row's encoded key
//! unconditionally, NULLs included, and lets identical byte sequences collide
//! on purpose.
//!
//! # Multiset vs. set semantics
//!
//! `UNION`/`INTERSECT`/`EXCEPT` (no `ALL`) treat each side as a *set*: the
//! result never contains a duplicate row, no matter how many times it
//! occurred on either input. `UNION ALL`/`INTERSECT ALL`/`EXCEPT ALL` treat
//! each side as a *multiset* (bag): duplicates carry a count, and that count
//! flows through arithmetically —
//! `count_out = count_left + count_right` for `UNION ALL` (implemented as a
//! plain concatenation, since nothing needs to be deduplicated),
//! `count_out = min(count_left, count_right)` for `INTERSECT ALL`, and
//! `count_out = max(count_left - count_right, 0)` for `EXCEPT ALL`. See
//! [`SetOp::next_batch`] for where each formula is applied.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::compute::{concat, concat_batches, take};
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use basin_plan::{Expr, SetOpKind};

use crate::eval;
use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// A zero-column batch with exactly one row. Used to evaluate expressions
/// that (like every `VALUES` row expression) reference no input column —
/// mirrors [`crate::project::Project`]'s zero-row probe batch, except this
/// probe carries one row rather than zero, because `eval`'s output length is
/// driven by the batch's row count and a `VALUES` row must produce exactly
/// one row of output per expression.
fn one_row_probe() -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("a zero-column batch with an explicit row count is always valid")
}

/// Reorder every column of `batch` by `indices`, keeping `batch`'s own
/// schema. Shared by [`Distinct`] and [`SetOp`]; mirrors `sort.rs`'s
/// `take_batch` and `join.rs`'s `take_by_indices`, which live in their own
/// modules rather than a shared one because each predates this file.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, ExecError> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(arrow_err)
}

// ============================================================================
// Values
// ============================================================================

/// `VALUES (…), (…), …` — a literal row source with no input operator at
/// all.
///
/// # Schema inference
///
/// Like [`crate::project::Project`] (see that module's doc comment), there is
/// no separate "what type would this expression produce" query on `Expr` —
/// only [`eval::eval`], which needs a batch to evaluate against. Each row's
/// expressions are evaluated once against [`one_row_probe`] to learn their
/// Arrow types; the *first* row is used to fix the declared schema, since
/// every row of a `VALUES` list is required (by the SQL grammar, and by
/// whatever unified their literal types during planning) to agree on it.
///
/// If `rows` is empty — not reachable from real SQL, since `VALUES` requires
/// at least one row, but not rejected here either — there is no row left to
/// infer a type from at all. Rather than fail construction, every declared
/// column is still reported, typed [`DataType::Null`]: the shape (column
/// count and names) is still the one that was declared, even though no
/// concrete Arrow type can be. See
/// `values_with_zero_rows_still_reports_the_declared_column_shape` below.
pub struct Values {
    schema: SchemaRef,
    rows: Vec<Vec<Expr>>,
    emitted: bool,
}

impl Values {
    /// `rows` is one inner `Vec` per output row, in row-major order.
    /// `column_names` gives the output column names, one per column — every
    /// row in `rows` must carry exactly `column_names.len()` expressions,
    /// checked eagerly here rather than discovered row-by-row at execution
    /// time.
    pub fn new(rows: Vec<Vec<Expr>>, column_names: Vec<String>) -> Result<Self, ExecError> {
        let n_cols = column_names.len();
        for (i, row) in rows.iter().enumerate() {
            if row.len() != n_cols {
                return Err(ExecError::TypeMismatch(format!(
                    "VALUES row {i} has {} expressions, expected {n_cols}",
                    row.len()
                )));
            }
        }

        let schema = match rows.first() {
            Some(first_row) => {
                let probe = one_row_probe();
                let mut fields = Vec::with_capacity(n_cols);
                for (expr, name) in first_row.iter().zip(&column_names) {
                    let array = eval::eval(expr, &probe)?;
                    fields.push(Field::new(name, array.data_type().clone(), true));
                }
                Arc::new(Schema::new(fields))
            }
            None => Arc::new(Schema::new(
                column_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Null, true))
                    .collect::<Vec<_>>(),
            )),
        };

        Ok(Self {
            schema,
            rows,
            emitted: false,
        })
    }
}

impl Operator for Values {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        if self.rows.is_empty() {
            return Ok(None);
        }

        let probe = one_row_probe();
        let n_cols = self.schema.fields().len();
        let mut per_column: Vec<Vec<ArrayRef>> = (0..n_cols)
            .map(|_| Vec::with_capacity(self.rows.len()))
            .collect();
        for row in &self.rows {
            for (j, expr) in row.iter().enumerate() {
                per_column[j].push(eval::eval(expr, &probe)?);
            }
        }

        let arrays: Vec<ArrayRef> = per_column
            .into_iter()
            .map(|parts| {
                let refs: Vec<&dyn Array> =
                    parts.iter().map(|a| a.as_ref() as &dyn Array).collect();
                concat(&refs).map_err(arrow_err)
            })
            .collect::<Result<_, _>>()?;

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(arrow_err)?;
        Ok(Some(batch))
    }
}

// ============================================================================
// Empty
// ============================================================================

/// The empty relation. `SELECT 1` (and every other constant `SELECT` with no
/// `FROM`) plans as a `Project` over `Empty { produce_one_row: true, .. }` —
/// this operator supplies the one row of "nothing" for the projection to
/// compute its constant expressions against; a genuinely empty input (e.g.
/// one side of a provably-unsatisfiable `WHERE FALSE`) is
/// `produce_one_row: false`.
///
/// Getting `produce_one_row: true` right is the load-bearing case: an
/// operator that always yields zero rows would silently turn every constant
/// `SELECT` into an empty result set.
pub struct Empty {
    schema: SchemaRef,
    produce_one_row: bool,
    done: bool,
}

impl Empty {
    pub fn new(schema: SchemaRef, produce_one_row: bool) -> Self {
        Self {
            schema,
            produce_one_row,
            done: false,
        }
    }
}

impl Operator for Empty {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        if !self.produce_one_row {
            return Ok(None);
        }

        if self.schema.fields().is_empty() {
            // The common `SELECT 1` shape: no columns underneath, one row for
            // a `Project` above to compute constants against. A plain
            // `RecordBatch::try_new` cannot express "zero columns, one row"
            // on its own — row count has to be given explicitly.
            let batch = RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )
            .map_err(arrow_err)?;
            return Ok(Some(batch));
        }

        // A declared, non-empty schema with `produce_one_row: true`: there is
        // no data to fill the row with (this operator carries no
        // expressions, only a schema), so every column comes back NULL.
        let arrays: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| arrow_array::new_null_array(f.data_type(), 1))
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(arrow_err)?;
        Ok(Some(batch))
    }
}

// ============================================================================
// Distinct
// ============================================================================

/// `DISTINCT` and `DISTINCT ON (…)`.
///
/// # Streaming, not blocking
///
/// Unlike [`SetOp`] below, `Distinct` does not need to see the whole input
/// before producing anything — a row can be judged "new" or "a duplicate of
/// one already seen" the moment it arrives, by checking (and updating) a
/// running set of encoded keys. So `next_batch` pulls exactly one batch from
/// its child per call, the same shape as [`crate::project::Filter`], and
/// remains cancellable between batches. The `seen` set itself is the
/// unavoidable buffering: it grows with the number of *distinct* keys ever
/// observed, not with the size of the input, and [`Distinct::memory_used`]
/// reports it honestly against `memory_budget`.
///
/// # `DISTINCT` vs. `DISTINCT ON`
///
/// Both are "keep the first row seen for a given key, drop the rest" — they
/// differ only in which columns make up the key. Plain `DISTINCT` keys on
/// every output column ([`Distinct::new`]); `DISTINCT ON (exprs)` keys on
/// just the resolved columns behind `exprs` ([`Distinct::new_on`]), and
/// still emits the *whole* row (every column, not just the key columns) for
/// whichever row was first. Postgres requires `DISTINCT ON` to be paired
/// with a matching `ORDER BY` to make "first" meaningful; this operator does
/// not enforce that — it simply keeps whichever row arrives first in
/// whatever order the input hands it, same as Postgres does once the input
/// actually is ordered.
pub struct Distinct {
    input: Box<dyn Operator>,
    key_cols: Vec<usize>,
    schema: SchemaRef,
    row_converter: RowConverter,
    seen: HashSet<OwnedRow>,
    budget: usize,
    bytes_used: usize,
}

impl Distinct {
    /// Plain `DISTINCT`: the key is every output column.
    pub fn new(input: Box<dyn Operator>, memory_budget: usize) -> Self {
        let key_cols: Vec<usize> = (0..input.schema().fields().len()).collect();
        Self::with_key(input, key_cols, memory_budget)
    }

    /// `DISTINCT ON (…)`: `on_cols` are column positions in `input`'s schema,
    /// one per `DISTINCT ON` expression, already resolved — physical
    /// operators key on column positions throughout this crate (see
    /// `crate::build::BuildError::NonColumnKey`), and a `DISTINCT ON`
    /// expression more complex than a column reference is a `Project`
    /// materialising it ahead of this operator, same as `GROUP BY`/join
    /// keys.
    pub fn new_on(input: Box<dyn Operator>, on_cols: Vec<usize>, memory_budget: usize) -> Self {
        Self::with_key(input, on_cols, memory_budget)
    }

    fn with_key(input: Box<dyn Operator>, key_cols: Vec<usize>, memory_budget: usize) -> Self {
        let schema = input.schema();
        let key_fields: Vec<SortField> = key_cols
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect();
        let row_converter =
            RowConverter::new(key_fields).expect("DISTINCT key columns support row encoding");
        Self {
            input,
            key_cols,
            schema,
            row_converter,
            seen: HashSet::new(),
            budget: memory_budget,
            bytes_used: 0,
        }
    }
}

impl Operator for Distinct {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.input.next_batch()? else {
            return Ok(None);
        };

        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&key_arrays)
            .map_err(arrow_err)?;

        let mut keep: Vec<u32> = Vec::new();
        for i in 0..batch.num_rows() {
            let key = rows.row(i).owned();
            // NULL equals NULL here — see the module doc's "NULL equals
            // NULL" section. `RowConverter` gives every NULL the same
            // sentinel bytes, so two all-NULL rows collide in `seen` exactly
            // like two rows that happen to share the same real values.
            if self.seen.insert(key) {
                self.bytes_used += std::mem::size_of::<OwnedRow>();
                if self.bytes_used > self.budget {
                    return Err(ExecError::OutOfMemory {
                        requested: self.bytes_used,
                        budget: self.budget,
                    });
                }
                keep.push(i as u32);
            }
        }

        let idx = UInt32Array::from(keep);
        take_batch(&batch, &idx).map(Some)
    }

    fn memory_used(&self) -> usize {
        self.bytes_used
    }
}

// ============================================================================
// SetOp
// ============================================================================

/// Combine both sides' schemas into one, taking the left side's column names,
/// types and metadata but computing each column's nullability with `merge`.
///
/// A set operation reports the LEFT side's column names — that part matches
/// Postgres, which names a `UNION`'s output after its first branch. Taking
/// the left side's *nullability* along with them was a bug: a set operation
/// puts rows from both sides into one batch under one declared schema, and
/// `RecordBatch::try_new` rejects a `NOT NULL` field holding a NULL. With
/// `t.id BIGINT NOT NULL` and `u.tid BIGINT`, `SELECT id FROM t UNION ALL
/// SELECT tid FROM u` failed exactly there. The wrong rule had been here all
/// along, invisible while `project.rs` declared every column nullable and so
/// never handed this a `NOT NULL` left side to over-claim from.
///
/// The caller is responsible for having checked arity first; the `zip` here
/// would otherwise silently truncate to the shorter side.
fn merge_schemas(
    left: &SchemaRef,
    right: &SchemaRef,
    merge: impl Fn(bool, bool) -> bool,
) -> SchemaRef {
    let fields: Vec<Field> = left
        .fields()
        .iter()
        .zip(right.fields().iter())
        .map(|(l, r)| {
            l.as_ref()
                .clone()
                .with_nullable(merge(l.is_nullable(), r.is_nullable()))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, left.metadata().clone()))
}

/// Whether an output column of `op` can hold a NULL, given whether each side's
/// corresponding column can. Per operator, because the three differ and the
/// difference is worth having — see the three arms.
///
/// Verified against live Postgres 18.2 two ways, since Postgres exposes no
/// nullability for a set operation on the wire:
///
///   - **By value.** `SELECT id FROM t UNION ALL SELECT tid FROM u` returns a
///     NULL row: a `NOT NULL` left branch does not stop a nullable right
///     branch contributing NULLs. The `INTERSECT` and `EXCEPT ALL` forms of
///     the same pair return no NULL at all.
///
///   - **By the planner**, the sharper instrument. `EXPLAIN (COSTS OFF)
///     SELECT * FROM (SELECT id AS v FROM t UNION ALL SELECT uid FROM u) s
///     WHERE v IS NOT NULL` — both branches `NOT NULL` — DELETES the
///     `IS NOT NULL` filter, having proven it cannot fail. Swap `uid` for the
///     nullable `tid` and the filter SURVIVES on that branch.
///
/// Postgres's planner does not go on to prove the `INTERSECT`/`EXCEPT` cases
/// below (it keeps the filter for both), but that is a missing optimisation
/// on its part, not a disagreement: no query shape can observe a NULL where
/// these arms say there is none, and the arguments are given per arm.
fn setop_column_is_nullable(op: SetOpKind, left: bool, right: bool) -> bool {
    match op {
        // A UNION emits rows from both sides, so either side's NULL reaches
        // the output. This is the arm the regression was about.
        SetOpKind::Union => left || right,
        // A row survives an INTERSECT only by appearing on BOTH sides, so a
        // NULL in the output needs that same NULL-bearing row present on both
        // — impossible if either side's column cannot hold one. (NULL matches
        // NULL here, per this module's header: the row encoding gives every
        // NULL the same key bytes. That is what makes the match possible at
        // all, and it still requires a NULL on each side.)
        SetOpKind::Intersect => left && right,
        // An EXCEPT emits a subset of the LEFT side's rows and never a right
        // side row, so the right side cannot contribute a NULL no matter what
        // it holds — it can only ever remove rows.
        SetOpKind::Except => left,
    }
}

/// `UNION` / `INTERSECT` / `EXCEPT`, each with and without `ALL`.
///
/// # Blocking
///
/// Unlike [`Distinct`], a set operation (other than `UNION ALL`) cannot
/// decide whether a given row survives without knowing its *total* count on
/// both sides — `EXCEPT ALL` in particular needs `count_left - count_right`,
/// which is not knowable until both children are exhausted. So `SetOp`
/// materializes both sides completely before producing anything, the same
/// documented shape (and the same cancellation gap: this cannot honour
/// `statement_timeout` mid-materialization) as [`crate::sort::Sort`] and
/// [`crate::join::HashJoin`]'s build phase. The whole result comes back from
/// a single `next_batch` call; the next call returns `None`.
///
/// # Arity
///
/// The two sides must have the same number of columns — SQL requires the
/// column *count* (and, informally, type compatibility the planner is
/// responsible for) to line up before a `UNION`/`INTERSECT`/`EXCEPT` can even
/// be written. A mismatch is reported as [`ExecError::TypeMismatch`] at
/// construction time rather than silently truncated to the shorter side's
/// width — see `mismatched_arity_is_an_error_not_a_truncation` below.
///
/// # Output schema
///
/// The output takes the left side's column *names and types* — matching
/// Postgres, which reports the left side's column names for a `UNION`. See
/// `output_schema_uses_the_left_sides_column_names` below.
///
/// Nullability is the one thing it does NOT take from the left alone; it is
/// merged across both sides by [`setop_column_is_nullable`], which see for
/// the rule and the live-Postgres evidence behind it.
///
/// That produces TWO schemas, because a set operation's *intermediate* batch
/// is more permissive than its output. Both sides are concatenated before any
/// row is eliminated, so that intermediate carries whatever either side holds
/// and is declared under `working_schema` (nullable if either side is). The
/// surviving rows are then re-declared under `schema`, which is what
/// [`Operator::schema`] reports: for `UNION` the two are identical, but
/// `INTERSECT` and `EXCEPT` can eliminate every NULL a side contributed and
/// so declare `NOT NULL` where the intermediate could not. Doing this in one
/// schema would force a choice between rejecting the intermediate and
/// under-reporting the output.
///
/// Which side's copy of a row becomes the "representative" when a key appears
/// on both sides is an internal bookkeeping detail (arrival order — the two
/// sides are concatenated left-then-right before keys are computed, so the
/// left side's occurrence is always seen first), not an observable one: the
/// key IS the whole row for `SetOp` (unlike `Distinct ON`, which keys on a
/// subset of columns), so two rows sharing a key are, by construction,
/// identical in every column.
pub struct SetOp {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    op: SetOpKind,
    all: bool,
    budget: usize,
    /// What [`Operator::schema`] reports and the surviving rows are declared
    /// under — nullability per [`setop_column_is_nullable`].
    schema: SchemaRef,
    /// What the concatenation of both sides is declared under, before any row
    /// is eliminated: nullable wherever either side is. Never narrower than
    /// `schema`.
    working_schema: SchemaRef,
    row_converter: RowConverter,
    bytes_used: usize,
    done: bool,
}

impl SetOp {
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        op: SetOpKind,
        all: bool,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if left_schema.fields().len() != right_schema.fields().len() {
            return Err(ExecError::TypeMismatch(format!(
                "{op:?} requires both sides to have the same number of columns: {} vs {}",
                left_schema.fields().len(),
                right_schema.fields().len()
            )));
        }

        // Both schemas are built here, before anything else reads either —
        // see the struct doc's "Output schema". The arity check above has
        // already established the two field lists zip evenly.
        let schema = merge_schemas(&left_schema, &right_schema, |l, r| {
            setop_column_is_nullable(op, l, r)
        });
        let working_schema = merge_schemas(&left_schema, &right_schema, |l, r| l || r);

        let key_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let row_converter =
            RowConverter::new(key_fields).expect("set-op columns support row encoding");

        Ok(Self {
            left,
            right,
            op,
            all,
            budget: memory_budget,
            schema,
            working_schema,
            row_converter,
            bytes_used: 0,
            done: false,
        })
    }

    /// Drain `op` fully into one concatenated batch, charging its array
    /// bytes against `self.budget` starting from `bytes_so_far` (so the two
    /// sides share one running total, mirroring `HashJoin`'s single budget
    /// for its build side). Returns the combined batch and the new running
    /// byte total.
    fn materialize(
        op: &mut dyn Operator,
        budget: usize,
        bytes_so_far: usize,
    ) -> Result<(RecordBatch, usize), ExecError> {
        let schema = op.schema();
        let mut batches = Vec::new();
        let mut bytes = bytes_so_far;
        while let Some(b) = op.next_batch()? {
            let requested = bytes + b.get_array_memory_size();
            if requested > budget {
                return Err(ExecError::OutOfMemory { requested, budget });
            }
            bytes = requested;
            batches.push(b);
        }
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(schema)
        } else {
            concat_batches(&schema, batches.iter()).map_err(arrow_err)?
        };
        Ok((combined, bytes))
    }

    /// Re-declare `batch` — built under the permissive `working_schema` — as
    /// the operator's actual output schema. The two differ only in
    /// nullability, and only for `INTERSECT`/`EXCEPT`, so the column data is
    /// reused untouched; this is a relabelling, not a conversion. (The same
    /// shape `recursive.rs`'s anchor-schema wrapper uses, and for the same
    /// reason: [`Operator::schema`] promises every batch carries exactly the
    /// schema it reports.)
    ///
    /// It can fail, and that is the point. `INTERSECT`'s and `EXCEPT`'s
    /// narrower declarations rest on an argument about which rows can survive
    /// (see [`setop_column_is_nullable`]); if that argument were ever wrong,
    /// `RecordBatch::try_new` rejects a `NOT NULL` field holding a NULL right
    /// here, rather than letting a wrong `RowDescription` reach the client.
    fn declare_output(&self, batch: RecordBatch) -> Result<RecordBatch, ExecError> {
        if batch.schema_ref() == &self.schema {
            return Ok(batch);
        }
        RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
            .map_err(arrow_err)
    }
}

impl Operator for SetOp {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let (left_batch, bytes_after_left) = Self::materialize(self.left.as_mut(), self.budget, 0)?;
        let (right_batch, bytes_total) =
            Self::materialize(self.right.as_mut(), self.budget, bytes_after_left)?;
        self.bytes_used = bytes_total;

        let num_left = left_batch.num_rows();
        // Under `working_schema`, not `schema`: nothing has been eliminated
        // yet, so this batch still holds every NULL either side contributed —
        // including, for `INTERSECT`/`EXCEPT`, ones that will not survive.
        let combined =
            concat_batches(&self.working_schema, [&left_batch, &right_batch]).map_err(arrow_err)?;
        let total_rows = combined.num_rows();
        if total_rows == 0 {
            return Ok(None);
        }

        // `UNION ALL` is a plain concatenation — every row from both sides,
        // no deduplication, no row-key bookkeeping needed at all.
        if self.op == SetOpKind::Union && self.all {
            return self.declare_output(combined).map(Some);
        }

        let rows = self
            .row_converter
            .convert_columns(combined.columns())
            .map_err(arrow_err)?;

        // First arrival index and per-side occurrence counts for every
        // distinct key. Iterated once, left-then-right (since `combined` is
        // `left ++ right`), so `first_index[key]` is always the earliest
        // copy — the left side's, if the key appears there at all, which is
        // what makes EXCEPT's representative row always come from the left
        // side (see the module doc's "Output schema and column order").
        let mut first_index: HashMap<OwnedRow, u32> = HashMap::new();
        let mut left_count: HashMap<OwnedRow, u32> = HashMap::new();
        let mut right_count: HashMap<OwnedRow, u32> = HashMap::new();
        for i in 0..total_rows {
            let key = rows.row(i).owned();
            first_index.entry(key.clone()).or_insert(i as u32);
            let counts = if i < num_left {
                &mut left_count
            } else {
                &mut right_count
            };
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut idx: Vec<u32> = Vec::new();
        for i in 0..total_rows as u32 {
            let key = rows.row(i as usize).owned();
            // Only the representative (first-arrival) row for this key does
            // any work; every later occurrence of the same key is folded
            // into the counts already collected above.
            if first_index[&key] != i {
                continue;
            }
            let l = left_count.get(&key).copied().unwrap_or(0);
            let r = right_count.get(&key).copied().unwrap_or(0);
            let n = match self.op {
                // UNION (not ALL): every key present on either side appears
                // exactly once — deduplicated, per item 2 in the module doc.
                SetOpKind::Union => 1,
                // INTERSECT: only keys present on BOTH sides survive at all.
                // Without ALL, one copy; with ALL, the smaller of the two
                // per-key counts (a multiset intersection) — item 3.
                SetOpKind::Intersect => {
                    if l == 0 || r == 0 {
                        0
                    } else if self.all {
                        l.min(r)
                    } else {
                        1
                    }
                }
                // EXCEPT: only keys present on the left survive. Without ALL,
                // any right-side occurrence at all removes it entirely; with
                // ALL, occurrences are removed one-for-one — item 3.
                SetOpKind::Except => {
                    if l == 0 {
                        0
                    } else if self.all {
                        l.saturating_sub(r)
                    } else if r == 0 {
                        1
                    } else {
                        0
                    }
                }
            };
            idx.extend(std::iter::repeat_n(i, n as usize));
        }

        if idx.is_empty() {
            return Ok(None);
        }
        let survivors = take_batch(&combined, &UInt32Array::from(idx))?;
        self.declare_output(survivors).map(Some)
    }

    fn memory_used(&self) -> usize {
        self.bytes_used
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::DataType;
    use basin_pgtype::PgType;
    use basin_plan::Datum;
    use std::collections::VecDeque;

    /// A child operator that replays a fixed list of batches, one per
    /// `next_batch` call — same test double every other file in this crate
    /// uses, so batch boundaries in a test are exactly the boundaries the
    /// operator under test sees.
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

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<Option<i32>> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    fn lit_null_i32() -> Expr {
        Expr::Literal(Datum::Null, PgType::INT4)
    }

    // ── Values ──────────────────────────────────────────────────────────

    #[test]
    fn values_evaluates_literal_rows_and_reports_a_correct_schema() {
        let rows = vec![
            vec![
                lit_i32(1),
                Expr::Literal(Datum::Utf8("a".into()), PgType::TEXT),
            ],
            vec![
                lit_i32(2),
                Expr::Literal(Datum::Utf8("b".into()), PgType::TEXT),
            ],
        ];
        let mut values = Values::new(rows, vec!["n".into(), "s".into()]).unwrap();
        assert_eq!(values.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(values.schema().field(1).data_type(), &DataType::Utf8);

        let out = values.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(2)]);
        let s = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "a");
        assert_eq!(s.value(1), "b");
        assert!(values.next_batch().unwrap().is_none(), "single-shot output");
    }

    // Item 1 (Values half): the declared shape (column count and names) is
    // reported even with zero literal rows to infer a type from — the wrong
    // answer this prevents is either panicking at construction or silently
    // reporting zero columns instead of the declared two.
    #[test]
    fn values_with_zero_rows_still_reports_the_declared_column_shape() {
        let values = Values::new(vec![], vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().field(0).name(), "a");
        assert_eq!(values.schema().field(1).name(), "b");
    }

    #[test]
    fn values_with_zero_rows_yields_no_batches() {
        let mut values = Values::new(vec![], vec!["a".into()]).unwrap();
        assert!(values.next_batch().unwrap().is_none());
    }

    #[test]
    fn values_rejects_a_row_with_the_wrong_arity() {
        let rows = vec![vec![lit_i32(1), lit_i32(2)], vec![lit_i32(3)]];
        let err = match Values::new(rows, vec!["a".into(), "b".into()]) {
            Err(e) => e,
            Ok(_) => panic!("a row with the wrong number of expressions must not build"),
        };
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // ── Empty ───────────────────────────────────────────────────────────

    // Item 2: `produce_one_row: true` over a zero-column schema (the `SELECT
    // 1` shape) must yield exactly one row with zero columns — not zero rows
    // (which would silently turn every constant SELECT into an empty
    // result), and not a panic from trying to construct a zero-column batch
    // without an explicit row count.
    #[test]
    fn empty_with_produce_one_row_and_no_columns_yields_exactly_one_row() {
        let schema = Arc::new(Schema::empty());
        let mut empty = Empty::new(schema, true);
        let out = empty.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "SELECT 1's underlying Empty must produce one row"
        );
        assert_eq!(out.num_columns(), 0);
        assert!(empty.next_batch().unwrap().is_none());
    }

    #[test]
    fn empty_without_produce_one_row_yields_no_batches() {
        let schema = Arc::new(Schema::empty());
        let mut empty = Empty::new(schema, false);
        assert!(empty.next_batch().unwrap().is_none());
    }

    #[test]
    fn empty_with_produce_one_row_and_declared_columns_fills_them_with_null() {
        let schema = schema_1i32("x");
        let mut empty = Empty::new(schema, true);
        let out = empty.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert!(out.column(0).is_null(0));
    }

    // ── Distinct ────────────────────────────────────────────────────────

    // Item 1 (Distinct half): two all-NULL rows must collapse into one —
    // NULL equals NULL for DISTINCT, the opposite of a join key (see
    // join.rs's `null_keys_never_match_each_other`, which tests the same
    // encoding for the opposite conclusion).
    #[test]
    fn null_equals_null_in_distinct() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![None, None, Some(1)]);
        let input = Feed::boxed(schema, vec![batch]);
        let mut distinct = Distinct::new(input, 1 << 30);
        let mut rows = 0;
        while let Some(b) = distinct.next_batch().unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(
            rows, 2,
            "the two NULLs must collapse into one row, plus the Some(1) row"
        );
    }

    #[test]
    fn distinct_dedups_whole_rows_across_multiple_batches() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(1), Some(2), Some(1)]);
        let b2 = batch_i32(&schema, vec![Some(2), Some(3)]);
        let input = Feed::boxed(schema, vec![b1, b2]);
        let mut distinct = Distinct::new(input, 1 << 30);
        let mut seen: Vec<Option<i32>> = Vec::new();
        while let Some(b) = distinct.next_batch().unwrap() {
            seen.extend(col_i32(&b, 0));
        }
        let mut sorted = seen.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![Some(1), Some(2), Some(3)],
            "each distinct value must appear exactly once, spanning the batch boundary"
        );
    }

    #[test]
    fn distinct_over_empty_input_yields_no_batches() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(schema, vec![]);
        let mut distinct = Distinct::new(input, 1 << 30);
        assert!(distinct.next_batch().unwrap().is_none());
    }

    // A batch that contributes nothing new (every value already seen) still
    // comes back as `Some(empty)`, not `None` — later batches from the same
    // child must still be pulled, the same contract `Filter` documents.
    #[test]
    fn a_distinct_batch_with_nothing_new_is_an_empty_batch_not_end_of_stream() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(1)]);
        let b2 = batch_i32(&schema, vec![Some(1)]); // nothing new
        let b3 = batch_i32(&schema, vec![Some(2)]);
        let input = Feed::boxed(schema, vec![b1, b2, b3]);
        let mut distinct = Distinct::new(input, 1 << 30);
        assert_eq!(distinct.next_batch().unwrap().unwrap().num_rows(), 1);
        let second = distinct.next_batch().unwrap();
        assert!(
            second.is_some(),
            "must still be Some, not None, with nothing new"
        );
        assert_eq!(second.unwrap().num_rows(), 0);
        assert_eq!(distinct.next_batch().unwrap().unwrap().num_rows(), 1);
        assert!(distinct.next_batch().unwrap().is_none());
    }

    // `DISTINCT ON (g)` keeps the FIRST row per key in arrival order, and
    // still emits every column of that row, not just the key column.
    #[test]
    fn distinct_on_keeps_the_first_row_per_key_and_the_whole_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int32, true),
            Field::new("payload", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 1])),
                Arc::new(Int32Array::from(vec![100, 200, 300, 400])),
            ],
        )
        .unwrap();
        let input = Feed::boxed(schema, vec![batch]);
        let mut distinct = Distinct::new_on(input, vec![0], 1 << 30);
        let out = distinct.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2, "one row per distinct key (g=1, g=2)");
        let g: Vec<Option<i32>> = col_i32(&out, 0);
        let payload: Vec<Option<i32>> = col_i32(&out, 1);
        let mut rows: Vec<(Option<i32>, Option<i32>)> = g.into_iter().zip(payload).collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![(Some(1), Some(100)), (Some(2), Some(300))],
            "g=1's FIRST row (payload=100) must win, not the later payload=200/400 rows"
        );
    }

    #[test]
    fn distinct_reports_memory_growing_with_distinct_keys_seen() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(1), Some(1), Some(2)]);
        let input = Feed::boxed(schema, vec![batch]);
        let mut distinct = Distinct::new(input, 1 << 30);
        assert_eq!(
            distinct.memory_used(),
            0,
            "nothing consumed before the first pull"
        );
        distinct.next_batch().unwrap();
        let after_one_batch = distinct.memory_used();
        assert!(
            after_one_batch > 0,
            "two distinct keys were seen, memory_used must reflect that"
        );
    }

    #[test]
    fn distinct_exceeding_its_budget_is_out_of_memory() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, (0..1000).map(Some).collect());
        let input = Feed::boxed(schema, vec![batch]);
        let mut distinct = Distinct::new(input, 8);
        let err = distinct.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }

    // ── SetOp ───────────────────────────────────────────────────────────

    fn setop(
        left: Vec<Option<i32>>,
        right: Vec<Option<i32>>,
        op: SetOpKind,
        all: bool,
    ) -> Vec<Option<i32>> {
        let schema = schema_1i32("x");
        let l = Feed::boxed(schema.clone(), vec![batch_i32(&schema, left)]);
        let r = Feed::boxed(schema.clone(), vec![batch_i32(&schema, right)]);
        let mut op = SetOp::new(l, r, op, all, 1 << 30).unwrap();
        let mut out: Vec<Option<i32>> = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.extend(col_i32(&b, 0));
        }
        out.sort();
        out
    }

    // Item 1 (SetOp half): `SELECT NULL UNION SELECT NULL` is one row, not
    // two — NULL equals NULL for set membership, unlike a join key.
    #[test]
    fn null_equals_null_in_union() {
        let out = setop(vec![None], vec![None], SetOpKind::Union, false);
        assert_eq!(
            out,
            vec![None],
            "two NULLs on either side of UNION must dedup to one row"
        );
    }

    // The literal end-to-end shape of `SELECT NULL UNION SELECT NULL`: two
    // `Values` operators (mirroring how each side of a `SELECT`-list-only
    // query plans) feeding a `SetOp`, rather than a `Feed` handing over an
    // already-NULL Arrow array directly. Exercises `Values`, `eval::eval`'s
    // literal-NULL handling, and `SetOp`'s NULL-equals-NULL rule together.
    #[test]
    fn select_null_union_select_null_end_to_end() {
        let mut left = Values::new(vec![vec![lit_null_i32()]], vec!["?column?".into()]).unwrap();
        let mut right = Values::new(vec![vec![lit_null_i32()]], vec!["?column?".into()]).unwrap();
        let l = left.next_batch().unwrap().unwrap();
        let r = right.next_batch().unwrap().unwrap();
        let schema = left.schema();
        let l_op = Feed::boxed(schema.clone(), vec![l]);
        let r_op = Feed::boxed(schema, vec![r]);
        let mut op = SetOp::new(l_op, r_op, SetOpKind::Union, false, 1 << 30).unwrap();
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "SELECT NULL UNION SELECT NULL must be one row"
        );
        assert!(out.column(0).is_null(0));
    }

    // Item 2: UNION deduplicates, UNION ALL does not.
    #[test]
    fn union_dedups_but_union_all_does_not() {
        let deduped = setop(
            vec![Some(1), Some(2)],
            vec![Some(2), Some(3)],
            SetOpKind::Union,
            false,
        );
        assert_eq!(deduped, vec![Some(1), Some(2), Some(3)]);

        let all = setop(
            vec![Some(1), Some(2)],
            vec![Some(2), Some(3)],
            SetOpKind::Union,
            true,
        );
        assert_eq!(
            all,
            vec![Some(1), Some(2), Some(2), Some(3)],
            "UNION ALL must keep the duplicate 2 from both sides"
        );
    }

    // Item 3: EXCEPT removes every occurrence of a matching row; EXCEPT ALL
    // removes them one-for-one (multiset difference).
    #[test]
    fn except_removes_every_occurrence_but_except_all_is_one_for_one() {
        // left has three 1's, right has one 1.
        let left = vec![Some(1), Some(1), Some(1), Some(2)];
        let right = vec![Some(1)];

        let plain = setop(left.clone(), right.clone(), SetOpKind::Except, false);
        assert_eq!(
            plain,
            vec![Some(2)],
            "plain EXCEPT drops every 1 from the left, since 1 appears on the right at all"
        );

        let all = setop(left, right, SetOpKind::Except, true);
        assert_eq!(
            all,
            vec![Some(1), Some(1), Some(2)],
            "EXCEPT ALL removes only ONE of the three 1's (one-for-one against the right's single 1)"
        );
    }

    // Item 3 (INTERSECT half): same multiset-vs-set distinction as EXCEPT.
    #[test]
    fn intersect_is_deduped_but_intersect_all_is_multiset_min() {
        let left = vec![Some(1), Some(1), Some(1), Some(2)];
        let right = vec![Some(1), Some(1)];

        let plain = setop(left.clone(), right.clone(), SetOpKind::Intersect, false);
        assert_eq!(
            plain,
            vec![Some(1)],
            "plain INTERSECT: one copy of 1, 2 is absent from the right"
        );

        let all = setop(left, right, SetOpKind::Intersect, true);
        assert_eq!(
            all,
            vec![Some(1), Some(1)],
            "INTERSECT ALL keeps min(3, 2) = 2 copies of 1, and 2 is still absent"
        );
    }

    // Item 4: mismatched arity is an error, never a silent truncation to the
    // shorter side's column count.
    #[test]
    fn mismatched_arity_is_an_error_not_a_truncation() {
        let left_schema = schema_1i32("x");
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let l = Feed::boxed(left_schema, vec![]);
        let r = Feed::boxed(right_schema, vec![]);
        let err = match SetOp::new(l, r, SetOpKind::Union, false, 1 << 30) {
            Err(e) => e,
            Ok(_) => panic!("mismatched arity must not build"),
        };
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // Item 5: empty-input identities. `A UNION ALL {}` is A; `A INTERSECT {}`
    // is empty; `{} EXCEPT A` is empty.
    #[test]
    fn empty_input_identities() {
        let a = vec![Some(1), Some(2), Some(2)];

        let union_all_empty = setop(a.clone(), vec![], SetOpKind::Union, true);
        let mut expected_a = a.clone();
        expected_a.sort();
        assert_eq!(
            union_all_empty, expected_a,
            "A UNION ALL {{}} must be exactly A, duplicates included"
        );

        let intersect_empty = setop(a.clone(), vec![], SetOpKind::Intersect, false);
        assert_eq!(
            intersect_empty,
            Vec::<Option<i32>>::new(),
            "A INTERSECT {{}} must be empty"
        );

        let except_from_empty = setop(vec![], a, SetOpKind::Except, false);
        assert_eq!(
            except_from_empty,
            Vec::<Option<i32>>::new(),
            "{{}} EXCEPT A must be empty"
        );
    }

    #[test]
    fn both_sides_empty_yields_no_batches_for_every_kind() {
        for op in [SetOpKind::Union, SetOpKind::Intersect, SetOpKind::Except] {
            for all in [false, true] {
                let out = setop(vec![], vec![], op, all);
                assert!(out.is_empty(), "{op:?} all={all}");
            }
        }
    }

    // Output schema (item: column names) — a `UNION`/`INTERSECT`/`EXCEPT`
    // reports the LEFT side's column names, even when the right side's
    // schema uses different ones (both must still agree on column count and
    // type, but names are allowed to differ, e.g. `SELECT x FROM a UNION
    // SELECT y FROM b`).
    #[test]
    fn output_schema_uses_the_left_sides_column_names() {
        let left_schema = schema_1i32("left_name");
        let right_schema = schema_1i32("right_name");
        let l = Feed::boxed(left_schema, vec![]);
        let r = Feed::boxed(right_schema, vec![]);
        let op = SetOp::new(l, r, SetOpKind::Union, false, 1 << 30).unwrap();
        assert_eq!(op.schema().field(0).name(), "left_name");
    }

    // ── Output schema: nullability ──────────────────────────────────────
    //
    // The regression these guard against: `SetOp` used to adopt the left
    // side's schema WHOLE, nullability included. That was invisible for as
    // long as `project.rs` declared every column nullable, and became a live
    // error the moment it started inferring — `SELECT id FROM t UNION ALL
    // SELECT tid FROM u` (`t.id BIGINT NOT NULL`, `u.tid BIGINT`) failed at
    // `RecordBatch::try_new`, because the combined batch carries the right
    // side's NULLs under the left side's `NOT NULL` field.
    //
    // A schema-only assertion would not have caught it (nothing checks a
    // declared schema against reality until data arrives), and a data-only
    // assertion would not have said why. So both, on the same operators.

    /// A one-column `Int32` schema declared `NOT NULL` — the left side of
    /// every test below, standing in for a `BIGINT NOT NULL` table column.
    fn schema_1i32_notnull(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
    }

    // The exact shape that regressed, with the value assertion that says the
    // NULL is not merely tolerated but RETURNED — live Postgres 18.2 answers
    // `1, 2, NULL` for `SELECT id FROM t UNION ALL SELECT tid FROM u` reduced
    // to this data.
    #[test]
    fn union_all_of_a_notnull_left_and_a_nullable_right_keeps_the_right_sides_null() {
        let left_schema = schema_1i32_notnull("id");
        let right_schema = schema_1i32("tid");
        let l = Feed::boxed(
            left_schema.clone(),
            vec![batch_i32(&left_schema, vec![Some(1)])],
        );
        let r = Feed::boxed(
            right_schema.clone(),
            vec![batch_i32(&right_schema, vec![Some(2), None])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Union, true, 1 << 30).unwrap();

        assert!(
            op.schema().field(0).is_nullable(),
            "a NULL arrives from the right side, so the output column is nullable"
        );
        let out = op
            .next_batch()
            .expect("a nullable output column accepts the right side's NULL")
            .expect("three rows");
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(2), None]);
    }

    // The sweep, and the test that would have caught the regression before it
    // was committed. Every kind, with and without ALL, concatenates both sides
    // before any kind-specific logic runs, so a left-only nullability rule
    // breaks all six — including the ones whose final answer contains no NULL
    // at all. Testing only the shape that happened to get reported would have
    // left five equally broken paths behind.
    //
    // The declared nullability is checked against `setop_column_is_nullable`
    // rather than a literal, so this stays a statement about the operator
    // agreeing with the rule; the rule itself is pinned separately by
    // `the_nullability_rule_differs_per_operator` below.
    #[test]
    fn a_nullable_right_side_never_makes_the_output_reject_its_nulls() {
        for kind in [SetOpKind::Union, SetOpKind::Intersect, SetOpKind::Except] {
            for all in [false, true] {
                let left_schema = schema_1i32_notnull("id");
                let right_schema = schema_1i32("tid");
                let l = Feed::boxed(
                    left_schema.clone(),
                    vec![batch_i32(&left_schema, vec![Some(1), Some(2)])],
                );
                let r = Feed::boxed(
                    right_schema.clone(),
                    vec![batch_i32(&right_schema, vec![Some(2), None])],
                );
                let mut op = SetOp::new(l, r, kind, all, 1 << 30).unwrap();
                assert_eq!(
                    op.schema().field(0).is_nullable(),
                    setop_column_is_nullable(kind, false, true),
                    "{kind:?} all={all}: declared nullability disagrees with the rule"
                );
                // The regression signal: this errored for all six.
                op.next_batch().unwrap_or_else(|e| {
                    panic!("{kind:?} all={all} rejected the right side's NULL: {e}")
                });
            }
        }
    }

    // The rule itself, as a truth table, so the three operators cannot quietly
    // collapse into one. Each `false` is an argument about which rows survive,
    // not a measurement — see `setop_column_is_nullable`.
    #[test]
    fn the_nullability_rule_differs_per_operator() {
        use SetOpKind::{Except, Intersect, Union};
        // A UNION emits both sides' rows: either side's NULL reaches the
        // output.
        assert!(!setop_column_is_nullable(Union, false, false));
        assert!(setop_column_is_nullable(Union, false, true));
        assert!(setop_column_is_nullable(Union, true, false));
        assert!(setop_column_is_nullable(Union, true, true));
        // An INTERSECT needs the NULL-bearing row on BOTH sides.
        assert!(!setop_column_is_nullable(Intersect, false, false));
        assert!(!setop_column_is_nullable(Intersect, false, true));
        assert!(!setop_column_is_nullable(Intersect, true, false));
        assert!(setop_column_is_nullable(Intersect, true, true));
        // An EXCEPT emits only left-side rows; the right side can remove rows
        // but never contribute one.
        assert!(!setop_column_is_nullable(Except, false, false));
        assert!(!setop_column_is_nullable(Except, false, true));
        assert!(setop_column_is_nullable(Except, true, false));
        assert!(setop_column_is_nullable(Except, true, true));
    }

    // `INTERSECT` against a `NOT NULL` side, with the data that proves the
    // narrower declaration honest: the right side's NULL is present in the
    // materialized concatenation and eliminated by the intersection, so the
    // output really does hold no NULL. Live Postgres 18.2 agrees by value —
    // `SELECT id FROM t INTERSECT SELECT tid FROM u` returns `1, 2` and no
    // NULL row.
    #[test]
    fn intersect_with_one_notnull_side_reports_notnull_and_drops_the_null() {
        let left_schema = schema_1i32_notnull("id");
        let right_schema = schema_1i32("tid");
        let l = Feed::boxed(
            left_schema.clone(),
            vec![batch_i32(&left_schema, vec![Some(1), Some(2)])],
        );
        let r = Feed::boxed(
            right_schema.clone(),
            vec![batch_i32(&right_schema, vec![Some(2), None])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Intersect, false, 1 << 30).unwrap();

        assert!(
            !op.schema().field(0).is_nullable(),
            "a NULL cannot survive an INTERSECT unless both sides can hold one"
        );
        let out = op.next_batch().unwrap().expect("one row");
        assert_eq!(col_i32(&out, 0), vec![Some(2)]);
    }

    // Two nullable sides is the case where `INTERSECT` really can emit a NULL
    // — the other half of `left && right`, and the reason it is not simply
    // `false`. Live Postgres: `SELECT tid FROM u INTERSECT SELECT tid FROM u`
    // keeps the NULL row.
    #[test]
    fn intersect_of_two_nullable_sides_keeps_the_null() {
        let schema = schema_1i32("tid");
        let l = Feed::boxed(
            schema.clone(),
            vec![batch_i32(&schema, vec![Some(1), None])],
        );
        let r = Feed::boxed(
            schema.clone(),
            vec![batch_i32(&schema, vec![Some(1), None])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Intersect, false, 1 << 30).unwrap();

        assert!(op.schema().field(0).is_nullable());
        let out = op.next_batch().unwrap().expect("two rows");
        assert_eq!(col_i32(&out, 0), vec![Some(1), None]);
    }

    // `EXCEPT` ignores the right side's nullability entirely: its output is a
    // subset of the LEFT side's rows. Live Postgres 18.2: `SELECT id FROM t
    // EXCEPT ALL SELECT tid FROM u` returns no NULL row even though `u.tid`
    // holds one.
    #[test]
    fn except_takes_its_nullability_from_the_left_side_alone() {
        let left_schema = schema_1i32_notnull("id");
        let right_schema = schema_1i32("tid");
        let l = Feed::boxed(
            left_schema.clone(),
            vec![batch_i32(&left_schema, vec![Some(1), Some(3)])],
        );
        let r = Feed::boxed(
            right_schema.clone(),
            vec![batch_i32(&right_schema, vec![Some(1), None])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Except, true, 1 << 30).unwrap();

        assert!(
            !op.schema().field(0).is_nullable(),
            "the right side's NULL can only remove rows, never appear in them"
        );
        let out = op.next_batch().unwrap().expect("one row");
        assert_eq!(col_i32(&out, 0), vec![Some(3)]);
    }

    // Nullability is merged, not inherited from whichever side happens to be
    // second: a nullable LEFT and a `NOT NULL` right is nullable too, and the
    // left's own NULL survives.
    #[test]
    fn a_nullable_left_side_makes_the_output_nullable_too() {
        let left_schema = schema_1i32("tid");
        let right_schema = schema_1i32_notnull("id");
        let l = Feed::boxed(
            left_schema.clone(),
            vec![batch_i32(&left_schema, vec![Some(1), None])],
        );
        let r = Feed::boxed(
            right_schema.clone(),
            vec![batch_i32(&right_schema, vec![Some(2)])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Union, true, 1 << 30).unwrap();

        assert!(op.schema().field(0).is_nullable());
        let out = op.next_batch().unwrap().expect("three rows");
        assert_eq!(col_i32(&out, 0), vec![Some(1), None, Some(2)]);
    }

    // The other half of the rule, and the reason it is `||` rather than a
    // blanket `true`: two `NOT NULL` sides still report `NOT NULL`. Postgres
    // agrees — `EXPLAIN (COSTS OFF)` over a `UNION ALL` of two `NOT NULL`
    // columns DELETES an `IS NOT NULL` filter above it, having proven it
    // cannot fail. Widening everything back to nullable here would fix the
    // error by giving up the inference that motivated it.
    #[test]
    fn two_notnull_sides_stay_notnull() {
        let left_schema = schema_1i32_notnull("id");
        let right_schema = schema_1i32_notnull("uid");
        let l = Feed::boxed(
            left_schema.clone(),
            vec![batch_i32(&left_schema, vec![Some(1)])],
        );
        let r = Feed::boxed(
            right_schema.clone(),
            vec![batch_i32(&right_schema, vec![Some(2)])],
        );
        let mut op = SetOp::new(l, r, SetOpKind::Union, true, 1 << 30).unwrap();

        assert!(
            !op.schema().field(0).is_nullable(),
            "neither side can produce a NULL, so the output column keeps NOT NULL"
        );
        let out = op.next_batch().unwrap().expect("two rows");
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(2)]);
    }

    // Per column, not per operator: a two-column set operation merges each
    // position independently, so one nullable column does not drag the other
    // along with it.
    #[test]
    fn nullability_is_merged_column_by_column() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, true),
        ]));
        let l = Feed::boxed(left_schema, vec![]);
        let r = Feed::boxed(right_schema, vec![]);
        let op = SetOp::new(l, r, SetOpKind::Union, false, 1 << 30).unwrap();

        assert!(!op.schema().field(0).is_nullable(), "NOT NULL on both sides");
        assert!(op.schema().field(1).is_nullable(), "nullable on the right");
    }

    // Multi-batch input on both sides: the whole point of materializing
    // fully before deciding anything is that a key's total count is only
    // known once every batch (on both sides) has been seen.
    #[test]
    fn setop_counts_span_multiple_input_batches_on_both_sides() {
        let schema = schema_1i32("x");
        let l = Feed::boxed(
            schema.clone(),
            vec![
                batch_i32(&schema, vec![Some(1), Some(1)]),
                batch_i32(&schema, vec![Some(1)]),
            ],
        );
        let r = Feed::boxed(
            schema.clone(),
            vec![
                batch_i32(&schema, vec![Some(1)]),
                batch_i32(&schema, vec![Some(1), Some(1)]),
            ],
        );
        // left has 3 ones (across 2 batches), right has 3 ones (across 2
        // batches) — EXCEPT ALL must see all of both to compute 3 - 3 = 0.
        let mut op = SetOp::new(l, r, SetOpKind::Except, true, 1 << 30).unwrap();
        let out = op.next_batch().unwrap();
        assert!(
            out.is_none(),
            "3 - 3 = 0 copies must survive, spanning batches on both sides"
        );
    }

    #[test]
    fn setop_reports_no_memory_before_the_first_pull_and_grows_after() {
        let schema = schema_1i32("x");
        let l = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let r = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(2)])]);
        let mut op = SetOp::new(l, r, SetOpKind::Union, false, 1 << 30).unwrap();
        assert_eq!(op.memory_used(), 0);
        op.next_batch().unwrap();
        assert!(op.memory_used() > 0);
    }

    #[test]
    fn setop_exceeding_its_budget_is_out_of_memory() {
        let schema = schema_1i32("x");
        let big = batch_i32(&schema, (0..1000).map(Some).collect());
        let size = big.get_array_memory_size();
        let l = Feed::boxed(schema.clone(), vec![big]);
        let r = Feed::boxed(schema.clone(), vec![]);
        let mut op = SetOp::new(l, r, SetOpKind::Union, true, size - 1).unwrap();
        let err = op.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }
}
