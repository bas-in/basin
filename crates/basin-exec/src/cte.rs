//! `CteBuffer`/`CteReader` (materialize-once, replay-many `WITH` bodies) and
//! [`ProjectSet`] (set-returning functions in a target list).
//!
//! # Why this file exists
//!
//! `build.rs` refuses `LogicalPlan::Cte`, `CteRef` and `ProjectSet` today —
//! see `an_unbuildable_plan_names_the_construct` in that module's tests. Of
//! the two, `ProjectSet` is the one that actually motivated the owned engine:
//! `basin-engine/src/jsonb_udf.rs:16` records that set-returning functions
//! "cannot be true SRFs inside DataFusion", so `generate_series`/`unnest` in a
//! `SELECT` list are approximated rather than run for real today. This module
//! is where that stops being true — see [`ProjectSet`] below.
//!
//! # CTE materialization: why once, not per reference
//!
//! Postgres materializes a `WITH` body — always before v12's inlining, and
//! still today whenever the CTE is referenced more than once or is
//! data-modifying (`INSERT`/`UPDATE`/`DELETE` inside `WITH`, which
//! `basin_plan::LogicalPlan::Cte` already allows nesting, per that crate's
//! module docs). Materialize-once/replay-many is therefore the right default
//! physical shape, not just a convenient one: a data-modifying CTE body must
//! run exactly once regardless of how many times its result is read, and
//! `SELECT` bodies referenced twice (`WITH x AS (...) SELECT * FROM x a JOIN
//! x b ON ...`) must not silently recompute (or, worse, silently return zero
//! rows to the second reference because the first one drained a shared
//! stream).
//!
//! [`CteBuffer`] owns the body operator and a shared, `RefCell`-guarded
//! materialization state; [`CteBuffer::reader`] hands out a [`CteReader`] per
//! `CteRef` occurrence. The first reader to be pulled drains the body fully
//! (the same blocking shape [`crate::setop::SetOp`] and
//! [`crate::window::WindowAgg`] already use for "must see everything before
//! anything is valid" operators) and every reader — including ones created
//! *after* materialization already happened — replays the identical batches
//! from position zero. `Rc`, not `Arc`: this crate's whole physical layer is
//! single-partition (`lib.rs`'s module docs, "Single-partition first"), so
//! there is no cross-thread sharing to support, and `Rc<RefCell<_>>` is the
//! plain single-threaded equivalent of the pattern.
//!
//! `WITH RECURSIVE` is out of scope here. `LogicalPlan::Cte::recursive` is
//! carried by the plan but not consulted by this file — recursive evaluation
//! needs an actual fixed-point loop (repeatedly re-running the body against
//! its own prior output until it stops growing), which is a different
//! operator shape than "materialize once, replay many", not a variant of it.
//! Reaching a recursive CTE here today would silently run the non-recursive
//! body once, which is why nothing in `build.rs` wires this module to a
//! `recursive: true` node — that wiring is a separate increment.
//!
//! # `ProjectSet` — set-returning functions, verified against a live server
//!
//! Per this crate's project convention (see `window.rs`'s module docs),
//! everything below was checked against `psql -U postgres` on PostgreSQL
//! 18.2 rather than recalled, because recall has been wrong before — and it
//! was wrong again while writing this file. The starting brief for this
//! operator claimed multiple SRFs in one target list expand to the **least
//! common multiple** of their row counts, with shorter ones **cycling**.
//! Live Postgres does neither:
//!
//! 1. **A single SRF is unsurprising**: `SELECT generate_series(1,3)` is 3
//!    rows. See [`project_set_single_srf_generate_series_yields_n_rows`].
//! 2. **Multiple SRFs run in lockstep to the row count of the LONGEST one,
//!    NOT the LCM, and the shorter ones are NULL-padded, NOT cycled.**
//!    `SELECT generate_series(1,2), generate_series(1,3)` gives:
//!    ```text
//!     generate_series | generate_series
//!    -----------------+-----------------
//!                    1 |               1
//!                    2 |               2
//!                      |               3
//!    ```
//!    3 rows (`max(2,3)`, not `lcm(2,3)=6`), and the first column's third row
//!    is NULL, not a cycled repeat of `1`. This is Postgres's documented
//!    behaviour since version 10 (`SRF-in-targetlist` release note): before
//!    10, multiple SRFs were the CROSS PRODUCT of their outputs; 10 replaced
//!    that with lockstep-with-NULL-padding, and neither historical rule is
//!    "LCM with cycling". See
//!    [`project_set_multiple_srfs_run_in_lockstep_to_the_longest_not_the_lcm`].
//! 3. **A non-SRF column is repeated once per generated row of its input
//!    row**: `SELECT 'x' AS tag, generate_series(1,2), generate_series(1,3)`
//!    repeats `'x'` on all 3 output rows. See
//!    [`project_set_non_srf_columns_repeat_per_generated_row`].
//! 4. **A SOLE SRF yielding zero rows for a given input row eliminates that
//!    row entirely** — `SELECT generate_series(1,0)` is 0 rows, and a
//!    3-row source table with one row's bound at 0 loses exactly that row
//!    (`SELECT id, generate_series(1,n) FROM t` with `n = 3,0,1` yields id 1
//!    three times and id 3 once — id 2 is gone, not present with a NULL).
//!    See [`project_set_a_single_srfs_zero_rows_eliminates_the_input_row`].
//! 5. **But when SEVERAL SRFs share a row, one of them being empty does NOT
//!    eliminate the row** — only item 2's `max`, not any individual SRF's
//!    count, decides survival. `SELECT generate_series(1,0),
//!    generate_series(1,3)` is 3 rows, with the first column NULL on every
//!    one — not 0 rows, and not "eliminate rows where *any* SRF is empty".
//!    See [`project_set_one_srf_empty_among_several_nulls_it_rather_than_eliminating_the_row`].
//! 6. **`generate_series` is STRICT**: any NULL argument (start, stop, or
//!    step) yields zero rows for that row, not an error and not one
//!    all-NULL row — `SELECT generate_series(NULL::int,3)` is 0 rows. See
//!    [`project_set_generate_series_null_argument_yields_zero_rows`].
//! 7. **A step of exactly `0` is an ERROR** (`ERROR: step size cannot equal
//!    zero`, SQLSTATE `22023`), but only once start/stop/step are all
//!    non-NULL — `generate_series(NULL,5,0)` is empty (item 6 short-circuits
//!    first), while `generate_series(1,5,0)` raises. See
//!    [`project_set_generate_series_zero_step_is_an_error`].
//! 8. **Negative step walks downward, and a step whose sign disagrees with
//!    the start/stop direction yields zero rows, not an error**:
//!    `generate_series(5,1,-1)` is 5 rows counting down; `generate_series(
//!    1,5,-1)` is 0 rows. See
//!    [`project_set_generate_series_negative_step_descends`] and
//!    [`project_set_generate_series_step_direction_mismatch_yields_zero_rows`].
//! 9. **`unnest` explodes an array's elements in order, including NULL
//!    elements**; a NULL array or an empty array both yield zero rows
//!    (`unnest(NULL::int[])` and `unnest('{}'::int[])` are both empty). See
//!    [`project_set_unnest_explodes_array_elements_in_order`],
//!    [`project_set_unnest_preserves_null_elements`],
//!    [`project_set_unnest_of_a_null_array_yields_zero_rows`] and
//!    [`project_set_unnest_of_an_empty_array_yields_zero_rows`].
//! 10. **`unnest` and `generate_series` share the same lockstep rule** when
//!     mixed in one list — `SELECT generate_series(1,2), unnest(array[100,
//!     200,300])` is 3 rows, first column NULL on the third. See
//!     [`project_set_generate_series_and_unnest_together_run_in_lockstep`].
//!
//! Only `generate_series` (`int4`/`int8` forms) and `unnest(anyarray)` are
//! implemented — real, verified OIDs from a live `pg_proc`
//! (`generate_series(int,int[,int])` is 1067/1066, `generate_series(bigint,
//! bigint[,bigint])` is 1069/1068, `unnest(anyarray)` is 2331). Any other
//! set-returning function is a named [`ExecError::Internal`], not a silent
//! wrong answer — the same posture `build.rs`'s `BuildError::Unsupported`
//! takes for whole unimplemented plan shapes.
//!
//! # `ProjectSet`'s memory contract — the deliberate NON-buffering half
//!
//! Unlike [`CteReader`], `ProjectSet` does not buffer anything across
//! batches: row expansion (including the lockstep/NULL-pad computation in
//! items 2 and 5 above) is entirely a per-row, per-input-batch computation,
//! so `next_batch` pulls exactly one batch from its child and returns exactly
//! one (possibly row-count-changed) batch, the same shape as
//! [`crate::project::Filter`]. `memory_used` reflects that honestly by
//! delegating straight to the child, holding nothing of its own — see
//! `filter_and_project_report_no_retained_memory` in `project.rs` for the
//! precedent this mirrors.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use basin_plan::Expr;

use crate::eval;
use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

// ============================================================================
// CteBuffer / CteReader
// ============================================================================

/// Materialization state shared by a [`CteBuffer`] and every [`CteReader`] it
/// hands out. `Failed` caches an evaluation error (`ExecError` is `Clone`) so
/// a second reader that pulls after a first reader's materialization failed
/// gets the same error back rather than silently re-draining an already
/// partially-consumed, broken body operator.
enum CteState {
    Pending(Box<dyn Operator>),
    Materialized {
        batches: Rc<Vec<RecordBatch>>,
        bytes_used: usize,
    },
    Failed(ExecError),
}

/// Owns a CTE body operator and lets it be materialized once and replayed by
/// as many [`CteReader`]s as the query has `CteRef`s to this `CteId`. See the
/// module docs for why "materialize once, replay many" is the right default
/// rather than a shortcut.
pub struct CteBuffer {
    state: Rc<RefCell<CteState>>,
    schema: SchemaRef,
    budget: usize,
}

impl CteBuffer {
    /// `body` is the operator tree for the CTE's own query
    /// (`LogicalPlan::Cte::body`, built the same way any other subplan is).
    /// Nothing is pulled from it yet — materialization is lazy, triggered by
    /// the first [`CteReader::next_batch`] call across every reader this
    /// buffer ever hands out. `memory_budget` bounds the fully-buffered
    /// result, the same contract [`crate::setop::SetOp`] and
    /// [`crate::window::WindowAgg`] already apply to their own blocking
    /// materialization.
    pub fn new(body: Box<dyn Operator>, memory_budget: usize) -> Self {
        let schema = body.schema();
        Self {
            state: Rc::new(RefCell::new(CteState::Pending(body))),
            schema,
            budget: memory_budget,
        }
    }

    /// A new handle onto this buffer's (possibly not-yet-materialized)
    /// result. Every reader — whether created before or after another reader
    /// has already triggered materialization — replays the identical batches
    /// from its own independent position, starting at 0.
    pub fn reader(&self) -> CteReader {
        CteReader {
            state: Rc::clone(&self.state),
            schema: Arc::clone(&self.schema),
            budget: self.budget,
            pos: 0,
        }
    }

    /// Bytes held by the materialized result, or 0 before materialization has
    /// happened. Shared: this is the same number every [`CteReader::memory_used`]
    /// derived from this buffer reports, not a per-reader multiple of it —
    /// see the module docs' note on what that implies for a caller that sums
    /// memory across a whole operator tree.
    pub fn memory_used(&self) -> usize {
        match &*self.state.borrow() {
            CteState::Materialized { bytes_used, .. } => *bytes_used,
            CteState::Pending(_) | CteState::Failed(_) => 0,
        }
    }
}

/// One reader over a [`CteBuffer`]'s (shared, materialize-once) result. Each
/// `CteRef` in the plan becomes one `CteReader`.
///
/// # A known, named tradeoff, not a bug
///
/// Every `CteReader` sharing one `CteBuffer` reports the SAME `memory_used`
/// (the buffer's total materialized bytes), because they share the one
/// underlying `Rc<Vec<RecordBatch>>` rather than each holding their own copy.
/// A parent memory accountant that naively sums `memory_used()` across every
/// leaf of the operator tree will over-count a CTE referenced N times by a
/// factor of N, even though only one copy of the data exists. Fixing that
/// requires the accountant to know about sharing, which is a caller-side
/// concern outside this operator's reach — named here rather than silently
/// producing a budget number nobody can trust, the same posture `window.rs`
/// takes toward its own named performance gap.
pub struct CteReader {
    state: Rc<RefCell<CteState>>,
    schema: SchemaRef,
    budget: usize,
    pos: usize,
}

impl CteReader {
    /// Drain the body fully into `state`, unless another reader already did
    /// (successfully or not). The body is taken out of `state` (leaving a
    /// placeholder) before draining, so draining itself never holds the
    /// `RefCell` borrow — the loop below calls into an arbitrary child
    /// operator, and there is no reason to assume it will not, however
    /// indirectly, touch this same buffer.
    fn materialize(&self) -> Result<(), ExecError> {
        {
            match &*self.state.borrow() {
                CteState::Materialized { .. } => return Ok(()),
                CteState::Failed(e) => return Err(e.clone()),
                CteState::Pending(_) => {}
            }
        }

        let mut body = {
            let mut st = self.state.borrow_mut();
            let placeholder = CteState::Failed(ExecError::Internal(
                "CTE buffer materialized re-entrantly (a cyclic CTE reference?)".into(),
            ));
            match std::mem::replace(&mut *st, placeholder) {
                CteState::Pending(b) => b,
                CteState::Materialized { .. } | CteState::Failed(_) => {
                    unreachable!("checked Pending under the same borrow discipline above")
                }
            }
        };

        let mut batches = Vec::new();
        let mut bytes = 0usize;
        let result: Result<(), ExecError> = (|| {
            while let Some(b) = body.next_batch()? {
                let requested = bytes + b.get_array_memory_size();
                if requested > self.budget {
                    return Err(ExecError::OutOfMemory {
                        requested,
                        budget: self.budget,
                    });
                }
                bytes = requested;
                batches.push(b);
            }
            Ok(())
        })();

        let mut st = self.state.borrow_mut();
        *st = match &result {
            Ok(()) => CteState::Materialized {
                batches: Rc::new(batches),
                bytes_used: bytes,
            },
            Err(e) => CteState::Failed(e.clone()),
        };
        result
    }
}

impl Operator for CteReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        self.materialize()?;
        let st = self.state.borrow();
        let CteState::Materialized { batches, .. } = &*st else {
            unreachable!("materialize() only returns Ok(()) after reaching Materialized")
        };
        let batch = batches.get(self.pos).cloned();
        self.pos += 1;
        Ok(batch)
    }

    fn memory_used(&self) -> usize {
        match &*self.state.borrow() {
            CteState::Materialized { bytes_used, .. } => *bytes_used,
            CteState::Pending(_) | CteState::Failed(_) => 0,
        }
    }
}

// ============================================================================
// ProjectSet
// ============================================================================

/// Which set-returning function an [`Expr::SetReturning`] resolved to. See
/// the module docs for the exact `pg_proc` OIDs behind each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SrfKind {
    /// `generate_series(int[,int,int])` — OID 1067 (2-arg) / 1066 (3-arg).
    GenerateSeriesI32,
    /// `generate_series(bigint[,bigint,bigint])` — OID 1069 (2-arg) / 1068
    /// (3-arg).
    GenerateSeriesI64,
    /// `unnest(anyarray)` — OID 2331.
    Unnest,
}

fn srf_kind_of(oid: u32) -> Option<SrfKind> {
    match oid {
        1066 | 1067 => Some(SrfKind::GenerateSeriesI32),
        1068 | 1069 => Some(SrfKind::GenerateSeriesI64),
        2331 => Some(SrfKind::Unnest),
        _ => None,
    }
}

/// One resolved target-list SRF: which function, and its (still-unevaluated)
/// argument expressions, evaluated fresh against every input batch — mirrors
/// `WindowSpec`/`AggregateSpec` carrying `Expr`-shaped arguments while keying
/// column *references* by position (this file resolves arguments through
/// `eval::eval` rather than pre-resolving to column positions, since a
/// `generate_series` bound is an arbitrary expression, not necessarily a bare
/// column).
struct ResolvedSrf {
    kind: SrfKind,
    args: Vec<Expr>,
}

/// The target list — `SELECT ..., generate_series(...), unnest(...), ...`.
///
/// See the module docs for the exact lockstep/NULL-pad/row-elimination rules
/// this implements, each pinned against a live Postgres 18 query.
/// Manual `Debug` — the child is a trait object and cannot be derived through.
/// The child is elided rather than given a placeholder: what feeds a
/// ProjectSet is the child's business, what it expands is this struct's.
impl std::fmt::Debug for ProjectSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectSet")
            .field("srfs", &self.srfs.len())
            .field("input_ncols", &self.input_ncols)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

pub struct ProjectSet {
    child: Box<dyn Operator>,
    srfs: Vec<ResolvedSrf>,
    input_ncols: usize,
    schema: SchemaRef,
}

impl ProjectSet {
    /// `srfs` is `(expression, output name)` pairs, mirroring
    /// [`crate::project::Project`]'s constructor — every expression must be
    /// an [`Expr::SetReturning`] over a supported function (see the module
    /// docs' list); anything else is rejected at construction, not
    /// discovered mid-query. An empty `srfs` list is accepted and makes this
    /// operator a pass-through — `basin_plan::schema::output_schema`'s own
    /// `project_set_appends_srf_columns_after_the_input_schema` test treats
    /// zero SRFs as the "plus zero" sanity check on the same principle.
    pub fn new(child: Box<dyn Operator>, srfs: Vec<(Expr, String)>) -> Result<Self, ExecError> {
        let input_schema = child.schema();
        let input_ncols = input_schema.fields().len();
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut resolved = Vec::with_capacity(srfs.len());
        for (expr, alias) in srfs {
            let (rs, field) = resolve_srf(expr, &alias, &input_schema)?;
            fields.push(field);
            resolved.push(rs);
        }
        Ok(Self {
            child,
            srfs: resolved,
            input_ncols,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

/// Resolve one `(expr, alias)` pair into a [`ResolvedSrf`] and its output
/// [`Field`]. Follows `window.rs`'s `resolve_window`/`aggregate.rs`'s
/// resolution pattern: a zero-row probe batch (mirrors `Project::new`'s own
/// use of the same trick) lets argument types be checked once, at
/// construction, via the same [`eval::eval`] that will run for real later.
fn resolve_srf(
    expr: Expr,
    alias: &str,
    input_schema: &SchemaRef,
) -> Result<(ResolvedSrf, Field), ExecError> {
    let Expr::SetReturning { func, args } = expr else {
        return Err(ExecError::TypeMismatch(format!(
            "{alias}: a ProjectSet target must be a set-returning function expression"
        )));
    };
    let oid = func.0.get();
    let kind = srf_kind_of(oid).ok_or_else(|| {
        ExecError::Internal(format!(
            "set-returning function with OID {oid} is not implemented"
        ))
    })?;

    let probe = RecordBatch::new_empty(Arc::clone(input_schema));
    let data_type = match kind {
        SrfKind::GenerateSeriesI32 => {
            check_generate_series_arity(&args)?;
            for a in &args {
                check_arg_type(a, &probe, &DataType::Int32, "generate_series")?;
            }
            DataType::Int32
        }
        SrfKind::GenerateSeriesI64 => {
            check_generate_series_arity(&args)?;
            for a in &args {
                check_arg_type(a, &probe, &DataType::Int64, "generate_series")?;
            }
            DataType::Int64
        }
        SrfKind::Unnest => {
            if args.len() != 1 {
                return Err(ExecError::TypeMismatch(
                    "unnest() takes exactly one array argument".into(),
                ));
            }
            let arr = eval::eval(&args[0], &probe)?;
            match arr.data_type() {
                DataType::List(field) => field.data_type().clone(),
                other => {
                    return Err(ExecError::TypeMismatch(format!(
                        "unnest() argument must be an array, got {other:?}"
                    )));
                }
            }
        }
    };

    Ok((
        ResolvedSrf { kind, args },
        Field::new(alias, data_type, true),
    ))
}

fn check_generate_series_arity(args: &[Expr]) -> Result<(), ExecError> {
    if args.len() == 2 || args.len() == 3 {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch(format!(
            "generate_series() takes 2 or 3 arguments, got {}",
            args.len()
        )))
    }
}

fn check_arg_type(
    arg: &Expr,
    probe: &RecordBatch,
    expected: &DataType,
    func_name: &str,
) -> Result<(), ExecError> {
    let arr = eval::eval(arg, probe)?;
    if arr.data_type() == expected {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch(format!(
            "{func_name}() argument must be {expected:?}, got {:?}",
            arr.data_type()
        )))
    }
}

/// Per-row generated values for one SRF over one input batch — the physical
/// payload behind a [`ResolvedSrf`] once evaluated against a real batch.
enum SrfRowData {
    /// `generate_series`'s materialized sequence, one `Vec` per input row
    /// (empty where the row's bounds/step were NULL, or the range was
    /// empty).
    I32(Vec<Vec<i32>>),
    I64(Vec<Vec<i64>>),
    /// `unnest`: rather than copying element values out by hand (which would
    /// mean re-deriving a per-`DataType` extraction path — arrays can hold
    /// any element type), this keeps the array's own flattened element
    /// values and each row's `[start, end)` offset into them, and lets
    /// `arrow::compute::take` do the actual (type-generic, null-aware) copy
    /// at output-assembly time — see `ProjectSet::next_batch`.
    Unnest {
        values: ArrayRef,
        offsets: Vec<i32>,
        row_is_null: Vec<bool>,
    },
}

impl SrfRowData {
    /// `c_k(i)` in the module docs' item 2/5 terminology: how many rows this
    /// SRF alone would produce for input row `i`.
    fn counts(&self) -> Vec<usize> {
        match self {
            SrfRowData::I32(rows) => rows.iter().map(Vec::len).collect(),
            SrfRowData::I64(rows) => rows.iter().map(Vec::len).collect(),
            SrfRowData::Unnest {
                offsets,
                row_is_null,
                ..
            } => (0..row_is_null.len())
                .map(|i| {
                    if row_is_null[i] {
                        0
                    } else {
                        (offsets[i + 1] - offsets[i]) as usize
                    }
                })
                .collect(),
        }
    }
}

fn eval_i32_array(expr: &Expr, batch: &RecordBatch, ctx: &str) -> Result<Int32Array, ExecError> {
    let arr = eval::eval(expr, batch)?;
    arr.as_any()
        .downcast_ref::<Int32Array>()
        .cloned()
        .ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "{ctx} argument must evaluate to int4, got {:?} — a planner bug",
                arr.data_type()
            ))
        })
}

fn eval_i64_array(expr: &Expr, batch: &RecordBatch, ctx: &str) -> Result<Int64Array, ExecError> {
    let arr = eval::eval(expr, batch)?;
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "{ctx} argument must evaluate to int8, got {:?} — a planner bug",
                arr.data_type()
            ))
        })
}

/// `generate_series(start, stop[, step])`, materialized as a plain `Vec` —
/// module docs items 1, 6, 7, 8. `None` for any of `start`/`stop`/`step`
/// (item 6: NULL is STRICT, not an error) short-circuits to an empty
/// sequence before `step == 0` is even checked (item 7: `generate_series(
/// NULL,5,0)` is empty, not an error). Intermediate arithmetic runs in `i64`
/// so stepping one increment past an `i32` bound never overflows before the
/// loop condition catches it.
fn gen_series_i32(
    start: Option<i32>,
    stop: Option<i32>,
    step: Option<i32>,
) -> Result<Vec<i32>, ExecError> {
    let (Some(start), Some(stop), Some(step)) = (start, stop, step) else {
        return Ok(Vec::new());
    };
    if step == 0 {
        return Err(ExecError::Internal(
            "generate_series() step size must not be zero".into(),
        ));
    }
    let mut out = Vec::new();
    let mut cur = start as i64;
    let stop = stop as i64;
    let step = step as i64;
    if step > 0 {
        while cur <= stop {
            out.push(cur as i32);
            cur += step;
        }
    } else {
        while cur >= stop {
            out.push(cur as i32);
            cur += step;
        }
    }
    Ok(out)
}

/// `i8`-argument counterpart of [`gen_series_i32`]. Intermediate arithmetic
/// runs in `i128` for the same overflow-safety reason, scaled up because the
/// bound values themselves are already `i64`.
fn gen_series_i64(
    start: Option<i64>,
    stop: Option<i64>,
    step: Option<i64>,
) -> Result<Vec<i64>, ExecError> {
    let (Some(start), Some(stop), Some(step)) = (start, stop, step) else {
        return Ok(Vec::new());
    };
    if step == 0 {
        return Err(ExecError::Internal(
            "generate_series() step size must not be zero".into(),
        ));
    }
    let mut out = Vec::new();
    let mut cur = start as i128;
    let stop = stop as i128;
    let step = step as i128;
    if step > 0 {
        while cur <= stop {
            out.push(cur as i64);
            cur += step;
        }
    } else {
        while cur >= stop {
            out.push(cur as i64);
            cur += step;
        }
    }
    Ok(out)
}

fn evaluate_srf(rs: &ResolvedSrf, batch: &RecordBatch) -> Result<SrfRowData, ExecError> {
    let n = batch.num_rows();
    match rs.kind {
        SrfKind::GenerateSeriesI32 => {
            let start = eval_i32_array(&rs.args[0], batch, "generate_series")?;
            let stop = eval_i32_array(&rs.args[1], batch, "generate_series")?;
            let step = match rs.args.get(2) {
                Some(e) => Some(eval_i32_array(e, batch, "generate_series")?),
                None => None,
            };
            let mut rows = Vec::with_capacity(n);
            for i in 0..n {
                let s = (!start.is_null(i)).then(|| start.value(i));
                let e = (!stop.is_null(i)).then(|| stop.value(i));
                let st = match &step {
                    Some(a) => (!a.is_null(i)).then(|| a.value(i)),
                    None => Some(1),
                };
                rows.push(gen_series_i32(s, e, st)?);
            }
            Ok(SrfRowData::I32(rows))
        }
        SrfKind::GenerateSeriesI64 => {
            let start = eval_i64_array(&rs.args[0], batch, "generate_series")?;
            let stop = eval_i64_array(&rs.args[1], batch, "generate_series")?;
            let step = match rs.args.get(2) {
                Some(e) => Some(eval_i64_array(e, batch, "generate_series")?),
                None => None,
            };
            let mut rows = Vec::with_capacity(n);
            for i in 0..n {
                let s = (!start.is_null(i)).then(|| start.value(i));
                let e = (!stop.is_null(i)).then(|| stop.value(i));
                let st = match &step {
                    Some(a) => (!a.is_null(i)).then(|| a.value(i)),
                    None => Some(1),
                };
                rows.push(gen_series_i64(s, e, st)?);
            }
            Ok(SrfRowData::I64(rows))
        }
        SrfKind::Unnest => {
            let arr = eval::eval(&rs.args[0], batch)?;
            let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "unnest() argument must evaluate to an array, got {:?} — a planner bug",
                    arr.data_type()
                ))
            })?;
            let values = Arc::clone(list.values());
            let offsets = list.value_offsets().to_vec();
            let row_is_null: Vec<bool> = (0..n).map(|i| list.is_null(i)).collect();
            Ok(SrfRowData::Unnest {
                values,
                offsets,
                row_is_null,
            })
        }
    }
}

impl Operator for ProjectSet {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };

        // No SRFs: this node is a pure pass-through (the "plus zero" case
        // the module docs' `ProjectSet::new` doc references) — nothing to
        // expand, nothing to NULL-pad.
        if self.srfs.is_empty() {
            return Ok(Some(batch));
        }

        let n = batch.num_rows();
        let per_srf: Vec<SrfRowData> = self
            .srfs
            .iter()
            .map(|rs| evaluate_srf(rs, &batch))
            .collect::<Result<_, _>>()?;
        let counts: Vec<Vec<usize>> = per_srf.iter().map(SrfRowData::counts).collect();

        // Module docs item 2/5: per input row, the lockstep row count is the
        // MAX across every SRF sharing this target list, not their LCM — and
        // a row with lockstep length 0 (every SRF empty for that row, item
        // 4) contributes nothing at all, which is exactly what an empty
        // `0..lockstep_len[i]` range below does.
        let lockstep_len: Vec<usize> = (0..n)
            .map(|i| counts.iter().map(|c| c[i]).max().unwrap_or(0))
            .collect();
        let total_out: usize = lockstep_len.iter().sum();

        let mut out_to_in: Vec<u32> = Vec::with_capacity(total_out);
        let mut within: Vec<u32> = Vec::with_capacity(total_out);
        for i in 0..n {
            for j in 0..lockstep_len[i] {
                out_to_in.push(i as u32);
                within.push(j as u32);
            }
        }

        // Passthrough columns (module docs item 3): every input column
        // repeated once per output row of its source row — a plain `take`
        // with `out_to_in` as the index array.
        let repeat_idx = UInt32Array::from(out_to_in.clone());
        let mut arrays: Vec<ArrayRef> = (0..self.input_ncols)
            .map(|c| take(batch.column(c).as_ref(), &repeat_idx, None).map_err(arrow_err))
            .collect::<Result<_, _>>()?;

        for (k, data) in per_srf.iter().enumerate() {
            let counts_k = &counts[k];
            let arr: ArrayRef = match data {
                SrfRowData::I32(rows) => {
                    let vals: Vec<Option<i32>> = (0..total_out)
                        .map(|m| {
                            let i = out_to_in[m] as usize;
                            let j = within[m] as usize;
                            (j < counts_k[i]).then(|| rows[i][j])
                        })
                        .collect();
                    Arc::new(Int32Array::from(vals))
                }
                SrfRowData::I64(rows) => {
                    let vals: Vec<Option<i64>> = (0..total_out)
                        .map(|m| {
                            let i = out_to_in[m] as usize;
                            let j = within[m] as usize;
                            (j < counts_k[i]).then(|| rows[i][j])
                        })
                        .collect();
                    Arc::new(Int64Array::from(vals))
                }
                SrfRowData::Unnest {
                    values, offsets, ..
                } => {
                    // NULL-padding (items 2, 5) falls out of `take` for free:
                    // a NULL entry in the index array produces a NULL output
                    // element without this file having to special-case it —
                    // see the module docs on `SrfRowData::Unnest`.
                    let idx: Vec<Option<u32>> = (0..total_out)
                        .map(|m| {
                            let i = out_to_in[m] as usize;
                            let j = within[m] as usize;
                            (j < counts_k[i]).then(|| offsets[i] as u32 + j as u32)
                        })
                        .collect();
                    take(values.as_ref(), &UInt32Array::from(idx), None).map_err(arrow_err)?
                }
            };
            arrays.push(arr);
        }

        let out = RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(arrow_err)?;
        Ok(Some(out))
    }

    fn memory_used(&self) -> usize {
        // See the module docs' "ProjectSet's memory contract" section: this
        // operator never buffers across batches, so it holds nothing beyond
        // whatever its child already reports.
        self.child.memory_used()
    }
}

#[cfg(test)]
mod tests {

    /// A single row with no columns — Postgres's one-row empty relation, which
    /// is what a `FROM`-less `SELECT` runs against.
    ///
    /// NOT `RecordBatch::new_empty`, which has ZERO rows. A set-returning
    /// function expands per input row, so a zero-row input correctly produces
    /// zero output rows — feeding one here would make every `SELECT
    /// generate_series(...)` test silently assert against an empty result.
    fn one_row_no_cols() -> RecordBatch {
        let schema = Arc::new(Schema::empty());
        RecordBatch::try_new_with_options(
            schema,
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .expect("a one-row, zero-column batch is valid")
    }
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::DataType;
    use basin_pgtype::{Oid, PgType};
    use basin_plan::{ColumnRef, Datum, FuncId};
    use std::collections::VecDeque;

    /// The same test double every other file in this crate uses — see
    /// `setop.rs`'s copy for why it is duplicated per-module rather than
    /// shared.
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

    /// A `Feed` that also counts how many times `next_batch` was called past
    /// exhaustion versus in total — used to prove a CTE body is drained
    /// exactly once even when materialized on behalf of several readers.
    struct CountingFeed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
        calls: Rc<RefCell<usize>>,
    }

    impl Operator for CountingFeed {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            *self.calls.borrow_mut() += 1;
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

    fn col_i64(batch: &RecordBatch, i: usize) -> Vec<Option<i64>> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
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
    fn lit_null_i32() -> Expr {
        Expr::Literal(Datum::Null, PgType::INT4)
    }
    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(Datum::Int64(v), PgType::INT8)
    }

    /// `generate_series(start, stop[, step])` int4 — OIDs verified live
    /// against `pg_proc` (see module docs).
    fn gen_series_i32_expr(args: Vec<Expr>) -> Expr {
        let oid = if args.len() == 3 { 1066 } else { 1067 };
        Expr::SetReturning {
            func: FuncId(Oid(oid)),
            args,
        }
    }

    fn gen_series_i64_expr(args: Vec<Expr>) -> Expr {
        let oid = if args.len() == 3 { 1068 } else { 1069 };
        Expr::SetReturning {
            func: FuncId(Oid(oid)),
            args,
        }
    }

    fn unnest_expr(arg: Expr) -> Expr {
        Expr::SetReturning {
            func: FuncId(Oid(2331)),
            args: vec![arg],
        }
    }

    fn drain(mut op: Box<dyn Operator>) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.push(b);
        }
        out
    }

    // ── CteBuffer / CteReader ──────────────────────────────────────────

    // The headline requirement: a CTE referenced twice gets the FULL result
    // both times, not a full result once and an empty stream the second time
    // (the bug a naively-shared, single-cursor "stream" would produce).
    #[test]
    fn cte_referenced_twice_returns_full_results_both_times() {
        let schema = schema_1i32("x");
        let body = Feed::boxed(
            schema,
            vec![batch_i32(
                &schema_1i32("x"),
                vec![Some(1), Some(2), Some(3)],
            )],
        );
        let buffer = CteBuffer::new(body, 1 << 30);

        let reader1 = buffer.reader();
        let reader2 = buffer.reader();

        let out1: Vec<Option<i32>> = drain(Box::new(reader1))
            .iter()
            .flat_map(|b| col_i32(b, 0))
            .collect();
        let out2: Vec<Option<i32>> = drain(Box::new(reader2))
            .iter()
            .flat_map(|b| col_i32(b, 0))
            .collect();

        assert_eq!(out1, vec![Some(1), Some(2), Some(3)]);
        assert_eq!(
            out2, out1,
            "the second reference must see the exact same full result, not zero rows"
        );
    }

    // The body operator must be drained exactly once total, no matter how
    // many readers pull from the buffer it backs — proves "materialize once"
    // is real, not just "each reader happens to see the same values because
    // the body is cheap to re-run".
    #[test]
    fn cte_body_is_drained_exactly_once_across_multiple_readers() {
        let schema = schema_1i32("x");
        let calls = Rc::new(RefCell::new(0usize));
        let body: Box<dyn Operator> = Box::new(CountingFeed {
            schema: schema.clone(),
            batches: vec![
                batch_i32(&schema, vec![Some(1)]),
                batch_i32(&schema, vec![Some(2)]),
            ]
            .into(),
            calls: Rc::clone(&calls),
        });
        let buffer = CteBuffer::new(body, 1 << 30);

        let mut r1 = buffer.reader();
        let mut r2 = buffer.reader();
        // Interleave pulls from both readers to rule out "only the first
        // reader's pulls trigger materialization" accidentally working.
        r1.next_batch().unwrap();
        r2.next_batch().unwrap();
        r1.next_batch().unwrap();
        r1.next_batch().unwrap(); // exhausts r1
        r2.next_batch().unwrap();
        r2.next_batch().unwrap(); // exhausts r2

        // 2 real batches + 1 exhausting `None` call from the body = 3 calls
        // total, regardless of how many readers or how they were
        // interleaved.
        assert_eq!(
            *calls.borrow(),
            3,
            "the body must be pulled to exhaustion exactly once, not once per reader"
        );
    }

    #[test]
    fn cte_empty_body_yields_no_batches_for_every_reader() {
        let schema = schema_1i32("x");
        let body = Feed::boxed(schema, vec![]);
        let buffer = CteBuffer::new(body, 1 << 30);
        let mut r1 = buffer.reader();
        let mut r2 = buffer.reader();
        assert!(r1.next_batch().unwrap().is_none());
        assert!(r2.next_batch().unwrap().is_none());
    }

    // Replay must preserve the body's own batch boundaries, not just its
    // total row content — a reader pulling batch-by-batch across a
    // multi-batch body must see the same shape every time.
    #[test]
    fn cte_multi_batch_body_replays_the_same_batch_boundaries() {
        let schema = schema_1i32("x");
        let b1 = batch_i32(&schema, vec![Some(1), Some(2)]);
        let b2 = batch_i32(&schema, vec![Some(3)]);
        let b3 = batch_i32(&schema, vec![Some(4), Some(5), Some(6)]);
        let body = Feed::boxed(schema, vec![b1, b2, b3]);
        let buffer = CteBuffer::new(body, 1 << 30);

        let mut reader = buffer.reader();
        let sizes: Vec<usize> = std::iter::from_fn(|| reader.next_batch().unwrap())
            .map(|b| b.num_rows())
            .collect();
        assert_eq!(sizes, vec![2, 1, 3], "batch boundaries must survive replay");
    }

    #[test]
    fn cte_memory_used_is_zero_before_materialization() {
        let schema = schema_1i32("x");
        let body = Feed::boxed(schema, vec![batch_i32(&schema_1i32("x"), vec![Some(1)])]);
        let buffer = CteBuffer::new(body, 1 << 30);
        assert_eq!(buffer.memory_used(), 0);
        let reader = buffer.reader();
        assert_eq!(reader.memory_used(), 0, "nothing pulled yet");
    }

    // The memory contract's load-bearing claim: two readers over the same
    // buffer report the IDENTICAL byte count after materialization, not a
    // doubled one — the underlying batches are shared, not duplicated.
    #[test]
    fn cte_memory_used_is_shared_across_readers_not_doubled() {
        let schema = schema_1i32("x");
        let body = Feed::boxed(
            schema.clone(),
            vec![batch_i32(&schema, (0..1000).map(Some).collect())],
        );
        let buffer = CteBuffer::new(body, 1 << 30);
        let mut r1 = buffer.reader();
        let r2 = buffer.reader();

        r1.next_batch().unwrap();
        let bytes_via_r1 = r1.memory_used();
        let bytes_via_r2 = r2.memory_used();
        let bytes_via_buffer = buffer.memory_used();
        assert!(bytes_via_r1 > 0);
        assert_eq!(
            bytes_via_r1, bytes_via_r2,
            "a reader that never pulled must still see the shared total, not zero and not double"
        );
        assert_eq!(bytes_via_r1, bytes_via_buffer);
    }

    #[test]
    fn cte_exceeding_its_budget_is_out_of_memory_for_every_reader() {
        let schema = schema_1i32("x");
        let big = batch_i32(&schema, (0..1000).map(Some).collect());
        let size = big.get_array_memory_size();
        let body = Feed::boxed(schema, vec![big]);
        let buffer = CteBuffer::new(body, size - 1);

        let mut r1 = buffer.reader();
        let err1 = r1.next_batch().unwrap_err();
        assert!(matches!(err1, ExecError::OutOfMemory { .. }), "{err1:?}");

        // A second reader, pulling AFTER the first already failed, must get
        // the same cached failure rather than trying (and being unable) to
        // re-drain the now half-consumed body operator.
        let mut r2 = buffer.reader();
        let err2 = r2.next_batch().unwrap_err();
        assert!(matches!(err2, ExecError::OutOfMemory { .. }), "{err2:?}");
    }

    // A reader created only AFTER materialization already completed (via a
    // different, earlier reader) must still see the FULL result from
    // position zero — proves materialization state, not a first reader's
    // iteration position, is what's shared.
    #[test]
    fn cte_reader_created_after_materialization_still_replays_from_the_start() {
        let schema = schema_1i32("x");
        let body = Feed::boxed(
            schema,
            vec![batch_i32(&schema_1i32("x"), vec![Some(1), Some(2)])],
        );
        let buffer = CteBuffer::new(body, 1 << 30);

        let mut r1 = buffer.reader();
        // Fully drain r1 first, triggering materialization.
        while r1.next_batch().unwrap().is_some() {}

        let mut r2 = buffer.reader();
        let out: Vec<Option<i32>> = std::iter::from_fn(|| r2.next_batch().unwrap())
            .flat_map(|b| col_i32(&b, 0))
            .collect();
        assert_eq!(
            out,
            vec![Some(1), Some(2)],
            "a reader created after materialization must still get the full result"
        );
    }

    #[test]
    fn cte_reports_its_own_schema_from_the_body() {
        let schema = schema_1i32("value");
        let body = Feed::boxed(schema.clone(), vec![]);
        let buffer = CteBuffer::new(body, 1 << 30);
        assert_eq!(buffer.reader().schema(), schema);
    }

    // ── ProjectSet: single SRF ──────────────────────────────────────────

    // Item 1: a lone SRF is unsurprising.
    #[test]
    fn project_set_single_srf_generate_series_yields_n_rows() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), lit_i32(3)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(2), Some(3)]);
        assert!(ps.next_batch().unwrap().is_none());
    }

    // Item 2 — the headline correction: lockstep to the LONGEST SRF's row
    // count, with the shorter one NULL-padded, NOT the LCM with cycling.
    // Verified live: `select generate_series(1,2), generate_series(1,3);`
    // gives 3 rows (max(2,3)), not 6 (lcm(2,3)) and not 2.
    #[test]
    fn project_set_multiple_srfs_run_in_lockstep_to_the_longest_not_the_lcm() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(2)]),
                "a".to_string(),
            ),
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(3)]),
                "b".to_string(),
            ),
        ];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_rows(),
            3,
            "max(2,3) = 3 rows, not lcm(2,3) = 6 and not min(2,3) = 2"
        );
        assert_eq!(
            col_i32(&out, 0),
            vec![Some(1), Some(2), None],
            "the shorter SRF's exhausted row must be NULL, not a cycled repeat of 1"
        );
        assert_eq!(col_i32(&out, 1), vec![Some(1), Some(2), Some(3)]);
    }

    // Item 3: non-SRF columns repeat once per generated row.
    #[test]
    fn project_set_non_srf_columns_repeat_per_generated_row() {
        let schema = Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["x"]))])
                .unwrap();
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), lit_i32(3)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);
        let tag = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| tag.value(i)).collect::<Vec<_>>(),
            vec!["x", "x", "x"]
        );
        assert_eq!(col_i32(&out, 1), vec![Some(1), Some(2), Some(3)]);
    }

    // Item 4: a SOLE SRF yielding zero rows for an input row eliminates that
    // whole row from the output — including for a multi-row input table,
    // not just the constant `SELECT generate_series(1,0)` case.
    #[test]
    fn project_set_a_single_srfs_zero_rows_eliminates_the_input_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("n", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![3, 0, 1])),
            ],
        )
        .unwrap();
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(1, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        // id=1 (n=3) -> 3 rows; id=2 (n=0) -> 0 rows, eliminated entirely;
        // id=3 (n=1) -> 1 row.
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(1), Some(1), Some(3)]);
        assert_eq!(col_i32(&out, 2), vec![Some(1), Some(2), Some(3), Some(1)]);
    }

    // Item 5: the correction to item 4 — with SEVERAL SRFs sharing a row,
    // one being empty NULLs it rather than eliminating the row. Verified
    // live: `select generate_series(1,0), generate_series(1,3);` is 3 rows
    // with the first column NULL throughout, not 0 rows.
    #[test]
    fn project_set_one_srf_empty_among_several_nulls_it_rather_than_eliminating_the_row() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(0)]),
                "empty".to_string(),
            ),
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(3)]),
                "full".to_string(),
            ),
        ];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3, "max(0,3) = 3, the row is not eliminated");
        assert_eq!(
            col_i32(&out, 0),
            vec![None, None, None],
            "the zero-length SRF must be NULL on every lockstep row"
        );
        assert_eq!(col_i32(&out, 1), vec![Some(1), Some(2), Some(3)]);
    }

    // Both SRFs empty for a row: max(0,0) = 0, the row vanishes (not the
    // same as item 5, where only one side was empty).
    #[test]
    fn project_set_all_srfs_empty_for_a_row_eliminates_it() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(0)]),
                "a".to_string(),
            ),
            (
                gen_series_i32_expr(vec![lit_i32(5), lit_i32(3)]),
                "b".to_string(),
            ),
        ];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // Item 6: generate_series is STRICT.
    #[test]
    fn project_set_generate_series_null_argument_yields_zero_rows() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_null_i32(), lit_i32(3)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // Item 7: step == 0 is an error once start/stop/step are all non-NULL.
    #[test]
    fn project_set_generate_series_zero_step_is_an_error() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), lit_i32(5), lit_i32(0)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let err = ps.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::Internal(_)), "{err:?}");
    }

    // Item 7 corollary: a NULL start short-circuits BEFORE the zero-step
    // check, so `generate_series(NULL,5,0)` is empty, not an error.
    #[test]
    fn project_set_generate_series_null_short_circuits_before_the_zero_step_check() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_null_i32(), lit_i32(5), lit_i32(0)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // Item 8: negative step descends.
    #[test]
    fn project_set_generate_series_negative_step_descends() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(5), lit_i32(1), lit_i32(-1)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(
            col_i32(&out, 0),
            vec![Some(5), Some(4), Some(3), Some(2), Some(1)]
        );
    }

    // Item 8 corollary: step direction disagreeing with start/stop is zero
    // rows, not an error.
    #[test]
    fn project_set_generate_series_step_direction_mismatch_yields_zero_rows() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), lit_i32(5), lit_i32(-1)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // The bigint form (OID 1069/1068) — a distinct code path from int4.
    #[test]
    fn project_set_generate_series_bigint_variant() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![one_row_no_cols()]);
        let srfs = vec![(
            gen_series_i64_expr(vec![lit_i64(1), lit_i64(3)]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        assert_eq!(ps.schema().field(0).data_type(), &DataType::Int64);
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i64(&out, 0), vec![Some(1), Some(2), Some(3)]);
    }

    // ── ProjectSet: unnest ──────────────────────────────────────────────

    fn list_schema(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]))
    }

    fn list_batch(schema: &SchemaRef, rows: Vec<Option<Vec<Option<i32>>>>) -> RecordBatch {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(rows);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap()
    }

    // Item 9: unnest explodes elements in order.
    //
    // Previously ignored on a misdiagnosis: the note claimed
    // `ProjectSet::schema()` reported the LIST type where it should report
    // the ELEMENT type. `resolve_srf`'s `Unnest` arm was never the culprit —
    // it always computed the element type correctly (verified by the very
    // next field). Field 0 here is `arr`, the PASSTHROUGH of the input
    // column `ProjectSet::new` always clones ahead of any appended SRF
    // field (see `project_set_schema_appends_srf_columns_after_input_columns`
    // and the mixed generate_series/unnest
    // `project_set_generate_series_and_unnest_together_run_in_lockstep`,
    // both already passing on that ordering) — of course it is the LIST
    // type; that is the passthrough column's real type, unrelated to
    // unnest. `e`, the actual unnest output, is field 1, and reports
    // `Int32` correctly. The test was checking the wrong column.
    #[test]
    fn project_set_unnest_explodes_array_elements_in_order() {
        let schema = list_schema("arr");
        let batch = list_batch(&schema, vec![Some(vec![Some(10), Some(20), Some(30)])]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(unnest_expr(col(0, "arr")), "e".to_string())];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        assert_eq!(ps.schema().field(1).data_type(), &DataType::Int32);
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&out, 1), vec![Some(10), Some(20), Some(30)]);
    }

    // Item 9: NULL elements inside the array survive unnest.
    //
    // Same misdiagnosis as the sibling test above: column 0 of the output
    // is the passthrough `arr` (a `List<Int32>`), not the unnest result —
    // `col_i32` panicked trying to downcast it to `Int32Array`. The unnest
    // output `e` is column 1.
    #[test]
    fn project_set_unnest_preserves_null_elements() {
        let schema = list_schema("arr");
        let batch = list_batch(&schema, vec![Some(vec![Some(1), None, Some(3)])]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(unnest_expr(col(0, "arr")), "e".to_string())];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&out, 1), vec![Some(1), None, Some(3)]);
    }

    // Item 9: unnest(NULL::int[]) is zero rows.
    #[test]
    fn project_set_unnest_of_a_null_array_yields_zero_rows() {
        let schema = list_schema("arr");
        let batch = list_batch(&schema, vec![None]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(unnest_expr(col(0, "arr")), "e".to_string())];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // Item 9: unnest('{}') is also zero rows (distinct from the NULL case).
    #[test]
    fn project_set_unnest_of_an_empty_array_yields_zero_rows() {
        let schema = list_schema("arr");
        let batch = list_batch(&schema, vec![Some(vec![])]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(unnest_expr(col(0, "arr")), "e".to_string())];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // Item 10: unnest and generate_series share the lockstep rule.
    #[test]
    fn project_set_generate_series_and_unnest_together_run_in_lockstep() {
        let schema = list_schema("arr");
        let batch = list_batch(&schema, vec![Some(vec![Some(100), Some(200), Some(300)])]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![
            (
                gen_series_i32_expr(vec![lit_i32(1), lit_i32(2)]),
                "gs".to_string(),
            ),
            (unnest_expr(col(0, "arr")), "e".to_string()),
        ];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(col_i32(&out, 1), vec![Some(1), Some(2), None]);
        assert_eq!(col_i32(&out, 2), vec![Some(100), Some(200), Some(300)]);
    }

    // A non-array argument to unnest is a construction-time type error, not
    // a runtime panic.
    #[test]
    fn project_set_unnest_over_a_non_array_is_a_type_mismatch() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(schema, vec![]);
        let srfs = vec![(unnest_expr(col(0, "x")), "e".to_string())];
        let err = ProjectSet::new(input, srfs).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)), "{err:?}");
    }

    // ── ProjectSet: general operator contract ───────────────────────────

    // An unimplemented (but recognizably a SetReturning node) function is a
    // named Internal error naming the OID — never a silent wrong answer.
    #[test]
    fn project_set_unsupported_function_is_an_internal_error_naming_it() {
        let input = Feed::boxed(Arc::new(Schema::empty()), vec![]);
        let srfs = vec![(
            Expr::SetReturning {
                func: FuncId(Oid(999999)),
                args: vec![],
            },
            "x".to_string(),
        )];
        let err = ProjectSet::new(input, srfs).unwrap_err();
        match err {
            ExecError::Internal(msg) => assert!(msg.contains("999999"), "{msg}"),
            other => panic!("expected Internal naming the OID, got {other:?}"),
        }
    }

    #[test]
    fn project_set_multi_batch_input_is_expanded_batch_by_batch() {
        let schema = schema_1i32("n");
        let b1 = batch_i32(&schema, vec![Some(2)]);
        let b2 = batch_i32(&schema, vec![Some(3)]);
        let input = Feed::boxed(schema, vec![b1, b2]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(0, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let first = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&first, 1), vec![Some(1), Some(2)]);
        let second = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&second, 1), vec![Some(1), Some(2), Some(3)]);
        assert!(ps.next_batch().unwrap().is_none());
    }

    #[test]
    fn project_set_with_an_empty_srf_list_passes_the_batch_through_unchanged() {
        let schema = schema_1i32("x");
        let batch = batch_i32(&schema, vec![Some(1), Some(2)]);
        let input = Feed::boxed(schema.clone(), vec![batch]);
        let mut ps = ProjectSet::new(input, vec![]).unwrap();
        assert_eq!(ps.schema(), schema, "no SRFs means no columns are added");
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(col_i32(&out, 0), vec![Some(1), Some(2)]);
    }

    // Schema ordering: input columns first, SRF columns appended after —
    // matches `basin_plan::schema::output_schema`'s own
    // `project_set_appends_srf_columns_after_the_input_schema` rule.
    #[test]
    fn project_set_schema_appends_srf_columns_after_input_columns() {
        let schema = schema_1i32("a");
        let input = Feed::boxed(schema, vec![]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), lit_i32(3)]),
            "gs".to_string(),
        )];
        let ps = ProjectSet::new(input, srfs).unwrap();
        assert_eq!(ps.schema().fields().len(), 2);
        assert_eq!(ps.schema().field(0).name(), "a");
        assert_eq!(ps.schema().field(1).name(), "gs");
    }

    #[test]
    fn project_set_over_an_empty_input_batch_yields_an_empty_output_batch() {
        let schema = schema_1i32("n");
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        let input = Feed::boxed(Arc::clone(&schema), vec![empty_batch]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(0, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.num_columns(), 2);
    }

    #[test]
    fn project_set_over_a_child_with_no_batches_yields_no_batches() {
        let schema = schema_1i32("n");
        let input = Feed::boxed(schema.clone(), vec![]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(0, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        assert!(ps.next_batch().unwrap().is_none());
    }

    // SRF bounds may reference input columns, not just literals — the shape
    // `SELECT id, generate_series(1, n) FROM t` actually needs.
    #[test]
    fn project_set_srf_bounds_can_reference_input_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![2, 4]))])
                .unwrap();
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(0, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        let out = ps.next_batch().unwrap().unwrap();
        assert_eq!(
            col_i32(&out, 1),
            vec![Some(1), Some(2), Some(1), Some(2), Some(3), Some(4)]
        );
    }

    // Memory contract: ProjectSet holds nothing of its own — mirrors
    // `project.rs`'s `filter_and_project_report_no_retained_memory`.
    #[test]
    fn project_set_reports_no_retained_memory_of_its_own() {
        let schema = schema_1i32("n");
        let batch = batch_i32(&schema, vec![Some(3)]);
        let input = Feed::boxed(schema, vec![batch]);
        let srfs = vec![(
            gen_series_i32_expr(vec![lit_i32(1), col(0, "n")]),
            "gs".to_string(),
        )];
        let mut ps = ProjectSet::new(input, srfs).unwrap();
        ps.next_batch().unwrap();
        assert_eq!(
            ps.memory_used(),
            0,
            "ProjectSet must delegate memory_used to its child, holding nothing extra"
        );
    }

    #[test]
    fn project_set_rejects_a_non_set_returning_target_expression() {
        let schema = schema_1i32("x");
        let input = Feed::boxed(schema, vec![]);
        let err = ProjectSet::new(input, vec![(lit_i32(1), "x".to_string())]).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)), "{err:?}");
    }

    #[test]
    fn project_set_rejects_the_wrong_generate_series_arity() {
        let schema = Arc::new(Schema::empty());
        let input = Feed::boxed(schema, vec![]);
        let srfs = vec![(gen_series_i32_expr(vec![lit_i32(1)]), "gs".to_string())];
        let err = ProjectSet::new(input, srfs).unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)), "{err:?}");
    }
}
