//! Hash aggregate — `GROUP BY` and the ungrouped case (`SELECT count(*) FROM
//! t`, one output row) in a single operator, always `mode=Single`. Basin
//! never builds a `Partial`/`Final` split: `target_partitions=1` disables
//! aggregate repartitioning unconditionally (`executor.rs:10984`, see this
//! crate's `lib.rs` and `docs/migration/df-removal/03-physical-operators.md`
//! §3.2), so a two-phase accumulator would be dead code no query plan ever
//! exercises.
//!
//! # Two engines, one operator
//!
//! - **Ungrouped, no `DISTINCT`** (`SELECT sum(x), avg(x) FROM t` — the
//!   common case): goes straight through arrow's aggregate kernels
//!   (`arrow::compute::kernels::aggregate::{sum_checked, min, max, …}`) per
//!   batch, narrowed by `arrow::compute::kernels::filter::filter` when
//!   `FILTER (WHERE …)` is present, and combined into one running scalar
//!   across batches. This is the path the doc comment above means by "use
//!   arrow kernels for the per-batch work".
//! - **Grouped, or any `DISTINCT` aggregate**: row-wise. There is no public
//!   vectorised `GroupsAccumulator` we can drive from outside DataFusion, and
//!   `DISTINCT` needs per-value dedup that no arrow kernel performs for us
//!   either. `03-physical-operators.md` §3.2 names this split explicitly —
//!   "a two-tier accumulator design (row-wise accumulator + a vectorised
//!   group-wise fast path)" — and this operator implements the row-wise tier
//!   only. The vectorised group-wise tier is future work, not a correctness
//!   gap: every result below is still exact, just not as fast as it could be
//!   on a wide `GROUP BY`.
//!
//! - **Statistical** (`stddev*`/`var*`, `bool_and`/`bool_or`, `bit_and`/
//!   `bit_or`/`bit_xor`, the nine `regr_*` plus `corr`/`covar_pop`/
//!   `covar_samp`): row-wise in *arrival order*, on both paths. These were
//!   the aggregate half of the orphan census in
//!   `docs/migration/df-removal/17-udf-rehosting.md` §3 — names Basin
//!   answered only because DataFusion's builtin registry answered them. The
//!   variance family is not eligible for the arrow-kernel path even when
//!   ungrouped and non-`DISTINCT`, because PostgreSQL's answer comes from a
//!   *sequential* Youngs-Cramer recurrence: reassociating the work into a
//!   kernel changes the last digits. See the section comment above `VarAcc`,
//!   which also records how that arithmetic was recovered from a live server
//!   rather than recalled.
//!
//! # What this operator does not evaluate
//!
//! `eval.rs` (general `basin_plan::Expr` evaluation) is still a stub, so this
//! operator does not take `Expr`. Group keys, aggregate arguments and
//! `FILTER (WHERE …)` predicates are all pre-resolved to column positions in
//! the input schema: [`AggregateSpec`] mirrors `Expr::Aggregate`'s shape
//! (func / args / distinct / filter) but on a resolved column index rather
//! than an expression. Lowering an `Expr::Aggregate` into an `AggregateSpec`
//! is the planner's job once `eval.rs` exists; a `Project` ahead of this
//! operator is expected to have already materialised any non-trivial
//! aggregate argument (`sum(a + b)`) into its own column. `Expr::Aggregate`
//! also carries `order_by` for `array_agg(x ORDER BY y)` / `WITHIN GROUP` —
//! still out of scope: `array_agg`/`string_agg` are implemented below, but
//! only the unordered form. `build.rs`'s `agg_spec` refuses any aggregate
//! with a non-empty `order_by` (`BuildError::Unsupported("ORDER BY inside
//! an aggregate")`) before an [`AggregateSpec`] is ever constructed, so this
//! file never receives — and never has to detect — a silently-reordered
//! `array_agg`.
//!
//! # Memory
//!
//! `memory_used()` tracks the hash table (group keys) plus the accumulator
//! state (running sums/counts/extremes, and any `DISTINCT` seen-sets) — never
//! the input, which is consumed batch-by-batch and dropped. Growth is
//! checked at every insertion into the hash table or a `DISTINCT` set, so a
//! budget violation is caught at the point it happens rather than after
//! building an oversized table (see [`HashAggregate::bump_memory`]).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::operator::{ExecError, Operator};

/// Which member of the variance family an [`AggFunc::Variance`] accumulator
/// finalizes to. All four (six, counting the `stddev`/`variance` aliases)
/// share one Youngs-Cramer accumulator and differ only in [`AccState::finalize`]
/// — see [`VarAcc`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarKind {
    /// `var_pop(x)` — `Sxx / N`. NULL over zero non-NULL rows, `0` over one.
    VarPop,
    /// `var_samp(x)`, and its Postgres alias `variance(x)` — `Sxx / (N - 1)`.
    /// NULL over fewer than two non-NULL rows.
    VarSamp,
    /// `stddev_pop(x)` — `sqrt(Sxx / N)`.
    StddevPop,
    /// `stddev_samp(x)`, and its Postgres alias `stddev(x)` —
    /// `sqrt(Sxx / (N - 1))`.
    StddevSamp,
}

/// Which member of the two-argument statistical family an [`AggFunc::Regr`]
/// accumulator finalizes to. All twelve share one [`RegrAcc`] — Postgres
/// gives all twelve the same `float8_regr_accum` transition function
/// (confirmed against the live server's `pg_aggregate`) and differ only in
/// the final function.
///
/// Postgres's argument order is `f(Y, X)` — the *dependent* variable first.
/// [`AggregateSpec::input_col`] is therefore `Y` and `AggFunc::Regr::x_col`
/// is `X`, not the other way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegrKind {
    /// `regr_count(y, x)` — the count of rows where *both* are non-NULL.
    /// The one member of this family that is `bigint`, and the one that is
    /// never NULL: over zero rows it is `0` (verified live), because its
    /// aggregate has a non-NULL `agginitval`.
    Count,
    /// `regr_avgx(y, x)` — `Sx / N`.
    AvgX,
    /// `regr_avgy(y, x)` — `Sy / N`.
    AvgY,
    /// `regr_sxx(y, x)` — `sum((x - avg(x))^2)`.
    Sxx,
    /// `regr_syy(y, x)` — `sum((y - avg(y))^2)`.
    Syy,
    /// `regr_sxy(y, x)` — `sum((x - avg(x)) * (y - avg(y)))`.
    Sxy,
    /// `regr_slope(y, x)` — `Sxy / Sxx`.
    Slope,
    /// `regr_intercept(y, x)` — `(Sy - Sx * Sxy / Sxx) / N`.
    Intercept,
    /// `regr_r2(y, x)` — `Sxy^2 / (Sxx * Syy)`, or exactly `1.0` when
    /// `Syy = 0`.
    R2,
    /// `corr(y, x)` — `Sxy / sqrt(Sxx * Syy)`.
    Corr,
    /// `covar_pop(y, x)` — `Sxy / N`.
    CovarPop,
    /// `covar_samp(y, x)` — `Sxy / (N - 1)`.
    CovarSamp,
}

/// The aggregate functions this operator implements.
///
/// This is deliberately not the full `pg_proc` aggregate surface — no
/// ordered-set aggregates (`percentile_cont`/`percentile_disc`/`mode`, which
/// need `WITHIN GROUP` machinery this operator has no representation for) or
/// `GROUPING SETS` — see
/// `docs/migration/df-removal/03-physical-operators.md` §3.2 for what else
/// basin-exec eventually needs; those are separate operators/specs.
/// `array_agg`/`string_agg` were the last of the "ordinary" aggregates
/// missing; both are row-wise only (see the module doc) — a vectorised
/// group-wise variant was prototyped on `spike/vectorised-aggregate` and
/// measured at 0.60x the row-wise loop, so it was shelved rather than
/// merged (`docs/migration/df-removal/17-udf-rehosting.md`).
///
/// # Extension shape
///
/// Adding a function is four edits, all in this file: a variant here, an arm
/// in [`resolve_aggregate`] (which fixes the output `Field`), a variant in
/// [`AccState`] plus arms in `new`/`update_scalar`/`update_kernel`/`finalize`,
/// and — only if the function needs a *second* per-row column — a payload
/// field on the variant itself (`StringAgg { delim_col }`,
/// `Regr { x_col, .. }`) rather than a new field on [`AggregateSpec`], so
/// that every other aggregate's spec stays the shape it already is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    /// `count(*)`. Counts rows, including all-null ones — never inspects a
    /// column.
    CountStar,
    /// `count(expr)`. Counts non-null values of `expr` only.
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// `array_agg(expr)`. Unlike every aggregate above, NULLs are *included*
    /// in the result rather than skipped — see [`AccState::push_array_agg`].
    /// `ORDER BY` inside the call (`array_agg(x ORDER BY y)`) is real syntax
    /// but is refused upstream, in `build.rs`'s `agg_spec`, for *every*
    /// aggregate unconditionally (`if !order_by.is_empty() { return
    /// Err(BuildError::Unsupported(...)) }`) before an `AggregateSpec` is
    /// ever constructed — so it can never reach this file silently
    /// reordered. This variant carries no ordering state because none can
    /// arrive.
    ArrayAgg,
    /// `string_agg(expr, delimiter)`. NULLs are skipped, like `sum`/`avg`.
    /// The delimiter is a second resolved column position — evaluated per
    /// row, not a single constant — carried here rather than as a second
    /// field on [`AggregateSpec`] (whose single `input_col` already covers
    /// every other aggregate's one argument). Same `ORDER BY` refusal note
    /// as `ArrayAgg` applies.
    StringAgg {
        delim_col: usize,
    },
    /// `var_pop` / `var_samp` / `variance` / `stddev_pop` / `stddev_samp` /
    /// `stddev`. NULLs are skipped like `sum`/`avg`; see [`VarKind`] for what
    /// each spelling finalizes to and [`VarAcc`] for the accumulator.
    Variance(VarKind),
    /// `bool_and(b)` — `true` iff every non-NULL input is true. NULL over
    /// zero non-NULL rows (verified live), never `true`.
    BoolAnd,
    /// `bool_or(b)` — `true` iff any non-NULL input is true. NULL over zero
    /// non-NULL rows (verified live), never `false`.
    BoolOr,
    /// `bit_and(i)` over `smallint`/`integer`/`bigint`. Preserves the input's
    /// exact integer width (verified live: `pg_typeof(bit_and(x::int2))` is
    /// `smallint`). NULL over zero non-NULL rows.
    BitAnd,
    /// `bit_or(i)` — see [`AggFunc::BitAnd`].
    BitOr,
    /// `bit_xor(i)` — see [`AggFunc::BitAnd`].
    BitXor,
    /// The `regr_*` / `corr` / `covar_*` family. `x_col` is the *second*
    /// SQL argument's resolved column; [`AggregateSpec::input_col`] is the
    /// first (`Y`). Same second-column-on-the-variant convention as
    /// [`AggFunc::StringAgg`], and for the same reason.
    ///
    /// A row is skipped unless *both* columns are non-NULL — not just the
    /// first — verified live: `regr_count(y, x)` over
    /// `(1,10),(2,NULL),(NULL,30),(4,25)` is `2`, not `3` or `4`.
    Regr {
        kind: RegrKind,
        x_col: usize,
    },
}

/// Aggregates that can never take the ungrouped arrow-kernel fast path in
/// [`HashAggregate::update_ungrouped_one`], because they need something a
/// single filtered `ArrayRef` cannot express: `array_agg` needs each row's
/// NULL kept as an element, and `string_agg`/`regr_*` need a *second*
/// per-row column read in lockstep with the first.
fn is_row_wise_only(func: AggFunc) -> bool {
    matches!(
        func,
        AggFunc::ArrayAgg | AggFunc::StringAgg { .. } | AggFunc::Regr { .. }
    )
}

/// The second per-row column an aggregate reads, if it has one. Carried on
/// the [`AggFunc`] variant rather than on [`AggregateSpec`] — see
/// [`AggFunc::StringAgg`].
fn second_arg_col(func: AggFunc) -> Option<usize> {
    match func {
        AggFunc::StringAgg { delim_col } => Some(delim_col),
        AggFunc::Regr { x_col, .. } => Some(x_col),
        _ => None,
    }
}

/// One aggregate in the SELECT list, already resolved to a column position.
///
/// This is the physical-layer analogue of `basin_plan::Expr::Aggregate`
/// (func/args/distinct/filter) — see the module doc for why it carries a
/// column index rather than an `Expr`.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub func: AggFunc,
    /// Column in the input batch this aggregate reads. Must be `None` for
    /// `CountStar` (it counts rows, not values) and `Some` for every other
    /// function — enforced in [`HashAggregate::new`]. For `StringAgg`, this
    /// is the value expression; the delimiter lives on `func` itself
    /// (`AggFunc::StringAgg::delim_col`), not here — see that variant's doc.
    pub input_col: Option<usize>,
    /// `count(DISTINCT x)`, `sum(DISTINCT x)`, `array_agg(DISTINCT x)`,
    /// `string_agg(DISTINCT x, ',')`, … — Postgres allows `DISTINCT` on any
    /// of these, not just `count`. See [`AccState::push_array_agg`] for why
    /// `array_agg(DISTINCT x)` needs a different NULL rule than the rest.
    pub distinct: bool,
    /// Column index of a boolean predicate already evaluated for
    /// `FILTER (WHERE …)`. Applies to this aggregate only, not to the row's
    /// membership in its group: a row that fails the filter is still part of
    /// its `GROUP BY` group, it just does not update *this* accumulator.
    pub filter_col: Option<usize>,
    /// Output column name, carried through to `RowDescription`.
    pub alias: String,
}

/// Whether an aggregate's input column is the integer or the floating
/// family. Determines the accumulator's native representation and the
/// widened output type for `SUM`/`AVG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumKind {
    Int,
    Float,
}

fn num_kind_of(dt: &DataType) -> Option<NumKind> {
    match dt {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => Some(NumKind::Int),
        DataType::Float32 | DataType::Float64 => Some(NumKind::Float),
        _ => None,
    }
}

/// An `AggregateSpec` after validation against the input schema, plus the
/// facts needed at execution time so we never have to re-derive them per row.
struct ResolvedAgg {
    spec: AggregateSpec,
    /// Meaningful for `Sum`/`Avg` only (selects the accumulator's native
    /// type); a harmless default (`Int`) otherwise.
    num_kind: NumKind,
}

fn require_input_col(spec: &AggregateSpec) -> Result<usize, ExecError> {
    spec.input_col
        .ok_or_else(|| ExecError::TypeMismatch(format!("{:?} requires an input column", spec.func)))
}

/// Validate one `AggregateSpec` against the input schema and compute its
/// output `Field`. Reaching a `TypeMismatch` here is a planner bug — see
/// `ExecError::TypeMismatch`'s doc comment — because the planner is supposed
/// to have already checked, e.g., that `sum()`'s argument is numeric.
fn resolve_aggregate(
    spec: AggregateSpec,
    input_schema: &Schema,
) -> Result<(ResolvedAgg, Field), ExecError> {
    match spec.func {
        AggFunc::CountStar => {
            if spec.input_col.is_some() {
                return Err(ExecError::TypeMismatch(
                    "count(*) must not carry an input column".into(),
                ));
            }
            let field = Field::new(&spec.alias, DataType::Int64, false);
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Int,
                },
                field,
            ))
        }
        AggFunc::Count => {
            require_input_col(&spec)?;
            let field = Field::new(&spec.alias, DataType::Int64, false);
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Int,
                },
                field,
            ))
        }
        AggFunc::Sum => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type();
            let num_kind = num_kind_of(dt).ok_or_else(|| {
                ExecError::TypeMismatch(format!("sum() over non-numeric column {dt:?}"))
            })?;
            // Postgres widens sum(int2)/sum(int4) to bigint and sum(int8) to
            // numeric (unbounded). We widen everything integer to i64 and
            // raise `ExecError::Overflow` past that — see item 5 in this
            // file's tests. sum(float4)/sum(float8) both widen to float8.
            let out_ty = match num_kind {
                NumKind::Int => DataType::Int64,
                NumKind::Float => DataType::Float64,
            };
            // Nullable: SUM over zero rows (or all-NULL input) is NULL, not
            // 0 — see item 1 in this file's tests.
            let field = Field::new(&spec.alias, out_ty, true);
            Ok((ResolvedAgg { spec, num_kind }, field))
        }
        AggFunc::Avg => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type();
            let num_kind = num_kind_of(dt).ok_or_else(|| {
                ExecError::TypeMismatch(format!("avg() over non-numeric column {dt:?}"))
            })?;
            // FIDELITY GAP, documented rather than hidden: Postgres
            // avg(smallint|integer|bigint) returns NUMERIC and avg(numeric)
            // stays NUMERIC — arbitrary precision, no rounding. basin-exec
            // has no arbitrary-precision decimal type yet, so this returns
            // DOUBLE PRECISION for every input type. Values agree for
            // anything that survives an f64 round-trip; they can diverge in
            // the low digits for large bigint sums or high-precision numeric
            // input. See item 4 in this file's tests.
            let field = Field::new(&spec.alias, DataType::Float64, true);
            Ok((ResolvedAgg { spec, num_kind }, field))
        }
        AggFunc::Min | AggFunc::Max => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            let supported = num_kind_of(&dt).is_some()
                || matches!(dt, DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean);
            if !supported {
                return Err(ExecError::TypeMismatch(format!(
                    "min()/max() over unsupported column type {dt:?}"
                )));
            }
            let num_kind = num_kind_of(&dt).unwrap_or(NumKind::Int);
            // min/max preserve the input's exact type — unlike sum/avg they
            // never widen.
            let field = Field::new(&spec.alias, dt, true);
            Ok((ResolvedAgg { spec, num_kind }, field))
        }
        AggFunc::ArrayAgg => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            let supported = num_kind_of(&dt).is_some()
                || matches!(dt, DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean);
            if !supported {
                return Err(ExecError::TypeMismatch(format!(
                    "array_agg() over unsupported column type {dt:?}"
                )));
            }
            let num_kind = num_kind_of(&dt).unwrap_or(NumKind::Int);
            // array_agg preserves the input's exact element type inside the
            // list, the same as min/max — no widening. Verified against a
            // live PG 18: `select pg_typeof(array_agg(id))` over an int4
            // column returns `integer[]`, not `bigint[]`.
            //
            // The item field is nullable: array_agg is the one aggregate in
            // this file that puts NULLs *into* its result rather than
            // skipping them — `select array_agg(x) from (values
            // (1),(NULL),(3)) t(x)` on live PG 18 returns `{1,NULL,3}`, a
            // 3-element array, not `{1,3}`.
            let item_field = Arc::new(Field::new("item", dt, true));
            // The list itself is nullable too, but for a different reason:
            // zero input rows (or, under FILTER, zero passing rows) is NULL,
            // not `{}` — verified: `select array_agg(id) from t where
            // false` on live PG 18 returns a blank (NULL), not `{}`. See
            // [`AccState::push_array_agg`] and the empty-input tests below.
            let field = Field::new(&spec.alias, DataType::List(item_field), true);
            Ok((ResolvedAgg { spec, num_kind }, field))
        }
        AggFunc::StringAgg { delim_col } => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            if !matches!(dt, DataType::Utf8 | DataType::LargeUtf8) {
                return Err(ExecError::TypeMismatch(format!(
                    "string_agg() over non-text column {dt:?}"
                )));
            }
            let delim_dt = input_schema.field(delim_col).data_type().clone();
            if !matches!(delim_dt, DataType::Utf8 | DataType::LargeUtf8) {
                return Err(ExecError::TypeMismatch(format!(
                    "string_agg() delimiter must be text, got {delim_dt:?}"
                )));
            }
            // Nullable: zero rows, or every value NULL, is NULL — not an
            // empty string. Verified against live PG 18: `select
            // string_agg(name, ',') from t where false` and `select
            // string_agg(name, ',') from t where name is null` both return a
            // blank (NULL), not `''`.
            let field = Field::new(&spec.alias, DataType::Utf8, true);
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Int, // meaningless for StringAgg; see the field's own doc
                },
                field,
            ))
        }
        AggFunc::Variance(_) => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type();
            let num_kind = num_kind_of(dt).ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "stddev()/variance() over non-numeric column {dt:?}"
                ))
            })?;
            // FIDELITY GAP, the same one `Avg` documents and measured the
            // same way. Postgres routes `stddev(int2|int4|int8|numeric)`
            // through `numeric_accum`/`int*_accum` — *exact* arbitrary-
            // precision accumulation — and only `stddev(float4|float8)`
            // through the f64 `float8_accum` this file reproduces. basin-exec
            // has no arbitrary-precision decimal type, so integer input is
            // widened to f64 and answered by the float path. The divergence
            // is real and was measured rather than assumed: on the live
            // server `stddev(x::float8)` over `{1,2,4}` is
            // 1.5275252316519468 and `stddev(x::int)` over the same values is
            // 1.5275252316519467 — one ULP apart, and a different declared
            // type (`double precision` vs `numeric`).
            let field = Field::new(&spec.alias, DataType::Float64, true);
            Ok((ResolvedAgg { spec, num_kind }, field))
        }
        AggFunc::BoolAnd | AggFunc::BoolOr => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type();
            if !matches!(dt, DataType::Boolean) {
                return Err(ExecError::TypeMismatch(format!(
                    "bool_and()/bool_or() over non-boolean column {dt:?}"
                )));
            }
            // Nullable: zero non-NULL rows is NULL, not the operator's
            // identity element — verified live, `bool_and` over an empty
            // input is a blank, not `t`.
            let field = Field::new(&spec.alias, DataType::Boolean, true);
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Int,
                },
                field,
            ))
        }
        AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor => {
            let col = require_input_col(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            if !matches!(dt, DataType::Int16 | DataType::Int32 | DataType::Int64) {
                return Err(ExecError::TypeMismatch(format!(
                    "bit_and()/bit_or()/bit_xor() over non-integer column {dt:?}"
                )));
            }
            // Preserves the input's exact width, like min/max and unlike
            // sum — verified live: `pg_typeof(bit_and(x))` is `smallint`,
            // `integer` and `bigint` for int2/int4/int8 input respectively.
            // Postgres also defines these over `bit` strings; Basin has no
            // bit-string type, so that overload is out of scope rather than
            // silently answered with an integer.
            let field = Field::new(&spec.alias, dt, true);
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Int,
                },
                field,
            ))
        }
        AggFunc::Regr { kind, x_col } => {
            let y_col = require_input_col(&spec)?;
            for (which, c) in [("first", y_col), ("second", x_col)] {
                let dt = input_schema.field(c).data_type();
                if num_kind_of(dt).is_none() {
                    return Err(ExecError::TypeMismatch(format!(
                        "regr_*()/corr()/covar_*() {which} argument is non-numeric column {dt:?}"
                    )));
                }
            }
            // `regr_count` is `bigint` and never NULL (zero rows is 0);
            // every other member is `double precision` and nullable. Both
            // read off the live server's `pg_typeof`.
            let field = match kind {
                RegrKind::Count => Field::new(&spec.alias, DataType::Int64, false),
                _ => Field::new(&spec.alias, DataType::Float64, true),
            };
            Ok((
                ResolvedAgg {
                    spec,
                    num_kind: NumKind::Float,
                },
                field,
            ))
        }
    }
}

/// A scalar pulled out of an arrow array, widened to one of four families.
/// Integers are always widened to `i64` and floats to `f64` regardless of
/// the source column's exact width (`Int16`/`Int32`/`Int64` all become
/// `Int64`) — the output-building step in [`build_typed_array`] narrows back
/// down using the *output* field's declared type, which for `MIN`/`MAX`/group
/// keys is the original column's exact type.
///
/// `List` is not a *per-row* cell — no input column is ever a list here —
/// it is `array_agg`'s own *finalized* accumulator value, reusing this enum
/// only so [`AccState::finalize`] and [`build_typed_array`] have one output
/// shape to handle instead of two. Each element is itself a per-row cell
/// (`Option` because array_agg includes NULLs).
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum CellValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    List(Vec<Option<CellValue>>),
}

/// A hashable, `Eq` form of [`CellValue`] (plus `Null`) used for `GROUP BY`
/// keys and `DISTINCT` seen-sets. Floats need special handling to be usable
/// as a hash key at all:
///
/// - NaN is canonicalised to one bit pattern. Two `sum(x)/0.0`-style NaNs
///   with different bit patterns must still group/dedup together — Postgres
///   treats all NaNs as equal for grouping and `DISTINCT`, only `=` treats
///   them as unordered/not-equal.
/// - `-0.0` is canonicalised to `0.0`, matching numeric equality (`-0.0 =
///   0.0` is true in Postgres, so they must land in the same group).
#[derive(Clone, PartialEq, Eq, Hash)]
enum HashKey {
    Null,
    Int64(i64),
    Float64Bits(u64),
    Utf8(String),
    Bool(bool),
    /// A two-column key, used only by `DISTINCT` on a two-argument aggregate
    /// (`regr_slope(DISTINCT y, x)`). Postgres applies `DISTINCT` to the
    /// whole argument *list*, not to the first argument — verified live:
    /// `regr_count(DISTINCT y, x)` over `(1,10),(1,10),(2,20)` is `2` and
    /// `regr_avgx(DISTINCT y, x)` is `1.5`, so the duplicate *pair*
    /// collapses. Boxed to keep `HashKey` the size it already was; group
    /// keys and single-column `DISTINCT` sets never allocate one.
    Pair(Box<(HashKey, HashKey)>),
}

impl From<&Option<CellValue>> for HashKey {
    fn from(v: &Option<CellValue>) -> Self {
        match v {
            None => HashKey::Null,
            Some(CellValue::Int64(i)) => HashKey::Int64(*i),
            Some(CellValue::Float64(f)) => {
                let canon = if f.is_nan() {
                    f64::NAN
                } else if *f == 0.0 {
                    0.0
                } else {
                    *f
                };
                HashKey::Float64Bits(canon.to_bits())
            }
            Some(CellValue::Utf8(s)) => HashKey::Utf8(s.clone()),
            Some(CellValue::Bool(b)) => HashKey::Bool(*b),
            Some(CellValue::List(_)) => unreachable!(
                "HashKey::from only ever sees a per-row cell (GROUP BY key or \
                 DISTINCT dedup value) — array_agg's own accumulated List is a \
                 finalized *output*, never fed back through hashing"
            ),
        }
    }
}

/// Rough heap-byte cost of one `HashKey`, for the memory accountant. Not
/// byte-exact (HashMap/HashSet bucket overhead isn't modelled) but honest in
/// the sense that matters: it grows with the data, not with a constant.
fn hash_key_bytes(k: &HashKey) -> usize {
    std::mem::size_of::<HashKey>()
        + match k {
            HashKey::Utf8(s) => s.len(),
            HashKey::Pair(p) => hash_key_bytes(&p.0) + hash_key_bytes(&p.1),
            _ => 0,
        }
}

/// PostgreSQL's ascending order for a `DISTINCT` aggregate's input.
///
/// `DISTINCT` inside an aggregate is implemented in PostgreSQL by *sorting*
/// the input and feeding the transition function the deduplicated run, so for
/// the two aggregates here whose output order is observable the result comes
/// back sorted rather than in arrival order. Measured on a live PG 18.2:
///
/// ```text
/// select array_agg(distinct x) from (values (3),(1),(3),(null),(2)) v(x);
///   -> {1,2,3,NULL}          -- sorted, NULL last; NOT {3,1,NULL,2}
/// select string_agg(distinct s, ',')
///   from (values ('c'),('a'),('c'),(null),('b')) v(s);
///   -> a,b,c                 -- NOT c,a,b
/// select array_agg(distinct x)
///   from (values ('NaN'::float8),(1.0),(null),('Infinity'::float8),(-1.0)) v(x);
///   -> {-1,1,Infinity,NaN,NULL}
/// ```
///
/// Two rules the last one pins, both of which a plain `partial_cmp` would get
/// wrong: **NULLs sort last**, and **`NaN` sorts after every number**
/// including `Infinity` — that is `float8`'s btree ordering, not IEEE's,
/// under which every NaN comparison is `false` and `partial_cmp` returns
/// `None`.
///
/// Text ordering is byte order here, as it is everywhere else in this crate
/// (`update_extreme`, and `sort.rs`); PostgreSQL uses the database collation
/// (`en_US.UTF-8` on the reference server). The two agree on ASCII of one
/// case and can disagree on mixed case or non-ASCII — a pre-existing,
/// engine-wide collation gap, not one this function introduces.
fn distinct_sort_cmp(a: &Option<CellValue>, b: &Option<CellValue>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(CellValue::Float64(x)), Some(CellValue::Float64(y))) => {
            match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => x
                    .partial_cmp(y)
                    .expect("neither operand is NaN, so float8 compares totally"),
            }
        }
        (Some(x), Some(y)) => x.partial_cmp(y).expect(
            "one aggregate reads one column, so every cell shares a CellValue variant, and \
             the only partial variant (Float64) is handled above",
        ),
    }
}

fn cell_heap_bytes(v: &Option<CellValue>) -> usize {
    match v {
        Some(CellValue::Utf8(s)) => s.len(),
        _ => 0,
    }
}

/// Extract row `row` of `array` as a [`CellValue`], or `None` for SQL NULL.
fn extract_cell(array: &ArrayRef, row: usize) -> Result<Option<CellValue>, ExecError> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Int16 => CellValue::Int64(array.as_primitive::<Int16Type>().value(row) as i64),
        DataType::Int32 => CellValue::Int64(array.as_primitive::<Int32Type>().value(row) as i64),
        DataType::Int64 => CellValue::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::Float32 => {
            CellValue::Float64(array.as_primitive::<Float32Type>().value(row) as f64)
        }
        DataType::Float64 => CellValue::Float64(array.as_primitive::<Float64Type>().value(row)),
        DataType::Boolean => CellValue::Bool(array.as_boolean().value(row)),
        DataType::Utf8 => CellValue::Utf8(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => CellValue::Utf8(array.as_string::<i64>().value(row).to_string()),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "hash aggregate: unsupported column type {other:?} (planner should have rejected this)"
            )));
        }
    }))
}

/// Widen an already-non-NULL numeric cell to `f64` for the statistical
/// accumulators. Reaching the error arm is a planner bug — `resolve_aggregate`
/// has already rejected a non-numeric input column.
fn numeric_f64(v: &CellValue, who: &str) -> Result<f64, ExecError> {
    match v {
        CellValue::Int64(i) => Ok(*i as f64),
        CellValue::Float64(f) => Ok(*f),
        _ => Err(ExecError::TypeMismatch(format!(
            "{who}(): non-numeric value reached the accumulator"
        ))),
    }
}

fn update_extreme(cur: &mut Option<CellValue>, v: &CellValue, want_min: bool) {
    let better = match cur {
        None => true,
        Some(c) => {
            let ord = v
                .partial_cmp(c)
                .expect("min/max accumulate values from one column, so the variant always matches");
            if want_min {
                ord == Ordering::Less
            } else {
                ord == Ordering::Greater
            }
        }
    };
    if better {
        *cur = Some(v.clone());
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The statistical accumulators
//
// # Why these are not a two-pass or a Welford loop
//
// PostgreSQL's `float8_accum`/`float8_regr_accum` use the **Youngs-Cramer**
// algorithm (`src/backend/utils/adt/float.c`), not `sum(x)`/`sum(x^2)`/`n`
// and not textbook Welford. Reproducing its *answers* means reproducing its
// *arithmetic*, operation for operation and in row order — a mathematically
// equivalent rearrangement disagrees in the last digits.
//
// This was not taken on faith. `float8_accum` and `float8_regr_accum` are
// ordinary SQL-callable functions, so the transition state was read straight
// out of the live server, one row at a time, and the Rust below was fitted to
// it:
//
// ```sql
// with recursive s(i, st) as (
//   select 0, '{0,0,0}'::float8[]
//   union all
//   select s.i+1, float8_accum(s.st, d.x) from s join d on d.rn = s.i+1)
// select i, st::text from s order by i;
// ```
//
// Two things that trace settled, both of which a from-memory implementation
// gets wrong:
//
// 1. **`float8_accum` divides where `float8_regr_accum` multiplies by a
//    reciprocal.** `Sxx += tmp*tmp/(N*Nold)` versus
//    `scale = 1.0/(N*Nold); Sxx += tmp*tmp*scale`. Those are different f64
//    expressions and they produce different last bits. Both spellings are
//    reproduced verbatim below.
// 2. **`tmp = newval * N - Sx` is compiled as a fused multiply-add** by the
//    server's build (PostgreSQL 18.2, Homebrew, Apple clang 17, aarch64 —
//    `-ffp-contract=on` is clang's default), and `Sxx += tmp*tmp*scale` in
//    the regr accumulator is fused too. `f64::mul_add` below is what
//    reproduces that. Without it the answers drift: measured over 20 random
//    53-row datasets × 6 statistics, the non-fused spelling matched the
//    server exactly 50/120 times with a worst case of 18 ULP, while the
//    spelling below matched **300/300** across 20 datasets × 15 statistics.
//
// **The honest caveat that measurement also exposes**: FP contraction is a
// property of *the server's build*, not of PostgreSQL. A build compiled
// without it (a generic x86-64 Linux package, say) would produce the
// non-fused answers, which differ from these by ≤ 2 ULP. Rust's `mul_add` is
// correctly rounded and deterministic on every target, so Basin's answer is
// stable where PostgreSQL's is not. The residual disagreement against a
// non-contracting build is ≤ 2 ULP — two orders of magnitude smaller than the
// `avg`-returns-`float8`-instead-of-`numeric` gap this file already documents.
// ─────────────────────────────────────────────────────────────────────────

/// Youngs-Cramer state for the one-argument variance family, mirroring
/// PostgreSQL's `float8_accum` three-element transition array `{N, Sx, Sxx}`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct VarAcc {
    n: f64,
    sx: f64,
    sxx: f64,
}

impl VarAcc {
    /// One row, in row order. Transcribed from `float8_accum` — including
    /// the Inf/NaN rule, which is not decoration: `stddev` over a column
    /// containing `Infinity` is `NaN` on the live server, and an
    /// implementation that lets `Sxx` go infinite reports `Infinity` instead.
    /// The first-row branch matters for the same reason — a lone `Infinity`
    /// input must poison `Sxx` immediately or `stddev_pop` falsely reports
    /// `0` (verified live: it is `NaN`).
    fn push(&mut self, x: f64) {
        let n_old = self.n;
        self.n += 1.0;
        self.sx += x;
        if n_old > 0.0 {
            // `newval * N - Sx`, fused — see the section comment above.
            let tmp = x.mul_add(self.n, -self.sx);
            // Division, not multiplication by a reciprocal. `float8_accum`
            // and `float8_regr_accum` genuinely differ here.
            self.sxx += tmp * tmp / (self.n * n_old);
            if self.sx.is_infinite() || self.sxx.is_infinite() {
                self.sxx = f64::NAN;
            }
        } else if !x.is_finite() {
            self.sxx = f64::NAN;
        }
    }

    /// The finalizer for one member of the family, or `None` for SQL NULL.
    /// The N thresholds differ between the population and sample forms and
    /// were each confirmed live: over exactly one row `var_pop`/`stddev_pop`
    /// are `0` while `var_samp`/`stddev_samp` are NULL; over zero rows all
    /// four are NULL.
    fn finalize(&self, kind: VarKind) -> Option<f64> {
        match kind {
            VarKind::VarPop | VarKind::StddevPop => {
                if self.n == 0.0 {
                    return None;
                }
                let v = self.sxx / self.n;
                Some(if matches!(kind, VarKind::StddevPop) {
                    v.sqrt()
                } else {
                    v
                })
            }
            VarKind::VarSamp | VarKind::StddevSamp => {
                if self.n <= 1.0 {
                    return None;
                }
                let v = self.sxx / (self.n - 1.0);
                Some(if matches!(kind, VarKind::StddevSamp) {
                    v.sqrt()
                } else {
                    v
                })
            }
        }
    }
}

/// Youngs-Cramer state for the two-argument family, mirroring PostgreSQL's
/// `float8_regr_accum` six-element transition array
/// `{N, Sx, Sxx, Sy, Syy, Sxy}`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct RegrAcc {
    n: f64,
    sx: f64,
    sxx: f64,
    sy: f64,
    syy: f64,
    sxy: f64,
}

impl RegrAcc {
    /// One row, in row order, with `y` the *first* SQL argument. Only called
    /// when both are non-NULL — the skip-if-either-is-NULL rule lives in
    /// [`AccState::push_regr`] so that `DISTINCT`'s dedup sees the same rows
    /// this does.
    fn push(&mut self, y: f64, x: f64) {
        let n_old = self.n;
        self.n += 1.0;
        self.sx += x;
        self.sy += y;
        if n_old > 0.0 {
            let tmp_x = x.mul_add(self.n, -self.sx);
            let tmp_y = y.mul_add(self.n, -self.sy);
            // Reciprocal-multiply, not division — the opposite of
            // `VarAcc::push`, matching `float8_regr_accum`. The three
            // accumulations are fused multiply-adds; see the section comment.
            let scale = 1.0 / (self.n * n_old);
            self.sxx = (tmp_x * tmp_x).mul_add(scale, self.sxx);
            self.syy = (tmp_y * tmp_y).mul_add(scale, self.syy);
            self.sxy = (tmp_x * tmp_y).mul_add(scale, self.sxy);
            if self.sx.is_infinite() || self.sxx.is_infinite() {
                self.sxx = f64::NAN;
            }
            if self.sy.is_infinite() || self.syy.is_infinite() {
                self.syy = f64::NAN;
            }
            if self.sx.is_infinite() || self.sy.is_infinite() || self.sxy.is_infinite() {
                self.sxy = f64::NAN;
            }
        } else {
            if !x.is_finite() {
                self.sxx = f64::NAN;
                self.sxy = f64::NAN;
            }
            if !y.is_finite() {
                self.syy = f64::NAN;
                self.sxy = f64::NAN;
            }
        }
    }

    /// The finalizer, or `None` for SQL NULL. Every threshold below was read
    /// off the live server rather than recalled — over a single row
    /// `regr_sxx`/`regr_syy`/`regr_sxy`/`covar_pop` are `0`,
    /// `regr_avgx`/`regr_avgy` are the values themselves, and
    /// `regr_slope`/`regr_intercept`/`regr_r2`/`corr`/`covar_samp` are all
    /// NULL. [`RegrKind::Count`] is handled by the caller: it is the only
    /// member returning `bigint`, and the only one that is `0` rather than
    /// NULL over zero rows.
    fn finalize(&self, kind: RegrKind) -> Option<f64> {
        let n = self.n;
        match kind {
            RegrKind::Count => Some(n),
            RegrKind::AvgX => (n >= 1.0).then(|| self.sx / n),
            RegrKind::AvgY => (n >= 1.0).then(|| self.sy / n),
            RegrKind::Sxx => (n >= 1.0).then_some(self.sxx),
            RegrKind::Syy => (n >= 1.0).then_some(self.syy),
            RegrKind::Sxy => (n >= 1.0).then_some(self.sxy),
            RegrKind::CovarPop => (n >= 1.0).then(|| self.sxy / n),
            RegrKind::CovarSamp => (n >= 2.0).then(|| self.sxy / (n - 1.0)),
            RegrKind::Slope => (n >= 2.0 && self.sxx != 0.0).then(|| self.sxy / self.sxx),
            RegrKind::Intercept => {
                (n >= 2.0 && self.sxx != 0.0).then(|| (self.sy - self.sx * self.sxy / self.sxx) / n)
            }
            RegrKind::R2 => {
                if n < 2.0 || self.sxx == 0.0 {
                    None
                } else if self.syy == 0.0 {
                    Some(1.0)
                } else {
                    Some((self.sxy * self.sxy) / (self.sxx * self.syy))
                }
            }
            RegrKind::Corr => (n >= 2.0 && self.sxx != 0.0 && self.syy != 0.0)
                .then(|| self.sxy / (self.sxx * self.syy).sqrt()),
        }
    }
}

/// Per-group (or, for the ungrouped path, the single implicit group)
/// accumulator state for one aggregate.
#[derive(Clone)]
enum AccState {
    /// `count(*)`.
    CountStar(i64),
    /// `count(expr)` — counts non-null values only.
    Count(i64),
    /// `sum(expr)` over an integer column. `None` until the first non-null
    /// value arrives, and stays `None` if none ever does — see item 1.
    SumInt(Option<i64>),
    SumFloat(Option<f64>),
    Min(Option<CellValue>),
    Max(Option<CellValue>),
    /// `sum` and `count` of non-null values, divided at `finalize()`. See
    /// the FIDELITY GAP note in [`resolve_aggregate`] for `Avg`.
    Avg {
        sum: f64,
        count: i64,
    },
    /// `array_agg(expr)`. `None` until the first row is accepted (passes
    /// `FILTER`, and — if `DISTINCT` — is a first-seen value) and stays
    /// `None` forever if none ever is; `Some(list)` otherwise, where `list`
    /// may itself contain `None` entries (NULLs are elements, not skipped —
    /// see [`AccState::push_array_agg`]). The outer `None` is what makes
    /// zero-rows-accepted finalize to SQL NULL rather than `{}` (item: array
    /// agg NULL-over-empty).
    ArrayAgg {
        list: Option<Vec<Option<CellValue>>>,
        /// `array_agg(DISTINCT x)` sorts; plain `array_agg(x)` does not. See
        /// [`distinct_sort_cmp`] for the ordering and the live evidence.
        sorted: bool,
    },
    /// `string_agg(expr, delimiter)`. `None` until the first *non-null*
    /// value is accepted and stays `None` forever if none ever is (NULLs
    /// are skipped entirely, unlike `ArrayAgg` — see
    /// [`AccState::push_string_agg`]).
    StringAgg {
        /// The joined text, built incrementally in arrival order. Used when
        /// the aggregate is not `DISTINCT`.
        built: Option<String>,
        /// `string_agg(DISTINCT v, d)` only: one `(value, delimiter)` per
        /// accepted row, because the join cannot happen until every row is
        /// in and sorted. `None` for a non-`DISTINCT` `string_agg`, which
        /// never needs to hold the values at all.
        pending: Option<Vec<(String, String)>>,
    },
    /// The variance family. NULLs are skipped before reaching [`VarAcc`], so
    /// `n` counts non-NULL rows only.
    Variance(VarKind, VarAcc),
    /// `bool_and`/`bool_or`. `None` until the first non-NULL input, which is
    /// what makes zero-non-NULL-rows finalize to SQL NULL rather than to the
    /// operator's identity element (`true` for `and`, `false` for `or`) —
    /// the same trap [`AccState::SumInt`] documents for `sum`.
    Bool {
        and_semantics: bool,
        acc: Option<bool>,
    },
    /// `bit_and`/`bit_or`/`bit_xor`. Same `None`-until-first-value rule and
    /// for the same reason (`bit_and`'s identity is all-ones, `bit_or`'s and
    /// `bit_xor`'s is zero; neither is the right answer over zero rows —
    /// verified live, all three are NULL). Accumulates in `i64` regardless of
    /// the input width; [`build_typed_array`] narrows back to the output
    /// field's declared type, which `resolve_aggregate` sets to the input's
    /// exact type.
    Bits {
        op: AggFunc,
        acc: Option<i64>,
    },
    /// The `regr_*`/`corr`/`covar_*` family. See [`RegrAcc`].
    Regr(RegrKind, RegrAcc),
}

impl AccState {
    /// `distinct` is [`AggregateSpec::distinct`]. Only `array_agg` and
    /// `string_agg` need it — they are the two aggregates here whose *output
    /// order* is observable, and PostgreSQL sorts a `DISTINCT` aggregate's
    /// input before feeding the transition function (see
    /// [`distinct_sort_cmp`]). Every other aggregate's result is
    /// order-independent, so they ignore it.
    fn new(func: AggFunc, num_kind: NumKind, distinct: bool) -> Self {
        match func {
            AggFunc::CountStar => AccState::CountStar(0),
            AggFunc::Count => AccState::Count(0),
            AggFunc::Sum => match num_kind {
                NumKind::Int => AccState::SumInt(None),
                NumKind::Float => AccState::SumFloat(None),
            },
            AggFunc::Min => AccState::Min(None),
            AggFunc::Max => AccState::Max(None),
            AggFunc::Avg => AccState::Avg { sum: 0.0, count: 0 },
            AggFunc::ArrayAgg => AccState::ArrayAgg {
                list: None,
                sorted: distinct,
            },
            AggFunc::StringAgg { .. } => AccState::StringAgg {
                built: None,
                pending: distinct.then(Vec::new),
            },
            AggFunc::Variance(k) => AccState::Variance(k, VarAcc::default()),
            AggFunc::BoolAnd => AccState::Bool {
                and_semantics: true,
                acc: None,
            },
            AggFunc::BoolOr => AccState::Bool {
                and_semantics: false,
                acc: None,
            },
            AggFunc::BitAnd | AggFunc::BitOr | AggFunc::BitXor => AccState::Bits {
                op: func,
                acc: None,
            },
            AggFunc::Regr { kind, .. } => AccState::Regr(kind, RegrAcc::default()),
        }
    }

    /// `regr_*`/`corr`/`covar_*`: one row's `(y, x)` pair. Never routed
    /// through [`AccState::update_scalar`] — it needs two per-row values, the
    /// same reason `string_agg` has its own entry point.
    ///
    /// **A row is skipped unless BOTH arguments are non-NULL.** This is not
    /// the same as the blanket null-skip every one-argument aggregate here
    /// does, and getting it wrong inflates `regr_count` and silently biases
    /// every other member of the family. Verified live: `regr_count(y, x)`
    /// over `(1,10),(2,NULL),(NULL,30),(4,25)` is `2`, and over the same data
    /// `regr_avgx` is `2.5` — the mean of `{1, 4}`, not of `{1, 2, 4}`.
    fn push_regr(&mut self, y: &Option<CellValue>, x: &Option<CellValue>) -> Result<(), ExecError> {
        let AccState::Regr(_, acc) = self else {
            unreachable!("push_regr is only called for a Regr accumulator")
        };
        let (Some(y), Some(x)) = (y, x) else {
            return Ok(()); // either argument NULL: the row does not exist for this aggregate
        };
        acc.push(numeric_f64(y, "regr")?, numeric_f64(x, "regr")?);
        Ok(())
    }

    /// `array_agg`: append one row's value — NULL included — and return the
    /// extra heap bytes it costs, for the memory accountant. Never called
    /// through [`AccState::update_scalar`]: that method's blanket `let
    /// Some(v) = val else { return Ok(()) }` null-skip is correct for every
    /// *other* aggregate here but wrong for this one, so `array_agg` gets
    /// its own row-wise entry point instead (see `update_ungrouped_one` and
    /// `build_grouped`, the only two call sites). Verified against a live
    /// PG 18: `select array_agg(x) from (values (1),(NULL),(3)) t(x)` →
    /// `{1,NULL,3}`, a 3-element array.
    fn push_array_agg(&mut self, val: Option<CellValue>) -> usize {
        let AccState::ArrayAgg { list, .. } = self else {
            unreachable!("push_array_agg is only called for an ArrayAgg accumulator")
        };
        let bytes = std::mem::size_of::<Option<CellValue>>() + cell_heap_bytes(&val);
        list.get_or_insert_with(Vec::new).push(val);
        bytes
    }

    /// `string_agg`: append one row's value with its own row's delimiter,
    /// and return the extra heap bytes it costs. Also never routed through
    /// `update_scalar` — it needs two per-row values (expr and delimiter),
    /// not one. Two rules, both verified against a live PG 18:
    ///
    /// - A NULL value is skipped entirely — no text, no delimiter, no-op.
    ///   (`select string_agg(name,',') from t where name is null` → NULL,
    ///   not `''`; `select string_agg(name,',') from t` — the row where
    ///   `name` is NULL contributes nothing, e.g. `x,y,z` from 4 rows one of
    ///   which is NULL, not `x,,y,z`.)
    /// - A NULL delimiter contributes no separator (not an error, not
    ///   treated as an empty *value*) — `select string_agg(name, NULL) from
    ///   t` → `xyz` (concatenated with nothing between). The delimiter used
    ///   for a given append is the *current* row's delimiter, not the
    ///   previous row's or a single constant — confirmed with a per-row
    ///   delimiter column: appending `y` after `x` used `y`'s own row's
    ///   delimiter, not `x`'s.
    /// - The delimiter is never emitted before the first accepted value.
    ///
    /// Under `DISTINCT` the join is deferred to [`AccState::finalize`]
    /// instead, because the rows have to be sorted first and this method sees
    /// them one at a time. Both rules above still hold there — in particular
    /// each row keeps its *own* delimiter across the sort, which is what
    /// makes `string_agg(DISTINCT s, d)` over `('c','-'),('a','+'),('b','|')`
    /// come back `a|b-c` on a live PG 18.2 and not `a+b|c`.
    fn push_string_agg(
        &mut self,
        val: &Option<CellValue>,
        delim: &Option<CellValue>,
    ) -> Result<usize, ExecError> {
        let AccState::StringAgg { built, pending } = self else {
            unreachable!("push_string_agg is only called for a StringAgg accumulator")
        };
        let Some(v) = val else {
            return Ok(0); // NULL value: skip entirely, per the doc above
        };
        let CellValue::Utf8(s) = v else {
            return Err(ExecError::TypeMismatch(
                "string_agg(): non-text value reached the accumulator".into(),
            ));
        };
        let delim_str: &str = match delim {
            Some(CellValue::Utf8(d)) => d.as_str(),
            Some(_) => {
                return Err(ExecError::TypeMismatch(
                    "string_agg(): non-text delimiter reached the accumulator".into(),
                ));
            }
            None => "", // NULL delimiter: no separator, not an error
        };
        if let Some(rows) = pending {
            rows.push((s.clone(), delim_str.to_string()));
            return Ok(s.len() + delim_str.len() + std::mem::size_of::<(String, String)>());
        }
        Ok(match built {
            None => {
                *built = Some(s.clone());
                s.len()
            }
            Some(buf) => {
                buf.push_str(delim_str);
                buf.push_str(s);
                delim_str.len() + s.len()
            }
        })
    }

    fn add_count(&mut self, n: i64) {
        match self {
            AccState::CountStar(c) | AccState::Count(c) => *c += n,
            _ => unreachable!("add_count is only called for Count/CountStar accumulators"),
        }
    }

    /// Row-wise update from one already-extracted, already-filtered,
    /// already-(if DISTINCT)-deduped value. `None` (SQL NULL) is a no-op for
    /// every one of these accumulators — sum/avg/min/max/count(expr) all
    /// ignore nulls (item 2).
    fn update_scalar(&mut self, val: &Option<CellValue>) -> Result<(), ExecError> {
        let Some(v) = val else { return Ok(()) };
        match self {
            AccState::Count(c) => *c += 1,
            AccState::SumInt(sum) => {
                let CellValue::Int64(n) = v else {
                    return Err(ExecError::TypeMismatch(
                        "sum(): non-integer value reached an integer accumulator".into(),
                    ));
                };
                *sum = Some(match sum {
                    Some(s) => s.checked_add(*n).ok_or(ExecError::Overflow("bigint"))?,
                    None => *n,
                });
            }
            AccState::SumFloat(sum) => {
                let CellValue::Float64(f) = v else {
                    return Err(ExecError::TypeMismatch(
                        "sum(): non-float value reached a float accumulator".into(),
                    ));
                };
                *sum = Some(sum.unwrap_or(0.0) + f);
            }
            AccState::Min(cur) => update_extreme(cur, v, true),
            AccState::Max(cur) => update_extreme(cur, v, false),
            AccState::Avg { sum, count } => {
                let f = match v {
                    CellValue::Int64(i) => *i as f64,
                    CellValue::Float64(f) => *f,
                    _ => {
                        return Err(ExecError::TypeMismatch(
                            "avg(): non-numeric value reached the accumulator".into(),
                        ));
                    }
                };
                *sum += f;
                *count += 1;
            }
            AccState::Variance(_, acc) => acc.push(numeric_f64(v, "variance")?),
            AccState::Bool { and_semantics, acc } => {
                let CellValue::Bool(b) = v else {
                    return Err(ExecError::TypeMismatch(
                        "bool_and()/bool_or(): non-boolean value reached the accumulator".into(),
                    ));
                };
                *acc = Some(match *acc {
                    None => *b,
                    Some(cur) => {
                        if *and_semantics {
                            cur && *b
                        } else {
                            cur || *b
                        }
                    }
                });
            }
            AccState::Bits { op, acc } => {
                let CellValue::Int64(n) = v else {
                    return Err(ExecError::TypeMismatch(
                        "bit_and()/bit_or()/bit_xor(): non-integer value reached the accumulator"
                            .into(),
                    ));
                };
                *acc = Some(match *acc {
                    None => *n,
                    Some(cur) => match op {
                        AggFunc::BitAnd => cur & *n,
                        AggFunc::BitOr => cur | *n,
                        AggFunc::BitXor => cur ^ *n,
                        _ => unreachable!("AccState::Bits only ever carries a bitwise AggFunc"),
                    },
                });
            }
            AccState::CountStar(_) => unreachable!("count(*) never inspects a per-row value"),
            AccState::ArrayAgg { .. } => unreachable!(
                "array_agg is never routed through update_scalar — see push_array_agg's doc"
            ),
            AccState::StringAgg { .. } => unreachable!(
                "string_agg is never routed through update_scalar — see push_string_agg's doc"
            ),
            AccState::Regr(..) => unreachable!(
                "regr_*/corr/covar_* are never routed through update_scalar — they read two \
                 per-row columns; see push_regr's doc"
            ),
        }
        Ok(())
    }

    /// Batch-at-a-time update via arrow's aggregate kernels — the fast path,
    /// used only when there is no `GROUP BY` and this aggregate is not
    /// `DISTINCT`. `arr` already has `FILTER (WHERE …)` applied. Never
    /// called for `array_agg`/`string_agg` — those are always row-wise (see
    /// the module doc and `update_ungrouped_one`), so no arm here builds a
    /// list or concatenates text; both would be `unreachable!` too, but
    /// there is nothing to match on `self` before reaching one, since this
    /// whole function is skipped for those two functions upstream.
    fn update_kernel(&mut self, arr: &ArrayRef) -> Result<(), ExecError> {
        // No arrow kernel computes any of these three families, and for the
        // variance family none *could*: Youngs-Cramer is a strictly
        // sequential recurrence whose answer depends on row order (see the
        // section comment on `VarAcc`), so a kernel that reassociated the
        // work would stop matching Postgres. Looping in row order *is* the
        // fast path here — it is what the row-wise path does, minus the
        // per-row `FILTER`/`DISTINCT` re-checks the caller already applied
        // to `arr`. Handled ahead of the `match` because the loop needs
        // `self` mutably again for `update_scalar`.
        if matches!(
            self,
            AccState::Variance(..) | AccState::Bool { .. } | AccState::Bits { .. }
        ) {
            for row in 0..arr.len() {
                let val = extract_cell(arr, row)?;
                self.update_scalar(&val)?;
            }
            return Ok(());
        }
        match self {
            AccState::Count(c) => {
                *c += (arr.len() - arr.null_count()) as i64;
            }
            AccState::SumInt(sum) => {
                // Widen to i64 first so sum(int2)/sum(int4) accumulate in
                // bigint range like Postgres, rather than overflowing at the
                // source column's native width.
                let widened = arrow::compute::kernels::cast::cast(arr.as_ref(), &DataType::Int64)
                    .map_err(|e| ExecError::Internal(e.to_string()))?;
                let typed = widened.as_primitive::<Int64Type>();
                let partial = arrow::compute::kernels::aggregate::sum_checked(typed)
                    .map_err(|_| ExecError::Overflow("bigint"))?;
                if let Some(p) = partial {
                    *sum = Some(match sum {
                        Some(s) => s.checked_add(p).ok_or(ExecError::Overflow("bigint"))?,
                        None => p,
                    });
                }
            }
            AccState::SumFloat(sum) => {
                let widened = arrow::compute::kernels::cast::cast(arr.as_ref(), &DataType::Float64)
                    .map_err(|e| ExecError::Internal(e.to_string()))?;
                let typed = widened.as_primitive::<Float64Type>();
                if let Some(p) = arrow::compute::kernels::aggregate::sum(typed) {
                    *sum = Some(sum.unwrap_or(0.0) + p);
                }
            }
            AccState::Min(cur) => kernel_extreme(cur, arr, true)?,
            AccState::Max(cur) => kernel_extreme(cur, arr, false)?,
            AccState::Avg { sum, count } => {
                let widened = arrow::compute::kernels::cast::cast(arr.as_ref(), &DataType::Float64)
                    .map_err(|e| ExecError::Internal(e.to_string()))?;
                let typed = widened.as_primitive::<Float64Type>();
                if let Some(p) = arrow::compute::kernels::aggregate::sum(typed) {
                    *sum += p;
                }
                *count += (arr.len() - arr.null_count()) as i64;
            }
            AccState::Variance(..) | AccState::Bool { .. } | AccState::Bits { .. } => {
                unreachable!("handled by the row-order loop above, before this match")
            }
            AccState::CountStar(_) => {
                unreachable!("count(*) takes the row-count fast path in build_ungrouped, never a column kernel")
            }
            AccState::ArrayAgg { .. } | AccState::StringAgg { .. } | AccState::Regr(..) => unreachable!(
                "array_agg/string_agg/regr_* are always row-wise (push_array_agg/\
                 push_string_agg/push_regr), never dispatched to the arrow-kernel fast path"
            ),
        }
        Ok(())
    }

    /// Produce this accumulator's final value, or `None` for SQL NULL —
    /// e.g. `sum`/`avg`/`min`/`max` over zero (or all-null) input (item 1),
    /// and — for `array_agg`/`string_agg` — zero rows *accepted* (which,
    /// under `FILTER`, can happen even for a non-empty `GROUP BY` group; see
    /// `push_array_agg`/`push_string_agg`'s docs for the live-PG evidence).
    fn finalize(&self) -> Option<CellValue> {
        match self {
            AccState::CountStar(c) | AccState::Count(c) => Some(CellValue::Int64(*c)),
            AccState::SumInt(s) => s.map(CellValue::Int64),
            AccState::SumFloat(s) => s.map(CellValue::Float64),
            AccState::Min(v) | AccState::Max(v) => v.clone(),
            AccState::Avg { sum, count } => {
                if *count == 0 {
                    None
                } else {
                    Some(CellValue::Float64(sum / *count as f64))
                }
            }
            // `array_agg(DISTINCT x)` comes back SORTED, not in arrival
            // order — see [`distinct_sort_cmp`]. Plain `array_agg(x)` keeps
            // arrival order, so the sort is conditional and not a blanket
            // tidy-up: sorting the non-DISTINCT case too would be a wrong
            // answer in the opposite direction.
            AccState::ArrayAgg { list, sorted } => list.clone().map(|mut v| {
                if *sorted {
                    v.sort_by(distinct_sort_cmp);
                }
                CellValue::List(v)
            }),
            // Non-DISTINCT: already joined in arrival order. DISTINCT: sort
            // the held rows by VALUE (only — see below) and join, each row
            // contributing its own delimiter ahead of its own value.
            AccState::StringAgg { built, pending } => match pending {
                None => built.clone().map(CellValue::Utf8),
                Some(rows) if rows.is_empty() => None,
                Some(rows) => {
                    let mut rows = rows.clone();
                    // `sort_by` is stable, and the key is the value alone
                    // rather than the `(value, delimiter)` pair: measured on
                    // PG 18.2, `string_agg(DISTINCT s, d)` over
                    // `('a','-'),('a','+')` returns `a+a` — i.e. the two rows
                    // stayed in ARRIVAL order, which a sort on the pair would
                    // have reversed (`'+' < '-'`). Two rows sharing a value
                    // but not a delimiter is a pathological shape whose order
                    // PostgreSQL does not document; matching what it does
                    // here costs nothing and surprises no one.
                    rows.sort_by(|a, b| a.0.cmp(&b.0));
                    let mut out = String::new();
                    for (i, (value, delim)) in rows.iter().enumerate() {
                        if i > 0 {
                            out.push_str(delim);
                        }
                        out.push_str(value);
                    }
                    Some(CellValue::Utf8(out))
                }
            },
            AccState::Variance(kind, acc) => acc.finalize(*kind).map(CellValue::Float64),
            AccState::Bool { acc, .. } => acc.map(CellValue::Bool),
            AccState::Bits { acc, .. } => acc.map(CellValue::Int64),
            // `regr_count` is the family's one integer member and its one
            // never-NULL member: `RegrAcc::finalize` hands back `N` as an
            // f64 and this narrows it, so zero rows give `0` rather than SQL
            // NULL (verified live).
            AccState::Regr(RegrKind::Count, acc) => acc
                .finalize(RegrKind::Count)
                .map(|n| CellValue::Int64(n as i64)),
            AccState::Regr(kind, acc) => acc.finalize(*kind).map(CellValue::Float64),
        }
    }
}

fn kernel_extreme(
    cur: &mut Option<CellValue>,
    arr: &ArrayRef,
    want_min: bool,
) -> Result<(), ExecError> {
    use arrow::compute::kernels::aggregate::{
        max, max_boolean, max_string, min, min_boolean, min_string,
    };

    let candidate: Option<CellValue> = match arr.data_type() {
        DataType::Int16 => {
            let a = arr.as_primitive::<Int16Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| CellValue::Int64(v as i64))
        }
        DataType::Int32 => {
            let a = arr.as_primitive::<Int32Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| CellValue::Int64(v as i64))
        }
        DataType::Int64 => {
            let a = arr.as_primitive::<Int64Type>();
            (if want_min { min(a) } else { max(a) }).map(CellValue::Int64)
        }
        DataType::Float32 => {
            let a = arr.as_primitive::<Float32Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| CellValue::Float64(v as f64))
        }
        DataType::Float64 => {
            let a = arr.as_primitive::<Float64Type>();
            (if want_min { min(a) } else { max(a) }).map(CellValue::Float64)
        }
        DataType::Boolean => {
            let a = arr.as_boolean();
            (if want_min {
                min_boolean(a)
            } else {
                max_boolean(a)
            })
            .map(CellValue::Bool)
        }
        DataType::Utf8 => {
            let a = arr.as_string::<i32>();
            (if want_min {
                min_string(a)
            } else {
                max_string(a)
            })
            .map(|s| CellValue::Utf8(s.to_string()))
        }
        DataType::LargeUtf8 => {
            let a = arr.as_string::<i64>();
            (if want_min {
                min_string(a)
            } else {
                max_string(a)
            })
            .map(|s| CellValue::Utf8(s.to_string()))
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "min()/max() over unsupported type {other:?}"
            )));
        }
    };
    if let Some(v) = candidate {
        update_extreme(cur, &v, want_min);
    }
    Ok(())
}

/// Build one output column, narrowing each [`CellValue`] to `dtype`'s exact
/// width. Safe because every value in `values` originated from a column of
/// that same family (group keys and `MIN`/`MAX` never widen; see the
/// [`CellValue`] doc comment).
fn build_typed_array(
    dtype: &DataType,
    values: &[Option<CellValue>],
) -> Result<ArrayRef, ExecError> {
    fn as_i64(v: &Option<CellValue>) -> Option<i64> {
        match v {
            Some(CellValue::Int64(i)) => Some(*i),
            _ => None,
        }
    }
    fn as_f64(v: &Option<CellValue>) -> Option<f64> {
        match v {
            Some(CellValue::Float64(f)) => Some(*f),
            _ => None,
        }
    }

    Ok(match dtype {
        DataType::Int16 => Arc::new(Int16Array::from(
            values
                .iter()
                .map(|v| as_i64(v).map(|i| i as i16))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| as_i64(v).map(|i| i as i32))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(
            values.iter().map(as_i64).collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| as_f64(v).map(|f| f as f32))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(
            values.iter().map(as_f64).collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| match v {
                    Some(CellValue::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Utf8 | DataType::LargeUtf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| match v {
                    Some(CellValue::Utf8(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        // `array_agg`'s output: one row per group (or the single ungrouped
        // row), each either NULL (the accumulator saw zero accepted rows —
        // `values[i]` is `None`, not `Some(CellValue::List(vec![]))`, since
        // those are different things: NULL vs. `{}`, and this operator only
        // ever produces the former, matching live PG 18) or a list of
        // per-row cells, which may themselves be NULL (array_agg includes
        // NULL elements). Flatten every row's elements into one child array
        // with `array_agg`'s own [`build_typed_array`] (recursion bottoms
        // out because element types are never themselves `List`), and record
        // each row's length as `0` for a NULL row — `OffsetBuffer` only
        // needs lengths, and a NULL row's `NullBuffer` bit is what actually
        // marks it absent, not a nonzero-vs-zero length.
        DataType::List(item_field) => {
            let mut lengths: Vec<usize> = Vec::with_capacity(values.len());
            let mut validity: Vec<bool> = Vec::with_capacity(values.len());
            let mut flat: Vec<Option<CellValue>> = Vec::new();
            for v in values {
                match v {
                    Some(CellValue::List(items)) => {
                        validity.push(true);
                        lengths.push(items.len());
                        flat.extend(items.iter().cloned());
                    }
                    _ => {
                        validity.push(false);
                        lengths.push(0);
                    }
                }
            }
            let child = build_typed_array(item_field.data_type(), &flat)?;
            let offsets = arrow::buffer::OffsetBuffer::<i32>::from_lengths(lengths);
            let nulls = arrow::buffer::NullBuffer::from(validity);
            Arc::new(
                ListArray::try_new(item_field.clone(), offsets, child, Some(nulls))
                    .map_err(|e| ExecError::Internal(e.to_string()))?,
            ) as ArrayRef
        }
        other => {
            return Err(ExecError::Internal(format!(
                "hash aggregate: cannot build an output column of type {other:?}"
            )));
        }
    })
}

/// Output batches are chunked so a `GROUP BY` over a huge key space doesn't
/// hand the caller one unbounded `RecordBatch`.
const OUTPUT_BATCH_SIZE: usize = 8192;

/// Grouped and ungrouped hash aggregation — see the module doc comment for
/// the full design.
pub struct HashAggregate {
    input: Box<dyn Operator>,
    group_cols: Vec<usize>,
    aggregates: Vec<ResolvedAgg>,
    schema: SchemaRef,
    budget: usize,
    bytes_used: usize,
    output: Option<VecDeque<RecordBatch>>,
}

impl HashAggregate {
    /// `group_cols` are column positions in `input`'s schema; an empty list
    /// means ungrouped (one output row, even over zero input rows — item 1).
    /// `memory_budget` bounds the hash table plus accumulator state; see the
    /// module doc's Memory section.
    pub fn new(
        input: Box<dyn Operator>,
        group_cols: Vec<usize>,
        aggregates: Vec<AggregateSpec>,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        let input_schema = input.schema();
        let mut fields = Vec::with_capacity(group_cols.len() + aggregates.len());
        for &gc in &group_cols {
            fields.push(input_schema.field(gc).clone());
        }
        let mut resolved = Vec::with_capacity(aggregates.len());
        for spec in aggregates {
            let (agg, field) = resolve_aggregate(spec, &input_schema)?;
            fields.push(field);
            resolved.push(agg);
        }
        Ok(Self {
            input,
            group_cols,
            aggregates: resolved,
            schema: Arc::new(Schema::new(fields)),
            budget: memory_budget,
            bytes_used: 0,
            output: None,
        })
    }

    fn agg_output_dtype(&self, i: usize) -> DataType {
        self.schema
            .field(self.group_cols.len() + i)
            .data_type()
            .clone()
    }

    fn bump_memory(&mut self, add: usize) -> Result<(), ExecError> {
        self.bytes_used += add;
        if self.bytes_used > self.budget {
            return Err(ExecError::OutOfMemory {
                requested: self.bytes_used,
                budget: self.budget,
            });
        }
        Ok(())
    }

    fn build(&mut self) -> Result<(), ExecError> {
        let batches = if self.group_cols.is_empty() {
            self.build_ungrouped()?
        } else {
            self.build_grouped()?
        };
        self.output = Some(batches.into());
        Ok(())
    }

    /// No `GROUP BY`: exactly one output row, produced even over zero input
    /// rows (item 1 — `SUM`/`AVG`/`MIN`/`MAX` are NULL, `COUNT` is 0).
    fn build_ungrouped(&mut self) -> Result<Vec<RecordBatch>, ExecError> {
        let n = self.aggregates.len();
        let mut accs: Vec<AccState> = self
            .aggregates
            .iter()
            .map(|a| AccState::new(a.spec.func, a.num_kind, a.spec.distinct))
            .collect();
        let mut distinct_seen: Vec<Option<HashSet<HashKey>>> = self
            .aggregates
            .iter()
            .map(|a| a.spec.distinct.then(HashSet::new))
            .collect();

        loop {
            let batch = match self.input.next_batch()? {
                Some(b) => b,
                None => break,
            };
            if batch.num_rows() == 0 {
                continue;
            }
            for i in 0..n {
                self.update_ungrouped_one(&batch, i, &mut accs[i], &mut distinct_seen[i])?;
            }
        }

        let mut arrays = Vec::with_capacity(n);
        for (i, acc) in accs.iter().enumerate() {
            let dtype = self.agg_output_dtype(i);
            arrays.push(build_typed_array(
                &dtype,
                std::slice::from_ref(&acc.finalize()),
            )?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| ExecError::Internal(e.to_string()))?;
        Ok(vec![batch])
    }

    fn update_ungrouped_one(
        &mut self,
        batch: &RecordBatch,
        i: usize,
        acc: &mut AccState,
        distinct: &mut Option<HashSet<HashKey>>,
    ) -> Result<(), ExecError> {
        let agg = &self.aggregates[i];
        let func = agg.spec.func; // `AggFunc` is `Copy`; see the borrow note below
        let mask: Option<&BooleanArray> =
            agg.spec.filter_col.map(|fc| batch.column(fc).as_boolean());

        if func == AggFunc::CountStar {
            let n = match mask {
                Some(m) => m.true_count() as i64,
                None => batch.num_rows() as i64,
            };
            acc.add_count(n);
            return Ok(());
        }

        // array_agg/string_agg are always row-wise, never the arrow-kernel
        // fast path below: array_agg must keep every row's NULL as an
        // element (the DISTINCT loop and the kernel path below both drop
        // NULLs, which is right for every other aggregate but wrong here),
        // and string_agg reads a *second* column — the delimiter — in
        // lockstep with the value column, which the single `filtered` array
        // built below has no room for. `col`/`delim_idx` are extracted from
        // `agg` up front (not re-read inside the loop) so this immutable
        // borrow of `self.aggregates[i]` ends before the loop's
        // `self.bump_memory` calls need `self` mutably.
        // `regr_*` joins them for the second of those two reasons: it reads a
        // `(y, x)` pair per row, and its NULL rule is "skip unless *both* are
        // non-NULL", which the single-column `filtered` array cannot express
        // either.
        if is_row_wise_only(func) {
            let col = agg
                .spec
                .input_col
                .expect("resolved: every row-wise-only aggregate carries an input column");
            let value_col = batch.column(col).clone();
            let second_col = second_arg_col(func).map(|d| batch.column(d).clone());
            let is_array_agg = matches!(func, AggFunc::ArrayAgg);
            let is_regr = matches!(func, AggFunc::Regr { .. });

            for row in 0..batch.num_rows() {
                if let Some(m) = mask {
                    if !(m.is_valid(row) && m.value(row)) {
                        continue;
                    }
                }
                let val = extract_cell(&value_col, row)?;
                let second = match &second_col {
                    Some(sc) => extract_cell(sc, row)?,
                    None => None,
                };
                if let Some(seen) = distinct.as_mut() {
                    // array_agg(DISTINCT x) still keeps one NULL entry if any
                    // accepted row's value was NULL — verified against a
                    // live PG 18: `array_agg(distinct id)` over
                    // `{1,2,1,NULL,2}` → `{1,2,NULL}`. string_agg(DISTINCT
                    // …) drops NULLs from the dedup set, matching its own
                    // ignore-nulls rule (verified: `string_agg(distinct
                    // name, ',')` over `{x,y,x,NULL}` → `'x,y'`).
                    if val.is_none() && !is_array_agg {
                        continue;
                    }
                    // DISTINCT on a two-argument aggregate dedups the whole
                    // argument list — see `HashKey::Pair`'s doc for the live
                    // evidence. `string_agg` is two-argument too (value and
                    // delimiter), and behaves the same way: measured on PG
                    // 18.2, `string_agg(DISTINCT s, d)` over
                    // `('a','-'),('a','+')` returns `a+a`, so the two rows
                    // are NOT duplicates of each other even though their
                    // values are equal.
                    let key = if is_regr || matches!(func, AggFunc::StringAgg { .. }) {
                        HashKey::Pair(Box::new((HashKey::from(&val), HashKey::from(&second))))
                    } else {
                        HashKey::from(&val)
                    };
                    if !seen.insert(key.clone()) {
                        continue;
                    }
                    self.bump_memory(hash_key_bytes(&key))?;
                }
                if is_array_agg {
                    let added = acc.push_array_agg(val);
                    self.bump_memory(added)?;
                } else if is_regr {
                    acc.push_regr(&val, &second)?;
                } else {
                    let added = acc.push_string_agg(&val, &second)?;
                    self.bump_memory(added)?;
                }
            }
            return Ok(());
        }

        let col = agg
            .spec
            .input_col
            .expect("resolved: every non-CountStar aggregate carries an input column");
        let raw = batch.column(col);
        // FILTER narrows this aggregate's own input; it does not touch the
        // group (there is only one group here, so this is moot for grouping
        // but not for the aggregate's result).
        let filtered: ArrayRef = match mask {
            Some(m) => arrow::compute::kernels::filter::filter(raw.as_ref(), m)
                .map_err(|e| ExecError::Internal(e.to_string()))?,
            None => raw.clone(),
        };

        if let Some(seen) = distinct.as_mut() {
            // No kernel deduplicates values, so DISTINCT is row-wise even on
            // the otherwise-vectorised ungrouped path.
            for row in 0..filtered.len() {
                let val = extract_cell(&filtered, row)?;
                if val.is_none() {
                    continue; // DISTINCT ignores NULLs, like the aggregate itself
                }
                let key = HashKey::from(&val);
                if seen.insert(key.clone()) {
                    self.bump_memory(hash_key_bytes(&key))?;
                    acc.update_scalar(&val)?;
                }
            }
            return Ok(());
        }

        acc.update_kernel(&filtered)
    }

    /// `GROUP BY` present: zero output rows over zero input rows (unlike the
    /// ungrouped case — there is no group to report a value for), one row per
    /// distinct key otherwise. NULL is a valid, single group (item 3).
    fn build_grouped(&mut self) -> Result<Vec<RecordBatch>, ExecError> {
        let mut group_index: HashMap<Vec<HashKey>, usize> = HashMap::new();
        let mut group_values: Vec<Vec<Option<CellValue>>> = Vec::new();
        let mut accs: Vec<Vec<AccState>> = Vec::new();
        let mut distinct_seen: Vec<Vec<Option<HashSet<HashKey>>>> = Vec::new();

        loop {
            let batch = match self.input.next_batch()? {
                Some(b) => b,
                None => break,
            };
            let group_cols: Vec<&ArrayRef> =
                self.group_cols.iter().map(|&c| batch.column(c)).collect();
            let filter_cols: Vec<Option<&BooleanArray>> = self
                .aggregates
                .iter()
                .map(|a| a.spec.filter_col.map(|c| batch.column(c).as_boolean()))
                .collect();
            let value_cols: Vec<Option<&ArrayRef>> = self
                .aggregates
                .iter()
                .map(|a| a.spec.input_col.map(|c| batch.column(c)))
                .collect();

            for row in 0..batch.num_rows() {
                let mut key_vals = Vec::with_capacity(group_cols.len());
                for gc in &group_cols {
                    key_vals.push(extract_cell(gc, row)?);
                }
                // All NULL group-key rows hash and compare equal, so they
                // collapse into one group here regardless of how many group
                // columns there are — item 3.
                let hash_key: Vec<HashKey> = key_vals.iter().map(HashKey::from).collect();

                let gid = match group_index.get(&hash_key) {
                    Some(&id) => id,
                    None => {
                        let id = group_values.len();
                        let key_bytes: usize = key_vals.iter().map(cell_heap_bytes).sum::<usize>()
                            + std::mem::size_of::<HashKey>() * key_vals.len();
                        let acc_bytes = self.aggregates.len() * std::mem::size_of::<AccState>();
                        group_values.push(key_vals.clone());
                        accs.push(
                            self.aggregates
                                .iter()
                                .map(|a| AccState::new(a.spec.func, a.num_kind, a.spec.distinct))
                                .collect(),
                        );
                        distinct_seen.push(
                            self.aggregates
                                .iter()
                                .map(|a| a.spec.distinct.then(HashSet::new))
                                .collect(),
                        );
                        group_index.insert(hash_key, id);
                        self.bump_memory(key_bytes + acc_bytes)?;
                        id
                    }
                };

                for i in 0..self.aggregates.len() {
                    if let Some(mask) = filter_cols[i] {
                        // NULL in the FILTER predicate excludes the row, the
                        // same as `WHERE NULL` would.
                        let pass = mask.is_valid(row) && mask.value(row);
                        if !pass {
                            continue;
                        }
                    }
                    let agg = &self.aggregates[i];
                    // Copy everything needed out of `agg` (an immutable
                    // borrow of `self.aggregates[i]`) before any
                    // `self.bump_memory` call below, which needs `self`
                    // mutably — mirrors the same borrow shape
                    // `update_ungrouped_one` uses for the same reason.
                    let func = agg.spec.func;
                    let distinct = agg.spec.distinct;
                    if func == AggFunc::CountStar {
                        accs[gid][i].add_count(1);
                        continue;
                    }
                    let col = value_cols[i]
                        .expect("resolved: every non-CountStar aggregate carries an input column");
                    let val = extract_cell(col, row)?;
                    let is_array_agg = matches!(func, AggFunc::ArrayAgg);
                    let is_regr = matches!(func, AggFunc::Regr { .. });
                    let second = match second_arg_col(func) {
                        Some(c) => extract_cell(batch.column(c), row)?,
                        None => None,
                    };

                    if distinct {
                        // See update_ungrouped_one's identical rule and its
                        // live-PG citations: array_agg(DISTINCT x) keeps one
                        // NULL entry, every other DISTINCT aggregate here
                        // (including string_agg) drops NULLs, and DISTINCT on
                        // a two-argument aggregate dedups the whole argument
                        // list (`HashKey::Pair`).
                        if val.is_none() && !is_array_agg {
                            continue; // DISTINCT ignores NULLs, like the aggregate itself
                        }
                        let key = if is_regr || matches!(func, AggFunc::StringAgg { .. }) {
                            HashKey::Pair(Box::new((HashKey::from(&val), HashKey::from(&second))))
                        } else {
                            HashKey::from(&val)
                        };
                        let seen = distinct_seen[gid][i].as_mut().expect(
                            "distinct flag on the spec implies a seen-set was allocated above",
                        );
                        if !seen.insert(key.clone()) {
                            continue;
                        }
                        self.bump_memory(hash_key_bytes(&key))?;
                    }

                    if is_array_agg {
                        let added = accs[gid][i].push_array_agg(val);
                        self.bump_memory(added)?;
                    } else if is_regr {
                        accs[gid][i].push_regr(&val, &second)?;
                    } else if matches!(func, AggFunc::StringAgg { .. }) {
                        let added = accs[gid][i].push_string_agg(&val, &second)?;
                        self.bump_memory(added)?;
                    } else {
                        accs[gid][i].update_scalar(&val)?;
                    }
                }
            }
        }

        let num_groups = group_values.len();
        let mut batches = Vec::new();
        let mut start = 0;
        while start < num_groups {
            let end = (start + OUTPUT_BATCH_SIZE).min(num_groups);
            let mut arrays = Vec::with_capacity(self.group_cols.len() + self.aggregates.len());
            for j in 0..self.group_cols.len() {
                let dtype = self.schema.field(j).data_type().clone();
                let col_values: Vec<Option<CellValue>> = group_values[start..end]
                    .iter()
                    .map(|row| row[j].clone())
                    .collect();
                arrays.push(build_typed_array(&dtype, &col_values)?);
            }
            for i in 0..self.aggregates.len() {
                let dtype = self.agg_output_dtype(i);
                let col_values: Vec<Option<CellValue>> = accs[start..end]
                    .iter()
                    .map(|row| row[i].finalize())
                    .collect();
                arrays.push(build_typed_array(&dtype, &col_values)?);
            }
            batches.push(
                RecordBatch::try_new(self.schema.clone(), arrays)
                    .map_err(|e| ExecError::Internal(e.to_string()))?,
            );
            start = end;
        }
        Ok(batches)
    }
}

impl Operator for HashAggregate {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.output.is_none() {
            // Aggregation is inherently blocking — no output row is valid
            // until every input row has been seen, since a later batch can
            // still change any group's sum/min/max. This one call therefore
            // drains `input` completely rather than returning between
            // batches. `statement_timeout` still applies at the statement
            // level, but a single `next_batch` call on this operator is not
            // itself interruptible mid-build — the same limitation any
            // blocking operator (sort, hash join build side) has, and a
            // known gap rather than one specific to this file.
            self.build()?;
        }
        Ok(self.output.as_mut().and_then(VecDeque::pop_front))
    }

    fn memory_used(&self) -> usize {
        self.bytes_used
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::Field;

    /// A fixed sequence of pre-built batches, fed to the operator under test
    /// one at a time — stands in for a real scan/filter/project operator.
    struct VecOperator {
        schema: SchemaRef,
        batches: std::collections::VecDeque<RecordBatch>,
    }

    impl VecOperator {
        fn boxed(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
            Box::new(VecOperator {
                schema,
                batches: batches.into(),
            })
        }
    }

    impl Operator for VecOperator {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            Ok(self.batches.pop_front())
        }
    }

    fn schema_1int(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]))
    }

    fn int_batch(schema: &SchemaRef, values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn sum_spec(col: usize) -> AggregateSpec {
        AggregateSpec {
            func: AggFunc::Sum,
            input_col: Some(col),
            distinct: false,
            filter_col: None,
            alias: "s".into(),
        }
    }

    fn array_agg_spec(col: usize, distinct: bool) -> AggregateSpec {
        AggregateSpec {
            func: AggFunc::ArrayAgg,
            input_col: Some(col),
            distinct,
            filter_col: None,
            alias: "aa".into(),
        }
    }

    fn string_agg_spec(col: usize, delim_col: usize, distinct: bool) -> AggregateSpec {
        AggregateSpec {
            func: AggFunc::StringAgg { delim_col },
            input_col: Some(col),
            distinct,
            filter_col: None,
            alias: "sa".into(),
        }
    }

    /// Pull row `row` of a `LIST<Int64>` output column back out as a plain
    /// `Vec`, or `None` if the list itself is NULL — the shape tests want to
    /// assert against, rather than re-deriving arrow array plumbing in every
    /// test body.
    fn list_i64_values(arr: &ArrayRef, row: usize) -> Option<Vec<Option<i64>>> {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(row) {
            return None;
        }
        let elems = list.value(row);
        let ints = elems.as_any().downcast_ref::<Int64Array>().unwrap();
        Some(
            (0..ints.len())
                .map(|i| (!ints.is_null(i)).then(|| ints.value(i)))
                .collect(),
        )
    }

    fn count_star_spec() -> AggregateSpec {
        AggregateSpec {
            func: AggFunc::CountStar,
            input_col: None,
            distinct: false,
            filter_col: None,
            alias: "n".into(),
        }
    }

    // Item 1a: SUM over zero rows is NULL, not 0. The single most commonly
    // botched aggregate rule — an implementation that special-cases "no
    // rows" to return 0 (the identity element for +) silently corrupts every
    // ungrouped SUM over an empty table or empty group.
    #[test]
    fn sum_of_empty_input_is_null_not_zero() {
        let schema = schema_1int("x");
        let input = VecOperator::boxed(schema.clone(), vec![]);
        let mut agg = HashAggregate::new(input, vec![], vec![sum_spec(0)], usize::MAX).unwrap();

        let batch = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            batch.num_rows(),
            1,
            "ungrouped aggregate always emits one row"
        );
        let sums = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            sums.is_null(0),
            "SUM() over zero rows must be NULL, got {:?}",
            sums.value(0)
        );
        assert!(agg.next_batch().unwrap().is_none());
    }

    // Item 1b: COUNT over zero rows is 0, not NULL — the mirror-image bug to
    // 1a. An implementation that treats "no groups seen" uniformly as NULL
    // for every aggregate gets this one wrong.
    #[test]
    fn count_of_empty_input_is_zero_not_null() {
        let schema = schema_1int("x");
        let input = VecOperator::boxed(schema.clone(), vec![]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![count_star_spec()], usize::MAX).unwrap();

        let batch = agg.next_batch().unwrap().unwrap();
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(!counts.is_null(0), "COUNT(*) must never be NULL");
        assert_eq!(counts.value(0), 0, "COUNT(*) over zero rows must be 0");
    }

    // Item 2: sum/avg/min/max ignore NULLs; count(*) counts every row
    // (including all-null ones) while count(expr) counts only non-null
    // values. A naive implementation that treats NULL as 0 for SUM, or that
    // makes count(expr) behave like count(*), gets silently wrong answers on
    // any table with nulls.
    #[test]
    fn nulls_are_ignored_by_value_aggregates_but_counted_by_count_star() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(1), None, Some(3), None]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let specs = vec![
            count_star_spec(),
            AggregateSpec {
                func: AggFunc::Count,
                input_col: Some(0),
                distinct: false,
                filter_col: None,
                alias: "c".into(),
            },
            sum_spec(0),
        ];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();

        let count_star = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let count_expr = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sum = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            count_star.value(0),
            4,
            "count(*) must count the NULL rows too"
        );
        assert_eq!(
            count_expr.value(0),
            2,
            "count(x) must count only the two non-null rows"
        );
        assert_eq!(
            sum.value(0),
            4,
            "sum(x) must ignore the NULLs (1 + 3), not treat them as 0"
        );
    }

    // Item 3: NULL is a valid GROUP BY key, and all NULLs collapse into one
    // group rather than each starting its own group (NULL is not
    // self-distinct the way `NULL = NULL` is unknown) and rather than being
    // dropped from the result the way a naive `WHERE key IS NOT NULL` filter
    // would drop them.
    #[test]
    fn null_group_key_forms_one_group_and_is_not_dropped() {
        let group_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            group_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None, None])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(group_schema, vec![batch]);
        let mut agg = HashAggregate::new(input, vec![0], vec![sum_spec(1)], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();

        assert_eq!(
            out.num_rows(),
            2,
            "NULL keys must collapse into exactly one extra group, not vanish or fan out"
        );
        let groups = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut seen: Vec<(Option<i64>, i64)> = (0..out.num_rows())
            .map(|i| {
                (
                    if groups.is_null(i) {
                        None
                    } else {
                        Some(groups.value(i))
                    },
                    sums.value(i),
                )
            })
            .collect();
        seen.sort_by_key(|(g, _)| *g);
        assert_eq!(
            seen,
            vec![(None, 110), (Some(1), 40)],
            "NULL group must sum 20+40+50, key 1 must sum 10+30"
        );
    }

    // Item 4: AVG(integer) in Postgres returns NUMERIC (arbitrary precision).
    // basin-exec has no arbitrary-precision decimal type yet, so it returns
    // DOUBLE PRECISION instead — a documented fidelity gap, not a silent
    // substitution. This test pins the *value* basin-exec returns today so a
    // future regression (e.g. accidentally truncating to an integer average)
    // is caught, while the doc comment on `resolve_aggregate`'s `Avg` arm
    // names the gap explicitly.
    #[test]
    fn avg_of_integers_returns_float_a_documented_fidelity_gap() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(1), Some(2)]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let specs = vec![AggregateSpec {
            func: AggFunc::Avg,
            input_col: Some(0),
            distinct: false,
            filter_col: None,
            alias: "a".into(),
        }];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        assert_eq!(agg.schema().field(0).data_type(), &DataType::Float64);
        let out = agg.next_batch().unwrap().unwrap();
        let avg = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // The true Postgres answer, 1.5, happens to be exact in f64 too —
        // this only demonstrates the common case works, not that basin-exec
        // matches Postgres bit-for-bit on precision. See the doc comment.
        assert_eq!(
            avg.value(0),
            1.5,
            "avg(integer) is NUMERIC in Postgres, DOUBLE PRECISION here — value must still be 1.5"
        );
    }

    // Item 5: integer SUM overflow must be a hard error, matching Postgres's
    // "bigint out of range" — not a silently wrapped result, which is what
    // Rust's release-mode wrapping add (and some SIMD kernels) would produce
    // if used unchecked.
    #[test]
    fn integer_sum_overflow_errors_instead_of_wrapping() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(i64::MAX), Some(1)]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let mut agg = HashAggregate::new(input, vec![], vec![sum_spec(0)], usize::MAX).unwrap();
        let err = agg.next_batch().unwrap_err();
        assert_eq!(
            err,
            ExecError::Overflow("bigint"),
            "overflow must be reported, not wrapped into a bogus negative sum"
        );
    }

    // Overflow must also be caught when it happens across group boundaries
    // in the row-wise engine (as opposed to within a single arrow-kernel
    // batch sum), since that accumulation path uses a different code path
    // (`AccState::update_scalar` vs. `update_kernel`).
    #[test]
    fn integer_sum_overflow_errors_in_the_grouped_path_too() {
        let group_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            group_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![i64::MAX, 1])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(group_schema, vec![batch]);
        let mut agg = HashAggregate::new(input, vec![0], vec![sum_spec(1)], usize::MAX).unwrap();
        let err = agg.next_batch().unwrap_err();
        assert_eq!(err, ExecError::Overflow("bigint"));
    }

    // COUNT(DISTINCT x) must dedup values, including across multiple
    // batches, and must not count NULLs.
    #[test]
    fn count_distinct_dedups_across_batches_and_ignores_null() {
        let schema = schema_1int("x");
        let b1 = int_batch(&schema, vec![Some(1), Some(2), Some(1), None]);
        let b2 = int_batch(&schema, vec![Some(2), Some(3), None]);
        let input = VecOperator::boxed(schema.clone(), vec![b1, b2]);
        let specs = vec![AggregateSpec {
            func: AggFunc::Count,
            input_col: Some(0),
            distinct: true,
            filter_col: None,
            alias: "cd".into(),
        }];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let n = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 3, "distinct non-null values are {{1, 2, 3}}");
    }

    // FILTER (WHERE …) applies per-aggregate, not to the group: a row that
    // fails one aggregate's filter must still count toward a sibling
    // aggregate without a filter, and (in the grouped case) must still
    // belong to its group.
    #[test]
    fn filter_clause_applies_per_aggregate_not_to_the_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("keep", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                ])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![
            AggregateSpec {
                func: AggFunc::Sum,
                input_col: Some(0),
                distinct: false,
                filter_col: Some(1),
                alias: "filtered_sum".into(),
            },
            AggregateSpec {
                func: AggFunc::CountStar,
                input_col: None,
                distinct: false,
                filter_col: None,
                alias: "n".into(),
            },
        ];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let sum = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 4, "only rows 1 and 3 pass FILTER (true), NULL is treated as excluding like WHERE NULL: 1+3=4");
        assert_eq!(n.value(0), 4, "count(*) has no FILTER, so it must still see all 4 rows regardless of the sibling's filter");
    }

    // A basic grouped, multi-batch, multi-aggregate correctness check tying
    // grouping + sum + min + max + avg together, to catch any interaction
    // bug the single-purpose tests above miss.
    #[test]
    fn grouped_aggregation_spans_batches_and_computes_every_function() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("x", DataType::Int32, true),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int32Array::from(vec![10, 1, 20])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["b", "a"])),
                Arc::new(Int32Array::from(vec![5, 30])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![b1, b2]);
        let specs = vec![
            sum_spec(1),
            AggregateSpec {
                func: AggFunc::Min,
                input_col: Some(1),
                distinct: false,
                filter_col: None,
                alias: "mn".into(),
            },
            AggregateSpec {
                func: AggFunc::Max,
                input_col: Some(1),
                distinct: false,
                filter_col: None,
                alias: "mx".into(),
            },
            AggregateSpec {
                func: AggFunc::Avg,
                input_col: Some(1),
                distinct: false,
                filter_col: None,
                alias: "av".into(),
            },
        ];
        let mut agg = HashAggregate::new(input, vec![0], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);

        let groups = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mins = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        let maxs = out.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
        let avgs = out
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut rows: Vec<(String, i64, i32, i32, f64)> = (0..out.num_rows())
            .map(|i| {
                (
                    groups.value(i).to_string(),
                    sums.value(i),
                    mins.value(i),
                    maxs.value(i),
                    avgs.value(i),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        // group "a": 10, 20, 30 (spans both batches); group "b": 1, 5.
        assert_eq!(rows[0], ("a".to_string(), 60, 10, 30, 20.0));
        assert_eq!(rows[1], ("b".to_string(), 6, 1, 5, 3.0));
        assert!(
            agg.next_batch().unwrap().is_none(),
            "operator must be exhausted after its one output batch"
        );
    }

    // GROUP BY over zero input rows produces zero output rows — unlike the
    // ungrouped case, there is no group to report a NULL/zero value for.
    #[test]
    fn grouped_aggregate_over_empty_input_has_no_output_rows() {
        let group_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let input = VecOperator::boxed(group_schema.clone(), vec![]);
        let mut agg = HashAggregate::new(input, vec![0], vec![sum_spec(1)], usize::MAX).unwrap();
        assert!(agg.next_batch().unwrap().is_none());
    }

    // The memory accountant must reject growth past the budget rather than
    // let the hash table grow unbounded — the whole point of taking a budget
    // in the constructor.
    #[test]
    fn exceeding_the_memory_budget_returns_out_of_memory_not_unbounded_growth() {
        let group_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
        ]));
        // Many distinct groups, each of which must allocate hash-table
        // state; a budget of a few bytes cannot hold more than one.
        let batch = RecordBatch::try_new(
            group_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())),
                Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>())),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(group_schema, vec![batch]);
        let mut agg = HashAggregate::new(input, vec![0], vec![sum_spec(1)], 8).unwrap();
        let err = agg.next_batch().unwrap_err();
        assert!(
            matches!(err, ExecError::OutOfMemory { .. }),
            "expected OutOfMemory, got {err:?}"
        );
    }

    // A rejection of a malformed spec (count(*) carrying an input column) at
    // construction time is a planner-bug guard, not user-facing behaviour —
    // but it must fail loudly rather than silently ignoring the column.
    #[test]
    fn count_star_with_an_input_column_is_rejected_at_construction() {
        let schema = schema_1int("x");
        let input = VecOperator::boxed(schema, vec![]);
        let bad = AggregateSpec {
            func: AggFunc::CountStar,
            input_col: Some(0),
            distinct: false,
            filter_col: None,
            alias: "n".into(),
        };
        let err = HashAggregate::new(input, vec![], vec![bad], usize::MAX)
            .err()
            .unwrap();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // array_agg's defining difference from every other aggregate in this
    // file: NULLs are elements of the result, not skipped. Verified against
    // a live PostgreSQL 18.2: `select array_agg(x) from (values
    // (1),(NULL),(3)) t(x)` -> `{1,NULL,3}`, a 3-element array.
    #[test]
    fn array_agg_includes_nulls_ungrouped() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(1), None, Some(3)]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![array_agg_spec(0, false)], usize::MAX).unwrap();

        // array_agg's output type is LIST — a different shape from the
        // scalar output every other aggregate in this file produces.
        assert!(
            matches!(agg.schema().field(0).data_type(), DataType::List(_)),
            "array_agg's resolved output type must be List, got {:?}",
            agg.schema().field(0).data_type()
        );

        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "ungrouped aggregate always emits one row"
        );
        assert_eq!(
            list_i64_values(out.column(0), 0),
            Some(vec![Some(1), None, Some(3)]),
            "array_agg must keep the NULL as an element, not drop it"
        );
    }

    // The mirror-image rule to sum/avg/min/max's own zero-row NULL (item 1),
    // but array_agg's is easy to get wrong in the *other* direction: a naive
    // "no rows accepted" case could plausibly return `{}` (the identity
    // element for concatenation) instead of NULL. Verified against a live
    // PostgreSQL 18.2: `select array_agg(id) from t where false` returns a
    // blank (NULL), not `{}`.
    #[test]
    fn array_agg_over_zero_rows_is_null_not_empty_array() {
        let schema = schema_1int("x");
        let input = VecOperator::boxed(schema.clone(), vec![]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![array_agg_spec(0, false)], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(
            list.is_null(0),
            "array_agg() over zero rows must be NULL, not an empty array"
        );
    }

    // string_agg's NULL rule is the opposite of array_agg's: NULLs are
    // skipped entirely (no element, no delimiter for that position) — the
    // same ignore-nulls rule sum/avg/min/max already follow. Also checks
    // that the delimiter is read per row (not a single constant): row 2's
    // NULL name contributes nothing, and the delimiter used before
    // appending a value is that value's *own* row's delimiter. Verified
    // against a live PostgreSQL 18.2 with the identical 4 rows: `select
    // string_agg(name, g) from t` (name = x,NULL,y,z; g = a,a,b,b) ->
    // `xbybz`.
    #[test]
    fn string_agg_skips_nulls_and_reads_a_per_row_delimiter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    None,
                    Some("y"),
                    Some("z"),
                ])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![string_agg_spec(0, 1, false)];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        assert_eq!(agg.schema().field(0).data_type(), &DataType::Utf8);
        let out = agg.next_batch().unwrap().unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "xbybz");
    }

    // Zero rows -> NULL, not `''` — verified against a live PostgreSQL
    // 18.2: `select string_agg(name, ',') from t where false` returns a
    // blank (NULL).
    #[test]
    fn string_agg_over_zero_rows_is_null_not_empty_string() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let input = VecOperator::boxed(schema, vec![]);
        let specs = vec![string_agg_spec(0, 1, false)];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            s.is_null(0),
            "string_agg() over zero rows must be NULL, not an empty string"
        );
    }

    // Every value NULL (rows exist, but none pass the ignore-nulls filter)
    // must also be NULL, not `''` — verified against a live PostgreSQL
    // 18.2: `select string_agg(name, ',') from t where name is null`
    // returns a blank (NULL).
    #[test]
    fn string_agg_all_null_values_is_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![Some(","), Some(",")])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![string_agg_spec(0, 1, false)];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(s.is_null(0));
    }

    // A NULL delimiter contributes no separator (it is not an error, and it
    // is not treated as an empty *value*). Verified against a live
    // PostgreSQL 18.2: `select string_agg(name, NULL) from t` (name =
    // x,NULL,y,z) -> `xyz` — the three non-null names concatenated with
    // nothing between them.
    #[test]
    fn string_agg_null_delimiter_produces_no_separator() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    None,
                    Some("y"),
                    Some("z"),
                ])),
                Arc::new(StringArray::from(vec![None::<&str>, None, None, None])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![string_agg_spec(0, 1, false)];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "xyz");
    }

    // Grouped path: array_agg must span batches within a group, include
    // NULLs, and keep each group's own elements separate from its
    // sibling's.
    #[test]
    fn array_agg_grouped_spans_batches_and_keeps_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("x", DataType::Int64, true),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![Some(1), Some(9), None])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![Some(3)])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![b1, b2]);
        let mut agg =
            HashAggregate::new(input, vec![0], vec![array_agg_spec(1, false)], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let groups = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows: Vec<(String, Option<Vec<Option<i64>>>)> = (0..out.num_rows())
            .map(|i| {
                (
                    groups.value(i).to_string(),
                    list_i64_values(out.column(1), i),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        // Insertion order is deterministic in this row-wise implementation
        // (not a contractual guarantee absent `ORDER BY` — see the module
        // doc) — pinned here as a regression check, not a claim about SQL
        // semantics.
        assert_eq!(
            rows[0],
            ("a".to_string(), Some(vec![Some(1), None, Some(3)]))
        );
        assert_eq!(rows[1], ("b".to_string(), Some(vec![Some(9)])));
    }

    // Grouped path: string_agg per group, still skipping NULLs and reading
    // a per-row delimiter.
    #[test]
    fn string_agg_grouped_keeps_groups_separate() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("p"),
                    None,
                    Some("q"),
                ])),
                Arc::new(StringArray::from(vec!["-", "-", "-", "-"])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![string_agg_spec(1, 2, false)];
        let mut agg = HashAggregate::new(input, vec![0], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let groups = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sa = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows: Vec<(String, String)> = (0..out.num_rows())
            .map(|i| (groups.value(i).to_string(), sa.value(i).to_string()))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        // group "a": only "x" survives (its second row's name is NULL).
        assert_eq!(rows[0], ("a".to_string(), "x".to_string()));
        // group "b": "p" then "q" joined by "-".
        assert_eq!(rows[1], ("b".to_string(), "p-q".to_string()));
    }

    // FILTER (WHERE …) applies per-aggregate, not to group membership (see
    // `filter_clause_applies_per_aggregate_not_to_the_group` above for the
    // scalar-aggregate version of this rule). For array_agg/string_agg this
    // has a sharp edge: a group can be non-empty (it exists because a
    // sibling aggregate, or the group-by column itself, saw rows) while
    // *this* aggregate's FILTER rejects every one of them — which must
    // still finalize to NULL, exactly like the true zero-row case.
    #[test]
    fn array_agg_and_string_agg_are_null_when_filter_excludes_every_row_in_a_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("x", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
            Field::new("keep", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(StringArray::from(vec![",", ","])),
                Arc::new(BooleanArray::from(vec![false, false])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![
            AggregateSpec {
                func: AggFunc::ArrayAgg,
                input_col: Some(1),
                distinct: false,
                filter_col: Some(4),
                alias: "aa".into(),
            },
            AggregateSpec {
                func: AggFunc::StringAgg { delim_col: 3 },
                input_col: Some(2),
                distinct: false,
                filter_col: Some(4),
                alias: "sa".into(),
            },
        ];
        let mut agg = HashAggregate::new(input, vec![0], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1, "the group itself still exists");
        assert_eq!(
            list_i64_values(out.column(1), 0),
            None,
            "array_agg must be NULL when FILTER rejects every row in the group"
        );
        let sa = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            sa.is_null(0),
            "string_agg must be NULL when FILTER rejects every row in the group"
        );
    }

    // array_agg(DISTINCT x) needs a different NULL rule than every other
    // DISTINCT aggregate in this file: it keeps a single NULL element rather
    // than dropping NULLs from the dedup set. Verified against a live
    // PostgreSQL 18.2: `select array_agg(distinct id) from (values
    // (1),(2),(1),(NULL),(2)) v(id)` -> `{1,2,NULL}`.
    #[test]
    fn array_agg_distinct_dedups_but_keeps_one_null() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(1), Some(2), Some(1), None, Some(2)]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![array_agg_spec(0, true)], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let got = list_i64_values(out.column(0), 0).unwrap();
        let mut non_null: Vec<i64> = got.iter().flatten().copied().collect();
        non_null.sort();
        assert_eq!(
            non_null,
            vec![1, 2],
            "distinct non-null values are {{1, 2}}"
        );
        assert_eq!(
            got.iter().filter(|v| v.is_none()).count(),
            1,
            "the NULL must be deduped to exactly one element, not zero (dropped) or two (kept as-is)"
        );
    }

    // string_agg(DISTINCT x, delim) dedups on the value and — unlike
    // array_agg(DISTINCT …) — drops NULLs entirely, matching string_agg's
    // own non-distinct ignore-nulls rule. Verified against a live
    // PostgreSQL 18.2: `select string_agg(distinct name, ',') from (values
    // ('x'),('y'),('x'),(NULL)) v(name)` -> `'x,y'`.
    #[test]
    fn string_agg_distinct_dedups_and_still_skips_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("y"),
                    Some("x"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![",", ",", ",", ","])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![string_agg_spec(0, 1, true)];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "x,y");
    }

    // The two tests above dedup correctly but say nothing about ORDER,
    // because their fixtures happen to arrive already sorted — which is
    // exactly how `array_agg(DISTINCT …)` and `string_agg(DISTINCT …)` shipped
    // returning arrival order while every test agreed with PostgreSQL. The
    // two below feed data whose arrival order is NOT its sorted order, so
    // they fail against an implementation that just appends.
    //
    // PostgreSQL sorts a DISTINCT aggregate's input; see `distinct_sort_cmp`
    // for the measurements and for NULL/NaN placement.

    /// Live PG 18.2:
    /// `select array_agg(distinct x) from (values (3),(1),(3),(null),(2)) v(x)`
    /// -> `{1,2,3,NULL}`.
    #[test]
    fn array_agg_distinct_returns_sorted_order_not_arrival_order() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(3), Some(1), Some(3), None, Some(2)]);
        let input = VecOperator::boxed(schema.clone(), vec![batch]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![array_agg_spec(0, true)], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            list_i64_values(out.column(0), 0).unwrap(),
            vec![Some(1), Some(2), Some(3), None],
            "array_agg(DISTINCT x) sorts ascending with NULL last"
        );
    }

    /// Live PG 18.2, both measured on the same server:
    ///
    /// ```text
    /// select string_agg(distinct s, ',')
    ///   from (values ('c'),('a'),('c'),(null),('b')) v(s);         -> a,b,c
    /// select string_agg(distinct s, d)
    ///   from (values ('c','-'),('a','+'),('b','|')) v(s,d);        -> a|b-c
    /// ```
    ///
    /// The second is the one that pins *which* delimiter survives the sort:
    /// each row keeps its own, so the delimiters follow the values into
    /// sorted order rather than staying where they arrived.
    #[test]
    fn string_agg_distinct_sorts_by_value_and_each_row_keeps_its_delimiter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let constant_delim = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("c"),
                    Some("a"),
                    Some("c"),
                    None,
                    Some("b"),
                ])),
                Arc::new(StringArray::from(vec![","; 5])),
            ],
        )
        .unwrap();
        let mut agg = HashAggregate::new(
            VecOperator::boxed(schema.clone(), vec![constant_delim]),
            vec![],
            vec![string_agg_spec(0, 1, true)],
            usize::MAX,
        )
        .unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a,b,c"
        );

        let per_row_delim = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("c"), Some("a"), Some("b")])),
                Arc::new(StringArray::from(vec!["-", "+", "|"])),
            ],
        )
        .unwrap();
        let mut agg = HashAggregate::new(
            VecOperator::boxed(schema, vec![per_row_delim]),
            vec![],
            vec![string_agg_spec(0, 1, true)],
            usize::MAX,
        )
        .unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a|b-c",
            "sorted a,b,c — and `b`'s own '|' and `c`'s own '-' came with them"
        );
    }

    // string_agg() requires text input — enforced at construction, the same
    // planner-bug-guard posture as `count_star_with_an_input_column_is_rejected_at_construction`.
    #[test]
    fn string_agg_over_non_text_column_is_rejected_at_construction() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let input = VecOperator::boxed(schema, vec![]);
        let err = HashAggregate::new(
            input,
            vec![],
            vec![string_agg_spec(0, 1, false)],
            usize::MAX,
        )
        .err()
        .unwrap();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // `array_agg(x ORDER BY y)` is real Postgres syntax this operator does
    // not implement (see the module doc and `AggFunc::ArrayAgg`'s doc). It
    // is refused, not silently reordered — but the refusal lives one layer
    // up: `AggregateSpec` (this file, owned here) structurally has no
    // `order_by` field for `ArrayAgg`/`StringAgg` to carry in the first
    // place, and `build.rs`'s `agg_spec` (which lowers `Expr::Aggregate`
    // into an `AggregateSpec`) already refuses a non-empty `order_by` on
    // *every* aggregate function unconditionally, before any
    // `AggregateSpec` is constructed:
    //
    // ```text
    // if !order_by.is_empty() {
    //     return Err(BuildError::Unsupported("ORDER BY inside an aggregate".into()));
    // }
    // ```
    //
    // So there is no runtime case for *this* file to test: an ordered
    // `array_agg` cannot reach `HashAggregate::new` at all, ordered or
    // silently reordered. This test pins that `AggregateSpec` construction
    // for `array_agg` only ever takes `func`/`input_col`/`distinct`/
    // `filter_col`/`alias` — if a future change added an `order_by` field
    // here without wiring a refusal, this call site would need updating,
    // which is the point.
    #[test]
    fn array_agg_spec_has_no_order_by_field_to_silently_ignore() {
        let spec = array_agg_spec(0, false);
        assert!(matches!(spec.func, AggFunc::ArrayAgg));
        // If this compiles, `AggregateSpec` has exactly the fields listed
        // above — Rust's struct-literal field list is itself the assertion.
    }

    // ── The statistical family ───────────────────────────────────────────
    //
    // Every expected value below was read off a live PostgreSQL 18.2
    // (`postgres://pc@127.0.0.1:5432/postgres`) and is quoted with the query
    // that produced it, so any of them can be re-run. `tests/
    // statistical_aggregates.rs` re-derives the same answers from the server
    // at test time over randomised data; these unit tests pin the specific
    // corners that are cheap to get wrong and expensive to notice.

    fn schema_1f64(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Float64, true)]))
    }

    fn f64_batch(schema: &SchemaRef, values: Vec<Option<f64>>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    fn agg1(func: AggFunc, col: usize, alias: &str) -> AggregateSpec {
        AggregateSpec {
            func,
            input_col: Some(col),
            distinct: false,
            filter_col: None,
            alias: alias.into(),
        }
    }

    /// Run an ungrouped aggregate over one `float8` column and return the
    /// single output row's value (or `None` for SQL NULL).
    fn run_f64_agg(values: Vec<Option<f64>>, func: AggFunc) -> Option<f64> {
        let schema = schema_1f64("x");
        let batches = vec![f64_batch(&schema, values)];
        let input = VecOperator::boxed(schema, batches);
        let mut agg =
            HashAggregate::new(input, vec![], vec![agg1(func, 0, "v")], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    // THE headline number for this whole family, and the reason a naive
    // implementation is not good enough. Postgres does not accumulate
    // `sum(x)`/`sum(x^2)`; it runs Youngs-Cramer (see the section comment on
    // `VarAcc`). All five spellings are pinned bit-for-bit — `assert_eq!` on
    // f64, not an epsilon — because an epsilon comparison here would pass for
    // every wrong-but-close algorithm this test exists to reject.
    //
    //   SELECT stddev(x)::text, stddev_samp(x)::text, stddev_pop(x)::text,
    //          var_samp(x)::text, var_pop(x)::text
    //     FROM (VALUES (1::float8),(2),(4),(NULL)) t(x);
    //   → 1.5275252316519468 | 1.5275252316519468 | 1.247219128924647
    //     | 2.3333333333333335 | 1.5555555555555556
    #[test]
    fn variance_family_matches_postgres_bit_for_bit_on_the_orphan_battery() {
        let data = || vec![Some(1.0), Some(2.0), Some(4.0), None];
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::StddevSamp)),
            Some(1.5275252316519468),
            "stddev/stddev_samp over {{1,2,4,NULL}}"
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::StddevPop)),
            Some(1.247219128924647),
            "stddev_pop over {{1,2,4,NULL}}"
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::VarSamp)),
            Some(2.3333333333333335),
            "var_samp over {{1,2,4,NULL}}"
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::VarPop)),
            Some(1.5555555555555556),
            "var_pop over {{1,2,4,NULL}}"
        );
    }

    // The discriminator between Youngs-Cramer and `sum(x^2) - sum(x)^2/n`.
    // With x ≈ 1e8, `sum(x^2)` ≈ 4e16 has an ULP of 8, so the naive
    // subtraction loses every significant digit of a variance of 30 — it
    // returns garbage on the order of 1e0..1e1, not 30.
    //
    //   SELECT var_samp(x)::text
    //     FROM (VALUES (1e8::float8+4),(1e8+7),(1e8+13),(1e8+16)) t(x);   → 30
    #[test]
    fn variance_survives_the_cancellation_dataset_that_defeats_sum_of_squares() {
        let base = 1e8;
        let data = vec![
            Some(base + 4.0),
            Some(base + 7.0),
            Some(base + 13.0),
            Some(base + 16.0),
        ];
        assert_eq!(
            run_f64_agg(data, AggFunc::Variance(VarKind::VarSamp)),
            Some(30.0),
            "var_samp of 1e8+{{4,7,13,16}} is exactly 30 on live PG 18.2; a \
             sum-of-squares accumulator loses every digit of it"
        );
    }

    // A second cancellation dataset where the true answer is *not* exactly
    // representable, so it also pins the exact rounding Postgres performs
    // rather than only "didn't catastrophically cancel". The true value is
    // 0.05/3 = 0.01666…; both Postgres and this operator report
    // 0.016666665176550587, the error coming from the inputs' own f64
    // quantisation.
    //
    //   SELECT var_samp(x)::text, var_pop(x)::text, stddev_samp(x)::text,
    //          stddev_pop(x)::text
    //     FROM (VALUES (100000000.0::float8),(100000000.1),(100000000.2),
    //                  (100000000.3)) t(x);
    #[test]
    fn variance_matches_postgres_on_a_dataset_whose_answer_is_not_exact() {
        let data = || {
            vec![
                Some(100000000.0),
                Some(100000000.1),
                Some(100000000.2),
                Some(100000000.3),
            ]
        };
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::VarSamp)),
            Some(0.016666665176550587)
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::VarPop)),
            Some(0.012499998882412941)
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::StddevSamp)),
            Some(0.12909943910238567)
        );
        assert_eq!(
            run_f64_agg(data(), AggFunc::Variance(VarKind::StddevPop)),
            Some(0.11180339387698811)
        );
    }

    // The population/sample split at N = 1 is the family's own version of
    // "SUM over zero rows is NULL, not 0" — and it is asymmetric, which is
    // what makes it easy to get wrong. Verified live:
    //
    //   SELECT stddev(x), stddev_pop(x), var_samp(x), var_pop(x)
    //     FROM (VALUES (5::float8)) t(x);          → NULL | 0 | NULL | 0
    //   ... same four over zero rows                → NULL | NULL | NULL | NULL
    #[test]
    fn variance_over_one_row_is_zero_for_pop_and_null_for_samp() {
        for (kind, want) in [
            (VarKind::StddevSamp, None),
            (VarKind::VarSamp, None),
            (VarKind::StddevPop, Some(0.0)),
            (VarKind::VarPop, Some(0.0)),
        ] {
            assert_eq!(
                run_f64_agg(vec![Some(5.0)], AggFunc::Variance(kind)),
                want,
                "{kind:?} over exactly one row"
            );
        }
    }

    #[test]
    fn variance_over_zero_non_null_rows_is_null_for_every_spelling() {
        for kind in [
            VarKind::StddevSamp,
            VarKind::StddevPop,
            VarKind::VarSamp,
            VarKind::VarPop,
        ] {
            assert_eq!(
                run_f64_agg(vec![], AggFunc::Variance(kind)),
                None,
                "{kind:?} over zero rows"
            );
            assert_eq!(
                run_f64_agg(vec![None, None], AggFunc::Variance(kind)),
                None,
                "{kind:?} over all-NULL rows"
            );
        }
    }

    // A non-finite input poisons the whole accumulator, including when it is
    // the *first* row (which is a separate branch in `VarAcc::push` and the
    // one an implementation is most likely to omit — leaving `Sxx` at 0 and
    // falsely reporting a population stddev of 0).
    //
    //   SELECT stddev(x) FROM (VALUES (1::float8),('Infinity'::float8)) t(x);  → NaN
    //   SELECT stddev_pop(x) FROM (VALUES ('Infinity'::float8)) t(x);          → NaN
    #[test]
    fn non_finite_input_makes_the_variance_family_nan_not_infinity_or_zero() {
        let got = run_f64_agg(
            vec![Some(1.0), Some(f64::INFINITY)],
            AggFunc::Variance(VarKind::StddevSamp),
        )
        .expect("two rows: stddev_samp is defined");
        assert!(
            got.is_nan(),
            "stddev over {{1, Infinity}} must be NaN, got {got}"
        );

        let got = run_f64_agg(
            vec![Some(f64::INFINITY)],
            AggFunc::Variance(VarKind::StddevPop),
        )
        .expect("one row: stddev_pop is defined");
        assert!(
            got.is_nan(),
            "stddev_pop over a lone Infinity must be NaN, not 0 — got {got}"
        );
    }

    // Integer input is a documented fidelity gap, not a silent substitution:
    // Postgres answers `stddev(int)` through exact numeric accumulation and
    // returns NUMERIC, this returns DOUBLE PRECISION through the float path.
    // Measured, not assumed — the two disagree in the last ULP:
    //
    //   SELECT stddev(x::float8) FROM (VALUES (1),(2),(4)) t(x);  → 1.5275252316519468
    //   SELECT stddev(x)         FROM (VALUES (1),(2),(4)) t(x);  → 1.5275252316519467
    //
    // This test pins the float answer this operator gives, so the gap stays
    // visible and a future numeric accumulator changes a *test*, not just
    // behaviour.
    #[test]
    fn variance_over_integers_takes_the_float_path_a_documented_fidelity_gap() {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, vec![Some(1), Some(2), Some(4)]);
        let input = VecOperator::boxed(schema, vec![batch]);
        let mut agg = HashAggregate::new(
            input,
            vec![],
            vec![agg1(AggFunc::Variance(VarKind::StddevSamp), 0, "s")],
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            agg.schema().field(0).data_type(),
            &DataType::Float64,
            "Postgres says numeric here; basin-exec has no arbitrary-precision decimal"
        );
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            col.value(0),
            1.5275252316519468,
            "the float8 answer; Postgres's numeric answer for the same integers \
             is 1.5275252316519467, one ULP away — the documented gap"
        );
    }

    // DISTINCT dedups before the accumulator sees anything, so a duplicated
    // value must not move the answer at all.
    //
    //   SELECT stddev(DISTINCT x) FROM (VALUES (1::float8),(1),(2),(4)) t(x);
    //   → 1.5275252316519468
    #[test]
    fn distinct_variance_dedups_before_accumulating() {
        let schema = schema_1f64("x");
        let batch = f64_batch(&schema, vec![Some(1.0), Some(1.0), Some(2.0), Some(4.0)]);
        let input = VecOperator::boxed(schema, vec![batch]);
        let spec = AggregateSpec {
            func: AggFunc::Variance(VarKind::StddevSamp),
            input_col: Some(0),
            distinct: true,
            filter_col: None,
            alias: "s".into(),
        };
        let mut agg = HashAggregate::new(input, vec![], vec![spec], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1.5275252316519468);
    }

    // ── bool_and / bool_or ───────────────────────────────────────────────

    fn run_bool_agg(values: Vec<Option<bool>>, func: AggFunc) -> Option<bool> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(BooleanArray::from(values))])
                .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![agg1(func, 0, "b")], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    //   SELECT bool_and(b), bool_or(b) FROM (VALUES (true),(false),(NULL)) t(b);
    //   → false | true
    //   ... (true),(true),(NULL)  → true  | true
    //   ... (false),(false)       → false | false
    #[test]
    fn bool_and_or_skip_nulls_and_agree_with_postgres() {
        for (data, and_want, or_want) in [
            (vec![Some(true), Some(false), None], Some(false), Some(true)),
            (vec![Some(true), Some(true), None], Some(true), Some(true)),
            (vec![Some(false), Some(false)], Some(false), Some(false)),
        ] {
            assert_eq!(
                run_bool_agg(data.clone(), AggFunc::BoolAnd),
                and_want,
                "bool_and over {data:?}"
            );
            assert_eq!(
                run_bool_agg(data.clone(), AggFunc::BoolOr),
                or_want,
                "bool_or over {data:?}"
            );
        }
    }

    // The identity-element trap, exactly the one `sum_of_empty_input_is_null_
    // not_zero` guards for SUM: `and` over nothing is *not* true and `or`
    // over nothing is *not* false. Verified live — both are a blank.
    #[test]
    fn bool_and_or_over_zero_non_null_rows_are_null_not_the_identity_element() {
        for func in [AggFunc::BoolAnd, AggFunc::BoolOr] {
            assert_eq!(run_bool_agg(vec![], func), None, "{func:?} over zero rows");
            assert_eq!(
                run_bool_agg(vec![None, None], func),
                None,
                "{func:?} over all-NULL rows"
            );
        }
    }

    // ── bit_and / bit_or / bit_xor ───────────────────────────────────────

    fn run_bit_agg(values: Vec<Option<i64>>, func: AggFunc) -> Option<i64> {
        let schema = schema_1int("x");
        let batch = int_batch(&schema, values);
        let input = VecOperator::boxed(schema, vec![batch]);
        let mut agg =
            HashAggregate::new(input, vec![], vec![agg1(func, 0, "b")], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    //   SELECT bit_and(x), bit_or(x), bit_xor(x)
    //     FROM (VALUES (12),(10),(NULL)) t(x);   → 8 | 14 | 6
    //   ... (-1),(6),(3)                          → 2 | -1 | -6
    //
    // The negative row matters: these are two's-complement bit operations on
    // a *signed* integer, so `bit_and(-1, 6, 3)` is 2 and `bit_or` is -1 —
    // an implementation that accumulated in an unsigned type or masked to the
    // input's width would disagree on both.
    #[test]
    fn bitwise_aggregates_skip_nulls_and_agree_with_postgres_including_negatives() {
        let data = vec![Some(12), Some(10), None];
        assert_eq!(run_bit_agg(data.clone(), AggFunc::BitAnd), Some(8));
        assert_eq!(run_bit_agg(data.clone(), AggFunc::BitOr), Some(14));
        assert_eq!(run_bit_agg(data, AggFunc::BitXor), Some(6));

        let neg = vec![Some(-1), Some(6), Some(3)];
        assert_eq!(run_bit_agg(neg.clone(), AggFunc::BitAnd), Some(2));
        assert_eq!(run_bit_agg(neg.clone(), AggFunc::BitOr), Some(-1));
        assert_eq!(run_bit_agg(neg, AggFunc::BitXor), Some(-6));
    }

    // Same identity-element trap as bool_and/bool_or, and worse for
    // `bit_and`, whose identity is all-ones (-1) — a plausible-looking wrong
    // answer rather than an obviously wrong one. Verified live: all three are
    // NULL over zero rows.
    #[test]
    fn bitwise_aggregates_over_zero_non_null_rows_are_null() {
        for func in [AggFunc::BitAnd, AggFunc::BitOr, AggFunc::BitXor] {
            assert_eq!(run_bit_agg(vec![], func), None, "{func:?} over zero rows");
            assert_eq!(
                run_bit_agg(vec![None], func),
                None,
                "{func:?} over an all-NULL column"
            );
        }
    }

    // bit_and/bit_or/bit_xor preserve the input's exact integer width — they
    // do not widen to bigint the way `sum` does. Verified live:
    // `pg_typeof(bit_and(x::int2))` is `smallint`.
    #[test]
    fn bitwise_aggregates_preserve_the_input_width() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int16, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int16Array::from(vec![
                Some(12i16),
                Some(10),
            ]))],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let mut agg = HashAggregate::new(
            input,
            vec![],
            vec![agg1(AggFunc::BitAnd, 0, "b")],
            usize::MAX,
        )
        .unwrap();
        assert_eq!(agg.schema().field(0).data_type(), &DataType::Int16);
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int16Array>()
            .unwrap();
        assert_eq!(col.value(0), 8);
    }

    // ── regr_* / corr / covar_* ──────────────────────────────────────────

    /// `(x, y)` pairs into a two-column batch, then one two-argument
    /// aggregate over it. Column 0 is `x`, column 1 is `y`; the spec reads
    /// `y` as its first argument, matching Postgres's `f(Y, X)` order.
    fn run_regr(pairs: Vec<(Option<f64>, Option<f64>)>, kind: RegrKind) -> Option<f64> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let xs: Vec<Option<f64>> = pairs.iter().map(|(x, _)| *x).collect();
        let ys: Vec<Option<f64>> = pairs.iter().map(|(_, y)| *y).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(xs)),
                Arc::new(Float64Array::from(ys)),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let spec = agg1(AggFunc::Regr { kind, x_col: 0 }, 1, "r");
        let mut agg = HashAggregate::new(input, vec![], vec![spec], usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        if kind == RegrKind::Count {
            let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            return Some(col.value(0) as f64);
        }
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    fn regr_battery() -> Vec<(Option<f64>, Option<f64>)> {
        vec![
            (Some(1.0), Some(10.0)),
            (Some(2.0), Some(20.0)),
            (Some(4.0), Some(25.0)),
            (None, None),
        ]
    }

    // All twelve members, pinned bit-for-bit against the live server over the
    // orphan battery's own dataset:
    //
    //   SELECT regr_count(y,x), regr_sxx(y,x), regr_syy(y,x), regr_sxy(y,x),
    //          regr_avgx(y,x), regr_avgy(y,x), regr_slope(y,x),
    //          regr_intercept(y,x), regr_r2(y,x), corr(y,x),
    //          covar_pop(y,x), covar_samp(y,x)
    //     FROM (VALUES (1::float8,10::float8),(2,20),(4,25),(NULL,NULL)) t(x,y);
    //
    // Note `regr_sxy` is 21.666666666666664 and not 21.666666666666668 — the
    // Youngs-Cramer accumulation's own rounding, not the mathematically
    // "nicer" value a two-pass formula produces.
    #[test]
    fn regr_family_matches_postgres_bit_for_bit_on_the_orphan_battery() {
        for (kind, want) in [
            (RegrKind::Count, 3.0),
            (RegrKind::Sxx, 4.666666666666666),
            (RegrKind::Syy, 116.66666666666666),
            (RegrKind::Sxy, 21.666666666666664),
            (RegrKind::AvgX, 2.3333333333333335),
            (RegrKind::AvgY, 18.333333333333332),
            (RegrKind::Slope, 4.642857142857143),
            (RegrKind::Intercept, 7.5),
            (RegrKind::R2, 0.8622448979591837),
            (RegrKind::Corr, 0.9285714285714285),
            (RegrKind::CovarPop, 7.222222222222221),
            (RegrKind::CovarSamp, 10.833333333333332),
        ] {
            assert_eq!(
                run_regr(regr_battery(), kind),
                Some(want),
                "{kind:?} over the orphan battery"
            );
        }
    }

    // The rule that separates this family from every other aggregate in this
    // file: a row is skipped unless BOTH arguments are non-NULL. An
    // implementation that applies the ordinary per-column null-skip counts 3
    // rows here and averages x over {1, 2, 4}.
    //
    //   SELECT regr_count(y,x), regr_avgx(y,x), regr_avgy(y,x)
    //     FROM (VALUES (1::float8,10::float8),(2,NULL),(NULL,30),(4,25)) t(x,y);
    //   → 2 | 2.5 | 17.5
    #[test]
    fn regr_skips_a_row_unless_both_arguments_are_non_null() {
        let data = vec![
            (Some(1.0), Some(10.0)),
            (Some(2.0), None),
            (None, Some(30.0)),
            (Some(4.0), Some(25.0)),
        ];
        assert_eq!(run_regr(data.clone(), RegrKind::Count), Some(2.0));
        assert_eq!(
            run_regr(data.clone(), RegrKind::AvgX),
            Some(2.5),
            "avg of {{1,4}}, not of {{1,2,4}}"
        );
        assert_eq!(run_regr(data, RegrKind::AvgY), Some(17.5));
    }

    // regr_count is the family's odd one out twice over: it is the only
    // bigint member and the only one that is 0 rather than NULL over zero
    // rows (its Postgres aggregate has a non-NULL `agginitval`, unlike the
    // other eleven). Verified live.
    #[test]
    fn regr_count_over_zero_rows_is_zero_while_every_sibling_is_null() {
        assert_eq!(run_regr(vec![], RegrKind::Count), Some(0.0));
        for kind in [
            RegrKind::Sxx,
            RegrKind::Syy,
            RegrKind::Sxy,
            RegrKind::AvgX,
            RegrKind::AvgY,
            RegrKind::Slope,
            RegrKind::Intercept,
            RegrKind::R2,
            RegrKind::Corr,
            RegrKind::CovarPop,
            RegrKind::CovarSamp,
        ] {
            assert_eq!(run_regr(vec![], kind), None, "{kind:?} over zero rows");
        }
    }

    // The N thresholds inside the family are not uniform, which is the whole
    // reason to check them one by one. Over exactly one row, live PG 18.2
    // gives:
    //
    //   regr_count=1, regr_sxx=0, regr_syy=0, regr_sxy=0, regr_avgx=2,
    //   regr_avgy=3, covar_pop=0, and NULL for regr_slope, regr_intercept,
    //   regr_r2, corr and covar_samp.
    #[test]
    fn regr_over_exactly_one_row_splits_into_defined_and_null_members() {
        let one = vec![(Some(2.0), Some(3.0))];
        for (kind, want) in [
            (RegrKind::Count, Some(1.0)),
            (RegrKind::Sxx, Some(0.0)),
            (RegrKind::Syy, Some(0.0)),
            (RegrKind::Sxy, Some(0.0)),
            (RegrKind::AvgX, Some(2.0)),
            (RegrKind::AvgY, Some(3.0)),
            (RegrKind::CovarPop, Some(0.0)),
            (RegrKind::Slope, None),
            (RegrKind::Intercept, None),
            (RegrKind::R2, None),
            (RegrKind::Corr, None),
            (RegrKind::CovarSamp, None),
        ] {
            assert_eq!(run_regr(one.clone(), kind), want, "{kind:?} over one row");
        }
    }

    // DISTINCT on a two-argument aggregate dedups the whole argument *list*,
    // not the first argument. Verified live:
    //
    //   SELECT regr_count(DISTINCT y, x), regr_avgx(DISTINCT y, x),
    //          regr_avgy(DISTINCT y, x)
    //     FROM (VALUES (1::float8,10::float8),(1,10),(2,20)) t(x,y);
    //   → 2 | 1.5 | 15
    #[test]
    fn distinct_on_a_two_argument_aggregate_dedups_the_whole_pair() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0), Some(1.0), Some(2.0)])),
                Arc::new(Float64Array::from(vec![Some(10.0), Some(10.0), Some(20.0)])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![
            AggregateSpec {
                func: AggFunc::Regr {
                    kind: RegrKind::Count,
                    x_col: 0,
                },
                input_col: Some(1),
                distinct: true,
                filter_col: None,
                alias: "n".into(),
            },
            AggregateSpec {
                func: AggFunc::Regr {
                    kind: RegrKind::AvgX,
                    x_col: 0,
                },
                input_col: Some(1),
                distinct: true,
                filter_col: None,
                alias: "ax".into(),
            },
            AggregateSpec {
                func: AggFunc::Regr {
                    kind: RegrKind::AvgY,
                    x_col: 0,
                },
                input_col: Some(1),
                distinct: true,
                filter_col: None,
                alias: "ay".into(),
            },
        ];
        let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2,
            "the duplicated (y,x) pair collapses"
        );
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.5
        );
        assert_eq!(
            out.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            15.0
        );
    }

    // The grouped path is a different code path from the ungrouped one (see
    // `build_grouped` vs. `update_ungrouped_one`), so every new family is
    // exercised through it too — including a group whose sample statistic is
    // NULL because it has a single row, next to one that is not.
    //
    //   SELECT g, stddev(x), var_pop(x), bool_or(x>1), bit_xor(x::int)
    //     FROM (VALUES (1,1::float8),(1,2),(1,4),(2,10),(2,NULL)) t(g,x)
    //    GROUP BY g ORDER BY g;
    //   → 1 | 1.5275252316519468 | 1.5555555555555556 | true | 7
    //     2 | NULL               | 0                  | true | 10
    #[test]
    fn statistical_aggregates_work_through_the_grouped_path_too() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("xi", DataType::Int64, true),
            Field::new("gt1", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1, 1, 2, 2])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(4.0),
                    Some(10.0),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(1i64),
                    Some(2),
                    Some(4),
                    Some(10),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(true),
                    None,
                ])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![
            agg1(AggFunc::Variance(VarKind::StddevSamp), 1, "sd"),
            agg1(AggFunc::Variance(VarKind::VarPop), 1, "vp"),
            agg1(AggFunc::BoolOr, 3, "bo"),
            agg1(AggFunc::BitXor, 2, "bx"),
        ];
        let mut agg = HashAggregate::new(input, vec![0], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);

        let g = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sd = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vp = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let bo = out
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let bx = out.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..2 {
            match g.value(row) {
                1 => {
                    assert_eq!(sd.value(row), 1.5275252316519468);
                    assert_eq!(vp.value(row), 1.5555555555555556);
                    assert!(bo.value(row));
                    assert_eq!(bx.value(row), 7, "1 ^ 2 ^ 4");
                }
                2 => {
                    assert!(sd.is_null(row), "one-row group: stddev_samp is NULL");
                    assert_eq!(vp.value(row), 0.0, "one-row group: var_pop is 0");
                    assert!(bo.value(row), "the NULL row is skipped, not false");
                    assert_eq!(bx.value(row), 10, "the NULL row contributes nothing");
                }
                other => panic!("unexpected group {other}"),
            }
        }
    }

    // The grouped path for the two-argument family specifically, which routes
    // through `push_regr` rather than `update_scalar`.
    //
    //   SELECT g, regr_slope(y,x), regr_count(y,x)
    //     FROM (VALUES (1,1::float8,10::float8),(1,2,20),(1,4,25),
    //                  (2,1,1),(2,2,5)) t(g,x,y)
    //    GROUP BY g ORDER BY g;   → 1 | 4.642857142857143 | 3
    //                               2 | 4                 | 2
    #[test]
    fn regr_works_through_the_grouped_path_too() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1, 1, 2, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0, 1.0, 2.0])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 25.0, 1.0, 5.0])),
            ],
        )
        .unwrap();
        let input = VecOperator::boxed(schema, vec![batch]);
        let specs = vec![
            agg1(
                AggFunc::Regr {
                    kind: RegrKind::Slope,
                    x_col: 1,
                },
                2,
                "slope",
            ),
            agg1(
                AggFunc::Regr {
                    kind: RegrKind::Count,
                    x_col: 1,
                },
                2,
                "n",
            ),
        ];
        let mut agg = HashAggregate::new(input, vec![0], specs, usize::MAX).unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let g = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let slope = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let n = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..out.num_rows() {
            match g.value(row) {
                1 => {
                    assert_eq!(slope.value(row), 4.642857142857143);
                    assert_eq!(n.value(row), 3);
                }
                2 => {
                    assert_eq!(slope.value(row), 4.0);
                    assert_eq!(n.value(row), 2);
                }
                other => panic!("unexpected group {other}"),
            }
        }
    }

    // Multi-batch input must give the same answer as single-batch input:
    // Youngs-Cramer is a sequential recurrence, so an operator that reset or
    // re-seeded state per batch would silently produce a different number.
    #[test]
    fn variance_is_identical_across_a_batch_boundary() {
        let schema = schema_1f64("x");
        let split = vec![
            f64_batch(&schema, vec![Some(1.0), Some(2.0)]),
            f64_batch(&schema, vec![Some(4.0), None]),
        ];
        let input = VecOperator::boxed(schema, split);
        let mut agg = HashAggregate::new(
            input,
            vec![],
            vec![agg1(AggFunc::Variance(VarKind::StddevSamp), 0, "s")],
            usize::MAX,
        )
        .unwrap();
        let out = agg.next_batch().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            col.value(0),
            1.5275252316519468,
            "two batches must give the same bits as one"
        );
    }

    // Type checking is construction-time for the new families too, matching
    // the planner-bug-guard posture the rest of this file takes.
    #[test]
    fn statistical_aggregates_reject_the_wrong_input_type_at_construction() {
        let text = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        for func in [
            AggFunc::Variance(VarKind::VarPop),
            AggFunc::BoolAnd,
            AggFunc::BitOr,
        ] {
            let input = VecOperator::boxed(text.clone(), vec![]);
            let err = HashAggregate::new(input, vec![], vec![agg1(func, 0, "v")], usize::MAX)
                .err()
                .unwrap_or_else(|| panic!("{func:?} over text must be rejected"));
            assert!(matches!(err, ExecError::TypeMismatch(_)), "{func:?}: {err}");
        }
        // bool_and over an integer column, and bit_and over a float one, are
        // the near-miss cases a numeric-family check would wave through.
        let ints = schema_1int("x");
        let input = VecOperator::boxed(ints.clone(), vec![]);
        assert!(matches!(
            HashAggregate::new(
                input,
                vec![],
                vec![agg1(AggFunc::BoolAnd, 0, "v")],
                usize::MAX
            )
            .err(),
            Some(ExecError::TypeMismatch(_))
        ));
        let floats = schema_1f64("x");
        let input = VecOperator::boxed(floats, vec![]);
        assert!(matches!(
            HashAggregate::new(
                input,
                vec![],
                vec![agg1(AggFunc::BitAnd, 0, "v")],
                usize::MAX
            )
            .err(),
            Some(ExecError::TypeMismatch(_))
        ));
    }

    // Both of `regr`'s columns are type-checked, not just the first — the
    // second arrives from the `AggFunc` payload rather than from
    // `AggregateSpec::input_col`, so it is the one an implementation forgets.
    #[test]
    fn regr_type_checks_its_second_argument_column_too() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let input = VecOperator::boxed(schema, vec![]);
        let spec = agg1(
            AggFunc::Regr {
                kind: RegrKind::Slope,
                x_col: 0,
            },
            1,
            "s",
        );
        let err = HashAggregate::new(input, vec![], vec![spec], usize::MAX)
            .err()
            .expect("a text second argument must be rejected");
        assert!(matches!(err, ExecError::TypeMismatch(_)), "{err}");
    }
}
