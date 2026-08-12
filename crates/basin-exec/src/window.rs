//! `WindowAgg` — the window-function operator (`OVER (...)`).
//!
//! # Precondition: the input MUST already be sorted
//!
//! **This operator does not sort.** It assumes the child stream arrives
//! ordered by `PARTITION BY` columns first, then `ORDER BY` columns — the
//! planner is responsible for inserting a [`crate::sort::Sort`] ahead of this
//! operator (mirroring DataFusion's `BoundedWindowAggExec mode=[Sorted]`,
//! see `docs/migration/df-removal/03-physical-operators.md` §3.3, and the
//! `catalog_window_exec.rs` rule that elides the sort when file order already
//! covers it). This is deliberate and load-bearing, not an oversight: if this
//! operator silently re-sorted on a mismatch, a planner bug that forgot the
//! `Sort` node would produce *plausible-looking wrong answers* instead of a
//! loud failure. It does not re-validate its precondition at runtime either,
//! for the same reason `Sort` doesn't re-check that its own output is sorted:
//! doing so would mask exactly the bug this contract exists to catch.
//!
//! # The four function families
//!
//! - **Ranking** (no frame, no argument): `row_number`, `rank`, `dense_rank`.
//! - **Offset** (no frame): `lag`, `lead`, with a per-row offset and default.
//! - **Aggregate-as-window** (frame-driven): `sum`, `count`, `min`, `max`, `avg`.
//! - **Value-at-position** (frame-driven): `first_value`, `last_value`,
//!   `nth_value`.
//!
//! # Semantics verified against a live PostgreSQL 18 server
//!
//! Per this crate's project convention, the tricky corners below were checked
//! against `psql -U postgres` rather than recalled, because recall has been
//! wrong before. Each item names the query used so the check is reproducible;
//! see the matching test for the same claim pinned as an assertion.
//!
//! 1. **The default frame is not "the whole partition".** With an `ORDER BY`
//!    and no explicit frame, the default is `RANGE BETWEEN UNBOUNDED
//!    PRECEDING AND CURRENT ROW` — verified with:
//!    ```sql
//!    select g,y,x, last_value(x) over (partition by g order by y) as lv_default,
//!           last_value(x) over (partition by g order by y
//!                                rows between unbounded preceding and current row) as lv_rows_curr
//!    from t order by y, x;
//!    ```
//!    Without an `ORDER BY` the default is the whole partition — verified
//!    with `select g,x, sum(x) over (partition by g) as s from t8`.
//! 2. **RANGE's `CURRENT ROW` bound means "through the end of the current
//!    peer group", not "through this physical row".** The query above shows
//!    `lv_default` (200, the *second* tied row's value) differs from
//!    `lv_rows_curr` (100, the physical row's own value) for the first of two
//!    `y=10` rows — a `ROWS`-shaped implementation of the RANGE default frame
//!    gets this wrong.
//! 3. **RANGE and ROWS diverge under ties even with an explicit numeric
//!    offset**, not just under the default frame — verified with:
//!    ```sql
//!    select y,x, sum(x) over (order by y rows between 1 preceding and current row) as rows_sum,
//!           sum(x) over (order by y range between 1 preceding and current row) as range_sum
//!    from tr order by y,x;  -- y=(1,1,2,4), x=(10,20,30,40)
//!    ```
//!    gives `rows_sum = (10,30,50,70)` but `range_sum = (30,30,60,40)`.
//! 4. **GROUPS counts peer groups, not rows** — verified with the `scores`
//!    fixture from `window_fns.rs`'s own `groups_1_preceding_current_row`
//!    test (`score = 10,10,20,20,30`): `GROUPS BETWEEN 1 PRECEDING AND
//!    CURRENT ROW` gives `(20,20,60,60,70)`, which is only reachable by
//!    stepping in peer-group units.
//! 5. **`rank` leaves gaps after ties; `dense_rank` does not; `row_number`
//!    never ties** — verified with `v = 10,10,20,30,30,40`:
//!    `rank = (1,1,3,4,4,6)`, `dense_rank = (1,1,2,3,3,4)`,
//!    `row_number = (1,2,3,4,5,6)`.
//! 6. **`lag`/`lead` past a partition edge yield the default argument (or
//!    NULL with none given), never a neighbouring partition's value** —
//!    verified with a two-partition fixture (`g=1: y=1,2`; `g=2: y=1,2`):
//!    `lag(x) over (partition by g order by y)` is NULL at each partition's
//!    first row, never the previous partition's last value.
//! 7. **Window aggregates ignore NULLs like plain aggregates do, and an empty
//!    frame yields NULL for SUM but 0 for COUNT** — verified with
//!    `ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING` on a partition's first row
//!    (no rows can possibly be 1-or-2-preceding it, so the frame is empty):
//!    `sum` is NULL, `count` is 0, `avg` is NULL.
//! 8. **A NULL `ORDER BY` value collapses a RANGE offset frame to just the
//!    NULL peer group**, independent of the requested offset — verified with
//!    `y = NULL,NULL,1,2`: `RANGE BETWEEN 1 PRECEDING AND CURRENT ROW` gives
//!    both NULL rows the same answer (the sum of the NULL peers only, `3`),
//!    not NULL (arithmetic against NULL) and not the whole partition.
//! 9. **RANGE's PRECEDING/FOLLOWING direction is relative to the `ORDER BY`
//!    direction, not to ascending value order** — verified with `y =
//!    10,12,15,20 DESC`: `RANGE BETWEEN 5 PRECEDING AND CURRENT ROW` widens
//!    *upward* in value (toward larger `y`), the mirror image of the
//!    ascending case. This operator handles it by comparing a *signed* order
//!    value (`-value` when the key is `DESC`), which is monotonically
//!    non-decreasing in row-arrival order regardless of direction — see
//!    [`resolve_range_frame_single_key`].
//!
//! # RANGE with a value/interval offset: exactly one ORDER BY column
//!
//! Postgres requires this (`ERROR: RANGE with offset PRECEDING/FOLLOWING
//! requires exactly one ORDER BY column`, confirmed live), and this operator
//! enforces it too — see [`validate_frame`]. `RANGE BETWEEN ... UNBOUNDED /
//! CURRENT ROW` (no explicit numeric offset) has no such restriction and
//! works with any number of `ORDER BY` columns, using tuple peer-group
//! equality exactly like `GROUPS` does.
//!
//! # What a RANGE offset means physically
//!
//! [`FrameOffset::Range`] is a plain `f64` already expressed in the `ORDER
//! BY` column's own comparable unit: a numeric difference for an integer or
//! float column, or **microseconds** for a `TIMESTAMP`/`TIMESTAMPTZ` column
//! (Basin's physical representation for both, per `basin_pgtype::physical`).
//! Resolving `INTERVAL '5 minutes'` into `300_000_000.0` microseconds is the
//! planner's job, mirroring `sort::SortKey`'s precedent of taking an
//! already-resolved physical value rather than an `Expr` — see that module's
//! docs for why. This was verified end-to-end against the `readings` fixture
//! from `window_fns.rs`'s `range_interval_5min_preceding_sum` test (timestamps
//! two minutes apart, `RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND
//! CURRENT ROW`): expected sums `(10,30,60,70,90)`, reproduced by
//! [`range_interval_five_minutes_preceding_matches_postgres`] below using
//! microsecond deltas as the physical offset unit.
//!
//! # What is out of scope
//!
//! `EXCLUDE` (`CURRENT ROW` / `GROUP` / `TIES` / `NO OTHERS`) has no
//! representation in [`WindowFrame`] at all — there is no variant to
//! populate, matching `03-physical-operators.md` §3.3's finding that no
//! passing test needs it. `IGNORE NULLS` (as opposed to Postgres's default
//! `RESPECT NULLS`) for `first_value`/`last_value`/`nth_value`/`lag`/`lead`
//! is likewise unimplemented — these functions return whatever raw value
//! (including NULL) sits at the resolved position, never skipping past it.
//!
//! # Memory
//!
//! Like [`crate::sort::Sort`] and [`crate::aggregate::HashAggregate`], this
//! operator is fully blocking: no output row is valid until the entire
//! partition it belongs to has been seen (an `UNBOUNDED FOLLOWING` bound, or
//! even just `last_value`, needs the partition's last row). Since a partition
//! boundary cannot be recognized without seeing the row that ends it, the
//! whole input is buffered — there is no cheaper alternative to being handed
//! a genuinely unbounded partition. `next_batch`'s buffering loop checks the
//! configured budget after every child batch and raises
//! [`ExecError::OutOfMemory`] rather than growing without bound; there is no
//! disk spill, for the same reasons `Sort` doesn't have one.
//!
//! # A known performance gap, named rather than hidden
//!
//! Frame resolution for `RANGE` uses a linear scan over the partition's order
//! values (not a binary search), and the frame-aggregate path (`sum`/`avg`/
//! `min`/`max`/`count`) re-runs an arrow kernel over the resolved `[start,
//! end]` slice for every output row rather than maintaining an incremental
//! sliding accumulator. Both are `O(partition size)` per row in the worst
//! case. This is orchestration-over-arrow-kernels in the same spirit as the
//! rest of this crate, correct but not asymptotically optimal; a sliding
//! accumulator is a reasonable follow-up once a real workload demands it.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::compute::kernels::aggregate as arrow_agg;
use arrow::compute::kernels::cast as arrow_cast;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

// ─────────────────────────────────────────────────────────────────────────
// Physical types
// ─────────────────────────────────────────────────────────────────────────

/// A `PARTITION BY` / `ORDER BY` key, resolved to a column position.
///
/// Deliberately not `basin_plan::expr::SortKey` — same reasoning as
/// `sort::SortKey`: this layer takes pre-resolved column positions, not
/// `Expr`. Field names echo both `sort::SortKey` and
/// `basin_plan::expr::SortKey` so wiring is a straight copy.
#[derive(Debug, Clone)]
pub struct OrderKey {
    pub column: usize,
    pub descending: bool,
    /// Carried for symmetry with `sort::SortKey` (whose `nulls_first` the
    /// upstream `Sort` this operator depends on actually consults) and to
    /// document the precondition, but not read by this file: every frame
    /// computation here only needs SQL NULLs to be *contiguous* within a
    /// partition, which holds for either placement, not which end they sit
    /// at — see [`null_peer_bounds`].
    pub nulls_first: bool,
}

/// Which of the three frame units is in play. Mirrors
/// `basin_plan::expr::FrameUnits` by name; see the module docs for how the
/// bound *payloads* differ (a resolved [`FrameOffset`] instead of a boxed
/// `Expr`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameUnits {
    Rows,
    Range,
    /// Counts peer groups (rows sharing equal `ORDER BY` values), not rows.
    Groups,
}

/// A resolved `n PRECEDING` / `n FOLLOWING` offset.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameOffset {
    /// `ROWS`/`GROUPS`: an exact row count / peer-group count.
    Count(u64),
    /// `RANGE`: an offset already expressed in the single `ORDER BY`
    /// column's own comparable unit — see the module docs' "What a RANGE
    /// offset means physically". Must be non-negative.
    Range(f64),
}

/// One frame bound. Mirrors `basin_plan::expr::FrameBound`'s shape, but
/// `Preceding`/`Following` carry a resolved [`FrameOffset`] rather than a
/// boxed `Expr` — the offset is a plan-time constant here, the same
/// resolved-not-evaluated posture `sort::SortKey` takes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(FrameOffset),
    CurrentRow,
    Following(FrameOffset),
    UnboundedFollowing,
}

/// A window frame. See the module docs' item 9 for why `PRECEDING`/
/// `FOLLOWING` are relative to the `ORDER BY` direction, not to ascending
/// value order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WindowFrame {
    pub units: FrameUnits,
    pub start: FrameBound,
    pub end: FrameBound,
}

/// The window functions this operator implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
    Sum,
    /// `count(expr)` — non-null values in the frame.
    Count,
    /// `count(*)` — rows in the frame, including all-null ones.
    CountStar,
    Min,
    Max,
    Avg,
    FirstValue,
    LastValue,
    NthValue,
}

/// One window expression in the `SELECT` list, resolved to column positions.
/// The physical-layer analogue of `basin_plan::expr::Expr::Window` — see
/// `aggregate::AggregateSpec`'s doc comment for why this crate resolves to
/// column indices rather than `Expr` at this layer.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub func: WindowFunc,
    /// The value argument: `sum(x)`, `lag(x)`, `first_value(x)`, … `None`
    /// for `row_number`/`rank`/`dense_rank`/`count(*)`, `Some` otherwise.
    pub arg_col: Option<usize>,
    /// `lag`/`lead`'s offset, evaluated per row (Postgres evaluates it "with
    /// respect to the current row", so it is a column, not a plan-time
    /// constant — a `Project` ahead of this operator materializes a literal
    /// offset into a broadcast column the same way `aggregate.rs` expects
    /// non-trivial aggregate arguments to already be materialized). `None`
    /// means the SQL-level default of 1.
    pub offset_col: Option<usize>,
    /// `lag`/`lead`'s default value when the offset lands outside the
    /// partition. `None` means SQL NULL.
    pub default_col: Option<usize>,
    /// `nth_value`'s 1-based position argument, evaluated per row.
    pub nth_col: Option<usize>,
    /// The frame for aggregate/value-at-position functions. `None` means
    /// "not specified in SQL" — resolved to Postgres's default (module docs
    /// item 1) rather than treated as "no frame". Ignored for ranking and
    /// offset functions, which are not frame-sensitive.
    pub frame: Option<WindowFrame>,
    /// Output column name.
    pub alias: String,
}

// ─────────────────────────────────────────────────────────────────────────
// Cell values — a small typed union for window outputs, mirroring
// aggregate.rs's CellValue. Kept local (not shared) to keep each physical
// operator file self-contained, the same tradeoff aggregate.rs and sort.rs
// already make independently.
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Cell {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
}

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

/// Types this operator's value-carrying functions (`lag`/`lead`/`min`/`max`/
/// `first_value`/`last_value`/`nth_value`) accept. Same set `aggregate.rs`
/// supports for `MIN`/`MAX` — a documented scope boundary, not an oversight;
/// notably this excludes `Timestamp`, which is supported only as an `ORDER
/// BY` column for `RANGE`, not as a value argument.
fn check_supported_cell_type(dt: &DataType) -> Result<(), ExecError> {
    let ok = num_kind_of(dt).is_some()
        || matches!(dt, DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean);
    if ok {
        Ok(())
    } else {
        Err(ExecError::TypeMismatch(format!(
            "window function over unsupported column type {dt:?}"
        )))
    }
}

fn extract_cell(array: &ArrayRef, row: usize) -> Result<Option<Cell>, ExecError> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Int16 => Cell::Int64(array.as_primitive::<Int16Type>().value(row) as i64),
        DataType::Int32 => Cell::Int64(array.as_primitive::<Int32Type>().value(row) as i64),
        DataType::Int64 => Cell::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::Float32 => Cell::Float64(array.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Cell::Float64(array.as_primitive::<Float64Type>().value(row)),
        DataType::Boolean => Cell::Bool(array.as_boolean().value(row)),
        DataType::Utf8 => Cell::Utf8(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Cell::Utf8(array.as_string::<i64>().value(row).to_string()),
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "window function: unsupported column type {other:?} (planner should have rejected \
                 this)"
            )));
        }
    }))
}

/// Extract row `row` of an `ORDER BY` column as an `f64`, in the column's own
/// comparable unit — plain numeric value, or microseconds for a timestamp.
/// Used only for `RANGE` frame arithmetic (see the module docs).
fn extract_order_f64(array: &ArrayRef, row: usize) -> Result<Option<f64>, ExecError> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row) as f64,
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row) as f64,
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row) as f64,
        DataType::Float32 => array.as_primitive::<Float32Type>().value(row) as f64,
        DataType::Float64 => array.as_primitive::<Float64Type>().value(row),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>().value(row) as f64
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "RANGE frame ORDER BY column has unsupported type {other:?} — expected numeric or \
                 a microsecond timestamp"
            )));
        }
    }))
}

/// Row-wise equality used for peer-group detection: two NULLs are peers
/// (equal), a NULL and a non-NULL are never peers. Built on
/// `arrow::compute::kernels::cmp::eq` (which is generic over every type arrow
/// can compare) rather than a per-type dispatch, so this works for `Utf8`,
/// `Boolean`, `Timestamp`, … `ORDER BY` columns alike, not just numerics.
fn peer_equal(array: &ArrayRef, a: usize, b: usize) -> Result<bool, ExecError> {
    let a_null = array.is_null(a);
    let b_null = array.is_null(b);
    if a_null || b_null {
        return Ok(a_null && b_null);
    }
    let sa = array.slice(a, 1);
    let sb = array.slice(b, 1);
    let eq = arrow::compute::kernels::cmp::eq(&sa, &sb).map_err(arrow_err)?;
    Ok(eq.value(0))
}

fn build_typed_array(dtype: &DataType, values: &[Option<Cell>]) -> Result<ArrayRef, ExecError> {
    fn as_i64(v: &Option<Cell>) -> Option<i64> {
        match v {
            Some(Cell::Int64(i)) => Some(*i),
            _ => None,
        }
    }
    fn as_f64(v: &Option<Cell>) -> Option<f64> {
        match v {
            Some(Cell::Float64(f)) => Some(*f),
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
                    Some(Cell::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        DataType::Utf8 | DataType::LargeUtf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| match v {
                    Some(Cell::Utf8(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        other => {
            return Err(ExecError::Internal(format!(
                "window operator: cannot build an output column of type {other:?}"
            )));
        }
    })
}

fn frame_extreme(
    arr: &ArrayRef,
    start: usize,
    len: usize,
    want_min: bool,
) -> Result<Option<Cell>, ExecError> {
    use arrow_agg::{max, max_boolean, max_string, min, min_boolean, min_string};
    let slice = arr.slice(start, len);
    let candidate: Option<Cell> = match slice.data_type() {
        DataType::Int16 => {
            let a = slice.as_primitive::<Int16Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| Cell::Int64(v as i64))
        }
        DataType::Int32 => {
            let a = slice.as_primitive::<Int32Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| Cell::Int64(v as i64))
        }
        DataType::Int64 => {
            let a = slice.as_primitive::<Int64Type>();
            (if want_min { min(a) } else { max(a) }).map(Cell::Int64)
        }
        DataType::Float32 => {
            let a = slice.as_primitive::<Float32Type>();
            (if want_min { min(a) } else { max(a) }).map(|v| Cell::Float64(v as f64))
        }
        DataType::Float64 => {
            let a = slice.as_primitive::<Float64Type>();
            (if want_min { min(a) } else { max(a) }).map(Cell::Float64)
        }
        DataType::Boolean => {
            let a = slice.as_boolean();
            (if want_min {
                min_boolean(a)
            } else {
                max_boolean(a)
            })
            .map(Cell::Bool)
        }
        DataType::Utf8 => {
            let a = slice.as_string::<i32>();
            (if want_min {
                min_string(a)
            } else {
                max_string(a)
            })
            .map(|s| Cell::Utf8(s.to_string()))
        }
        DataType::LargeUtf8 => {
            let a = slice.as_string::<i64>();
            (if want_min {
                min_string(a)
            } else {
                max_string(a)
            })
            .map(|s| Cell::Utf8(s.to_string()))
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "min()/max() OVER (...) over unsupported type {other:?}"
            )));
        }
    };
    Ok(candidate)
}

fn frame_sum(
    arr: &ArrayRef,
    num_kind: NumKind,
    start: usize,
    len: usize,
) -> Result<Option<Cell>, ExecError> {
    let slice = arr.slice(start, len);
    match num_kind {
        NumKind::Int => {
            let widened = arrow_cast::cast(&slice, &DataType::Int64)
                .map_err(|e| ExecError::Internal(e.to_string()))?;
            let typed = widened.as_primitive::<Int64Type>();
            let s = arrow_agg::sum_checked(typed).map_err(|_| ExecError::Overflow("bigint"))?;
            Ok(s.map(Cell::Int64))
        }
        NumKind::Float => {
            let widened = arrow_cast::cast(&slice, &DataType::Float64)
                .map_err(|e| ExecError::Internal(e.to_string()))?;
            let typed = widened.as_primitive::<Float64Type>();
            Ok(arrow_agg::sum(typed).map(Cell::Float64))
        }
    }
}

fn frame_avg(arr: &ArrayRef, start: usize, len: usize) -> Result<Option<Cell>, ExecError> {
    let slice = arr.slice(start, len);
    let widened = arrow_cast::cast(&slice, &DataType::Float64)
        .map_err(|e| ExecError::Internal(e.to_string()))?;
    let typed = widened.as_primitive::<Float64Type>();
    let sum = arrow_agg::sum(typed);
    let count = (slice.len() - slice.null_count()) as i64;
    Ok(match sum {
        Some(s) if count > 0 => Some(Cell::Float64(s / count as f64)),
        _ => None,
    })
}

// ─────────────────────────────────────────────────────────────────────────
// Construction / resolution
// ─────────────────────────────────────────────────────────────────────────

struct ResolvedWindow {
    spec: WindowSpec,
    /// Meaningful for `Sum` only (selects the widened output type / kernel);
    /// a harmless default (`Int`) otherwise — same convention as
    /// `aggregate::ResolvedAgg`.
    num_kind: NumKind,
    /// The frame to use, already defaulted per the module docs' item 1.
    /// `None` for ranking/offset functions, which never consult a frame.
    frame: Option<WindowFrame>,
}

fn require_arg(spec: &WindowSpec) -> Result<usize, ExecError> {
    spec.arg_col.ok_or_else(|| {
        ExecError::TypeMismatch(format!("{:?} requires an argument column", spec.func))
    })
}

/// Postgres's default frame (module docs item 1): the whole partition when
/// there is no `ORDER BY`, otherwise `RANGE UNBOUNDED PRECEDING AND CURRENT
/// ROW`.
fn default_frame(order_by_is_empty: bool) -> WindowFrame {
    if order_by_is_empty {
        WindowFrame {
            units: FrameUnits::Rows,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::UnboundedFollowing,
        }
    } else {
        WindowFrame {
            units: FrameUnits::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
        }
    }
}

/// Reject frame shapes this operator cannot execute: a `ROWS`/`GROUPS` bound
/// carrying a `RANGE`-shaped offset (or vice versa), and a `RANGE` bound with
/// a `PRECEDING`/`FOLLOWING` value offset when there isn't exactly one
/// `ORDER BY` column (Postgres's own restriction — see the module docs).
/// Reaching any of these is a planner bug, not user error.
fn validate_frame(frame: &WindowFrame, order_by: &[OrderKey]) -> Result<(), ExecError> {
    let check = |b: &FrameBound| -> Result<(), ExecError> {
        match (frame.units, b) {
            (FrameUnits::Rows | FrameUnits::Groups, FrameBound::Preceding(FrameOffset::Range(_)))
            | (FrameUnits::Rows | FrameUnits::Groups, FrameBound::Following(FrameOffset::Range(_))) => {
                Err(ExecError::TypeMismatch(
                    "ROWS/GROUPS frame bound must be a row/group count, not a RANGE value offset — \
                     a planner bug"
                        .into(),
                ))
            }
            (FrameUnits::Range, FrameBound::Preceding(FrameOffset::Count(_)))
            | (FrameUnits::Range, FrameBound::Following(FrameOffset::Count(_))) => {
                Err(ExecError::TypeMismatch(
                    "RANGE frame bound must be a value/interval offset, not a row count — a planner \
                     bug"
                        .into(),
                ))
            }
            (FrameUnits::Range, FrameBound::Preceding(FrameOffset::Range(_)))
            | (FrameUnits::Range, FrameBound::Following(FrameOffset::Range(_)))
                if order_by.len() != 1 =>
            {
                Err(ExecError::TypeMismatch(
                    "RANGE frame with a PRECEDING/FOLLOWING value offset requires exactly one \
                     ORDER BY column (confirmed against PostgreSQL 18, which raises exactly this \
                     error) — a planner bug"
                        .into(),
                ))
            }
            _ => Ok(()),
        }
    };
    check(&frame.start)?;
    check(&frame.end)
}

fn resolve_window(
    spec: WindowSpec,
    input_schema: &Schema,
    order_by: &[OrderKey],
) -> Result<(ResolvedWindow, Field), ExecError> {
    use WindowFunc::*;

    let needs_frame = matches!(
        spec.func,
        Sum | Count | CountStar | Min | Max | Avg | FirstValue | LastValue | NthValue
    );
    let frame = if needs_frame {
        let f = spec
            .frame
            .unwrap_or_else(|| default_frame(order_by.is_empty()));
        validate_frame(&f, order_by)?;
        Some(f)
    } else {
        None
    };

    match spec.func {
        RowNumber | Rank | DenseRank => {
            if spec.arg_col.is_some() {
                return Err(ExecError::TypeMismatch(format!(
                    "{:?} takes no argument column",
                    spec.func
                )));
            }
            let field = Field::new(spec.alias.as_str(), DataType::Int64, false);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        Lag | Lead => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            check_supported_cell_type(&dt)?;
            let field = Field::new(spec.alias.as_str(), dt, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        CountStar => {
            if spec.arg_col.is_some() {
                return Err(ExecError::TypeMismatch(
                    "count(*) OVER (...) must not carry an argument column".into(),
                ));
            }
            let field = Field::new(spec.alias.as_str(), DataType::Int64, false);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        Count => {
            require_arg(&spec)?;
            let field = Field::new(spec.alias.as_str(), DataType::Int64, false);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        Sum => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type();
            let num_kind = num_kind_of(dt).ok_or_else(|| {
                ExecError::TypeMismatch(format!("sum() OVER (...) over non-numeric column {dt:?}"))
            })?;
            let out_ty = match num_kind {
                NumKind::Int => DataType::Int64,
                NumKind::Float => DataType::Float64,
            };
            let field = Field::new(spec.alias.as_str(), out_ty, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind,
                    frame,
                },
                field,
            ))
        }
        Avg => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type();
            let num_kind = num_kind_of(dt).ok_or_else(|| {
                ExecError::TypeMismatch(format!("avg() OVER (...) over non-numeric column {dt:?}"))
            })?;
            let field = Field::new(spec.alias.as_str(), DataType::Float64, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind,
                    frame,
                },
                field,
            ))
        }
        Min | Max => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            check_supported_cell_type(&dt)?;
            let field = Field::new(spec.alias.as_str(), dt, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        FirstValue | LastValue => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            check_supported_cell_type(&dt)?;
            let field = Field::new(spec.alias.as_str(), dt, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
        NthValue => {
            let col = require_arg(&spec)?;
            let dt = input_schema.field(col).data_type().clone();
            check_supported_cell_type(&dt)?;
            if spec.nth_col.is_none() {
                return Err(ExecError::TypeMismatch(
                    "nth_value() requires a position argument column".into(),
                ));
            }
            let field = Field::new(spec.alias.as_str(), dt, true);
            Ok((
                ResolvedWindow {
                    spec,
                    num_kind: NumKind::Int,
                    frame,
                },
                field,
            ))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Partition / peer-group discovery
// ─────────────────────────────────────────────────────────────────────────

/// One partition: `[start, end)` row indices into the fully buffered input.
#[derive(Debug, Clone, Copy)]
struct Partition {
    start: usize,
    end: usize,
}

/// Peer-group boundaries within one partition. `id[i - start]` is the
/// 0-based peer group of absolute row `i`; `first[g]`/`last[g]` are the
/// absolute row indices bounding peer group `g`. With an empty `ORDER BY`
/// the whole partition is one peer group — see the module docs on the
/// default frame.
struct PeerGroups {
    id: Vec<u32>,
    first: Vec<usize>,
    last: Vec<usize>,
}

// ─────────────────────────────────────────────────────────────────────────
// The operator
// ─────────────────────────────────────────────────────────────────────────

const OUTPUT_BATCH_SIZE: usize = 8192;

/// The window-function operator. See the module docs for the sortedness
/// precondition, the default-frame surprise, and everything verified against
/// a live Postgres.
pub struct WindowAgg {
    child: Box<dyn Operator>,
    partition_by: Vec<usize>,
    order_by: Vec<OrderKey>,
    windows: Vec<ResolvedWindow>,
    input_ncols: usize,
    schema: SchemaRef,
    budget: usize,
    bytes_used: usize,
    output: Option<VecDeque<RecordBatch>>,
}

impl WindowAgg {
    /// `partition_by`/`order_by` are column positions in `child`'s output
    /// schema, and `child` MUST already be sorted by `partition_by` then
    /// `order_by` — see the module docs' precondition section. `windows` are
    /// every `OVER (...)` expression sharing this one `PARTITION BY`/`ORDER
    /// BY` pair; a query with multiple distinct `OVER` clauses needs one
    /// `WindowAgg` per clause, chained (mirroring how DataFusion groups
    /// window expressions that share a partitioning into one
    /// `BoundedWindowAggExec`). `memory_budget` bounds the fully-buffered
    /// input — see the module docs' Memory section.
    pub fn new(
        child: Box<dyn Operator>,
        partition_by: Vec<usize>,
        order_by: Vec<OrderKey>,
        windows: Vec<WindowSpec>,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        let input_schema = child.schema();
        let input_ncols = input_schema.fields().len();
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut resolved = Vec::with_capacity(windows.len());
        for spec in windows {
            let (rw, field) = resolve_window(spec, &input_schema, &order_by)?;
            fields.push(field);
            resolved.push(rw);
        }
        Ok(Self {
            child,
            partition_by,
            order_by,
            windows: resolved,
            input_ncols,
            schema: Arc::new(Schema::new(fields)),
            budget: memory_budget,
            bytes_used: 0,
            output: None,
        })
    }

    fn find_partitions(&self, batch: &RecordBatch) -> Result<Vec<Partition>, ExecError> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(Vec::new());
        }
        if self.partition_by.is_empty() {
            return Ok(vec![Partition { start: 0, end: n }]);
        }
        let cols: Vec<&ArrayRef> = self.partition_by.iter().map(|&c| batch.column(c)).collect();
        let mut parts = Vec::new();
        let mut start = 0usize;
        for i in 1..n {
            let mut same = true;
            for c in &cols {
                if !peer_equal(c, i - 1, i)? {
                    same = false;
                    break;
                }
            }
            if !same {
                parts.push(Partition { start, end: i });
                start = i;
            }
        }
        parts.push(Partition { start, end: n });
        Ok(parts)
    }

    fn compute_peer_groups(
        &self,
        batch: &RecordBatch,
        part: &Partition,
    ) -> Result<PeerGroups, ExecError> {
        let len = part.end - part.start;
        if self.order_by.is_empty() {
            return Ok(PeerGroups {
                id: vec![0; len],
                first: vec![part.start],
                last: vec![part.end - 1],
            });
        }
        let cols: Vec<&ArrayRef> = self
            .order_by
            .iter()
            .map(|k| batch.column(k.column))
            .collect();
        let mut id = Vec::with_capacity(len);
        let mut first = vec![part.start];
        let mut last = Vec::new();
        id.push(0u32);
        let mut cur = 0u32;
        for i in (part.start + 1)..part.end {
            let mut same = true;
            for c in &cols {
                if !peer_equal(c, i - 1, i)? {
                    same = false;
                    break;
                }
            }
            if !same {
                last.push(i - 1);
                first.push(i);
                cur += 1;
            }
            id.push(cur);
        }
        last.push(part.end - 1);
        Ok(PeerGroups { id, first, last })
    }

    /// The single `ORDER BY` column's per-row signed value, for `RANGE`
    /// frame arithmetic (module docs item 9: `DESC` flips the sign so the
    /// sequence stays non-decreasing in row-arrival order).
    fn extract_order_f64_signed(
        &self,
        batch: &RecordBatch,
        part: &Partition,
    ) -> Result<Vec<Option<f64>>, ExecError> {
        let key = &self.order_by[0];
        let col = batch.column(key.column);
        let sign = if key.descending { -1.0 } else { 1.0 };
        (part.start..part.end)
            .map(|i| Ok(extract_order_f64(col, i)?.map(|v| v * sign)))
            .collect()
    }

    fn build(&mut self) -> Result<(), ExecError> {
        let input_schema = self.child.schema();
        let mut buffered = Vec::new();
        let mut bytes = 0usize;
        while let Some(batch) = self.child.next_batch()? {
            let requested = bytes + batch.get_array_memory_size();
            if requested > self.budget {
                return Err(ExecError::OutOfMemory {
                    requested,
                    budget: self.budget,
                });
            }
            bytes = requested;
            buffered.push(batch);
        }
        self.bytes_used = bytes;

        if buffered.is_empty() {
            self.output = Some(VecDeque::new());
            return Ok(());
        }

        let combined = concat_batches(&input_schema, buffered.iter()).map_err(arrow_err)?;
        let n = combined.num_rows();
        let partitions = self.find_partitions(&combined)?;

        let need_order_f64 = self.order_by.len() == 1
            && self
                .windows
                .iter()
                .any(|w| matches!(w.frame.map(|f| f.units), Some(FrameUnits::Range)));

        let mut outputs: Vec<Vec<Option<Cell>>> =
            self.windows.iter().map(|_| vec![None; n]).collect();

        for part in &partitions {
            let peer = self.compute_peer_groups(&combined, part)?;
            let order_f64 = if need_order_f64 {
                Some(self.extract_order_f64_signed(&combined, part)?)
            } else {
                None
            };
            for (wi, rw) in self.windows.iter().enumerate() {
                compute_window_for_partition(
                    &combined,
                    part,
                    &peer,
                    order_f64.as_deref(),
                    rw,
                    &mut outputs[wi],
                )?;
            }
        }

        let mut batches = Vec::new();
        let mut start = 0;
        while start < n {
            let end = (start + OUTPUT_BATCH_SIZE).min(n);
            let mut arrays: Vec<ArrayRef> = (0..self.input_ncols)
                .map(|c| combined.column(c).slice(start, end - start))
                .collect();
            for (wi, _rw) in self.windows.iter().enumerate() {
                let dtype = self.schema.field(self.input_ncols + wi).data_type().clone();
                arrays.push(build_typed_array(&dtype, &outputs[wi][start..end])?);
            }
            batches.push(RecordBatch::try_new(self.schema.clone(), arrays).map_err(arrow_err)?);
            start = end;
        }
        self.output = Some(batches.into());
        Ok(())
    }
}

impl Operator for WindowAgg {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.output.is_none() {
            // Blocking, like Sort/HashAggregate — see the module docs'
            // Memory section for why a partial partition can never be
            // resolved early.
            self.build()?;
        }
        Ok(self.output.as_mut().and_then(VecDeque::pop_front))
    }

    fn memory_used(&self) -> usize {
        self.bytes_used
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Per-row computation
// ─────────────────────────────────────────────────────────────────────────

fn compute_window_for_partition(
    batch: &RecordBatch,
    part: &Partition,
    peer: &PeerGroups,
    order_f64: Option<&[Option<f64>]>,
    rw: &ResolvedWindow,
    out: &mut [Option<Cell>],
) -> Result<(), ExecError> {
    let out_part = &mut out[part.start..part.end];
    match rw.spec.func {
        WindowFunc::RowNumber => {
            for (offset, slot) in out_part.iter_mut().enumerate() {
                *slot = Some(Cell::Int64((offset + 1) as i64));
            }
        }
        WindowFunc::Rank => {
            for (offset, slot) in out_part.iter_mut().enumerate() {
                let g = peer.id[offset] as usize;
                let rank = (peer.first[g] - part.start + 1) as i64;
                *slot = Some(Cell::Int64(rank));
            }
        }
        WindowFunc::DenseRank => {
            for (offset, slot) in out_part.iter_mut().enumerate() {
                let g = peer.id[offset];
                *slot = Some(Cell::Int64((g + 1) as i64));
            }
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            compute_offset(batch, part, rw, out_part)?;
        }
        WindowFunc::Sum
        | WindowFunc::Count
        | WindowFunc::CountStar
        | WindowFunc::Min
        | WindowFunc::Max
        | WindowFunc::Avg
        | WindowFunc::FirstValue
        | WindowFunc::LastValue
        | WindowFunc::NthValue => {
            let frame = rw
                .frame
                .expect("resolved: aggregate/value-at-position functions always carry a frame");
            for (offset, slot) in out_part.iter_mut().enumerate() {
                let i = part.start + offset;
                let range = resolve_frame(part, peer, order_f64, &frame, i)?;
                *slot = apply_frame_func(batch, rw, i, range)?;
            }
        }
    }
    Ok(())
}

fn compute_offset(
    batch: &RecordBatch,
    part: &Partition,
    rw: &ResolvedWindow,
    out: &mut [Option<Cell>],
) -> Result<(), ExecError> {
    let arg = batch.column(
        rw.spec
            .arg_col
            .expect("resolved: lag/lead always carry an argument column"),
    );
    let offset_arr = rw.spec.offset_col.map(|c| batch.column(c));
    let default_arr = rw.spec.default_col.map(|c| batch.column(c));
    let sign: i64 = if rw.spec.func == WindowFunc::Lag {
        -1
    } else {
        1
    };

    for (rel, slot) in out.iter_mut().enumerate() {
        let i = part.start + rel;
        let offset = match offset_arr {
            Some(a) => match extract_cell(a, i)? {
                Some(Cell::Int64(n)) => n,
                Some(_) => {
                    return Err(ExecError::TypeMismatch(
                        "lag/lead offset must evaluate to an integer".into(),
                    ));
                }
                None => {
                    // A NULL offset makes the result NULL, mirroring Postgres.
                    *slot = None;
                    continue;
                }
            },
            None => 1,
        };
        let target = i as i64 + sign * offset;
        *slot = if target >= part.start as i64 && target < part.end as i64 {
            extract_cell(arg, target as usize)?
        } else {
            match default_arr {
                Some(a) => extract_cell(a, i)?,
                None => None,
            }
        };
    }
    Ok(())
}

/// A row-count/peer-group-count bound resolver shared by `ROWS` and
/// `GROUPS`: both work over an integer "position" space (row index or peer
/// group index) with the same clamp/empty rules, only the space and the
/// meaning of one unit differ.
///
/// `is_start` distinguishes the two sides because `PRECEDING` clamps to
/// `min_p` when it's the frame's start bound (as far back as the partition
/// goes is still a valid start) but signals an empty frame when it's the end
/// bound (an end before the partition start means nothing qualifies) — and
/// symmetrically for `FOLLOWING`.
fn resolve_bound_pos(
    bound: &FrameBound,
    is_start: bool,
    current: i64,
    min_p: i64,
    max_p: i64,
) -> Result<Option<i64>, ExecError> {
    Ok(match bound {
        FrameBound::UnboundedPreceding => Some(min_p),
        FrameBound::UnboundedFollowing => Some(max_p),
        FrameBound::CurrentRow => Some(current),
        FrameBound::Preceding(off) => {
            let n = frame_offset_count(off)?;
            let raw = current - n as i64;
            if is_start {
                Some(raw.max(min_p))
            } else if raw < min_p {
                None
            } else {
                Some(raw)
            }
        }
        FrameBound::Following(off) => {
            let n = frame_offset_count(off)?;
            let raw = current + n as i64;
            if !is_start {
                Some(raw.min(max_p))
            } else if raw > max_p {
                None
            } else {
                Some(raw)
            }
        }
    })
}

fn frame_offset_count(off: &FrameOffset) -> Result<u64, ExecError> {
    match off {
        FrameOffset::Count(n) => Ok(*n),
        FrameOffset::Range(_) => Err(ExecError::Internal(
            "ROWS/GROUPS frame requires a row/group count offset, not a RANGE value offset — a \
             planner bug (validate_frame should have rejected this at construction)"
                .into(),
        )),
    }
}

/// Resolve one row's frame to an inclusive `[start, end]` absolute row-index
/// range, or `None` if the frame is empty for this row (module docs item 7).
fn resolve_frame(
    part: &Partition,
    peer: &PeerGroups,
    order_f64: Option<&[Option<f64>]>,
    frame: &WindowFrame,
    i: usize,
) -> Result<Option<(usize, usize)>, ExecError> {
    match frame.units {
        FrameUnits::Rows => {
            let current = i as i64;
            let min_p = part.start as i64;
            let max_p = (part.end - 1) as i64;
            let s = resolve_bound_pos(&frame.start, true, current, min_p, max_p)?;
            let e = resolve_bound_pos(&frame.end, false, current, min_p, max_p)?;
            Ok(match (s, e) {
                (Some(s), Some(e)) if s <= e => Some((s as usize, e as usize)),
                _ => None,
            })
        }
        FrameUnits::Groups => {
            let g = peer.id[i - part.start] as i64;
            let min_g = 0i64;
            let max_g = (peer.first.len() - 1) as i64;
            let sg = resolve_bound_pos(&frame.start, true, g, min_g, max_g)?;
            let eg = resolve_bound_pos(&frame.end, false, g, min_g, max_g)?;
            Ok(match (sg, eg) {
                (Some(sg), Some(eg)) if sg <= eg => {
                    Some((peer.first[sg as usize], peer.last[eg as usize]))
                }
                _ => None,
            })
        }
        FrameUnits::Range => {
            if let Some(vals) = order_f64 {
                resolve_range_frame_single_key(part, vals, frame, i)
            } else {
                // Zero or multiple ORDER BY columns: validate_frame already
                // guarantees both bounds are Unbounded/CurrentRow here, so
                // this is exactly the tuple peer-group logic GROUPS uses.
                let g = peer.id[i - part.start] as i64;
                let min_g = 0i64;
                let max_g = (peer.first.len() - 1) as i64;
                let sg = resolve_bound_pos(&frame.start, true, g, min_g, max_g)?;
                let eg = resolve_bound_pos(&frame.end, false, g, min_g, max_g)?;
                Ok(match (sg, eg) {
                    (Some(sg), Some(eg)) if sg <= eg => {
                        Some((peer.first[sg as usize], peer.last[eg as usize]))
                    }
                    _ => None,
                })
            }
        }
    }
}

/// `RANGE` frame resolution for the exactly-one-`ORDER BY`-column case.
/// `vals` holds each partition row's *signed* order value (module docs item
/// 9) — monotonically non-decreasing in row-arrival order regardless of
/// `ASC`/`DESC`, with `None` for SQL NULL.
///
/// A NULL current-row value collapses the frame to just the contiguous NULL
/// peer block, independent of the bound kind (module docs item 8) — verified
/// against Postgres 18 with:
/// ```sql
/// select y,x, sum(x) over (order by y range between 1 preceding and current row) as s
/// from tn order by y nulls first, x;  -- y = NULL,NULL,1,2 (window's own order: 1,2,NULL,NULL)
/// ```
/// which gives both NULL rows the same sum (the NULL peers only), not NULL
/// (propagated from undefined `NULL ± offset` arithmetic) and not the whole
/// partition.
fn resolve_range_frame_single_key(
    part: &Partition,
    vals: &[Option<f64>],
    frame: &WindowFrame,
    i: usize,
) -> Result<Option<(usize, usize)>, ExecError> {
    let rel = i - part.start;
    let Some(cur) = vals[rel] else {
        return Ok(Some(null_peer_bounds(part, vals, rel)));
    };

    let threshold = |bound: &FrameBound| -> Result<f64, ExecError> {
        match bound {
            FrameBound::CurrentRow => Ok(cur),
            FrameBound::Preceding(FrameOffset::Range(o)) => Ok(cur - o),
            FrameBound::Following(FrameOffset::Range(o)) => Ok(cur + o),
            FrameBound::UnboundedPreceding | FrameBound::UnboundedFollowing => {
                unreachable!("caller only invokes threshold() for non-unbounded bounds")
            }
            FrameBound::Preceding(FrameOffset::Count(_))
            | FrameBound::Following(FrameOffset::Count(_)) => Err(ExecError::Internal(
                "RANGE frame requires a value/interval offset, not a row count — a planner bug"
                    .into(),
            )),
        }
    };

    let start_idx = match &frame.start {
        FrameBound::UnboundedPreceding => Some(part.start),
        b => find_first_sv_ge(part, vals, threshold(b)?),
    };
    let end_idx = match &frame.end {
        FrameBound::UnboundedFollowing => Some(part.end - 1),
        b => find_last_sv_le(part, vals, threshold(b)?),
    };
    Ok(match (start_idx, end_idx) {
        (Some(s), Some(e)) if s <= e => Some((s, e)),
        _ => None,
    })
}

fn find_first_sv_ge(part: &Partition, vals: &[Option<f64>], threshold: f64) -> Option<usize> {
    for (rel, v) in vals.iter().enumerate() {
        if let Some(v) = v {
            if *v >= threshold {
                return Some(part.start + rel);
            }
        }
    }
    None
}

fn find_last_sv_le(part: &Partition, vals: &[Option<f64>], threshold: f64) -> Option<usize> {
    for (rel, v) in vals.iter().enumerate().rev() {
        if let Some(v) = v {
            if *v <= threshold {
                return Some(part.start + rel);
            }
        }
    }
    None
}

/// The contiguous block of NULL order values containing `rel` (relative to
/// `part.start`). NULLs are always contiguous here because the input is
/// pre-sorted by a single key with one NULL placement.
fn null_peer_bounds(part: &Partition, vals: &[Option<f64>], rel: usize) -> (usize, usize) {
    let mut lo = rel;
    while lo > 0 && vals[lo - 1].is_none() {
        lo -= 1;
    }
    let mut hi = rel;
    while hi + 1 < vals.len() && vals[hi + 1].is_none() {
        hi += 1;
    }
    (part.start + lo, part.start + hi)
}

fn apply_frame_func(
    batch: &RecordBatch,
    rw: &ResolvedWindow,
    i: usize,
    range: Option<(usize, usize)>,
) -> Result<Option<Cell>, ExecError> {
    match rw.spec.func {
        WindowFunc::CountStar => Ok(Some(Cell::Int64(
            range.map(|(s, e)| (e - s + 1) as i64).unwrap_or(0),
        ))),
        WindowFunc::Count => {
            let arg = batch.column(
                rw.spec
                    .arg_col
                    .expect("resolved: count(expr) always carries an argument column"),
            );
            Ok(Some(Cell::Int64(match range {
                Some((s, e)) => {
                    let slice = arg.slice(s, e - s + 1);
                    (slice.len() - slice.null_count()) as i64
                }
                None => 0,
            })))
        }
        WindowFunc::Sum => match range {
            Some((s, e)) => frame_sum(
                batch.column(rw.spec.arg_col.expect("resolved")),
                rw.num_kind,
                s,
                e - s + 1,
            ),
            None => Ok(None),
        },
        WindowFunc::Avg => match range {
            Some((s, e)) => frame_avg(
                batch.column(rw.spec.arg_col.expect("resolved")),
                s,
                e - s + 1,
            ),
            None => Ok(None),
        },
        WindowFunc::Min => match range {
            Some((s, e)) => frame_extreme(
                batch.column(rw.spec.arg_col.expect("resolved")),
                s,
                e - s + 1,
                true,
            ),
            None => Ok(None),
        },
        WindowFunc::Max => match range {
            Some((s, e)) => frame_extreme(
                batch.column(rw.spec.arg_col.expect("resolved")),
                s,
                e - s + 1,
                false,
            ),
            None => Ok(None),
        },
        WindowFunc::FirstValue => match range {
            Some((s, _)) => extract_cell(batch.column(rw.spec.arg_col.expect("resolved")), s),
            None => Ok(None),
        },
        WindowFunc::LastValue => match range {
            Some((_, e)) => extract_cell(batch.column(rw.spec.arg_col.expect("resolved")), e),
            None => Ok(None),
        },
        WindowFunc::NthValue => {
            let Some((s, e)) = range else {
                return Ok(None);
            };
            let nth_col = batch.column(
                rw.spec
                    .nth_col
                    .expect("resolved: nth_value always carries a position column"),
            );
            let n = match extract_cell(nth_col, i)? {
                Some(Cell::Int64(n)) if n >= 1 => n,
                _ => return Ok(None),
            };
            let idx = s + (n as usize - 1);
            if idx > e {
                Ok(None)
            } else {
                extract_cell(batch.column(rw.spec.arg_col.expect("resolved")), idx)
            }
        }
        WindowFunc::RowNumber
        | WindowFunc::Rank
        | WindowFunc::DenseRank
        | WindowFunc::Lag
        | WindowFunc::Lead => {
            unreachable!("apply_frame_func is only called for frame-driven functions")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, TimestampMicrosecondArray};
    use arrow_schema::Field;
    use std::collections::VecDeque as Deque;

    // ── Fixtures ─────────────────────────────────────────────────────────

    /// A child operator that replays one fixed batch. Every test here feeds
    /// a single already-sorted batch — WindowAgg buffers everything before
    /// producing output anyway (see the module docs' Memory section), so a
    /// single-batch feed exercises the same code path a multi-batch one
    /// would, and keeps each fixture's "this is the required physical
    /// order" contract visible in one place.
    struct Feed {
        schema: SchemaRef,
        batches: Deque<RecordBatch>,
    }

    impl Feed {
        fn boxed(schema: SchemaRef, batch: RecordBatch) -> Box<dyn Operator> {
            Box::new(Feed {
                schema,
                batches: Deque::from([batch]),
            })
        }

        fn multi(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
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

    fn oc(column: usize, descending: bool) -> OrderKey {
        OrderKey {
            column,
            descending,
            nulls_first: !descending,
        }
    }

    fn i64_col(name: &str) -> Field {
        Field::new(name, DataType::Int64, true)
    }

    /// Run an operator to exhaustion and concatenate every output batch into
    /// one, so assertions don't need to know `WindowAgg`'s internal output
    /// chunking.
    fn run_all(op: &mut dyn Operator) -> RecordBatch {
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

    fn col_i64(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Int64"))
            .iter()
            .collect()
    }

    fn col_f64(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Float64"))
            .iter()
            .collect()
    }

    fn simple_frame(units: FrameUnits, start: FrameBound, end: FrameBound) -> WindowFrame {
        WindowFrame { units, start, end }
    }

    fn sum_spec(alias: &str, arg: usize, frame: Option<WindowFrame>) -> WindowSpec {
        WindowSpec {
            func: WindowFunc::Sum,
            arg_col: Some(arg),
            offset_col: None,
            default_col: None,
            nth_col: None,
            frame,
            alias: alias.into(),
        }
    }

    // ── Item 1: the default frame is NOT "the whole partition" ─────────────
    //
    // With an ORDER BY and no explicit frame, last_value must equal the
    // CURRENT ROW (for a partition with no ties), never the partition's last
    // value. Verified live: `select last_value(x) over (partition by g
    // order by y) from t` — see module docs item 1.
    #[test]
    fn default_frame_with_order_by_returns_current_row_not_the_whole_partition() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 10, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 100, 400, 900])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![WindowSpec {
            func: WindowFunc::LastValue,
            arg_col: Some(2),
            offset_col: None,
            default_col: None,
            nth_col: None,
            frame: None, // resolves to the Postgres default
            alias: "lv".into(),
        }];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        let lv = col_i64(&out, "lv");
        assert_eq!(
            lv,
            vec![
                Some(100),
                Some(200),
                Some(300),
                Some(100),
                Some(400),
                Some(900)
            ],
            "last_value must track the current row (no ties here), not jump to the \
             partition's final value — got {lv:?}"
        );
    }

    // ── Item 1 (continued) / item 2: RANGE's CURRENT ROW bound covers the
    // whole peer group, not just the physical row ─────────────────────────
    //
    // Verified live against a `g,y,x` fixture with ties on `y`:
    // `last_value` at the FIRST of two tied rows must equal the SECOND tied
    // row's value (200), which is neither the physical row's own value (100
    // — the wrong answer a ROWS-shaped implementation would give) nor the
    // partition's true last value (500 — the wrong answer a
    // whole-partition implementation would give).
    #[test]
    fn default_frame_range_current_row_includes_the_whole_peer_group() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![10, 10, 20, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::LastValue,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "lv".into(),
            },
            WindowSpec {
                func: WindowFunc::FirstValue,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "fv".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "lv"),
            vec![Some(200), Some(200), Some(400), Some(400), Some(500)],
            "last_value must extend through the tied peer, not stop at the physical row \
             (100,300 would be the ROWS-shaped wrong answer) and not jump to the \
             partition end (500 everywhere would be the whole-partition wrong answer)"
        );
        assert_eq!(
            col_i64(&out, "fv"),
            vec![Some(100); 5],
            "first_value's start bound is UNBOUNDED PRECEDING regardless of ties, so it \
             is constant across the whole partition"
        );
    }

    // Item 1: without an ORDER BY, the default frame really is the whole
    // partition. Verified live: `select sum(x) over (partition by g) from t8`.
    #[test]
    fn default_frame_without_order_by_is_the_whole_partition() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![sum_spec("s", 1, None)];
        let mut op = WindowAgg::new(child, vec![0], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(600), Some(600), Some(600)],
            "no ORDER BY means every row sees the whole partition"
        );
    }

    // ── Items 2/3: RANGE differs from ROWS whenever there are ties, even
    // with an explicit numeric offset (not just under the default frame) ──
    //
    // Verified live: `select sum(x) over (order by y rows between 1
    // preceding and current row), sum(x) over (order by y range between 1
    // preceding and current row) from tr` with y=(1,1,2,4), x=(10,20,30,40).
    #[test]
    fn range_and_rows_one_preceding_diverge_under_ties() {
        let schema = Arc::new(Schema::new(vec![i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 4])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let rows_frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::Preceding(FrameOffset::Count(1)),
            FrameBound::CurrentRow,
        );
        let range_frame = simple_frame(
            FrameUnits::Range,
            FrameBound::Preceding(FrameOffset::Range(1.0)),
            FrameBound::CurrentRow,
        );
        let windows = vec![
            sum_spec("rows_sum", 1, Some(rows_frame)),
            sum_spec("range_sum", 1, Some(range_frame)),
        ];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "rows_sum"),
            vec![Some(10), Some(30), Some(50), Some(70)],
            "ROWS counts physical rows back, oblivious to ties"
        );
        assert_eq!(
            col_i64(&out, "range_sum"),
            vec![Some(30), Some(30), Some(60), Some(40)],
            "RANGE must widen to cover the whole tied peer group at y=1 (giving both \
             y=1 rows the tied-pair sum 30, not the ROWS answer of 10 then 30) — a \
             ROWS-shaped RANGE implementation is the wrong answer this catches"
        );
    }

    // ── Item 3: GROUPS counts peer groups, not rows ─────────────────────────
    //
    // Verified live against window_fns.rs's own `scores` fixture
    // (score=10,10,20,20,30): GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW.
    #[test]
    fn groups_frame_counts_peer_groups_not_rows() {
        let schema = Arc::new(Schema::new(vec![i64_col("id"), i64_col("score")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 10, 20, 20, 30])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Groups,
            FrameBound::Preceding(FrameOffset::Count(1)),
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("gs", 1, Some(frame))];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "gs"),
            vec![Some(20), Some(20), Some(60), Some(60), Some(70)],
            "GROUPS 1 PRECEDING must step by peer group: a ROWS-shaped \
             implementation counting 1 physical row back would get id=3 wrong \
             (10+20=30 instead of the correct 10+10+20+20=60)"
        );
    }

    // GROUPS UNBOUNDED PRECEDING AND CURRENT ROW: cumulative by peer group.
    #[test]
    fn groups_unbounded_preceding_current_row_is_cumulative_by_peer_group() {
        let schema = Arc::new(Schema::new(vec![i64_col("id"), i64_col("score")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 10, 20, 20, 30])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Groups,
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("gs", 1, Some(frame))];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "gs"),
            vec![Some(20), Some(20), Some(60), Some(60), Some(90)]
        );
    }

    // ── Item 4: rank leaves gaps, dense_rank does not, row_number never ties ─
    //
    // Verified live: v=(10,10,20,30,30,40) -> rank=(1,1,3,4,4,6),
    // dense_rank=(1,1,2,3,3,4), row_number=(1,2,3,4,5,6).
    #[test]
    fn rank_dense_rank_and_row_number_disagree_on_ties() {
        let schema = Arc::new(Schema::new(vec![i64_col("v")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 10, 20, 30, 30, 40]))],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Rank,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "rnk".into(),
            },
            WindowSpec {
                func: WindowFunc::DenseRank,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "drnk".into(),
            },
            WindowSpec {
                func: WindowFunc::RowNumber,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "rn".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "rnk"),
            vec![Some(1), Some(1), Some(3), Some(4), Some(4), Some(6)],
            "rank must leave a gap after ties (jumping straight to 3, not 2)"
        );
        assert_eq!(
            col_i64(&out, "drnk"),
            vec![Some(1), Some(1), Some(2), Some(3), Some(3), Some(4)],
            "dense_rank must NOT leave a gap after ties"
        );
        assert_eq!(
            col_i64(&out, "rn"),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)],
            "row_number must never tie, even when every other ranking function does"
        );
    }

    // rank/dense_rank/row_number must reset at each partition boundary.
    #[test]
    fn ranking_functions_reset_at_each_partition() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("v")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![10, 10, 20, 5, 5])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Rank,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "rnk".into(),
            },
            WindowSpec {
                func: WindowFunc::RowNumber,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "rn".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "rnk"),
            vec![Some(1), Some(1), Some(3), Some(1), Some(1)],
            "the second partition's rank must restart at 1, not continue from the first"
        );
        assert_eq!(
            col_i64(&out, "rn"),
            vec![Some(1), Some(2), Some(3), Some(1), Some(2)]
        );
    }

    // ── Item 5: lag/lead past a partition edge never leak the neighbouring
    // partition's value ────────────────────────────────────────────────────
    //
    // Verified live: g=(1,1,2,2), y=(1,2,1,2), x=(100,200,300,400) ->
    // lag(x)=(NULL,100,NULL,300), lead(x)=(200,NULL,400,NULL).
    #[test]
    fn lag_lead_never_reach_across_a_partition_boundary() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Lag,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "lg".into(),
            },
            WindowSpec {
                func: WindowFunc::Lead,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "ld".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "lg"),
            vec![None, Some(100), None, Some(300)],
            "lag at each partition's first row must be NULL, never the previous \
             partition's last value (100 or 200 leaking across would be the wrong \
             answer this catches)"
        );
        assert_eq!(
            col_i64(&out, "ld"),
            vec![Some(200), None, Some(400), None],
            "lead at each partition's last row must be NULL, never the next \
             partition's first value"
        );
    }

    // lag/lead with an explicit default: the default (not NULL) is used at
    // the partition edge, and is itself per-row (a materialized column).
    #[test]
    fn lag_lead_use_the_explicit_default_at_the_partition_edge() {
        let schema = Arc::new(Schema::new(vec![
            i64_col("g"),
            i64_col("y"),
            i64_col("x"),
            i64_col("def"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(Int64Array::from(vec![-1, -1, -1, -1])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Lag,
                arg_col: Some(2),
                offset_col: None,
                default_col: Some(3),
                nth_col: None,
                frame: None,
                alias: "lg".into(),
            },
            WindowSpec {
                func: WindowFunc::Lead,
                arg_col: Some(2),
                offset_col: None,
                default_col: Some(3),
                nth_col: None,
                frame: None,
                alias: "ld".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "lg"),
            vec![Some(-1), Some(100), Some(-1), Some(300)]
        );
        assert_eq!(
            col_i64(&out, "ld"),
            vec![Some(200), Some(-1), Some(400), Some(-1)]
        );
    }

    // lag/lead's offset is itself a per-row column, not hard-wired to 1.
    #[test]
    fn lag_lead_honour_an_explicit_offset_column() {
        let schema = Arc::new(Schema::new(vec![i64_col("x"), i64_col("off")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
                Arc::new(Int64Array::from(vec![2, 2, 2, 2, 2])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Lag,
                arg_col: Some(0),
                offset_col: Some(1),
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "lg2".into(),
            },
            WindowSpec {
                func: WindowFunc::Lead,
                arg_col: Some(0),
                offset_col: Some(1),
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "ld2".into(),
            },
        ];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "lg2"),
            vec![None, None, Some(10), Some(20), Some(30)]
        );
        assert_eq!(
            col_i64(&out, "ld2"),
            vec![Some(30), Some(40), Some(50), None, None]
        );
    }

    // ── Item 6: window aggregates ignore NULLs the same as plain aggregates;
    // an empty frame is NULL for SUM but 0 for COUNT ────────────────────────
    //
    // Verified live: x=(10,NULL,30), ROWS BETWEEN UNBOUNDED PRECEDING AND
    // CURRENT ROW -> sum=(10,10,40), count=(1,1,2), avg=(10,10,20).
    #[test]
    fn window_aggregates_ignore_nulls_like_plain_aggregates() {
        let schema = Arc::new(Schema::new(vec![i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        );
        let windows = vec![
            sum_spec("s", 1, Some(frame)),
            WindowSpec {
                func: WindowFunc::Count,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "c".into(),
            },
            WindowSpec {
                func: WindowFunc::Avg,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "a".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(10), Some(10), Some(40)],
            "sum must skip the NULL, not treat it as 0 (which would give 10,10,40 \
             anyway here — the real trap is count/avg below)"
        );
        assert_eq!(
            col_i64(&out, "c"),
            vec![Some(1), Some(1), Some(2)],
            "count(x) must count only non-null values, not every row in the frame \
             (which would give 1,2,3)"
        );
        assert_eq!(col_f64(&out, "a"), vec![Some(10.0), Some(10.0), Some(20.0)]);
    }

    // Item 6 (continued): an empty frame — ROWS BETWEEN 2 PRECEDING AND 1
    // PRECEDING on a partition's first row can never have any member — is
    // NULL for SUM/AVG but 0 for COUNT. Verified live with the same clause.
    #[test]
    fn empty_frame_is_null_for_sum_but_zero_for_count() {
        let schema = Arc::new(Schema::new(vec![i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::Preceding(FrameOffset::Count(2)),
            FrameBound::Preceding(FrameOffset::Count(1)),
        );
        let windows = vec![
            sum_spec("s", 1, Some(frame)),
            WindowSpec {
                func: WindowFunc::Count,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "c".into(),
            },
            WindowSpec {
                func: WindowFunc::Avg,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "a".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![None, Some(10), Some(30)],
            "SUM over an empty frame (the first row has no 1-or-2-preceding \
             neighbour) must be NULL, not 0"
        );
        assert_eq!(
            col_i64(&out, "c"),
            vec![Some(0), Some(1), Some(2)],
            "COUNT over an empty frame must be 0, not NULL — the mirror-image bug"
        );
        assert_eq!(col_f64(&out, "a"), vec![None, Some(10.0), Some(15.0)]);
    }

    // ── Item 8 (module docs): a NULL ORDER BY value collapses a RANGE offset
    // frame to just the NULL peer group, independent of the offset ─────────
    //
    // Verified live: window's own physical order is y=1,y=2,then the two
    // NULLs (NULLS LAST is Postgres's ASC default) -> sums (3,7,3,3).
    #[test]
    fn null_order_value_collapses_the_range_frame_to_the_null_peer_group() {
        let schema = Arc::new(Schema::new(vec![i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, None])),
                Arc::new(Int64Array::from(vec![3, 4, 1, 2])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Range,
            FrameBound::Preceding(FrameOffset::Range(1.0)),
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("s", 1, Some(frame))];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(3), Some(7), Some(3), Some(3)],
            "both NULL rows must sum exactly their NULL peer group (1+2=3), not NULL \
             (from undefined NULL-offset arithmetic) and not the whole partition"
        );
    }

    // ── Item 9 (module docs): RANGE PRECEDING/FOLLOWING is relative to the
    // ORDER BY direction, not to ascending value order ──────────────────────
    //
    // Verified live: y=(20,15,12,10) DESC, x=(4,3,2,1) ->
    // RANGE BETWEEN 5 PRECEDING AND CURRENT ROW gives (4,7,5,6).
    #[test]
    fn range_preceding_direction_follows_order_by_desc() {
        let schema = Arc::new(Schema::new(vec![i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![20, 15, 12, 10])),
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Range,
            FrameBound::Preceding(FrameOffset::Range(5.0)),
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("s", 1, Some(frame))];
        let mut op = WindowAgg::new(child, vec![], vec![oc(0, true)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(4), Some(7), Some(5), Some(6)],
            "under DESC, PRECEDING must widen toward LARGER values (an implementation \
             that always subtracts the offset regardless of direction would instead \
             see every window as a singleton here, since consecutive values are 3-5 \
             apart and 'toward smaller' never finds a neighbour)"
        );
    }

    // ── RANGE with an INTERVAL offset on a TIMESTAMP ORDER BY column ────────
    //
    // Reproduces window_fns.rs's `range_interval_5min_preceding_sum` fixture:
    // readings at 0,2,4,8,10 minutes, x=10,20,30,40,50, RANGE BETWEEN
    // INTERVAL '5 minutes' PRECEDING AND CURRENT ROW -> (10,30,60,70,90).
    // The physical offset is 300_000_000.0 microseconds (see module docs'
    // "What a RANGE offset means physically").
    #[test]
    fn range_interval_five_minutes_preceding_matches_postgres() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            i64_col("x"),
        ]));
        let minute = 60_000_000i64;
        let ts = vec![0, 2 * minute, 4 * minute, 8 * minute, 10 * minute];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts)),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Range,
            FrameBound::Preceding(FrameOffset::Range(5.0 * minute as f64)),
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("window_sum", 1, Some(frame))];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "window_sum"),
            vec![Some(10), Some(30), Some(60), Some(70), Some(90)]
        );
    }

    // ── General frame-clause coverage (mirrors window_fns.rs) ──────────────

    #[test]
    fn rows_unbounded_preceding_current_row_is_a_running_sum_even_with_ties() {
        let schema = Arc::new(Schema::new(vec![i64_col("amount")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                100, 100, 200, 300, 400, 400,
            ]))],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("running_sum", 0, Some(frame))];
        let mut op =
            WindowAgg::new(child, vec![], vec![oc(0, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "running_sum"),
            vec![
                Some(100),
                Some(200),
                Some(400),
                Some(700),
                Some(1100),
                Some(1500)
            ]
        );
    }

    #[test]
    fn range_unbounded_both_directions_is_the_partition_total_with_no_order_by() {
        let schema = Arc::new(Schema::new(vec![i64_col("dept"), i64_col("amount")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 100, 400, 400])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Range,
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );
        let windows = vec![sum_spec("total", 1, Some(frame))];
        // No ORDER BY: RANGE UNBOUNDED..UNBOUNDED with zero ORDER BY columns
        // exercises the multi/zero-column peer-group path directly.
        let mut op = WindowAgg::new(child, vec![0], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "total"),
            vec![
                Some(600),
                Some(600),
                Some(600),
                Some(900),
                Some(900),
                Some(900)
            ]
        );
    }

    #[test]
    fn rows_sliding_window_clamps_at_partition_edges() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40]))],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::Preceding(FrameOffset::Count(1)),
            FrameBound::Following(FrameOffset::Count(1)),
        );
        let windows = vec![sum_spec("s", 0, Some(frame))];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(30), Some(60), Some(90), Some(70)],
            "the first/last row's window must clamp to the partition edge rather than \
             going empty or out of bounds"
        );
    }

    // ── first_value / last_value / nth_value ────────────────────────────────

    #[test]
    fn first_last_and_nth_value_over_the_full_partition() {
        // Column 2 ("n") is nth_value's position argument, a per-row column
        // per this operator's convention (see WindowSpec::nth_col's doc
        // comment) — here a broadcast constant 2.
        let schema = Arc::new(Schema::new(vec![
            i64_col("dept"),
            i64_col("amount"),
            i64_col("n"),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 100, 400, 400])),
                Arc::new(Int64Array::from(vec![2, 2, 2, 2, 2, 2])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );
        let windows = vec![
            WindowSpec {
                func: WindowFunc::FirstValue,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "fv".into(),
            },
            WindowSpec {
                func: WindowFunc::LastValue,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "lv".into(),
            },
            WindowSpec {
                func: WindowFunc::NthValue,
                arg_col: Some(1),
                offset_col: None,
                default_col: None,
                nth_col: Some(2),
                frame: Some(frame),
                alias: "nv".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "fv"),
            vec![
                Some(100),
                Some(100),
                Some(100),
                Some(100),
                Some(100),
                Some(100)
            ]
        );
        assert_eq!(
            col_i64(&out, "lv"),
            vec![
                Some(300),
                Some(300),
                Some(300),
                Some(400),
                Some(400),
                Some(400)
            ]
        );
        assert_eq!(
            col_i64(&out, "nv"),
            vec![
                Some(200),
                Some(200),
                Some(200),
                Some(400),
                Some(400),
                Some(400)
            ],
            "nth_value(amount, 2): dept 1's 2nd-smallest is 200, dept 2's is 400"
        );
    }

    #[test]
    fn nth_value_is_null_when_the_position_exceeds_the_frame() {
        let schema = Arc::new(Schema::new(vec![i64_col("x"), i64_col("n")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(Int64Array::from(vec![10, 10, 10, 10])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );
        let windows = vec![WindowSpec {
            func: WindowFunc::NthValue,
            arg_col: Some(0),
            offset_col: None,
            default_col: None,
            nth_col: Some(1),
            frame: Some(frame),
            alias: "nv".into(),
        }];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(col_i64(&out, "nv"), vec![None; 4]);
    }

    // ── count(*) vs count(expr) in a frame with NULLs ───────────────────────

    #[test]
    fn count_star_counts_rows_count_expr_counts_non_null_values() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let frame = simple_frame(
            FrameUnits::Rows,
            FrameBound::UnboundedPreceding,
            FrameBound::UnboundedFollowing,
        );
        let windows = vec![
            WindowSpec {
                func: WindowFunc::CountStar,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "cs".into(),
            },
            WindowSpec {
                func: WindowFunc::Count,
                arg_col: Some(0),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: Some(frame),
                alias: "ce".into(),
            },
        ];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(col_i64(&out, "cs"), vec![Some(3), Some(3), Some(3)]);
        assert_eq!(col_i64(&out, "ce"), vec![Some(2), Some(2), Some(2)]);
    }

    // ── Multi-partition and single-row-partition coverage ──────────────────

    #[test]
    fn aggregates_do_not_leak_across_partitions() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 100, 200, 300])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![sum_spec("s", 1, None)]; // no ORDER BY -> whole-partition default
        let mut op = WindowAgg::new(child, vec![0], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(30), Some(30), Some(600), Some(600), Some(600)]
        );
    }

    #[test]
    fn single_row_partitions_behave_sanely_for_every_function_family() {
        let schema = Arc::new(Schema::new(vec![i64_col("g"), i64_col("y"), i64_col("x")]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let child = Feed::boxed(schema, batch);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::RowNumber,
                arg_col: None,
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "rn".into(),
            },
            WindowSpec {
                func: WindowFunc::Lag,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "lg".into(),
            },
            sum_spec("s", 2, None), // default frame: current row only
            WindowSpec {
                func: WindowFunc::FirstValue,
                arg_col: Some(2),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "fv".into(),
            },
        ];
        let mut op =
            WindowAgg::new(child, vec![0], vec![oc(1, false)], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        assert_eq!(col_i64(&out, "rn"), vec![Some(1), Some(1), Some(1)]);
        assert_eq!(col_i64(&out, "lg"), vec![None, None, None]);
        assert_eq!(
            col_i64(&out, "s"),
            vec![Some(100), Some(200), Some(300)],
            "a single-row partition's default frame is just that row"
        );
        assert_eq!(col_i64(&out, "fv"), vec![Some(100), Some(200), Some(300)]);
    }

    #[test]
    fn zero_rows_of_input_produce_zero_rows_of_output() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema.clone()));
        let windows = vec![sum_spec("s", 0, None)];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        assert!(
            op.next_batch().unwrap().is_none(),
            "no input rows means no output rows, not a phantom single row"
        );
    }

    // ── Memory contract ──────────────────────────────────────────────────

    #[test]
    fn exceeding_the_memory_budget_returns_out_of_memory() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let big = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>()))],
        )
        .unwrap();
        let size = big.get_array_memory_size();
        let child = Feed::boxed(schema, big);
        let windows = vec![sum_spec("s", 0, None)];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, size - 1).unwrap();
        let err = op.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }

    #[test]
    fn memory_used_is_zero_before_the_first_next_batch_call() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema));
        let windows = vec![sum_spec("s", 0, None)];
        let op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        assert_eq!(op.memory_used(), 0);
    }

    // ── Construction-time validation (planner-bug guards) ───────────────────

    #[test]
    fn range_with_a_value_offset_and_multiple_order_by_columns_is_rejected() {
        let schema = Arc::new(Schema::new(vec![i64_col("a"), i64_col("b"), i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema));
        let frame = simple_frame(
            FrameUnits::Range,
            FrameBound::Preceding(FrameOffset::Range(5.0)),
            FrameBound::CurrentRow,
        );
        let windows = vec![sum_spec("s", 2, Some(frame))];
        let err = WindowAgg::new(
            child,
            vec![],
            vec![oc(0, false), oc(1, false)],
            windows,
            usize::MAX,
        )
        .err()
        .unwrap();
        assert!(
            matches!(err, ExecError::TypeMismatch(_)),
            "RANGE with a PRECEDING/FOLLOWING value offset over more than one ORDER BY \
             column must be rejected (confirmed live: PostgreSQL 18 raises exactly this \
             error), got {err:?}"
        );
    }

    #[test]
    fn row_number_with_an_argument_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema));
        let windows = vec![WindowSpec {
            func: WindowFunc::RowNumber,
            arg_col: Some(0),
            offset_col: None,
            default_col: None,
            nth_col: None,
            frame: None,
            alias: "rn".into(),
        }];
        let err = WindowAgg::new(child, vec![], vec![], windows, usize::MAX)
            .err()
            .unwrap();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn count_star_with_an_argument_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema));
        let windows = vec![WindowSpec {
            func: WindowFunc::CountStar,
            arg_col: Some(0),
            offset_col: None,
            default_col: None,
            nth_col: None,
            frame: None,
            alias: "cs".into(),
        }];
        let err = WindowAgg::new(child, vec![], vec![], windows, usize::MAX)
            .err()
            .unwrap();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn nth_value_without_a_position_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![i64_col("x")]));
        let child = Feed::boxed(schema.clone(), RecordBatch::new_empty(schema));
        let windows = vec![WindowSpec {
            func: WindowFunc::NthValue,
            arg_col: Some(0),
            offset_col: None,
            default_col: None,
            nth_col: None,
            frame: None,
            alias: "nv".into(),
        }];
        let err = WindowAgg::new(child, vec![], vec![], windows, usize::MAX)
            .err()
            .unwrap();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    // ── min/max also work over a non-numeric (string) column ────────────────

    #[test]
    fn min_and_max_work_over_a_string_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec![
                "b", "a", "c",
            ]))],
        )
        .unwrap();
        let child = Feed::multi(schema, vec![batch]);
        let windows = vec![
            WindowSpec {
                func: WindowFunc::Min,
                arg_col: Some(0),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "mn".into(),
            },
            WindowSpec {
                func: WindowFunc::Max,
                arg_col: Some(0),
                offset_col: None,
                default_col: None,
                nth_col: None,
                frame: None,
                alias: "mx".into(),
            },
        ];
        let mut op = WindowAgg::new(child, vec![], vec![], windows, usize::MAX).unwrap();
        let out = run_all(&mut op);
        let mn = out
            .column_by_name("mn")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mx = out
            .column_by_name("mx")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..3 {
            assert_eq!(mn.value(i), "a");
            assert_eq!(mx.value(i), "c");
        }
    }
}
