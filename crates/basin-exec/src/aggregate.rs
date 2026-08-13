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

/// The eight aggregate functions this operator implements.
///
/// This is deliberately not the full `pg_proc` aggregate surface — no
/// ordered-set aggregates or `GROUPING SETS` — see
/// `docs/migration/df-removal/03-physical-operators.md` §3.2 for what else
/// basin-exec eventually needs; those are separate operators/specs.
/// `array_agg`/`string_agg` were the last of the "ordinary" aggregates
/// missing; both are row-wise only (see the module doc) — a vectorised
/// group-wise variant was prototyped on `spike/vectorised-aggregate` and
/// measured at 0.60x the row-wise loop, so it was shelved rather than
/// merged (`docs/migration/df-removal/17-udf-rehosting.md`).
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
            _ => 0,
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
    ArrayAgg(Option<Vec<Option<CellValue>>>),
    /// `string_agg(expr, delimiter)`. `None` until the first *non-null*
    /// value is accepted and stays `None` forever if none ever is (NULLs
    /// are skipped entirely, unlike `ArrayAgg` — see
    /// [`AccState::push_string_agg`]).
    StringAgg(Option<String>),
}

impl AccState {
    fn new(func: AggFunc, num_kind: NumKind) -> Self {
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
            AggFunc::ArrayAgg => AccState::ArrayAgg(None),
            AggFunc::StringAgg { .. } => AccState::StringAgg(None),
        }
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
        let AccState::ArrayAgg(list) = self else {
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
    fn push_string_agg(
        &mut self,
        val: &Option<CellValue>,
        delim: &Option<CellValue>,
    ) -> Result<usize, ExecError> {
        let AccState::StringAgg(acc) = self else {
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
        Ok(match acc {
            None => {
                *acc = Some(s.clone());
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
            AccState::CountStar(_) => unreachable!("count(*) never inspects a per-row value"),
            AccState::ArrayAgg(_) => unreachable!(
                "array_agg is never routed through update_scalar — see push_array_agg's doc"
            ),
            AccState::StringAgg(_) => unreachable!(
                "string_agg is never routed through update_scalar — see push_string_agg's doc"
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
            AccState::CountStar(_) => {
                unreachable!("count(*) takes the row-count fast path in build_ungrouped, never a column kernel")
            }
            AccState::ArrayAgg(_) | AccState::StringAgg(_) => unreachable!(
                "array_agg/string_agg are always row-wise (push_array_agg/push_string_agg), \
                 never dispatched to the arrow-kernel fast path"
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
            AccState::ArrayAgg(list) => list.clone().map(CellValue::List),
            AccState::StringAgg(s) => s.clone().map(CellValue::Utf8),
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
            .map(|a| AccState::new(a.spec.func, a.num_kind))
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
        if matches!(func, AggFunc::ArrayAgg | AggFunc::StringAgg { .. }) {
            let col = agg
                .spec
                .input_col
                .expect("resolved: array_agg/string_agg always carry an input column");
            let delim_idx = match func {
                AggFunc::StringAgg { delim_col } => Some(delim_col),
                _ => None,
            };
            let value_col = batch.column(col).clone();
            let delim_col = delim_idx.map(|d| batch.column(d).clone());
            let is_array_agg = matches!(func, AggFunc::ArrayAgg);

            for row in 0..batch.num_rows() {
                if let Some(m) = mask {
                    if !(m.is_valid(row) && m.value(row)) {
                        continue;
                    }
                }
                let val = extract_cell(&value_col, row)?;
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
                    let key = HashKey::from(&val);
                    if !seen.insert(key.clone()) {
                        continue;
                    }
                    self.bump_memory(hash_key_bytes(&key))?;
                }
                if is_array_agg {
                    let added = acc.push_array_agg(val);
                    self.bump_memory(added)?;
                } else {
                    let delim_val = match &delim_col {
                        Some(dc) => extract_cell(dc, row)?,
                        None => None,
                    };
                    let added = acc.push_string_agg(&val, &delim_val)?;
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
                                .map(|a| AccState::new(a.spec.func, a.num_kind))
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
                    let delim_idx = match func {
                        AggFunc::StringAgg { delim_col } => Some(delim_col),
                        _ => None,
                    };

                    if distinct {
                        // See update_ungrouped_one's identical rule and its
                        // live-PG citations: array_agg(DISTINCT x) keeps one
                        // NULL entry, every other DISTINCT aggregate here
                        // (including string_agg) drops NULLs.
                        if val.is_none() && !is_array_agg {
                            continue; // DISTINCT ignores NULLs, like the aggregate itself
                        }
                        let key = HashKey::from(&val);
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
                    } else if let Some(delim_col) = delim_idx {
                        let delim_val = extract_cell(batch.column(delim_col), row)?;
                        let added = accs[gid][i].push_string_agg(&val, &delim_val)?;
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
}
