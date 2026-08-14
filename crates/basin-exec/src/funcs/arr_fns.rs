//! Array functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! Twelve of the nineteen `OID_ARRAY_*` arms out of `eval_scalar_fn`'s
//! `match`. The seven left behind (`array_sort` ×3, `array_to_string` ×2,
//! `string_to_array` ×2) are named in `arr_fns.integration.md` §5, together
//! with the two edits this slice could not make itself: the registrations in
//! [`crate::funcs::builtins`] and the deletion of the arms in `eval.rs`.
//! **Until that file is applied, everything here is tested and unreachable**
//! — the defect [`crate::funcs`]' module doc names. Applying it is the port;
//! this is the first of its three edits.
//!
//! # Three properties shape every function below
//!
//! Reproduced from the family header comment at `eval.rs:1632`, because that
//! comment is trimmed to the three remaining functions when the arms go.
//!
//! 1. **They are polymorphic, and the implementation must be too.**
//!    PostgreSQL declares them on `anycompatiblearray`/`anyarray`. So none of
//!    these may match on the element type: elements are *moved* with
//!    `take`/`interleave` and *compared* with `not_distinct`, both generic
//!    over Arrow's type system. Adding `int8[]`/`numeric[]` rows to
//!    `basin_pgtype::func::FUNCS` needs no change here.
//!
//! 2. **NULL has three distinct meanings and they are not interchangeable.**
//!    A NULL *array*, a NULL *element inside* an array, and an *empty* array
//!    are three different things and PostgreSQL answers differently for each.
//!    Every row below was measured on live PostgreSQL 18.2, and several are
//!    the opposite of the natural guess:
//!
//!    | call | result | the wrong guess |
//!    |---|---|---|
//!    | `array_append(NULL::int[], 1)` | `{1}` | NULL — the function is NOT strict |
//!    | `array_append(NULL::int[], NULL)` | `{NULL}` | NULL — a one-element array *containing* NULL |
//!    | `array_cat(NULL::int[], ARRAY[1])` | `{1}` | NULL |
//!    | `array_cat(NULL::int[], NULL::int[])` | NULL | `{}` |
//!    | `array_position(ARRAY[1,NULL,3], NULL)` | `2` | NULL — a NULL element IS found |
//!    | `array_positions(ARRAY[1,2], 9)` | `{}` | NULL — empty array, not NULL |
//!    | `array_position(ARRAY[1,2], 9)` | NULL | `{}`/0 — the singular form DOES answer NULL |
//!    | `array_remove(ARRAY[1,NULL,3], NULL)` | `{1,3}` | `{1,NULL,3}` — NULLs ARE removed |
//!    | `array_ndims(ARRAY[]::int[])` | NULL | 1 — an empty array has ZERO dimensions |
//!    | `cardinality(ARRAY[]::int[])` | `0` | NULL — and it disagrees with `array_length`, which IS NULL |
//!
//!    The `array_position`/`array_positions` row and the
//!    `array_length`/`cardinality` row are the two a shared helper gets
//!    wrong, which is why neither pair shares one here.
//!
//! 3. **Element equality is `IS NOT DISTINCT FROM`, not `=`.** That is what
//!    makes `array_position(…, NULL)` find a NULL and `array_remove(…, NULL)`
//!    remove one. `cmp::not_distinct` is exactly that predicate and returns a
//!    null-free `BooleanArray`, so no search function here has a three-valued
//!    branch to get wrong.
//!
//! # The two gaps Basin cannot currently express
//!
//! Both are recorded in full, with `psql` output, in
//! `arr_fns.integration.md` §6 (D1 and D2). In summary:
//!
//! * **Non-1 lower bounds.** PostgreSQL's `array_position` returns a
//!   *subscript*, and its `start` argument is a *subscript*:
//!   `array_position('[5:7]={a,b,c}'::text[], 'b')` is **6**, not 2.
//!   [`ArrayPosition`] computes the 1-based *ordinal* and clamps `start` to
//!   1. `basin_pgtype::physical` maps every array to Arrow's `ListArray`,
//!   which has no lower bound, so for every value that can reach here
//!   subscript == ordinal and the answers agree. The day a lower bound
//!   becomes representable, this family is wrong.
//! * **Multi-dimensional arrays.** `array_position`, `array_positions`,
//!   `array_remove` and `array_append` *raise* on one; `cardinality` counts
//!   across all dimensions while `array_length(a, 1)` counts only the first.
//!   No multi-dimensional physical type exists, so [`ArrayNdims`] can only
//!   ever answer 1 or NULL and [`Cardinality`] is always the row length.
//!
//! Neither is fixed here. A port moves behaviour; it does not improve it.
//!
//! # A verbatim move
//!
//! Every function body below is a character-for-character copy of the
//! corresponding `eval.rs` body, and the free functions keep their `eval_`
//! names for exactly that reason. The only additions are the [`ScalarFunc`]
//! wrapper structs, the argument plumbing, and the [`ScalarFunc::return_type`]
//! overrides §7 of the `.md` explains. The copied helpers (`map_arrow`,
//! `require_list`, `flatten_list`, `expand_per_row`, `elements_not_distinct`,
//! `assemble_list`) are likewise verbatim; they are private to `eval.rs` and
//! cannot be reached from here. `num_fns.rs` set that precedent with its own
//! copy of `map_arrow`, and its note applies: this should end up shared once
//! a third family needs it, which is a change to `eval.rs` no slice may make
//! while several agents share one tree.

use std::sync::Arc;

use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::kernels::{cast, cmp};
// `Array`'s `len`/`is_null`/`data_type` are trait methods, not inherent ones,
// and every function here walks rows by index to keep a NULL array
// distinguishable from an empty one.
use arrow_array::{
    new_empty_array, Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, UInt32Array,
};
use arrow_schema::{ArrowError, DataType, Field};
use arrow_select::{interleave, take};
use basin_pgtype::{Oid, PgType};

use crate::eval::EvalSession;
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// Translate an arrow kernel failure into an [`ExecError`].
///
/// A verbatim copy of `eval.rs`'s `map_arrow`, which is private to that
/// module and cannot be reached from here — the same copy, for the same
/// reason, that `num_fns.rs` carries. Copied rather than re-derived so the
/// ported functions keep raising *exactly* the errors their `match` arms
/// raised.
fn map_arrow(err: ArrowError, op: &'static str) -> ExecError {
    match err {
        ArrowError::DivideByZero => ExecError::DivisionByZero,
        ArrowError::ArithmeticOverflow(_) => ExecError::Overflow(op),
        ArrowError::InvalidArgumentError(msg)
        | ArrowError::CastError(msg)
        | ArrowError::ComputeError(msg) => ExecError::TypeMismatch(msg),
        other => ExecError::Internal(other.to_string()),
    }
}

/// The declared type of argument `i`, for a [`ScalarFunc::return_type`]
/// override.
///
/// Six of the twelve functions here return an array whose type is their array
/// argument's, and the catalog-backed default cannot answer that: every one of
/// these oids has *two* `FUNCS` rows (one at `int4[]`, one at `text[]`) and
/// [`crate::funcs::catalog_row`] returns the first, so the default would type
/// `array_append(text[], text)` as `int4[]`. See `arr_fns.integration.md` §7.
///
/// This reads the argument's *declared type*, never a value, so it is
/// unaffected by `Project::new` probing a projection's output schema against a
/// zero-row batch — the schema is present there even when the rows are not.
fn arg_type(args: &[PgType], i: usize, oid: u32) -> Result<PgType, ExecError> {
    args.get(i).copied().ok_or_else(|| {
        ExecError::Internal(format!(
            "function oid {oid} typed with only {} argument(s) — a planner \
             bug, not user error",
            args.len()
        ))
    })
}

/// Downcast to the one array layout `basin_pgtype::physical` produces for a
/// Postgres array type. Anything else reaching here is a planner bug, so it
/// is reported rather than improvised around.
fn require_list<'a>(arr: &'a ArrayRef, what: &str) -> Result<&'a ListArray, ExecError> {
    arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        ExecError::TypeMismatch(format!(
            "{what} expects an array, found {:?}",
            arr.data_type()
        ))
    })
}

/// Gather a list column's elements into one compact child array in row order,
/// plus each row's `(start, len)` window inside it.
///
/// Exists because `ListArray::values()` is the *whole* child buffer, which for
/// a sliced or out-of-order list is neither compact nor in row order — every
/// function below wants "row `i`'s elements" as a contiguous run, and building
/// that once is both simpler and cheaper than re-deriving it from
/// `value_offsets()` in each.
///
/// A NULL row contributes no elements and gets a zero-length window, so the
/// caller decides what a NULL array means for its own function (they differ —
/// see the module doc) rather than inheriting a decision made here.
fn flatten_list(
    list: &ListArray,
    what: &'static str,
) -> Result<(ArrayRef, Vec<(usize, usize)>), ExecError> {
    let offsets = list.value_offsets();
    let mut picks: Vec<u32> = Vec::new();
    let mut spans: Vec<(usize, usize)> = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        let start = picks.len();
        if !list.is_null(i) {
            for k in offsets[i]..offsets[i + 1] {
                picks.push(u32::try_from(k).expect("list offset fits u32"));
            }
        }
        spans.push((start, picks.len() - start));
    }
    let values = take::take(list.values().as_ref(), &UInt32Array::from(picks), None)
        .map_err(|e| map_arrow(e, what))?;
    Ok((values, spans))
}

/// Build a `ListArray` from element sources picked by `indices`, cut into rows
/// by `lengths`, with `validity[i] == false` making row `i` a NULL array.
///
/// The output field is `("item", nullable)` because that is what
/// `basin_pgtype::physical` produces for a Postgres array type — a different
/// name or nullability would be a different Arrow type from the one the plan
/// declared, and the batch would be rejected downstream.
fn assemble_list(
    elem_type: &DataType,
    sources: &[&dyn Array],
    indices: &[(usize, usize)],
    lengths: &[usize],
    validity: &[bool],
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let values: ArrayRef = if indices.is_empty() {
        new_empty_array(elem_type)
    } else {
        interleave::interleave(sources, indices).map_err(|e| map_arrow(e, what))?
    };
    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
    let field = Arc::new(Field::new("item", elem_type.clone(), true));
    let nulls = if validity.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(validity.to_vec()))
    };
    Ok(Arc::new(
        ListArray::try_new(field, offsets, values, nulls).map_err(|e| map_arrow(e, what))?,
    ))
}

// ─── measurement: array_length, cardinality, array_ndims ────────────────────
//
// Three functions that all answer "how big is it", all return `integer`, and
// all disagree about the empty array. They must never be unified behind one
// helper — the disagreement IS the behaviour.

/// `array_length(anyarray, integer) -> integer`, `pg_proc` oid 2176.
///
/// PostgreSQL answers NULL — not 0, and not an error — in four distinct
/// situations. All six rows below were measured live on PostgreSQL 18.2:
///
/// | call | result |
/// |---|---|
/// | `array_length(ARRAY['x','y'], 1)` | 2 |
/// | `array_length(ARRAY['x','y'], 2)` | NULL — no such dimension |
/// | `array_length(ARRAY['x','y'], 0)` | NULL — dimensions are 1-based |
/// | `array_length(ARRAY[]::text[], 1)` | NULL — an EMPTY array has no dimensions at all, so not even dimension 1 exists |
/// | `array_length(NULL::text[], 1)` | NULL — strict in the array |
/// | `array_length(ARRAY[1,NULL,3], 1)` | 3 — a NULL *element* still occupies a slot |
///
/// The empty-array row is the one worth stating explicitly: `0` is the
/// natural reading of "length" there and it is the wrong answer.
///
/// Only dimension 1 can ever be non-NULL here because Arrow's `ListArray` is
/// the physical form of a one-dimensional Postgres array; a genuinely
/// multi-dimensional `int[][]` has no physical type in `basin_pgtype` yet, so
/// no such value can reach this function.
pub struct ArrayLength;

impl ScalarFunc for ArrayLength {
    fn oid(&self) -> Oid {
        Oid(2176)
    }

    /// The catalog default is right: both `FUNCS` rows for 2176 return
    /// `int4`, whatever the element type.
    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_length(arg(args, 0, 2176)?, arg(args, 1, 2176)?)
    }
}

fn eval_array_length(arr: &ArrayRef, dim: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_length")?;
    let dims = cast::cast(dim, &DataType::Int64).map_err(|e| map_arrow(e, "array_length"))?;
    let dims = dims
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("just cast to Int64");
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| {
            if list.is_null(i) || dims.is_null(i) || dims.value(i) != 1 {
                return None;
            }
            let len = offsets[i + 1] - offsets[i];
            // An empty array has NO dimensions — see the table above.
            if len == 0 {
                None
            } else {
                Some(len)
            }
        })
        .collect();
    Ok(Arc::new(out))
}

/// `cardinality(anyarray) -> integer`, `pg_proc` oid 3179.
///
/// Deliberately NOT sharing [`eval_array_length`]:
/// `cardinality(ARRAY[]::int[])` is **0** while
/// `array_length(ARRAY[]::int[], 1)` is **NULL**, both measured live. The two
/// functions disagree on exactly the input a shared helper would unify. NULL
/// in, NULL out.
///
/// PostgreSQL's `cardinality` counts elements across *all* dimensions; Arrow's
/// `ListArray` is one-dimensional, so here it is the row's length.
pub struct Cardinality;

impl ScalarFunc for Cardinality {
    fn oid(&self) -> Oid {
        Oid(3179)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_cardinality(arg(args, 0, 3179)?)
    }
}

fn eval_cardinality(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "cardinality")?;
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| (!list.is_null(i)).then(|| offsets[i + 1] - offsets[i]))
        .collect();
    Ok(Arc::new(out))
}

/// `array_ndims(anyarray) -> integer`, `pg_proc` oid 748.
///
/// `array_ndims(ARRAY[]::int[])` is **NULL**, not 1 and not 0: an empty array
/// has no dimensions at all, the same rule that makes `array_length` NULL for
/// it (measured live). A NULL array is NULL.
///
/// Only ever 1 or NULL here, because `basin_pgtype::physical` has no
/// multi-dimensional array type — see the module doc.
pub struct ArrayNdims;

impl ScalarFunc for ArrayNdims {
    fn oid(&self) -> Oid {
        Oid(748)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_ndims(arg(args, 0, 748)?)
    }
}

fn eval_array_ndims(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_ndims")?;
    let offsets = list.value_offsets();
    let out: Int32Array = (0..list.len())
        .map(|i| {
            if list.is_null(i) || offsets[i + 1] == offsets[i] {
                None
            } else {
                Some(1)
            }
        })
        .collect();
    Ok(Arc::new(out))
}

// ─── array_reverse ──────────────────────────────────────────────────────────

/// `array_reverse(anyarray) -> anyarray`, `pg_proc` oid 6381.
///
/// `array_reverse(NULL::int[])` is NULL, `array_reverse(ARRAY[]::int[])` is
/// `{}` and `array_reverse(ARRAY[1,NULL,3])` is `{3,NULL,1}` — measured live:
/// a NULL *element* is reversed into place like any other, only a NULL *array*
/// stays NULL.
pub struct ArrayReverse;

impl ScalarFunc for ArrayReverse {
    fn oid(&self) -> Oid {
        Oid(6381)
    }

    /// Overridden: 6381 has two `FUNCS` rows with *different* `ret`
    /// (`int4[]` and `text[]`), and the default would answer with whichever
    /// is first in the table. See `arr_fns.integration.md` §7.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 0, 6381)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_reverse(arg(args, 0, 6381)?)
    }
}

fn eval_array_reverse(arr: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_reverse")?;
    let (flat, spans) = flatten_list(list, "array_reverse")?;
    let mut indices = Vec::with_capacity(flat.len());
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        indices.extend((*start..start + len).rev().map(|k| (0usize, k)));
        lengths.push(*len);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_reverse",
    )
}

// ─── constructors: array_append, array_prepend, array_cat ───────────────────
//
// The family's non-strict corner. None of these three answers NULL because an
// *argument* was NULL — `array_append(NULL::int[], 1)` is `{1}` — and
// `array_cat` is the only one that can produce a NULL array at all, when both
// sides are NULL.

/// `array_append(anycompatiblearray, anycompatible)` (oid 378) and
/// `array_prepend(anycompatible, anycompatiblearray)` (oid 379) — the same
/// function with the element on the other end.
///
/// Neither is strict, and that is the whole subtlety. Measured live:
/// `array_append(NULL::int[], 1)` is `{1}`, not NULL — a NULL array behaves as
/// an empty one — and `array_append(NULL::int[], NULL)` is `{NULL}`, a
/// one-element array whose single element is NULL. So the result is **never**
/// a NULL array, which is why `validity` below is unconditionally true.
fn eval_array_add_element(
    arr: &ArrayRef,
    elem: &ArrayRef,
    at_front: bool,
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, what)?;
    let (flat, spans) = flatten_list(list, what)?;
    if flat.data_type() != elem.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "{what} expects the element type {:?} of its array, found {:?}",
            flat.data_type(),
            elem.data_type()
        )));
    }
    let mut indices = Vec::with_capacity(flat.len() + spans.len());
    let mut lengths = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        if at_front {
            indices.push((1usize, row));
        }
        indices.extend((*start..start + len).map(|k| (0usize, k)));
        if !at_front {
            indices.push((1usize, row));
        }
        lengths.push(len + 1);
    }
    let validity = vec![true; spans.len()];
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref(), elem.as_ref()],
        &indices,
        &lengths,
        &validity,
        what,
    )
}

/// `array_append(anycompatiblearray, anycompatible)`, `pg_proc` oid 378.
///
/// **KNOWN DIVERGENCE, inherited and unchanged.** PostgreSQL's
/// `anycompatible` resolution picks a common type and casts *both* sides, so
/// `array_append(ARRAY[1,2], 3.5)` is `{1,2,3.5}` of type `numeric[]`
/// (measured live). [`eval_array_add_element`] compares `data_type()`s and
/// raises [`ExecError::TypeMismatch`] instead. `basin_pgtype::func::FUNCS`
/// tabulates only the `int4[]` and `text[]` monomorphizations, so such a call
/// most likely fails to resolve before it reaches here — which layer refuses
/// it is on the `.md`'s NEEDS VERIFICATION list. Either way the port did not
/// change it: the type check is copied verbatim.
pub struct ArrayAppend;

impl ScalarFunc for ArrayAppend {
    fn oid(&self) -> Oid {
        Oid(378)
    }

    /// Overridden — the array is argument 0. See `arr_fns.integration.md` §7.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 0, 378)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_add_element(
            arg(args, 0, 378)?,
            arg(args, 1, 378)?,
            false,
            "array_append",
        )
    }
}

/// `array_prepend(anycompatible, anycompatiblearray)`, `pg_proc` oid 379.
///
/// **Note the argument order**: the element comes FIRST, the reverse of
/// [`ArrayAppend`]. `basin_pgtype::func::FUNCS` has a test asserting
/// `array_prepend(array, element)` does NOT resolve, for exactly this reason —
/// getting the order wrong here is a silent wrong answer, not a type error,
/// because both orders type-check whenever the element type and the array's
/// agree.
pub struct ArrayPrepend;

impl ScalarFunc for ArrayPrepend {
    fn oid(&self) -> Oid {
        Oid(379)
    }

    /// Overridden, and the array is argument **1**, not 0 — the one place in
    /// this module where that index differs. See `arr_fns.integration.md` §7.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 1, 379)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_add_element(
            arg(args, 1, 379)?,
            arg(args, 0, 379)?,
            true,
            "array_prepend",
        )
    }
}

/// `array_cat(anycompatiblearray, anycompatiblearray)`, `pg_proc` oid 383.
///
/// NULL is absorbed on either side but not on both: `array_cat(NULL, {1})` is
/// `{1}` and `array_cat({1}, NULL)` is `{1}`, while
/// `array_cat(NULL::int[], NULL::int[])` is NULL, not `{}`. All three measured
/// live. The result is a NULL array exactly when *both* inputs are.
pub struct ArrayCat;

impl ScalarFunc for ArrayCat {
    fn oid(&self) -> Oid {
        Oid(383)
    }

    /// Overridden. Both arguments are arrays and `eval_array_cat` already
    /// refuses a pair whose element types differ, so argument 0 decides the
    /// answer for every call that gets past that check.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 0, 383)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_cat(arg(args, 0, 383)?, arg(args, 1, 383)?)
    }
}

fn eval_array_cat(l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let ll = require_list(l, "array_cat")?;
    let rl = require_list(r, "array_cat")?;
    let (lflat, lspans) = flatten_list(ll, "array_cat")?;
    let (rflat, rspans) = flatten_list(rl, "array_cat")?;
    if lflat.data_type() != rflat.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "array_cat expects two arrays of the same element type, found {:?} and {:?}",
            lflat.data_type(),
            rflat.data_type()
        )));
    }
    let mut indices = Vec::with_capacity(lflat.len() + rflat.len());
    let mut lengths = Vec::with_capacity(lspans.len());
    let mut validity = Vec::with_capacity(lspans.len());
    for row in 0..ll.len() {
        let (ls, ln) = lspans[row];
        let (rs, rn) = rspans[row];
        indices.extend((ls..ls + ln).map(|k| (0usize, k)));
        indices.extend((rs..rs + rn).map(|k| (1usize, k)));
        lengths.push(ln + rn);
        validity.push(!(ll.is_null(row) && rl.is_null(row)));
    }
    assemble_list(
        &lflat.data_type().clone(),
        &[lflat.as_ref(), rflat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_cat",
    )
}

// ─── the search family: array_remove, array_replace ─────────────────────────
//
// Everything from here down compares elements. All of it goes through
// `elements_not_distinct`, and none of it has a three-valued branch, because
// `IS NOT DISTINCT FROM` is never NULL.
//
// `expand_per_row` and `elements_not_distinct` exist ONLY for this family and
// for `array_position`/`array_positions` below. Taking all four is what leaves
// them dead in `eval.rs` rather than half-used — see
// `arr_fns.integration.md` §2c.

/// Repeat a one-value-per-row array so it lines up element-for-element with a
/// [`flatten_list`] child array — the shape `cmp::not_distinct` needs to
/// compare every element of row `i` against row `i`'s search value.
fn expand_per_row(
    per_row: &ArrayRef,
    spans: &[(usize, usize)],
    what: &'static str,
) -> Result<ArrayRef, ExecError> {
    let mut idx: Vec<u32> = Vec::new();
    for (row, (_, len)) in spans.iter().enumerate() {
        let row = u32::try_from(row).expect("row index fits u32");
        idx.extend(std::iter::repeat_n(row, *len));
    }
    take::take(per_row.as_ref(), &UInt32Array::from(idx), None).map_err(|e| map_arrow(e, what))
}

/// `IS NOT DISTINCT FROM`, element by element, between a flattened child array
/// and the per-row search value expanded to match it. Never NULL — that is the
/// whole point of `not_distinct` over `eq`, and it is why
/// `array_position(ARRAY[1,NULL,3], NULL)` is `2` rather than NULL.
fn elements_not_distinct(
    flat: &ArrayRef,
    needle: &ArrayRef,
    spans: &[(usize, usize)],
    what: &'static str,
) -> Result<BooleanArray, ExecError> {
    let expanded = expand_per_row(needle, spans, what)?;
    cmp::not_distinct(flat, &expanded).map_err(|e| map_arrow(e, what))
}

/// `array_remove(anycompatiblearray, anycompatible)`, `pg_proc` oid 3167 —
/// every element not distinct from the second argument, dropped.
///
/// `array_remove(ARRAY[1,NULL,3], NULL)` is `{1,3}`, measured live: a NULL
/// search value removes the NULL elements rather than matching nothing. It
/// reads as "remove nothing" and it is not, because element comparison is
/// `IS NOT DISTINCT FROM`. A NULL *array* still answers NULL.
///
/// Removing every element leaves a 0-dimensional array, not an empty
/// 1-dimensional one — live, `array_ndims(array_remove(ARRAY[1,1], 1))` is
/// NULL. [`ArrayNdims`] reproduces that here for free, because it keys on the
/// row being empty rather than on a stored dimension count.
///
/// **Note it has NO element-type check at all**, unlike the rest of the
/// family — `eval_array_remove` goes straight to `elements_not_distinct`, so a
/// mismatched element type surfaces as whatever `cmp::not_distinct` says
/// rather than as the family's own message. Copied as-is; see
/// `arr_fns.integration.md` §6 D6.
pub struct ArrayRemove;

impl ScalarFunc for ArrayRemove {
    fn oid(&self) -> Oid {
        Oid(3167)
    }

    /// Overridden — the result is the array argument's type. See
    /// `arr_fns.integration.md` §7.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 0, 3167)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_remove(arg(args, 0, 3167)?, arg(args, 1, 3167)?)
    }
}

fn eval_array_remove(arr: &ArrayRef, elem: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_remove")?;
    let (flat, spans) = flatten_list(list, "array_remove")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_remove")?;
    let mut indices = Vec::new();
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        let before = indices.len();
        indices.extend(
            (*start..start + len)
                .filter(|k| !matches.value(*k))
                .map(|k| (0usize, k)),
        );
        lengths.push(indices.len() - before);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_remove",
    )
}

/// `array_replace(anycompatiblearray, anycompatible, anycompatible)`,
/// `pg_proc` oid 3168 — every element not distinct from `from`, replaced by
/// `to`.
///
/// Both `from` and `to` may be NULL and both are meaningful:
/// `array_replace(ARRAY[1,NULL,3], NULL, 9)` is `{1,9,3}` and
/// `array_replace(ARRAY[1,2,3], 2, NULL)` is `{1,NULL,3}`, measured live.
/// Unlike [`ArrayRemove`] the length never changes, and a NULL array is the
/// only input that answers NULL.
pub struct ArrayReplace;

impl ScalarFunc for ArrayReplace {
    fn oid(&self) -> Oid {
        Oid(3168)
    }

    /// Overridden — the result is the array argument's type. See
    /// `arr_fns.integration.md` §7.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError> {
        arg_type(args, 0, 3168)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_replace(
            arg(args, 0, 3168)?,
            arg(args, 1, 3168)?,
            arg(args, 2, 3168)?,
        )
    }
}

fn eval_array_replace(
    arr: &ArrayRef,
    from: &ArrayRef,
    to: &ArrayRef,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_replace")?;
    let (flat, spans) = flatten_list(list, "array_replace")?;
    if flat.data_type() != to.data_type() {
        return Err(ExecError::TypeMismatch(format!(
            "array_replace expects the element type {:?} of its array, found {:?}",
            flat.data_type(),
            to.data_type()
        )));
    }
    let matches = elements_not_distinct(&flat, from, &spans, "array_replace")?;
    let mut indices = Vec::with_capacity(flat.len());
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        for k in *start..start + len {
            if matches.value(k) {
                indices.push((1usize, row));
            } else {
                indices.push((0usize, k));
            }
        }
        lengths.push(*len);
        validity.push(!list.is_null(row));
    }
    assemble_list(
        &flat.data_type().clone(),
        &[flat.as_ref(), to.as_ref()],
        &indices,
        &lengths,
        &validity,
        "array_replace",
    )
}

// ─── the position pair ──────────────────────────────────────────────────────
//
// `array_position` and `array_positions` are deliberately two functions, not
// one with a flag. They disagree about the absent case — the singular answers
// NULL, the plural answers `{}` — which is exactly the input a shared helper
// would unify.

/// `array_position(anycompatiblearray, anycompatible)`, `pg_proc` oid 3277 —
/// the 1-based index of the first element not distinct from the second
/// argument, or NULL if there is none.
///
/// Measured live, and each row is a case a plausible implementation gets
/// wrong:
///
/// | call | result |
/// |---|---|
/// | `array_position(ARRAY[1,NULL,3], NULL)` | 2 — a NULL element IS matched |
/// | `array_position(ARRAY[1,2], 9)` | NULL — absent is NULL, **not 0** |
/// | `array_position(NULL::int[], 1)` | NULL |
/// | `array_position(ARRAY[]::int[], 1)` | NULL |
///
/// **KNOWN DIVERGENCE, inherited and unchanged.** PostgreSQL returns the
/// *subscript*, not the ordinal: `array_position('[5:7]={a,b,c}'::text[],
/// 'b')` is **6**. [`eval_array_position`] computes `k - start_off + 1`, the
/// 1-based ordinal. Unobservable today — `basin_pgtype::physical` gives every
/// array a lower bound of 1, so subscript == ordinal — and wrong the day a
/// lower bound becomes representable. See `arr_fns.integration.md` §6 D1.
pub struct ArrayPosition;

impl ScalarFunc for ArrayPosition {
    fn oid(&self) -> Oid {
        Oid(3277)
    }

    /// The catalog default is right: `array_position` returns a plain `int4`
    /// whatever the element type, so both `FUNCS` rows for 3277 agree. The
    /// polymorphism is entirely in its arguments.
    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_position(arg(args, 0, 3277)?, arg(args, 1, 3277)?, None)
    }
}

/// `array_position(anycompatiblearray, anycompatible, integer)`, `pg_proc`
/// oid 3278 — the same search, started partway in.
///
/// Live PostgreSQL 18.2:
///
/// | call | result |
/// |---|---|
/// | `array_position(ARRAY[1,2,1], 1, 2)` | 3 |
/// | `array_position(ARRAY[1,2,1], 1, 0)` | 1 — a start below 1 clamps |
/// | `array_position(ARRAY[1,2,1], 1, -5)` | 1 — so does a negative one |
/// | `array_position(ARRAY[1,2,1], 1, 9)` | NULL — past the end finds nothing |
/// | `array_position(ARRAY[1,2,1], 1, NULL)` | **ERROR**: `initial position must not be null` |
///
/// The last row is the only input anything in this module *rejects*, and it
/// rejects rather than returning NULL even though the function is not strict.
///
/// **KNOWN DIVERGENCE on the error, inherited and unchanged.** PostgreSQL
/// raises SQLSTATE `22004`; Basin raises
/// `ExecError::TypeMismatch("initial position must not be null")` — the
/// message is verbatim, the class is not, and a wire client sees the wrong
/// SQLSTATE. Basin also errors for the **whole batch** (the check is
/// `null_count() > 0` over the argument array) where PostgreSQL errors per
/// row; for a scalar `start` those are the same thing. Both are pre-existing
/// and family-wide. See `arr_fns.integration.md` §6 D3.
///
/// The `start` argument is also a *subscript* on the server, clamped up to
/// the array's lower bound rather than to 1 — the same D1 gap
/// [`ArrayPosition`] carries.
pub struct ArrayPositionStart;

impl ScalarFunc for ArrayPositionStart {
    fn oid(&self) -> Oid {
        Oid(3278)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_position(
            arg(args, 0, 3278)?,
            arg(args, 1, 3278)?,
            Some(arg(args, 2, 3278)?),
        )
    }
}

fn eval_array_position(
    arr: &ArrayRef,
    elem: &ArrayRef,
    start: Option<&ArrayRef>,
) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_position")?;
    let (flat, spans) = flatten_list(list, "array_position")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_position")?;
    let from = match start {
        None => None,
        Some(s) => {
            let s = cast::cast(s, &DataType::Int64).map_err(|e| map_arrow(e, "array_position"))?;
            let s = s
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("just cast to Int64")
                .clone();
            if s.null_count() > 0 {
                return Err(ExecError::TypeMismatch(
                    "initial position must not be null".to_string(),
                ));
            }
            Some(s)
        }
    };
    let out: Int32Array = spans
        .iter()
        .enumerate()
        .map(|(row, (start_off, len))| {
            if list.is_null(row) {
                return None;
            }
            // 1-based, clamped: a start below 1 means "from the beginning",
            // not an error and not an empty search.
            let skip = from
                .as_ref()
                .map(|s| (s.value(row) - 1).max(0))
                .unwrap_or(0);
            let skip = usize::try_from(skip).unwrap_or(usize::MAX);
            if skip >= *len {
                return None;
            }
            (start_off + skip..start_off + len)
                .find(|k| matches.value(*k))
                .map(|k| i32::try_from(k - start_off + 1).expect("array position fits i32"))
        })
        .collect();
    Ok(Arc::new(out))
}

/// `array_positions(anycompatiblearray, anycompatible)`, `pg_proc` oid 3279 —
/// *all* the 1-based indices, as an `int4[]`.
///
/// It does not share [`ArrayPosition`]'s "absent is NULL" rule:
/// `array_positions(ARRAY[1,2], 9)` is `{}`, an empty array, while the
/// singular form is NULL. A NULL array is still NULL. Both measured live, and
/// this disagreement is why the two are separate functions here.
pub struct ArrayPositions;

impl ScalarFunc for ArrayPositions {
    fn oid(&self) -> Oid {
        Oid(3279)
    }

    /// The catalog default is right and the override would be WRONG here:
    /// `array_positions` returns `int4[]` whatever the element type, so it
    /// must NOT be typed from its array argument the way the six
    /// array-returning functions are. Both `FUNCS` rows for 3279 agree on
    /// `int4[]`, and `basin_pgtype::func`'s own test asserts it.
    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_array_positions(arg(args, 0, 3279)?, arg(args, 1, 3279)?)
    }
}

fn eval_array_positions(arr: &ArrayRef, elem: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let list = require_list(arr, "array_positions")?;
    let (flat, spans) = flatten_list(list, "array_positions")?;
    let matches = elements_not_distinct(&flat, elem, &spans, "array_positions")?;
    let mut values: Vec<i32> = Vec::new();
    let mut lengths = Vec::with_capacity(spans.len());
    let mut validity = Vec::with_capacity(spans.len());
    for (row, (start, len)) in spans.iter().enumerate() {
        let before = values.len();
        for k in *start..start + len {
            if matches.value(k) {
                values.push(i32::try_from(k - start + 1).expect("array position fits i32"));
            }
        }
        lengths.push(values.len() - before);
        validity.push(!list.is_null(row));
    }
    let child = Int32Array::from(values);
    let offsets = OffsetBuffer::from_lengths(lengths);
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let nulls = if validity.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    Ok(Arc::new(
        ListArray::try_new(field, offsets, Arc::new(child), nulls)
            .map_err(|e| map_arrow(e, "array_positions"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::catalog_row;
    use arrow_array::StringArray;

    /// A `text[]` column. `None` is a NULL *array*; an inner `None` is a NULL
    /// *element*; an empty slice is an empty array. Those three are different
    /// things to every function in this module, so the helper has to be able
    /// to say all three.
    fn text_list(rows: &[Option<&[Option<&str>]>]) -> ArrayRef {
        let mut values: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut validity: Vec<bool> = Vec::new();
        for row in rows {
            match row {
                Some(elems) => {
                    values.extend(elems.iter().map(|e| e.map(str::to_string)));
                    validity.push(true);
                }
                None => validity.push(false),
            }
            offsets.push(i32::try_from(values.len()).expect("test offsets fit i32"));
        }
        let child = Arc::new(StringArray::from(values)) as ArrayRef;
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(
            ListArray::try_new(
                field,
                OffsetBuffer::new(offsets.into()),
                child,
                Some(NullBuffer::from(validity)),
            )
            .expect("test list is well formed"),
        )
    }

    /// The inverse of [`text_list`], so an assertion can be written the way
    /// `psql` prints the answer.
    fn read_text_list(arr: &ArrayRef) -> Vec<Option<Vec<Option<String>>>> {
        let list = arr.as_any().downcast_ref::<ListArray>().expect("array out");
        (0..list.len())
            .map(|i| {
                (!list.is_null(i)).then(|| {
                    let row = list.value(i);
                    let row = row
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("text elements");
                    (0..row.len())
                        .map(|k| (!row.is_null(k)).then(|| row.value(k).to_string()))
                        .collect()
                })
            })
            .collect()
    }

    /// The same, for `array_positions`' `int4[]`. Its elements are never
    /// NULL — an index is either found or not emitted — so the inner type is
    /// a plain `i32` and a NULL appearing there would fail the downcast-free
    /// read below rather than being silently accepted.
    fn read_int_list(arr: &ArrayRef) -> Vec<Option<Vec<i32>>> {
        let list = arr.as_any().downcast_ref::<ListArray>().expect("array out");
        (0..list.len())
            .map(|i| {
                (!list.is_null(i)).then(|| {
                    let row = list.value(i);
                    let row = row
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("integer elements");
                    (0..row.len())
                        .map(|k| {
                            assert!(!row.is_null(k), "an emitted position is never NULL");
                            row.value(k)
                        })
                        .collect()
                })
            })
            .collect()
    }

    fn out_i32(a: &ArrayRef) -> &Int32Array {
        a.as_any()
            .downcast_ref::<Int32Array>()
            .expect("integer out")
    }

    fn i32s(v: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(v))
    }

    fn texts(v: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    /// Read an `Int32Array` result the way `psql` prints a column.
    fn seen_i32(a: &ArrayRef) -> Vec<Option<i32>> {
        let a = out_i32(a);
        (0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i)))
            .collect()
    }

    /// Registration asserts a `pg_proc` row exists and PANICS if it does not,
    /// so every oid in this slice is checked here — while `arr_fns.rs` can
    /// still be edited — rather than at the moment someone applies
    /// `arr_fns.integration.md` and gets a panic in `builtins()` instead of a
    /// working registry. The name check is the real content: an oid that
    /// resolves to the wrong catalog row would register happily and then
    /// answer calls to a different function.
    ///
    /// It is `catalog_row` rather than `builtins()` because step 2 of the
    /// port has not been applied yet — see this module's doc.
    #[test]
    fn every_oid_in_this_slice_has_the_catalog_row_registration_will_demand() {
        let slice: Vec<(&str, Box<dyn ScalarFunc>)> = vec![
            ("array_length", Box::new(ArrayLength)),
            ("cardinality", Box::new(Cardinality)),
            ("array_ndims", Box::new(ArrayNdims)),
            ("array_reverse", Box::new(ArrayReverse)),
            ("array_append", Box::new(ArrayAppend)),
            ("array_prepend", Box::new(ArrayPrepend)),
            ("array_cat", Box::new(ArrayCat)),
            ("array_remove", Box::new(ArrayRemove)),
            ("array_replace", Box::new(ArrayReplace)),
            ("array_position", Box::new(ArrayPosition)),
            ("array_position", Box::new(ArrayPositionStart)),
            ("array_positions", Box::new(ArrayPositions)),
        ];
        for (name, f) in slice {
            let row = catalog_row(f.oid())
                .unwrap_or_else(|| panic!("{name} oid {} has no FUNCS row", f.oid().get()));
            assert_eq!(row.name, name, "oid {} names {}", f.oid().get(), row.name);
        }
    }

    /// The empty array is where these three functions disagree, and the
    /// disagreement is the whole reason they are not one helper. Every
    /// expectation is live PostgreSQL 18.2:
    ///
    /// ```text
    /// array_length(ARRAY[]::text[], 1) -> NULL
    /// cardinality(ARRAY[]::int[])      -> 0
    /// array_ndims(ARRAY[]::int[])      -> NULL
    /// ```
    #[test]
    fn the_three_measurements_disagree_about_the_empty_array() {
        // row 0: {x,y}   row 1: {}   row 2: NULL   row 3: {a,NULL,c}
        let a = text_list(&[
            Some(&[Some("x"), Some("y")]),
            Some(&[]),
            None,
            Some(&[Some("a"), None, Some("c")]),
        ]);
        let s = &EvalSession::DEFAULT;
        let ones = i32s(vec![Some(1); 4]);

        let len = ArrayLength.invoke(&[a.clone(), ones], s).unwrap();
        assert_eq!(
            seen_i32(&len),
            vec![Some(2), None, None, Some(3)],
            "array_length: NULL for the EMPTY array — not 0 — and a NULL \
             element still occupies a slot"
        );

        let card = Cardinality.invoke(&[a.clone()], s).unwrap();
        assert_eq!(
            seen_i32(&card),
            vec![Some(2), Some(0), None, Some(3)],
            "cardinality: 0 for the EMPTY array, where array_length is NULL"
        );

        let ndims = ArrayNdims.invoke(&[a], s).unwrap();
        assert_eq!(
            seen_i32(&ndims),
            vec![Some(1), None, None, Some(1)],
            "array_ndims: an empty array has ZERO dimensions, so NULL"
        );
    }

    /// Dimensions are 1-based and a `ListArray` only ever has one, so every
    /// dimension but 1 is NULL. Live: `array_length(ARRAY['x','y'], 2)`,
    /// `(…, 0)` and `(…, -1)` are all NULL, as is `(…, NULL)`.
    #[test]
    fn array_length_answers_only_for_dimension_one() {
        let s = &EvalSession::DEFAULT;
        for dim in [Some(2), Some(0), Some(-1), None] {
            let a = text_list(&[Some(&[Some("x"), Some("y")])]);
            let got = ArrayLength.invoke(&[a, i32s(vec![dim])], s).unwrap();
            assert_eq!(
                seen_i32(&got),
                vec![None],
                "array_length(ARRAY['x','y'], {dim:?}) is NULL"
            );
        }
    }

    /// `array_reverse(ARRAY[1,NULL,3])` is `{3,NULL,1}` on 18.2 — a NULL
    /// element moves like any other. Only a NULL *array* answers NULL, and an
    /// empty array reverses to an empty array rather than to NULL.
    #[test]
    fn array_reverse_moves_null_elements_and_preserves_the_three_null_kinds() {
        let a = text_list(&[
            Some(&[Some("a"), None, Some("c")]),
            Some(&[]),
            None,
            Some(&[Some("only")]),
        ]);
        let got = ArrayReverse.invoke(&[a], &EvalSession::DEFAULT).unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                Some(vec![Some("c".into()), None, Some("a".into())]),
                Some(vec![]),
                None,
                Some(vec![Some("only".into())]),
            ]
        );
    }

    /// The output of an array-returning function must keep the `("item",
    /// nullable)` child field `basin_pgtype::physical` produces, or the batch
    /// is a different Arrow type from the one the plan declared and is
    /// rejected downstream. This is the property `assemble_list` exists to
    /// hold, and it is invisible in a value-only assertion.
    #[test]
    fn an_array_result_keeps_the_physical_list_type_of_its_input() {
        let a = text_list(&[Some(&[Some("a"), Some("b")]), None]);
        let got = ArrayReverse
            .invoke(&[a.clone()], &EvalSession::DEFAULT)
            .unwrap();
        assert_eq!(got.data_type(), a.data_type());
        assert_eq!(got.len(), a.len(), "output length == input length");
    }

    /// `ArrayReverse` must NOT use the catalog-backed default `return_type`.
    /// Oid 6381 has two `FUNCS` rows — `int4[] -> int4[]` and
    /// `text[] -> text[]` — and `catalog_row` returns the first, so the
    /// default would type `array_reverse(text[])` as `int4[]`. See
    /// `arr_fns.integration.md` §7.
    #[test]
    fn array_reverse_types_from_its_argument_not_from_the_first_catalog_row() {
        let text_array = PgType::new(Oid(1009)); // text[]
        assert_eq!(
            ArrayReverse.return_type(&[text_array]).unwrap(),
            text_array,
            "array_reverse(text[]) is text[], not the int4[] the first \
             catalog row would give"
        );
        // The default, for contrast — this is what the override avoids.
        assert_eq!(
            catalog_row(Oid(6381)).unwrap().ret,
            Oid(1007),
            "the FIRST row for 6381 returns int4[], which is why the \
             default is wrong here"
        );
    }

    /// `array_append` and `array_prepend` are NOT strict, which is the single
    /// most surprising thing about them. Live PostgreSQL 18.2:
    ///
    /// ```text
    /// array_append(NULL::int[], 1)    -> {1}      not NULL
    /// array_append(NULL::int[], NULL) -> {NULL}   a ONE-element array holding NULL
    /// array_append(ARRAY[1,2], NULL)  -> {1,2,NULL}
    /// array_prepend(1, NULL::int[])   -> {1}
    /// ```
    ///
    /// So the result is never a NULL array, however NULL the arguments were.
    #[test]
    fn append_and_prepend_are_not_strict_in_either_argument() {
        let s = &EvalSession::DEFAULT;
        // row 0: {a,b}   row 1: {}   row 2: NULL array   row 3: {a,b}
        let a = text_list(&[
            Some(&[Some("a"), Some("b")]),
            Some(&[]),
            None,
            Some(&[Some("a"), Some("b")]),
        ]);
        // The element is NULL on the two rows that matter most.
        let e = texts(vec![Some("z"), Some("z"), None, None]);

        let got = ArrayAppend.invoke(&[a.clone(), e.clone()], s).unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                Some(vec![Some("a".into()), Some("b".into()), Some("z".into())]),
                Some(vec![Some("z".into())]),
                // The NULL array behaved as an empty one AND the NULL element
                // was appended: {NULL}, not NULL.
                Some(vec![None]),
                Some(vec![Some("a".into()), Some("b".into()), None]),
            ],
            "array_append is strict in neither argument"
        );

        // Note the argument order: the element comes FIRST.
        let got = ArrayPrepend.invoke(&[e, a], s).unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                Some(vec![Some("z".into()), Some("a".into()), Some("b".into())]),
                Some(vec![Some("z".into())]),
                Some(vec![None]),
                Some(vec![None, Some("a".into()), Some("b".into())]),
            ],
            "array_prepend puts the element at the FRONT and takes it FIRST"
        );
    }

    /// `array_cat` absorbs a NULL on one side and answers NULL only when
    /// *both* are. Live: `array_cat(NULL::int[], ARRAY[1])` and
    /// `array_cat(ARRAY[1], NULL::int[])` are both `{1}`, while
    /// `array_cat(NULL::int[], NULL::int[])` is NULL — **not** `{}`. It is the
    /// one function in this batch that can produce a NULL array at all.
    #[test]
    fn array_cat_absorbs_one_null_side_but_not_two() {
        let l = text_list(&[
            None,
            Some(&[Some("a")]),
            None,
            Some(&[]),
            Some(&[Some("a")]),
        ]);
        let r = text_list(&[
            Some(&[Some("b")]),
            None,
            None,
            Some(&[]),
            Some(&[Some("b")]),
        ]);
        let got = ArrayCat.invoke(&[l, r], &EvalSession::DEFAULT).unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                Some(vec![Some("b".into())]),
                Some(vec![Some("a".into())]),
                // BOTH NULL — and this is the only row that is NULL.
                None,
                Some(vec![]),
                Some(vec![Some("a".into()), Some("b".into())]),
            ]
        );
    }

    /// The `anycompatible` divergence, pinned with BOTH answers. PostgreSQL
    /// widens and casts; Basin refuses.
    ///
    /// ```text
    /// select array_append(ARRAY[1,2], 3.5);            -> {1,2,3.5}
    /// select pg_typeof(array_append(ARRAY[1,2], 3.5)); -> numeric[]
    /// ```
    ///
    /// Basin raises `TypeMismatch` on any element whose Arrow type is not the
    /// array's. Copied verbatim from the `match` arm; a port does not improve
    /// behaviour. See `arr_fns.integration.md` §6 D6.
    #[test]
    fn a_widened_element_type_is_refused_where_postgres_would_cast() {
        let a = text_list(&[Some(&[Some("a")])]);
        let wrong: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let err = ArrayAppend
            .invoke(&[a, wrong], &EvalSession::DEFAULT)
            .unwrap_err();
        assert!(
            matches!(
                err,
                ExecError::TypeMismatch(ref m)
                    if m.contains("array_append expects the element type")
            ),
            "got {err:?}"
        );
    }

    /// `ArrayPrepend`'s array is argument **1**. Typing it from argument 0
    /// would answer with the *element* type, which is the mistake the index
    /// exists to avoid — and it is invisible unless the two differ.
    #[test]
    fn prepend_types_from_argument_one_and_append_from_argument_zero() {
        let text_array = PgType::new(Oid(1009)); // text[]
        let text = PgType::TEXT;
        assert_eq!(
            ArrayPrepend.return_type(&[text, text_array]).unwrap(),
            text_array,
            "array_prepend(text, text[]) is text[]"
        );
        assert_eq!(
            ArrayAppend.return_type(&[text_array, text]).unwrap(),
            text_array,
            "array_append(text[], text) is text[]"
        );
        assert_eq!(
            ArrayCat.return_type(&[text_array, text_array]).unwrap(),
            text_array
        );
    }

    /// The one the brief flagged: `array_remove(a, NULL)` reads as "remove
    /// nothing" and it removes the NULLs, because element comparison is
    /// `IS NOT DISTINCT FROM`, not `=`. Live PostgreSQL 18.2:
    ///
    /// ```text
    /// array_remove(ARRAY[1,NULL,3], NULL) -> {1,3}
    /// array_remove(ARRAY[1,2], 9)         -> {1,2}
    /// array_remove(NULL::int[], 1)        -> NULL
    /// array_remove(ARRAY[]::int[], 1)     -> {}
    /// ```
    #[test]
    fn array_remove_with_a_null_needle_removes_the_nulls() {
        // row 0: {a,NULL,c} remove NULL   row 1: {a,b} remove "z" (absent)
        // row 2: NULL array               row 3: {} empty
        // row 4: {a,a} remove "a" — every element goes
        let a = text_list(&[
            Some(&[Some("a"), None, Some("c")]),
            Some(&[Some("a"), Some("b")]),
            None,
            Some(&[]),
            Some(&[Some("a"), Some("a")]),
        ]);
        let needle = texts(vec![None, Some("z"), Some("a"), Some("a"), Some("a")]);
        let got = ArrayRemove
            .invoke(&[a, needle], &EvalSession::DEFAULT)
            .unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                Some(vec![Some("a".into()), Some("c".into())]),
                Some(vec![Some("a".into()), Some("b".into())]),
                // A NULL ARRAY is still NULL — only NULL ELEMENTS are removed.
                None,
                Some(vec![]),
                // Removing everything leaves an EMPTY array, which
                // `array_ndims` then reports as NULL — live,
                // `array_ndims(array_remove(ARRAY[1,1],1))` is NULL.
                Some(vec![]),
            ]
        );
    }

    /// Removing every element gives a 0-dimensional array. The two functions
    /// have to agree about that across a composition, which is the property a
    /// value-only assertion on either one alone would miss.
    #[test]
    fn removing_every_element_leaves_an_array_with_no_dimensions() {
        let s = &EvalSession::DEFAULT;
        let a = text_list(&[Some(&[Some("a"), Some("a")])]);
        let emptied = ArrayRemove.invoke(&[a, texts(vec![Some("a")])], s).unwrap();
        let ndims = ArrayNdims.invoke(&[emptied], s).unwrap();
        assert_eq!(
            seen_i32(&ndims),
            vec![None],
            "array_ndims(array_remove(ARRAY[1,1],1)) is NULL on 18.2"
        );
    }

    /// `array_replace` takes both a NULL `from` and a NULL `to`, and both
    /// mean something. Live:
    ///
    /// ```text
    /// array_replace(ARRAY[1,NULL,3], NULL, 9) -> {1,9,3}
    /// array_replace(ARRAY[1,2,3], 2, NULL)    -> {1,NULL,3}
    /// array_replace(NULL::int[], 1, 2)        -> NULL
    /// ```
    #[test]
    fn array_replace_matches_and_writes_nulls_in_both_directions() {
        let a = text_list(&[
            Some(&[Some("a"), None, Some("c")]),
            Some(&[Some("a"), Some("b"), Some("a")]),
            None,
        ]);
        let from = texts(vec![None, Some("a"), Some("a")]);
        let to = texts(vec![Some("Z"), None, Some("Z")]);
        let got = ArrayReplace
            .invoke(&[a, from, to], &EvalSession::DEFAULT)
            .unwrap();
        assert_eq!(
            read_text_list(&got),
            vec![
                // A NULL `from` MATCHES the NULL element.
                Some(vec![Some("a".into()), Some("Z".into()), Some("c".into())]),
                // A NULL `to` WRITES a NULL element.
                Some(vec![None, Some("b".into()), None]),
                None,
            ]
        );
    }

    /// Length is preserved by `array_replace` and only ever shrinks under
    /// `array_remove` — invariant 3 (output length == input length) is about
    /// the *column*, not the arrays inside it, and both hold it.
    #[test]
    fn both_rewriters_return_one_row_per_input_row() {
        let s = &EvalSession::DEFAULT;
        let a = text_list(&[Some(&[Some("a")]), None, Some(&[])]);
        let n = texts(vec![Some("a"), Some("a"), Some("a")]);
        for got in [
            ArrayRemove.invoke(&[a.clone(), n.clone()], s).unwrap(),
            ArrayReplace.invoke(&[a.clone(), n.clone(), n], s).unwrap(),
        ] {
            assert_eq!(got.len(), a.len());
            assert_eq!(got.data_type(), a.data_type());
        }
    }

    /// The two forms disagree about the absent case, and that disagreement is
    /// the reason they are two functions. Live PostgreSQL 18.2:
    ///
    /// ```text
    /// array_position(ARRAY[1,2], 9)    -> NULL   absent is NULL, NOT 0
    /// array_positions(ARRAY[1,2], 9)   -> {}     EMPTY, not NULL
    /// array_position(ARRAY[1,NULL,3], NULL)  -> 2
    /// array_positions(ARRAY[1,NULL,1], NULL) -> {2}
    /// array_positions(NULL::int[], 1)  -> NULL
    /// array_positions(ARRAY[]::int[],1)-> {}
    /// ```
    #[test]
    fn the_singular_answers_null_where_the_plural_answers_an_empty_array() {
        let s = &EvalSession::DEFAULT;
        // row 0: found at 1 and 3   row 1: absent   row 2: NULL array
        // row 3: empty array        row 4: NULL element matched
        let a = text_list(&[
            Some(&[Some("a"), Some("b"), Some("a")]),
            Some(&[Some("b")]),
            None,
            Some(&[]),
            Some(&[Some("x"), None, Some("x")]),
        ]);
        let needle = texts(vec![Some("a"), Some("a"), Some("a"), Some("a"), None]);

        let one = ArrayPosition
            .invoke(&[a.clone(), needle.clone()], s)
            .unwrap();
        assert_eq!(
            seen_i32(&one),
            vec![Some(1), None, None, None, Some(2)],
            "absent is NULL and NOT 0; a NULL element IS found"
        );

        let all = ArrayPositions.invoke(&[a, needle], s).unwrap();
        assert_eq!(
            read_int_list(&all),
            vec![
                Some(vec![1, 3]),
                // The row that differs: EMPTY, where the singular is NULL.
                Some(vec![]),
                // A NULL array is NULL in both.
                None,
                Some(vec![]),
                Some(vec![2]),
            ]
        );
    }

    /// The three-argument form's `start` clamps below and finds nothing past
    /// the end. Live: `(…,1,0)` and `(…,1,-5)` are both 1, `(…,1,2)` is 3 and
    /// `(…,1,9)` is NULL.
    #[test]
    fn a_start_below_one_clamps_and_a_start_past_the_end_finds_nothing() {
        let s = &EvalSession::DEFAULT;
        for (start, want) in [
            (0, Some(1)),
            (-5, Some(1)),
            (1, Some(1)),
            (2, Some(3)),
            (9, None),
        ] {
            let a = text_list(&[Some(&[Some("a"), Some("b"), Some("a")])]);
            let got = ArrayPositionStart
                .invoke(&[a, texts(vec![Some("a")]), i32s(vec![Some(start)])], s)
                .unwrap();
            assert_eq!(
                seen_i32(&got),
                vec![want],
                "array_position(ARRAY[a,b,a], 'a', {start})"
            );
        }
    }

    /// A NULL `start` is the only input this module REJECTS, and the
    /// divergence is pinned with both answers.
    ///
    /// ```text
    /// select array_position(ARRAY[1,2,1],1,NULL::int);
    ///   ERROR:  initial position must not be null      -- SQLSTATE 22004
    /// ```
    ///
    /// Basin raises `ExecError::TypeMismatch` with that exact message. The
    /// message matches, the SQLSTATE does not (`TypeMismatch` is not 22004),
    /// and Basin fails the whole batch where PostgreSQL fails per row. All
    /// three are inherited, not introduced. See `arr_fns.integration.md` §6 D3.
    #[test]
    fn a_null_start_is_an_error_whose_message_matches_and_whose_class_does_not() {
        let a = text_list(&[Some(&[Some("a")]), Some(&[Some("a")])]);
        // Only the SECOND row's start is NULL — PostgreSQL would answer row 0
        // and then fail; Basin fails the whole batch.
        let err = ArrayPositionStart
            .invoke(
                &[
                    a,
                    texts(vec![Some("a"), Some("a")]),
                    i32s(vec![Some(1), None]),
                ],
                &EvalSession::DEFAULT,
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                ExecError::TypeMismatch(ref m) if m == "initial position must not be null"
            ),
            "message is verbatim PostgreSQL's; the SQLSTATE class is not. got {err:?}"
        );
    }

    /// `array_positions` returns `int4[]` whatever the element type — so it
    /// must keep the catalog-backed default rather than typing from its array
    /// argument the way the six array-returning functions do. Getting this
    /// backwards would type `array_positions(text[], text)` as `text[]`.
    #[test]
    fn array_positions_returns_int4_array_whatever_it_searched() {
        let text_array = PgType::new(Oid(1009)); // text[]
        let int4_array = PgType::new(Oid(1007));
        assert_eq!(
            ArrayPositions
                .return_type(&[text_array, PgType::TEXT])
                .unwrap(),
            int4_array,
            "the default is CORRECT here — both FUNCS rows say int4[]"
        );
        assert_eq!(
            ArrayPosition
                .return_type(&[text_array, PgType::TEXT])
                .unwrap(),
            PgType::INT4,
            "and the singular is a plain integer"
        );
    }

    /// A non-array argument is a planner bug and is reported as one rather
    /// than improvised around.
    #[test]
    fn a_non_array_argument_is_a_type_mismatch() {
        let err = Cardinality
            .invoke(&[texts(vec![Some("not an array")])], &EvalSession::DEFAULT)
            .unwrap_err();
        assert!(
            matches!(err, ExecError::TypeMismatch(ref m) if m.contains("cardinality expects an array")),
            "got {err:?}"
        );
    }
}
