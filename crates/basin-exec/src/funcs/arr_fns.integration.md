# `arr_fns` — integration notes for the array slice

**Status: WRITING. This file is updated as each function lands, so it is
useful even if the slice is interrupted.**

---

## 0. Slice state — READ THIS FIRST

This file was written by a slice that **died before writing
`arr_fns.rs` at all**. Everything below §1 was already here and has been
re-verified against the tree and against live PostgreSQL 18.2 by the slice that
picked it up; the corrections that verification produced are marked
**[corrected]** inline. The `.rs` file is being written now, in batches, and
this section is the only place that says how far it got.

| batch | structs | in `arr_fns.rs`? |
|---|---|---|
| 1 | `ArrayLength`, `Cardinality`, `ArrayNdims`, `ArrayReverse` | **YES** — `rustfmt --edition 2021` clean |
| 2 | `ArrayAppend`, `ArrayPrepend`, `ArrayCat` | **YES** — `rustfmt --edition 2021` clean |
| 3 | `ArrayRemove`, `ArrayReplace` | **YES** — `rustfmt --edition 2021` clean |
| 4 | `ArrayPosition`, `ArrayPositionStart`, `ArrayPositions` | **YES** — `rustfmt --edition 2021` clean |

**All 12 are written.** `grep -n "pub struct" arr_fns.rs` lists exactly the
twelve in §3's table, in that order. §1's registrations can now be applied.

**If `arr_fns.rs` does not exist, §1's registrations must NOT be applied** —
`register_scalar` would not compile. The `.md` is ahead of the `.rs` by
construction and that is the interruption-safe order, not a defect.

### What the re-pickup verified, beyond writing the `.rs`

* **Every `psql` claim in §6 was re-measured** against
  `postgres://pc@127.0.0.1:5432/postgres`, `PostgreSQL 18.2 (Homebrew) on
  aarch64-apple-darwin24.6.0`. All of D1–D6 reproduced exactly, including the
  three that matter most: `array_position('[5:7]={a,b,c}'::text[],'b')` = **6**
  (subscript, not ordinal), `array_position('[5:7]={a,b,c}'::text[],'a',1)` =
  **5** (the `start` clamps up to the LOWER BOUND, not to 1), and
  `array_prepend('z','[5:7]={a,b,c}'::text[])` = `[5:8]={z,a,b,c}` (the new
  element takes subscript 5 and the array grows UPWARD). **No claim in §6 was
  found wrong.**
* **All 12 `pg_proc` rows exist** in `basin_pgtype::func::FUNCS`
  (`crates/basin-pgtype/src/func.rs:2276–2450`), each twice — `int4[]` and
  `text[]` — so `register_scalar`'s catalog assertion passes. §7's claim that
  the two rows carry *different* `ret` for the array-returning six is confirmed
  at the source: `FuncSig::new(6381, "array_reverse", &[INT4_ARRAY], INT4_ARRAY,
  …)` precedes the `TEXT_ARRAY` row, so `catalog_row(6381).ret` is `Oid(1007)`.
  That is asserted directly in
  `array_reverse_types_from_its_argument_not_from_the_first_catalog_row`.
* **`crate::operator::ExecError` and `crate::ExecError` are the same type** —
  `crates/basin-exec/src/lib.rs:54` re-exports it. NEEDS VERIFICATION item 1's
  second failure mode is closed.
* **The `Project::new` zero-row-batch hazard does not apply to this family.**
  A sibling slice found that `Project::new` fixes a projection's output schema
  by evaluating against a zero-row batch, which breaks any function whose
  result TYPE depends on an argument's VALUE. Every `return_type` here reads a
  declared `PgType` and every `invoke` derives its output element type from the
  input's Arrow `DataType` — both present in a zero-row batch. Nothing in this
  slice inspects a value to decide a type, so all 12 ports are safe through
  that ABI. Recorded because arrays are the family most exposed to it and the
  question will be asked again.

### Line numbers, corrected against the tree at `d65abd17`

The line references below were written against an earlier state and three of
them had drifted. Corrected:

| §  | said | actually |
|---|---|---|
| 2a | orphan-block arms at `~3001–3021` | **`2929–2950`** |
| 2b | `array_length` unit test at `eval.rs:7270` | **`eval.rs:7197`** (`array_length_is_null_for_an_empty_array_and_for_a_missing_dimension`, driving the batch built by `batch_text_list` at **`eval.rs:7147`**) |
| 2b | `OID_ARRAY_LENGTH` const | `eval.rs:320` — correct, and it is in the *string/measurement* const block, not with the other eleven at `503–513` |

The arm at `eval.rs:2758` (`OID_ARRAY_LENGTH`) is correct as written.

**Match on the TEXT below, never on the line number.** `eval.rs` is being
edited by several agents at once and the numbers move under you: the
`OID_ARRAY_APPEND` arm was at 3001 when this file was drafted, at 2929 when the
port started and at **2883** an hour later, without anything touching the array
family at all. The arm text in §2a was re-read at the tip and is
character-for-character exact; that is the durable half.

This slice ports **12** of the 19 array arms out of `eval_scalar_fn`'s `match`
in `crates/basin-exec/src/eval.rs` and into
`crates/basin-exec/src/funcs/arr_fns.rs`.

The slice owns exactly two files, both new:

* `crates/basin-exec/src/funcs/arr_fns.rs`
* `crates/basin-exec/src/funcs/arr_fns.integration.md` (this file)

It does **not** touch `eval.rs` or `funcs/mod.rs`. The two edits it cannot make
itself are written out verbatim below.

---

## 1. Edit to `crates/basin-exec/src/funcs/mod.rs`

Add the module declaration alongside the existing three (they are in
alphabetical order; `arr_fns` sorts first):

```rust
pub mod arr_fns;
pub mod dt_fns;
pub mod num_fns;
pub mod str_fns;
```

Then append these registrations inside `builtins()`, after the `dt_fns` block
and before the trailing `r`:

```rust
        // arr_fns — ported by the wave-16 array slice.
        r.register_scalar(Box::new(arr_fns::ArrayLength));
        r.register_scalar(Box::new(arr_fns::Cardinality));
        r.register_scalar(Box::new(arr_fns::ArrayNdims));
        r.register_scalar(Box::new(arr_fns::ArrayReverse));
        r.register_scalar(Box::new(arr_fns::ArrayAppend));
        r.register_scalar(Box::new(arr_fns::ArrayPrepend));
        r.register_scalar(Box::new(arr_fns::ArrayCat));
        r.register_scalar(Box::new(arr_fns::ArrayRemove));
        r.register_scalar(Box::new(arr_fns::ArrayReplace));
        r.register_scalar(Box::new(arr_fns::ArrayPosition));
        r.register_scalar(Box::new(arr_fns::ArrayPositionStart));
        r.register_scalar(Box::new(arr_fns::ArrayPositions));
```

`funcs/mod.rs` has a test that asserts the hosted count:

```rust
    fn the_registry_reports_what_is_actually_hosted() {
        assert_eq!(
            builtins().len(),
            35,
```

**[corrected] The number is a moving target and this slice is not the only one
moving it.** The earlier draft said "24 becomes 36". It was 24 when that was
written; by the time `arr_fns.rs` was finished the `str_fns` slice had landed
its eleven and `mod.rs:251` asserts **35**. Adding these twelve makes it
**47** — *if nothing else lands first.*

`crates/basin-exec/src/funcs/` currently holds three more unapplied slices
(`numx_fns.rs`, `trig_fns.rs`, `extract_fns.rs`, each with its own
`.integration.md`), so whoever applies this should **read the asserted number
in `mod.rs` and add 12**, not trust either figure written here. The message
beside it follows the established form: `"<n> hosted: … and the 12 array
ports. Read from the registry, never tracked by hand"`.

---

## 2. Edits to `crates/basin-exec/src/eval.rs`

### 2a. Match arms to DELETE

One arm at line ~2758, in the string/measurement block:

```rust
        OID_ARRAY_LENGTH => eval_array_length(&a(0)?, &a(1)?),
```

and this contiguous run out of the "DataFusion orphans" block at ~3001–3021:

```rust
        OID_ARRAY_APPEND => {
            eval_array_add_element(&a(0)?, &a(1)?, false, "array_append")
        }
        OID_ARRAY_PREPEND => {
            // Note the argument order: `array_prepend(element, array)`, the
            // reverse of `array_append`. `basin_pgtype::func::FUNCS` has a
            // test asserting `array_prepend(array, element)` does NOT resolve,
            // for exactly this reason.
            eval_array_add_element(&a(1)?, &a(0)?, true, "array_prepend")
        }
        OID_ARRAY_CAT => eval_array_cat(&a(0)?, &a(1)?),
        OID_ARRAY_REMOVE => eval_array_remove(&a(0)?, &a(1)?),
        OID_ARRAY_REPLACE => eval_array_replace(&a(0)?, &a(1)?, &a(2)?),
        OID_ARRAY_POSITION => eval_array_position(&a(0)?, &a(1)?, None),
        OID_ARRAY_POSITION_START => {
            let start = a(2)?;
            eval_array_position(&a(0)?, &a(1)?, Some(&start))
        }
        OID_ARRAY_POSITIONS => eval_array_positions(&a(0)?, &a(1)?),
        OID_ARRAY_NDIMS => eval_array_ndims(&a(0)?),
        OID_CARDINALITY => eval_cardinality(&a(0)?),
        OID_ARRAY_REVERSE => eval_array_reverse(&a(0)?),
```

**The `OID_ARRAY_SORT_1/2/3`, `OID_ARRAY_TO_STRING_2/3` and
`OID_STRING_TO_ARRAY_2/3` arms immediately below them STAY.** They are the
seven arms this slice deliberately did not take — see §5.

### 2b. `const`s that become unused

These are `const` items, so an unused one is a `dead_code` warning, not an
error. Delete them with their arms:

```rust
const OID_ARRAY_LENGTH: u32 = 2176; // array_length(anyarray, integer)
const OID_ARRAY_APPEND: u32 = 378; // array_append(anycompatiblearray, anycompatible)
const OID_ARRAY_PREPEND: u32 = 379; // array_prepend(anycompatible, anycompatiblearray)
const OID_ARRAY_CAT: u32 = 383; // array_cat(anycompatiblearray, anycompatiblearray)
const OID_ARRAY_REMOVE: u32 = 3167; // array_remove(anycompatiblearray, anycompatible)
const OID_ARRAY_REPLACE: u32 = 3168; // array_replace(anycompatiblearray, anycompatible, anycompatible)
const OID_ARRAY_POSITION: u32 = 3277; // array_position(anycompatiblearray, anycompatible)
const OID_ARRAY_POSITION_START: u32 = 3278; // array_position(anycompatiblearray, anycompatible, integer)
const OID_ARRAY_POSITIONS: u32 = 3279; // array_positions(anycompatiblearray, anycompatible)
const OID_ARRAY_NDIMS: u32 = 748; // array_ndims(anyarray)
const OID_CARDINALITY: u32 = 3179; // cardinality(anyarray)
const OID_ARRAY_REVERSE: u32 = 6381; // array_reverse(anyarray)
```

**`OID_ARRAY_LENGTH` has one other user**: the unit test
`array_length_is_null_for_an_empty_array_and_for_a_missing_dimension` at
eval.rs:7270 builds `sf(OID_ARRAY_LENGTH, …)`. That test must keep working —
it is the single strongest piece of evidence this port produces, because it
goes through `eval()` and will now be answered by the registry. Either keep
the `const` (it is then used only by the test, so move it into the test module
or mark it `#[allow(dead_code)]`) or inline `2176` at the two call sites. **Do
not delete the test.**

### 2c. `fn`s that become unused (delete with the arms)

Bodies of the ported functions — copied verbatim into `arr_fns.rs`, so these
are now dead:

```
fn eval_array_length      (eval.rs:1607)
fn eval_array_add_element (eval.rs:1788)
fn eval_array_cat         (eval.rs:1832)
fn eval_array_remove      (eval.rs:1870)
fn eval_array_replace     (eval.rs:1899)
fn eval_array_position    (eval.rs:1958)
fn eval_array_positions   (eval.rs:2014)
fn eval_cardinality       (eval.rs:2054)
fn eval_array_ndims       (eval.rs:2071)
fn eval_array_reverse     (eval.rs:2087)
```

Shared helpers — check each before deleting, because the seven arms left
behind still use some of them:

| helper | eval.rs | still used after this slice? |
|---|---|---|
| `require_list` | 1577 | **YES** — `eval_array_sort`, `eval_array_to_string`, `eval_subscript` |
| `flatten_list` | 1696 | **YES** — `eval_array_sort`, `eval_array_to_string` |
| `assemble_list` | 1755 | **YES** — `eval_array_sort` |
| `expand_per_row` | 1720 | **NO** — delete |
| `elements_not_distinct` | 1737 | **NO** — delete |
| `map_arrow` | 6673 | **YES** — the whole file |

`expand_per_row` and `elements_not_distinct` exist only for the search family
(`remove`/`replace`/`position`/`positions`), all four of which this slice
takes. They become dead and should go.

**[verified] `grep -rn --include='*.rs' <helper> crates tests`, run at the tip,
confirms every row of that table.** The call sites, excluding `arr_fns.rs`
itself:

```
expand_per_row        eval.rs:1743 only  — inside elements_not_distinct
elements_not_distinct eval.rs:1873 array_remove   1913 array_replace
                              1965 array_position 2017 array_positions
                      → all four are ported, so it is DEAD
require_list          …2137 array_sort  2211 array_to_string  2359 subscript
                      → STAYS
flatten_list          …2138 array_sort  2212 array_to_string
                      → STAYS
assemble_list         …1816 add_element 1855 cat 1883 remove 1899 replace
                              2098 reverse  ← all ported
                              2178 array_sort  ← the only survivor
                      → STAYS, for array_sort alone
```

So `expand_per_row` dies only because `elements_not_distinct` does, and
`elements_not_distinct` dies only because all four of its callers move
together. **Taking three of the four search functions instead of four would
leave both helpers alive** — that is the reason §3 gives for taking the whole
search family, and it is now evidence rather than an assertion.

The array family's long header comment at eval.rs:1632–1682 documents twelve
functions, ten of which move here. It should be trimmed to the three that
remain (`array_sort`, `array_to_string`, `string_to_array`) with a pointer to
`crate::funcs::arr_fns`, or moved wholesale — the measured rules table in it is
reproduced in `arr_fns.rs`'s module doc, so nothing is lost either way.

---

## 3. What was ported (12)

| struct | oid | signature |
|---|---|---|
| `ArrayLength` | 2176 | `array_length(anyarray, integer) -> integer` |
| `Cardinality` | 3179 | `cardinality(anyarray) -> integer` |
| `ArrayNdims` | 748 | `array_ndims(anyarray) -> integer` |
| `ArrayReverse` | 6381 | `array_reverse(anyarray) -> anyarray` |
| `ArrayAppend` | 378 | `array_append(anycompatiblearray, anycompatible)` |
| `ArrayPrepend` | 379 | `array_prepend(anycompatible, anycompatiblearray)` |
| `ArrayCat` | 383 | `array_cat(anycompatiblearray, anycompatiblearray)` |
| `ArrayRemove` | 3167 | `array_remove(anycompatiblearray, anycompatible)` |
| `ArrayReplace` | 3168 | `array_replace(anycompatiblearray, anycompatible, anycompatible)` |
| `ArrayPosition` | 3277 | `array_position(anycompatiblearray, anycompatible)` |
| `ArrayPositionStart` | 3278 | `array_position(anycompatiblearray, anycompatible, integer)` |
| `ArrayPositions` | 3279 | `array_positions(anycompatiblearray, anycompatible)` |

All twelve have `pg_proc` rows in `basin_pgtype::func::FUNCS` (checked: each
oid appears twice, once monomorphized at `int4[]` and once at `text[]`), so
`register_scalar`'s catalog assertion passes.

### Why these twelve

They already have tests, which is the evidence a port is supposed to produce:

* `crates/basin-exec/tests/orphan_functions.rs` — the differential battery
  against live PostgreSQL. It drives `eval()` for `array_length`,
  `cardinality`, `array_ndims`, `array_reverse`, `array_position`,
  `array_positions`, `array_remove`, `array_append`, `array_prepend`,
  `array_cat`, `array_replace` over an edge-case matrix (`int_array_cases()` /
  `text_array_cases()` include NULL arrays, empty arrays and NULL elements).
  Every one of them now runs through the registry, unchanged.
* `tests/integration/tests/array_fns.rs` — the same family end to end through
  SQL.
* `eval.rs:7270` `array_length_is_null_for_an_empty_array_and_for_a_missing_dimension`
  — a unit test through `eval()` (see §2b: keep it).

They are also closed over their helpers in one direction: the four search
functions are the *only* users of `expand_per_row`/`elements_not_distinct`, so
taking all four leaves those two dead rather than half-used.

---

## 4. Verbatim move

Every function body in `arr_fns.rs` is a **character-for-character copy** of
the corresponding `eval.rs` body, and the free functions keep their `eval_`
prefixed names for exactly that reason — `diff <(sed -n '1696,2106p'
crates/basin-exec/src/eval.rs) …` shows the bodies are identical. The only
things added are the `ScalarFunc` wrapper structs and the argument plumbing.
Copied helpers (`map_arrow`, `require_list`, `flatten_list`, `expand_per_row`,
`elements_not_distinct`, `assemble_list`) are likewise verbatim; they are
private to `eval.rs` and cannot be reached from `funcs/`. `num_fns.rs` already
set that precedent with its own copy of `map_arrow`, and its note applies here
too: this should end up shared once a third family needs it, which is a change
to `eval.rs` that no slice may make while six agents share one tree.

---

## 5. What was deliberately NOT ported (7)

`OID_ARRAY_SORT_1` (6388), `OID_ARRAY_SORT_2` (6389), `OID_ARRAY_SORT_3`
(6390), `OID_ARRAY_TO_STRING_2` (395), `OID_ARRAY_TO_STRING_3` (384),
`OID_STRING_TO_ARRAY_2` (394), `OID_STRING_TO_ARRAY_3` (376).

The brief said eight to twelve, not nineteen. These seven are the coherent
remainder: `array_sort` carries the collation divergence (a crate-wide
facility, see eval.rs:2123), and the two `*_to_*` pairs are the text-bridge
half of the family. They share `require_list`/`flatten_list`/`assemble_list`
with what moved, which is why those three helpers must stay in `eval.rs` for
now.

---

## 6. PostgreSQL divergences

Everything below was measured this session on
`postgres://pc@127.0.0.1:5432/postgres`, `PostgreSQL 18.2 (Homebrew) on
aarch64-apple-darwin24.6.0`. Nothing here is fixed by this slice — a port moves
behaviour.

### D1. Non-1 lower bounds: `array_position`/`array_positions` return a SUBSCRIPT, Basin returns an ORDINAL

The sharpest one in the family, and it is invisible until an array has a lower
bound other than 1.

```
$ psql -At
select 'lb array',            '[5:7]={a,b,c}'::text[]::text;                   -> [5:7]={a,b,c}
select 'array_lower lb',      array_lower('[5:7]={a,b,c}'::text[],1);          -> 5
select 'array_position lb',   array_position('[5:7]={a,b,c}'::text[],'b');     -> 6
select 'array_positions lb',  array_positions('[5:7]={a,b,c}'::text[],'b');    -> {6}
select 'subscript lb[6]',     ('[5:7]={a,b,c}'::text[])[6];                    -> b
select 'pos lb start 6',      array_position('[5:7]={a,b,c}'::text[],'c',6);   -> 7
select 'pos lb start 1',      array_position('[5:7]={a,b,c}'::text[],'a',1);   -> 5
```

PostgreSQL's `array_position` is documented as returning the **subscript**, not
the ordinal, and the `start` argument is a **subscript** too (clamped up to the
lower bound, not to 1). Basin computes `k - start_off + 1`, i.e. the 1-based
ordinal, and clamps `start` to 1.

**Basin cannot express the input at all**, which is why this is a gap and not a
live wrong answer: `basin_pgtype::physical` maps a Postgres array type to
Arrow's `ListArray`, which has no lower-bound concept — every array Basin holds
begins at subscript 1. So for every value that can reach these functions,
subscript == ordinal and Basin agrees with the server. Recorded because the day
a lower bound becomes representable, `array_position`, `array_positions` and
the `start` clamp are all wrong, and it is not obvious from reading them.

The other functions survive a non-1 lower bound (they preserve it rather than
indexing by it), which is why they are not in this entry:

```
array_append('[5:7]={a,b,c}'::text[],'d')            -> [5:8]={a,b,c,d}
array_prepend('z','[5:7]={a,b,c}'::text[])           -> [5:8]={z,a,b,c}
array_cat('[5:7]={a,b,c}'::text[], ARRAY['q'])       -> [5:8]={a,b,c,q}
array_remove('[5:7]={a,b,c}'::text[],'b')            -> [5:6]={a,c}
array_reverse('[5:7]={a,b,c}'::text[])               -> [5:7]={c,b,a}
array_replace('[5:7]={a,b,c}'::text[],'b','Z')       -> [5:7]={a,Z,c}
array_length('[5:7]={a,b,c}'::text[],1)              -> 3
cardinality('[5:7]={a,b,c}'::text[])                 -> 3
```

Note `array_prepend` keeps the lower bound at 5 and gives the new element
subscript 5 (the array becomes `[5:8]`), rather than extending downward to
`[4:7]`. On an ordinary 1-based array both `array_append` and `array_prepend`
leave `array_lower(…,1) = 1`.

### D2. Multi-dimensional arrays: PostgreSQL RAISES, Basin cannot represent them

```
select array_position(ARRAY[[1,2],[3,4]],1);
  ERROR:  searching for elements in multidimensional arrays is not supported
select array_positions(ARRAY[[1,2],[3,4]],1);
  ERROR:  searching for elements in multidimensional arrays is not supported
select array_remove(ARRAY[[1,2],[3,4]],1);
  ERROR:  removing elements from multidimensional arrays is not supported
select array_append(ARRAY[[1,2],[3,4]],5);
  ERROR:  argument must be empty or one-dimensional array

select array_reverse(ARRAY[[1,2],[3,4]]);   -> {{3,4},{1,2}}   (reverses dim 1)
select array_cat(ARRAY[[1,2]],ARRAY[[3,4]]);-> {{1,2},{3,4}}
select cardinality(ARRAY[[1,2],[3,4]]);     -> 4               (ALL dimensions)
select array_length(ARRAY[[1,2],[3,4]],1);  -> 2               (first only)
select array_length(ARRAY[[1,2],[3,4]],2);  -> 2
select array_ndims(ARRAY[[1,2],[3,4]]);     -> 2
```

`cardinality` counting across all dimensions while `array_length(a,1)` counts
only the first is the documented difference between them, and it is
unobservable in Basin for the same reason as D1: no multi-dimensional physical
type exists, so `array_ndims` can only ever answer `1` or NULL and
`cardinality` is always the row length. `orphan_functions.rs` already passes
multi-dimensional literals as `ArgVal::Unrepresentable` rather than as a case
Basin could fail (see its docs at ~line 208).

The four ERROR rows are the ones that will need real work if multi-dimensional
arrays ever land: Basin would have to *raise* there, and today's code would
silently answer.

### D3. `array_position(a, e, NULL)` is an ERROR, and the SQLSTATE does not match

```
select array_position(ARRAY[1,2,1],1,NULL::int);
  ERROR:  initial position must not be null            -- SQLSTATE 22004
```

Basin raises `ExecError::TypeMismatch("initial position must not be null")` —
the message is verbatim, the class is not. Same shape as the divergence
`num_fns.rs` records for `abs` overflow: the error-vs-success *outcome* matches,
which is all `orphan_functions.rs` compares, while a wire client sees the wrong
SQLSTATE. Fixing it means an `ExecError` variant carrying a SQLSTATE and an arm
in `basin-router`'s `error.rs`, which is two crates this slice does not touch.

Also note it errors for the **whole batch** in Basin (the check is
`s.null_count() > 0` over the argument array), where PostgreSQL errors per row.
For a scalar `start` that is the same thing; for a column of starts where only
some rows are NULL, PostgreSQL would produce rows before failing. Basin's
operators are batch-at-a-time, so this is pre-existing and family-wide, not
something the port introduced.

### D4. `array_length`, `array_ndims`, `cardinality` and the empty array

Confirmed, and it is the trap the brief named. `array_length` needs its
DIMENSION argument and answers NULL — never 0 — for an empty array:

```
array_length(ARRAY['x','y'],1)   -> 2
array_length(ARRAY['x','y'],2)   -> NULL     no such dimension
array_length(ARRAY['x','y'],0)   -> NULL     dimensions are 1-based
array_length(ARRAY['x','y'],-1)  -> NULL
array_length(ARRAY[]::text[],1)  -> NULL     an EMPTY array has NO dimensions
array_length(NULL::text[],1)     -> NULL
array_length(ARRAY[1,NULL,3],1)  -> 3        a NULL ELEMENT still occupies a slot
array_length(ARRAY['x','y'],NULL)-> NULL
cardinality(ARRAY[]::int[])      -> 0        <-- disagrees with array_length
cardinality(NULL::int[])         -> NULL
cardinality(ARRAY[1,NULL,3])     -> 3
array_ndims(ARRAY[]::int[])      -> NULL     ZERO dimensions, not 1 and not 0
array_ndims(NULL::int[])         -> NULL
array_ndims(ARRAY[1,2])          -> 1
```

Basin agrees with every row. **No divergence** — recorded because `0` is the
natural reading of three of them and the wrong answer, and because it is the
reason `eval_array_length` and `eval_cardinality` are separate functions that
must never be unified behind one helper.

### D5. The non-strict functions, and `array_remove(a, NULL)`

Also all agreeing, also all counter-intuitive:

```
array_append(NULL::int[],1)       -> {1}        not NULL: the function is NOT strict
array_append(NULL::int[],NULL)    -> {NULL}     a ONE-element array containing NULL
array_append(ARRAY[]::int[],1)    -> {1}
array_append(ARRAY[1,2],NULL)     -> {1,2,NULL}
array_prepend(1,NULL::int[])      -> {1}
array_cat(NULL::int[],ARRAY[1])   -> {1}
array_cat(ARRAY[1],NULL::int[])   -> {1}
array_cat(NULL::int[],NULL::int[])-> NULL       NOT {} — absorbed on one side, not both
array_cat(ARRAY[]::int[],ARRAY[1])-> {1}
array_cat(ARRAY[]::int[],ARRAY[]::int[]) -> {}  and array_ndims of it is NULL
array_remove(ARRAY[1,NULL,3],NULL)-> {1,3}      NULLs ARE removed
array_remove(ARRAY[1,2],9)        -> {1,2}
array_remove(NULL::int[],1)       -> NULL       the array being NULL still wins
array_remove(ARRAY[]::int[],1)    -> {}
array_ndims(array_remove(ARRAY[1,1],1)) -> NULL removing everything gives a 0-dim array
array_replace(ARRAY[1,NULL,3],NULL,9) -> {1,9,3}
array_replace(ARRAY[1,2,3],2,NULL)    -> {1,NULL,3}
array_replace(NULL::int[],1,2)        -> NULL
array_position(ARRAY[1,NULL,3],NULL)  -> 2      a NULL element IS found
array_position(ARRAY[1,2],9)          -> NULL   absent is NULL, NOT 0
array_position(NULL::int[],1)         -> NULL
array_position(ARRAY[]::int[],1)      -> NULL
array_position(ARRAY[1,2,1],1,0)      -> 1      start below 1 clamps
array_position(ARRAY[1,2,1],1,-5)     -> 1
array_position(ARRAY[1,2,1],1,2)      -> 3
array_position(ARRAY[1,2,1],1,9)      -> NULL   past the end finds nothing
array_positions(ARRAY[1,2],9)         -> {}     EMPTY, not NULL — unlike the singular
array_positions(ARRAY[1,NULL,1],NULL) -> {2}
array_positions(NULL::int[],1)        -> NULL
array_positions(ARRAY[]::int[],1)     -> {}
array_reverse(NULL::int[])            -> NULL
array_reverse(ARRAY[]::int[])         -> {}
array_reverse(ARRAY[1,NULL,3])        -> {3,NULL,1}
```

`array_remove(a, NULL)` is the one the brief flagged: it reads as "remove
nothing" and it removes the NULLs, because element comparison is
`IS NOT DISTINCT FROM`, not `=`. Basin gets this right via
`arrow_ord::cmp::not_distinct`, which is that predicate exactly.

### D6. `anycompatible` widening: PostgreSQL casts, Basin raises

```
select array_append(ARRAY[1,2], 3.5);            -> {1,2,3.5}
select pg_typeof(array_append(ARRAY[1,2], 3.5)); -> numeric[]
select array_cat(ARRAY[1,2], ARRAY[3.5]);        -> {1,2,3.5}
select array_remove(ARRAY[1,2], 2.0);            -> {1}
select pg_typeof(array_replace(ARRAY[1,2],2,3.5))-> numeric[]
```

PostgreSQL's `anycompatible` resolution picks a common type and casts *both*
sides, so `array_append(int4[], numeric)` is a `numeric[]`. Basin's
`eval_array_add_element`/`eval_array_cat`/`eval_array_replace` compare
`data_type()`s and raise `ExecError::TypeMismatch` on a mismatch, and
`basin_pgtype::func::FUNCS` only tabulates the `int4[]` and `text[]`
monomorphizations, so `array_append(int4[], numeric)` most likely fails to
resolve before it ever reaches here. **Unchanged by the port** — the type check
is copied verbatim. Whether the failure is a resolution miss or a
`TypeMismatch` at eval time is on the NEEDS VERIFICATION list.

`array_remove` is the odd one out and worth noting: it has **no** type check at
all (`eval_array_remove` goes straight to `elements_not_distinct`), so a
mismatched element type surfaces as whatever `cmp::not_distinct` says rather
than as the family's own message. Copied as-is.

---

## 7. `return_type` — the one place the catalog-backed default is WRONG

`ScalarFunc::return_type`'s default answers from `catalog_row(oid)`, which is a
**linear scan that returns the first `FUNCS` row with that oid**. Every one of
these twelve oids has *two* rows — one monomorphized at `int4[]`, one at
`text[]` — and for the six functions that return an array, the two rows have
different `ret`:

```rust
    FuncSig::new(378, "array_append", &[oid::INT4_ARRAY, oid::INT4], oid::INT4_ARRAY, …),
    FuncSig::new(378, "array_append", &[oid::TEXT_ARRAY, oid::TEXT], oid::TEXT_ARRAY, …),
```

So the default would type `array_append(text[], text)` as `int4[]`, because the
`int4[]` row is first in the table. The trait's own doc says to override
"where one `pg_proc.prorettype` cannot express the answer", and this is that
case. `arr_fns.rs` therefore overrides `return_type` for `ArrayAppend`,
`ArrayPrepend`, `ArrayCat`, `ArrayRemove`, `ArrayReplace` and `ArrayReverse`,
each returning the declared type of its *array* argument (index 0, except
`ArrayPrepend` where the array is argument 1).

The six that return a fixed type keep the default, because both of their rows
agree: `ArrayLength`/`Cardinality`/`ArrayNdims`/`ArrayPosition`/
`ArrayPositionStart` return `integer` and `ArrayPositions` returns `integer[]`
whatever the element type (`basin_pgtype::func::func_rs` has a test on exactly
this, `array_length`/`cardinality`/`array_ndims`/`array_positions`).

**This is not a behaviour change.** Nothing calls `ScalarFunc::return_type`
today — `grep -rn return_type crates/basin-exec/src crates/basin-plan/src` finds
only the definition, its doc and two unrelated `basin-plan` tests — so the old
`match` path never consulted it and neither does the new one. It is written
correctly now so that wiring it later does not silently retype every polymorphic
array expression to `int4[]`.

---

## 8. NEEDS VERIFICATION

Everything here needs a `cargo` run, which this slice was forbidden from doing
(**CLASS A: never run cargo**). `arr_fns.rs` was written by copying bodies
verbatim and re-read for balance, but it has **never been compiled**.

1. **It compiles.** No `cargo check` was run — `rustfmt --edition 2021` parses
   the file clean, which proves the *syntax* and nothing about name resolution
   or types. Most likely failure points, in order: the import list
   (`arrow::compute::kernels::{cast, cmp}`, `arrow_select::{interleave, take}`,
   `arrow::buffer::{NullBuffer, OffsetBuffer}` — all copied from `eval.rs`'s own
   `use` block at `eval.rs:217–243`, which is the evidence they resolve from
   this crate). **[corrected] The `crate::operator::ExecError` vs
   `crate::ExecError` question is settled**: `lib.rs:54` is
   `pub use operator::{default_session, ExecError, …}`, so they are one type
   and `num_fns.rs`'s spelling is safe to follow.

   **[corrected] The `from_iter_primitive` risk the earlier draft listed here
   is gone.** The test module does not use it. It builds lists with
   `ListArray::try_new` and an explicit `Field::new("item", …, true)`, copied
   from `eval.rs:7147`'s `batch_text_list` — a helper that is already green in
   the existing suite, so the `"item"` field name is verified rather than
   assumed. That is what makes
   `an_array_result_keeps_the_physical_list_type_of_its_input` a real assertion:
   it compares the whole `DataType`, field name included.
2. **`funcs/mod.rs`'s count test.** See §1 — it read 24 when this file was
   started and 35 when `arr_fns.rs` was finished, because the `str_fns` slice
   landed in between. Add 12 to whatever it says at merge time. Three further
   unapplied slices are sitting in the same directory, so it will move again.
3. **`crates/basin-exec/tests/orphan_functions.rs` still passes**, and passes
   for the same reason — it needs `PG_DIFF_TEST_DSN` set to reach the live
   server. This is the port's primary evidence and it was NOT run.
4. **`eval.rs:7270`'s `array_length` unit test still passes** and is now served
   by the registry. If `OID_ARRAY_LENGTH` was deleted per §2b, that test no
   longer compiles — see the warning there.
5. **`tests/integration/tests/array_fns.rs`** end-to-end.
6. **D6's resolution question**: does `array_append(int4[], numeric)` fail in
   `basin_pgtype::func::resolve` (no row) or reach `invoke` and raise
   `TypeMismatch`? Not determined; it changes nothing about the port either
   way, but it decides which layer owns the eventual `anycompatible` fix.
7. **The `dead_code` warnings** listed in §2c are predictions from reading, not
   from a build. Check the actual warning list after deleting the arms.
