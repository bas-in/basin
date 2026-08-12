---
title: "DF Removal 04 — The Function Gap"
nav_section: architecture
sidebar_position: 40
summary: "Exact inventory of the SQL functions Basin gets from DataFusion's default library and must reimplement: 127 required, ~26k LOC."
tags: [migration, datafusion, sql, functions]
---

# DF Removal 04 — The Function Gap

- **Status:** Analysis, current as of 2026-08-12 (branch `feat/own-engine-remove-datafusion`)
- **Scope:** scalar / aggregate / window functions only. Table functions, the
  planner's `Unnest` node and operator rewriting are called out where they
  masquerade as functions, but sizing them belongs to other documents in this
  series.
- **Pinned versions:** DataFusion 53.1.0 (`datafusion = "53"` in the workspace
  root `Cargo.toml`), arrow-rs as vendored by that release.

## 1. What this document answers

`crates/basin-engine/src/session.rs` no longer literally calls
`with_default_features()` on the hot path — the comment at lines 2756–2777
explains that it was replaced with targeted setters for a 3–10 ms per-session
win. But the substance is unchanged: the engine still installs
`SessionStateDefaults::default_window_functions()` and
`SessionStateDefaults::default_table_functions()` verbatim, and
`build_stateless_udf_cache()` (session.rs:364) seeds the shared
`StatelessUdfCache` with DataFusion's default scalar + aggregate UDFs before
layering Basin's own on top. The comment at session.rs:2774 says so plainly:

> The cache includes DF's default scalar+agg UDFs alongside Basin's, so passing
> it here is the single source of both.

So the dependency is total. When `datafusion-functions`,
`-functions-nested`, `-functions-aggregate` and `-functions-window` leave the
tree, every name they carried disappears from Basin's SQL surface unless we
provide it.

## 2. What Basin already defines itself

Enumerated from every `ScalarUDFImpl::name` / `AggregateUDFImpl::name` /
`create_udf` site and every `register_udf` / `register_udaf` call under
`crates/basin-engine/src/`.

| Source file | Names | Notes |
|---|---:|---|
| `jsonb_udf.rs` | 53 | incl. 10 UDTFs (`jsonb_each`, `json_array_elements`, …) |
| `pg_catalog_udf.rs` | 47 | 26 bare + 21 `pg_catalog.`-qualified twins |
| `geo_glue.rs` | 43 | PostGIS `st_*` |
| `udf.rs` | 33 | auth, vector, crypt/digest, tablesample, `mod`, `length`, `power` |
| `range_udf.rs` | 30 | range/multirange constructors + predicates |
| `pg_scalar_aliases.rs` | 24 | `ceiling`, `sign`, `div`, `width_bucket`, `make_*`, session fns |
| `string_dt_udf.rs` | 19 | `btrim`, `ltrim`, `rtrim`, `regexp_match*`, `format`, `quote_*` |
| `fts_udf.rs` | 17 | `to_tsvector`, `ts_rank*`, `setweight`, … |
| `interval_tz_udf.rs` | 10 | `at_time_zone`, `justify_*`, `to_char_interval` |
| `pg_agg_udf.rs` | 9 (UDAF) | `array_agg`, `json_agg`, `mode`, `percentile_disc`, `first`, `last` |
| `datetime_extras.rs` | 9 | `array_dims`, `array_lower/upper`, `generate_subscripts`, `overlaps` |
| `datetime_more_udf.rs` | 8 | `age`, `date_bin`, `to_char`, `to_date`, `to_timestamp` |
| `operators/citext_cmp.rs` | 7 | `citext_eq` … |
| `advisory_lock.rs` | 6 | `pg_advisory_*` |
| `string_more_udf.rs` | 6 | `encode`, `decode`, `left`, `right`, `regexp_replace`, `format` |
| `jsonb_modify_udf.rs` | 5 | `jsonb_set`, `jsonb_insert`, `jsonb_pretty`, … |
| `seq_udf.rs` | 4 | `nextval`, `currval`, `setval`, `lastval` |
| `jsonb_path_udf.rs` | 4 | `jsonb_path_*` |
| `json_build_udf.rs` | 3 | `json_build_array`, `json_object`, `jsonb_object` |
| `trgm_glue.rs` | 3 | `similarity`, `word_similarity`, `show_trgm` |
| `approx_percentile.rs` / `approx_count_distinct.rs` | 3 (UDAF) | |
| `inet_udf.rs`, `net_glue.rs`, `cron_glue.rs`, `cancel_udf.rs`, `hypertable.rs`, `notify_registry.rs`, `regex_udf.rs` | 11 | |
| `wasm_udf.rs` | 0 fixed | name comes from user DDL |

**309 globally unique names** (321 file-scoped entries; 22 names are registered
from two files). Breakdown: ~284 scalar, 12 aggregate, 14 table functions,
**zero window functions** — `register_udwf` does not appear anywhere in the
workspace. Every window function Basin advertises is DataFusion's.

29 of those names deliberately shadow a DataFusion builtin (`ascii`, `btrim`,
`chr`, `date_bin`, `decode`, `digest`, `encode`, `left`, `length`, `lower`,
`ltrim`, `make_time`, `power`, `regexp_match`, `regexp_replace`, `reverse`,
`right`, `rtrim`, `split_part`, `to_char`, `to_date`, `to_timestamp`,
`translate`, `upper`, `version`, `array_agg`, `array_contains`, `array_dims`,
`arrays_overlap`, `approx_percentile_cont`) — these are already PG-semantics
overrides and cost nothing to keep.

## 3. What is actually reachable — the evidence

### 3.1 `pg_operators.rs` rewrite targets (highest signal)

These are not optional. `pg_operators.rs` textually rewrites PostgreSQL
operators into named function calls before the planner ever runs; if the target
name is unresolvable the query is a hard error. Extracted from the non-test
region (lines 1–7601):

| PG surface | Rewrites to | Provider today |
|---|---|---|
| `~`, `!~`, `~*`, `!~*` | `regexp_like(lhs, rhs[, 'i'])` | **DF** |
| `@>`, `<@` (arrays) | `list_has_all(...)` | **DF** (alias of `array_has_all`) |
| `&&` (arrays) | `arrays_overlap(...)` | Basin (`datetime_extras.rs`) |
| `ARRAY[...]`, `IN (...)` → array | `make_array(...)` | **DF** |
| citext comparison / `ORDER BY` | `lower(...)` | Basin |
| `>= ALL`, `> ALL` | `array_max(...)` | **DF** |
| `<= ALL`, `< ALL` | `array_min(...)` | **DF** |
| `OVERLAPS` | `overlaps(...)` | Basin |
| `LATERAL unnest(...)` | `unnest(...)` | **DF planner node**, not a UDF |
| `LATERAL generate_series(...)` | `generate_series(...)` | **DF** (scalar + table form) |
| `agg(...) FILTER (WHERE ...)` | whitelist at pg_operators.rs:4134 | mixed |

The FILTER whitelist itself names: `json_agg`, `jsonb_agg`, `array_agg`,
`count`, `sum`, `avg`, `min`, `max`, `bool_and`, `bool_or`, `string_agg`,
`array_to_string`, `every`, `variance`, `stddev`, `var_pop`, `var_samp`,
`stddev_pop`, `stddev_samp`. Of those, only `json_agg`, `jsonb_agg` and
`array_agg` are Basin's; the rest are DataFusion's.

### 3.2 The published contract

`docs/sql-support.md`, `CAPABILITIES.md`, `docs/functions.md` and
`docs/sql-compatibility.md` together claim **347 distinct function names**.
The categories that map onto DataFusion builtins: string (49 claimed), math
(23), date/time (37), array (26), regex (5), aggregate (44), window (11),
conversion/system (58).

### 3.3 Tests

A sweep of ~11,300 SQL fragments across 469 test files in `tests/` and
`testing/` finds **358 distinct SQL functions actually called**, ~4,050 call
sites. The breadth file is
`tests/integration/tests/pg_scalar_fn_inventory.rs`.
Heaviest DF-provided names by call count: `count` (660), `sum` (155),
`avg` (28), `coalesce` (26), `max` (25), `array_agg` (19), `min` (15),
`cardinality` (15), `date_trunc` (15), `lag` (15), `substring` (15),
`row_number` (12), `rank` (10), `string_agg` (10).

## 4. The delta

DataFusion 53.1.0's default library (scalar + nested + aggregate + window,
including aliases) exposes **245 names** reachable from Basin's SQL surface.
Subtracting the 29 Basin already shadows leaves **215 names Basin does not
define**.

Splitting those 215 against the evidence in §3:

- **127 are required** — named by a rewriter, the published contract, or an
  existing test.
- **88 are DataFusion-only** and can be dropped without breaking any documented
  or tested behaviour (see §4.9).

### 4.1 String — 22

`bit_length`, `char_length`, `character_length`, `concat`, `concat_ws`,
`ends_with`, `initcap`, `lpad`, `md5`, `octet_length`, `overlay`, `position`,
`repeat`, `replace`, `rpad`, `sha256`, `starts_with`, `strpos`, `substr`,
`substring`, `to_hex`, `trim`

arrow-rs coverage is good: `arrow_string::length::{length, bit_length}`,
`arrow_string::substring::substring`, `arrow_string::concat_elements`. `md5` /
`sha256` reduce to the `md-5` / `sha2` crates, both already in the tree behind
`udf.rs`'s `digest`. The genuinely fiddly ones are `overlay` (4-arg, char vs
byte offsets) and `lpad`/`rpad` (multi-char fill, grapheme-correct).

### 4.2 Math — 25

`abs`, `acos`, `asin`, `atan`, `atan2`, `ceil`, `cos`, `degrees`, `exp`,
`factorial`, `floor`, `gcd`, `lcm`, `ln`, `log`, `log10`, `pi`, `radians`,
`random`, `round`, `signum`, `sin`, `sqrt`, `tan`, `trunc`

No arrow kernel for transcendentals, but none is needed: each is a
`arrow::compute::kernels::arity::unary()` map over a `Float64Array`. `abs` has
`arrow::compute::kernels::numeric::neg`-adjacent support. The only non-trivial
members are 2-arg `round`/`trunc` (scale argument, banker's-rounding parity
with PG) and `log(base, x)`. `signum` is load-bearing indirectly: Basin's own
`sign` in `pg_scalar_aliases.rs` is a thin PG-naming wrapper.

### 4.3 Date/time — 7

`current_date`, `current_time`, `current_timestamp`, `date_part`, `date_trunc`,
`make_date`, `now`

Cheapest category relative to its importance. `arrow_arith::temporal` exposes
`date_part(&array, DatePart)` covering the whole `EXTRACT` field set, and
Basin's `date_bin` in `datetime_more_udf.rs` already contains the interval
arithmetic `date_trunc` needs. Note `EXTRACT(...)` is parsed into `date_part`,
so `date_part` carries 28 test call sites it does not appear to own.

### 4.4 Array / list — 23

`array_append`, `array_cat`, `array_concat`, `array_element`, `array_has`,
`array_has_all`, `array_has_any`, `array_length`, `array_max`, `array_min`,
`array_ndims`, `array_position`, `array_positions`, `array_prepend`,
`array_remove`, `array_repeat`, `array_replace`, `array_slice`,
`array_to_string`, `cardinality`, `generate_series`, `make_array`,
`string_to_array`

**The expensive category.** arrow-rs has essentially no list-manipulation
kernels — `arrow_select::concat` and `arrow_ord::sort` are the only reusable
pieces. Everything else is manual `ListArray` offset-buffer surgery against a
child array, with null-slot and nested-list handling written by hand each time.
`array_max` / `array_min` are non-negotiable (the `ALL`-quantifier rewrite),
`array_has_all` backs the `@>` / `<@` rewrite, and `make_array` backs every
`ARRAY[...]` literal and `IN`-list rewrite.

### 4.5 Regex — 2

`regexp_count`, `regexp_like`

Small only because `string_dt_udf.rs` and `regex_udf.rs` already own
`regexp_match`, `regexp_matches`, `regexp_split_to_array`,
`regexp_split_to_table` and `regexp_replace`. `regexp_like` is nonetheless the
single highest-consequence name in this document — it is the rewrite target for
all four PG regex operators. `arrow::compute::kernels::comparison::regexp_is_match`
plus the existing `regex` dependency covers it; the work is flag handling
(`'i'`, `'g'`) and PG-vs-Rust regex dialect parity.

### 4.6 Core / conditional — 5

`coalesce`, `greatest`, `least`, `named_struct`, `nullif`

Small but not free: all three of `coalesce`/`greatest`/`least` are variadic
with PG type-coercion rules, and `zip`/`nullif` need a coercion table before
they can dispatch. `named_struct` has 3 internal references in
`crates/` — it backs record-shaped returns and cannot simply be dropped.

### 4.7 Aggregate — 32

`avg`, `bit_and`, `bit_or`, `bit_xor`, `bool_and`, `bool_or`, `corr`, `count`,
`covar_pop`, `covar_samp`, `grouping`, `max`, `median`, `min`,
`percentile_cont`, `regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`,
`regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy`, `stddev`,
`stddev_pop`, `stddev_samp`, `string_agg`, `sum`, `var_pop`, `var_samp`,
`variance`

32 names collapse to roughly 14 accumulator families: the 9 `regr_*` share one
bivariate-moments accumulator with `corr` and `covar_*`; `variance`/`var_pop`/
`var_samp`/`stddev*` share a Welford accumulator; `bit_and`/`bit_or`/`bit_xor`
share one; `min`/`max` share one. arrow-rs helps at the leaf:
`arrow::compute::kernels::aggregate::{sum, min, max, bool_and, bool_or}` give
correct single-batch reductions, but each still needs the accumulator,
state-serialisation and merge halves that a distributed group-by demands.
`percentile_cont` and `median` need a t-digest or sort-based approach —
`approx_percentile.rs` already has one to borrow from. `count` is the highest
call count in the whole test corpus (660).

### 4.8 Window — 11

`cume_dist`, `dense_rank`, `first_value`, `lag`, `last_value`, `lead`,
`nth_value`, `ntile`, `percent_rank`, `rank`, `row_number`

This category is the sharpest surprise in the audit: **Basin currently owns no
window-function code at all.** There is no `register_udwf` call, no
`PartitionEvaluator` implementation, no frame-evaluation machinery anywhere in
`crates/`. All 11 documented window functions and the frame semantics behind
them come from `datafusion-functions-window` plus DataFusion's `WindowAggExec`.
Closing this needs the evaluator abstraction built first, then 4 evaluator
families (rank-peer-group, offset, value-at-position, ntile bucketing).
`arrow_ord::rank::rank` gives a usable primitive for the rank family.

### 4.9 Droppable — 88

Not named by any rewriter, doc or test; safe to let go with the removal:

`acosh`, `approx_distinct`, `approx_median`,
`approx_percentile_cont_with_weight`, `array_any_value`, `array_distance`,
`array_distinct`, `array_empty`, `array_except`, `array_extract`,
`array_indexof`, `array_intersect`, `array_join`, `array_pop_back`,
`array_pop_front`, `array_push_back`, `array_push_front`, `array_remove_all`,
`array_remove_n`, `array_replace_all`, `array_replace_n`, `array_resize`,
`array_reverse`, `array_sort`, `array_union`, `arrays_zip`, `arrow_cast`,
`arrow_metadata`, `arrow_typeof`, `asinh`, `atanh`, `cbrt`, `contains`,
`cosh`, `cot`, `covar`, `date_format`, `datepart`, `datetrunc`, `element_at`,
`empty`, `find_in_set`, `flatten`, `from_unixtime`, `get_field`, `ifnull`,
`instr`, `isnan`, `iszero`, `levenshtein`, `log2`, `make_list`, `map`,
`map_entries`, `map_extract`, `map_keys`, `map_values`, `mean`, `nanvl`,
`nvl`, `nvl2`, `pow`, `quantile_cont`, `range`, `regexp_instr`, `sha224`,
`sha384`, `sha512`, `sinh`, `string_to_list`, `struct`, `substr_index`,
`substring_index`, `tanh`, `to_local_time`, `to_time`, `to_timestamp_micros`,
`to_timestamp_millis`, `to_timestamp_nanos`, `to_timestamp_seconds`,
`to_unixtime`, `today`, `union_extract`, `union_tag`, `uuid`, `var`,
`var_population`, `var_sample`, plus the ~30 `list_*` aliases of `array_*`
(`list_has_all` excepted — see §3.1).

Two caveats. `list_has_all` sits in this alias family but is a hard rewrite
target, so the alias mechanism itself must survive even if the other 29 aliases
do not. And `arrow_cast` / `arrow_typeof` have zero references in `crates/`
today but are the conventional escape hatch for debugging type coercion —
worth keeping as a 2-function debug affordance.

## 5. Things that look like functions but are not

Sizing these belongs elsewhere in this series; flagging them here so they are
not double-counted or forgotten.

- **`unnest`** — a `LogicalPlan::Unnest` node in `datafusion-expr`, not a UDF.
  `pg_operators.rs` rewrites `LATERAL unnest(...)` into it in four places.
  `docs/sql-support.md` already records `unnest(a, b)` (multi-array) and
  `UNNEST … WITH ORDINALITY` as unsupported, so the target is the single-array
  form only.
- **`generate_series`** — exists in DataFusion in *both* forms: a scalar UDF in
  `datafusion-functions-nested` and a table function in
  `SessionStateDefaults::default_table_functions()` (installed at
  session.rs:2770). Both are reachable; the LATERAL rewrite at
  pg_operators.rs:3215 needs the table form.
- **`EXTRACT(field FROM x)`** — parsed to `date_part`; counted in §4.3.
- **`every`** — appears in Basin's FILTER whitelist (pg_operators.rs:4146) and
  the executor's aggregate list (executor.rs:824), and is claimed supported in
  the docs, but **nothing defines it** — not Basin, and not DataFusion (it is
  not an alias of `bool_and` in 53.1.0). This is a pre-existing gap the removal
  should close, not one it creates.
- **`SIMILAR TO`, `IS DISTINCT FROM`, `ANY`/`ALL`** — operator-level rewrites
  handled by `pg_operators.rs`, `any_all_rewrite.rs` and `is_distinct_rewrite.rs`.

## 6. LOC estimate

Calibrated against Basin's own measured density, which is the only honest
baseline available: `string_dt_udf.rs` is 68 LOC per scalar across 19
functions, `range_udf.rs` 105 across 30, `udf.rs` 188 across 33,
`datetime_more_udf.rs` 161 across 8, and `pg_agg_udf.rs` 390 per aggregate
across 9. These figures are inclusive of the `#[cfg(test)]` blocks those files
carry, so the implementation-only rates below are deliberately lower.

| Category | Count | Impl LOC | Rationale |
|---|---:|---:|---|
| String | 22 | 1,900 | arrow_string kernels cover most; `overlay`/`lpad`/`rpad` fiddly |
| Math | 25 | 1,300 | `unary()` maps; 2-arg `round`/`trunc`/`log` are the cost |
| Date/time | 7 | 1,100 | `arrow_arith::temporal::date_part` + existing `date_bin` |
| Array/list | 23 | 3,800 | no arrow kernels — manual offset-buffer work throughout |
| Regex | 2 | 200 | `regexp_is_match` + existing `regex` dep; flag/dialect parity |
| Core/conditional | 5 | 650 | variadic + PG type coercion |
| Aggregate | 32 | 5,300 | ~14 accumulator families × ~300, + 18 thin wrappers |
| Window | 11 | 3,000 | ~800 for the evaluator framework Basin lacks entirely |
| **Implementation subtotal** | **127** | **17,250** | |
| Registry, signatures, coercion tables | — | 1,500 | 127 signature declarations + dispatch |
| Tests (at the ~40% ratio the existing UDF files carry) | — | 7,500 | |
| **Total** | | **~26,000** | realistic band **22,000–30,000** |

For scale: Basin's existing hand-written function surface is **42,704 LOC**
across the 20 `*udf*.rs` / `sql_functions.rs` / `geo_glue.rs` files. Closing
this gap is therefore roughly a **60% increase** on the function layer — large,
but of a kind the codebase has already demonstrated it can absorb, and at a
per-function rate the repo's own history supports.

If the 88 droppable names in §4.9 were kept for compatibility rather than
dropped, add approximately **6,000 LOC** (most are thin aliases or trivial
`unary()` maps; the `map_*` family and `array_*` long tail dominate).

## 7. Sequencing recommendation

Ordered by consequence-per-LOC, not by size:

1. **`regexp_like`, `array_has_all`/`list_has_all`, `make_array`, `array_max`,
   `array_min`** (~5 functions, ~700 LOC). These five are what
   `pg_operators.rs` unconditionally emits. Without them the operator layer —
   `~`, `@>`, `<@`, `ARRAY[...]`, `ALL` — is dead, and the operator layer is
   most of what makes Basin read as PostgreSQL.
2. **Aggregates** (32, ~5,300 LOC). `count`/`sum`/`avg`/`min`/`max` alone carry
   ~900 test call sites; nothing meaningful runs without them.
3. **Window** (11, ~3,000 LOC). Highest risk, because there is no existing code
   to extend — the evaluator framework is greenfield.
4. **Array/list** (23, ~3,800 LOC). Highest LOC, but degrades gracefully:
   missing entries fail individual queries rather than the operator layer.
5. **String / math / date-time / core** (59, ~4,950 LOC). Broad, shallow,
   parallelisable, and the category where arrow-rs does the most work for us.

## 8. Provenance

- Basin's own registrations: every `fn name(&self) -> &str`, `create_udf`,
  `register_udf`, `register_udaf` site under `crates/basin-engine/src/`.
- DataFusion's surface: `all_default_functions()`, `all_default_nested_functions()`,
  `all_default_aggregate_functions()`, `all_default_window_functions()` and the
  per-module `functions()` lists plus `aliases()` vectors, read from
  `datafusion-{functions,functions-nested,functions-aggregate,functions-window}-53.1.0`.
- Reachability: `pg_operators.rs` lines 1–7601 (test module excluded),
  `docs/sql-support.md`, `CAPABILITIES.md`, `docs/functions.md`,
  `docs/sql-compatibility.md`, and ~11,300 SQL fragments extracted from 469
  files under `tests/` and `testing/`.
