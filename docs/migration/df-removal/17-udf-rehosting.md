---
title: "DF removal — UDF re-hosting inventory"
nav_section: migration
sidebar_position: 17
summary: "Basin registers 308 SQL function names on DataFusion. basin-exec already implements 12 of them. A separate, previously uncounted 48 pg_catalog function names are served today only by DataFusion's own builtins and have no Basin code at all — deleting the dependency deletes them."
tags: [migration, udf, functions, pg-compat]
---

# 17 — UDF re-hosting inventory

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [04](./04-function-gap.md) measured the *size* of the block;
the previous revision of this document surveyed its *character*. This revision
answers the question that actually gates the work:

> **How much of it does `basin-exec` already do?**

**Answer: 12 names of 308. 296 remain, plus 48 more nobody had counted.**

## Method, and its limits

Everything below was measured on 2026-08-13 against
`feat/own-engine-remove-datafusion` at `6f0d9630`, with the working tree dirty
(`crates/basin-exec/src/eval.rs` and four `basin-engine` files are being edited
concurrently by other agents — `eval.rs`'s coverage numbers are a *floor* and
will only go up). Every command is stated inline so any number can be re-run.
Where a count could not be obtained mechanically, that is said rather than
smoothed over.

**The one thing that is genuinely hard to count**, and the reason this was not
done earlier: *registered function names are not statically enumerable by a
single grep.* Basin registers UDFs five different ways —

1. a literal in `fn name(&self) -> &str { "foo" }`;
2. a `match self.field { … => "foo" }` inside that same method;
3. a `name: "foo"` / `name: "foo".into()` field on a parameterised struct
   filled at the registration site (all of `pg_catalog_udf.rs`, most of
   `range_udf.rs`, `advisory_lock.rs`, `string_dt_udf.rs`, …);
4. a constructor argument — `JsonAggUdaf::new("json_agg")`,
   `make_alias_f64("ceiling", f64::ceil)`, `make_udf("l2_distance", …)`;
5. a macro literal — `simple_udf!(GeomFromTextUdf, "st_geomfromtext", …)`,
   which is how 26 of the geo functions are declared.

A name-only grep sees (1) and misses the rest. **46 `fn name()` bodies return
`self.name` rather than a literal** —

```
$ grep -rEA2 'fn name\(&self\) -> &str' --include='*.rs' crates/basin-engine/src \
    | grep -c 'self\.name\|self\.fn_name\|\$name'
46
```

— so the true set was assembled by a script combining all five forms, with the
enclosing `impl … for` walked backwards from each `fn name()` so that
`ExecutionPlan`/`OptimizerRule`/`AnalyzerRule` name methods (which are *not*
SQL functions) are excluded. That exclusion matters: a naive harvest returns
`sort_streaming_limit`, `GinRowGroupScanExec` and `citext_analyzer` as if they
were callable SQL.

Two things remain genuinely uncountable and are excluded from every total:

- **WASM UDFs.** `wasm_udf.rs`'s names come from the project catalog at session
  open (`session.rs:2965-2980`), not from source. The count is per-project and
  unbounded. Treated as a category, not a number — see §5.3.
- **DataFusion's own builtin registry.** Enumerated below at **174 names**, but
  by the same `fn name()`/`aliases()` heuristic applied to
  `datafusion-functions{,-aggregate,-nested,-window}-53.1.0` in
  `~/.cargo/registry`. It is known to **under**-count: `rank`, `dense_rank` and
  `percent_rank` are macro-generated in `datafusion-functions-window`'s
  `rank.rs` and the heuristic misses them. So "48 orphaned pg_catalog names"
  in §3 is a floor, not a ceiling.

## 1. The measured baseline

```
$ grep -rc datafusion --include='*.rs' crates/basin-engine/src | awk -F: '{s+=$2} END {print s}'
1017
$ grep -rl datafusion --include='*.rs' crates/basin-engine/src | wc -l
67
$ grep -rE 'register_udf|register_udaf|ScalarUDF::|AggregateUDF::' --include='*.rs' crates/basin-engine/src | wc -l
372
```

Two corrections to how those figures have been quoted:

- **67 files, not 69.** `find crates/basin-engine/src -name '*.rs' | wc -l` is
  133; 67 of them mention `datafusion`.
- **372 is a line count, not a function count.** Most registration lines match
  two of the four patterns at once (`ctx.register_udf(ScalarUDF::from(…))`).
  The component counts are `register_udf` 349, `register_udaf` 13,
  `register_udtf` 14, `ScalarUDF::` 348, `AggregateUDF::` 13. Those ~362
  registration calls resolve to **308 distinct SQL names**, because 23 names
  are registered twice under a `pg_catalog.`-qualified alias and 18 more are
  registered from two different modules (§6).

Also settled: `datafusion = "53"` (`Cargo.toml:149`) is consumed by
**`basin-engine` only**. `basin-plan/Cargo.toml` mentions DataFusion in a
comment; it has no dependency on it. `grep -rln datafusion --include=Cargo.toml`
returns the workspace root, `basin-engine` (real) and `basin-plan` (comment).

## 2. The three lists

`basin-exec` dispatches scalar functions on `pg_proc` OID, not on name
(`eval.rs:1195 fn eval_scalar_fn(func: FuncId, …)`), so both sides were
reduced to names before diffing. The exec side is:

| Where | How counted | Names |
|---|---|---:|
| `eval.rs` scalar | `grep -c 'const OID_' crates/basin-exec/src/eval.rs` → 52 OIDs, whose trailing comments give the signature | **35** |
| `build.rs` `agg_func_of` (l. 1639) | read directly | 7 |
| `build.rs` `window_func_of` (l. 1747) | read directly | 8 |
| `cte.rs` `srf_kind_of` (l. 353) | read directly | 2 |
| | | **52 total** |

`eval_scalar_fn`'s `match` is **151 lines** and references exactly 52 `OID_*`
constants — there is no second dispatch table, no macro expansion, and no
name-keyed fallback. Everything else in the `match` falls to
`ExecError::Internal("… is not implemented in eval yet")`.

### 2a. ALREADY COVERED — 12

Engine-registered **and** implemented in `basin-exec`:

```
array_agg  btrim  ceiling  left  length  lower  ltrim
power  right  rtrim  sign  upper
```

### 2b. NOT COVERED — 296

The remaining engine-registered names. 178 of them are real
`pg_catalog` functions in PostgreSQL 18.2; 118 are not. Verified by loading
the list into the live server:

```
$ psql postgres://pc@127.0.0.1:5432/postgres -tAF'|' <<'SQL'
create temp table n(name text);
\copy n from 'notcov.txt'
select case when exists (select 1 from pg_proc p
                          where p.proname = n.name and p.pronamespace = 11)
            then 1 else 0 end, n.name from n order by 1 desc, 2;
SQL
… in pg_catalog: 178   not: 118
```

Family breakdown in §4.

### 2c. COVERED BUT DIVERGENT — 12 of 12

**Every one of the 12 covered names is implemented on both sides**, so all 12
are divergence candidates by construction. Reading both implementations, five
are materially divergent and one of those is a live wrong answer:

| Name | Engine | `basin-exec` | Verdict |
|---|---|---|---|
| **`lower` / `upper`** | `range_udf.rs:503 RangeAccessorUdf` — registered under the bare names `lower`/`upper`, shadowing DataFusion's string builtins (`session.rs:378`, after `string_dt_udf` at 369; `register_udf` overwrites by name). Dispatches on a **content heuristic**: if the text starts with `{` and parses as range JSON, return the bound; else lowercase/uppercase. | `OID_LOWER` 870 / `OID_UPPER` 871 — unconditional case conversion | **Engine is wrong.** `SELECT lower('{"l":1,"u":5,"li":true,"ui":false}')` is the lowercased literal in Postgres 18.2 (verified); Basin returns `1`. exec is correct — port nothing, delete the shadow, and give `lower(anyrange)` its own OID. |
| **`sign` / `ceiling`** | `pg_scalar_aliases.rs:67-69 make_alias_f64` — `Signature::one_of([Float64, Int64, Int32, Float32])`, returns `Float64`. No `Decimal128` arm. | `OID_SIGN_NUMERIC` 1706 → `decimal_sign`, `OID_CEILING_NUMERIC` 2167 → `decimal_ceil` | **exec is correct and wider.** Postgres: `pg_typeof(sign(2.5::numeric))` is `numeric` (verified). exec already covers the numeric overload the engine never had. |
| **`length`** | `udf.rs:3066 LengthPgUdf` — also counts **bytes** for `Binary` input (`length(bytea)`) | `OID_LENGTH_TEXT` 1317 only | **exec is narrower.** `length('abc'::bytea)` is `3::integer` in PG 18.2. The bytea overload is a real gap exec must add before the engine copy is deleted. |
| **`array_agg`** | `pg_agg_udf.rs` `PgArrayAggUdaf` — supports in-call `ORDER BY`, and hand-rolls a vectorised `GroupsAccumulator` (`pg_agg_udf.rs:1030`) | `aggregate.rs` `AggFunc::ArrayAgg`, row-wise; in-call `ORDER BY` is *refused* in `build.rs`'s `agg_spec` for every aggregate | **exec is narrower on capability and slower.** See §5.1. |
| `btrim` `ltrim` `rtrim` `left` `right` `power` | `string_dt_udf.rs:183-220`, `string_more_udf.rs:98`, `udf.rs:3684` | `OID_BTRIM_1/2`, `OID_LTRIM_1/2`, `OID_RTRIM_1/2`, `OID_LEFT`, `OID_RIGHT`, `OID_POWER_FLOAT8` | **Agree.** Negative-`n` `left`/`right` semantics match char-for-char (`eval.rs:1499 pg_left` vs `string_more_udf.rs:500`). `power(numeric,numeric)` (oid 1738) is missing on both sides. |

`crates/basin-exec/tests/function_equivalence.rs` can adjudicate all of these
today: it enumerates the same 52 OIDs from `eval_scalar_fn`'s `match`, batters
each with a per-type edge-case battery (NULL, `i32::MIN`, `±inf`, `NaN`,
subnormals, multibyte UTF-8, embedded NUL, half-integer ties), and diffs value,
NULL-ness **and error-vs-success** against PostgreSQL 18.2. It does not yet
cover the engine's implementations — pointing it at both sides of the 12 is
cheap and is Tranche 0 below.

## 3. The fourth list nobody counted: functions with no Basin code at all

This is the finding that most changes the plan. `SELECT date_trunc(…)` works
today, and there is nothing in `basin-engine` to re-host — DataFusion's own
builtin registry supplies it. The previous revision of this document caught
this for the math family; the same hole is much larger than math.

```
$ python3 …  # fn name()/aliases() harvest over datafusion-functions{,-aggregate,-nested,-window}-53.1.0
datafusion-functions-53.1.0            94
datafusion-functions-aggregate-53.1.0  26
datafusion-functions-nested-53.1.0     48
datafusion-functions-window-53.1.0      9
union                                 174
```

Of those 174, **33 are already implemented in `basin-exec`** (the math family
and core aggregates/window functions — that work has been done), and 18 are
shadowed by a Basin registration. **123 have no owned implementation on either
side. 48 of those 123 are real `pg_catalog` function names** (same
`pg_proc` check as §2b):

```
array_append  array_length  array_ndims  array_position  array_positions
array_prepend  array_remove  array_replace  array_reverse  array_sort
array_to_string  bit_length  bool_and  bool_or  cardinality  character_length
concat_ws  corr  cot  covar_pop  covar_samp  cume_dist  date_part  date_trunc
factorial  gcd  initcap  lcm  lpad  make_date  md5  now  ntile  octet_length
overlay  percentile_cont  random  regexp_count  regexp_instr  regexp_like
repeat  rpad  starts_with  stddev  stddev_pop  string_to_array  to_hex  var_pop
```

Plus `percent_rank`, which the heuristic missed (macro-generated) and which
`basin-exec` also does not implement — `grep -rn 'percent_rank\|cume_dist\|ntile'
crates/basin-exec/src` returns nothing.

**`date_trunc`, `date_part`, `now`, `md5`, `lpad`/`rpad`, `initcap`, `repeat`,
`concat_ws`, `overlay`, `starts_with`, `stddev`, `bool_and`/`bool_or`,
`array_length`, `cardinality`, `string_to_array`, `random`, `ntile`,
`cume_dist`, `percent_rank`** are not exotic. They are ordinary SQL that Basin
answers correctly today and will stop answering the moment
`Cargo.toml:149` is deleted, with no code to port and nothing in
`basin-engine` to point at.

> **The real remaining function surface is 296 + 49 = 345 names, not 296.**
> The 49 are strictly harder to notice and strictly easier to write.

## 4. What remains, by family

296 not-covered names. "pg-std" = the name exists in PostgreSQL 18.2's
`pg_catalog`; "ext" = it does not (a Basin extension, or a PG extension
Basin reimplements: PostGIS, pgvector, pg_trgm, citext, pgcrypto, uuid-ossp).

| Family | Names | pg-std | ext | Verdict |
|---|---:|---:|---:|---|
| jsonb / json | 54 | 43 | 11 | **Must reimplement.** The 11 non-pg names (`jsonb_has_key`, `jsonb_contained_by`, `json_get`, `jsonb_delete_*`, `json_path_extract*`) are Basin's spellings for the `?`/`<@`/`->`/`#>`/`-` **operators**, which Postgres exposes only as operators. They stay, but as `pg_operator` rows in `basin-pgtype`, not as function names. |
| pg_catalog / system | 47 | 46 | 1 | **Must reimplement**, but see §5.4 — the majority are constant stubs. |
| geo (PostGIS) | 43 | 0 | 43 | **Reimplement** — see §5.2, the dependency is not DataFusion. |
| range / multirange | 28 | 21 | 7 | **Must reimplement.** The 7 non-pg names are function spellings of `&&`/`@>`/`<<`/`>>`/`-|-`; same operator note as jsonb. Carries the correctness debt in §7. |
| datetime / interval | 28 | 17 | 11 | **Must reimplement.** Non-pg: `at_time_zone` (an operator in PG), `time_bucket` (TimescaleDB), `date_add_int`/`date_sub_int`/`date_diff_days`/`extract_epoch_from_interval`/`cast_infinity_timestamp` (Basin internals). |
| string | 19 | 19 | 0 | **Must reimplement.** Entirely Postgres-standard, entirely pure text manipulation. Cheapest large family. |
| FTS (tsvector / tsquery) | 17 | 14 | 3 | **Must reimplement.** `tsvector`/`tsquery` are plain `Utf8` holding a canonical text form, not Arrow extension types; the whole stack is self-contained Rust touching DataFusion only at `invoke_with_args`. |
| aggregate | 11 | 6 | 5 | **Structurally hard** — §5.1. |
| vector (pgvector) | 7 | 0 | 7 | **Reimplement.** `basin-vector` has no DataFusion dep; `vector_avg` is an aggregate (§5.1). |
| citext | 7 | 0 | 7 | **Drop as functions.** `citext_eq`…`citext_like` exist only to back the citext operators; in the owned engine they are `pg_operator` rows plus a collation rule, not seven UDFs. |
| crypto / encoding | 6 | 2 | 4 | **Reimplement.** `encode`/`decode` are pg-std; `crypt`/`gen_salt`/`digest`/`uuid_generate_v4` are pgcrypto / uuid-ossp. |
| advisory locks | 6 | 6 | 0 | **Must reimplement**, and they are the canonical ENTANGLED case: they need a session context reachable from function resolution. |
| sequences | 4 | 4 | 0 | **Must reimplement — but not as UDFs.** §7, "red herring". |
| internal / tablesample | 4 | 0 | 4 | **Drop.** `basin_tablesample_*` are planner-internal predicates; the owned planner should express TABLESAMPLE as a plan node, not a UDF call. |
| trigram (pg_trgm) | 3 | 0 | 3 | **Reimplement.** `basin-trgm` has no DataFusion dep. The ~250 lines of operator rewriting in `trgm_glue.rs` travel with it and are not UDF-body work. |
| auth (Basin ext) | 3 | 0 | 3 | **Reimplement.** Needs session context (the JWT claims) — same prerequisite as advisory locks. |
| net / http | 2 | 0 | 2 | **Reimplement or drop.** §7 — they `block_on` inside a scalar function today. |
| internal (`__basin_*`) | 2 | 0 | 2 | **Drop.** Planner-internal. |
| inet / cidr | 2 | 0 | 2 | **Reimplement** as operators (`<<`/`>>` on inet). |
| cron | 2 | 0 | 2 | **Reimplement.** Needs session context + async catalog. |
| regex | 1 | 0 | 1 | `substring_regex` — unreachable today without a SQL-text rewrite that is still a TODO (§7). |
| **Total** | **296** | **178** | **118** | |

## 5. Structurally hard cases

### 5.1 Aggregates and window functions need machinery, not signatures

11 not-covered aggregate names —
`approx_count_distinct`, `approx_percentile`, `approx_percentile_cont`,
`first`, `last`, `mode`, `percentile_disc`, `json_agg`, `json_object_agg`,
`jsonb_agg`, `jsonb_object_agg` — plus `vector_avg` in the vector family.

```
$ grep -rn 'impl.*Accumulator for' --include='*.rs' crates/basin-engine/src | wc -l
10
$ grep -rn 'impl GroupsAccumulator for' --include='*.rs' crates/basin-engine/src
crates/basin-engine/src/pg_agg_udf.rs:1030:impl GroupsAccumulator for OrderedArrayAggGroupsAccumulator
```

Each is an `Accumulator` — `update_batch` / `merge_batch` / `state` /
`evaluate` — with partial-state serialisation for two-phase aggregation.
`basin-exec`'s `aggregate.rs` has one row-wise `AccState` enum over eight
fixed variants; it has no extension point for a user accumulator and no
vectorised group-wise tier. Two of the eleven (`percentile_disc`, `mode`) are
ordered-set aggregates, which `aggregate.rs`'s own module doc explicitly
excludes.

Basin registers **zero** custom window functions — there is no `WindowUDFImpl`
anywhere in `basin-engine`. Every window function Basin answers today is either
a DataFusion builtin or, for the eight in `window_func_of`, already
reimplemented in `basin-exec/src/window.rs`. The window gap is therefore
entirely in §3's orphan list (`percent_rank`, `cume_dist`, `ntile`), not here —
but those three do need real frame/partition machinery, not a scalar signature.

**On `array_agg` specifically, the previous revision's measurement stands and
should not be re-litigated:** a vectorised group-wise tier was built,
benchmarked in release, measured at **0.60× (1M rows / 100k groups)** and
**0.76× (1M rows / 10 groups)** against the row-wise loop, correct on all 334
tests, and deliberately shelved on `spike/vectorised-aggregate`. The expensive
part is not "add a vectorised tier"; it is building one that *beats* the scalar
loop, reproducing `PgArrayAggUdaf`'s specific `lexsort_to_indices` +
`interleave` algorithm. Treat the existing row-wise `AggFunc::ArrayAgg` as
correct-and-slower and move on; this is not on the critical path.

A caution carried forward: that benchmark was initially broken in a way that
flattered the result — it pulled one batch from the operator and compared its
row count to the group count, asserting `8192 == 100000`, because the operator
emits groups in output-sized batches. Any re-measurement must drain the
operator.

### 5.2 Geo: the real dependency is not DataFusion

Checked, and the answer is clean:

```
$ sed -n '/^\[dependencies\]/,/^\[/p' crates/basin-geo/Cargo.toml
basin-common, serde, serde_json, thiserror, geo, proj4rs
```

**Zero DataFusion, zero Arrow.** Same for `basin-trgm` (`basin-common`, serde
only) and `basin-vector` (arrow-array/arrow-schema, no DataFusion).
`geo_glue.rs`'s 3,820 lines are a trait shell over an independent crate; 26 of
the 43 names are declared through one `simple_udf!` macro
(`geo_glue.rs:2340`) whose body is `fn(&ScalarFunctionArgs) -> DFResult<ColumnarValue>`.
Swapping that macro's expansion for a `basin-exec` signature re-hosts 26
functions in one edit.

**One real exception, and it is a design question rather than a translation.**
`st_srid` and `st_transform` read `BASIN_SRID` out of DataFusion's per-call
`ScalarFunctionArgs.arg_fields[].metadata()` (`geo_glue.rs:2009`, `:2032`;
the producing side is `geo_glue.rs:495-502`). `basin-exec`'s evaluator has no
equivalent — `grep -c metadata crates/basin-exec/src/eval.rs` is **0**. It sees
`ArrayRef`s, not the source columns' `Field`s. Either `Expr` carries the SRID
through the plan, or the geometry encoding carries it inline. Decide before
porting, not during.

### 5.3 WASM: not a function-porting problem

`wasm_udf.rs` (1,782 lines) hosts *user* code. `session.rs:2965-2980` lists the
project's `LANGUAGE wasm` functions from the catalog at session open and
registers one `ScalarUDF` per definition; names are per-project and unbounded,
which is why they appear in no count here. The module talks to `wasmtime`
directly (`Engine`, `Store`, `StoreLimits`, epoch interruption, a compiled-module
LRU) — DataFusion contributes only `ScalarUDFImpl`, `ColumnarValue` and the
error type.

The re-hosting question is therefore not "port a function" but **"what is the
owned engine's extension point for a dynamically-registered, catalog-sourced
scalar function?"** `basin-exec` dispatches scalar calls on a compile-time
`match` over `pg_proc` OIDs (`eval.rs:1208`), which has no room for a name
resolved at session open. That extension point does not exist and is a
prerequisite for this family alone. It is also the only family where getting it
wrong is a sandbox-escape question rather than a wrong-answer question.

### 5.4 The session-context prerequisite (carried forward, still true)

Advisory locks (6), auth (3), cron (2), net (2), `pg_cancel_backend`,
`current_setting`/`set_config`, `statement_timestamp`/`transaction_timestamp`,
and the sequence rewrite path all need something `eval.rs` does not have: a
session context reachable from function resolution. `eval.rs` is
scalar-expression-only — no session, no lock table, no cancellation channel,
no HTTP client, and (per §5.2) not even the input columns' `Field` metadata.
That plumbing is a bigger lift than porting any individual function's logic and
is shared across ~18 names. It is the single design decision that unblocks the
most entries.

## 6. Registration collisions found while counting

Two overlapping-name mechanisms, both silent, both able to make a port pick the
wrong body:

**23 names are registered twice under a `pg_catalog.`-qualified alias** —
`pg_table_is_visible` and `pg_catalog.pg_table_is_visible`, etc., almost all in
`pg_catalog_udf.rs`. These are the same implementation twice and collapse to
one entry in the owned engine's `pg_proc` (schema qualification is resolution's
job, not the function's).

**18 names are registered from two different modules**, where DataFusion's
`register_udf` overwrites by name and `session.rs`'s call order decides the
winner:

```
age                     datetime_more_udf.rs  udf.rs
decode                  string_more_udf.rs    udf.rs
encode                  string_more_udf.rs    udf.rs
format                  string_dt_udf.rs      string_more_udf.rs
jsonb_agg               jsonb_udf.rs          pg_agg_udf.rs
jsonb_insert            jsonb_modify_udf.rs   jsonb_udf.rs
jsonb_object_agg        jsonb_udf.rs          pg_agg_udf.rs
jsonb_path_match        jsonb_path_udf.rs     jsonb_udf.rs
jsonb_path_query_array  jsonb_path_udf.rs     jsonb_udf.rs
jsonb_path_query_first  jsonb_path_udf.rs     jsonb_udf.rs
jsonb_pretty            jsonb_modify_udf.rs   jsonb_udf.rs
jsonb_set               jsonb_modify_udf.rs   jsonb_udf.rs
jsonb_strip_nulls       jsonb_modify_udf.rs   jsonb_udf.rs
jsonb_typeof            jsonb_modify_udf.rs   jsonb_udf.rs
to_char                 datetime_more_udf.rs  udf.rs
to_date                 datetime_more_udf.rs  udf.rs
to_number               pg_scalar_aliases.rs  udf.rs
to_timestamp            datetime_more_udf.rs  udf.rs
```

Plus the 16 geo names shadowed *within* `geo_glue.rs` (wave-α POINT-only
structs beaten by later general-geometry ones), documented in the previous
revision and still true. **Porting rule: always take the version that wins the
registration race**, never the one left behind. Porting the loser silently
downgrades a function that works today — and in geo's case, the losers are
still unit-tested by tests that instantiate them by hand, so those tests pass
against code no query can reach.

## 7. Traps that change how the work is done (carried forward, re-verified)

These findings were established in the previous revision against a live
PostgreSQL 18.2 and remain accurate; they are kept because they are the
difference between porting a function and porting a bug.

**Range predicates other than `range_eq` silently mishandle date/timestamp
bounds.** `range_bound_f64` (`range_udf.rs:849`) parses a bound as `f64`; date
and timestamp bounds are stored as JSON strings like `"2024-01-01"`, which fail
that parse and become `None` — indistinguishable from a genuine infinite bound.
Consequences, all confirmed against PG 18.2: `range_overlaps` reports **every**
pair of daterange/tsrange/tstzrange as overlapping; `range_contains_range` is
**always false**; `range_contains_elem` returns **NULL** instead of a boolean;
`range_strictly_left`/`right`/`adjacent` are **always false**;
`range_merge`/`union`/`intersection`/`diff` silently rewrite finite date bounds
to `(-infinity, +infinity)`. `range_eq` alone escapes, because it is the only
predicate the pre-parse rewriter hands a subtype argument. Re-hosting this
family correctly requires first deciding **how every predicate learns its
subtype** — a smaller instance of the same missing-context problem as §5.4.
Related: `isempty('(5,6)'::int4range)` is `t` in Postgres (discrete
canonicalisation) and non-empty in Basin, for the same reason.

**`jsonb_path_exists` and scalar-position `jsonb_path_query` implement a
different, much weaker JSONPath than the other three.**
`jsonb_path_query_first`, `jsonb_path_query_array` and `jsonb_path_match`
resolve to `jsonb_path_udf.rs`'s real recursive-descent parser (`[*]`, filters,
`..key`). `jsonb_path_exists` and plain `jsonb_path_query` are never
re-registered by that module and keep `jsonb_udf.rs`'s dot-split-only parser.
Confirmed: `jsonb_path_exists('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)')` is
`t` in PG 18.2, `f` on Basin. Route both names through `jsonpath_eval` when
re-hosting; do not port `JsonbPathQueryUdf`/`JsonbPathExistsUdf`.

**Several functions are fake stubs returning wrong answers today** — not
simplified, wrong. `array_contains` (`@>`) checks `rhs.len() <= lhs.len()`
instead of element containment; `arrays_overlap` (`&&`) checks "both non-empty";
`timezone`/`at_time_zone` pass non-UTC zones through **unchanged**;
`regexp_split_to_table` returns its input; `regexp_matches` reuses single-match
logic; `websearch_to_tsquery` is a bare alias to `plainto_tsquery` with no
websearch syntax. Re-hosting these mechanically carries the wrongness forward
under an implementation that looks more trustworthy. **Fixing them is separable
from the migration — they are wrong on `main` today.**

**The sequence UDFs are a red herring.** `nextval`/`currval`/`setval`/`lastval`
in `seq_udf.rs` are dead-code tombstones that always error. The real logic is
`rewrite_sequence_calls`, a **pre-parse SQL string rewriter** that resolves
literal-argument sequence calls via an async catalog call before the parser
runs — written that way to dodge DataFusion's synchronous `invoke`. The struct
is not the target.

**Some "UDFs" in `hypertable.rs` are not UDFs.** `create_hypertable`,
`add_retention_policy`, `drop_chunks` are `match_*` SQL-text pattern matchers
that intercept whole statements. Only `time_bucket` is a real `ScalarUDFImpl`.

**`to_char` is registered three times** — `udf.rs`, `datetime_more_udf.rs`, and
`to_char_interval` under a different name. Registration order picks the winner,
nothing visibly routes `to_char(interval, fmt)` to `to_char_interval`, and the
winner does not handle interval input.

**`statement_timestamp`/`transaction_timestamp` do not work today** — session
state exists but no executor hook ticks it, so both fall back to `Utc::now()`
per call.

**Interval maths uses a 30-day month** in `date_bin` and
`extract_epoch_from_interval`, with no leap-year awareness. Both self-flag it.

**`net_http_get`/`net_http_post` call `block_on` inside a scalar function.**

**Wiring debt travels with some UDFs.** `substring_regex` is unreachable without
a SQL-text rewrite that is still a TODO. `trgm_glue.rs` carries ~250 lines of
rewriters lowering `%`, `<%` and `<->` with quote-aware scanning. None of that
is UDF-body work and all of it has to move.

## 8. The other 1,017: function hosting is less than half of it

The 1,017 `datafusion` references are five different removal problems with
different owners. Classified by walking each file's `impl <Trait> for` set:

| Category | Refs | Files | What it is |
|---|---:|---:|---|
| **Function hosting** | **521** | **29** | `ScalarUDFImpl` / `AggregateUDFImpl` bodies and registration. This document. |
| Session / context / driver | 118 | 6 | `session.rs` (46), `executor.rs` (33), `prepared.rs` (15), `lib.rs` (10), `lifecycle.rs` (7), `query_shape.rs` (7) |
| Physical plan nodes + physical optimizer | 164 | 8 | `catalog_window_exec.rs` (27), `sort_streaming_limit.rs` (23), `hot_tombstone.rs` (22), `tombstone_cold_scan.rs` (16), `rtree_rowgroup_scan.rs` (12), `jsonb_posting_scan.rs` (12), `gin_rowgroup_scan.rs` (12) — → [03](./03-physical-operators.md) |
| Table providers / scan | 74 | 7 | `vortex_listing_format.rs` (40), `realtime_catalog.rs` (13), `info_schema_provider.rs` (13), `project_usage_view.rs` (12), `hypertable_provider.rs` (11), `query_stats_export.rs` (11) — → [06](./06-scan-and-storage.md) |
| Logical optimizer / analyzer rules | 59 | 5 | `any_all_rewrite.rs` (19), `citext_analyzer.rs` (13), `nullif_rewrite.rs` (10), `is_distinct_rewrite.rs` (10), `union_scan_collapse.rs` (7) — → [05](./05-optimizer-rules.md) |
| Type conversion | 44 | 2 | `convert.rs` (39), `pg_colnames.rs` (5) — → [12](./12-pg-type-fidelity.md) |
| Remainder | 37 | 10 | `pg_plan.rs` (13), `rls.rs` (11), and eight files with ≤3 refs each |

(`vortex_listing_format.rs` and five scan files impl both `ExecutionPlan` and a
provider trait and are counted once, under the plan-node row. `session.rs` is
counted as context setup rather than provider work despite registering tables,
because that is what its 46 references are.)

**Function hosting is 51% of the reference count and involves none of the other
four problems' owners.** Do not let a plan that says "1,017 references" imply
that finishing the functions finishes the migration; do not let one that says
"372 UDF sites" imply the functions are the whole 1,017 either.

## 9. Proposed order

Sized so each tranche is one agent-sitting. Ordered by *unblocking value per
hour*, not by family size.

**Tranche 0 — adjudicate the 12 (half a sitting).**
Point `function_equivalence.rs` at the engine's implementations of the 12
covered names as well as `basin-exec`'s. Delete the `lower`/`upper` range
shadow (`range_udf.rs:503`) and give range bound accessors their own OIDs; add
`length(bytea)` to `eval.rs`. Nothing else in the plan is safe until the
overlap is known to be *equivalent*, not merely *present*. **This is the only
tranche that can produce a regression on `main`, so it goes first.**

**Tranche 1 — the 37 *scalar* orphans of §3's 49 (one sitting, maybe two).**
`date_trunc`, `date_part`, `now`, `md5`, `lpad`/`rpad`, `initcap`, `repeat`,
`concat_ws`, `overlay`, `starts_with`, `octet_length`, `bit_length`,
`character_length`, `string_to_array`, `array_to_string`, `array_length`,
`cardinality`, `random`, `to_hex`, `gcd`/`lcm`/`factorial`/`cot`. Individually
trivial, all Postgres-standard, all with zero Basin code to consult, and all
**hard blockers on deleting `Cargo.toml:149`**. This is the cheapest tranche in
the whole program and the one with the largest blast radius if skipped.
Explicitly *defer* the nine statistical aggregates (`stddev`, `stddev_pop`,
`var_pop`, `corr`, `covar_pop`, `covar_samp`, `bool_and`, `bool_or`,
`percentile_cont`) and the three window orphans (`ntile`, `cume_dist`,
`percent_rank`) — they need §5.1's machinery and belong with Tranche 6. That
is what turns 49 into 37.

**Tranche 2 — string (19) + regex (1) (one sitting).**
Entirely `pg_catalog`, zero entangled, pure text manipulation, and `eval.rs`
already speaks raw `ArrayRef`/`RecordBatch` rather than `ColumnarValue`, so the
shell swap is mechanical. Carries `substring_regex`'s wiring TODO with it.

**Tranche 3 — the `pg_catalog`/system stubs (~26 of 47) (one sitting).**
`pg_catalog_udf.rs`'s own module doc (lines 5-8) says it plainly: *"stub
implementations that always return a plausible constant so psql's queries plan
and execute without 'Invalid function' errors. Correctness is deliberately not
a goal."* `pg_table_is_visible` is a `SimpleOidBoolUdf { value: true }`;
`pg_get_userbyid` is a `SimpleOidTextUdf { value: "basin" }`. No session, no
catalog, no async.
The 23 `pg_catalog.`-qualified duplicates collapse to nothing. Leaves the
~20 genuinely session-dependent system functions for Tranche 5.

**Tranche 4 — geo (43) (one sitting for the macro block, one for the rest).**
`basin-geo` is DataFusion-free; 26 of 43 names come from one macro. **Settle the
SRID design (§5.2) before starting**, and take the general-geometry
implementation wherever a name is shadowed. Large name count, small conceptual
surface — the best names-per-hour in the inventory.

**Tranche 5 — the session-context abstraction, then the ~18 it unblocks (two
sittings, one design + one port).**
Advisory locks (6), auth (3), cron (2), net (2), `pg_cancel_backend`,
`current_setting`/`set_config`, `statement_timestamp`/`transaction_timestamp`,
and the sequence rewrite path. **The design is the deliverable**; the eighteen
ports are mechanical once it exists. This is also where the WASM extension
point (§5.3) should be settled, since it is the same question — "how does a
function reach something that is not its arguments?" — asked about a name
resolved at session open instead of a value resolved per session.

**Tranche 6 — aggregates (11 + 5 statistical + 3 window) (two sittings).**
Needs an `Accumulator`-shaped extension point in `aggregate.rs` and frame
machinery for `ntile`/`cume_dist`/`percent_rank`. Ordered-set aggregates
(`percentile_disc`, `mode`, `percentile_cont`) are a further step beyond that.
Do **not** re-attempt vectorised `array_agg` here (§5.1).

**Tranche 7 — range (28) (one sitting, but decide the subtype question first).**
Zero entangled, but every predicate must learn its own subtype before any of it
is worth porting, or the §7 bugs come along for free. Reimplementing against
`basin-common`'s `RangeValue` rather than the JSON-text-plus-`f64` encoding is
probably cheaper than porting `range_udf.rs` and then fixing it.

**Tranche 8 — jsonb/json (54) (three sittings).**
Largest family and the largest file in the crate (`jsonb_udf.rs`, 7,524 lines).
Retire the eight shadowed structs, route both weak JSONPath entry points
through `jsonpath_eval`, and land the 11 operator-spelled names as
`pg_operator` rows rather than functions.

**Tranche 9 — FTS (17), datetime/interval (28), vector (7), trgm (3),
crypto (6), inet (2), citext (7 → operators), sequences (4 → rewriter),
tablesample + internal (6 → drop).**
Independent of each other; parallelisable across agents once Tranche 5's
abstraction exists.

**Deferred indefinitely — WASM.** Blocked on Tranche 5's extension point, and
the only family whose failure mode is a sandbox question rather than a wrong
answer. It should not gate deleting `Cargo.toml:149`; a project with WASM
functions can keep falling back until the extension point lands.
