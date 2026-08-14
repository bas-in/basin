---
title: "DF removal — what actually breaks, compiled"
nav_section: migration
sidebar_position: 22
summary: "Removing datafusion from Cargo.toml produces 802 compiler errors across 65 files — all of them name-resolution errors, because rustc aborts before type-checking, so the type-level surface remains unmeasured. The mechanical arrow sweep removes 232 of them and decouples exactly one file. The dev-dependency move is not an early milestone: it is 81% of the work. And deleting Cargo.toml:149 does not remove DataFusion, because 18 datafusion-* crates arrive through four other manifest lines."
tags: [migration, datafusion, metrics, build]
---

# 22 — What actually breaks, compiled

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [18](./18-removal-surface.md) counted the coupling by grepping
it. This document counts it by **deleting the dependency and compiling**, which
answers a different question: not "how many references are there" but "how many
things stop working, and of what kind".

Measured on a throwaway worktree cut from `c8061346`. Every number below is
followed by the command that produced it. Where the measurement hit a wall,
that is stated as a result rather than filled in with an estimate.

---

## 0. The headline

```
$ cargo check -p basin-engine --message-format=short   # baseline, unmodified
0 errors, 194 warnings, 7m32s
```

Remove `datafusion = "53"` from the workspace `[dependencies]`
(`Cargo.toml:149`) and `datafusion.workspace = true` from
`crates/basin-engine/Cargo.toml`, then rebuild:

```
$ cargo check -p basin-engine --message-format=short
error: could not compile `basin-engine` (lib) due to 803 previous errors
```

**802 coded error lines across 65 files.** (rustc's own tally is 803; the
short-format stream carries 802 lines matching `error[E`. The discrepancy is
one uncoded error and is not material.)

---

## 1. The most important result: this number is a floor, and it is not the number you want

Every one of the 802 errors is a **name-resolution** error:

| Code | Count | Meaning |
|---|---:|---|
| `E0433` | 782 | cannot find module or crate `datafusion` |
| `E0425` | 18 | cannot find value in this scope |
| `E0432` | 1 | unresolved import |
| `E0531` | 1 | cannot find tuple struct/variant |

```
$ grep -cE "error\[E0308|error\[E0277|error\[E0599|error\[E0061|error\[E0053" exp1.txt
0
```

**Zero type-check errors.** rustc resolves names, finds 802 failures, and
aborts the compilation before type-checking runs at all. So the 802 measures
*where the word `datafusion` appears in a path*, and measures **nothing** about
whether the replacement types fit the signatures they must fit.

That second surface is the expensive one and it is currently invisible. What
would be needed to see it is a **stub `datafusion` crate** — a shim exporting
the right module tree with placeholder types — swapped in via
`[patch.crates-io]`. Resolution would then succeed, type-checking would run,
and the resulting `E0308`/`E0277` count would be the first honest measurement
of the *semantic* surface. That was not built here.

A proxy for its size, which is measurable today:

```
$ grep -rhoE "datafusion(_[a-z_]+)?::[A-Za-z0-9_:]+" crates/basin-engine/src --include='*.rs' \
    | grep -vE "^datafusion(_[a-z_]+)?::arrow" | grep -oE "[A-Z][A-Za-z0-9_]*$" | sort -u | wc -l
103
```

**103 distinct non-arrow DataFusion types, traits and enum variants** are named
in `basin-engine`. Each one is a signature that must be satisfied by something
Basin owns. `Execution` (70 occurrences), `SessionContext` (47), `TableProvider`
(32), `Result` (28), `ScalarValue` (23), `Accumulator` (23), `Expr` (18),
`ConfigOptions` (18), `MemTable` (16), `ExecutionPlan` (15), `DataFusionError`
(13) lead the list.

> **Read the 802 as "sites to touch", not "work to do".** The work is bounded
> below by 802 edits and above by something no one has measured.

---

## 2. The census, re-verified

Doc 18 and ADR 0030's amendment have drifted. Re-run on `c8061346`:

| Figure | Doc 18 / ADR 0030 | Measured now |
|---|---:|---:|
| `datafusion` references in `basin-engine/src` | 1,017 / 1,041 | **1,041** |
| …across files | 67 | **69** |
| `.rs` files in `basin-engine/src` | 133 | **135** |
| `use datafusion::<mod>` occurrences | 566 | **580** |
| …of which `use datafusion::arrow` | 186 | **186** |
| Files decoupled by an arrow-only sweep | 1 | **1** (`convert.rs`) |

```
$ grep -rc datafusion crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
1041
```

The 1,041 counts lines containing the lowercase crate token. A
case-insensitive count that also catches prose mentions of "DataFusion" is
**2,387 across 98 files** — that figure should never be quoted as coupling.

Import module breakdown (`grep -rho 'use datafusion::[a-z_]*' | sort | uniq -c`):

```
186 arrow          94 logical_expr   71 common        48 physical_plan
 43 prelude        37 datasource     19 scalar        18 error
 15 execution      15 catalog        12 physical_expr  9 optimizer
  8 config          2 physical_optimizer  2 functions_aggregate  1 functions
```

Four of the 69 referencing files produced **no** compile error —
`fast_select.rs`, `pg_operators.rs`, `schema_ddl.rs`, `window_extras.rs`. Their
references are comments and one test function name. They are not coupling.

---

## 3. The 802, classified

Files classified by the trait they actually implement (`impl ExecutionPlan
for`, `impl TableProvider for`, `impl OptimizerRule for`,
`impl ScalarUDFImpl for` / `register_udf`), not by filename.

| Category | Errors | Files | Share |
|---|---:|---:|---:|
| Function hosting (UDF/UDAF bodies + registration) | **409** | 27 | 51.0% |
| Table provider / scan / file format | 122 | 10 | 15.2% |
| Session / context / driver | 89 | 6 | 11.1% |
| Physical plan node (`ExecutionPlan`) | 89 | 7 | 11.1% |
| Optimizer / analyzer rule | 42 | 7 | 5.2% |
| Remainder | 28 | 7 | 3.5% |
| Type conversion | 23 | 1 | 2.9% |

**Function hosting is 51% of the compiled surface — the same 51% the grep-based
disaggregation found.** The two methods agreeing is worth something: the
category split is not an artifact of how the references were counted.

Largest single files: `pg_agg_udf.rs` 58, `geo_glue.rs` 47, `udf.rs` 40,
`session.rs` 38, `wasm_udf.rs` 34, `jsonb_path_udf.rs` 27, `convert.rs` 23,
`jsonb_udf.rs` 22, `range_udf.rs` 21, `hot_tombstone.rs` 21, `executor.rs` 21.

Note that `jsonb_udf.rs` and `jsonb_path_udf.rs` classify as *table providers*,
not function hosting: their set-returning functions are registered via
`register_udtf`, which requires a `TableProvider` impl. That is the shape ADR
0030 calls out — SRFs cannot be SRFs inside DataFusion, so they are smuggled in
as table functions. Those 49 errors are load-bearing evidence for the ADR, not
incidental glue.

---

## 4. Load-bearing versus incidental

### 4.1 Incidental — the arrow sweep, now measured end to end

`datafusion::arrow::*` is arrow-rs re-exported. `basin-engine` already depends
on `arrow` directly, so the rewrite is a rename with no semantic content:

```
$ grep -rl 'datafusion::arrow' crates/basin-engine/src --include='*.rs' | xargs sed -i 's/datafusion::arrow/arrow/g'
277 lines rewritten across 42 files
$ cargo check -p basin-engine --message-format=short
570 errors   (was 802)
```

| | Before | After | Δ |
|---|---:|---:|---:|
| References | 1,041 | 764 | −277 |
| Referencing files | 69 | 68 | **−1** |
| Compile errors | 802 | **570** | −232 (−29%) |
| Erroring files | 65 | 64 | −1 |

The one file that decouples is `convert.rs`, exactly as doc 18 predicted from
the grep. That prediction is now confirmed by a compiler rather than asserted.

Where the 232 came from is the useful part:

| Category | Before | After | Removed by sweep |
|---|---:|---:|---:|
| Function hosting | 409 | 257 | **−152** |
| Table provider / scan | 122 | 74 | −48 |
| Type conversion | 23 | 0 | −23 |
| Session / driver | 89 | 81 | −8 |
| Remainder | 28 | 27 | −1 |
| Physical plan node | 89 | 89 | **0** |
| Optimizer / analyzer rule | 42 | 42 | **0** |

**The free work is concentrated in the shallow categories and removes exactly
nothing from the two deepest ones.** Every `ExecutionPlan` and every
`OptimizerRule` error survives the sweep untouched. This is the sharpest
available statement of why the reference count is a bad progress metric: the
cheapest 29% of it can be deleted this afternoon without moving the migration
forward by one operator.

### 4.2 Deletes with the feature — currently, almost nothing

The hope is that a UDF file whose functions `basin-exec` already implements can
simply be deleted. Measured against the 29 function-hosting files, harvesting
registered names from all five declaration forms:

```
344 distinct names harvested across the function-hosting files
 15 of them are implemented in basin-exec
 22 of 29 files have ZERO basin-exec coverage
```

**No function-hosting file deletes for free today.** The largest —
`jsonb_udf.rs` (59 names), `geo_glue.rs` (46), `pg_catalog_udf.rs` (45),
`fts_udf.rs` (18) — have zero overlap with `basin-exec`. The overlap that does
exist is spread thinly across `udf.rs` (3 of 35), `string_dt_udf.rs` (4 of 20),
`range_udf.rs` (2 of 31), `pg_scalar_aliases.rs` (2 of 18).

### 4.3 Needs a Basin equivalent that already exists

- **`ExecutionPlan` impls → `basin-exec`'s `Operator`.** 89 errors, 7 files
  (`hot_tombstone.rs` 21, `vortex_listing_format.rs` 16,
  `tombstone_cold_scan.rs` 14, `gin_rowgroup_scan.rs` 10,
  `jsonb_posting_scan.rs` 10, `rtree_rowgroup_scan.rs` 10,
  `interval_storage.rs` 8). The receiving trait exists and is pull-based. Cost
  is the algorithm inside each, not the trait.
- **`OptimizerRule` / `AnalyzerRule` → `basin-plan`'s own rule trait.** 42
  errors, 7 files. Ports, not rewrites — but they must actually be ported, and
  the LATERAL decorrelation among them is worth 462×.
- **`DataFusionError` → `basin_common`'s error type.** 13 sites naming
  `DataFusionError`, plus 18 `use datafusion::error`. A shared-enum swap, and
  the one category where a single edit unblocks many files.

### 4.4 Needs a Basin equivalent that does not exist yet

- **A session/context abstraction.** `SessionContext` is named 47 times.
  `session.rs` is 38 errors and every UDF file reaches through it. Nothing in
  `basin-plan` or `basin-exec` receives this today.
- **A UDF hosting ABI.** `ScalarUDFImpl` / `AggregateUDFImpl` /
  `Accumulator` (23 sites) have no owned counterpart; `basin-exec` dispatches
  scalars on `pg_proc` OID through a hard-coded `match`, which is a different
  shape entirely and does not admit registration.
- **Set-returning functions as relations.** The `register_udtf` +
  `TableProvider` pattern in `jsonb_udf.rs` / `jsonb_path_udf.rs` (49 errors).
- **A vectorised group-wise aggregate tier**, to receive `pg_agg_udf.rs`'s
  hand-rolled `GroupsAccumulator` (58 errors, the single largest file).

### 4.5 Genuinely hard

- **`wasm_udf.rs` (34 errors).** Its function names come from the project
  catalog at session open, not from source. It is unbounded per-project, and
  re-hosting it needs the ABI of §4.4 before it can begin.
- **`vortex_listing_format.rs` (16) + `parquet_listing_format.rs` (7).** These
  implement DataFusion's `FileFormat`. Vortex is Basin's default format and its
  DataFusion integration is a third-party crate — see §5.
- **`info_schema_provider.rs` (11) and the catalog providers.** ADR 0030
  already commits to replacing these with real relations rather than porting
  them.

---

## 5. The correction that changes the endgame

ADR 0030 sequences removal as two moves and names the artifact precisely:
`datafusion = "53"` at `Cargo.toml:149`, one consumer. **The one-consumer claim
is correct and re-verified. The one-line claim is not.**

`crates/basin-engine/Cargo.toml` declares four more DataFusion crates directly,
with literal versions rather than through the workspace:

```
datafusion-datasource   = "53"
datafusion-physical-expr = "53"
datafusion-physical-plan = "53"
datafusion-session       = "53"
```

plus `vortex-datafusion = "0.71"` and `vortex = "0.71"`.

With `datafusion` removed from **both** manifests — the exact state that
produced the 802 errors above:

```
$ cargo tree -p basin-engine -e normal --prefix none | grep -E "^datafusion-" | awk '{print $1}' | sort -u | wc -l
18
```

**Eighteen `datafusion-*` crates at v53.1.0 remain in the normal dependency
tree**, including `datafusion-common`, `datafusion-expr`,
`datafusion-physical-plan`, `datafusion-execution`, `datafusion-catalog` and —
critically — **`datafusion-functions`**, the builtin registry that serves the
orphaned function names of [17](./17-udf-rehosting.md) §3.

Consequences for the plan:

1. **Deleting `Cargo.toml:149` does not remove DataFusion.** It removes the
   umbrella facade. A downstream consumer's dependency tree is essentially
   unchanged.
2. **Moving `datafusion` to `[dev-dependencies]` does not deliver "a build with
   no DataFusion in it"** — the outcome ADR 0030 says makes removal real for
   downstream users. Five manifest lines must move, not one, and
   `vortex-datafusion` must be re-hosted or replaced before the last of them
   can.
3. The version lockstep of ADR 0015 is load-bearing in more places than the
   workspace pin suggests.

---

## 6. Costing the two moves

`[dev-dependencies]` are visible to `tests/`, benches, examples **and
`#[cfg(test)]` modules inside `src/`**. So move 1 does not require deleting all
1,041 references — only those outside test modules. Measured by brace-matching
`#[cfg(test)] mod` blocks:

| | Refs | Files |
|---|---:|---:|
| Production code | **845** | 68 |
| Inside `#[cfg(test)]` | **196** | 31 |
| Total | 1,041 | 69 |

Exactly **one** file (`fast_select.rs`, one reference) is test-only. After the
arrow sweep the split becomes 632 production / 132 test.

**Move 1 is 81% of the reference count; move 2 is the remaining 19%.**

That alone would make move 1 the expensive step. The structural argument is
worse. `session.rs`'s `SessionContext` *is* the production execution path —
`executor.rs` routes to the owned engine only inside
`if matches!(kind, StmtKind::Select)` ([21](./21-write-path.md) §0), and the
probe reads 193 served of 231 with **DML 0 of 15**. Under
`[dev-dependencies]`, shipped `basin-engine` code cannot construct a
`SessionContext` at all. There is no partial state: the owned engine must serve
**every** statement, including the entire write path, before the manifest line
can move.

> **Move 1 is not an earlier, cheaper milestone. It is the whole migration
> minus test code.** ADR 0030's framing — that the dev-dependency move "may be
> reachable far sooner than full deletion" — is not supported by this
> measurement. What move 2 adds beyond move 1 is 196 references and the
> shadow-comparison oracle, and that genuinely is small.

---

## 7. The smallest first cut

Three tiers, in order, each measured.

### Tier 0 — free, today, zero risk

**Delete `datafusion-session = "53"` from `crates/basin-engine/Cargo.toml`.**

```
$ grep -rc 'datafusion_session::' crates/basin-engine/src --include='*.rs'   # 0 uses
$ # remove the line, then:
$ cargo check -p basin-engine --message-format=short
0 errors — Finished in 3m46s
```

An unused direct dependency. **One line, zero errors, verified by compiling.**
It removes nothing from the 802, which is the point: it is the only part of the
manifest that is already dead.

### Tier 1 — mechanical, today, one afternoon

**The arrow re-export sweep.** 277 lines across 42 files, one `sed`.

- Removes **232 of 802 errors (29%)**
- Fully decouples **`convert.rs`** (23 errors → 0)
- Zero semantic change; every rewritten path resolves to the same arrow-rs item
- Leaves `ExecutionPlan` and `OptimizerRule` errors **untouched**

Worth doing not because it advances the migration but because it makes the
residual legible: after it, every remaining `datafusion` reference in the crate
is genuine API use, and the count becomes a metric that means something.

### Tier 2 — the first cut with actual engineering in it

**The optimizer/analyzer rules: 42 errors, 7 files.**

`citext_analyzer.rs` (9), `sort_streaming_limit.rs` (9),
`catalog_window_exec.rs` (8), `any_all_rewrite.rs` (5),
`is_distinct_rewrite.rs` (5), `nullif_rewrite.rs` (3),
`union_scan_collapse.rs` (3).

This is the smallest category with a receiving abstraction that already exists
(`basin-plan`'s `OptimizerRule` over its own IR), it is 5% of the surface, and
it is the only deep category that is *self-contained* — rules read and write
plans and do not thread a session. It does not depend on the UDF ABI, the
session abstraction, or the write path, none of which exist.

**What it is not:** it does not reduce the dependency, because DataFusion stays
until every category is done. Its value is that it is the first category that
could be finished and would stay finished.

---

## 8. The function overlap, re-measured

[17](./17-udf-rehosting.md) measured `basin-exec` at 52 names against `6f0d9630`
and predicted the number was a floor. It was. On `c8061346`:

| | Doc 17 | Now |
|---|---:|---:|
| `eval.rs` `OID_*` constants | 52 | **85** |
| Distinct scalar names | 35 | **51** |
| Aggregates (`agg_func_of`) | 7 | **31** |
| Window (`window_func_of`) | 8 | **11** |
| SRF (`srf_kind_of`) | 2 | **2** |
| **Total distinct names** | **52** | **95** |

Where that growth landed is the interesting part.

**Against doc 17 §3's 49 orphans** — names with no Basin code at all, answered
today only by `datafusion-functions`, which would have broken silently on
removal:

```
20 of 49 are now implemented in basin-exec
```

`array_length  bool_and  bool_or  character_length  concat_ws  corr  covar_pop
covar_samp  cume_dist  date_part  date_trunc  initcap  lpad  ntile
percent_rank  repeat  rpad  stddev  stddev_pop  var_pop`

**29 orphans remain**: `array_append  array_ndims  array_position
array_positions  array_prepend  array_remove  array_replace  array_reverse
array_sort  array_to_string  bit_length  cardinality  cot  factorial  gcd  lcm
make_date  md5  now  octet_length  overlay  percentile_cont  random
regexp_count  regexp_instr  regexp_like  starts_with  string_to_array  to_hex`

The array family is 10 of the 29 — a single coherent tranche.

**Against the engine's own registrations:** of `basin-exec`'s 95 names, 47
appear as a string literal somewhere in `basin-engine` and **48 do not**. Those
48 are functions Basin now owns that it never had code for — the work has been
almost entirely on the orphan list rather than on displacing engine UDFs. Only
**15** of the 344 names harvested from the 29 function-hosting files are
covered.

> **The 345-name surface is now 310** (296 engine-registered − 15 displaced,
> plus 49 orphans − 20 done). Progress is real and it is concentrated where
> there was no code to port, which is the cheap half. The 51% of the compile
> surface that is function hosting has barely moved.

All 12 of doc 17 §2a's original names remain covered; §2c's divergences are
unaffected by this measurement and still stand.

---

## 9. Reproducing everything here

```sh
git worktree add --detach /tmp/df-surface c8061346
cd /tmp/df-surface

# baseline
cargo check -p basin-engine --message-format=short          # 0 errors

# the census
grep -rc datafusion crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'   # 1041
grep -rl datafusion crates/basin-engine/src --include='*.rs' | wc -l                             # 69

# remove the umbrella crate from BOTH manifests, then
cargo check -p basin-engine --message-format=short 2>&1 | grep -c 'error\['                      # 802
cargo check -p basin-engine --message-format=short 2>&1 | grep -o 'error\[E[0-9]*\]' | sort | uniq -c

# what remains in the tree even so
cargo tree -p basin-engine -e normal --prefix none | grep -E "^datafusion-" | awk '{print $1}' | sort -u | wc -l   # 18

# the arrow sweep
grep -rl 'datafusion::arrow' crates/basin-engine/src --include='*.rs' | xargs sed -i '' 's/datafusion::arrow/arrow/g'
cargo check -p basin-engine --message-format=short 2>&1 | grep -c 'error\['                      # 570

# the free deletion
# remove `datafusion-session = "53"` from crates/basin-engine/Cargo.toml
cargo check -p basin-engine --message-format=short                                               # 0 errors
```

The `#[cfg(test)]` split needs brace matching rather than grep; the script is
30 lines and is described in §6.

---

## 10. What this document still does not know

- **The type-check surface.** §1. Unmeasured, and larger than zero by 103
  distinct symbols. Building the stub crate is the next measurement, and it is
  the one that would turn this into an estimate anybody could schedule.
- **Behavioural coupling.** Anything depending on DataFusion's coercion rules,
  null ordering, or expression simplification compiles fine and answers
  differently. [16](./16-differential-baseline.md) is the instrument.
- **Whether 570 is even the right residual.** It is the resolution count after
  the free sweep. It will rise before it falls, exactly as 1,017 rose to 1,041
  when `BasinParquetFormat` was added — every capability that ships against
  DataFusion before the switchover adds to it.

---

## Tier 2 measured: four of five optimizer rules DIE, none port

Measured in a throwaway worktree from `ab73dff2`, `datafusion` removed from both
manifests. Before: **802 errors / 65 files**, reproducing this document exactly
(E0433 782, E0425 18, E0432 1, E0531 1). After cutting the four dead rules:
**786 / 61**. Delta **−16 errors, −4 files**; the optimizer/analyzer category
goes 42 → 26.

| Rule | Errors | Compensates for | Verdict |
|---|---|---|---|
| `is_distinct_rewrite.rs` | 5 | Vortex won't push `IsDistinctFrom`; beating DF's stock `DISTINCT ON` | **dies** |
| `nullif_rewrite.rs` | 3 | Vortex won't push `NULLIF` | **dies** |
| `union_scan_collapse.rs` | 3 | duplicate file reads across UNION branches | **dies** |
| `any_all_rewrite.rs` | 5 | DF's O(n²) `LeftMark NestedLoopJoin` | **dies** (already near-dead) |
| `citext_analyzer.rs` | 9 | real PG semantics — case-insensitive compare | **BLOCKED** |

The owned path already has each dead rule's substance natively: `Expr::DistinctFrom`
as a first-class IR node evaluated by `cmp::distinct`; `DISTINCT ON` lowered to
`Distinct{on:Some(..)}`; `NULLIF` desugared to a searched CASE over the real `=`
operator; quantified subqueries as a Kleene fold. The probe corroborates —
Ordering 12/12 including both `DISTINCT ON` shapes, SetOps 11/11, Subqueries
15/15, Predicates 25/25.

**`citext` is blocked on two things outside the optimizer:** `basin-pgtype` has
no CITEXT oid, and `owned_engine::pgtype_of` deliberately returns `UNKNOWN` for
*every* `BASIN_TYPE`-marked field. Until both change the rule has nowhere in
`basin-plan` to land.

### The timing this changes

These four cannot be cut *now*. They compensate for DataFusion, so removing them
while DataFusion is still the fallback **degrades the incumbent** — it drops
Vortex pushdown for `IS DISTINCT FROM` and `NULLIF`, and restores DF's O(n²)
set-comparison plan. They are removals that happen *with* the DataFusion cut,
not before it. A ready patch (6 files, src-only) exists but is deliberately not
applied.

### Four bugs found while reading

1. **The owned engine answers `citext` CASE-SENSITIVELY** — a wrong answer, not
   a fallback. `pgtype_of` → `UNKNOWN`, `operator::resolve("=", UNKNOWN, UNKNOWN)`
   returns oid 91 (`boolean = boolean`) by arbitrary first-row match, but
   `eval_binary` dispatches on operator *name*, so `cmp::eq` runs a
   case-sensitive byte compare on the real Utf8 arrays. Reachable: the probe
   serves `WHERE s = 'héllo'`.
2. **`pg_operators::rewrite_all_subquery` is wrong for `> ALL`, on the SHIPPING
   path.** It emits a bare `> (SELECT MAX(...))`. Live-verified: `> ALL` over a
   non-empty subquery returns `{101}` where PG returns no rows; over an **empty**
   subquery it returns none where PG returns **all** rows. The owned path's
   Kleene fold gets both right.
3. **Latent** — the `IS [NOT] DISTINCT FROM` rewrite returns NULL where PG
   returns FALSE for `(NULL, NULL)`. Invisible in WHERE/JOIN, wrong in a value
   position, and no test covers that position.
4. **Latent** — `union_scan_collapse` is unsound for `UNION ALL`: its guard only
   catches syntactically identical predicates. Overlapping-but-different ones
   give PG 6 rows against 3 collapsed.

---

## `vortex-datafusion` is not a second reader — Basin already has its own

Measured, because it was flagged as possibly keeping DataFusion linked regardless
of the other four declarations:

    basin-storage/Cargo.toml   vortex-array, vortex-file, vortex-btrblocks,
                               vortex-session          <- CORE vortex, no DataFusion
    basin-engine/Cargo.toml    vortex-datafusion = "0.71"

`basin-storage` reads Vortex **directly** — `lib.rs`, `metadata_cache.rs`,
`page_cache.rs`, `disk_cache.rs`, `vortex_footer_cache.rs` — against the core
crates only. `vortex_datafusion` is imported by exactly **two** files, both in
`basin-engine`: `session.rs` (registration) and `vortex_listing_format.rs` (the
`FileFormat` / `TableProvider` impl).

So it is **purely the adapter that lets DataFusion read Vortex**, not a Vortex
reader Basin depends on. The owned engine already bypasses it: its scan path is
`storage_source.rs` → `basin_storage::read_paths_with_schema`, which is the path
`553f4f8b` rewired when it fixed the owned scan reading superseded files.

**Consequence for the endgame:** `vortex-datafusion` deletes *with* the
DataFusion read path, not before it and not as separate work. It pulls its own
`datafusion-*` crates only because the thing it adapts *to* is DataFusion. There
is no "build our own Vortex reader" task — that task was already done, and it is
what the owned engine uses today.

The scan-track work that remains is the DataFusion-side table providers (122
errors) and `ExecutionPlan` nodes (89), which exist to serve the *incumbent*
path. Those are removals, not ports.
