---
title: "DF removal — optimizer rule dependency map"
nav_section: migration
sidebar_position: 5
summary: "Maps each published benchmark win to the optimizer rule that produces it, verified by rule-by-rule ablation against real DataFusion 53 plans. Five attributions were wrong: the risk is not in join-tree construction or lateral decorrelation but in projection pruning and predicate-into-scan, and DataFusion has no cost-based join reordering to port at all."
tags: [migration, query-engine, optimizer, benchmarks]
---

# 05 — Optimizer rule dependency map

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. ADR 0030 names the optimizer as the risk concentration:

> **The optimizer is the risk concentration.** Underscoping the optimizer keeps
> the engine and loses the benchmarks.

This document tests that claim against the published numbers. ADR 0030 is right
that the optimizer is the risk concentration — but **it is concentrated in a
different place than either ADR 0030 or the first draft of this document
assumed**. It is not in join-tree construction or subquery decorrelation. It is
in the two rules that connect the planner to the storage format: column pruning
and predicate-into-scan. Relocating that risk is the most useful result here.

> **Method note (added on revision).** The first version of this document derived
> its rule-to-benchmark mapping from *rule purpose* — reading each rule's source
> and reasoning about which shape it must serve. That method got four
> attributions wrong. Everything below is now backed by a **rule-by-rule
> ablation** against real DataFusion 53 plans; see
> [How this was verified](#how-this-was-verified). Where a claim is still
> inference rather than measurement it is marked **UNVERIFIED**.

## What Basin runs today

DataFusion's default logical optimizer, in order. This list is dumped from
`Optimizer::new().rules` at DataFusion 53.1.0, not read off the source file —
**it is 24 rules, not 23**, and the earlier revision of this table had both a
missing rule (`filter_null_join_keys`) and the wrong relative order for
positions 13–18:

| # | Rule (`name()`) | Non-test LOC |
|---|---|---|
| 1 | `rewrite_set_comparison` | 129 |
| 2 | `optimize_unions` | 415 |
| 3 | `simplify_expressions` | (in `datafusion-optimizer/src/simplify_expressions/`) |
| 4 | `replace_distinct_aggregate` | 207 |
| 5 | `eliminate_join` | 83 |
| 6 | `decorrelate_predicate_subquery` | 1,780 |
| 7 | `scalar_subquery_to_join` | 980 |
| 8 | `decorrelate_lateral_join` | 99 |
| 9 | `extract_equijoin_predicate` | 400 |
| 10 | `eliminate_duplicated_expr` | 141 |
| 11 | `eliminate_filter` | 163 |
| 12 | `eliminate_cross_join` | 1,133 |
| 13 | `eliminate_limit` | 246 |
| 14 | `propagate_empty_relation` | 473 |
| 15 | `filter_null_join_keys` | 304 |
| 16 | `eliminate_outer_join` | 371 |
| 17 | `push_down_limit` | 918 |
| 18 | `push_down_filter` | 3,469 |
| 19 | `single_distinct_aggregation_to_group_by` | 585 |
| 20 | `eliminate_group_by_constant` | 225 |
| 21 | `common_sub_expression_eliminate` | 1,491 |
| 22 | `extract_leaf_expressions` | 2,158 |
| 23 | `push_down_leaf_projections` | — |
| 24 | `optimize_projections` | — |

Two facts about **how** the list is applied, both load-bearing for any build
order we choose:

- The list is **not a single pass**. `Optimizer::optimize` loops
  `while i < options.optimizer.max_passes` with
  `datafusion.optimizer.max_passes` **default 3**, breaking early when a
  `LogicalPlanSignature` repeats (`optimizer.rs:380–469`). Rules therefore see
  each other's output repeatedly, and a rule that "does nothing" on pass 1 can
  fire on pass 2. Observed directly: on the star join, `eliminate_cross_join`,
  `push_down_filter` and `push_down_limit` each fire on **three** separate
  passes.
- Because of that fixpoint loop, **ordering is a performance property more than
  a correctness one** for most of the list — which is why the ablation below is
  a better guide than the source order.

Plus **seven rules Basin wrote itself**, which must be carried across
regardless — they are Basin's, not DataFusion's, but they are written against
DataFusion's plan types and so need porting to the owned IR:

| Rule | Kind | File | Purpose |
|---|---|---|---|
| `AnyAllToScalarSubquery` | logical | `any_all_rewrite.rs:196` | Fires **before** DF's `RewriteSetComparison` so uncorrelated ANY/ALL doesn't become a LeftMark NestedLoopJoin |
| `UnionScanCollapse` | logical | `union_scan_collapse.rs` | Collapses union-of-scans |
| `NullifRewrite` | logical | `nullif_rewrite.rs:40` | PG `NULLIF` semantics |
| `IsDistinctRewrite` | logical | `is_distinct_rewrite.rs:125` | `IS DISTINCT FROM` |
| `CitextAnalyzerRule` | analyzer | `citext_analyzer.rs:62` | Case-insensitive text |
| `SortStreamingLimit` | physical | `sort_streaming_limit.rs:81` | Streaming top-K |
| `CatalogWindowExecSortElision` | physical | `catalog_window_exec.rs:79` | Elides sorts under catalog windows |

All four logical rules are `insert(0, …)` — they run *before* DataFusion's
list (`session.rs:2727–2745`). That ordering is load-bearing and must be
preserved. Nothing is removed from or disabled in DataFusion's default list, and
`skip_failed_rules` is never set (so it keeps its `false` default): Basin runs
**all 24 DF rules plus 4 of its own**, and an optimizer rule failure is a hard
query error today.

### The rewrites that are not rules at all

Beyond the `OptimizerRule` implementations, Basin performs a set of **textual
SQL pre-rewrites** in `crates/basin-engine/src/executor.rs:996–1023`, before
any plan exists. Six of them concern LATERAL:

| Rewrite | Source |
|---|---|
| `rewrite_lateral_unnest` | `pg_operators.rs:1839` |
| `rewrite_lateral_uncorrelated` | `pg_operators.rs:2176` |
| `rewrite_lateral_nested_agg` | `pg_operators.rs:2485` |
| `rewrite_lateral_correlated_row` | `pg_operators.rs:2837` |
| `rewrite_lateral_generate_series` | `pg_operators.rs:3212` |
| **`rewrite_lateral_order_limit`** | **`pg_operators.rs:3461`** |

The last one is load-bearing for a headline number and is analysed below. These
rewrites are **not** part of the optimizer sizing in any earlier document in
this series, and they must be carried across the migration (or, better,
reimplemented as real plan rewrites over the owned IR — they are currently
string surgery guarded by `find_keyword_at_depth0` / `find_matching_close_paren`
heuristics).

## How this was verified

Each headline query shape was planned through a real DataFusion 53.1.0
`SessionContext` built with `with_default_features()`. Then, for each of the 24
rules in turn, that rule was **removed** from the list passed to
`SessionStateBuilder::with_optimizer_rules(…)` and every shape re-planned; the
final optimized `LogicalPlan` was diffed against the baseline. A rule that
changes no headline plan when removed is not producing any headline number.

The sweep was run twice against two different table backings, because the
backing changes what the rules can do:

1. **`MemTable`** — reports `TableProviderFilterPushDown::Unsupported`, so
   filter-into-scan effects are invisible. Used for the join/subquery shapes.
2. **Parquet `ListingTable`** via `register_parquet` — supports pushdown, so
   `TableScan … partial_filters=[…]` appears and is observable. Used to settle
   the scan-bound shapes.

Caveats to keep attached to these results: the ablation compares **plan text**,
not wall-clock, so it proves *whether* a rule shapes the plan, not the size of
the resulting speedup; the fixture tables are small, so nothing statistics-
dependent is exercised; and `datafusion.optimizer.max_passes = 3` means a
removed rule's work is sometimes silently done by another rule on a later pass
(a genuine finding about redundancy, but it means "no plan change" is evidence
about *this rule set*, not about the rule in isolation).

## The central finding, corrected

The earlier revision claimed six of thirteen wins are storage/execution results
that "survive the migration regardless of what the optimizer does." **That is
half right and the wrong half is dangerous.** Those six shapes do not depend on
any *join or subquery* rule — correct. But they depend absolutely on two rules
that the earlier revision left untiered and unsized:

- **`optimize_projections`** — removing it changes **12 of 12** shapes tested,
  every single one. It is what sets `TableScan … projection=[id, user_id,
  amount]`; without it the scan is `TableScan: events` with no projection and
  the reader decodes **every column**. The "we only touch the projected
  columns" property that produces the columnar win *is* this rule.
- **`push_down_filter`** — on the Parquet backing, removing it turns
  `TableScan: events projection=[id, user_id, amount],
  partial_filters=[events.id >= Int64(100), events.id <= Int64(200)]` into a
  bare `TableScan: events projection=[id, user_id, amount]` with the filter
  stranded above. No predicate reaches the scan, so **no row-group pruning, no
  bloom probe, no zone-map skip**. The 81× range scan and 51× selective COUNT
  are pruning results, and pruning is fed by this rule.

So the honest statement is: **the storage wins survive any change to the
join/subquery rules, and collapse without projection pruning and
filter-into-scan.** Column pruning and predicate-into-scan are not
"optimizations" for Basin — they are the interface between the planner and the
storage format, and they are the highest-priority items in the whole rewrite.

Revised attribution table. "Ablation" is the set of rules whose removal
changed that shape's final plan:

| Published win | Multiplier | Rules whose removal changes the plan (measured) | Verdict |
|---|---|---|---|
| LATERAL JOIN | 462× | `optimize_projections`, `push_down_filter`, `push_down_limit`, `simplify_expressions`, `extract_equijoin_predicate` — **but not `decorrelate_lateral_join`** | Basin's own textual rewrite + universals |
| Star join (3-table) | 261× | `optimize_projections`, `push_down_filter`, `push_down_limit`, `simplify_expressions`, `extract_equijoin_predicate` — **but not `eliminate_cross_join`** | **YES — but cheaper than believed** |
| Correlated subquery | 113× | `scalar_subquery_to_join`, `extract_equijoin_predicate`, `push_down_limit`, `eliminate_group_by_constant`, `optimize_projections` | **YES — high** |
| Range scan p50 | 81× | `optimize_projections`, `push_down_filter`, `simplify_expressions` | **YES — via scan interface** |
| `WHERE col = ANY(int[])` | 63× | not ablated; same scan-interface rules expected — **not** Basin's `AnyAllToScalarSubquery` | **UNVERIFIED**, see correction 5 |
| Selective low-card COUNT | 51× | `optimize_projections`, `push_down_filter`, `simplify_expressions` | **YES — via scan interface** |
| `COUNT(DISTINCT …)` per status | 30× | `single_distinct_aggregation_to_group_by`, `optimize_projections` | **YES — moderate** |
| 2-table JOIN GROUP BY | 28× | `extract_equijoin_predicate`, `optimize_projections` — **nothing else** | **YES — but only 400 LOC of it** |
| `PERCENTILE_CONT` | 19× | `optimize_projections` only | Execution result |
| UNION (dedup) | 11× | `replace_distinct_aggregate`, `optimize_projections` — **not `optimize_unions`** | **YES — moderate** |
| Aggregate GROUP BY | 10× | `optimize_projections` only | Execution result |
| Window frame SUM | 9.3× | `push_down_limit`, `optimize_projections` | Partial — see below |

### Five attributions the earlier revision got wrong

**1. LATERAL 462× is not `DecorrelateLateralJoin`.** Removing
`decorrelate_lateral_join` changes no plan. The reason is visible in the
baseline plan for the exact benchmark query — DataFusion **does not decorrelate
it at all**:

```text
Cross Join:
  SubqueryAlias: u
    Filter: users.id < Int64(50)
      TableScan: users projection=[id]
  SubqueryAlias: e
    Subquery:
      Projection: e.amount
        Sort: e.created_at DESC NULLS FIRST, fetch=3
          SubqueryAlias: e
            Projection: events.amount, events.created_at
              Filter: events.user_id = outer_ref(u.id)
                TableScan: events projection=[user_id, amount, created_at]
```

The `Subquery` node and the `outer_ref(u.id)` survive optimization. DF's
`PullUpCorrelatedExpr` refuses to pull a correlated predicate through the
`Sort`/`Limit` that the benchmark's `ORDER BY … LIMIT 3` introduces, so
`decorrelate_lateral_join` bails. What actually produces the 462× is Basin's own
**`rewrite_lateral_order_limit`** (`pg_operators.rs:3461`), which textually
rewrites the shape into a `ROW_NUMBER() OVER (PARTITION BY fk ORDER BY …)`
window join with `__basin_rn <= n` — exactly the classic top-N-per-group
decorrelation, applied before DataFusion sees the SQL. Consequence for sizing:
**`decorrelate_lateral_join`'s 99 LOC buys us nothing on this benchmark**, and
the real dependency is a Basin-owned rewrite plus a correct windowed
`ROW_NUMBER` + equijoin path.

**2. Star join 261× is not `EliminateCrossJoin`.** Removing
`eliminate_cross_join` changes no headline plan — not for the explicit
`JOIN … ON` form the benchmark uses, and *not for a hand-written comma-join
variant of the same query either*. The benchmark's join order is already
optimal as written (`events ⋈ users` on equality, then `categories` via
`BETWEEN`), so there is nothing to eliminate. This retires the single largest
Tier 1 line item: **1,133 LOC moves out of the must-have list.**

**3. UNION 11× is not `OptimizeUnions`.** Removing `optimize_unions` changes no
plan; `replace_distinct_aggregate` (207 LOC) is the rule that lowers the
`UNION`'s implicit `DISTINCT` into an `Aggregate`. That one is a correctness
requirement, not an optimization — without it there is no dedup operator at all.

**4. `SimplifyExpressions` matters, but not for the reason assumed.** The
earlier revision put it in Tier 2 as "foundational — most later rules assume
simplified predicates." The ablation refutes the specific worry raised in
*Still outstanding*: with `simplify_expressions` removed, the comma-join star
join still produces an **identical join structure** — cross joins eliminated,
equijoin extracted, join order unchanged. `eliminate_cross_join` does **not**
require it. What breaks instead is scan-level predicate *quality*:

```text
# with simplify_expressions              # without
partial_filters=[events.id >= 100,       partial_filters=[events.id
                 events.id <= 200]                        BETWEEN 100 AND 200]

partial_filters=[events.status =         partial_filters=[events.status =
                 Utf8("pending")]                CAST(Utf8("pending") AS Utf8View)]
```

Unexpanded `BETWEEN` and an unfolded `CAST` around the literal. Basin's own
pruning layer takes a concrete value —
`Predicate::Eq(String, ScalarValue)` in `basin-storage/src/predicate.rs:36` —
so a predicate whose right-hand side is still a `CAST` expression has no
representation there and degrades to no pruning. **UNVERIFIED:** the exact
DF-`Expr` → `basin_storage::Predicate` conversion was not traced end-to-end, so
the claim that the unfolded `CAST` specifically defeats the bloom/zone-map path
is inferred from the `Predicate` enum's shape, not measured. It is worth
measuring, because it decides whether constant folding is Tier 1.

**5. The 63× `= ANY(int[])` win is not Basin's `AnyAllToScalarSubquery`.** That
rule's own doc comment scopes it to `lhs op ANY/ALL (SELECT col FROM subq)` with
`subquery.outer_ref_columns.is_empty()` — it rewrites ANY/ALL over a
**subquery**, and requires exactly one projected column. The benchmark shape is
`WHERE user_id = ANY('{1,…,10}'::int[])`, an **array literal**, which never
constructs a `Subquery` node at all. The rule cannot fire on it. The 63× is
array-containment evaluation plus scan pruning — the same class as the range
scan, and dependent on the same two scan-interface rules. **UNVERIFIED:** this
shape was not included in the ablation sweep; the reasoning is from the rule's
documented firing conditions, not from a measured plan diff.

### What is still genuinely execution, not planning

`PERCENTILE_CONT` (19×) and aggregate `GROUP BY` (10×) respond to **no rule but
`optimize_projections`** — they are ordered-set-aggregate and hash-aggregate
implementation wins. The window frame SUM (9.3×) responds to `push_down_limit`
as well, so it is not purely an execution result: `LIMIT 1000` above a window
has to reach the scan or the query materializes the whole table. The earlier
revision listed the window shape as not optimizer-dependent; that is wrong, and
`push_down_limit` accordingly moves up a tier.

## Consequence: the must-have list is smaller than the full 24

Ordered by measured blast radius — how many headline shapes change when the rule
is removed — not by rule purpose.

| Rule | Shapes changed | LOC (DF) | Est. LOC (ours) | Tier | Risk notes |
|---|---|---|---|---|---|
| `optimize_projections` | **12 of 12** | — (large) | 600–900 | **MUST** | The scan interface. Sets `TableScan.projection`; without it every scan is a wide decode. Needs correct column-requirement propagation through joins, aggregates, windows, unions — the recursion is where bugs live |
| `push_down_filter` | 6 | 3,469 | 900–1,400 | **MUST** | The other scan interface: populates `TableScan.filters` for row-group/bloom pruning. Full DF version handles ~20 node types; a version that pushes only into Scan/Projection/Filter/Join/Union covers the benchmark |
| `simplify_expressions` | 6 | (own dir) | 400–700 | **MUST** | Not for join normalisation (refuted above) but for constant folding + `BETWEEN` expansion, without which pushed predicates may be unprunable. Scope to: literal folding, cast folding, `BETWEEN`/`IN` expansion, boolean identities |
| `extract_equijoin_predicate` | 4 | 400 | 200–300 | **MUST** | Best leverage-per-line in the set. Sole optimizer dependency of the 28× 2-table JOIN GROUP BY. Without it equijoins plan as nested loop |
| `push_down_limit` | 4 | 918 | 250–400 | **MUST** | Needed by star join, LATERAL, correlated subquery **and** the 9.3× window frame. Was Tier 2; promoted |
| `scalar_subquery_to_join` | 1 (the 113×) | 980 | 250–400 (+ pull-up) | **MUST** | Needs the count-bug `__always_true` machinery: the baseline plan really does contain `CASE WHEN __scalar_sq_1.__always_true IS NULL THEN Int64(0) …`. Dropping that gives wrong answers (NULL for 0), not slow ones |
| *shared:* correlated pull-up | — | 513 (`decorrelate.rs`) | 250–350, +150–200 with count-bug | **MUST** | Shared by all decorrelation rules. The real complexity sink: correlated-column set tracking, empty-batch evaluation, pull-through-aggregate legality |
| `decorrelate_predicate_subquery` | 0 headline; 2 near-headline (`IN`, `NOT IN`), and the 4.19× `EXISTS` | 1,780 | 350–450 | **SHOULD** | No *headline* number depends on it, but `EXISTS`/`IN` are pervasive in ORM SQL. Null-aware anti-join and `LeftMark` join are the expensive correctness corners; both are droppable at a stated cost |
| `single_distinct_aggregation_to_group_by` | 1 (the 30×) | 585 | 200–300 | **SHOULD** | Sole dependency of the 30× `COUNT(DISTINCT)` win |
| `replace_distinct_aggregate` | 1 (the 11×) | 207 | 100–150 | **MUST** | Correctness, not perf: lowers `DISTINCT`/`UNION` dedup to an `Aggregate`. Nothing else provides dedup |
| `eliminate_group_by_constant` | 1 (the 113×) | 225 | 80–120 | **SHOULD** | Was Tier 3. It participates in the correlated-subquery plan, so a Tier 1 shape depends on it — see the build-order warning below |
| `eliminate_cross_join` | **0** | 1,133 | 350–500 | **NICE** | Refuted as the source of the 261×. Still wanted eventually for comma-join SQL from ORMs, where DF's greedy connectivity pairing beats a naive left-deep pairing. Not needed to reproduce any published number |
| `rewrite_set_comparison` | 0 | 129 | ~100 (40 without NULL branches) | **NICE** | Basin's own `AnyAllToScalarSubquery` already pre-empts it; see correction 2 below |
| `optimize_unions` | 0 | 415 | 150–250 | **SKIP** | Nested-union flattening. Measured cost of skipping: none on any published shape |
| `eliminate_outer_join` | 0 | 371 | — | **SKIP** | Was Tier 2 on purpose-based reasoning; no shape needs it |
| `filter_null_join_keys` | 0 | 304 | — | **SKIP** | Absent from the earlier table entirely |
| `common_sub_expression_eliminate` | 0 | 1,491 | — | **SKIP** | Constant-factor loss only, as the earlier revision said — now confirmed by ablation |
| `extract_leaf_expressions` + `push_down_leaf_projections` | 0 | 2,158 + — | — | **SKIP** | Nested/struct-column pruning. Basin's JSONB path uses promoted shadow columns (`fast_select.rs`) instead. **The single largest skippable block** |
| `propagate_empty_relation` (473), `eliminate_limit` (246), `eliminate_filter` (163), `eliminate_duplicated_expr` (141), `eliminate_join` (83) | 0 | 1,106 | — | **SKIP** | Narrow simplifications for degenerate plans |
| `decorrelate_lateral_join` | **0** | 99 | — | **SKIP** | Never fires on the benchmark shape; Basin's textual rewrite does the work |
| Basin's `rewrite_lateral_*` (6 rewrites) | 1 (the 462×) | — | 400–700 as real IR rewrites | **MUST** | Currently string surgery. Porting them *as* plan rewrites is strictly better and is the actual owner of the 462× |

**Total for MUST + SHOULD: roughly 4,400–6,700 LOC**, against ~10,500 LOC of
DataFusion source replaced. The measurable skip list is
`extract_leaf_expressions` + `push_down_leaf_projections` +
`common_sub_expression_eliminate` + `optimize_unions` + `eliminate_outer_join` +
`filter_null_join_keys` + the five degenerate-plan eliminators +
`decorrelate_lateral_join` — **about 6,100 LOC of DataFusion that no published
number depends on.**

This is *lower* than the earlier "~8–12k LOC stands" estimate, and the reason is
not optimism: it is that ablation moved `eliminate_cross_join` (1,133),
`decorrelate_lateral_join` (99), `optimize_unions` (415) and
`eliminate_outer_join` (371) off the critical path, while adding
`optimize_projections` to it. The risk did not shrink, it **relocated** — from
join-tree construction, which DataFusion barely does, to the planner/storage
interface, which it does constantly.

### What we lose by skipping, stated measurably

- `extract_leaf_expressions` / `push_down_leaf_projections`: no headline
  regression. Cost lands on queries that select a few fields out of large
  structs — for Basin, JSONB. Basin's promoted-shadow-column path already covers
  the measured JSONB shapes, all of which are **losses** to Postgres today, so
  skipping cannot regress a published win.
- `common_sub_expression_eliminate`: repeated evaluation of shared
  subexpressions. Constant factor, worst on wide expression lists reusing the
  same `CASE`/JSONB extraction. No published win moves.
- `optimize_unions`: nested `UNION ALL` trees stay nested. Extra operator
  layers, no asymptotic change. The 11× UNION win is unaffected (it comes from
  `replace_distinct_aggregate`).
- `eliminate_cross_join`: **the one skip with a real forward risk.** Nothing
  published depends on it, but comma-join SQL (`FROM a, b, c WHERE …`) is
  common in ORM and legacy output. Without it, an N-relation comma join
  degenerates to a cross product with a filter on top — an *asymptotic* loss,
  not a constant one. Recommendation: skip it for the benchmark, but budget it
  before any ORM-compat claim, and keep the ORM corpus test as the guard.
- `propagate_empty_relation` and friends: degenerate plans stay in the plan.
  Costs a scan of an empty input. Immaterial.

### Build order: the tiers are not one

**The earlier Tier 1/2/3 split is not a safe build order, and the ablation shows
two concrete violations:**

1. **A Tier 1 shape depends on a Tier 3 rule.** The 113× correlated subquery's
   plan changes when `eliminate_group_by_constant` (Tier 3, "defensible to
   skip") is removed. Building Tier 1 alone would not reproduce that number.
2. **The most load-bearing rule was in no tier at all.**
   `optimize_projections` appears in the rule inventory with LOC "—" and is
   absent from all three tiers, yet it changes every shape tested.

There is also a structural reason not to treat any linear list as a build
order: DataFusion applies the whole list up to `max_passes = 3` and relies on
that loop. On the star join, `eliminate_cross_join`, `push_down_filter` and
`push_down_limit` each fire on three separate passes — the final plan is a
*fixpoint*, not the output of a pipeline. The owned optimizer therefore needs
the same fixpoint driver (a plan-signature loop with a pass cap) as part of the
**first** increment, not as a later refinement, or rules ported later will
silently fail to compose with rules ported earlier.

Recommended increments, replacing the tiers:

1. **Fixpoint driver + `optimize_projections` + `push_down_filter` +
   `simplify_expressions`.** This is the planner/storage interface; it makes the
   six scan-and-execution wins real and is a prerequisite for everything else.
2. **`extract_equijoin_predicate` + `push_down_limit` +
   `replace_distinct_aggregate`.** Cheap, and lands the 28× JOIN GROUP BY, the
   11× UNION and the 9.3× window frame.
3. **Correlated pull-up + `scalar_subquery_to_join` +
   `eliminate_group_by_constant`.** Lands the 113×.
4. **Basin's LATERAL rewrites as IR rewrites + `ROW_NUMBER` window path.**
   Lands the 462×. Note this increment is mostly *not* optimizer work.
5. **`single_distinct_aggregation_to_group_by`** (30×), then
   `decorrelate_predicate_subquery` for ORM `EXISTS`/`IN` coverage.

The 261× star join needs nothing beyond increments 1 and 2 — which is the
single most surprising result in this document.

## Corrections to earlier assumptions

Three things assumed earlier in this migration are wrong and are corrected here:

1. **Spilling is reachable and must be built.** `session.rs` wires a
   process-wide bounded `query_memory_pool` into the `RuntimeEnv` precisely so
   "a single heavy aggregate spills or fails cleanly instead of OOM-killing the
   shared node." Deferring spill entirely, as the initial sizing suggested,
   would regress a deliberate multi-tenant safety property. The owned aggregate
   and sort operators need a spill path, or an equally explicit fail-clean path.

2. **`RewriteSetComparison` ordering is load-bearing.** Basin's
   `AnyAllToScalarSubquery` exists specifically to pre-empt it. In the owned
   optimizer there is no `RewriteSetComparison` to pre-empt, so Basin's rule
   should be reconsidered as the *primary* ANY/ALL strategy rather than a
   defensive prepend. Note it covers only ANY/ALL over a **subquery**; the
   array-literal form is a separate path (correction 5 above).

3. **Nothing is currently disabled, so there is no "DF defaults we already
   don't trust" to inherit.** `with_optimizer_rules` only ever *prepends*;
   `skip_failed_rules` is never set anywhere in `crates/`, leaving its `false`
   default, and no analyzer or optimizer rule is removed. Every plan Basin
   produces today is the product of all 24 DF rules run to fixpoint. Any rule we
   choose not to port is therefore a behaviour change, not the removal of
   something already bypassed — which is what makes the SKIP list above worth
   stating measurably rather than assuming.

## Resolved: join-order selection does not exist to port

This was flagged as the biggest unresolved risk. It resolves in the most
convenient possible way: **DataFusion 53 performs no cost-based join
reordering anywhere, logical or physical.** There is nothing to port because
there is nothing there.

What exists is `JoinSelection`, a *physical* rule
(`datafusion-physical-optimizer-53.1.0/src/join_selection.rs`, position 3 in the
default physical list) that does three things, none of them reordering:

- **Build-side swap only.** `statistical_join_selection_subrule` inspects each
  `HashJoinExec` / `CrossJoinExec` / `NestedLoopJoinExec` **individually** and
  calls `swap_inputs()` when `should_swap_join_order(left, right)` is true. It
  swaps the two children of one join node. It never moves a relation between
  join nodes, never re-associates the tree, and never enumerates alternative
  shapes. No DP, no greedy cardinality ordering, no join-graph search.
- **Statistics consulted, shallowly.** `should_swap_join_order` compares
  `total_byte_size` first, falls back to `num_rows`, and — decisively for us —
  returns `Ok(false)` when either side's value is missing. **Absent statistics
  mean no swap: the join order stays exactly as the SQL wrote it.**
  (`datafusion.execution.collect_statistics` defaults to `true` in 53, so
  Parquet-backed `ListingTable`s do supply footer row counts.)
- **Partition-mode choice.** `try_collect_left` picks `CollectLeft` vs
  `Partitioned` against `hash_join_single_partition_threshold` (default
  `1024 * 1024` bytes) and `hash_join_single_partition_threshold_rows` (default
  `1024 * 128`). Basin pins `with_target_partitions(1)` in `session.rs`, so the
  partitioned path is largely moot for us and `CollectLeft` is the operative
  mode.

The only thing in DataFusion that reorders relations at all is
`eliminate_cross_join`, and its algorithm is **syntactic, not cost-based**:
`find_inner_join` walks the remaining inputs in order and takes the **first**
one that shares any equijoin key with the accumulated left side
(`eliminate_cross_join.rs:299–340`), building a left-deep tree. It consults
`JoinKeySet` connectivity and `can_hash` on the key type — never a cardinality
or a byte size. If nothing connects, it emits a cross join and moves on.

**Consequence for the 261× star join: it does not depend on join-order
selection.** The benchmark SQL already writes the good order —
`events ⋈ users` on `e.user_id = u.id`, then `categories` via
`e.amount BETWEEN c.min_amt AND c.max_amt` — and DataFusion preserves it. Both
engines execute the same order; the 261× comes from executing that order over
pruned columnar batches instead of heap tuples. This is corroborated by the
ablation: removing `eliminate_cross_join` leaves the star join plan unchanged.

What the owned engine still must do, and must not confuse with reordering:

- Pick a **build side** per join. Cheap heuristic parity with DF: prefer the
  smaller side by byte size, then row count, and leave the order alone when
  either is unknown. ~50 LOC.
- Convert non-equi join conditions to a nested-loop join. The star join's
  `BETWEEN` arm is exactly this shape and `extract_equijoin_predicate`
  deliberately leaves it as a join `Filter`.

**Forward risk, stated plainly:** inheriting "no join reordering" means Basin is
as exposed as DataFusion to badly-ordered user SQL. That is a *pre-existing*
property of the published numbers, not a migration regression, and any future
reordering work is a net-new feature rather than parity work. It should not be
funded out of the migration budget.

## Still outstanding

- Whether the unfolded `CAST` left behind by skipping `simplify_expressions`
  actually defeats Basin's bloom/zone-map pruning. Marked **UNVERIFIED** above;
  needs the DF-`Expr` → `basin_storage::Predicate` conversion traced, or simply
  a timed run with the rule removed.
- The ablation measures plan-text change, not wall-clock. A follow-up that times
  each headline shape with each rule removed would convert "changes the plan"
  into "costs N×" and would let the SKIP list be defended numerically rather
  than structurally.
- Physical-rule dependencies were not ablated. `AggregateStatistics`,
  `TopKAggregation`, `LimitedDistinctAggregation` and `LimitPushPastWindows` are
  plausible contributors to the `COUNT(*)`, top-K and window numbers and are
  outside this document's scope.
