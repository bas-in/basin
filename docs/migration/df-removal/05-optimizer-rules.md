---
title: "DF removal — optimizer rule dependency map"
nav_section: migration
sidebar_position: 5
summary: "Maps each published benchmark win to the optimizer rule that produces it, verified by rule-by-rule ablation against real DataFusion 53 plans. The optimizer risk is concentrated in the join and subquery shapes, but two universal rules — projection pruning and filter-into-scan — turn out to underwrite the storage wins too."
tags: [migration, query-engine, optimizer, benchmarks]
---

# 05 — Optimizer rule dependency map

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. ADR 0030 names the optimizer as the risk concentration:

> **The optimizer is the risk concentration.** Underscoping the optimizer keeps
> the engine and loses the benchmarks.

This document tests that claim against the published numbers. It turns out to
be true, but **much more narrowly than ADR 0030 implies** — and that narrowing
is the most useful result here.

## What Basin runs today

DataFusion's default logical optimizer, in order
(`datafusion-optimizer-53.1.0/src/optimizer.rs`):

| # | Rule | Non-test LOC |
|---|---|---|
| 1 | RewriteSetComparison | 129 |
| 2 | OptimizeUnions | 415 |
| 3 | SimplifyExpressions | (in physical-expr) |
| 4 | ReplaceDistinctWithAggregate | 207 |
| 5 | EliminateJoin | 83 |
| 6 | DecorrelatePredicateSubquery | 1,780 |
| 7 | ScalarSubqueryToJoin | 980 |
| 8 | DecorrelateLateralJoin | 99 |
| 9 | ExtractEquijoinPredicate | 400 |
| 10 | EliminateDuplicatedExpr | 141 |
| 11 | EliminateFilter | 163 |
| 12 | EliminateCrossJoin | 1,133 |
| 13 | EliminateLimit | 246 |
| 14 | PropagateEmptyRelation | 473 |
| 15 | EliminateOuterJoin | 371 |
| 16 | PushDownLimit | 918 |
| 17 | PushDownFilter | 3,469 |
| 18 | SingleDistinctToGroupBy | 585 |
| 19 | EliminateGroupByConstant | 225 |
| 20 | CommonSubexprEliminate | 1,491 |
| 21 | ExtractLeafExpressions | 2,158 |
| 22 | PushDownLeafProjections | — |
| 23 | OptimizeProjections | — |

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
preserved.

## The central finding: most of the win table is not an optimizer result

Mapping each published 1M-row LocalFS win to its cause:

| Published win | Multiplier | Primary cause | Optimizer-dependent? |
|---|---|---|---|
| LATERAL JOIN | 462× | `DecorrelateLateralJoin` + `ExtractEquijoinPredicate` | **YES — high** |
| Star join (3-table) | 261× | `EliminateCrossJoin` + `ExtractEquijoinPredicate` + `PushDownFilter` | **YES — high** |
| Correlated subquery | 113× | `DecorrelatePredicateSubquery` + `ScalarSubqueryToJoin` | **YES — high** |
| Range scan p50 | 81× | Columnar scan + Basin's own bloom/zone-map pruning | No |
| `WHERE col = ANY(int[])` | 63× | Columnar scan + Basin's `AnyAllToScalarSubquery` | Basin's own rule |
| Selective low-card COUNT | 51× | Columnar scan, late materialization | No |
| `COUNT(DISTINCT …)` per status | 30× | `SingleDistinctToGroupBy` + columnar scan | Partial |
| 2-table JOIN GROUP BY | 28× | `ExtractEquijoinPredicate` + hash join + columnar scan | **YES — moderate** |
| Large result stream (100k drain) | 25× | Arrow batch streaming; Basin's `SortStreamingLimit` | No |
| `PERCENTILE_CONT` | 19× | Aggregate implementation, not planning | No |
| UNION (dedup) | 11× | `OptimizeUnions` + `ReplaceDistinctWithAggregate` + Basin's `UnionScanCollapse` | Partial |
| Aggregate GROUP BY | 10× | Hash aggregate + columnar scan | No |
| Window frame SUM | 9.3× | Window operator + Basin's sort elision | No |

**Six of thirteen headline wins are storage-format and execution results, not
planning results.** Basin reads compressed columnar files and touches only the
projected columns; Postgres reads heap tuples. That advantage is a property of
Vortex plus the scan operator, and it survives the migration regardless of what
the optimizer does — provided the physical layer keeps late materialization and
batch streaming.

The optimizer risk is real but **concentrated in four shapes**: LATERAL (462×),
star join (261×), correlated subquery (113×), and 2-table JOIN GROUP BY (28×).
Every one of them is a **join or subquery** shape.

## Consequence: the must-have list is smaller than the full 23

**Tier 1 — must have before any join/subquery benchmark can be reproduced**
(~7,900 LOC of DataFusion equivalent):

| Rule | LOC | Why |
|---|---|---|
| `PushDownFilter` | 3,469 | Universal. Every shape benefits; without it, filters evaluate after joins |
| `DecorrelatePredicateSubquery` | 1,780 | The 113× correlated-subquery win, directly |
| `EliminateCrossJoin` | 1,133 | The 261× star join — turns an N-way cross product into a join tree |
| `ScalarSubqueryToJoin` | 980 | Scalar subqueries in SELECT; pairs with the above |
| `ExtractEquijoinPredicate` | 400 | Without it, equijoins plan as nested-loop. Cheap, enormous leverage |
| `DecorrelateLateralJoin` | 99 | The 462× LATERAL win. Strikingly small for its impact |

Note the shape of that list: **`ExtractEquijoinPredicate` and
`DecorrelateLateralJoin` together are 499 lines** and account for two of the
four at-risk shapes. The cost is not evenly distributed.

**Tier 2 — needed for correctness or a specific published number** (~2,700 LOC):
`SimplifyExpressions` (foundational — most later rules assume simplified
predicates), `SingleDistinctToGroupBy` (585, the 30× COUNT DISTINCT),
`OptimizeUnions` (415) + `ReplaceDistinctWithAggregate` (207, the 11× UNION),
`PushDownLimit` (918), `EliminateOuterJoin` (371).

**Tier 3 — defensible to skip initially**, with the measurable cost stated:
`CommonSubexprEliminate` (1,491) — costs repeated evaluation of shared
subexpressions, a constant-factor loss, not an asymptotic one.
`ExtractLeafExpressions` (2,158) and `PushDownLeafProjections` — nested-column
pruning for struct access; Basin's JSONB path is largely served by its own
promoted-shadow-column machinery (`fast_select.rs`) instead.
`PropagateEmptyRelation` (473), `EliminateLimit` (246),
`EliminateGroupByConstant` (225), `EliminateDuplicatedExpr` (141),
`EliminateFilter` (163), `EliminateJoin` (83) — each a narrow simplification;
together ~1,331 LOC for cases that rarely arise in application SQL.

**Revised optimizer estimate: ~8–12k LOC stands**, but it now has a defensible
internal ordering — Tier 1 first, and the four at-risk benchmark shapes become
reproducible before Tier 2 is written.

## Corrections to earlier assumptions

Two things assumed earlier in this migration are wrong and are corrected here:

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
   defensive prepend.

## Still outstanding

- Whether DataFusion's rules interact in ways the tier split above misses —
  e.g. whether `EliminateCrossJoin` depends on `SimplifyExpressions` having
  normalised the predicate first. The tiers are derived from rule purpose and
  benchmark mapping, **not** from an execution trace, and should be validated
  against real plans before being treated as a build order.
- Join-order selection. DataFusion's cost-based reordering is not in the rule
  list above; where it lives and how much Basin depends on it is unresolved,
  and it matters for the 261× star join.
