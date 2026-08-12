---
title: "DF removal 02 — logical IR surface Basin actually uses"
nav_section: migration
sidebar_position: 2
summary: "DF removal 02: an exhaustive audit of which DataFusion LogicalPlan and Expr variants Basin constructs, inspects, or never touches — and why the string-planning path makes most of the 'never touched' list an illusion."
---

# 02 — How much of DataFusion's logical IR does Basin actually use?

- **Status:** Survey, complete as of `feat/own-engine-remove-datafusion` @ `1d5fa7e6`
- **Scope:** `datafusion::logical_expr::LogicalPlan` (25 variants in DF 53) and
  `datafusion::logical_expr::Expr` (34 variants in DF 53)
- **Tags:** query-engine, datafusion-removal, logical-ir

## TL;DR

The reachable `Expr` surface is **effectively open, not bounded.**

Basin *names* only 12 of 34 `Expr` variants and 9 of 25 `LogicalPlan` variants
in code that reads or writes their fields. But **every production query reaches
DataFusion through `SessionContext::sql(&str)`** — 38 call sites, zero calls to
`execute_logical_plan`. The SQL string planner can emit any variant it likes,
and Basin then hands the resulting plan to DataFusion's **full default
optimizer rule list** (27 rules, cloned wholesale at
`session.rs:2727`) plus its default analyzer rules. Basin's own four optimizer
rules and one analyzer rule run *inside* that pipeline and must survive
whatever the other 27 produce.

So the "never touched" list below is real as *Basin-authored code*, and mostly
**not** real as *runtime reachability*. The genuinely-dead variants are a much
smaller set, enumerated in [§5](#5-what-is-genuinely-dead).

## 1. Two different `Expr` enums — do not conflate them

A naive `grep -r 'Expr::'` over `crates/` returns ~750 hits across 52 variant
names. That is misleading: **most of them are `sqlparser::ast::Expr`, not
DataFusion's `Expr`.** The two enums share a name and share almost no variants.

| File | Which `Expr` is in scope | Evidence |
| --- | --- | --- |
| `executor.rs`, `dml_mutate.rs`, `prepared.rs`, `fast_select.rs`, `session.rs` | **sqlparser** (`Value`, `Identifier`, `BinaryOp`, `Nested`, `CompoundIdentifier`, `AnyOp`, `TypedString`, …) | `session.rs:50`, `dml_mutate.rs` / `executor.rs` `use sqlparser::ast::…` |
| `query_shape.rs`, `citext_analyzer.rs`, `is_distinct_rewrite.rs`, `nullif_rewrite.rs`, `any_all_rewrite.rs`, `union_scan_collapse.rs`, `pg_plan.rs`, `rls.rs`, `lifecycle.rs`, `jsonb_udf.rs`, `jsonb_path_udf.rs` | **DataFusion** | `use datafusion::logical_expr::{…, Expr, …}` |

`session.rs` imports both: `datafusion::logical_expr::{col, SortExpr}` at line
47 (used only at `session.rs:3135–3139` to build listing-table file sort orders)
and `sqlparser::ast::Expr` at line 50 (everything else). Its
`recognise_selective_predicate` (`session.rs:4379`) and `walk_predicate`
(`session.rs:6393`) walk **sqlparser** trees.

**This document is exclusively about the DataFusion enums.** The sqlparser
surface is the front end and is governed by ADR 0014 (pg_query migration), not
by the owned-IR scoping decision.

## 2. `LogicalPlan` variants Basin constructs

These need a real constructor, real field layout, and real schema derivation in
the owned IR.

| Variant | Constructed at | What is built |
| --- | --- | --- |
| `TableScan` | `pg_plan.rs:154` (`LogicalPlanBuilder::scan`) | Only pg_plan builds one directly; the rewrite rules re-wrap an existing `Arc<LogicalPlan>` scan (`union_scan_collapse.rs:216`). |
| `Filter` | `pg_plan.rs:160`, `rls.rs:421`, `lifecycle.rs:590`, `citext_analyzer.rs:112`, `union_scan_collapse.rs:229` | RLS and soft-delete both wrap every matching `TableScan` in a `Filter` via `LogicalPlanBuilder::filter`. |
| `Projection` | `pg_plan.rs:192`, `is_distinct_rewrite.rs:244`, `union_scan_collapse.rs:256` | `union_scan_collapse` uses `Projection::new_from_schema` to preserve the exact output qualifier. |
| `Sort` | `pg_plan.rs:221`, `is_distinct_rewrite.rs:243`, `citext_analyzer.rs:141`, `citext_analyzer.rs:147` | Needs `fetch: Option<usize>` (top-k fused into sort). |
| `Limit` | `pg_plan.rs:229` | `skip` + `fetch`, both `Option`. |
| `Aggregate` | `is_distinct_rewrite.rs:214` (`Aggregate::try_new`), `any_all_rewrite.rs:155` (`LogicalPlanBuilder::aggregate`) | Output layout contract is load-bearing: group columns first, then aggregate columns (`is_distinct_rewrite.rs:217–222`). |
| `Union` | `union_scan_collapse.rs:131`, `:148`, `:162`, `:292` | Built via `Union::try_new_with_loose_types`. |
| `Distinct(Distinct::On)` | `is_distinct_rewrite.rs:173` (re-emits untransformed) | Constructed only to hand back unchanged; the rule's job is to *lower* it. |
| `Subquery` | `any_all_rewrite.rs:160` via `expr_fn::scalar_subquery` | Wrapped immediately into `Expr::ScalarSubquery`. |
| `Explain` | `explain.rs:85` via `DataFrame::explain(verbose, analyze)` | Never pattern-matched; only its `(plan_type, plan)` output batch is read. |

## 3. `LogicalPlan` variants Basin inspects (field-level reads)

| Variant | Inspected at | Fields read |
| --- | --- | --- |
| `TableScan` | `executor.rs:11205`, `prepared.rs:2537`, `rls.rs:415`, `lifecycle.rs:587`, `union_scan_collapse.rs:87`, `:105`, `query_shape.rs:87` | `table_name` (schema + table) only. Never `projection`, never `filters`, never `fetch`. |
| `Filter` | `rls.rs:483`, `lifecycle.rs:616`, `citext_analyzer.rs:108`, `union_scan_collapse.rs:93`, `query_shape.rs:68` | `predicate`, `input`. |
| `Projection` | `union_scan_collapse.rs:43`, `:76`, `:96`, `query_shape.rs:63` | `expr`, `input`. |
| `Aggregate` | `any_all_rewrite.rs:83`, `query_shape.rs:96` | `group_expr`, `aggr_expr`. |
| `Sort` | `citext_analyzer.rs:121`, `query_shape.rs:106` | `expr: Vec<SortExpr>`, `fetch` (presence only), `input`. |
| `Join` | `query_shape.rs:71` | `join_type`, `on: Vec<(Expr, Expr)>`, `filter: Option<Expr>`. All 10 `JoinType`s are enumerated at `query_shape.rs:312–323` including `LeftMark` / `RightMark`. |
| `Limit` | `query_shape.rs:113` | `skip` / `fetch` **presence** only — values are deliberately erased. |
| `Distinct` | `is_distinct_rewrite.rs:140` | `Distinct::On(DistinctOn { on_expr, select_expr, sort_expr, input, schema })` — all five fields, `is_distinct_rewrite.rs:177–182`. |
| `Window` | `union_scan_collapse.rs:39` | Presence only (bail-out guard). |
| `Repartition` | `union_scan_collapse.rs:_` (via the `_ => true` catch-all) | Presence only. |

`union_scan_collapse.rs:33–48` is the one place with a deliberate
**closed-world bail-out**: `Aggregate | Limit | Sort | Distinct | Window | Join`
block collapsing, `Filter` / `Projection` recurse, `TableScan` terminates, and
`_ => true` refuses everything else. That `_` arm is the correct pattern for the
owned IR: unknown node ⇒ decline the optimisation.

## 4. `LogicalPlan` variants named but only as a discriminant tag

`query_shape.rs:334–361` is an **exhaustive** `match` over all 25 variants that
emits a single stable `u8`. It reads no fields for these 15:

`Repartition` (342), `EmptyRelation` (345), `Subquery` (346), `SubqueryAlias`
(347), `Statement` (349), `Values` (350), `Explain` (351), `Analyze` (352),
`Extension` (353), `Dml` (355), `Ddl` (356), `Copy` (357), `DescribeTable`
(358), `Unnest` (359), `RecursiveQuery` (360).

The `hash_plan` body at `query_shape.rs:122–126` explicitly documents the
choice: "For all other plan types … the discriminant hash is sufficient."

**Implication for the owned IR:** the shape-hash contract (ADR 0017, seed
`basin_sketch::QUERY_SHAPE_SEED`, "do not change that constant") pins a *stable
numeric ordering* of plan variants. Whatever the owned IR's variant set is, the
mapping from owned variant → these tag bytes must be preserved for the variants
that survive, or every downstream shape record is invalidated. This is a hard
constraint the migration cannot ignore.

## 5. What is genuinely dead

Two different questions. Answering both honestly:

### 5a. Never named by Basin at all — zero occurrences outside `query_shape`'s tag table

`Values`, `EmptyRelation`, `SubqueryAlias`, `Statement`, `Ddl`, `Dml`, `Copy`,
`DescribeTable`, `Analyze`, `Unnest`, `RecursiveQuery`, `Extension`,
`Repartition`.

### 5b. Actually unreachable at runtime — the real savings

Cross-checked against what can be fed to `ctx.sql`:

| Variant | Verdict | Why |
| --- | --- | --- |
| `Extension` | **Truly dead.** | `grep -r UserDefinedLogicalNode crates/` returns zero hits. Basin has no custom logical node. (`alter.rs:53 BasinAlterExtension` is an unrelated `enum` about `ALTER TABLE` syntax.) |
| `Ddl` | **Truly dead.** | Every `ctx.sql` call site passes a SELECT-shaped string. DDL is intercepted by `executor.rs` and executed against Basin's own catalog; DataFusion's `CREATE TABLE` planner is never invoked. |
| `Dml` | **Truly dead.** | `dml_mutate.rs:108`, `:169` pass *subquery* SQL (`Expr::InSubquery` / `Expr::Subquery` rendered back to text). `executor.rs:7490` passes the SELECT half of `INSERT … SELECT`. DataFusion never plans an INSERT/UPDATE/DELETE. |
| `Copy` | **Truly dead.** | `COPY` is handled by `copy_ingest.rs`, not routed to DF. |
| `DescribeTable` | **Truly dead.** | Basin answers `\d` / describe via `info_schema_provider.rs`. |
| `Analyze` | **Truly dead.** | `explain.rs:85` calls `df.explain(verbose, analyze)`, which builds `LogicalPlan::Explain` with an `analyze` flag — DF 53 does not emit a separate `Analyze` node from this path. |
| `Statement` | **Truly dead.** | `SET` / `PREPARE` / `DEALLOCATE` are intercepted in `executor.rs` and `prepared.rs` before DF. |
| `Repartition` | **Truly dead.** | Only produced by DF's `DISTRIBUTE BY` / `PARTITION BY` extension syntax, which Basin's parser rejects upstream. |
| `Values` | **Reachable.** | `SELECT * FROM (VALUES (1),(2))` plans fine through `ctx.sql`. |
| `EmptyRelation` | **Reachable.** | `SELECT 1` with no FROM. |
| `SubqueryAlias` | **Reachable, and common.** | Every derived table and CTE. `view_ddl::rewrite_view_refs` (`executor.rs:11153`) deliberately rewrites view references *into* derived tables — Basin manufactures `SubqueryAlias` nodes on the standard read path without ever naming the variant. |
| `Subquery` | **Reachable.** | Correlated subqueries in WHERE; also constructed by `any_all_rewrite.rs:160`. |
| `Unnest` | **Reachable.** | `unnest(array_col)` is a DF built-in and Basin registers DF's default expr planners (`session.rs:2769`). |
| `RecursiveQuery` | **Reachable.** | `executor.rs:13033` routes `WITH RECURSIVE … INSERT` specially, which implies plain `WITH RECURSIVE … SELECT` falls through to DF. |
| `Window` | **Reachable.** | Window functions are registered explicitly (`session.rs:2770 with_window_functions`). |

**Net:** 8 of 25 `LogicalPlan` variants are genuinely eliminable
(`Extension`, `Ddl`, `Dml`, `Copy`, `DescribeTable`, `Analyze`, `Statement`,
`Repartition`). The other 17 are all reachable from user SQL.

## 6. `Expr` variants — construction

| Variant | Constructed at |
| --- | --- |
| `Column` | `pg_plan.rs:218`, `:258`, `:300` (via `col()`); `any_all_rewrite.rs:148` (explicit `Expr::Column(Column::new(…))`); `is_distinct_rewrite.rs:238`; `session.rs:3139` |
| `Literal` | `pg_plan.rs:321–330` — **only four literal kinds**: `i64`, `f64`, `&str`, `bool`; `any_all_rewrite.rs:178` (`lit(true)`) |
| `BinaryExpr` | `pg_plan.rs:302–307` (`Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq` only); `any_all_rewrite.rs:161`; `citext_analyzer.rs:184`; `is_distinct_rewrite.rs:261–272` (`And`/`Or`/`Eq`/`NotEq`); `nullif_rewrite.rs:78`, `:87`; `rls.rs:499` (`combine_or`); `union_scan_collapse.rs:224` (`acc.or(p)`) |
| `IsNull` / `IsNotNull` | `any_all_rewrite.rs:177`; `is_distinct_rewrite.rs:261–271`; `nullif_rewrite.rs:78`, `:87` |
| `IsTrue` | `any_all_rewrite.rs:170` |
| `Case` | `any_all_rewrite.rs:174` (single `when_then` pair + `else`, no base expr) |
| `ScalarSubquery` | `any_all_rewrite.rs:160` |
| `AggregateFunction` | `any_all_rewrite.rs:152–153` (`min`, `max`); `is_distinct_rewrite.rs:209` (`first_value` with an ORDER BY tail) |
| `ScalarFunction` | `citext_analyzer.rs:231–232` (`lower()` only) |
| `Alias` | `is_distinct_rewrite.rs:238` (`alias_qualified`) |
| `Like` | `citext_analyzer.rs:210` (rebuilt with `case_insensitive: true`) |

Note the `SortExpr` struct (`{ expr, asc, nulls_first }`) is **not** an `Expr`
variant but is constructed at `pg_plan.rs:218`, `citext_analyzer.rs:141`,
`session.rs:3139`, and read at `query_shape.rs:297`. It needs its own type in
the owned IR.

## 7. `Expr` variants — field-level inspection

| Variant | Inspected at | Fields read |
| --- | --- | --- |
| `BinaryExpr` | `is_distinct_rewrite.rs:246`, `citext_analyzer.rs:172`, `query_shape.rs:158` | `left`, `op`, `right`. Operators actually branched on: `IsDistinctFrom`, `IsNotDistinctFrom` (`is_distinct_rewrite.rs:249`, `:266`); `Eq`/`NotEq`/`Lt`/`LtEq`/`Gt`/`GtEq` (`citext_analyzer.rs:176–181`); `And`/`Or` (`query_shape.rs:160`). |
| `SetComparison` | `any_all_rewrite.rs:96–147` | `expr`, `subquery` (incl. `subquery.outer_ref_columns` and its schema arity), `op`, `quantifier`. The only place Basin reads this DF-53-specific variant. |
| `ScalarFunction` | `nullif_rewrite.rs:60–71`, `query_shape.rs:173` | `func.name()` (string-compared to `"nullif"`), `args`. |
| `IsNull` / `IsNotNull` | `nullif_rewrite.rs:76`, `:85` | inner expression. |
| `Column` | `citext_analyzer.rs:240`, `query_shape.rs:153`, `union_scan_collapse.rs` | `relation`, `name` — resolved against a `DFSchema` for field metadata (`BASIN_TYPE=CITEXT`). |
| `Cast` / `TryCast` | `citext_analyzer.rs:244`, `:247`; `query_shape.rs:205`, `:209` | `expr`, `data_type`. |
| `Alias` | `citext_analyzer.rs:252`, `query_shape.rs:200` | `expr`. |
| `Like` | `citext_analyzer.rs:199`, `query_shape.rs:232` | `negated`, `expr`, `pattern`, `escape_char`, `case_insensitive`. |
| `SimilarTo` | `query_shape.rs:232` | same `Like` struct. |
| `Literal` | `jsonb_udf.rs:5788–5789`, `jsonb_path_udf.rs:1541–1543`, `:1567–1568`, `query_shape.rs:147` | `ScalarValue` — only `Utf8`, `LargeUtf8`, `LargeBinary` are decoded by the table functions; `query_shape` reads only `.data_type()` and discards the value. |
| `AggregateFunction` | `query_shape.rs:284` | `func.name()`, `params.distinct`, `params.args`. |
| `InList` | `query_shape.rs:216` | `expr`, `negated`, `list.len()` — list contents are erased. |
| `Between` | `query_shape.rs:223` | `expr`, `negated`, `low`, `high`. |
| `Case` | `query_shape.rs:245`, `any_all_rewrite.rs:368` (test) | `expr`, `when_then_expr`, `else_expr`. |
| `Not`, `IsTrue`, `IsFalse`, `IsUnknown`, `IsNotTrue`, `IsNotFalse`, `IsNotUnknown`, `Negative` | `query_shape.rs:186–197` | single boxed inner expression, uniformly. |
| `ScalarSubquery` | `any_all_rewrite.rs:305` (test), `query_shape.rs` (tag) | `subquery`. |

## 8. `Expr` variants named only as a discriminant tag

`query_shape.rs:366–403` is an exhaustive `match` over all 34 variants
producing a stable `u8`. The following 9 are **tag-only** — no code anywhere in
the workspace reads their fields:

| Variant | Tag | `query_shape.rs` line |
| --- | --- | --- |
| `ScalarVariable` | 2 | 370 |
| `WindowFunction` | 23 | 391 |
| `Exists` | 25 | 393 |
| `InSubquery` | 26 | 394 |
| `GroupingSet` | 28 | 396 |
| `Placeholder` | 29 | 397 |
| `OuterReferenceColumn` | 30 | 398 |
| `Unnest` | 31 | 399 |
| `Wildcard` (deprecated in DF 53) | 33 | 402 |

`hash_expr`'s `_ => {}` arm at `query_shape.rs:264–269` documents the intent:
"the expression-type tag … is sufficient to distinguish shape without exposing
literal values."

### Are these real savings?

Mostly **no**:

- `WindowFunction`, `Exists`, `InSubquery`, `GroupingSet` (`GROUPING SETS` /
  `CUBE` / `ROLLUP`), `Unnest`, `OuterReferenceColumn` (any correlated
  subquery), and `Wildcard` (`SELECT *` before expansion) are all trivially
  reachable from user SQL through `ctx.sql`.
- `ScalarVariable` — **plausibly dead.** DF emits it for `@@`-style system
  variables; Basin intercepts `SHOW`/`SET` in `executor.rs` and serves
  `current_user`/`version()` etc. as scalar UDFs (`pg_catalog_udf.rs`). No
  evidence of a reachable path.
- `Placeholder` — **genuinely dead in DF plans.** Basin substitutes `$N`
  bind parameters at the **sqlparser AST level** before the SQL string is
  handed to DF. `prepared.rs:1996` states the contract explicitly: *"after the
  walk, NO `Value::Placeholder` may remain."* Basin never calls
  `DataFrame::with_param_values` (zero hits). So the owned IR does not need a
  placeholder expression node at all — parameter binding happens strictly
  upstream of the IR.

**Net:** 2 of 34 `Expr` variants (`ScalarVariable`, `Placeholder`) are
eliminable. `SetComparison` and `Wildcard` are DF-53 artefacts an owned IR could
redesign away (`SetComparison` folded into a general quantified-comparison node;
`Wildcard` resolved during name resolution and never reaching the IR), which
brings the practical target to ~30 expression forms.

## 9. The string-planning path — the decisive finding

**Basin has no programmatic plan-execution path in production.**

| Fact | Evidence |
| --- | --- |
| 38 `ctx.sql(...)` call sites | `executor.rs:4709`, `:7490`, `:7505`, `:11186`, `:13147`, `:15526`, `:15547`; `dml_mutate.rs:108`, `:169`, `:7648`; `prepared.rs:2516`, `:2527`, `:2745`; `rls.rs:466`, `:827`; `lifecycle.rs:572`; `explain.rs:76`; `constraints.rs:1664`; `generated_cols.rs:112`; `type_ddl.rs:539`; plus test sites. |
| 0 calls to `execute_logical_plan` | `grep -rn execute_logical_plan crates/` → only the doc comment at `pg_plan.rs:22`. |
| `pg_plan::translate` is **dead code in production** | The only non-test caller of anything in `pg_plan.rs` is `executor.rs:3152`, which calls `supports_shape(node)` purely to bump the `pg_plan_routing_count` telemetry counter (`lib.rs:1403`). The translated `LogicalPlan` is never executed. |
| RLS and soft-delete predicates are *obtained by planning a SQL probe* | `rls.rs:466` plans `SELECT 1 FROM "t" WHERE <user's USING clause>` then extracts the `Filter`'s predicate (`rls.rs:481–494`). `lifecycle.rs:570–581` does the same for `"<col>" IS NULL`. The injected `Expr` is therefore **whatever DF's SQL planner produced from a user-authored policy string** — arbitrary expression surface, by construction. |
| DF's full default optimizer + analyzer pipeline is retained | `session.rs:2727` clones the pre-built 27-rule list; Basin `insert(0, …)`s four rules. `session.rs:2765–2800` keeps `default_expr_planners`, `default_window_functions`, `default_table_functions`, `default_file_formats`, `default_table_factories`. |

### Honest assessment

The `Expr` surface reachable from user SQL is **effectively open**. Basin's own
`Expr::` grep hits are not a bound on what the engine must handle — they are a
bound on what Basin *chooses to reason about*. Everything else is delegated.

Three consequences for scoping the owned IR:

1. **The owned IR's expression enum cannot be scoped from this survey.** It
   must be scoped from the *SQL surface Basin commits to supporting*, which is a
   product decision, not a code-archaeology one. This survey bounds only the
   *rewrite/analysis* surface — the part of the IR that Basin's own passes must
   pattern-match.

2. **The rewrite surface genuinely is small and is the right thing to design
   first.** Basin's five owned passes (`is_distinct_rewrite`, `nullif_rewrite`,
   `any_all_rewrite`, `union_scan_collapse`, `citext_analyzer`) collectively
   touch 9 plan variants and 12 expression variants. Every one of them is
   written as `match { specific arms…, _ => no-op }`, so they compose safely
   with an IR that carries variants they do not know. That is the property to
   preserve.

3. **Two hard contracts constrain the migration independent of IR design:**
   - `QueryShapeHash` (ADR 0017): the tag bytes at `query_shape.rs:336–360` and
     `:368–402` and the `Operator` tags at `:409–441` are a persisted join key.
     Renumbering breaks every downstream shape record.
   - The probe-SQL trick in `rls.rs` / `lifecycle.rs`: the owned engine must
     still be able to plan an arbitrary Boolean SQL fragment against a table
     schema and hand back a predicate node, or RLS policies and soft-delete both
     need a new mechanism.

## 10. Out of scope for the logical IR

Confirmed physical-plan rules, not logical:

- `sort_streaming_limit.rs:81` — `PhysicalOptimizerRule`, walks
  `Arc<dyn ExecutionPlan>`.
- `catalog_window_exec.rs:79` — `PhysicalOptimizerRule`, elides `SortExec` above
  `WindowAggExec`.

Confirmed *not* an `Expr` consumer despite the signature:

- Every Basin `TableProvider::scan` takes `_filters: &[Expr]` and **ignores it**
  (`info_schema_provider.rs` ×19, `realtime_catalog.rs`, `query_stats_export.rs`,
  `project_usage_view.rs`, `gin_rowgroup_scan.rs`, `jsonb_posting_scan.rs`,
  `rtree_rowgroup_scan.rs`, `replication/slot_udf.rs`, `tombstone_cold_scan.rs`).
- `supports_filters_pushdown` in `session.rs:4098` and `hot_tombstone.rs:1263`
  **delegates verbatim to the cold `ListingTable`** and only downgrades
  `Exact → Inexact` when a hot overlay is live. It never inspects an `Expr`.
- Basin's own read-time predicate type (`basin-storage/src/predicate.rs`) is an
  independent enum (`Eq`/`Gt`/`Lt`/`StartsWith`/`InInt64`) built from
  **sqlparser** AST in `fast_select.rs:1454`, not from DF `Expr`. It has zero
  DataFusion coupling and survives the migration untouched.

Table-function argument inspection (`jsonb_udf.rs:5833`,
`jsonb_path_udf.rs:1549`, `notify_registry.rs:268`) reads `&[Expr]` but matches
only `Expr::Literal` with `Utf8` / `LargeUtf8` / `LargeBinary` payloads — a
one-variant surface.
