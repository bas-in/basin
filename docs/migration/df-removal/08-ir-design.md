---
title: "DF removal — owned IR and engine design"
nav_section: architecture
sidebar_position: 80
summary: "Crate layout, type system, Expr/LogicalPlan shape, and lowering contract for Basin's own query engine. The load-bearing decision is that the logical type system becomes Postgres's while Arrow stays purely physical."
tags: [migration, query-engine, ir, pg-compat, datafusion-removal]
---

# 08 — Owned IR and engine design

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. This is the spec implementation builds against.

**Status:** design, not yet implemented. Nothing here is written to disk as code
yet, and per ADR 0030 as amended, none of it should be until the Phase 0 oracle
lands ([07](./07-conformance-tests.md)).

## 0. What already exists, and what it is worth

`crates/basin-engine/src/pg_plan.rs` (529 LOC) is ADR 0014 Phase 2: a
`pg_query` parse-tree → **DataFusion `LogicalPlan`** translator. It handles
single-table SELECT with projection, one WHERE comparison, ORDER BY, and LIMIT.

It is **dead code in production**. Only `supports_shape` is called
(`executor.rs:3152`), to bump a telemetry counter; `translate()` is never
invoked on a real query.

That makes it a **template, not a foundation**. Its traversal of `pg_query`
protobuf nodes — `NodeEnum` matching, `AExprKind`, `SortByDir`, `SetOperation`
— is directly reusable and is the only code in the tree that has done this job.
Its output type is not: it builds `LogicalPlanBuilder`, which is exactly what
goes away. Port the walk; replace the target.

## 1. Crate layout

Four new crates. The split is by *dependency direction*, so each can be tested
without the ones above it:

| Crate | Depends on | Contains |
|---|---|---|
| `basin-pgtype` | arrow-schema | The Postgres type system: `PgType`, OIDs, typmod, cast categories, operator/function overload resolution tables |
| `basin-plan` | `basin-pgtype`, `basin-catalog`, pg_query | `Expr`, `LogicalPlan`, lowering from the parse tree, the optimizer rules |
| `basin-exec` | `basin-plan`, `basin-storage`, arrow | Physical operators, expression evaluation, the streaming/suspendable execution model |
| `basin-pgcatalog` | `basin-pgtype`, `basin-catalog` | `pg_catalog` / `information_schema` as real relations (see [11](./11-pg-catalog-fidelity.md)) |

`basin-engine` keeps session management, DDL, DML orchestration, and the
pgwire-facing surface, and becomes a consumer of these rather than a wrapper
around DataFusion.

**Why `basin-pgtype` is its own crate and comes first:** it is the thing 249
UDFs, the catalog, the wire protocol, and the planner all agree on. If it lives
inside `basin-plan`, the UDF re-hosting work is blocked behind planner work for
no reason. It should be buildable and testable on day one.

## 2. The type system — the load-bearing decision

**Arrow is the physical representation. Postgres is the logical type system.**
Today these are conflated: Basin's logical types are Arrow types with PG
semantics bolted on, and DataFusion's `type_coercion` — which is not
Postgres's — governs coercion.

```rust
/// The logical type of a value, as Postgres understands it.
pub struct PgType {
    pub oid: Oid,          // 23 = int4, 25 = text, 1043 = varchar, 3802 = jsonb …
    pub typmod: i32,       // -1 when unspecified; varchar(n), numeric(p,s), timestamp(n)
}

/// How a PgType is laid out in Arrow. Many-to-one: varchar(10), text and
/// name all land on Utf8; uuid and point both land on FixedSizeBinary.
pub fn physical(ty: PgType) -> arrow_schema::DataType;
```

Three consequences that fall straight out of this and are impossible today:

- **`typmod` is enforced and reported.** `varchar(10)` rejects an 11-character
  write; `RowDescription` reports the real typmod. Clients that introspect
  column widths stop seeing `-1`.
- **`unknown` is a real type.** Postgres resolves untyped literals late
  (`SELECT 'x' = col` types the literal *from* the column). This drives a large
  share of real-world query behaviour, and an Arrow-first type system cannot
  express it — Arrow has no `unknown`.
- **Overload resolution is Postgres's algorithm**, over a real `pg_operator` /
  `pg_proc` table, with Postgres's three cast categories (implicit /
  assignment / explicit). This is what deletes `pg_operators.rs`: `~`, `@>`,
  `&&`, `#>>` stop being string rewrites and become operator lookups.

Deep semantics — NUMERIC beyond Decimal128, NaN ordering, empty-input
aggregates, 3VL in `NOT IN` — are specified in
[12-pg-type-fidelity.md](./12-pg-type-fidelity.md), which this section defers to.

## 3. `Expr`

Scoped from the **committed SQL surface**, not from a grep of what Basin names
today. [02](./02-logical-ir-surface.md) established that every production query
arrives via `SessionContext::sql(&str)`, so the reachable surface is open —
only 2 of DataFusion's 34 variants are genuinely dead.

```rust
pub enum Expr {
    Column(ColumnRef),
    Literal(Datum, PgType),
    Parameter(u16),                 // $1 — see note below
    Unary  { op: OpId, arg: Box<Expr> },
    Binary { op: OpId, lhs: Box<Expr>, rhs: Box<Expr> },
    Cast   { arg: Box<Expr>, to: PgType, kind: CastKind },
    Case   { operand: Option<Box<Expr>>, whens: Vec<(Expr, Expr)>, else_: Option<Box<Expr>> },
    Coalesce(Vec<Expr>),
    NullTest { arg: Box<Expr>, is_null: bool },
    BoolTest { arg: Box<Expr>, test: BoolTestKind },   // IS TRUE / IS NOT UNKNOWN …
    DistinctFrom { lhs: Box<Expr>, rhs: Box<Expr>, negated: bool },
    InList  { arg: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { arg: Box<Expr>, low: Box<Expr>, high: Box<Expr>, symmetric: bool, negated: bool },
    Like    { arg: Box<Expr>, pattern: Box<Expr>, case_insensitive: bool, negated: bool },
    ScalarFn { func: FuncId, args: Vec<Expr> },
    Aggregate { func: FuncId, args: Vec<Expr>, distinct: bool,
                filter: Option<Box<Expr>>, order_by: Vec<SortKey> },
    Window  { func: FuncId, args: Vec<Expr>, partition_by: Vec<Expr>,
              order_by: Vec<SortKey>, frame: WindowFrame },
    SetReturning { func: FuncId, args: Vec<Expr> },     // see §4
    Subquery(SubqueryKind, Box<LogicalPlan>),           // Scalar / Exists / In / Any / All
    ArrayLit(Vec<Expr>),
    RowLit(Vec<Expr>),
    Subscript { arg: Box<Expr>, indices: Vec<Subscript> },
    FieldSelect { arg: Box<Expr>, field: FieldId },     // composite / row types
    GroupingSetRef(u8),
}
```

Notes on three choices:

- **`Parameter` is retained** even though today's engine substitutes bind
  parameters at the sqlparser AST level and `prepared.rs:1996` asserts none
  survive. Correct extended-query support requires `Describe` to report
  inferred parameter types *before* values arrive, which is impossible if
  parameters are textually substituted. Keeping the node is what makes
  `ParameterDescription` honest.
- **`SetReturning` is an `Expr`, not only a plan node.** Postgres allows SRFs in
  the target list with LCM expansion semantics across multiple SRFs. This is
  the thing `jsonb_udf.rs:16` records as structurally impossible under
  DataFusion.
- **`OpId` / `FuncId` are catalog references**, not names or enum variants.
  They resolve through `pg_operator` / `pg_proc`, which is what makes
  user-defined operators and `CREATE EXTENSION`-registered functions work
  through the same path as builtins.

## 4. `LogicalPlan`

```rust
pub enum LogicalPlan {
    Scan { table: TableId, projection: Vec<ColId>,
           filters: Vec<Expr>, snapshot: SnapshotId },
    Values { rows: Vec<Vec<Expr>>, schema: Schema },
    Project  { input: Box<LogicalPlan>, exprs: Vec<(Expr, Alias)> },
    Filter   { input: Box<LogicalPlan>, predicate: Expr },
    Aggregate{ input: Box<LogicalPlan>, group: Vec<Expr>,
               aggs: Vec<Expr>, grouping_sets: Option<GroupingSets> },
    Sort     { input: Box<LogicalPlan>, keys: Vec<SortKey> },
    Limit    { input: Box<LogicalPlan>, skip: Option<Expr>,
               fetch: Option<Expr>, with_ties: bool },
    Join     { left: Box<LogicalPlan>, right: Box<LogicalPlan>,
               kind: JoinKind, on: Vec<Expr>, filter: Option<Expr> },
    LateralJoin { outer: Box<LogicalPlan>, inner: Box<LogicalPlan>, kind: JoinKind },
    SetOp    { left: Box<LogicalPlan>, right: Box<LogicalPlan>,
               op: SetOpKind, all: bool },
    Distinct { input: Box<LogicalPlan>, on: Option<Vec<Expr>> },
    Window   { input: Box<LogicalPlan>, windows: Vec<Expr> },
    ProjectSet { input: Box<LogicalPlan>, srfs: Vec<Expr> },   // SRF expansion
    Cte      { name: CteId, recursive: bool,
               body: Box<LogicalPlan>, input: Box<LogicalPlan> },
    // DML as a relation — the thing DataFusion cannot represent
    Insert { table: TableId, input: Box<LogicalPlan>,
             on_conflict: Option<OnConflict>, returning: Option<Vec<Expr>> },
    Update { table: TableId, set: Vec<(ColId, Expr)>,
             from: Option<Box<LogicalPlan>>, predicate: Option<Expr>,
             returning: Option<Vec<Expr>> },
    Delete { table: TableId, using: Option<Box<LogicalPlan>>,
             predicate: Option<Expr>, returning: Option<Vec<Expr>> },
}
```

Two structural points:

**DML is a relation.** `Insert`/`Update`/`Delete` carry `returning` and nest
under `Cte`, so `WITH x AS (INSERT … RETURNING …) SELECT … FROM x` is
expressible. `executor.rs:3128` and `:13038` record that DataFusion cannot do
this; it is not an extension bolted on later, it is why the enum is shaped this
way.

**`Scan` carries a `SnapshotId`.** Visibility is a plan property, not something
the storage layer infers from ambient state. This is the hook that makes real
`BEGIN`/`COMMIT` isolation levels and `SELECT … FOR UPDATE` implementable
later. It costs nothing now — a single constant snapshot — and is very
expensive to retrofit.

## 5. Lowering, and the death of the string rewriter

```
SQL text
  → pg_query::parse                      (already ours, ADR 0014)
  → PgNode walk                          (port pg_plan.rs's traversal)
  → name resolution + overload resolution against pg_catalog
  → LogicalPlan + Expr, fully typed
  → mandatory rewrites (RLS, CHECK, FK)  ← unbypassable, see §6
  → optimizer rules                      (see 05)
  → physical plan
```

`pg_operators.rs` (9,546 LOC) exists because DataFusion's SQL surface differs
from Postgres's, and it operates on **strings** — its own header concedes it
cannot handle dollar-quoted strings, comments, or quoted identifiers. In this
pipeline there is nothing for it to do: `~` is an operator lookup in
`pg_operator` returning a `FuncId`, resolved by argument types like any other.
The whole file is deleted, not ported.

**One contract that must survive.** `QueryShapeHash` tag bytes
(`query_shape.rs:336-441`) are a **persisted downstream join key**. The shape
hash must be computed over the new IR while producing byte-identical tags for
equivalent queries, or previously collected shape records are orphaned. This is
a hard compatibility requirement, not a nice-to-have.

**One capability that must exist early.** `rls.rs:466` and `lifecycle.rs:572`
obtain injected predicates by *planning a SQL probe* and extracting the
resulting `Filter`. The owned planner must therefore be able to plan a bare
Boolean fragment against a schema before RLS works at all.

## 6. Mandatory rewrites

RLS today needs a separate gate (`executor.rs:3182`) because the fast paths
skip the planner. That is a correctness hazard by construction: the security
check lives beside the execution path rather than above it.

In the owned engine there is one planner, and RLS/CHECK/FK rewrites run between
lowering and optimization, on **every** plan. There is no path to physical
execution that does not pass through them. This is the single strongest
correctness argument for the migration, and it is worth more than any benchmark
line.

## 7. Physical execution

- **Single-partition streaming first.** `target_partitions = 1` is already
  pinned by default (`session.rs:2705`); parallelism gets an extension point,
  not an implementation.
- **Pull-based `Stream` of `RecordBatch`**, so execution is naturally
  **suspendable** — which is what `DECLARE CURSOR` / `FETCH` needs, and what
  DataFusion's model makes awkward.
- **Cancellation at every batch boundary.** DataFusion's synchronous UDFs
  cannot be preempted (`executor.rs:12`), capping `statement_timeout`. An
  owned evaluator checks a cancellation token between batches.
- **Memory accounting is required; disk spill is not — yet.** A `FairSpillPool`
  is wired (`lib.rs:519-526`, 50% of RAM with a 256 MiB floor, tunable via
  `BASIN_QUERY_MEMORY_BYTES` / `_FRACTION`), but **no `DiskManager` is
  configured**, no test asserts a spill or `ResourcesExhausted`, results are
  fully materialized regardless, and the 30 s statement timeout dominates in
  practice. So today's actual behaviour under memory pressure is *fail clean*,
  not spill.

  What must be preserved is therefore the **bounded-memory guarantee** — one
  heavy aggregate must not OOM a shared multi-tenant node — which needs memory
  accounting in the aggregate and sort operators, not a disk-spill
  implementation. Deferring true spill saves ~1.4k LOC and matches current
  behaviour rather than aspiring past it. It is also a safety property the
  benchmark suite never exercises, so it needs dedicated tests either way.
- The five existing bypass modules (`fast_select`, `fast_aggregate`,
  `values_fast`, `index_probe`, `point_join`) become the **basis** of the
  physical layer, generalized — not a parallel path beside it.

## 8. Sequencing

The branch must compile and pass tests at every commit, so nothing here lands
as a big-bang cutover:

0. **Oracle first** (Phase 0, [07](./07-conformance-tests.md)). No engine code
   before it.
1. `basin-pgtype` standalone, with its own tests. Touches nothing.
2. `basin-plan` types + lowering, behind an env flag, **producing plans nobody
   executes** — validated by comparing against DataFusion's plan for the same
   SQL. This is the `BASIN_PG_QUERY` pattern ADR 0014 already established.
3. Re-host the 249 UDFs onto `basin-pgtype` signatures while DataFusion still
   executes them (they are largely arrow-kernel wrappers; the signature layer
   is what changes).
4. `basin-exec` scan + expression evaluation. `vortex_listing_format.rs` is
   deleted here ([06](./06-scan-and-storage.md)).
5. Stateful operators — join, aggregate, sort, window. Window is **greenfield**:
   Basin has zero window code today ([04](./04-function-gap.md)).
6. Optimizer, ordered by published-benchmark coverage ([05](./05-optimizer-rules.md)).
7. Cutover behind the flag; soak; then delete `datafusion = "53"`.

## 9. Open questions

- **Window frames** are the largest single unknown, and larger than first
  assumed: [03](./03-physical-operators.md) confirms **all three frame units are
  live and tested**, including `RANGE BETWEEN INTERVAL '5 minutes' PRECEDING`
  and `GROUPS BETWEEN 1 PRECEDING`, with no `#[ignore]`s in that suite. Zero
  existing Basin code, 11 documented functions, subtle semantics, and a test
  suite that already demands the hard cases.
- **`WITH RECURSIVE` is 100% DataFusion today** — no Basin-side implementation
  exists, yet it is exercised in three test suites. Another greenfield item,
  alongside window functions.
- **Join-order selection** — flagged unresolved in [05](./05-optimizer-rules.md);
  the 261× star join may depend on cost-based reordering that is not in the
  logical rule list.
- **NUMERIC beyond Decimal128.** Postgres NUMERIC is arbitrary-precision. Basin
  stores Decimal128. Where that breaks is [12](./12-pg-type-fidelity.md)'s
  problem, but it constrains `PgType` here.
