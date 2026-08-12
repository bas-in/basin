---
title: Unlocked Capabilities
nav_section: migration
sidebar_position: 13
summary: The opportunity register for removing DataFusion — capabilities DataFusion structurally prevented, with evidence, user value, effort, and priority. The counterpart to the risk register in 10-risk-and-phases.md.
tags:
  - migration
  - datafusion
  - query-engine
  - roadmap
  - pg-compat
---

# 13 — Unlocked capabilities (opportunity register)

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. Where [10-risk-and-phases.md](./10-risk-and-phases.md) records what
the migration can break, this document records what it makes possible.

STATUS: IN PROGRESS — being filled incrementally. Claims not yet checked against
code are marked **UNVERIFIED**.

## 1. Purpose and method

The register is mined from the codebase, not from a wishlist. A capability
qualifies only when a source comment, an ADR, or an observable degradation
identifies DataFusion as the thing in the way. Every row carries a `file:line`.

Two disciplines are applied deliberately:

1. **Blocker attribution is checked, not assumed.** Several statements Basin
   rejects with SQLSTATE `0A000` are blocked by storage, WAL, or MVCC — not by
   DataFusion. Removing DataFusion does not unlock those, and §12 says so
   explicitly. A long register is worth less than an honest one.
2. **"Blocked" is distinguished from "degraded" and from "worked around."** Some
   features exist today via string rewriting or executor-level orchestration and
   the unlock is *correct semantics*, not first availability. Those rows say so.

Search method: workspace grep for `datafusion (cannot|can't|does not|lacks|
upstream|limitation|bug)` and near variants across `crates/**/*.rs` (78 hits),
plus a full read of ADR 0030, ADR 0014, ADR 0020, ADR 0026 and
[08-ir-design.md](./08-ir-design.md).

## 2. The register (summary table)

| # | Capability | Blocked by DataFusion? | Priority |
|---|---|---|---|
| [11](#11-retirement-of-pg_operatorsrs-string-rewriting) | Retire 9,546 lines of string rewriting | Yes — it exists only because DF's SQL surface differs | 1 |
| [7](#7-constraints-rls-and-triggers-as-mandatory-plan-rewrites) | RLS/CHECK/FK unbypassable | Yes — fast paths skip the planner | 2 |
| [3](#3-set-returning-functions-in-the-target-list) | SRFs in the target list | Yes — structurally impossible | 3 |
| [4](#4-data-modifying-ctes) | Data-modifying CTEs | Yes — DML is not a relation | 4 |
| [10](#10-expression-and-predicate-capabilities) | Correlated LATERAL, `ANY(ARRAY)`, `WITH TIES`, recursive CTEs | Yes — upstream limits | 5 |
| [9](#9-explain--explain-analyze-in-postgress-vocabulary) | Postgres-shaped `EXPLAIN` | Partly | 6 |
| [6](#6-mvcc-snapshots-as-a-first-class-plan-concern) | Snapshot-aware scans | No — enables later work | 7 |
| [8](#8-a-cost-model-denominated-in-s3-gets) | S3-GET cost model | Partly | 8 |
| [5](#5-real-transactions-and-cursors) | Cursors | Half | 9 |
| [12](#12-honest-assessment-what-datafusion-was-not-blocking) | The `0A000` statement list | **Mostly no** — see §12 | — |

## 3. Set-returning functions in the target list

**Blocked, wholly.** `crates/basin-engine/src/jsonb_udf.rs:15-19`:

> Set-returning functions (SRFs) — `jsonb_object_keys`, `jsonb_each`,
> `jsonb_array_elements`, etc. — cannot be true SRFs inside DataFusion's scalar
> UDF framework; they are implemented as best-effort stubs that return a single
> scalar (the first key / element / the whole JSON text representation).

This is the most severe class of defect in the register, because the failure mode
is a **wrong answer, not an error**: `SELECT jsonb_array_elements(tags) FROM t`
returns one row per input row with the first element, where Postgres returns one
row per array element.

The owned IR is shaped for this: `Expr::SetReturning` and
`LogicalPlan::ProjectSet` ([08](./08-ir-design.md) §3, §4), with the design note
naming `jsonb_udf.rs:16` as the reason `SetReturning` is an `Expr` rather than
only a plan node.

More detail pending verification of the current `generate_series` / `unnest`
behaviour. **UNVERIFIED** beyond the jsonb family.

## 4. Data-modifying CTEs

Not merely absent — **emulated**, and the emulation is visible in the executor.
`executor.rs:3126-3134` intercepts `WITH x AS (INSERT … RETURNING …) SELECT …`
because "DataFusion 53 cannot plan DML statements as relations", and orchestrates
it by running each DML CTE in declaration order, capturing `RETURNING`, and
registering the batch as a `MemTable` for the outer SELECT.
`executor.rs:13033-13072` does the same for `WITH RECURSIVE … INSERT …`.

So the unlock is semantic fidelity, not first availability. Details of where the
emulation diverges from Postgres are pending. **UNVERIFIED**.

## 5. Real transactions and cursors

Pending. Must first establish which half is DataFusion's fault: `DECLARE CURSOR`
needs suspendable execution (an executor concern, [08](./08-ir-design.md) §7),
while `BEGIN`/`COMMIT` isolation needs snapshots and WAL (ADR 0020, ADR 0026).

## 6. MVCC snapshots as a first-class plan concern

`LogicalPlan::Scan` carries a `SnapshotId` ([08](./08-ir-design.md) §4): "Visibility
is a plan property, not something the storage layer infers from ambient state …
the hook that makes real `BEGIN`/`COMMIT` isolation levels and `SELECT … FOR
UPDATE` implementable later. It costs nothing now … and is very expensive to
retrofit."

## 7. Constraints, RLS, and triggers as mandatory plan rewrites

`executor.rs:3182-3187` records the hazard directly: the point-query fast path
"bypasses DataFusion's logical planner entirely, which is where we inject
row-level predicates," so RLS-enabled tables must be *routed away* from the fast
path by a separate gate. A security rewrite that execution paths can route around
is the wrong shape for a security rewrite ([08](./08-ir-design.md) §6).

## 8. A cost model denominated in S3 GETs

ADR 0030 Consequences: "Cost modelling can use Basin's real unit — an S3 GET —
instead of DataFusion's local-page assumption." Verification pending.

## 9. EXPLAIN / EXPLAIN ANALYZE in Postgres's vocabulary

Pending. `executor.rs:11454` notes DataFusion does not surface
`files_opened` / `bytes_decoded` / `cache_hits`.

## 10. Expression and predicate capabilities

The long tail, all evidenced:

| Capability | Evidence | Today's behaviour |
|---|---|---|
| Correlated `LATERAL` | `pg_operators.rs:2168-2172`, `:2246`, `:2483` | Left unrewritten; DF 53 has "no physical plan support for correlated lateral subqueries (`OuterReferenceColumn` paths fail at execution)". Fails at execution. |
| `= ANY(ARRAY[…])` | `pg_operators.rs:1591-1598` | "DataFusion cannot plan `= ANY(ARRAY[...])` — its coercion path only works with subqueries." Rewritten textually to `IN (…)`. |
| Multi-column recursive CTEs | `pg_operators.rs:5069-5086`, `:8471` | DF 53 `optimize_projections` bug; worked around by wrapping the first anchor expression in a scalar subquery to *deliberately inhibit* an optimizer rule. |
| `FETCH FIRST … WITH TIES` | `select_advanced.rs:455` | "`WITH TIES` is treated as `ONLY` (DataFusion doesn't support TIES)." Silently returns fewer rows than Postgres. |
| OR-correlated subqueries | `pg_operators.rs:2473-2476` | "cannot be safely decorrelated without full subquery support; DataFusion's physical planner does not execute `OuterReferenceColumn`". |
| Preemptible UDFs / `statement_timeout` | `executor.rs:11-13`, `udf.rs:1466` | "DataFusion UDFs are synchronous and cannot be preempted"; `pg_sleep` needs a cooperative thread-local deadline hack. |

More rows pending.

## 11. Retirement of pg_operators.rs string rewriting

`pg_operators.rs` is 9,546 lines whose own header (lines 1-53) states the approach
and its limits: rewriters are "pure string-manipulation functions — no AST", and
are "best-effort: they understand parenthesised sub-expressions and single-quoted
string literals but do NOT handle dollar-quoted strings, comments, or identifier
quoting."

That is a correctness hazard sitting *in front of* the planner, and it is
structural: a string rewriter cannot be made safe. [08](./08-ir-design.md) §5:
"The whole file is deleted, not ported."

## 12. Honest assessment: what DataFusion was NOT blocking

The register above is only worth something if it does not over-claim. ADR 0014
lists eleven statement kinds Basin rejects with SQLSTATE `0A000`, and it would
be easy to present all of them as unlocked by this migration. That would be
false.

| Statement | Real blocker | Is DataFusion the blocker? |
|---|---|---|
| `BEGIN` / `COMMIT` / `ROLLBACK` | Transaction manager + MVCC visibility. ADR 0020's WAL markers are the groundwork; the snapshot plumbing is [§6](#6-mvcc-snapshots-as-a-first-class-plan-concern) | **No.** Storage and WAL |
| `LOCK` | A row/table lock manager. ADR 0026 **deliberately chose** optimistic CAS with 40001 and *no* lock manager | **No.** A decided architecture, not a gap |
| `DECLARE CURSOR` / `FETCH` | Suspendable execution across statements, plus transaction scope | **Partly.** The owned executor's pull-based stream makes suspension natural where DataFusion's model fights it — but the transaction half is still storage |
| `PREPARE` | A named-statement registry and plan cache | **No.** `prepared.rs` exists; this is wiring |
| `LISTEN` / `NOTIFY` | Session-scoped async delivery. `notify_registry.rs` and `basin-realtime` already exist | **No.** Wiring |
| `VACUUM` / `CLUSTER` | Nothing to vacuum: data files are immutable and space is reclaimed by the compactor. These are arguably permanent no-ops that should succeed silently rather than error | **No.** Storage model |
| `ANALYZE` | Statistics are computed at write time into catalog `column_stats`. `ANALYZE` would be a recompute trigger | **No.** Already-solved differently |
| `CREATE EXTENSION` | Catalog registration. See [11](./11-pg-catalog-fidelity.md) §6 — a pure catalog operation, no dlopen | **No.** ADR 0002 + catalog |
| `CREATE TRIGGER` | ADR 0012 **deliberately chose** declarative reactors and lifecycle rules over PL/pgSQL triggers | **No.** A decided architecture |

**So of eleven rejected statement kinds, DataFusion is a genuine blocker for
roughly one, and only half of that one.** Everything else is storage, WAL, MVCC,
wiring, or a decision already made on its own merits.

This matters for how the migration is justified. The case for removing
DataFusion rests on [§3](#3-set-returning-functions-in-the-target-list) through
[§7](#7-constraints-rls-and-triggers-as-mandatory-plan-rewrites) and
[§10](#10-expression-and-predicate-capabilities) — SRFs, data-modifying CTEs,
unbypassable security rewrites, correlated LATERAL, `WITH TIES`, the
string-rewriter retirement. It does **not** rest on the `0A000` list, and
presenting that list as unlocked work would be talking the project into
something on a false premise.

## 13. Priority ranking

By user-visible value per unit of work, given the migration is happening anyway:

1. **Retire the `pg_operators.rs` string rewriting** ([§11](#11-retirement-of-pg_operatorsrs-string-rewriting)). Not a feature, but 9,546 lines of acknowledged fragility deleted, and it is a precondition for most of the rest. `basin-pgtype`'s operator table is the replacement.
2. **RLS / CHECK / FK as unbypassable plan rewrites** ([§7](#7-constraints-rls-and-triggers-as-mandatory-plan-rewrites)). The only item on this list that fixes a *correctness hazard* rather than adding capability — `executor.rs:3182` shows RLS bolted beside the fast paths rather than above them. Highest severity even though it is invisible when it works.
3. **Set-returning functions in the target list** ([§3](#3-set-returning-functions-in-the-target-list)). `generate_series` and `unnest` are everyday Postgres and simply unavailable today.
4. **Data-modifying CTEs** ([§4](#4-data-modifying-ctes)). `WITH x AS (INSERT … RETURNING …)` is a common idiom; already expressible in the IR.
5. **Correlated `LATERAL`, `= ANY(ARRAY[…])`, `WITH TIES`, multi-column recursive CTEs** ([§10](#10-expression-and-predicate-capabilities)). A long tail of individually small fixes that currently fail at execution or silently return wrong row counts. `WITH TIES` returning fewer rows than Postgres without saying so is the worst of these.
6. **`EXPLAIN` in Postgres's vocabulary** ([§9](#9-explain--explain-analyze-in-postgress-vocabulary)). Cheap once the IR exists, and it is how everything else gets debugged.
7. **MVCC snapshots as a plan concern** ([§6](#6-mvcc-snapshots-as-a-first-class-plan-concern)). The field is already carried on `LogicalPlan::Scan`; the value it enables is gated on the transaction manager, which is not this migration.
8. **A cost model in S3 GETs** ([§8](#8-a-cost-model-denominated-in-s3-gets)). Real, but it needs the physical layer and a statistics story first.
9. **Cursors** ([§5](#5-real-transactions-and-cursors)). Half-blocked by transactions; the executor half comes free.
