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

TO BE FILLED — written last, once each section below is settled.

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

Pending — one row per statement kind in ADR 0014's `0A000` rejection list
(`LISTEN`, `NOTIFY`, `PREPARE`, `DECLARE CURSOR`, `LOCK`, `VACUUM`, `CLUSTER`,
`ANALYZE`, `CREATE EXTENSION`, `CREATE TRIGGER`, `BEGIN`/`COMMIT`/`ROLLBACK`),
each attributed to its real blocker.

## 13. Priority ranking

Pending.
