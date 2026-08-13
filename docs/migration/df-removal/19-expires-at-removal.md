---
title: "DF removal — what stops being safe when DataFusion goes"
nav_section: migration
sidebar_position: 19
summary: "A running list of decisions that are correct only because there is still something to fall back to. Each one becomes a real defect the moment the Cargo.toml line comes out, and none of them will announce itself."
tags: [migration, datafusion, correctness, blockers]
---

# 19 — What stops being safe when DataFusion goes

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [18](./18-removal-surface.md) measures how much code has to change
before step 5. This document tracks something different and easier to lose: the
decisions that are **defensible today and defective the moment step 5 lands**.

The owned engine is a *bridge*. When it cannot serve a query it declines, and
the statement runs on DataFusion instead, returning the right answer. That
fallback is load-bearing in a way that is easy to forget, because it makes a
whole class of gap invisible: a shape the owned engine gets wrong still produces
a correct result for the user.

**Deleting DataFusion deletes the safety net, not just the dependency.** Every
item below is a place where "falls back" is currently doing the work that
"correct" will have to do afterwards. None of them fails loudly at removal time;
each becomes a wrong answer or a hard error on ordinary SQL.

This list is maintained deliberately. Adding to it is the correct response to
accepting a trade-off, and each entry should be removed only by fixing it —
not by deciding it is unlikely.

## The list

### 1. Thirty `pg_proc` rows with no implementation behind them

`basin-pgtype`'s function table now carries the math family — `sqrt`, `cbrt`,
`ln`, `log`, `exp`, `power`, `trunc`, `degrees`, `radians`, `pi`, and the
trigonometric functions — with real OIDs taken from a live server. **`eval.rs`
implements none of them.**

Today: lowering resolves `sqrt(2)`, the executor's dispatch hits its catch-all,
returns `ExecError::Internal`, and the statement falls back. The user gets the
right answer.

After removal: `SELECT sqrt(2)` is a hard error.

This is the same finding as [17](./17-udf-rehosting.md)'s **ABSENT** category,
seen from the other side. The rows are groundwork, and they are a checklist.

### 2. `CASE` evaluates every branch

`eval_branches_unified` evaluates all THEN/ELSE branches over the whole batch
and then selects. Postgres does not evaluate a branch whose condition is false.

    CASE WHEN x <> 0 THEN 1/x ELSE 0 END     over x = [0, 2]

Postgres returns `{0, 0}`. Basin raises `DivisionByZero`.

Today: the error routes to `Fallback::Exec` and the statement re-runs on
DataFusion. The cost is a wasted attempt.

After removal: the query fails. This is guard-clause SQL — the single most
common reason anyone writes a `CASE` in a `WHERE`-adjacent position — so it is
not an exotic shape.

Fixing it needs per-branch row masking through `eval`, which the current
kernel-per-node evaluator does not support.

### 3. Every remaining fallback, by definition

The coverage probe stands at 52 of 75 representative shapes served. The other
23 are not "slower" today; they are *correct*, because DataFusion runs them.

The fallback-reason histogram is the tracking instrument, and its `unsupported`
bucket is the honest to-do list. But the reason buckets say nothing about
*volume*: a shape that falls back on every query in a workload matters more than
one that appears once, and the probe weights them equally.

Step 5 cannot happen while this number is above zero, and reaching zero is the
whole of step 3.

## What is NOT on this list, and why

**Performance regressions.** The owned path lacks the index-assisted file
pruning the DataFusion path accumulated — secondary B-tree, GIN, R-tree,
trigram. Those change how much is read, never which rows come back. A slower
correct answer is a different kind of problem and belongs in
[14](./14-performance-parity.md).

**Documented catalog gaps.** `pg_enum` and `pg_sequence` return zero rows;
`pg_index` omits `indcollation` and `indclass`; `comments()` returns empty.
These are already wrong-by-omission today and DataFusion is not hiding them —
removing it changes nothing about them. They belong in
[11](./11-pg-catalog-fidelity.md).

The distinction that puts an item here is narrow and worth stating exactly:
**the behaviour is correct today only because a fallback exists, and nothing
will fail when that fallback is removed except the user's query.**

## Using this document

Before step 5, every entry must be closed or consciously accepted with its
consequence written down. The check is not "does the branch compile without
DataFusion" — it will. The check is this list, plus a coverage probe at zero
fallbacks, plus a green differential gate.

A green build after deleting the dependency proves the code no longer *names*
DataFusion. It proves nothing about whether the engine can answer the queries
DataFusion was answering.
