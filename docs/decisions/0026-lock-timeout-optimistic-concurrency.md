---
title: "ADR 0026 — lock_timeout / 55P03 under Basin's optimistic concurrency (no row-lock manager)"
nav_section: decisions
sidebar_position: 26
summary: "Basin uses optimistic, copy-on-write concurrency for row writes — conflicts surface at commit as SQLSTATE 40001 (serialization_failure), never by blocking on a row lock. We will NOT build a PostgreSQL-style pessimistic row-lock manager. Therefore `lock_timeout` (SQLSTATE 55P03, lock_not_available) governs only the locks that actually block in Basin: advisory locks (`pg_advisory_lock`) and table/DDL locks. Phase 5.28.B is rescoped accordingly: make advisory-lock acquisition genuinely block + honor `lock_timeout`, and document the optimistic-concurrency contract for row writes."
tags: [concurrency, locking, pgwire-compat, timeouts]
---

# 0026 — lock_timeout / 55P03 under optimistic concurrency

- **Status:** Accepted, 2026-05-22.
- **Tags:** concurrency, locking, pgwire-compat, timeouts
- **Driving issue:** Phase 5.28.B (`lock_timeout` GUC + 55P03). The timeout-trio
  harness slice `lock_timeout_fires_and_returns_55P03` was left `#[ignore]`'d
  with the note "needs row-lock tracking across sessions." This ADR decides
  what that actually means for Basin.
- **Relates to:** ADR 0023 (lease-based ownership), the 6.P0.A statement-timeout
  work, and 5.28.A/C (`statement_timeout`, `idle_in_transaction_session_timeout`,
  which DID land because they govern wall-clock time, not lock waits).

## Context

PostgreSQL's `lock_timeout` bounds how long a statement waits to **acquire** a
lock before aborting with SQLSTATE `55P03` (`lock_not_available`). It is
meaningful only in a system that **blocks** on lock acquisition — i.e.
pessimistic concurrency control, where `UPDATE` takes a row lock and a
concurrent `UPDATE` of the same row *waits*.

**Basin does not work that way.** Basin is a bucket-native, copy-on-write /
snapshot engine with **optimistic** concurrency for row writes (confirmed in
`crates/basin-engine/src/executor.rs` — "Basin is append-only /
optimistic-concurrency so row locking is [absent]" — and
`crates/basin-engine/src/dml_mutate.rs`, which commits a file swap with an
optimistic-conflict retry). Concurrent writers do not block each other; instead,
the loser of a commit race gets `BasinError::CommitConflict` →
SQLSTATE `40001` (`serialization_failure`), and the client retries the
transaction. There is **no point in the row-write path where a statement waits
to acquire a lock**, so `lock_timeout` has nothing to fire on for row DML.

The only operations that genuinely **block on lock acquisition** in Basin are:

1. **Advisory locks** — `pg_advisory_lock(key)` / `pg_advisory_xact_lock(key)`.
   These are application-level mutexes; by definition a second caller *waits*
   for the holder to release. Currently a no-op stub in Basin.
2. **Table / DDL locks** — e.g. an `ALTER TABLE` that must exclude concurrent
   access, or `LOCK TABLE`. (Largely implicit / minimal in Basin today.)
3. **Lease/virtualxid locks** (ADR 0023, 6.X) — internal ownership, not
   user-statement-driven.

## Decision

1. **Do NOT build a PostgreSQL-style pessimistic row-lock manager.** It would
   contradict Basin's entire optimistic / copy-on-write concurrency model and
   the snapshot-isolation guarantees it already provides. Row-write contention
   is correctly expressed as `40001` at commit, not `55P03` on acquire.

2. **`lock_timeout` (55P03) governs only the locks that actually block:**
   advisory locks and table/DDL locks. The GUC + duration parsing already
   landed (5.28.B); enforcement is wired into those acquisition paths, not into
   the row-write path.

3. **Make `pg_advisory_lock` a real, blocking lock manager** (promote it from
   the current stub). A second session requesting a held advisory key waits;
   if the wait exceeds the session's `lock_timeout`, the acquisition aborts with
   `55P03`. Reuse the bounded-wait primitive from
   `crates/basin-shard/src/lock_wait.rs` (already built in 5.28.B) and the
   `LockRegistry` (5.23.D). Advisory locks are a standard PostgreSQL feature
   that real apps use for distributed mutexes / leader election, so this is
   worthwhile compat surface, and it is the *only* honest way to exercise
   `lock_timeout` → `55P03` in Basin.

4. **Document the row-write contract:** Basin row writes are optimistic.
   Concurrent conflicting writes do not block; the loser receives `40001`
   (`serialization_failure`) at commit and should retry. `lock_timeout` does
   not (and cannot) apply to row writes — this is a deliberate, correct
   divergence from PostgreSQL's pessimistic model, not a missing feature.

5. **Rescope the 5.28.B harness slice:** rewrite
   `lock_timeout_fires_and_returns_55P03` to exercise **advisory-lock**
   contention (session A holds `pg_advisory_lock(k)`; session B requests the
   same key with a short `lock_timeout`; B aborts with `55P03`), not row-lock
   contention. Add a companion assertion that two concurrent conflicting row
   `UPDATE`s produce `40001` (not `55P03`).

## Consequences

**Positive**
- No large, model-violating row-lock subsystem. The concurrency story stays
  coherent: optimistic for rows, pessimistic only where PostgreSQL semantics
  genuinely require blocking (advisory / table locks).
- `pg_advisory_lock` becomes functional — real compat win for apps using
  advisory locks as mutexes, and it gives `lock_timeout` a correct home.
- `lock_timeout` / `55P03` behavior matches PostgreSQL **for the operations
  PostgreSQL itself blocks on**; row contention matches PostgreSQL's
  `serialization_failure` retry contract (which Basin already implements).

**Negative / accepted trade-offs**
- Apps that rely on PostgreSQL `SELECT … FOR UPDATE` *blocking* semantics will
  instead see optimistic `40001` retries. Documented; consistent with Basin's
  storage model. `FOR UPDATE` is accepted-as-noop / advisory at most.
- Promoting advisory locks from stub to a real blocking manager is medium
  effort (wait queue per key, fairness, release-on-session-end, `lock_timeout`
  integration). Scoped to advisory + table/DDL acquisition only.

## What we do (implementation plan, Phase 5.28.B)

1. Replace the `pg_advisory_lock` / `pg_advisory_xact_lock` /
   `pg_try_advisory_lock` / `pg_advisory_unlock` stubs with a real per-(project)
   advisory-lock manager: a keyed wait queue; `_xact_` variants release on
   txn end; session-scoped variants release on session end (already partly
   handled by `AdvisorySessionLocks::release_all_on_session_end`).
2. Acquisition honors `lock_timeout`: bounded wait via `lock_wait.rs`; on
   expiry return `BasinError::LockNotAvailable` → `55P03` (mapping already in
   `basin-router/src/error.rs`).
3. Surface held advisory locks in `pg_locks` (5.23.D `LockRegistry`) so
   observability is consistent.
4. Rewrite the 5.28.B harness slice to advisory-lock contention + add the
   `40001`-on-row-conflict companion assertion. Un-ignore.
5. Add a short "Concurrency model" note to `CAPABILITIES.md` /
   `docs/` stating: optimistic row writes (40001 retry), `lock_timeout`/55P03
   for advisory + DDL locks, no pessimistic row locking.

## Cross-references

- ADR 0023 — lease-based partition ownership (the other place locks/leases live).
- `crates/basin-shard/src/lock_wait.rs` (5.28.B bounded-wait primitive),
  `crates/basin-shard/src/lock_registry.rs` (5.23.D), `crates/basin-engine/src/
  advisory_lock.rs`, `crates/basin-router/src/error.rs` (55P03 / 40001 mapping).
- `decisions.md` 2026-05-22 entries for the 5.28 timeout work.
