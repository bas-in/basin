---
title: "ADR 0011 — Cross-shard 2PC"
nav_section: decisions
sidebar_position: 11
summary: "ADR 0011: Cross-shard 2PC. See body for status, context, decision, consequences."
---

# 0011 — Cross-shard 2PC: structural rejection until customer demand

- **Status:** Accepted, deferred. Trigger to revisit below.
- **Date:** 2026-05-07
- **Tags:** scope, sharding, transactions, distributed, deferred

## Context

Basin shards by project. The router (`crates/basin-router/src/sharding.rs`)
hashes `ProjectId` to one shard endpoint and proxies the entire connection
to that owner via `remote_shard.rs`. There is no "project lives on two
shards" case in the substrate — a project is one shard's project, full
stop. Whale pinning (Phase 5.5) overrides the hash for individual
projects, but each pinned project still resolves to exactly one endpoint.

Two layers above are missing today:

- **No transactions yet, even single-shard.** [`TASK.md`](../../TASK.md)
  Phase 4: "Single-shard transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) —
  deferred." The wedge customer's audit-log workload doesn't need them
  for correctness (every `INSERT` is its own committed Iceberg snapshot;
  `UPDATE`/`DELETE` is single-statement copy-on-write).
- **No cross-shard writes.** Multi-shard fan-out is read-only — see
  [`CAPABILITIES.md`](../../CAPABILITIES.md) "Cross-shard query merging"
  (router → shard-owner protocol shipped, cross-shard JOIN deferred).

So "cross-shard 2PC" is **two missing layers, not one**: we'd need to
ship single-shard transactions first, then layer a coordinator on top.
A real 2PC implementation (coordinator process, prepare-vote-commit
protocol, recovery log, participant timeout machinery, in-doubt
transaction resolution) is a multi-month project for a team that has
done it before — and it permanently raises the operational complexity
floor of the cluster.

The wedge customer (multi-project SaaS / audit-heavy fintech / agent
platforms) writes only within a single project 99.9%+ of the time.
Per-project prefix isolation makes cross-shard writes structurally rare:
a SQL statement that mutates rows on two shards is, by Basin's
construction, a SQL statement that names two projects. The pgwire
session is bound to one project via `ProjectSession`; the SQL surface has
no qualifier syntax that names another project; the engine's
`single_part_name` helper rejects schema-qualified identifiers as
`InvalidIdent` already.

The honest question this ADR answers: *given that cross-shard atomic
writes are vanishingly rare in the wedge, and 2PC is a multi-month
commitment, what does Basin do today and what would change our mind?*

## The decision

**Option C: avoid the problem.** Basin does not implement cross-shard
two-phase commit. Cross-shard mutation shapes are rejected at planning
time with SQLSTATE `0A000` (`feature_not_supported`) and a Basin-specific
message that points the user at this ADR. Single-shard transactions
arrive on their own, separate timeline (Phase 4 deferred line) and do
not depend on this work.

We considered:

- **Option A — Standard 2PC.** A coordinator process logging prepares +
  commits to durable storage, participants voting and following, recovery
  on coordinator crash via the log. Real, well-understood, and the
  textbook answer. The cost is 3–6 months of focused engineering for the
  v0.1 implementation, plus the permanent operational surface: in-doubt
  transactions become an oncall concern, the coordinator is a new
  failure domain, and you pay the prepare round-trip on *every* write
  whose plan touches more than one shard (which the planner often
  cannot tell ahead of time without per-row routing). For a wedge
  customer who writes within one project 99.9% of the time, this is
  paying a tax on every write to insure against an event that doesn't
  happen.
- **Option B — Saga / TCC.** No global lock; each step is a local
  transaction with a documented compensating action. Eventually
  consistent under failure. The library a developer uses to *write* the
  compensations is the real product (every operation needs a tested
  inverse), and that library has to live somewhere — either we ship
  it (multi-month) or we expect every customer to write their own
  (correctness disaster). The pattern also doesn't give the customer
  what they think they want when they ask for "cross-shard transactions"
  — they want serializable, not eventual. We'd ship the wrong thing.
- **Option C — Reject.** Cross-shard mutations return a clear error.
  Customers whose use case truly needs them either model around it
  (the common case — see "What gets rejected and what to do instead"
  below) or wait until a paying customer triggers Option A. The only
  cost is the rejection plumbing itself (~50 lines), and we keep
  full optionality on which of A or B we ship later.

The recommendation argues from the wedge's actual shape, not from
textbook 2PC abstractness. **For Basin's wedge customer — multi-project
SaaS where each project's data is fully self-contained — C is the right
answer today.** The ADR locks C and documents what would flip it.

## What gets rejected today, and what to do instead

The engine rejects the following shapes with SQLSTATE `0A000`,
message `cross-shard mutations are not supported in v0.1; see ADR 0011`:

- **Multi-table DML referencing more than one project's table by name.**
  Today this is structurally impossible to express because the SQL
  surface has no `project.table` qualifier and `ProjectSession` is
  per-project. The rejection is defense-in-depth for the day a future
  feature (cross-project materialized views, an admin SQL surface) adds
  qualified naming.
- **`INSERT INTO a SELECT … FROM b` where `a` and `b` resolve to
  different shards.** Within one project (the only case the engine can
  currently express) this is a single-shard write — the engine
  already accepts it on the `single_part_name` path. The rejection
  fires only if a future planner extension routes the SELECT to a
  different shard owner.
- **`WITH` chains containing more than one DML clause whose targets
  resolve to different shards** (e.g. `WITH x AS (UPDATE t1 …),
  y AS (UPDATE t2 …) SELECT …` where `t1` and `t2` live on different
  endpoints).
- **Future cross-project `JOIN` with `INSERT INTO … SELECT`.** Same
  rule: any plan that fans a write to more than one shard is rejected.

The rejection lives at planning time inside the engine, not at the
router, because the router cannot tell mutation shape from connection
metadata alone — it forwards bytes to the shard owner that owns the
session's project, and the shard owner is the one with the parsed AST.

**What to do instead, today:**

- Run multi-project reporting against the analytical engine
  (`basin-analytical` / DuckDB on Iceberg) which reads the shared
  storage substrate. Reads do *not* span shards in the writer sense —
  the analytical pool reads Parquet directly.
- For cross-project aggregation that updates a derived table, write a
  job (using `basin-cron`, ADR 0002 substitute) that runs per-project,
  writes per-project, and produces per-project materializations.
  `basin-cv` (continuous-aggregate equivalent) already does this.
- For a true cross-project rollup table, model it as its own project
  whose owner is the platform: a single-shard write target the platform
  populates from per-project feeds. Crosses no shards on the write path.

## Recovery semantics

There is no coordinator to crash, no participant to wedge, no in-doubt
transaction to resolve. The rejection is a synchronous parse/plan-time
error. The connection stays usable; the next statement on the same
session executes normally. No state is left behind on any shard owner.

If the engine's planner ever produces a multi-shard write plan in
error, the rejection fires *before* any shard sees a mutation — the
gate is at plan acceptance, before dispatch. There is no half-applied
state to clean up.

The day Option A or B ships, recovery semantics will be the load-bearing
section of the successor ADR. We're explicit about not carrying that
debt today.

## Testing strategy (when an implementation lands)

Whichever of A or B Basin eventually ships, **v0.1 of the implementation
must include a chaos test, not just a happy-path test.** The bar is:

- **Coordinator kill mid-prepare** (Option A only). With a multi-shard
  write in flight, the test sends `kill -9` to the coordinator process
  after every participant has voted YES but before the coordinator has
  written its commit record. On restart, recovery must drive the
  transaction to a definite outcome (commit *or* abort, not "in
  doubt"). The test asserts that no participant ends up applied while
  another is rolled back. Run 1000 iterations across kill points; flake
  rate must be zero.
- **Participant timeout mid-prepare.** With the coordinator alive,
  pause one participant via SIGSTOP after it has voted YES. The
  coordinator must follow its timeout policy (presumed-abort or
  presumed-commit, documented) and the resumed participant on SIGCONT
  must reconcile to the same decision. Same zero-flake bar.
- **Network partition splitting coordinator from one participant.**
  Use `iptables -A OUTPUT -d <participant> -j DROP` mid-prepare. The
  coordinator times out, makes a decision, and on heal the participant
  must converge. Asserts no split-brain — both sides see the same
  outcome.
- **Compensating-action failure** (Option B only). A saga step's
  compensation itself fails (network error on the compensating call).
  The saga library must surface a poisoned-saga signal that the
  operator can resolve out of band; the test asserts the partial
  state is *visible* (not silently swept under the rug) and that
  retrying the compensation eventually succeeds.

Happy-path tests (two shards, write to both, both commit) are **not
sufficient to call 2PC shipped**. The chaos tests above are the bar.

We do not write the chaos tests today — there is nothing to chaos-test
— but the bar is recorded here so the v0.1 PR cannot ship without
them.

## What this does NOT commit us to

- **Serializable cross-region transactions.** Out of scope per
  [ADR 0001](./0001-single-region-only.md). Even when 2PC ships, it
  ships within a single region.
- **Distributed deadlock detection.** A cross-shard deadlock detector
  is a third missing layer (after single-shard transactions and
  cross-shard 2PC). When 2PC lands, deadlocks are handled by timeout +
  abort, not by a cycle-detection graph.
- **A Saga compensating-action library.** If the future ADR picks B
  over A, that ADR owns the library shape. We don't pre-build it.
- **Cross-shard `JOIN` in the *write* sense** (e.g. an `UPDATE … FROM`
  that pulls rows from another shard). Read-side cross-shard fan-out
  is a separate feature (CAPABILITIES.md "Cross-shard query merging");
  write-side cross-shard fan-out is exactly what this ADR rejects.
- **Catalog-level 2PC for DDL across shards.** DDL is per-project
  per-shard already; there is no DDL operation today that names two
  shards.

## Architectural compatibility

Picking C does not preclude later picking A or B. The pieces that
would need to exist for either:

- A `CrossShardCoordinator` trait — does not exist today; will be
  added in the implementing PR. We deliberately do *not* introduce a
  stub `unimplemented!()` trait now: an empty trait with no callers
  is decoration, and the future PR will know better what shape it
  needs than we can guess today. (Compare: `basin-pool` in
  [ADR 0007](./0007-connection-pooling.md) — we documented the trait
  shape there because the call site exists; here neither call site
  nor coordinator exists.)
- The `Session::execute` path on the router-side `RemoteShardSession`
  forwards opaque SQL today. A future 2PC path needs a structured
  `prepare(txn_id) -> vote` / `commit(txn_id)` / `abort(txn_id)` RPC
  surface in `basin-router`. The current `tokio_postgres::Client`
  forwarder is too coarse-grained — but it's also a thin layer that
  can be replaced wholesale without rippling into `basin-engine` or
  storage. Fine to defer.
- The catalog (`basin-catalog`) already takes optimistic-concurrency
  commits keyed on snapshot id. A future 2PC participant on shard
  owner *N* would prepare by reserving a snapshot bump and commit by
  flipping it. The existing `append_data_files` API is close to the
  shape we'd need.

The single concrete thing we should *not* do meanwhile: bake
single-shard assumptions into the SQL surface in a way that would
need a parser rewrite to support cross-shard later. The current shape
— planner sees the parsed AST before dispatch — is already the right
hook point.

## Trigger to reconsider

We write a successor ADR (likely 0012) and start the implementation
when **one** of:

1. A wedge customer signs ≥ $50k ARR contingent on cross-project
   reporting that *requires strong consistency at write time* (not
   eventual; not bounded-staleness — actual serializability across
   projects in a single transaction). The contract terms must include
   a delivery window of ≥ 4 months after signing.
2. A regulator or compliance regime mandates cross-shard atomicity
   for an audit-log requirement that genuinely cannot be served by
   per-project audit logs (rare; most regulators care about per-entity
   integrity, which is per-project by construction).
3. We onboard a customer whose project-level partitioning genuinely
   does not work — their data model is irreducibly cross-project in
   shape (e.g. a shared marketplace ledger), and the platform-project
   workaround in "What to do instead" above is unacceptable for
   their auditors.

A single prospect at smaller value, or "we'd love it someday," or "our
ORM emits cross-table updates we want to land atomically *within* a
project" (that's single-shard), is **not** the trigger. Log them in the
lost-deal tracker but do not start the work.

When the trigger fires, the successor ADR picks A or B explicitly. Our
prior, recorded here for the future ADR's author: **A unless the
customer's own data model is asynchronous-by-shape** (e.g. a
long-running approval workflow that already has compensation logic
baked in). 2PC's reputation is "operationally heavy"; the actual
operational cost is not high once the chaos tests above pass — it's
the *implementation* that's hard, not the running.

## Alternatives considered

- **Build A defensively.** Rejected: 3–6 months of engineering on a
  feature whose primary use case the wedge customer doesn't have. The
  brief warns explicitly against "rebuild Cockroach/Aurora" drift; this
  is the headline example.
- **Build B because it sounds lighter.** Rejected: B is *not* lighter,
  it just shifts the cost from a coordinator to a compensating-action
  library, and it ships the wrong semantics (eventual, when the
  customer asking is asking *because* they want serializable).
- **Implement single-shard transactions first and revisit.**
  Reasonable. Single-shard transactions are a Phase 4 deferred item on
  their own merits. They do not need to land before this ADR — and
  this ADR's decision (reject cross-shard mutations) is the same in
  either order.
- **Trait-shape the coordinator now, leave methods `unimplemented!()`.**
  Considered; rejected. An empty trait with no caller is decoration. We
  will write the trait in the implementing PR, when we know what it
  needs. The `CAPABILITIES.md` row reflects this honestly: "structural
  rejection until customer demand," not "trait shape locked, impl
  deferred."

## References

- [ADR 0001](./0001-single-region-only.md) — the related "deferred
  until paid" decision, same shape.
- [ADR 0007](./0007-connection-pooling.md) — contrast: trait shape
  *was* worth locking there because the call site already existed.
- [`crates/basin-router/src/sharding.rs`](../../crates/basin-router/src/sharding.rs)
  — the consistent-hash router that already exists.
- [`crates/basin-router/src/remote_shard.rs`](../../crates/basin-router/src/remote_shard.rs)
  — the per-connection upstream forwarder.
- [`crates/basin-engine/src/executor.rs`](../../crates/basin-engine/src/executor.rs)
  — where the planning-time rejection lives.
- [`TASK.md`](../../TASK.md) Phase 4 (single-shard transactions
  deferred) and Phase 6 (this ADR's open box).
- [`CAPABILITIES.md`](../../CAPABILITIES.md) "Cross-shard query merging"
  row — how the router's read-side fan-out compares to what this ADR
  rejects on the write side.
