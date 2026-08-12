---
title: "ADR 0030 — Remove DataFusion; build Basin's own query engine"
nav_section: decisions
sidebar_position: 30
summary: "DataFusion leaves the dependency tree entirely. Basin builds its own logical IR, optimizer, and physical executor on top of arrow-rs, in order to reach Postgres fidelity that DataFusion structurally prevents. The delete lands last; the branch stays green throughout."
tags: [architecture, query-engine, pg-compat, technical-debt]
---

# 0030 — Remove DataFusion; build Basin's own query engine

- **Status:** Accepted (2026-08-12), in progress
- **Tags:** architecture, query-engine, pg-compat, technical-debt
- **Cross-references:**
  [ADR 0014 — pg_query as canonical parser](./0014-pg-query-as-canonical-parser.md),
  [ADR 0015 — Vortex storage format](./0015-vortex-storage-format.md),
  [ADR 0002 — no Postgres extensions](./0002-no-postgres-extensions.md),
  [ADR 0022 — system schema namespacing](./0022-system-schema-namespacing.md),
  [ADR 0024 — UUID/Decimal128 storage](./0024-uuid-decimal128-storage.md),
  [ADR 0027 — binary JSONB encoding](./0027-binary-jsonb-encoding.md)

## Context

Basin has been built on DataFusion 53 since inception. DataFusion earned its
place: the analytics wins Basin publishes against Postgres 18 — LATERAL join
462×, star join 261×, correlated subquery 113×, `PERCENTILE_CONT` 19× — are
produced by DataFusion's logical optimizer and vectorised executor, not by
anything Basin wrote. That is a real debt owed to a good library, and this ADR
does not pretend otherwise.

The problem is that DataFusion's ceiling is no longer a performance ceiling. It
is a **Postgres-fidelity** ceiling, and Basin's product is Postgres
compatibility.

Measured state of the coupling as of this ADR (workspace-wide grep, 2026-08-12):

- **1,016 `datafusion` references across 69 files** — 67 in `basin-engine`, one
  in `basin-storage`, one in `basin-bench-harness`. The coupling is
  well-contained by crate, which is what makes removal tractable at all.
- **279 of those references are `datafusion::arrow::*` re-exports.** arrow-rs
  is a separate dependency that Basin keeps. Those references are a rename, not
  work.

The fidelity ceiling shows up as concrete, load-bearing damage:

- **`crates/basin-engine/src/pg_operators.rs` is 9,546 lines of string
  rewriting** that exists only because DataFusion's SQL surface is not
  Postgres's. Its own header concedes the approach: "pure string-manipulation
  functions — no AST", and "best-effort … do NOT handle dollar-quoted strings,
  comments, or identifier quoting." A correctness hazard sits in front of the
  planner, and it is structural, not a bug to be fixed.
- **`pg_catalog` is a simulation.** `info_schema_provider.rs` is 2,568 lines of
  `TableProvider` shims, and notes at line 80 that DataFusion's `TableProvider`
  "can't see through the `Arc<dyn Catalog>`". Real clients — `psql \d`,
  `pg_dump`, ORM introspection, migration tools — query the catalog as ordinary
  joinable relations with stable OIDs. A per-query shim cannot serve them.
- **Set-returning functions cannot exist.** `jsonb_udf.rs:16`:
  `jsonb_array_elements` and its family "cannot be true SRFs inside DataFusion".
  `generate_series` and `unnest` in the target list are ordinary Postgres, and
  are unavailable.
- **DML is not a relation.** `executor.rs:3128` and `:13038` — DataFusion cannot
  plan DML as a relation, so data-modifying CTEs
  (`WITH x AS (INSERT … RETURNING …) SELECT …`) are unexpressible.
- **RLS is bypassable by construction.** `executor.rs:3182` documents a separate
  RLS gate needed because the fast paths skip DataFusion's logical planner. A
  security rewrite that some execution paths can route around is the wrong
  shape for a security rewrite.
- **Upstream limitations Basin cannot fix**: correlated LATERAL left unrewritten
  (`pg_operators.rs:2246`), a DataFusion 53 optimizer bug worked around for
  multi-column recursive CTEs (`:5069`), `= ANY(ARRAY[…])` coercion
  (`:1593`, `:2409`), `WITH TIES` silently degraded to `ONLY`
  (`select_advanced.rs:455`), and synchronous UDFs that cannot be preempted,
  capping `statement_timeout` (`executor.rs:12`).

Two decisions already made point the same direction. ADR 0014 retired
DataFusion's `sqlparser` frontend for libpg_query, because the dialect was
"materially incomplete". And five modules — `fast_select`, `fast_aggregate`,
`values_fast`, `index_probe`, `point_join` — already execute queries without
DataFusion, because its per-query planning cost dominates OLTP latency.
`fast_select.rs:5` states it plainly. Basin's best OLTP numbers come from code
paths that do not touch DataFusion at all.

Basin has, in other words, already left DataFusion at the frontend and at the
OLTP hot path. What remains is an analytical execution engine whose SQL
semantics Basin must continuously translate into and out of.

## Decision

**Remove DataFusion from Basin entirely. No fallback path, no trait-boundary
retention, no vendored fork. DataFusion leaves `Cargo.toml`.**

Basin builds its own logical IR, optimizer, and physical executor. Concretely:

1. **arrow-rs 58 stays.** All compute kernels — filter, take, sort, compare,
   hash, cast — remain arrow's. Basin builds plan orchestration on top of them,
   not vectorised kernels from scratch. This is the single largest reason the
   effort is bounded.
2. **The logical type system becomes Postgres's, not Arrow's.** Arrow remains
   the physical representation. `typmod`, the `unknown` type for untyped
   literals, domains, composites, enums, ranges, and genuine multidimensional
   arrays are modelled directly, consistent with ADR 0024 and ADR 0027.
3. **`pg_catalog` becomes real relations** backed by Basin's actual metadata,
   with OIDs matching Postgres's well-known values for builtin types. Faithful
   catalog behaviour is a first-class goal, not an emulation layer.
4. **Extensions remain native Rust crates — no `.so`, no dlopen.** This extends
   rather than contradicts ADR 0002. `CREATE EXTENSION` becomes a catalog
   operation that registers an already-compiled crate's functions into
   `pg_proc`, so clients issue the statement and it works.
5. **The parser stays libpg_query** (ADR 0014). Lowering goes
   pg_query parse tree → Basin IR directly, which deletes the string-rewriting
   layer and replaces it with operator resolution against a real `pg_operator`
   catalog.
6. **Single-partition streaming execution first.** Basin already pins
   `target_partitions = 1` by default (`session.rs:2705`); parallelism gets a
   clean extension point rather than being built now.
7. **Security rewrites become structurally unbypassable.** RLS, CHECK, and FK
   enforcement are mandatory plan rewrites in one planner, not gates bolted onto
   each execution path.

### Sequencing

**DataFusion is deleted last, not first.** The replacement must exist before the
delete can compile. The branch (`feat/own-engine-remove-datafusion`) builds and
passes tests at every commit; the final commit in the sequence is the
`Cargo.toml` line removal. Migration analysis lives in
`docs/migration/df-removal/`.

## Consequences

### Positive

- Postgres fidelity stops being bounded by a third party's dialect. The
  blocked-feature list above becomes ordinary backlog.
- The 9,546-line string rewriter is deleted rather than extended.
- `pg_catalog` fidelity unlocks the tooling ecosystem — psql, pg_dump, ORM
  introspection, migration tools — which is worth more to Basin's users than
  any additional benchmark multiplier.
- The arrow 58 / DataFusion 53 / vortex 0.71 version lockstep documented in
  ADR 0015 and the root `Cargo.toml` loses one of its three legs.
- Cost modelling can use Basin's real unit — an S3 GET — instead of
  DataFusion's local-page assumption.

### Negative — stated plainly

- **Cost.** Measured estimate: ~62–98k LOC of new engine code (midpoint ~75k),
  ~115k including tests at Basin's own observed code:test ratio, plus a further
  ~15–25k for catalog and type fidelity. Roughly 18–30 engineer-months. This is
  the dominant consequence and it is not hedged.
- **The optimizer is the risk concentration.** DataFusion's
  `decorrelate_predicate_subquery` (1,780 lines), `push_down_filter` (3,469),
  `eliminate_cross_join` (1,133), and `extract_equijoin_predicate` (400) are
  what produce the published win table. Underscoping the optimizer keeps the
  engine and loses the benchmarks.
- **Semantic drift is the highest-severity risk** — a wrong answer is worse
  than a slow one. DataFusion's 475k LOC encode years of edge-case fixes in
  NULL semantics, decimal overflow, timestamp coercion, and NaN ordering. Basin
  will rediscover a long tail of these.
- **Opportunity cost.** Basin is pre-alpha with a v0.1 cut-off
  (`docs/V0_1_SCOPE.md`). Engine work competes directly with that roadmap.
- **Vortex coupling.** `vortex-datafusion` 0.71 implements DataFusion's
  `FileFormat` trait. Removing DataFusion means reading Vortex through the base
  `vortex` crate directly. This is being assessed as a potential blocker.

  > **Resolved 2026-08-12: not a blocker.** `basin-storage` carries no
  > DataFusion dependency at all and already reads both formats directly —
  > Vortex through `open_buffer().scan()` with projection and filter, Parquet
  > through arrow-rs with row-group, page-index and bloom pruning. The coupling
  > is confined to `crates/basin-engine/src/vortex_listing_format.rs`, a
  > 1,184-line adapter that exists to present files to DataFusion's
  > `ListingTable`. It is **deleted, not ported**, and takes four direct
  > dependencies with it. Verified three ways: `datafusion-pruning` has zero
  > call sites, Basin implements `PruningStatistics` nowhere, and the crate
  > reaches `Cargo.lock` only via DataFusion-side dependents. See
  > [`06-scan-and-storage.md`](../migration/df-removal/06-scan-and-storage.md).

### Estimates as measured

The sizing above was written before the subsystem surveys. Recording where it
moved, in both directions, so the number is not quietly re-anchored:

| Component | Original | Measured | Source |
|---|---|---|---|
| Optimizer | 8–12k | **4.4–6.7k** | Rule-by-rule ablation against real DataFusion 53 plans ([05](../migration/df-removal/05-optimizer-rules.md)) |
| Physical layer | 19–30k | **8.5–13k** | Parallelism is never constructed — the guard that raises `target_partitions` also disables repartitioning ([03](../migration/df-removal/03-physical-operators.md)) |
| Function library | 9–17k | **22–30k** | 215 missing names, 127 required; window functions are entirely greenfield ([04](../migration/df-removal/04-function-gap.md)) |
| Catalog fidelity | 15–25k | **9–13.5k** | ~65 relations already exist and are real queries, not stubs ([11](../migration/df-removal/11-pg-catalog-fidelity.md)) |

Two corrections worth naming, because both were assumptions rather than
measurements. The claim that six of thirteen published wins are storage results
that "survive whatever the optimizer does" is **false**: ablation showed they
collapse without `optimize_projections` and `push_down_filter`. And the 462×
LATERAL win is **not** DataFusion's `DecorrelateLateralJoin` — DataFusion never
decorrelates that query; Basin's own textual rewrite in `pg_operators.rs:3461`
does, which means it must be ported rather than discarded with the rest of that
file.

### Mitigations

- Basin's ~199,530 LOC integration suite plus ~66,583 LOC of in-file unit tests
  form an executable conformance spec for current behaviour. Because DataFusion
  is deleted last, a differential harness can run both engines and diff results
  during the entire migration.

  > **Amended 2026-08-12, same day, before any engine work.** The claim above is
  > materially overstated and is corrected here rather than left standing.
  > `.github/workflows/ci.yml:136` runs
  > `cargo test --workspace --exclude basin-integration-tests` — **every one of
  > the 383 integration test files is excluded from the PR gate**, on the
  > recorded grounds that their binaries OOM the runner at link time
  > (`ci.yml:79`). The differential-vs-Postgres oracle covers **23 shapes**, and
  > there is exactly **one** `.slt` file
  > (`tests/integration/sqllogictest-suites/basic.slt`).
  >
  > So the conformance spec exists as code but **does not run automatically**,
  > and the part that directly compares Basin against Postgres is a fraction of
  > the LOC figure. Semantic drift — this ADR's highest-severity risk — is
  > currently guarded by an instrument that no PR trips.
  >
  > **This makes the oracle a prerequisite, not a mitigation.** Phase 0 of the
  > migration is building it: get the integration suite running in CI (or a
  > nightly gate if the OOM constraint is real), and widen the differential
  > oracle well beyond 23 shapes. No engine code should be written before that
  > lands. See
  > [`docs/migration/df-removal/10-risk-and-phases.md`](../migration/df-removal/10-risk-and-phases.md).

- Fallback rate — the share of queries the owned engine cannot yet execute — is
  the governing metric. DataFusion is removed when it reaches zero, not on a
  date.

## Architectural compatibility

Nothing in this ADR changes the storage format, the wire protocol, the catalog
schema on disk, or the project-per-S3-prefix model. If the migration is halted,
Basin remains on DataFusion with the migration documents as sunk analysis; the
owned IR crates can sit unused without affecting the shipped engine, because
the delete is sequenced last and never lands early.

## Trigger to reconsider

This ADR is a "yes" to a large build, so its trigger is a kill criterion rather
than a customer contract. Pause or abandon if any of the following hold at a
phase boundary:

- The owned optimizer cannot reproduce the published benchmark table within a
  2× regression on any headline shape, after a genuine attempt at the
  must-have rules.
- The differential harness shows semantic divergences that are not converging
  cycle over cycle.
- Vortex cannot be read without `vortex-datafusion`, and the alternative is
  rewriting the Vortex reader as well.
- v0.1 slips materially because engine work has displaced roadmap work.

## Alternatives considered and why we didn't pick them

**Keep DataFusion behind a trait boundary, owning only the OLTP path.** The
lowest-cost option, and the one this ADR was originally going to recommend. It
fails the actual goal: `pg_catalog` fidelity, true SRFs, DML-as-relation, and
unbypassable RLS all require owning the planner. A trait boundary relocates the
ceiling without raising it.

**Fork DataFusion and modify it in place.** Licensing permits it — both projects
are Apache-2.0. Rejected because a fork inherits 475k LOC of design decisions
aimed at a different target (a general-purpose, extensible, distributed query
framework) and Basin would carry maintenance of code it did not write toward
objectives DataFusion does not share. The divergence cost compounds with every
upstream release.

**Stay as-is and accept the fidelity ceiling.** Viable if Basin's product were
analytics. It is not: Basin's value proposition is Postgres compatibility on
bucket-native storage, and the ceiling caps precisely that.
