---
title: "ADR 0010 — Catalog replication"
nav_section: decisions
sidebar_position: 10
summary: "ADR 0010: Catalog replication. See body for status, context, decision, consequences."
---

# 0010 — Catalog replication: single-writer global Postgres with regional read replicas

- **Status:** Accepted (decision only). v0.1 implementation deferred behind
  the milestone gating in this ADR.
- **Date:** 2026-05-07
- **Tags:** architecture, multi-region, catalog, deferred

## Context

Basin's catalog today is one of:

- `InMemoryCatalog` — process-local, evaporates on restart. Used by unit tests
  and the original Phase 1 integration test.
- `PostgresCatalog` — durable, single Postgres instance backs every region.
  Production path.
- `RestCatalog` — stub. Every method `unimplemented!()`. The trait shape is
  locked against a future Iceberg REST catalog deployment (Lakekeeper, Tabular,
  Polaris, …) but no Phase 2 work has landed on the impl.

[ADR 0004](./0004-multi-region-read-replicas.md) committed Basin to multi-region
read replicas — eventual-consistent cross-region reads with region-local writes
and writes-forward-to-primary semantics. ADR 0004's §"Implementation order"
item 4 is the catalog-side dependency:

> 4. Catalog replication: turn on Postgres logical replication; update
>    `PostgresCatalog::connect` to accept a `read_only` flag for replicas.

That sentence is the seed. This ADR is the design doc that locks the choice
and breaks the implementation into milestones, *before* the multi-month
implementation lands. Nothing in `crates/basin-catalog/` needs to change today;
the decision must.

[ADR 0009](./0009-multi-region-architecture.md) (multi-region architecture,
broader than 0004 — region discovery, regional WALs, write-forwarding
semantics, no cross-region writes) landed alongside this ADR. 0009 sketches
catalog logical replication as part of its overall topology; this ADR is
the catalog-specific design that 0009 defers to. The two ADRs are
deliberately decoupled: the catalog replication choice is independent of
how regions are discovered, how WALs are partitioned, or how writes forward
at the pgwire layer. If 0009 is later superseded, the choice in this ADR
should still hold so long as Postgres logical replication is operationally
viable for the catalog schema.

`TASK.md` Phase 6 has had this open box since the wedge expansion in 0004:

> [ ] Catalog replication strategy chosen and implemented

Three architectures are viable. Picking one before the implementation begins
is the entire point of this document.

## Decision

**Pick A: single-writer global Postgres catalog, with per-region read
replicas via Postgres logical replication.**

One Postgres instance — physically located in the project's primary region —
is the *single writer* for every catalog row that project owns. Other regions
run logical-replication subscribers that ship the `basin_catalog.*` schema
into a region-local Postgres replica. `basin-server` processes in a replica
region read from the local replica; writes (DDL, `append_data_files`,
`replace_data_files`, every `set_*`) round-trip back to the primary's
Postgres.

Concretely:

- **Read latency from a non-writer region**: target sub-50ms for
  `Catalog::load_table` / `list_tables`. The read goes to local Postgres
  — same data center, no cross-region RPC.
- **Write latency from a non-writer region**: cross-region round trip
  (typically 60–200ms continental, 200–400ms intercontinental) plus the
  primary Postgres commit. Writes from a replica region are *expected to
  be rare*; ADR 0004 explicitly bills this latency to the customer who
  chose to write away from their primary.
- **Operational complexity tolerance**: Postgres logical replication is
  battle-tested, vendor-supported (RDS, Cloud SQL, Aurora all ship it),
  and operationally well-understood. We do not write a replication state
  machine. We turn on a flag.

### Why not Option B (regional Lakekeeper)

Lakekeeper's own replication story is, today, a deferred decision in
*their* project. Its OSS docs cover single-instance deployment well; the
multi-region story is "deploy multiple instances against a shared Postgres"
or "deploy multiple instances each with their own Postgres and reconcile
externally." Either way, the replication problem is *still ours to solve*
— it just moves up a layer. We would gain nothing by adopting Lakekeeper
prematurely and lose the operational advantage of running plain Postgres.

`RestCatalog` stays in the codebase as the trait-shape lock for a *future*
direction (Iceberg ecosystem interop). Nothing about this ADR forecloses
on it. See "Trigger to revisit" below.

### Why not Option C (hybrid / batched metadata changes)

Hybrid is the wrong shape for our wedge. The wedge customer is multi-project
SaaS with audit-log workloads. Their write rate to the catalog is bounded
by `append_data_files` per project per WAL flush — call it tens of commits
per project per second at the high end, and far less for typical projects.
The catalog write rate is not the bottleneck; the catalog *consistency*
is. Batching metadata changes adds latency to commits in exchange for
throughput we don't need. Reject.

## Trade-offs

| Axis | A. Single-writer global PG (chosen) | B. Regional Lakekeeper | C. Hybrid (batched) |
|---|---|---|---|
| Read latency from non-writer region | sub-50ms (local PG replica) | sub-50ms (local Lakekeeper) | sub-50ms (local PG) |
| Write latency from non-writer region | cross-region RTT + PG commit (60–400ms) | depends on Lakekeeper's replication; today: same as A or worse | cross-region RTT + batch-flush window |
| Read consistency | bounded-stale (PG logical replication, seconds to ~minute) | depends on Lakekeeper config | bounded-stale + batch-window stale |
| Write consistency | strong on primary; replicas eventual | depends on Lakekeeper config | weak (writer can claim success before replica sees it) |
| Blast radius of single-region failure | primary-region outage = catalog writes globally fail; replica reads continue from last-replicated state | per-region writers may continue locally if Lakekeeper is configured for it; reconciliation cost on recovery | mid: writer-region down stalls global writes; replica-region down loses pending batches |
| Operational cost | low — Postgres logical replication is vendor-supported | medium — Lakekeeper is one more service per region, plus the replication still needs solving | high — bespoke batching system, custom GC, custom reconciliation |
| Fits-with-Iceberg-ecosystem score | medium — durable Iceberg metadata, but not exposed via the REST API | high — Lakekeeper *is* an Iceberg REST catalog; external Iceberg readers (Spark, Trino, DuckDB) plug in directly | low — bespoke shape, no external interop |
| Dev-time-to-v0.1 | ~1 sprint (turn on replication + `read_only` flag) | ~3+ sprints (deploy Lakekeeper, build replication, build the `RestCatalog` impl that's currently `unimplemented!()`) | ~4+ sprints (design + build + GC + ops) |
| Failure modes well-understood | yes (Postgres logical replication is the reference case) | partial (Lakekeeper is young) | no (novel) |

The "Iceberg ecosystem fit" row is the only one where Option B wins. We
take that loss explicitly: the wedge customer talks to Basin via pgwire,
not via an Iceberg REST client. External Iceberg interop is a Phase 7+
concern. When it becomes pressing, the trait shape we've already locked
in `RestCatalog` is the migration path.

## Implementation phases

Each milestone is the size of one engineering sprint (~2 weeks of focused
work), not one PR.

### v0.1 — Turn on replication, distinguish replicas at connect time

Goal: a basin-server process in a replica region can serve reads from a
local Postgres replica, and refuses writes with a clear error.

Concrete deliverables:

1. **Postgres logical-replication setup**, documented in
   `docs/operator/multi-region-catalog.md` (new file, written when this
   milestone starts). Covers: `wal_level=logical`, publication on the
   primary covering every `basin_catalog.*` table, subscription on each
   replica region's Postgres, monitoring of replication lag.
2. **`PostgresCatalog::connect_read_only(conn_str, schema)`** — a sibling
   constructor that opens the connection in a mode where every mutating
   method (`create_namespace`, `create_table`, `drop_*`,
   `append_data_files`, `replace_data_files`, every `set_*`,
   `fork_table`, `rollback_to_snapshot`) returns
   `BasinError::Catalog("catalog is read-only on this region")`. Read
   methods (`load_table`, `list_tables`, `list_snapshots`,
   `list_project_data_files`) work normally.
3. **One unit test** asserting a read-only `PostgresCatalog` returns the
   right error on every mutating method, and a read works.
4. **One integration test** (`tests/integration/tests/`, in another agent's
   sprint — coordination only): a primary catalog, a logical-replication
   subscriber, write-on-primary-read-on-replica round-trip with a
   replication-lag assertion.

That is the smallest shippable thing. With v0.1 in place, multi-region
read replicas (ADR 0004 item 4) is unblocked.

Out of scope for v0.1: write forwarding (router-layer, ADR 0004 item 3),
primary-failover automation, replication-lag SLO enforcement.

### v0.2 — Replication-lag observability + per-project primary region

Goal: an operator can see lag and a customer can be assigned a non-default
primary region.

Concrete deliverables:

1. **Replication-lag metric** exposed via `Catalog::replication_lag(&self)
   -> Result<Option<Duration>>` (default impl returns `Ok(None)`;
   `PostgresCatalog::connect_read_only` overrides to query
   `pg_stat_subscription` and return the lag).
2. **`projects.primary_region` column** on the catalog's namespaces row
   (additive migration in `PostgresCatalog::migrate`), populated at
   `create_namespace` from a new optional argument or the
   `BASIN_DEFAULT_PRIMARY_REGION` env var.
3. **Lag-based alerting**: a `basin-server` healthcheck that surfaces
   `replication_lag > 5min` as a degraded state. Wires into the existing
   metrics layer via `ProjectCounterRegistry` or a new top-level metric;
   choose one when this milestone starts.

### v0.3 — Catalog-side support for primary failover

Goal: an operator can fail a project's primary region over to a former
replica with a documented data-loss window.

Concrete deliverables:

1. **`Catalog::promote_to_writer(&self) -> Result<()>`** on the read-only
   side, which (a) verifies replication has caught up to a stop-write
   marker placed on the old primary, (b) flips the local Postgres from
   subscriber to publisher, (c) re-issues the publication. Documented as
   a manual operator action, not automatic.
2. **`Catalog::stop_writes(&self) -> Result<()>`** on the writer side,
   which sets a global `read_only = true` flag in the catalog schema and
   refuses subsequent writes.
3. **Operator runbook**: failover sequence, rollback sequence, data-loss
   window math.

v0.3 is *not* automatic failover. Per ADR 0004: "No automatic failover of
a project's primary region. Primary failover is a manual operator action
with explicit data-loss-window guarantees." This milestone preserves that
posture.

## What this does NOT commit us to

Explicitly out of scope for the entire ADR (i.e., not in v0.1, v0.2, *or*
v0.3):

- **Schema-change replication semantics.** If a Phase 7 work item adds a
  Postgres-extension-style on-line schema change, replication of that
  change is governed by Postgres's normal logical-replication DDL
  caveats (DDL is *not* replicated). A separate ADR will cover that
  when the trigger fires.
- **Multi-region 2PC.** Off the table per ADR 0001 and ADR 0004. Cross-
  region writes round-trip to the primary; we do not lift the
  consistency model to global.
- **Lakekeeper-vs-Tabular-vs-Polaris bake-off.** The `RestCatalog` stub
  stays an unimplemented placeholder. When (if) the trigger fires, a
  successor ADR runs the bake-off; this ADR does not pre-commit.
- **Billing / cost model for cross-region egress on the catalog tier.**
  Catalog egress is small relative to data-file egress (a `load_table`
  is kilobytes; a Parquet read is megabytes). The hosted-cloud billing
  repo's cost model is the right place for this; a placeholder note in
  there is enough until customer revenue exposes a number.
- **Active-active catalog writes.** Multi-master Postgres logical
  replication is operationally a nightmare and the wedge does not need
  it. Future direction stays single-writer-per-project.
- **Catalog sharding.** Today: one Postgres instance holds *every*
  project's catalog. At ~10⁶ projects × ~50 tables × ~10 snapshots, the
  catalog table size is ~500M rows — within Postgres's comfortable
  range with the right indexes. Sharding becomes a discussion at the
  ~10⁸-row threshold; that is well into Phase 7+.
- **Cross-region catalog backup / DR beyond what logical replication
  itself provides.** Backup is per-region operator concern, not a Basin
  feature.

## Trigger to revisit

We write a successor ADR (0011 or later) when **any** of:

1. **Lakekeeper (or another Iceberg REST catalog) ships a turn-key
   multi-region replication story** that is operationally cheaper than
   running Postgres logical replication ourselves. The bar: a vendor-
   supported deployment that handles failover, lag monitoring, and
   schema-change replication out of the box.
2. **A paying customer signs ≥ $50k ARR contingent on sub-100ms catalog
   reads across continents** AND their workload's catalog write rate
   exceeds what Postgres logical replication can sustain to multiple
   subscribers (rough threshold: > 5k catalog commits / second sustained
   on the primary). At that throughput, Postgres logical replication
   begins to fall behind on slow-network subscribers; Lakekeeper or a
   purpose-built per-region catalog becomes worth the operational cost.
3. **External Iceberg-ecosystem interop becomes wedge-aligned.** I.e., a
   wedge customer's analyst team needs to point Spark / Trino / DuckDB
   at Basin's catalog over the Iceberg REST API. At that point, the
   `RestCatalog` stub graduates to a real implementation, and the
   replication question gets re-examined alongside it.
4. **Postgres itself becomes the limit.** If the single-writer Postgres
   becomes a write-throughput bottleneck for any single project (the
   "noisy catalog project" problem, analogous to ADR 0008's noisy storage
   project), the answer is per-project Postgres sharding, *not* a different
   replication architecture. That sharding work gets its own ADR.

A single squeaky prospect at smaller value, or a vague "we'd love
sub-50ms catalog reads in Sydney," is **not** the trigger. Log it in the
lost-deal tracker and watch the aggregate.

## Alternatives considered

- **Option B — Regional Lakekeeper deployments.** Rejected for v1: the
  replication problem still falls on us (Lakekeeper's own multi-region
  story is itself deferred), we'd carry the operational cost of one more
  service per region, and the only customer-facing win is external
  Iceberg interop, which is not on the wedge customer's path. Reopen if
  trigger #1 or #3 fires.
- **Option C — Hybrid with batched catalog changes.** Rejected: the
  catalog write rate is not the bottleneck. Adding a batching layer
  costs commit latency and operational complexity for throughput we
  don't need. Hybrid is a solution looking for a problem.
- **Per-region independent catalogs (no replication).** Equivalent to
  pre-0004 single-region deployments stacked N times. Projects must
  choose one region, no read locality benefit, no cross-region read
  story. Rejected: this is what 0004 explicitly steered away from.
- **Replicate via the WAL.** Tempting (the WAL is already designed to
  ship metadata changes durably), but the WAL is keyed by
  `(project_id, partition_key)` and the catalog is keyed by
  `(project_id, table_name)`. Forcing the catalog through the WAL
  conflates two replication regimes with different consistency needs and
  fan-out shapes. Rejected: stays as a "WAL is for data, catalog is for
  metadata" boundary.
- **CRDT-based catalog state.** Rejected on first reading: the catalog's
  optimistic-concurrency contract on `expected_snapshot` is fundamentally
  a consensus problem (a commit either wins or conflicts). CRDTs that
  preserve that contract reduce to single-writer anyway; CRDTs that
  don't preserve it break the engine's snapshot semantics.

## References

- [ADR 0001 — Single-region only](./0001-single-region-only.md), the
  original "no multi-region" ADR (now partially superseded).
- [ADR 0004 — Multi-region read replicas](./0004-multi-region-read-replicas.md),
  which committed to the read-replica direction and pinned this ADR's
  problem statement.
- [ADR 0009 — Multi-region architecture](./0009-multi-region-architecture.md),
  which covers regional WALs, region discovery, and the broader topology;
  references catalog logical replication and defers the catalog-specific
  design to this ADR.
- `crates/basin-catalog/src/lib.rs` — the `Catalog` trait surface the
  v0.1 work extends (with a read-only constructor, no trait change).
- `crates/basin-catalog/src/postgres.rs` — the durable backend that
  v0.1 turns into both writer and read-only-replica modes.
- `crates/basin-catalog/src/rest.rs` — the stub kept around as the
  trait-shape lock for trigger #3.
