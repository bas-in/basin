---
title: "ADR 0023 — Lease-based ownership + partition-level routing + heartbeat budgets"
nav_section: decisions
sidebar_position: 23
summary: "Convert per-(project,partition) ownership from a hash on ProjectId into a lease in the catalog Postgres. Stateless replicas + partition-level routing + heartbeat-reconciled budgets fix hot-project pinning and multi-instance cap-bypass in one architecture, without a central coordinator service or distributed counters on the hot path. The architectural commitment for Basin's multi-replica scale-out."
tags: [architecture, scaling, multi-project, hot-tier]
---

# 0023 — Lease-based ownership + partition-level routing + heartbeat budgets

- **Status:** Accepted, 2026-05-21.
- **Tags:** architecture, scaling, multi-project, hot-tier
- **Supersedes / strengthens:** the implicit single-replica model in
  [ADR 0010 (catalog replication)](./0010-catalog-replication.md),
  [ADR 0016 (HTAP hot-tier architecture)](./0016-htap-hot-tier-architecture.md).
- **Driving audit:** [`docs/audits/2026-05-21-noisy-neighbor-fairness.md`](../audits/2026-05-21-noisy-neighbor-fairness.md)
  (4 P0 / 6 P1 / 5 P2-P3, 11 adversarial scenarios; commit `9316bf5`).

## Context

The noisy-neighbor audit surfaced two coupled architectural P0s:

1. **Hot-project pinning.** `ShardMap::shard_for` (`crates/basin-router/src/sharding.rs:88–94`)
   hashes `ProjectId → ShardOwner`. The HTAP memtable
   (`crates/basin-hottier/src/registry.rs:149`) and the per-`(project, partition)`
   WAL mutex (`crates/basin-wal/src/file_wal.rs:57`) live only on the hashed
   replica. A write-heavy whale pins one replica at 100 % CPU while sibling
   replicas stay cold.
2. **Multi-instance cap bypass.** Every per-project cap (REST QPS, pgwire QPS,
   memtable bytes, realtime `BUFFER_FULL`, Wasm semaphore, basin-net outbound)
   is a per-process `DashMap` with no cross-replica aggregation. A project capped
   at 100/s sustains `N × 100/s` by spraying across `N` replicas. The new
   overage prices (`acb2f7c`: storage $0.010/GB-mo, compute $0.018/CPU-hr) are
   similarly per-replica → undercharging.

The audit framed them as one gap with two repair options:
(a) a central coordinator/budget service, or (b) replicated counters via Raft/gossip.

**Both options fail the actual requirement.** Option (a) fixes the cap leak but
leaves whales pinned. Option (b) pays a distributed-systems cost on the hot
path for a strictly weaker version of (a). Neither addresses the underlying
structural choice: per-project state lives in process-local memory on
whichever replica `ShardMap` happened to hash to.

### The accidental half-architecture
Almost every OSS subsystem is *already* keyed by `(project, partition)` or
`(project, table)`, not by whole-project:
- WAL: `(project, partition) → segment` + per-pair mutex.
- HTAP memtable: `(project, table) → MemTable`.
- Catalog: `(project, table) → DataFileRef`.
- Per-table cluster/sort, blooms, sketches, secondary indexes.

The single thing that still keys on whole-project is the **router**. Fixing
that one mismatch is the architectural unlock — every downstream subsystem
already has the right shape.

## Decision

Adopt a **lease-based ownership model with partition-level routing and
heartbeat-reconciled budgets.** Specifically:

### Compute layer — fully stateless replicas
Replicas serve any request for any project. They hold no per-project durable
state. Each replica maintains only:
- A **lease cache** `(ProjectId, PartitionId) → ReplicaId` (with TTL,
  populated lazily from the coordinator).
- The transient runtime state for partitions whose lease it currently holds
  (memtable, WAL mutex, per-partition cap counters).

### State layer — leases in the catalog Postgres
A new table:

```sql
CREATE TABLE basin_catalog.partition_leases (
    project_id   UUID NOT NULL,
    partition_id TEXT NOT NULL,
    holder       TEXT NOT NULL,        -- replica id (host:pid or stable uuid)
    epoch        BIGINT NOT NULL,      -- monotonic fencing token
    granted_at   TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, partition_id)
);
```

- A replica acquires a lease via a CAS update (`UPDATE … WHERE
  holder = $existing AND expires_at < now()` for steal-on-expiry, or `INSERT
  … ON CONFLICT DO NOTHING` for first-grant).
- Default TTL: **15 s**, renewed every **5 s** by a per-replica heartbeat.
- On replica death the lease expires; any replica can take it after the TTL.
- The **epoch** is a fencing token: every WAL append carries the lease epoch;
  if a dual-leaseholder ever appears (e.g. network partition), the loser's
  appends fail at the WAL layer.
- This is a Postgres-only design — no Raft, no Redis, no new service.

### Routing — partition-aware
`ShardMap::shard_for` becomes `LeaseRegistry::owner_for(project, partition)`.
Cache hit → route. Cache miss → coordinator lookup (one indexed read on the
lease table) → cache with TTL. For projects with the default single partition,
behaviour is identical to today; this is the no-op back-compat path.

### Budgets — heartbeat reconciliation, not distributed counters
- The **leaseholder is the single arbiter** for its partition's cap counters
  (memtable bytes, in-flight requests, Wasm semaphore slots, etc.). No
  coordination per request — local arithmetic on the hot path.
- Every 5 s heartbeat carries per-`(project, partition)` deltas to the
  coordinator (same lease table, additional `usage_delta` column or sidecar
  table).
- The coordinator computes **project totals** as the sum across leaseholders
  and writes a **per-partition slice budget** back into each heartbeat
  response. Slice = `project_total_budget / partition_count`, conservatively
  rounded down.
- A project can over-spend by at most one heartbeat interval × the slack the
  scheduler grants. This is **acceptable for billing** (you charge actual
  usage; the cap is a slow brake, not a per-request gate) and for soft caps;
  hard limits set the slice conservatively.
- Hot path stays purely local. No RTT to the coordinator per request.

### Storage / catalog substrate — unchanged
Object store stays the durable substrate. Catalog Postgres gains the lease
table + the heartbeat sidecar. Per-`(project, partition)` data files, WAL
segments, snapshots, blooms, sketches — already correctly keyed; they travel
with the lease.

## Consequences

**Positive**
- **Hot-project pinning is automatic.** A project with N partitions distributes
  across replicas naturally. Whales partition explicitly; everyone else
  defaults to 1 partition and sees no behaviour change.
- **Caps are correct without per-request coordination.** The leaseholder owns
  the arithmetic. Project totals reconcile asynchronously; over-cap window is
  bounded by the heartbeat interval.
- **Replicas are stateless enough to scale elastically.** Add a replica →
  coordinator rebalances. Lose a replica → leases expire, others take over
  after `TTL + reconnect`.
- **Composes with everything already built.** HTAP, blooms, sketches,
  secondary indexes — already per-table; they travel with the lease unchanged.
- **No new infrastructure component.** The catalog Postgres already exists;
  this adds two tables and a heartbeat loop.

**Negative / accepted trade-offs**
- **Lease handoff under load stalls a partition** for the duration of a
  memtable flush + new-replica warmup (target: < 500 ms p99). Acceptable;
  comparable to a Cockroach range split.
- **Project-total budget is eventually consistent.** Over-cap by ≤ 1
  heartbeat interval (5 s default). Mitigated by sizing per-partition slices
  conservatively.
- **More moving parts than today's static hash.** Real, but bounded — the
  lease table is 4 indexed columns; the heartbeat is one UPSERT per 5 s per
  replica.
- **Fencing is via WAL epoch, not Raft.** A network partition can produce a
  brief dual-leaseholder window; the loser's appends fail at WAL write time
  (the epoch check is cheap and constant-time). Correctness preserved; one
  partition under partition loses up to 5 s of writes (the partitioned
  replica's in-flight WAL appends get rejected).

## Implementation — Phase 6.X (TOP PRIORITY)

Decomposed for parallel agent dispatch. Sequencing: P0 fixes (parallel) →
6.X.A (foundation) → 6.X.B (router) → 6.X.C (handoff) → 6.X.D (budgets) →
6.X.E (failure path) → 6.X.F (observability).

### Phase 6.P0 — single-instance mechanical fixes (parallel, ~1 day each)
These are independent of the architectural work, immediately good, and close
the audit's single-instance P0s while the bigger work proceeds.

- **6.P0.A — Statement-level wall-clock + CPU timeout.** Add to the executor
  hot path; default `BASIN_STATEMENT_TIMEOUT_MS` (e.g. 30 s). Closes the
  "any hostile/buggy query runs forever" hole today.
- **6.P0.B — Catalog connection pool.** Replace `Mutex<Client>` in
  `crates/basin-catalog/src/postgres.rs:57–60` with `deadpool-postgres`.
  Every DDL no longer serializes every catalog read.
- **6.P0.C — Wasm on a dedicated tokio runtime, not the shared
  `spawn_blocking` pool.** Kills cross-project Wasm starvation of the
  shard-mode executor.

### Phase 6.X — the architectural commitment (~6–10 weeks single-engineer)

- **6.X.A — Lease table + leaseholder primitive** (~2 wk). The
  `partition_leases` table; `LeaseRegistry` trait + Postgres impl; lease
  acquire / renew / steal / release; epoch fencing token at the WAL append
  site. **Foundation — blocks B/C/D/E/F.**
- **6.X.B — Partition-level routing** (~2 wk). `ShardMap::shard_for(project)`
  → `LeaseRegistry::owner_for(project, partition)`. Router caches with TTL
  + miss-fetch from coordinator. Default 1 partition per project (back-compat
  byte-for-byte); whales partition explicitly via DDL. **Depends on A.**
- **6.X.C — Lease handoff under load** (~1 wk). A replica voluntarily yields
  a lease on coordinator request: snapshot memtable → flush → transfer epoch
  → ack. Target < 500 ms p99 stall. **Depends on A + B.**
- **6.X.D — Heartbeat budget reconciliation** (~1 wk). Per-replica heartbeat
  loop pushes per-`(project, partition)` usage deltas; coordinator computes
  project totals and writes per-partition slice budgets into the heartbeat
  response. Replace all the per-process `DashMap` caps (REST QPS, memtable
  bytes, Wasm semaphore, etc.) with slice-budget consumers. **Depends on A.**
- **6.X.E — Failure-path hardening** (~1–2 wk). Replica-loss tests
  (kill + recover within TTL); dual-leaseholder fencing test (force a
  partition; verify the loser's WAL appends are rejected); network-partition
  simulator. **Depends on A–D.**
- **6.X.F — Observability + ops** (~1 wk). Dashboards for lease
  distribution, rebalance events, over-cap windows, heartbeat lag.
  Operator runbook (`docs/operators/lease-ownership.md`). **Depends on A–D.**

### Explicitly out of scope (or deferred)
- Raft / Paxos for lease consensus — Postgres CAS is sufficient.
- Distributed counters on the hot path — heartbeat reconciliation does the
  job at lower cost.
- A separate budget service — the catalog Postgres is the only coordinator.
- Cross-project budget aggregation — caps are per-project; org rollups happen
  in basin-cloud, not basin OSS.
- Project migration daemon (auto-rebalance heuristics) — for v1, ops trigger
  rebalance manually; auto comes when a real customer needs it.

## Cross-references

- Closes the noisy-neighbor audit's P0s coupled with hot-project pinning:
  [`docs/audits/2026-05-21-noisy-neighbor-fairness.md`](../audits/2026-05-21-noisy-neighbor-fairness.md).
- Strengthens [ADR 0010 (catalog replication)](./0010-catalog-replication.md)
  — the lease table is in the global Postgres ADR 0010 already selected.
- Strengthens [ADR 0016 (HTAP hot-tier)](./0016-htap-hot-tier-architecture.md)
  — the memtable's `(project, table)` keying is already correct; only the
  ownership-of-the-replica question moves.
- Unblocks the new overage pricing's per-replica cap correctness (basin-cloud
  commit `acb2f7c`); see basin TASK.md task #15 (BLOCKER metering).
