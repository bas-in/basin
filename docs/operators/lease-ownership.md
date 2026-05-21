---
title: "Lease ownership — operator runbook"
nav_section: operations
sidebar_position: 50
summary: "Day-2 ops guide for ADR 0023 lease-based ownership: how it works, how to query lease state, how to rebalance hot replicas, when to bump partition count, and the stuck-lease incident playbook."
tags: [operations, scaling, multi-project, lease, partitioning]
---

# Lease ownership — operator runbook

Day-2 operator's guide to Basin's lease-based ownership model
([ADR 0023](../decisions/0023-leases-and-partition-routing.md)).

The model is Postgres-backed leases on each `(project, partition)` pair.
Replicas are stateless — they hold leases, drain them on shutdown, and
take them over on peer death. Everything below is **what an on-call has to
know** to operate, debug, and tune that system.

---

## How lease ownership works (one-page summary)

Ownership of a `(project, partition)` lives in one row of the catalog
Postgres table `basin_catalog.partition_leases`:

```sql
CREATE TABLE basin_catalog.partition_leases (
    project_id   UUID NOT NULL,
    partition_id TEXT NOT NULL,
    holder       TEXT NOT NULL,        -- replica id (host:pid + salt)
    epoch        BIGINT NOT NULL,      -- monotonic fencing token
    granted_at   TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, partition_id)
);
```

Defaults (overridable via env):

| knob | default | env var |
|---|---|---|
| Lease TTL | 15 s | `BASIN_LEASE_TTL_SECS` |
| Heartbeat / renew cadence | 5 s | `BASIN_LEASE_RENEW_SECS` |

**Lifecycle**: a replica calls `LeaseRegistry::acquire` on first touch;
the heartbeat loop renews every 5 s; on replica death the lease expires
within TTL and any peer can take it via `acquire` (steal-on-expiry).

**Fencing**: every (re)grant bumps `epoch` monotonically. Every WAL append
carries the holder's epoch; a stale-epoch append (the loser of a network
partition) is rejected at the WAL layer. This is the correctness anchor —
correctness is preserved during the brief dual-leaseholder window without
Raft / Paxos.

**No central coordinator service.** The catalog Postgres is the only
shared component. Add replicas freely; lose replicas freely (degraded for
≤ TTL while peers steal).

Read the full architectural rationale in
[ADR 0023](../decisions/0023-leases-and-partition-routing.md).

---

## Observability — the dashboard cards

Every replica exports the following metrics (Phase 6.X.F). All names are
the OTLP / Prometheus convention; the basin-cloud exporter ships them to
your monitoring stack via the standard `/metrics` scrape.

| metric | type | dimensions | what it tells you |
|---|---|---|---|
| `basin_lease_holdings_total` | gauge | `replica` | How many partitions a replica currently owns. Skew across replicas = hot project. |
| `basin_lease_acquire_total` | counter | `replica`, `result={acquired,stolen,failed}` | Rate of new acquires. `stolen` rate spike = a peer is flapping. |
| `basin_lease_renew_total` | counter | `replica`, `result={ok,expired,failed}` | `expired` ≠ 0 means a replica lost a lease. `failed` ≠ 0 means coordinator transport hiccups. |
| `basin_lease_handoff_duration_ms` | histogram | `replica` | Voluntary handoff stall time. Empty until 6.X.C lands. ADR 0023 target: < 500 ms p99. |
| `basin_budget_over_cap_seconds_total` | counter | `project`, `cap` | Seconds the project's slice for `cap` was observed at-or-above 100 % utilisation. Rising = noisy neighbour. |
| `basin_heartbeat_lag_ms` | histogram | `replica` | Round-trip of the heartbeat renew + budget push to the coordinator Postgres. p99 > 1 s = coordinator unhealthy. |

> **Leading indicator priority**: `basin_heartbeat_lag_ms` p99 climbing is
> the earliest signal that ownership churn is imminent — the coordinator
> is the only shared dep, so its health degrades *before* leases expire.
> Alert on p99 > 1 s; page on p99 > TTL / 2.

The metric surface itself lives in
`basin_common::project_counters::LeaseMetrics`; the heartbeat sites in
`crates/basin-shard/src/in_process.rs` are the emission points.

---

## Routine: identify hot replicas

A "hot replica" is one whose `basin_lease_holdings_total` gauge sits well
above its peers' for an extended window. Two ways to confirm + diagnose:

**Dashboard read** — sort the `basin_lease_holdings_total{replica=…}`
panel by value. The top replica is the candidate; if its value is
> 2× the cluster median, escalate to manual rebalance (next section).

**Direct catalog query** — useful when the dashboard isn't trusted (e.g.
during an incident where metrics exporters are themselves under
suspicion). Run against the catalog Postgres:

```sql
-- Live leases grouped by holder. NULL holder = expired/reclaimable.
SELECT
    holder,
    count(*)                                        AS leases_held,
    count(*) FILTER (WHERE expires_at < now())      AS leases_expired,
    min(granted_at)                                 AS oldest_grant,
    max(epoch)                                      AS highest_epoch
FROM basin_catalog.partition_leases
GROUP BY holder
ORDER BY leases_held DESC;
```

**Top-10 partitions by activity** (when you suspect a single hot
partition rather than a hot replica):

```sql
-- Requires the per-(project, partition) byte-counter snapshot from
-- ProjectCounterRegistry, exposed via the `/v1/admin/project_counters`
-- endpoint or by joining against the change-event stream.
SELECT
    project_id,
    partition_id,
    holder,
    epoch,
    expires_at - granted_at AS lease_age
FROM basin_catalog.partition_leases
WHERE expires_at > now()
ORDER BY granted_at ASC
LIMIT 10;
```

---

## Routine: manual rebalance (force a yield)

If one replica is hot and rebalance is desired immediately — for example,
ahead of a planned bounce or because a whale is hogging it — the
operator-visible primitive is `ALTER PROJECT <project> YIELD PARTITION
<partition>`. This is delivered by Phase 6.X.C (lease handoff under load).

> **TODO — 6.X.C dependency.** The DDL itself, and the in-flight handoff
> state machine that drains the memtable before flipping the lease, are
> Phase 6.X.C work. Until that ships, the only manual primitive is the
> "stuck lease" recovery procedure (next section) — i.e. an operator
> deletes the lease row and pays the TTL for ownership to transfer. This
> path is correct but lossy in the sense that any in-flight writes during
> the steal window may be rejected by epoch fencing.

Once 6.X.C ships, the rebalance flow is:

```sql
-- Send the yield request to the current leaseholder via the SQL surface.
-- The leaseholder marks the partition draining, flushes its memtable
-- (target stall < 500 ms p99), and releases the lease so another replica
-- can acquire it. Reads stay live during the drain; new writes get
-- `BasinError::LeaseHandoffInProgress` and the router retries on the new
-- owner.
ALTER PROJECT acme_prod YIELD PARTITION 'p-7';
```

Observation: `basin_lease_handoff_duration_ms` records one sample per
yield. After the yield, `basin_lease_holdings_total` for the old + new
replica each shift by 1.

---

## Routine: partition-count tuning (whale identification)

A project with one partition has its load tied to one replica. Bumping
the partition count lets the lease coordinator spread the project across
many replicas. **Default = 1 partition** so the no-op path is byte-for-
byte today's behaviour; whales partition explicitly.

### When to bump

A project is a whale candidate when **all** of:

- It pins one replica's `basin_lease_holdings_total` upward — i.e. its
  partition's lease is rarely yielded.
- Its `basin_budget_over_cap_seconds_total{cap=memtable_bytes}` (or
  `rest_qps` / `pg_qps`) is non-zero across a 24h window.
- The same project shows up disproportionately in
  `basin_stat_statements` (next query).

**Top byte-volume projects** (whale identification):

```sql
-- Requires basin-engine's per-project counter snapshot. Persisted to the
-- `basin_admin.project_counters_snapshot` table by the periodic exporter
-- (Phase 5.16). For an on-the-wire query, hit the `/v1/admin/project_counters`
-- endpoint instead; this SQL form is for the durable-snapshot case.
SELECT
    project_id,
    bytes_written_total,
    bytes_read_total,
    cpu_micros_total / 1e6              AS cpu_seconds_total,
    ops_total
FROM basin_admin.project_counters_snapshot
WHERE snapshot_at > now() - interval '24 hours'
ORDER BY bytes_written_total DESC
LIMIT 10;
```

**Top per-shape query cost** (Phase 5.16, `basin_stat_statements`):

```sql
SELECT
    project_id,
    query_shape_hash,
    total_exec_time_ms,
    calls,
    total_exec_time_ms / NULLIF(calls, 0) AS mean_ms
FROM basin_stat_statements
WHERE last_seen > now() - interval '24 hours'
ORDER BY total_exec_time_ms DESC
LIMIT 20;
```

### How to bump

Set the partition count on the project. The router invalidates its
cache; subsequent writes hash to one of the N partitions and the
coordinator rebalances over the next few heartbeat intervals.

```sql
ALTER PROJECT acme_prod SET partitions = 8;
```

**Sizing rule of thumb.** Start at `partitions = ceil(p99_load / replica_capacity)`,
double until `basin_lease_holdings_total` is flat across the cluster.
Past `partitions = replica_count × 2` you stop helping spread and start
paying for more memtable cold-start cost on lease handoff — the
diminishing-returns knee is `partitions ≈ 2 × replica_count`.

> **TODO — 6.X.C dependency.** Without the lease handoff state machine,
> a `SET partitions = N` increase only takes effect on first touch of a
> newly-introduced partition; existing partition data does not
> automatically reshuffle. Operators wanting hot-data reshuffling will
> need to coordinate with downtime until 6.X.C ships an online
> repartition path.

---

## Incident playbook: stuck lease

**Symptom**: `basin_lease_renew_total{result=ok}` for one replica has
flatlined but the replica is still serving traffic (or, worse, it has
silently hung — process up, heartbeat thread stuck). `basin_lease_holdings_total`
is non-zero for that replica but its writes are not landing. Other
replicas can't take over until TTL expires.

**Root cause**: a process is hung but not dead. Its heartbeat loop has
stalled (e.g. wedged on a Postgres connection pool exhaustion); the lease
row's `expires_at` is being renewed *because the renew tick is queued
behind the hang*, or has just been renewed before the hang and is now
counting down. Until `expires_at < now()`, no peer can steal.

**Recovery time = TTL** (default 15 s). If that's unacceptable for the
SLA, the manual steal procedure follows.

### Manual steal procedure (catalog UPDATE)

> **Lossy operation — read this first.** This bypasses the steal-on-expiry
> safety net. The displaced holder, if it un-wedges, will attempt one more
> heartbeat renew; that renew will fail (the epoch has moved); its WAL
> appends will be fenced; its partition state will be dropped. Any
> in-flight writes on the old holder between the steal and the
> drop-state may be rejected. This is the **correct** behaviour
> (correctness is preserved) but operators should expect a small write
> failure spike.

Step 1. Confirm the lease is genuinely stuck (not just slow):

```sql
SELECT holder, epoch, granted_at, expires_at, now() AS now_ts
FROM basin_catalog.partition_leases
WHERE project_id = '<project_uuid>'
  AND partition_id = '<partition_id>';
```

If `expires_at > now()` and the holder is the wedged replica, proceed.

Step 2. Force-expire the lease by bumping `expires_at` into the past:

```sql
UPDATE basin_catalog.partition_leases
SET expires_at = now() - interval '1 second'
WHERE project_id = '<project_uuid>'
  AND partition_id = '<partition_id>'
  AND holder      = '<wedged_holder>';
```

Step 3. Wait one heartbeat interval (default 5 s) for a healthy peer to
acquire via the normal steal-on-expiry CAS. Confirm:

```sql
SELECT holder, epoch FROM basin_catalog.partition_leases
WHERE project_id = '<project_uuid>'
  AND partition_id = '<partition_id>';
```

`holder` should now be a different replica id and `epoch` should have
incremented by 1.

Step 4. Restart the wedged replica. Its un-wedged heartbeat will hit a
renew failure (`renew_total{result=expired}`) and it will drop its
partition state cleanly.

### When NOT to manually steal

If `basin_lease_renew_total{result=failed}` is rising across **every**
replica, the coordinator Postgres is the failed component, not any one
replica. Don't steal — fix the coordinator. Manual steals against an
unhealthy coordinator cause an `epoch` race that hits the WAL fencing
path harder than necessary.

---

## Failure modes summary

The set of compound failures ADR 0023 makes survivable, plus what
operators observe in each case.

| Failure | Visible signal | Behaviour | Recovery time |
|---|---|---|---|
| **Coordinator Postgres down** | `basin_heartbeat_lag_ms` p99 → timeout; `basin_lease_renew_total{result=failed}` across every replica | Replicas drop partition state on renew failure (conservative); cap consumers fall back to per-process defaults (degraded — no cross-replica aggregation; safe). | Until coordinator restored + one heartbeat round (≤ 5 s). |
| **Single replica killed** | `basin_lease_holdings_total{replica=dead}` flatlines; peers' `basin_lease_acquire_total{result=stolen}` rises | Lease TTL expires; peers steal via CAS; partition state cold-loads on the new owner. | ≤ TTL (default 15 s) + cold-load time. |
| **Network partition (dual-leaseholder)** | Two replicas report `basin_lease_holdings_total ≥ 1` for the same `(project, partition)` for a brief window | The lower-epoch replica's WAL appends are rejected at the WAL layer. Correctness preserved; up to one heartbeat interval of writes on the partitioned side are lost. | One heartbeat interval (5 s). |
| **Heartbeat budget over-cap (whale spraying)** | `basin_budget_over_cap_seconds_total{project=whale}` rises monotonically | The over-cap window is bounded by one heartbeat interval × scheduler slack. Hard caps set the slice conservatively. | Self-healing; only billing is exposed (charged actual usage). |
| **Stuck lease (replica hung but alive)** | One replica's renews continue but writes stall; other replicas can't take over | TTL expires once the renew thread fails; peer steals. If SLA can't wait, see manual-steal procedure above. | ≤ TTL or operator-driven (immediate). |

---

## Cross-references

- [ADR 0023 — Lease-based ownership + partition-level routing + heartbeat budgets](../decisions/0023-leases-and-partition-routing.md) — full architectural rationale.
- [ADR 0010 — Catalog replication](../decisions/0010-catalog-replication.md) — the global Postgres the lease table lives in.
- [Multi-project SaaS on Basin](../multi-project.md) — partition keying conventions for whales.
- [`docs/audits/2026-05-21-noisy-neighbor-fairness.md`](../audits/2026-05-21-noisy-neighbor-fairness.md) — the audit that drove ADR 0023.
