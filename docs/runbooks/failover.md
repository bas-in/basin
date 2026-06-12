---
title: "Failover runbook — node loss and recovery (single-writer)"
nav_section: operations
sidebar_position: 56
summary: "Operator runbook for losing and recovering a Basin node on today's single-writer deployment shape: detection, restart, WAL replay expectations, split-brain risk, and the lease/Raft roadmap."
tags: [operations, failover, recovery, wal, leases, runbooks]
---

# Failover runbook — node loss and recovery

What to do when a `basin-server` process or its machine dies, what
recovery actually does, and what you must not do (run two writers).

> **Deployment reality check.** The shipped `basin-server` binary is a
> **single-writer** process per catalog + bucket + WAL prefix. The
> lease-based multi-replica ownership system (ADR 0023) exists in the
> crates and is exercised by tests, but `basin-server` does **not** wire
> a lease registry today (`ShardConfig::new` leaves
> `lease_registry: None`; nothing in `services/basin-server/src/main.rs`
> calls `with_lease_registry`). The WAL's epoch fencing is therefore
> dormant in production: every append runs in no-lease mode
> (`epoch = None`) and is accepted unconditionally. "Failover" today
> means **restart or replace the one writer** — not promote a standby.

Read [durability.md](./durability.md) for what each ack means; this
runbook assumes that vocabulary.

---

## Detection

| Signal | Check |
|---|---|
| Process death | Supervisor (systemd / Fly machine state); `basinctl ping "$BASIN_URL"` fails to connect |
| Hung process | `basinctl ping` connects but `SELECT 1` stalls; pgwire accepts but queries time out |
| Partial degradation | Object-store errors in logs (`wal segment put`, storage PUT failures) — writes stall or error while the process lives; see the [storage runbook](../operators/storage.md) |

`basinctl ping` is the cheapest end-to-end probe (TCP + auth + `SELECT 1`):

```sh
basinctl ping "postgres://<user>@<host>:5433/basin"   # prints "OK in <ms>"
```

---

## What recovery does (so you know what to expect)

On every `basin-server` start with `BASIN_SHARD_ENABLED=1`:

1. **WAL open** (`LocalWal::open` → `recover_partitions`,
   `crates/basin-wal/src/file_wal.rs`): lists every `*.seg` under the
   WAL prefix, decodes each segment, and rebuilds per-
   `(project, partition)` state — `next_lsn` and the `durable_lsn`
   watermark are seeded from the recovered segments. Cost: one GET per
   segment; segments are ≤ ~1 MiB, and the WAL is truncated after every
   compaction (30 s cadence), so a healthy deployment recovers a small
   tail.
2. **Shard cold-load** (`replay_wal_into`,
   `crates/basin-shard/src/in_process.rs`): on first access of a
   partition, all surviving WAL entries replay from `Lsn::ZERO` into the
   in-memory tail. Reads merge that tail with the columnar base; the
   compactor re-commits it to data files on its next tick.
3. **Catalog + data files** need no recovery — they are the durable
   source of truth (Postgres catalog + object store).

No operator command is involved. The first query against a partition
pays the replay cost; everything after is normal.

### What survives a crash, what doesn't

| State | Crash (SIGKILL / OOM / power*) | Clean stop (SIGINT) |
|---|---|---|
| Compacted data (columnar files + catalog) | ✅ survives | ✅ survives |
| WAL-flushed INSERT tail (closed segments) | ✅ survives, replays | ✅ survives, replays |
| Async-commit INSERT acks not yet flushed (≤ 200 ms / 1 MiB window) | ❌ lost | ✅ flushed on shutdown (`wal.close()` drains) |
| `synchronous_commit = on` INSERT acks | ✅ survive (ack was after segment PUT + fsync) | ✅ survive |
| Hot-tier point UPDATE/DELETE overlays not yet reconciled (~15–60 s window) | ❌ lost | ❌ **not drained by shutdown either** — quiesce ≥ 60 s before stopping to bound this |
| Open transactions | rolled back (replay discards crash-mid-tx WAL markers per ADR 0020) | n/a |
| In-memory catalog (`BASIN_CATALOG=memory`) | ❌ everything lost | ❌ everything lost |

\* power loss additionally exposes the local-FS no-fsync caveat for
*data* PUTs — see [durability.md](./durability.md#power-loss-caveat-on-local-fs-data-storage).

---

## Procedure: process crash, machine intact

1. Restart the process (supervisor usually already did). Same env, same
   volumes.
2. Watch startup logs for `storage object-store backend configured`,
   `WAL object-store backend configured`, `shard owner enabled`.
3. Probe: `basinctl ping`, then a read of a recently-written table.
4. If the crash was mid-write, run the duplicate-PK check from
   [restore.md → verification](./restore.md#verification) on hot tables
   (the WAL-truncate crash window can double-apply one small batch).
5. Re-open traffic.

Expected client impact: connection drop; loss limited to the async
windows in the table above.

## Procedure: planned restart

<a name="planned-restart"></a>

1. Quiesce writes (drain the LB / stop the app).
2. **Wait ≥ 60 s** — one shard compaction interval (30 s) drains the WAL
   tail; the overlay reconciler (15 s age trigger on a 5 s tick) drains
   hot-tier point mutations. This is the step that protects the
   non-WAL-backed overlay writes; `SIGINT` alone does not drain them.
3. `SIGINT` the process — the shutdown path stops background loops and
   flushes the WAL buffer (`services/basin-server/src/main.rs`).
4. Do maintenance; start; verify; re-open.

## Procedure: machine / volume loss (object store is source of truth)

Scenario: the node is gone for good. Recovery = start a replacement
writer against the surviving durable state.

1. **Inventory what survived:**
   - Catalog Postgres (external) — survives node loss by construction.
   - Data bucket (`BASIN_STORAGE_BACKEND=s3|tigris`) — survives.
   - WAL: survives **only if** `BASIN_WAL_BACKEND=s3|tigris`, or the
     local `BASIN_WAL_DIR` volume can be re-attached. A lost local WAL
     volume loses the un-compacted tail: worst case, writes acked since
     the last compaction tick (≤ ~30 s of INSERTs), including
     `synchronous_commit = on` acks — sync commit makes the **WAL**
     durable, and if the WAL device itself is gone, so is its tail.
2. **Make sure the old node is dead** (machine deleted / fenced at the
   infra layer). This is a hard requirement — see split-brain below.
3. Provision the replacement with the **same env**: `BASIN_CATALOG`,
   `BASIN_STORAGE_BACKEND` + prefix, `BASIN_WAL_BACKEND` + prefix /
   re-attached `BASIN_WAL_DIR` volume, `BASIN_SHARD_ENABLED=1`.
4. Start; recovery is automatic (section above).
5. Verify per [restore.md](./restore.md#verification); re-open traffic
   (repoint DNS / LB at the new node).

If the catalog Postgres itself is lost too, this becomes a full restore
— switch to [restore.md](./restore.md#procedure-full-restore-disaster-recovery).

Hardening for this scenario, in order of value:

1. `BASIN_WAL_BACKEND=s3` — the WAL tail survives any node loss; the
   remaining loss is the in-RAM async window (≤ 200 ms) plus the
   hot-tier overlay window (which no WAL placement covers).
2. External, HA catalog Postgres (managed PG with PITR).
3. Bucket versioning (already on the [deployment checklist](../deployment.md)).

---

## Split-brain: the rule and why

**Never run two `basin-server` writer processes against the same
catalog + bucket + WAL prefix.**

Today there is **no enforcement** stopping you:

- No lease is acquired or checked at startup or on write —
  `basin-server` constructs the shard without a lease registry, so the
  per-partition epoch fence in the WAL never rejects anything (no-lease
  appends pass `epoch = None` and are "accepted unconditionally" —
  `crates/basin-wal/src/file_wal.rs`).
- Two writers will both recover the same WAL high-water mark and then
  **assign overlapping LSNs** to different entries, interleave segments
  under the same prefix, double-commit catalog snapshots from their
  independent compactors, and produce duplicate or conflicting rows.
  The catalog's optimistic concurrency protects single snapshots from
  torn commits, not your data from two engines that each believe they
  own the tail.

Operational corollaries:

- During machine replacement, confirm the old machine is destroyed or
  network-fenced **before** starting the new one. An orchestrator that
  starts the replacement while the old VM is merely unreachable is the
  classic trigger.
- Blue/green deploys must hand over the WAL prefix strictly serially:
  stop old (clean shutdown), then start new.
- Read scale-out is a different topic — see
  [scaling/read-replicas.md](../scaling/read-replicas.md); this rule is
  about **writers**.

---

## Failure modes summary

| Failure | Impact | Recovery | Bound |
|---|---|---|---|
| Process crash, node intact | Connections drop; async-window writes lost | Supervisor restart + automatic WAL replay | Seconds + replay of ≤ ~30 s tail |
| Node loss, WAL on S3 | As above | New node, same env | ≤ 200 ms of async INSERT acks + hot-tier overlay window |
| Node loss, WAL on lost local volume | Un-compacted tail gone | New node; data = last compaction | ≤ ~30 s of INSERT acks (including sync-commit), + overlay window |
| Catalog Postgres down | All queries needing catalog fail; engine can't commit compactions | Restore/failover the Postgres (it's a standard PG) | Your PG HA story |
| Object store down | Reads degrade to cache; WAL flushes + compactions retry and stall | Wait it out — flusher and compactor retry every tick; sync-commit waiters block rather than ack | Provider outage window |
| Two writers started (split-brain) | Duplicate/conflicting rows, interleaved WAL | Stop one immediately; audit duplicates (restore.md verification); per-table snapshot rollback if needed | Manual |

---

## Forward-looking: multi-replica failover (not shipped in the binary)

The architecture for real failover exists and is tested at the crate
level — operators should know the shape, and should not mistake it for
something they can enable today:

- **Leases + epoch fencing (ADR 0023).** `basin-shard` supports a
  Postgres-backed `LeaseRegistry` (`ShardConfig::with_lease_registry`):
  replicas acquire per-`(project, partition)` leases, heartbeat-renew
  every `BASIN_LEASE_RENEW_SECS` (default 5 s), and lose state on a
  failed renew; peers steal on expiry after `BASIN_LEASE_TTL_SECS`
  (default 15 s). Every fenced WAL append carries the holder's epoch and
  stale-epoch appends are rejected — that is the dual-writer guard the
  single-writer deployment lacks. The multi-process behaviour is
  validated by the harness in
  [operators/multi-instance-test.md](../operators/multi-instance-test.md),
  and day-2 ops for the lease system are pre-written in
  [operators/lease-ownership.md](../operators/lease-ownership.md).
  **Gap:** `basin-server` does not yet construct the registry or expose
  an env switch for it.
- **Raft WAL.** `RaftWal` (openraft) implements the same `Wal` trait
  with quorum-ack durability, but only inside a **single-process
  simulation cluster** (`SimCluster`) — 3-node, leader-failure, and
  quorum-commit tests pass; cross-process networking and persisted Raft
  state are v0.2 (`crates/basin-wal/RAFT.md`, `CAPABILITIES.md`). It is
  not a production durability boundary today.
- **Multi-region** stays "by deployment, not by code" —
  [ADR 0009](../decisions/0009-multi-region-architecture.md) locks
  regional Raft WALs + S3 CRR as the direction;
  [ADR 0010](../decisions/0010-catalog-replication.md) covers the
  catalog side.

When the lease wiring ships in `basin-server`, the failover story
upgrades from "restart the single writer" to "peers steal expired
leases within TTL" and the lease-ownership runbook becomes the
operative document. Until then, this runbook is.

---

## Cross-references

- [durability.md](./durability.md) — ack semantics and loss windows.
- [restore.md](./restore.md) — when node loss escalates to data restore.
- [operators/lease-ownership.md](../operators/lease-ownership.md) — the ADR 0023 system (crate-level today).
- [operators/storage.md](../operators/storage.md) — object-store incident handling.
- [ADR 0023](../decisions/0023-leases-and-partition-routing.md), [ADR 0020](../decisions/0020-wal-transaction-markers.md), [ADR 0009](../decisions/0009-multi-region-architecture.md).
