---
title: "Failover runbook — node loss and recovery"
nav_section: operations
sidebar_position: 56
summary: "Operator runbook for losing and recovering a Basin node: single-writer (default), raft-mode 3-node setup, leader-loss behavior, split-brain guards, and cross-references."
tags: [operations, failover, recovery, wal, leases, raft, runbooks]
---

# Failover runbook — node loss and recovery

What to do when a `basin-server` process or its machine dies, what
recovery actually does, and what you must not do.

Two deployment shapes are supported as of the multi-node commit. The
**single-writer** shape remains the default. The **raft-mode** 3-node
shape is now shippable but carries v1 caveats (see below). Pick the
section that matches your deployment.

> **Which shape are you running?** If `BASIN_WAL_MODE` is unset or
> `local`, you are in single-writer mode — skip to [single-writer
> procedures](#single-writer-shape-default). If `BASIN_WAL_MODE=raft`,
> read the [raft-mode section](#raft-mode-3-node-deployment) first.

Read [durability.md](./durability.md) for what each ack means; this
runbook assumes that vocabulary.

---

## Single-writer shape (default)

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

## Split-brain: the rule and why (single-writer)

**Never run two `basin-server` writer processes against the same
catalog + bucket + WAL prefix.**

In single-writer mode (`BASIN_WAL_MODE=local`, the default) there is
**no enforcement** stopping you:

- No lease is acquired or checked at startup or on write —
  `basin-server` only wires the lease registry when
  `BASIN_LEASE_MODE=required`. Without that knob the per-partition epoch
  fence in the WAL never rejects anything (no-lease appends pass
  `epoch = None` and are accepted unconditionally —
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
- In raft mode the raft leader IS the write fence — a non-leader write
  is refused before it reaches the WAL, so the split-brain class changes
  character: see [raft-mode split-brain](#raft-mode-split-brain).

---

## Failure modes summary (single-writer)

| Failure | Impact | Recovery | Bound |
|---|---|---|---|
| Process crash, node intact | Connections drop; async-window writes lost | Supervisor restart + automatic WAL replay | Seconds + replay of ≤ ~30 s tail |
| Node loss, WAL on S3 | As above | New node, same env | ≤ 200 ms of async INSERT acks + hot-tier overlay window |
| Node loss, WAL on lost local volume | Un-compacted tail gone | New node; data = last compaction | ≤ ~30 s of INSERT acks (including sync-commit), + overlay window |
| Catalog Postgres down | All queries needing catalog fail; engine can't commit compactions | Restore/failover the Postgres (it's a standard PG) | Your PG HA story |
| Object store down | Reads degrade to cache; WAL flushes + compactions retry and stall | Wait it out — flusher and compactor retry every tick; sync-commit waiters block rather than ack | Provider outage window |
| Two writers started (split-brain) | Duplicate/conflicting rows, interleaved WAL | Stop one immediately; audit duplicates (restore.md verification); per-table snapshot rollback if needed | Manual |

---

## Raft-mode 3-node deployment

`BASIN_WAL_MODE=raft` (requires `BASIN_SHARD_ENABLED=1`) replaces the
local file-backed WAL with a quorum-replicated WAL via `openraft`. Every
`append` blocks until the log entry is committed by a majority (2 of 3);
`durable_lsn` advances on raft commit rather than on a local fsync
watermark. The raft log, vote, and manifest snapshot persist under
`${BASIN_WAL_DIR}/raft` — a node that restarts against the same
directory rejoins with its hard state intact.

Source: `crates/basin-wal/src/raft_wal.rs` (module header + `RaftWal`
impl) and `services/basin-server/src/main.rs` (`build_raft_wal`).

### Transport security

- **Default: plaintext gRPC for a private cluster network.** The tonic
  transport (`crates/basin-wal/src/raft_net`) defaults to plaintext on
  the assumption that the raft port is reachable only on a private VPC /
  internal mesh (e.g. fly.io 6PN). `BASIN_RAFT_BIND` must not be exposed
  publicly in this mode. The openraft `Vote` in every RPC fences stale
  leaders at the protocol layer.
- **Mutual TLS (opt-in) removes the plaintext caveat.** Set
  `BASIN_RAFT_TLS_CERT` / `BASIN_RAFT_TLS_KEY` / `BASIN_RAFT_TLS_CA` on
  every node to run the transport over rustls with mutual authentication:
  each node presents a cluster-CA-signed leaf and requires its peers to
  do the same, giving confidentiality plus peer auth (a node that cannot
  prove cluster membership cannot speak raft). All three vars are required
  together — a partial config is a **startup error** (no silent plaintext
  fallback). See the [mTLS setup](#raft-mode-mtls-setup) below. Verified by
  `crates/basin-wal/tests/raft_net_tls.rs` (a 3-node TLS cluster elects +
  replicates).

### v1 caveats (honest bar)

- **Single raft group per region.** Basin runs one independent raft group
  per region — there is deliberately no cross-region quorum
  ([ADR 0009](../decisions/0009-multi-region-architecture.md)). A project's
  `home_region` selects which region's raft group owns its writes
  (`basin_common::raft_group_for`); a write to a non-home region is
  forwarded or rejected at the engine's region gate, never cross-region
  committed. Setting up two regional clusters and the cross-region link is
  a deployment exercise (independent `BASIN_RAFT_PEERS` per region + S3
  cross-region replication of the bucket); the routing decision itself is
  unit-tested (`basin_common::region` tests).
- **Bootstrap-once semantics.** Set `BASIN_RAFT_BOOTSTRAP=1` on exactly
  one node for the initial cluster bring-up. On restart of the bootstrap
  node `initialize` is skipped when the raft log is non-empty
  (`build_raft_wal` gates on `last_log_index == 0 && commit_index == 0`).
  Do not set `BASIN_RAFT_BOOTSTRAP=1` on more than one node in the same
  cluster — you will initialize two disjoint clusters.
- **`GET /admin/v1/cluster` is not yet wired.** The `RaftWal::cluster_status()`
  surface exists and the startup log emits a full cluster status line; the
  HTTP route is a documented seam in `main.rs` awaiting a follow-up PR.
  Use the startup log for cluster observability for now.
- **Per-project counters not wired through `RaftWal::append`.** The
  `attach_project_counters` impl is a no-op; per-project metrics in raft
  mode are a follow-up.

### 3-node setup walkthrough

Pick three machines (or Fly machines) in the same region on a private
network. Assign each a stable raft node id (positive `u64`; node ids 1,
2, 3 work fine). Choose their raft gRPC listen addresses — these must be
reachable among the three nodes but need not be reachable by clients.

```text
Node 1: BASIN_NODE_ID=1  BASIN_RAFT_BIND=10.0.0.1:6010
Node 2: BASIN_NODE_ID=2  BASIN_RAFT_BIND=10.0.0.2:6010
Node 3: BASIN_NODE_ID=3  BASIN_RAFT_BIND=10.0.0.3:6010
```

Required env on every node (`BASIN_RAFT_PEERS` is the same string on
all three):

```sh
BASIN_SHARD_ENABLED=1
BASIN_WAL_MODE=raft
BASIN_RAFT_PEERS=1@10.0.0.1:6010,2@10.0.0.2:6010,3@10.0.0.3:6010
```

Start order:

1. Start node 1 **with** `BASIN_RAFT_BOOTSTRAP=1`. It initializes the
   cluster with all three peers as voters and elects itself leader.
2. Start nodes 2 and 3 **without** `BASIN_RAFT_BOOTSTRAP`. They contact
   node 1 and catch up via raft log replication.
3. Confirm in the startup logs: look for `raft cluster status` lines on
   each node with `role=leader` (once) or `role=follower` (twice) and
   `members=3`.

**Rehearse it on one box first.** `scripts/raft-cluster-smoke.sh` brings up
a real 3-node cluster as three `basin-server` processes on distinct
ports + data dirs (the same env wiring as a 3-machine deploy, on
`127.0.0.1`), waits for readiness, then runs the end-to-end smoke: write
to the leader, read the replicated row from a follower, kill the leader,
confirm a new leader takes over and the pre-failover write survived. Run
it in CI or before a production bring-up to validate the binary + env
matrix. (Localhost cannot model a real cross-machine partition — the
in-process drills in `raft_failover_drills.rs` cover that.)

In production, route pgwire write traffic to the current leader. Reads
can land anywhere (followers serve reads from their replicated state
machine). The router re-resolves the leader on a `LeaseNotHeld` /
`RaftNoQuorum` (SQLSTATE 40001) response and retries.

### <a name="raft-mode-mtls-setup"></a>mTLS setup (optional, recommended off private net)

If the raft port is reachable from anything outside a fully trusted
cluster network, enable mutual TLS. You need a small private CA (a
self-managed CA, step-ca, cert-manager, or even `openssl`/`rcgen` for a
lab) and one leaf cert + key per node, each signed by that CA.

Each leaf cert must carry the verification hostname as a SAN. The client
verifies the peer cert against this name; it defaults to `basin-raft` and
is overridable with `BASIN_RAFT_TLS_DOMAIN`. Include the node's reachable
IP as an IP SAN as well if peers dial by IP.

Set on **every** node (all three required together, or startup fails):

```sh
BASIN_RAFT_TLS_CERT=/etc/basin/raft/node.crt   # this node's leaf cert (PEM)
BASIN_RAFT_TLS_KEY=/etc/basin/raft/node.key    # this node's private key (PEM)
BASIN_RAFT_TLS_CA=/etc/basin/raft/ca.crt       # cluster CA bundle (PEM)
# BASIN_RAFT_TLS_DOMAIN=basin-raft             # optional; SAN the leaf carries
```

With TLS on, a peer URI in `BASIN_RAFT_PEERS` written as a bare
`host:port` (or `http://…`) is dialed over `https://` automatically — you
do not need to rewrite the peer list. Confirm at startup: the transport
logs `raft transport listening (mutual TLS)` instead of
`(plaintext; private network assumed)`. A node presenting no client cert,
or one signed by a different CA, is rejected at the TLS handshake and
never reaches the raft protocol.

Source: `crates/basin-wal/src/raft_net/tls.rs`; verified by
`crates/basin-wal/tests/raft_net_tls.rs`.

### What changes vs. single-writer

| Aspect | Single-writer (`local`) | Raft mode |
|---|---|---|
| Write fence | None (local node always accepts) | Raft leadership — non-leader writes refused before reaching the WAL |
| Durability boundary | Local fsync (or S3 PUT) on this node | Quorum commit: 2 of 3 nodes must persist the entry |
| `durable_lsn` advances when | WAL segment PUT succeeds | Raft `client_write` returns (quorum committed) |
| `BASIN_WAL_BACKEND` | Selects the local file-WAL store | Ignored in raft mode — raft manages its own persistence under `${BASIN_WAL_DIR}/raft` |
| Node loss | Restart same node with same env | Cluster stays available (2 of 3 survive); replace lost node per procedure below |
| Split-brain guard | None in local mode | Raft leader election is the guard; see [raft-mode split-brain](#raft-mode-split-brain) |
| Hot-tier overlay window | Unchanged: ~15–60 s unguarded | Unchanged: overlay is not WAL-logged in either mode |

### Leader-loss behavior (verified by drills)

The failover drills in
`crates/basin-wal/tests/raft_failover_drills.rs` run against real
disk-backed `RaftWal` nodes with a `SimCluster` in-process mesh. Every
drill carries an `AckedSet` zero-data-loss invariant: every payload whose
`append` returned `Ok` must be present in the surviving cluster after the
drill. The drills cover the behaviors operators can rely on:

1. **Leader killed → new leader elected → writes continue, none lost.**
   (`drill_kill_leader_elects_new_and_continues_writes`) Writes that
   were acked before the kill are present on the surviving leader after
   election. Default election timeout: 1 500 ms (1 500–3 000 ms
   randomized; `election_timeout_ms = 1500`,
   `election_timeout_max = 2 × min`). Expect writes to block for up to
   one election timeout (~1.5–3 s) then resume on the new leader.

2. **Killed node restarts → catches up → write set identical.**
   (`drill_restart_killed_node_catches_up`) A node that was down while
   the rest of the cluster wrote entries rejoins and replicates the
   missing log via raft log replication or snapshot install. The
   recovered node's state is byte-identical to the acked set.

3. **Partitioned follower → cluster healthy, partitioned node rejects
   writes.** (`drill_partitioned_follower_rejected_cluster_healthy`) A
   follower that cannot reach the leader cannot win an election (no
   quorum), so its `append` returns the typed retryable error rather
   than silently committing a divergent entry. The remaining two nodes
   (leader + one follower) continue to commit writes normally.

4. **Non-leader write refused with typed not-leader error + leader
   hint.** (`drill_non_leader_write_redirect_contract`) A follower's
   `append` returns `BasinError::LeaseNotHeld` (SQLSTATE 40001) with a
   message containing `"not raft leader"` and a `"leader hint"` pointing
   at the current leader. The router uses this hint to re-resolve and
   retry.

5. **Leader killed UNDER SUSTAINED WRITE LOAD → stream resumes, none
   lost.** (`drill_kill_leader_under_sustained_write_load`) A background
   writer streams appends continuously; the leader is killed mid-stream so
   the election races live traffic. After failover the stream resumes on
   the new leader (strictly more acks than the warm-up set) and every
   acked write — including those acked during the chaos window — survives.
   This is the strongest zero-loss proof: in-flight-but-unacked writes may
   be rejected (the client retries), but no acked write is ever lost.

6. **Slow (lagging) follower does not stall commits.**
   (`drill_slow_follower_does_not_stall_commits`) With one follower down,
   a burst of writes each commit on the 2-node quorum within a tight
   per-append budget — proving the quorum commit never blocks waiting on
   the lagging replica. The laggard catches the full set up once healed.

7. **Partition + heal under load.**
   (`drill_partition_and_heal_under_load`) A follower is partitioned off
   mid-stream, the majority keeps committing under load, then the follower
   is healed while writes continue. The healed node converges to the full
   acked set.

8. **Snapshot-streaming follower catch-up past the purge floor.**
   (`drill_snapshot_streaming_follower_catch_up`) A follower misses the
   window in which the leader runs `record_flush_watermark` (snapshot +
   log purge), so the early log entries are gone. When it rejoins it can
   only be caught up by `install_snapshot` — and it ends with the
   identical committed write set. The raft snapshot the WAL builds is
   self-contained: it serialises the full applied state machine plus the
   manifest pointer, so the WAL follower rebuilds the entire committed
   write set from the snapshot alone (no object-store fetch). The
   `catalog_snapshot_id` in the pointer is the seam the **engine** layer
   uses to rebuild flushed *table* state from S3 — that fetch is above the
   WAL and out of scope of this drill.

### Procedure: replace a lost node (raft mode)

A raft cluster of 3 can absorb the loss of 1 node and remain available
(majority = 2). Recovery does not require stopping the cluster.

1. **Confirm the lost node is gone.** The surviving two nodes continue
   to commit writes; verify with the startup log's cluster-status lines
   (`role=leader` / `role=follower` on the two survivors, `members=3`
   still — raft remembers the membership configuration).
2. **Provision a replacement** on a new machine with the same
   `BASIN_NODE_ID` as the lost node and the same `BASIN_RAFT_PEERS` and
   `BASIN_RAFT_BIND`. Give it a **fresh** `BASIN_WAL_DIR` (or the
   original directory if the volume survived). Do **not** set
   `BASIN_RAFT_BOOTSTRAP=1` — the cluster is already initialized.
3. **Start the replacement.** It contacts the leader, learns the current
   membership, and replicates the log from the snapshot forward. This is
   automatic; no operator command is needed.
4. **Verify.** Watch for `role=follower` in the replacement's startup
   log and `members=3` on all surviving nodes.

If the replacement has a non-empty raft log from the original volume,
it rejoins cleanly — `initialize` is skipped (non-empty log), and it
catches up any missing tail via the leader.

### Procedure: planned restart (raft mode)

1. Quiesce writes to the node you are restarting (drain the LB for that
   node, or let the router re-direct to the leader after `LeaseNotHeld`).
2. `SIGINT` the process — shutdown calls `wal.close()` which calls
   `raft.shutdown()` cleanly. Pending writes that were already
   quorum-committed survive; in-flight async INSERT acks not yet
   quorum-committed are lost per the same windows as single-writer.
3. Start; the node rejoins with its hard state from `${BASIN_WAL_DIR}/raft`.
4. Verify `role=follower` in the startup log.

Quiescing writes is not required for availability (the other two nodes
keep quorum), but it bounds client-visible errors to the restart window
for connections pinned to this node.

### <a name="raft-mode-split-brain"></a>Raft-mode split-brain

Raft's leader election is the structural split-brain guard: only the
leader can commit entries. A non-leader write is refused before it
reaches the WAL (`RaftWal::append` checks `is_leader()` first, then
`client_write` additionally rejects it at the protocol level).
`map_client_write_err` in `raft_wal.rs` maps `ForwardToLeader` to the
retryable `RaftNoQuorum` — always a typed `Err`, never a silent commit.

The residual risk is the brief window between a leader being deposed
and learning it is deposed (one heartbeat interval, 500 ms default):
`is_leader()` reads local raft metrics and may briefly return `true` on
a stale leader. The `client_write` call downstream then fails with
`ForwardToLeader` (the raft protocol rejects it), so the window is
safe — a stale-leader write still fails closed.

Operationally: in raft mode you do NOT need to manually fence the old
machine before starting the replacement (the raft protocol does it).
The single-writer rule ("destroy the old machine first") still applies
to `BASIN_WAL_MODE=local`.

---

## Leases (BASIN_LEASE_MODE=required)

`BASIN_LEASE_MODE=required` wires the catalog's `LeaseRegistry` into
the shard: each partition acquires a Postgres CAS lease before writes
are accepted, heartbeating every `BASIN_LEASE_RENEW_SECS` (default 5 s)
against a `BASIN_LEASE_TTL_SECS` TTL (default 15 s). A write that
arrives at a process without a held lease returns `LeaseNotHeld` (SQLSTATE
40001, retryable) immediately. Reads continue throughout.

In `BASIN_WAL_MODE=raft`, raft leadership is the write fence and
**supersedes the lease**: a non-leader write is refused by the raft
check before the lease is ever consulted. Setting both knobs is allowed
— `basin-server` logs that the lease registry is wired but redundant.
The epoch fence in the WAL remains correct in both modes
(`services/basin-server/src/main.rs`, lease-mode wiring block).

`BASIN_LEASE_MODE=off` (the default) is the unchanged single-replica
behaviour.

---

## Failure modes summary (raft mode)

| Failure | Impact | Recovery | Bound |
|---|---|---|---|
| Leader crash, 2 nodes surviving | Writes pause for election | Automatic re-election (~1.5–3 s) | Election timeout window |
| 1 node lost (any role) | Cluster available at reduced redundancy | Replace node per procedure above; no write pause | Minutes to rejoin |
| 2 nodes lost simultaneously | Cluster loses quorum; writes block | Restore 1 node; elect leader; resume | Time to provision + election |
| Node restart (same volume) | Brief follower absence | Automatic rejoin + log catchup | Seconds |
| Network partition (1 node isolated) | Partitioned node refuses writes; main cluster continues | Heal partition; node catches up | Partition duration |
| Hot-tier overlay window | Unchanged: point UPDATE/DELETE not WAL-logged | Same mitigation as single-writer: quiesce + wait ~60 s before stop | ~15–60 s |
| Catalog Postgres down | All queries needing catalog fail | Failover the Postgres | Your PG HA story |

---

## Cross-references

- [durability.md](./durability.md) — ack semantics and loss windows; raft-mode row in the durability ladder.
- [restore.md](./restore.md) — when node loss escalates to data restore.
- [deployment.md](../deployment.md) — env table including raft/lease/node knobs.
- [operators/lease-ownership.md](../operators/lease-ownership.md) — the ADR 0023 lease system.
- [operators/storage.md](../operators/storage.md) — object-store incident handling.
- [ADR 0023](../decisions/0023-leases-and-partition-routing.md), [ADR 0020](../decisions/0020-wal-transaction-markers.md), [ADR 0009](../decisions/0009-multi-region-architecture.md).
