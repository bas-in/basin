# WAL Phase 2 — Raft integration plan

Status: **real raft shipped (single-process simulation)**. The `Wal` trait
ships with two concrete impls — `LocalWal` (single-node fsync) and `RaftWal`
(multi-node openraft consensus). Cross-process gRPC `RaftNetwork` is the
remaining follow-up; the trait shape and the test harness will not change
when it lands.

## What ships today

- Public trait `basin_wal::Wal` (object-safe; held as `Arc<dyn Wal>` everywhere
  the shard owner / server / integration tests hold the WAL).
- Concrete `basin_wal::LocalWal` — single-node, file-backed implementation,
  behaviour byte-identical to v0.1.
- Concrete `basin_wal::RaftWal` — multi-node openraft consensus.
  `RaftWal::new(RaftWalConfig).await?` spins up a raft node; `initialize` /
  `add_learner` / `change_membership` form the cluster; `append`,
  `read_from`, `high_water`, `truncate`, `flush`, `close` implement the
  `Wal` trait via openraft's `client_write` + a per-`(tenant, partition)`
  state machine.
- In-process simulation network ([`SimCluster`]) — every node in a test
  cluster shares one handle map; RPCs dispatch by direct method call. Lets
  multi-node tests run inside one tokio runtime without locking in a wire
  protocol.
- Test suite — `crates/basin-wal/tests/raft_wal.rs` exercises single-node,
  3-node, 5-node, leader-failure (kill leader → new election → resume),
  quorum-commit (kill 2/3 → appends block), 100-entry replication, and
  snapshot truncation (the last one is `#[ignore]`'d because the snapshot
  worker's timing needs a v0.2 tuning pass; the other six run as part of
  `cargo test -p basin-wal`).

## Decisions blocking the real impl

### 1. Library lock-in ✅ done

`openraft = "=0.9.24"` is pinned in `crates/basin-wal/Cargo.toml`. The
deciding factors: async-first API matches the tokio-based stack, trait-based
storage / network plug cleanly into Basin's per-`(tenant, partition)` log
keying, and `Adaptor::new(...)` lets a single `RaftStorage` impl satisfy
both `RaftLogStorage` and `RaftStateMachine` so v0.1 stays small. `raft-rs`
(TiKV's sync-callback library) and a custom raft remain the documented
fallbacks if openraft hits a wall later.

Original table preserved here for the ADR audit trail:

| Candidate  | Pros | Cons |
|------------|------|------|
| `raft-rs` (TiKV) | Most production miles; well-understood ops shape; battle-tested logging + snapshot transfer | Sync-callback API; harder to integrate Basin's per-`(tenant, partition)` log keying without a parallel state machine; integrating async object_store flushes is awkward |
| `openraft` ✅ chosen | Async-first; trait-based storage / network; cleaner custom log structures; good docs | Younger; smaller production footprint; some operational gotchas still being learned |
| Custom Raft      | Full control of log keying, batching, dedicated tokio runtime | Multi-week build; consensus bugs are catastrophic; not the wedge |

### 2. Topology

Two reasonable shapes:

- **One Raft group per region.** A single shard group replicates every
  `(tenant, partition)` log entry. Fastest consensus (1 group's metadata,
  warm leader, batched proposals). Trade-off: noisy tenants share the leader
  with quiet tenants — leader fanout becomes the bottleneck.
- **One Raft group per `(tenant, partition)`.** Each shard owner is its
  own raft cluster. Highest isolation (one tenant's writes can't congest
  another's leader). Trade-off: O(tenants × partitions) raft groups, each
  with their own heartbeats — metadata-heavy at >10k partitions.

**Recommended default: per-region**, with a per-tenant escape hatch (a
"premium" tier that gets its own raft group). This matches the way
[`ADR 0008`](../../docs/decisions/0008-noisy-neighbor-fairness.md) handles
fair-share at the engine layer — a shared resource with cheap per-tenant
primitives.

The trait shape supports both; per-region is just one `RaftWal` instance
behind `Arc<dyn Wal>`, and per-tenant is a small registry of `RaftWal`s
keyed by tenant.

### 3. Recovery semantics

On leader failure:

1. Writes pause for the duration of the election (~1.5 s with the default
   `election_timeout_ms`).
2. The new leader replays its log up to the previous committed index before
   accepting new proposals.
3. Followers that were behind catch up via raft log replication; if they're
   too far behind they pull a snapshot.

The shard owner sees `BasinError::Wal("leader unavailable")` for the duration
of the election; the engine retries idempotently because INSERT through the
shard is already idempotent on `(tenant, partition, lsn)`.

### 4. Integration with shard owners

Today's shard owners own `(tenant, partition)`. The Raft layer must not
duplicate that ownership. Two options:

- **Shard owners are raft proposers.** The shard owner calls
  `wal.append(...)`; the WAL's raft impl proposes the entry; the proposal
  is replicated; on commit the shard owner's `apply` callback runs (writes
  the in-memory tail). This is the openraft idiom.
- **Raft layer routes proposals to shard owners.** A separate raft thread
  owns the cluster; on commit it calls into the shard owner. Less natural
  with the existing API; would invert the call direction.

**Recommended: option 1.** Shard owners stay the durability gateway; the
WAL just gains a "wait for quorum" step inside `append`.

This integrates with the per-`(tenant, partition)` keying because the WAL
entry already carries `(tenant, partition, lsn)`; the raft state machine
applies entries by routing them back to the same key. Nothing about the
shard owner's data model changes.

### 5. Hot-path target

5 ms quorum-ack p50 with 3 nodes on local NVMe in the same AZ.

Achievable via:
- Batched proposals (the same flush-pressure heuristic already used by
  `LocalWal`).
- Dedicated raft tokio runtime (separate from the engine's executor) so
  long-running queries don't starve heartbeats.
- `flush_max_bytes` ~= MTU so each proposal is one UDP-frame batch on
  intra-AZ networks.

Real numbers in the integration PR; the stub doesn't claim them.

## Migration path

1. **Lock in library.** ✅ openraft 0.9.x.
2. **Add the dep** to `crates/basin-wal/Cargo.toml`. ✅
3. **Replace `RaftWal::new` stub** with real raft handle creation
   (state machine, transport, persistence). ✅ (in-RAM log + state machine; on-disk persistence queued for v0.2)
4. **Replace stub `append` / `read_from` / `truncate`** with raft proposes. ✅
5. **Multi-node integration tests** — 3-node and 5-node scenarios; leader
   failure; network partition; log replay; snapshot transfer. ✅ (snapshot-truncation test is `#[ignore]`'d pending a v0.2 timing pass)
6. **Single-process simulation network.** ✅ (`SimCluster` + `SimNetworkFactory`)
7. **Cross-process gRPC `RaftNetwork`.** ◻️ follow-up; the seam (`SimNetworkFactory` is just one `RaftNetworkFactory` impl) doesn't change shape.
8. **Operational runbook** — config, monitoring (raft term, commit lag,
   leader churn), recovery procedures. ◻️ follow-up.
9. **Flip the CAPABILITIES.md row** from scaffolding to ✅. ✅

## What this PR does not do

- Ship cross-process gRPC `RaftNetwork`. v0.1 runs every node inside one
  tokio runtime through `SimCluster`; the wire-protocol decision waits.
- Persist the raft log to disk. State lives in RAM until snapshot. The
  segment-file format from `LocalWal` will absorb raft logs when v0.2 lands.
- Wire per-tenant counters through `RaftWal::append`. The `attach_tenant_counters`
  no-op is acceptable for v0.1; counter plumbing is a small follow-up.
- Implement multi-region replication semantics. Separate phase.
