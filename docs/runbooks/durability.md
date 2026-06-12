---
title: "Durability reference — what is durable when"
nav_section: operations
sidebar_position: 54
summary: "Short reference for Basin's durability boundaries: WAL group-commit and durable_lsn, basin.synchronous_commit, FsyncOnPut, hot-tier caveats, and every env knob with its code default."
tags: [operations, durability, wal, fsync, runbooks]
---

# Durability reference — what is durable when

Companion to [restore.md](./restore.md) and [failover.md](./failover.md).
Every claim below is grounded in the current code; file references point
at the implementation so you can verify.

The one-line summary: **the object store + the catalog are the source of
truth; the WAL is the durable hot tail for shard-routed INSERTs; the
hot-tier memtable overlay is not durable until it flushes.**

---

## The write paths and their durability boundaries

Basin has several distinct write paths. Which one a statement takes
decides when its ack means "durable".

| Path | When it runs | Durable at ack? | Loss window on crash |
|---|---|---|---|
| Legacy synchronous INSERT | `BASIN_SHARD_ENABLED` unset/0 (the default) | Yes — Parquet/Vortex PUT + catalog commit before ack | None (see local-FS caveat below) |
| Shard WAL INSERT, async commit | `BASIN_SHARD_ENABLED=1`, `basin.synchronous_commit = off` (default) | **No** — ack from the in-RAM WAL buffer | `min(flush_interval = 200 ms, time-to-1 MiB)` of acked INSERTs |
| Shard WAL INSERT, synchronous commit | `BASIN_SHARD_ENABLED=1`, `basin.synchronous_commit = on` | Yes — ack after the WAL segment PUT (+ fsync on the local backend) | None for acked INSERTs |
| In-transaction DML (`BEGIN … COMMIT`) | Always (shard or not) | Yes at COMMIT — buffered in the session, written as Parquet + catalog commit when COMMIT executes | Uncommitted tx state is lost by design (correct) |
| Hot-tier point UPDATE/DELETE fast path | Single-column-PK point mutations on eligible tables | **No** — written to the in-RAM `MemTableRegistry` only, **no WAL record** | Until the overlay reconciler / hot-tier flush drains it (default triggers: 5 s tick, 15 s age, 8 MiB) |
| Cold-path UPDATE/DELETE | Mutations that miss the fast-path gates (RLS, multi-column PK, secondary indexes, FKs, …) | Yes — copy-on-write Parquet rewrite + catalog commit before ack | None |

Sources: `crates/basin-wal/src/file_wal.rs` (module header documents both
WAL contracts), `crates/basin-engine/src/executor.rs` (shard INSERT
routing, in-tx COMMIT path), `crates/basin-engine/src/dml_mutate.rs`
(`hot_tier_delete_by_pk_tx` doc-comment: "registry-only — no WAL").

---

## WAL durability modes

Two WAL durability backends are selectable via `BASIN_WAL_MODE`:

| Mode | Durability boundary | `durable_lsn` advances when | Crash loss window |
|---|---|---|---|
| `local` (default) | Local fsync (or S3 PUT) on the single writer | WAL segment PUT + optional fsync completes | ≤ 200 ms of async INSERTs; sync-commit acks survive |
| `raft` | Quorum commit — 2 of 3 nodes must persist the entry | `client_write` returns (raft `ForwardToLeader` / `RaftNoQuorum` → SQLSTATE 40001 retryable on failure) | ≤ 200 ms of async INSERTs (quorum acked before raft commit); sync-commit acks survive because `append` blocks until quorum |

In `raft` mode `BASIN_WAL_BACKEND` is **not consulted** for the WAL.
Raft manages its own persistence under `${BASIN_WAL_DIR}/raft` using
`DiskRaftStorage` (same segment framing as the file WAL; fsync'd vote
meta file; manifest-anchored snapshots). The WAL's `flush_interval` and
`flush_max_bytes` constants are also not in play for the raft path:
durability is the raft quorum commit, not a timed flush.

The local-fsync caveat (`FsyncOnPut` for sync-commit, no fsync for async)
applies only to `BASIN_WAL_MODE=local`. See the section below for details.

Source: `crates/basin-wal/src/raft_wal.rs` (`DurabilityBackend` trait
and `RaftDurability::commit_batch`, which blocks on `propose_batch` →
`client_write` → quorum ack), `services/basin-server/src/main.rs`
(`build_raft_wal`, `WalMode` enum).

---

## WAL durability mechanics (local mode)

From `crates/basin-wal/src/file_wal.rs` and `state.rs`:

- **Async append** (`synchronous_commit = off`): the entry is in the
  in-RAM buffer when the ack returns. The background flusher uploads the
  buffer as a closed segment every **200 ms** (`flush_interval`,
  hardcoded in `services/basin-server/src/main.rs`) or when the buffer
  exceeds **1 MiB** (`flush_max_bytes`, also hardcoded).
- **Synchronous append** (`synchronous_commit = on`): the call blocks
  until the partition's published **`durable_lsn`** covers the entry's
  LSN. Concurrent synchronous appends are **group-committed**: every
  append landing within the `commit_delay` window (default **2 ms**,
  `BASIN_WAL_COMMIT_DELAY_MS`) shares one segment PUT and one fsync.
- **`durable_lsn` advances contiguously only.** A segment PUT that
  completes out of order parks in `pending_durable` until the gap below
  it closes (`PartitionState::mark_range_durable`), so an out-of-order
  completion can never ack a waiter whose own segment is still in
  flight.
- **Backend nuance.** An S3-style PUT is durable on success. The local
  filesystem backend's PUT is write-temp + rename with **no fsync** — it
  survives a process crash but not necessarily an OS crash or power
  loss. `basin-server` therefore wraps the local WAL store in
  **`FsyncOnPut`** (`crates/basin-wal/src/fsync.rs`): segments that
  carry at least one synchronous-commit waiter are marked `DurablePut`
  and get `sync_data` on the file plus `sync_all` on the parent
  directory before waiters are released. Async-only segments are never
  fsync'd, so the async path's cost profile is unchanged.
- **WAL replay** happens automatically: `LocalWal::open` lists every
  `*.seg` under the WAL prefix and rebuilds per-`(project, partition)`
  LSN state; the shard replays entries into its in-memory tail on first
  partition access (`replay_wal_into`,
  `crates/basin-shard/src/in_process.rs`). No operator command is
  needed.

### Power-loss caveat on local-FS **data** storage

`FsyncOnPut` wraps the **WAL** store only. The data object store
(`BASIN_STORAGE_BACKEND=local`) is an unwrapped `LocalFileSystem`, so
Parquet/Vortex data files and the catalog commit that references them
are not fsync-gated. On a true power loss (not a process crash), a data
file the catalog already references can be lost from the page cache.
For production-shaped durability run data storage on an S3-compatible
backend (durable on PUT); local FS is dev-grade against power loss.

---

## Hot-tier (memtable) durability caveat

The point UPDATE/DELETE fast paths (Phase 5.14) ack after writing an
override row or tombstone into the in-process `MemTableRegistry`. There
is **no WAL record** for these writes
(`crates/basin-engine/src/executor.rs`, COMMIT arm: "registry-only;
durability comes from the registry's own flush/compaction, NOT the
WAL"). They become durable when:

- the **overlay reconciler** drains them into cold storage
  (`crates/basin-engine/src/overlay_reconcile.rs`; 5 s tick, triggers at
  8 MiB dirty bytes / 15 s oldest-entry age / key-count threshold), or
- the hot-tier flush runs (age threshold 60 s, soft caps — see
  `crates/basin-hottier/src/budget.rs`).

`basin.synchronous_commit = on` does **not** cover this path — it only
gates the shard WAL INSERT append. A crash (and a `SIGINT` shutdown —
the server's shutdown sequence drains the WAL but does not force an
overlay drain, `services/basin-server/src/main.rs`) can lose fast-path
point mutations acked within roughly the last ~15–60 s. To bound this
before planned maintenance, quiesce writes and wait ~60 s (one overlay
age window + one shard compaction interval) before stopping the
process; see [failover.md](./failover.md#planned-restart).

Kill switches if the trade-off is unacceptable for a workload:
`BASIN_HOTTIER_FASTPATH_DISABLE=1` (all hot-tier fast paths off) or
`BASIN_HOTTIER_DELETE_FASTPATH=0` (DELETE only). Mutations then take
the copy-on-write cold path, which is durable at ack.

---

## `basin.synchronous_commit` GUC

Defined in `crates/basin-engine/src/session.rs`.

```sql
SET basin.synchronous_commit = on;   -- ack-after-durable for shard INSERTs
SET basin.synchronous_commit = off;  -- ack-before-durable (the default)
SHOW basin.synchronous_commit;
```

- **Scope:** per session. Read once at session open from the engine-wide
  default; `SET` overrides for the session.
- **Engine-wide default:** `off`, unless `BASIN_SYNCHRONOUS_COMMIT` is
  set to `on`/`true`/`1`/`yes` at server start.
- **Value parsing is strict:** `SET basin.synchronous_commit = onn`
  errors instead of silently downgrading durability (`parse_pg_bool`).
- **Effect:** only on the shard WAL INSERT path. It selects
  `append_fenced_durable` (group commit + fsync) over `append_fenced`.
  It has no effect when `BASIN_SHARD_ENABLED` is off (that path is
  already durable at ack) and no effect on the hot-tier fast paths
  (see caveat above).
- Measured cost: ~2 % added latency on the 10 k bulk-INSERT probe
  (group commit amortizes one fsync per statement group) — see
  `README.md`'s durability disclosure.

---

## Env knobs (with defaults from code)

Durability-relevant configuration, all read by `basin-server` or the
crates it wires:

| Env var | Default | Effect |
|---|---|---|
| `BASIN_SHARD_ENABLED` | `0` | `1` routes INSERTs through the WAL + compactor. Off = legacy synchronous Parquet path. |
| `BASIN_WAL_MODE` | `local` | `local` = file-backed single-node WAL (default, unchanged). `raft` = quorum-replicated WAL; requires `BASIN_SHARD_ENABLED=1` and the raft topology env (see [deployment.md](../deployment.md#raft--lease-knobs-multi-node)). Any other value is a startup error. |
| `BASIN_CATALOG` | `memory` | **`memory` is volatile** — all table metadata is lost on restart. Set a `postgres://…` DSN for anything you want to restore. |
| `BASIN_CATALOG_SCHEMA` | `basin_catalog` | Schema for the Postgres catalog tables. |
| `BASIN_WAL_DIR` | `${BASIN_DATA_DIR}/wal` | Local WAL directory (when WAL backend is `local`). In raft mode the raft log/vote/snapshot persist under `${BASIN_WAL_DIR}/raft`. |
| `BASIN_WAL_BACKEND` | mirrors `BASIN_STORAGE_BACKEND`, else `local` | `local` wraps the store in `FsyncOnPut`; `s3`/`tigris` are durable on PUT and run unwrapped. **Not consulted in raft mode** — raft manages its own persistence. |
| `BASIN_WAL_ROOT_PREFIX` | `BASIN_STORAGE_ROOT_PREFIX` | Bucket sub-prefix; segments live at `{prefix}/wal/{project}/{partition}/{ulid}.seg`. Local-mode only. |
| `BASIN_WAL_COMMIT_DELAY_MS` | `2` | Group-commit coalescing window for synchronous appends (local mode only). `0` disables coalescing. |
| `BASIN_SYNCHRONOUS_COMMIT` | unset (= `off`) | Engine-wide default for the `basin.synchronous_commit` GUC. In raft mode `SET basin.synchronous_commit = on` still works: `append` already blocks until quorum, so both modes are durable-at-ack when this is on. |
| `BASIN_STORAGE_BACKEND` | `local` | Data object store. `s3`/`tigris` recommended for production durability. |
| WAL `flush_interval` | 200 ms | **Not env-tunable** — hardcoded in `basin-server` `main.rs`. The async-commit loss window in local mode. Not relevant in raft mode (quorum is the boundary). |
| WAL `flush_max_bytes` | 1 MiB | **Not env-tunable** — buffer-pressure flush threshold (local mode). |
| Shard `compaction_interval` | 30 s | **Not env-tunable** — WAL tail → Parquet/Vortex drain cadence (`ShardConfig::new`). |
| `BASIN_OVERLAY_RECONCILE_SECS` | `5` | Overlay-drain tick; `0` disables the reconciler (not recommended). |
| `BASIN_OVERLAY_RECONCILE_BYTES` | 8 MiB | Dirty-overlay bytes trigger. |
| `BASIN_OVERLAY_RECONCILE_AGE_SECS` | `15` | Oldest-dirty-entry age trigger. |
| `BASIN_MEMTABLE_MAX_AGE_SECS` | `60` | Hot-tier age-based flush threshold. |
| `BASIN_MEMTABLE_HARD_CAP` / `_SOFT_CAP` / `_TABLE_CAP` | 256 MiB / 192 MiB / 16 MiB | Hot-tier memory budget; soft caps trigger background flushes. |
| `BASIN_HOTTIER_FASTPATH_DISABLE` | unset | `1` = kill switch: all point-mutation fast paths off (durable-at-ack cold path instead). |
| `BASIN_HOTTIER_DELETE_FASTPATH` | `1` | `0` disables just the DELETE tombstone fast path. |

---

## Quick decision table

| You need | Do this |
|---|---|
| Zero-loss INSERT acks (single node) | `SET basin.synchronous_commit = on` (or `BASIN_SYNCHRONOUS_COMMIT=on` server-wide) |
| Zero-loss INSERT acks + node-loss tolerance | `BASIN_WAL_MODE=raft` (quorum durability; see [deployment.md](../deployment.md#raft--lease-knobs-multi-node) and v1 caveats in [failover.md](./failover.md#raft-mode-3-node-deployment)) |
| Zero-loss point UPDATE/DELETE acks | `BASIN_HOTTIER_FASTPATH_DISABLE=1` (pays the cold-path latency; not mitigated by raft mode — the overlay is not WAL-logged in either mode) |
| Survive node loss without the WAL volume (local mode) | `BASIN_WAL_BACKEND=s3` (WAL segments on the object store) |
| Survive restart at all | `BASIN_CATALOG=postgres://…` — the memory catalog is volatile |
| Survive power loss on a single box | S3-compatible data + WAL backends, or accept the local-FS caveat |

---

## Cross-references

- [restore.md](./restore.md) — backup + restore procedures built on these boundaries.
- [failover.md](./failover.md) — node loss, recovery, split-brain; raft-mode 3-node procedures.
- [deployment.md](../deployment.md) — env table for raft/lease/node knobs with v1 caveats.
- [ADR 0020 — WAL transaction markers + replay suppression](../decisions/0020-wal-transaction-markers.md)
- [ADR 0016 — HTAP hot tier architecture](../decisions/0016-htap-hot-tier-architecture.md)
- `crates/basin-wal/src/lib.rs` — the `Wal` trait's documented durability contracts.
- `crates/basin-wal/src/raft_wal.rs` — `RaftWal`, `DurabilityBackend`, quorum-commit semantics.
