---
title: "Session pool — operator runbook"
nav_section: operations
sidebar_position: 54
summary: "Day-2 ops guide for Basin's native session pool: pool exhaustion, per-project cap stalls, eviction tuning, hit-rate diagnostics, and incident playbooks."
tags: [operations, pool, sessions, exhaustion, eviction, scaling]
---

# Session pool — operator runbook

Day-2 operator's guide to Basin's native session pool (`basin-pool`,
ADR 0007).

The session pool caches `ProjectSession` objects keyed on
`(ProjectId, client_key)` so short-lived connections (Lambda, Cloud Run,
serverless workers) reuse warm sessions instead of paying the
`Engine::open_session` cost (catalog list + `ListingTable` registration)
per request.

Unlike Postgres + pgbouncer, Basin uses tokio tasks rather than forked
processes, so the motivation for a pool is different: it is not
connection-count reduction but open-session cost amortisation.

---

## Architecture in one page

```
Client request (HTTP)
  │
  ▼
basin-rest route handler
  │
  ▼
SessionPool::acquire(project, client_key)
  │
  ├─ cache hit (available[(project, client_key)] is non-empty)
  │     → pop session, return immediately (sub-µs; one mutex acquire)
  │       stats.hits++
  │
  ├─ cache miss, global cap and per-project cap allow it
  │     → reserve slot (under lock), release lock
  │     → Engine::open_session(project) [I/O, without holding pool lock]
  │       stats.misses++
  │
  └─ cache miss, per-project or global cap exhausted
        → register oneshot waiter, park on rx.await
        → woken when a session for the same project is released
        → retry loop

PooledSession::drop()
  → push session back to available[(project, client_key)]
  → wake one parked waiter for this project (if any)

Background eviction loop (spawn_eviction, configurable cadence):
  → walk available sessions; drop those with last_used > idle_ttl
  → stats.evictions++
```

The pool lock is never held across `await` — it is only held for the
in-memory bookkeeping under contention-free conditions. The slow
`Engine::open_session` happens outside the lock.

Key source files:
- `crates/basin-pool/src/lib.rs` — `SessionPool`, `PoolConfig`, main acquire logic
- `crates/basin-pool/src/pooled_session.rs` — `PooledSession` RAII guard
- `crates/basin-pool/src/state.rs` — `PoolState` (the in-RAM session map)
- `crates/basin-pool/src/eviction.rs` — `EvictionHandle`, background eviction loop
- `crates/basin-pool/src/stats.rs` — `PoolStats`, atomic counters

---

## Metrics

The pool exposes counters via `SessionPool::stats()`, which is consumed by
the server's `/metrics` endpoint and the `/v1/admin/pool/stats` endpoint.

| field | type | what it tells you |
|---|---|---|
| `basin_pool_hits_total` | counter | Acquires that found a warm cached session — no `open_session` call. |
| `basin_pool_misses_total` | counter | Acquires that required a cold `Engine::open_session` call. |
| `basin_pool_evictions_total` | counter | Sessions dropped by the idle-eviction loop. |
| `basin_pool_resident_sessions` | gauge | Total sessions held in the pool (in-use + idle). |
| `basin_pool_resident_per_project` | gauge | Maximum resident sessions across all projects — the pressure number to compare against `per_project_cap`. |

**Key derived signal**:

```
hit_rate = hits_total / (hits_total + misses_total)
```

A healthy deployment with long-lived clients has a hit rate > 90 %. A
serverless deployment where every request is from a fresh Lambda invocation
may have a hit rate near 0 % — in that case the pool adds latency overhead
on misses (the `open_session` cost) without benefit, and the `idle_ttl`
should be tuned down so sessions are not held unnecessarily between cold-start
windows.

---

## Configuration knobs

`PoolConfig` is constructed at startup. There is no live-reload; a restart is
required to change these.

| field | default | env var override | description |
|---|---|---|---|
| `max_sessions` | 1 024 | `BASIN_POOL_MAX_SESSIONS` | Hard ceiling on resident sessions across all projects. Acquires block (wait for a release) when this is reached. |
| `idle_ttl` | 300 s (5 min) | `BASIN_POOL_IDLE_TTL_SECS` | How long an idle session may sit in the cache before the eviction loop drops it. |
| `per_project_cap` | 64 | `BASIN_POOL_PER_PROJECT_CAP` | Max sessions per project (in-use + idle). Prevents one project's burst from starving the global pool. |
| `eviction_interval` | 60 s | `BASIN_POOL_EVICTION_INTERVAL_SECS` | How often the background eviction loop runs. |

**Tuning guidance**:

- `max_sessions`: sized for the number of concurrent requests across the
  process. At the Basin default of one tokio task per request,
  `max_sessions = 1 024` supports 1 024 concurrent in-flight requests before
  new acquires block. For high-throughput deployments, increase this in
  proportion to expected concurrent traffic.
- `per_project_cap`: the per-project ceiling prevents a whale from holding all
  1 024 global slots. At 64 sessions per project, a single project can consume
  6.25 % of the global pool; with 16 projects you could fill the global pool
  before any per-project cap is hit.
- `idle_ttl`: 5 minutes is generous for long-lived server connections. For
  serverless deployments where function instances live < 60 s, set this to
  30–60 s to avoid memory waste.
- `eviction_interval`: must be less than `idle_ttl` to be useful. At default
  60 s eviction interval and 300 s TTL, sessions can be resident for up to
  360 s after last use. For tighter memory budgets, reduce both.

---

## Common alerts

### ALERT: pool hit rate below 70 %

**Trigger**: `rate(basin_pool_hits_total[5m]) /
(rate(basin_pool_hits_total[5m]) + rate(basin_pool_misses_total[5m])) < 0.70`

**What happened**: More than 30 % of requests are paying the cold
`Engine::open_session` cost. The session pool is not amortising the open cost
effectively.

**Diagnosis**:

1. **Serverless / ephemeral clients**: if clients are short-lived (Lambda, Cloud
   Run) and the pool's `idle_ttl` exceeds the function invocation rate, sessions
   may be evicted between calls. This is expected behaviour — the pool is the
   wrong tool for pure serverless deployments.
2. **High client_key diversity**: each `(project, client_key)` is a separate
   cache slot. If clients supply unique `client_key` values per request, the
   cache never hits. Check the `client_key` attribution in the request path.
3. **Pool capacity too small**: if `max_sessions` is too small for the number
   of concurrent clients, sessions are evicted under pressure and cold opens
   become frequent. Check `basin_pool_evictions_total` rate.

---

### ALERT: pool resident sessions at or near max_sessions

**Trigger**: `basin_pool_resident_sessions` / `max_sessions > 0.9`.

**What happened**: The pool is near its global ceiling. New acquires may block
waiting for a session to be released.

**Diagnosis**:

```bash
curl -s http://localhost:9090/metrics | grep basin_pool_resident
```

Check:

1. **Blocked acquires**: if requests are timing out with errors like
   "pool: waiting for session", the ceiling is the bottleneck.
2. **Resident-per-project outlier**: if `basin_pool_resident_per_project` is
   close to `per_project_cap` for one project, that project is consuming a
   disproportionate share of the global pool.

**Remediation**:

1. Increase `max_sessions`:
   ```bash
   BASIN_POOL_MAX_SESSIONS=2048
   ```
   Restart required.

2. Reduce `idle_ttl` to release idle sessions faster:
   ```bash
   BASIN_POOL_IDLE_TTL_SECS=120  # 2 min instead of 5 min
   ```

3. Reduce `per_project_cap` for whale projects:
   ```bash
   BASIN_POOL_PER_PROJECT_CAP=32
   ```
   This is a global setting; per-project overrides are not yet exposed.

---

### ALERT: pool resident_per_project at or near per_project_cap

**Trigger**: `basin_pool_resident_per_project` / `per_project_cap > 0.9` sustained.

**What happened**: One project is consuming almost all of its per-project
session slots. Additional acquires for that project are queuing behind their
waiters while other projects may still have headroom.

**Note on deadlock prevention**: the pool has a built-in anti-deadlock path.
If a project's per-project cap is reached but the project has idle sessions
under a *different* `client_key`, the pool evicts the oldest idle session
(LRU) for that project to free a slot, rather than indefinitely queuing. This
means a client whose exact `(project, client_key)` is not cached can still
proceed even when the project cap is nominally full, as long as the project
has idle sessions it can sacrifice.

**Remediation**:

1. Identify the high-occupancy project from the admin endpoint:
   ```bash
   curl -s http://localhost:8080/v1/admin/pool/stats | jq '.per_project | sort_by(-.sessions) | .[0:5]'
   ```
2. If the project is legitimately busy, increase `per_project_cap`.
3. If the project has a client_key flood (each client sends a unique key and
   never reuses sessions), reduce diversity at the application level or set
   `client_key = None` in the request handler.

---

## Common operations

### Inspect current pool state

```bash
# Metrics endpoint
curl -s http://localhost:9090/metrics | grep basin_pool_

# Admin endpoint (if wired in basin-server)
curl -s http://localhost:8080/v1/admin/pool/stats | jq .
```

Sample healthy output:

```json
{
  "resident_sessions": 42,
  "resident_per_project": 8,
  "hits": 28103,
  "misses": 312,
  "evictions": 15
}
```

Hit rate = 28103 / (28103 + 312) ≈ 98.9 % — healthy.

### Drain the pool (emergency — force all sessions cold)

There is no live drain command. To force all sessions to close:

1. Restart the replica — all `PooledSession` guards drop on process exit,
   which returns sessions to the pool but since the pool is destroyed, they
   are effectively closed.
2. Reduce `idle_ttl` to 1 second and wait for the eviction loop to clear all
   idle sessions (up to one `eviction_interval` = 60 s).
   ```bash
   BASIN_POOL_IDLE_TTL_SECS=1
   # Sessions currently in-use are not affected until they are returned.
   ```

### Force an eviction pass immediately (test/debug only)

Via the test API (not available in production builds):

```rust
pool.run_eviction_once().await;
```

In production, trigger the eviction loop by restarting with a shorter
`BASIN_POOL_EVICTION_INTERVAL_SECS`.

### Identify the session open latency baseline

If `misses_total` is high and latency is elevated, measure
`Engine::open_session` cost directly:

```bash
# Time a cold project connect (no cached session) using basin CLI.
time basin connect --project <uuid> --measure-open
```

Expected cold open: 50–200 ms (catalog list + ListingTable registration).
If this exceeds 500 ms, the catalog Postgres or object store is slow — check
the lease-ownership runbook for catalog health signals.

---

## Troubleshooting

### Requests fail with "pool per-project cap reached" and never unblock

**Scenario**: A project has `per_project_cap` sessions all in-use. New
acquires park on a waiter. The waiters never wake because sessions are never
released.

**Root cause**: A caller is holding a `PooledSession` guard (i.e. has not
dropped it) indefinitely. This can happen if:

- A request handler panics and the guard is on a `futures::pin_mut!` that
  was abandoned (the panic unwinds past the guard, which should still drop
  correctly because Drop is called on unwind in Rust — but check for
  `std::panic::catch_unwind` paths that swallow the panic without dropping
  the guard).
- An `async fn` was cancelled (via `tokio::select!` or `timeout`) at a
  `.await` point while holding the guard. The `Drop` impl will be called
  on the future's drop, which triggers the return path. Check whether the
  Drop is running correctly by watching `basin_pool_resident_sessions` —
  if it is not decreasing after the requests complete, guards are leaking.

**Recovery**:

1. Restart the replica — all guards are dropped on process exit.
2. Ensure request handlers always release `PooledSession` within the
   request's lifetime (e.g. do not store them in `Arc` without proper cleanup).

---

### Sessions are returned to the pool in a bad state (leaked transaction, stale prepared statement)

**Known limitation** (documented in `crates/basin-pool/src/lib.rs`):

`ProjectSession::reset()` does not yet exist. A session returned to the pool
after a leaked transaction or a partially-executed statement can contaminate
the next borrower. This is a v1 limitation — the fix (adding a `reset()` call
in `PooledSession::drop`) is tracked as a follow-up.

**Mitigation until fixed**:

- Ensure every request that borrows a session either commits or rolls back
  explicitly before the `PooledSession` is dropped.
- If state corruption is suspected, restart the replica to flush all pooled
  sessions, forcing cold opens for subsequent requests.

**Detection**:

```bash
# Look for unexpected transaction-related errors in the log.
grep -E 'transaction|prepared statement|pool state' /var/log/basin-server.log | tail -50
```

---

### Eviction loop not running / sessions not being evicted

**Check**: `basin_pool_evictions_total` is not increasing even though sessions
are idle beyond `idle_ttl`.

1. Confirm the eviction loop was started at startup:
   ```bash
   grep 'spawn_eviction' /var/log/basin-server.log | head -3
   ```
   If missing, the server's startup sequence did not call
   `pool.spawn_eviction()`. This is a code-level bug — file an issue.

2. Confirm `eviction_interval` is shorter than `idle_ttl`. If both are
   at default (60 s interval, 300 s TTL), sessions will not be evicted
   until they are idle for 300 s, and the eviction loop runs every 60 s.
   This is expected. At high idle-session counts, the first eviction tick
   after 60 s will drop all sessions idle for > 300 s.

3. Check whether `EvictionHandle` was dropped prematurely — dropping the
   handle shuts down the eviction loop. The handle should be held for the
   lifetime of the server process.

---

## Failure modes summary

| Failure | Visible signal | Behaviour | Recovery |
|---|---|---|---|
| **Global pool full** | `basin_pool_resident_sessions` at `max_sessions`; new acquires block | All new requests queue behind `rx.await` waiter. No timeout — waiters block indefinitely until a session releases. | Increase `max_sessions`; restart. Or reduce `idle_ttl` to evict idle sessions faster. |
| **Per-project cap full** | `basin_pool_resident_per_project` at `per_project_cap` | New acquires for the full project queue. The anti-deadlock path evicts the project's oldest idle session (different `client_key`) if one exists. | Increase `per_project_cap`; restart. Reduce `client_key` diversity. |
| **Session guard leak** | `basin_pool_resident_sessions` never decreases; waiters never wake | Sessions are not returned to the pool. Waiters park indefinitely. | Restart the replica. Fix the guard-leak in application code. |
| **State-corrupted session** | Errors on subsequent requests that reuse the session (wrong transaction state, stale statement) | One request's session contaminates the next borrower. | Restart replica to flush pool. Add explicit rollback in request handlers. |
| **Eviction loop stopped** | `basin_pool_evictions_total` flat; `basin_pool_resident_sessions` grows monotonically | Sessions accumulate past idle TTL. Memory pressure increases. | Restart the replica; ensure `spawn_eviction` is called at startup. |
| **open_session slow** | `basin_pool_misses_total` rate rising; request latency increases | Cold opens dominate. Pool hits not amortising the open cost. | Check catalog Postgres health (see lease-ownership runbook); check object store latency. |

---

## Cross-references

- [ADR 0007 — Connection pooling design](../decisions/0007-connection-pooling.md) — full rationale, known limitations, and the `reset()` follow-up item.
- [Lease ownership runbook](./lease-ownership.md) — the catalog Postgres that `Engine::open_session` queries; slow catalog = slow pool misses.
- `crates/basin-pool/src/lib.rs` — `PoolConfig` defaults, `acquire` logic, anti-deadlock eviction path.
- `crates/basin-pool/src/stats.rs` — `PoolStats` struct and `AtomicStats` counters.
