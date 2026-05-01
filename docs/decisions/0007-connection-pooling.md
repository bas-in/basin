# 0007 — Native connection pooling (`basin-pool`)

- **Status:** Accepted, deferred. Build trigger below.
- **Date:** 2026-05-01
- **Tags:** scope, networking, performance, defers-to-trigger

## Context

Postgres needs pgbouncer because it forks an OS process (~10 MB) per
connection. Basin's pgwire path is built on tokio: each connection is a
task (~few KB), so the structural reason for pgbouncer doesn't apply.
Basin can hold tens of thousands of concurrent connections in one
process without RAM pressure.

Two real costs *do* exist, however, and they show up in the same
Lambda / Cloud Run / Kubernetes patterns where pgbouncer is normally
deployed:

1. **`Engine::open_session` does work.** It calls
   `catalog.list_tables(tenant)` and registers each table as a
   DataFusion `ListingTable`. For a tenant with 50 tables that is tens
   of milliseconds — fine for long-lived connections, but a hot loop
   for short-lived ones.
2. **Per-session DataFusion state is ~50-200 KB.** At 10k idle sessions
   that is 0.5-2 GB. The default eviction story today is "the OS will
   force you to fix this when you run out of RAM," which is bad.

Pgbouncer doesn't solve either problem cleanly. Its transaction-pooling
mode rewrites session state across upstream connections — Basin's
`TenantSession` carries cached `ListingTable` snapshots and prepared-
statement registries that don't multiplex across users. Its
session-pooling mode is just a queue with no real upside.

The right answer is a small, native pooler that caches `TenantSession`
objects directly.

## Decision

Basin will ship **`basin-pool`**, a `TenantSession` cache wired into
the pgwire accept path.

### Public surface

```rust
pub struct PoolConfig {
    pub max_sessions: usize,           // default 1024
    pub idle_ttl: Duration,            // default 5 min
    pub per_tenant_cap: usize,         // default 64
}

pub struct SessionPool { /* Arc inside */ }

impl SessionPool {
    pub fn new(engine: basin_engine::Engine, cfg: PoolConfig) -> Self;
    pub async fn acquire(
        &self,
        tenant: TenantId,
        client_key: Option<String>,   // optional sticky key (e.g. JWT subject)
    ) -> Result<PooledSession>;
}

pub struct PooledSession {
    // Drops returns the session to the pool, NOT the engine.
}
```

### Cache shape

`HashMap<(TenantId, ClientKey), VecDeque<TenantSession>>` behind a
`tokio::sync::Mutex`. `acquire` pops the back (most-recently-used);
`Drop` on `PooledSession` pushes it back (or drops it if the pool is
beyond `max_sessions`).

### Eviction

A background task wakes every `idle_ttl / 4`, walks the map, and drops
sessions whose `last_returned` is older than `idle_ttl`. Same pattern
as `basin-shard`'s eviction loop — they share an "idle eviction is a
background tokio task" idiom.

### Per-tenant cap

`per_tenant_cap` prevents one tenant's burst from starving others. A
tenant exceeding their cap waits on a per-tenant `Semaphore` rather
than getting a fresh session; once an existing session is returned,
the waiter wakes.

### Integration with `basin-router`

The pgwire accept loop is the only call site:

```rust
let session = pool.acquire(tenant, client_key).await?;
// Today: open_session(tenant) returns a brand-new session every time.
```

`client_key` defaults to `None` (any-session-for-this-tenant); when
the JWT-aware tenant resolver lands (ADR 0005), the pool key becomes
`(tenant_id, jwt.sub)` so each user gets their own session — keeping
prepared-statement caches warm across reconnects.

### What's *out* of v1

- Cross-process sharing (would require a shared-memory or RPC pool;
  not needed for the wedge).
- Custom routing policies (least-recently-used is enough).
- Pre-warming idle pools speculatively (defer; unjustified complexity).

## Consequences

**Positive**

- Lambda / Cloud Run / serverless workloads stop paying
  `Engine::open_session` cost per request. Hit rate above 95% is
  realistic.
- Memory ceiling becomes a configured number, not "however many
  connections happen to land."
- Closes the only structural reason a customer would want pgbouncer
  in front of Basin.

**Negative**

- Sessions are now reused across client connections. A bug in
  session-state cleanup (e.g. a leaked transaction or a stale
  `prepared` cache entry) could leak between clients.
- A burst of distinct tenants exceeding `max_sessions` evicts
  long-tail tenants. Customers with very-many-tenants workloads need
  to size the pool carefully.

**Mitigations**

- `PooledSession::Drop` calls a fixed `reset()` that clears any
  per-connection mutable state (open transactions, dirty prepared
  statements). Documented in `basin-engine` API.
- Metrics expose hit rate / eviction count / per-tenant resident
  count so operators can tune the pool size based on actual traffic.

## Architectural compatibility

`basin-pool` doesn't change `basin-engine`, `basin-storage`,
`basin-catalog`, `basin-wal`, or `basin-shard`. It sits between
`basin-router` and `basin-engine` as a cache. No data path changes.

If a future deploy needs pool sharing across processes, the trait
shape (`SessionPool::acquire`) is implementable against a different
backend without disturbing the router; that future ADR can swap the
in-process pool for a distributed one.

## Trigger to start building

We start when **one** of:

1. A wedge customer reports `Engine::open_session` showing up as a
   measurable share of their request latency (typical signal: their
   p99 latency is dominated by setup cost on cold connections, not by
   the actual SQL).
2. We deploy a workload pattern (Lambda / Cloud Run / serverless) where
   short-lived client connections are the norm.
3. We cross 10,000 concurrent persistent connections in a single
   process and start seeing memory pressure.

This is a low-bar trigger because the build is small (~1 week) and
the design is bounded. Don't pre-emptively build: there's no pool
churn to optimize today.

## Estimated effort once triggered

| Component | Effort |
|---|---|
| `basin-pool` crate scaffold + `SessionPool::acquire` + `PooledSession` | 1.5 days |
| Idle eviction loop + per-tenant `Semaphore` cap | 1 day |
| `TenantSession::reset()` for safe reuse | 0.5 day |
| Metrics integration | 0.5 day |
| Tests (hit rate, eviction, per-tenant fairness, reset correctness) | 1.5 days |
| Smoke test: 1000 short-lived conns through 10 pool slots | 0.5 day |
| **Total** | **~5 days calendar** |

## Alternatives considered

- **Run pgbouncer in front of Basin.** Rejected — pgbouncer's
  transaction-pooling mode would mangle Basin's session state;
  session mode adds queueing but no structural benefit since Basin
  already handles many connections natively.
- **Don't pool at all.** Status quo. Fine for the long-lived-conn
  webapp pattern. Falls over for serverless / per-request lifecycles.
- **Pool inside `basin-engine` directly** (no separate crate). Rejected
  — the engine is the SQL execution surface; pooling is a deployment
  concern. Separate crate keeps the engine's boundary clean and lets
  `basin-pool` evolve (e.g. add cross-process pooling later) without
  churning the engine's public API.
- **Use bb8 / deadpool-postgres / r2d2.** These pool client connections
  *to* Postgres, not server-side `TenantSession` objects in a Rust
  process. Wrong layer.
