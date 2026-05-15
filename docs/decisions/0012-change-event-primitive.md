# 0012 — Change-event sink trait as Basin's trigger / webhook / future-realtime primitive

- **Status:** Accepted, 2026-05-09.
- **Wedge call:** for **new SaaS workloads only** — Basin is *not* aiming
  for legacy-PG-migration parity on this feature. Customers with
  hand-written PL/pgSQL stay on Postgres; Basin is the place new SaaS
  apps get built.

## Context

Triggers, user-defined functions, and `LISTEN` / `NOTIFY` are the three
PG features new-SaaS apps reach for when "INSERT a row, then do
something else." PG ships them as three separate subsystems because
that's how PG grew historically — `CREATE TRIGGER` (1995), PL/pgSQL
(1998), `LISTEN` / `NOTIFY` (1996). They didn't get designed together;
they accreted.

Designing fresh, these collapse into one primitive: **a change-event
emitted by every committed write, with pluggable sinks consuming it.**
Every other feature is a different kind of sink.

The naive path — implement `CREATE TRIGGER` + PL/pgSQL + `LISTEN` /
`NOTIFY` matching PG semantics — has documented drawbacks (the
"10 drawbacks" audit on 2026-05-09; permanent-maintenance-load is the
worst). Most can't be fully fixed without forking Postgres.

## Decision

**Ship a `ChangeEventSink` trait + capture point in the engine commit
path now. Implement the immediately-needed sinks (reactors, webhooks)
in the same workspace. Defer the WebSocket-realtime sink — the trait
makes adding it later a separate-crate addition with no engine
changes.**

**Explicitly out of scope for this ADR's commit:**

- PL/pgSQL parser or interpreter at any layer
- `CREATE TRIGGER … EXECUTE FUNCTION` with a function body
- `LISTEN` / `NOTIFY` PG wire-protocol compat
- WebSocket realtime / SSE / presence channels (deferred — same repo,
  same workspace, separate crate when shipped)

**What ships now (Tier 0, ~3-5 days):**

The trait + capture point + zero default sinks. Engine works
byte-identically with no sinks attached.

```rust
// basin-common::events
pub struct ChangeEvent {
    pub project: ProjectId,
    pub table: TableName,
    pub op: ChangeOp,                    // Insert | Update | Delete
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub committed_at: DateTime<Utc>,
    pub seq: u64,                        // monotonic per-project
    pub causation_user: Option<String>,
}

#[async_trait]
pub trait ChangeEventSink: Send + Sync {
    async fn publish(&self, event: &ChangeEvent) -> Result<()>;
}

pub struct EventSinkRegistry {
    pre_commit: Vec<Arc<dyn ChangeEventSink>>,    // can abort
    post_commit: Vec<Arc<dyn ChangeEventSink>>,   // fire-and-forget
}

// basin-engine
impl Engine {
    pub fn attach_pre_commit_sink(&self, sink: Arc<dyn ChangeEventSink>);
    pub fn attach_post_commit_sink(&self, sink: Arc<dyn ChangeEventSink>);
}
```

The executor's INSERT/UPDATE/DELETE path emits an event:

1. **Pre-commit sinks** run synchronously *before* the catalog commit;
   any error rolls back the mutation. (PG `BEFORE`-trigger semantics
   for the SQL-body case.)
2. **Catalog commit** runs.
3. **Post-commit sinks** run fire-and-forget after the commit succeeds;
   their errors do not roll back. (PG `AFTER`-trigger semantics for the
   side-effect case.)

## Sink consumers

### Phase 5.11.C — SQL-bodied reactors (built-in `ReactorSink`, pre-commit)

```sql
ALTER TABLE orders REACT ON UPDATE
  WHEN (NEW.status = 'paid' AND OLD.status != 'paid')
  EXECUTE INSERT INTO billing_events (order_id, ts) VALUES (NEW.id, now());
```

Body is a single SQL statement. `NEW` / `OLD` / `TG_OP` /
`TG_TABLE_NAME` are bind variables. Implements `ChangeEventSink`,
attached as **pre-commit** so reactor failures abort the mutation.
Lives in `basin-engine`.

### Phase 5.11.C2 — Constraint-shaped reactors (pre-commit)

```sql
ALTER TABLE items REACT ON INSERT
  CONSTRAINT (SELECT count(*) FROM items WHERE project = NEW.project) <= 100;
```

If the predicate evaluates `false`, the INSERT aborts with `23514
check_violation`. Project-scoped invariant enforcement.

### Phase 5.11.I — Webhook fanout (built-in `WebhookSink`, post-commit)

```sql
ALTER TABLE orders SUBSCRIBE WEBHOOK
  TO 'https://app.example.com/order-events'
  ON INSERT OR UPDATE
  WHERE NEW.status = 'paid';
```

Implements `ChangeEventSink`, attached as **post-commit**. Owns its
own disk-backed retry queue (basin-wal sidecar; idempotency-keyed).
Reuses `basin-net` for the actual HTTP path (URL allowlist + per-
project rate limit + body cap + timeout already in place).

### Future basin-realtime crate (deferred — same workspace)

When realtime becomes a real ask, add `crates/basin-realtime/` to the
workspace — same shape as `crates/basin-cron/`, `crates/basin-net/`,
`crates/basin-trgm/`. Implements the same `ChangeEventSink` trait as a
post-commit sink. WebSocket axum handler + per-project ring buffer +
replay cursors + filter pushdown + disconnect protocol — all the
genuinely-novel infrastructure — lives in this future crate, not in
the engine.

If at that point a separate repo makes more sense (independent release
cadence, ecosystem signal), the workspace member becomes a separate
repo with a one-day `git mv` + new Cargo.toml. The trait surface
doesn't change. **Both paths stay open**; the same-repo default is
just lower-friction for current scale.

## Why same-repo (deferred), not separate repo

The arguments for separate repo only matter at scale:

- Independent release cadence — Basin has no release cadence yet
- Doesn't bloat the engine repo — it doesn't, at this size
- Forces clean abstractions — the trait already enforces this
- Healthier OSS ecosystem — premature; ship value first, then split

Same-repo wins on simpler development, single CI, single docs, one
issue tracker, easier refactoring. All matter at Basin's stage.

## What this does NOT commit us to

- **PL/pgSQL parser or interpreter** — out of scope, period
- **`CREATE TRIGGER … EXECUTE FUNCTION foo()`** with a PL/pgSQL body —
  reactors are SQL-bodied only
- **Full PG `LISTEN` / `NOTIFY` wire-protocol compat** — reactors and
  webhooks cover the use cases; future realtime is WebSocket-shaped
- **Cross-project subscribers** — every event is project-scoped; an admin
  tool that wants to fan out across projects does so by attaching a
  sink that ignores the `project` filter
- **Cross-region replication of change events** — Phase 6 multi-region
  work; ADR 0009 covers it
- **WASM UDFs** — Phase 5.11.J, gated on customer demand

## Implementation phases (mapped to TASK.md Phase 5.11)

### Tier 0 — Foundation (~3-5 days, no deps)

- **5.11.G** — `ChangeEventSink` trait + `EventSinkRegistry` + capture
  point in executor's commit path. Default: empty registries; engine
  byte-identical to today.

### Tier 1 — Ship now, ~12-15 weeks honest

Independent of any sink work. Customer-visible PG-compat upgrade.

- **5.11.A** — Built-in function catalogue + JSONB operators
  (`->`, `->>`, `#>`, `@>`) + recursive-CTE/window verification (~4w)
- **5.11.D** — `LANGUAGE sql` scalar functions, planning-time inlining (~3w)
- **5.11.B** — Declarative lifecycle (`AUTO_UPDATE`, `AUDIT TO`,
  `SOFT DELETE`) (~2w)
- **5.11.K2** — `CREATE TYPE … AS ENUM` + `CREATE DOMAIN` (~2w)
- **5.11.D2** — `CREATE MATERIALIZED VIEW` SQL surface (drops the
  `cv_glue` stub) (~1w)

### Tier 2 — Customer-signal-driven

Ship when Phase 0 interviews show real pull. Each is independent and
plugs into the Tier-0 trait.

- **5.11.C** — SQL-bodied reactors (`ReactorSink` pre-commit) (~2w)
- **5.11.C2** — Constraint-shaped reactors (~1w)
- **5.11.I** — Webhook fanout (`WebhookSink` post-commit + retry
  queue) (~4-5w honest)
- **5.11.E** — `LANGUAGE sql RETURNS TABLE` functions (~2w)
- **5.11.F** — Multi-statement `CALL` procedures (no logic) (~2w)
- **5.11.K** — Generated columns (`GENERATED ALWAYS AS … STORED`) (~2w)
- **5.11.K3** — Sequences (`CREATE SEQUENCE`, `nextval`, `currval`) (~2w)

### Tier 3 — Larger asks

- **5.11.M** — `information_schema` + `pg_catalog` read-only views
  (~6-8w honest; the gate for proper PG-ecosystem tooling)
- **5.11.J** — WASM UDFs (custom imperative logic, Wasmtime) (~3-4w
  gated)

### Deferred (placeholder, same workspace, gated on signal)

- **`crates/basin-realtime`** — WebSocket realtime as a `ChangeEventSink`
  implementation. Only ships when ≥2 design partners ask.

## Trade-off (read before reopening this decision)

This commits Basin to **new SaaS only** as the trigger / function /
realtime story. Legacy PG migrations that depend on hand-written
PL/pgSQL can't drop in their existing schema unchanged — they translate
trigger bodies (mechanical for ~95% of real-world cases per the
schema audit; the other ~5% is a real porting cost the customer bears).

The trade is:

- **Lose:** the "drop in any PG schema unchanged" claim. Customers with
  deeply legacy enterprise PG schemas are not Basin's wedge anyway.
- **Win:** wedge clarity ("the multi-project DB designed for new SaaS"),
  bounded engineering scope, no permanent PL/pgSQL maintenance load,
  clean trait-shaped extensibility for future sinks, no novel realtime
  infrastructure shipped speculatively.

## Trigger to revisit

**Reopen and consider Tier 4 (`crates/basin-realtime` + WebSocket /
SSE / presence)** when:

1. Two or more design partners (Phase 0) explicitly ask for
   server-pushed realtime updates that webhooks can't satisfy, AND
2. The customer can't bridge their existing realtime provider
   (Pusher, Ably, Supabase Realtime, their own WebSocket layer) to
   Basin's webhook fanout, AND
3. The engineering org has budget for the realtime infrastructure load
   (~6-8 weeks honest engineering plus operations).

**Reopen and consider Tier 4 (PL/pgSQL via libpg_query)** when:

1. Two or more design partners (Phase 0) explicitly ask for hand-
   written PL/pgSQL business logic in the database, AND
2. The same customers can't move that logic to their app server
   without a major rewrite, AND
3. The engineering org has budget for the permanent maintenance load
   (~0.5-1 FTE on the compat surface forever).

Without all three, the answer stays "no" for both.

## References

- [ADR 0002 — No upstream Postgres extensions](./0002-no-postgres-extensions.md) — the original
  "don't rebuild Aurora" call; this ADR is consistent with that.
- [ADR 0008 — Noisy-neighbor fairness](./0008-noisy-neighbor-fairness.md) — the per-project cost
  discipline this ADR preserves.
- 2026-05-09 conversation log — the "10 drawbacks of full PG compat"
  audit, the new-SaaS-only reframe, the WebSocket-removal decision,
  the same-repo deferred-implementation choice.
