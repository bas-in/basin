# Basin — wedge-deepening roadmap

The five things that turn Basin from "interesting PoC" into "developers can
ship a multi-project SaaS on it." All directly serve the wedge customer
(multi-project SaaS with audit-log workloads).

This is **not** a plan to beat Neon at Postgres-app workloads or Turso at
the edge. The brief is explicit: stay in the wedge until paying customers
ask for adjacent expansion. See [`TASK.md`](./TASK.md) for the full Phase
0–7 build plan; this file is the prioritized next-six-months slice.

Legend: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` deferred

---

## Roadmap status — checkpoint 2026-05-19

**The full WEDGE 1–5 + 5c slice is shipped.** All five items below are at
v0.1 with passing benchmark coverage. Workspace tests passing; dashboard
stories honest — every red card has a documented architectural reason and a
future trigger to revisit. SQL-compat fragment coverage: **97.2%** (423 / 435
non-design-excluded fragments — up from ~75% at last checkpoint). Remaining
real v0.2 gaps: `LATERAL` joins, `WITH RECURSIVE` + DML-in-CTE, advanced
window frames, `JSON_AGG(t)` whole-row, `EXCLUDE USING gist`. See
[`docs/sql-support.md`](./docs/sql-support.md).

**Vortex storage default since 2026-05-18 per [ADR 0015](./docs/decisions/0015-vortex-storage-format.md).**
~50 perf commits landed in #161/#162; the 88-shape `vortex_vs_parquet_smoke`
battery shows wins on metadata aggregates (~30-40×), joins (1.3-1.7×), and
most analytics shapes; honest trailing on point-lookup latency (≈0.65×) and
`ORDER BY … LIMIT` (≈0.38×) where native vortex-datafusion execution is
still maturing.

Three additional crates landed alongside the wedge by founder direction
and now ship as part of the open-source bundle (Phase 5.10 in
[`TASK.md`](./TASK.md)): `basin-auth` (identity, ADR 0005), `basin-rest`
(PostgREST equivalent, ADR 0006), `basin-pool` (per-project connection
pool, ADR 0007). All three are wired into `basin-server` behind opt-in
env vars; defaults preserve the original PoC behaviour.

**What's next, in priority order (engine / open-source repo only;
hosted-cloud product lives in a separate repo):**

1. **Phase 5.14 — Durable Basin moat (HTAP hot tier + catalog-driven optimization, IN FLIGHT).** Phase 5.14.A1-A4 (catalog blooms), 5.14.B3 (APPROX_COUNT_DISTINCT), 5.14.B4 (APPROX_PERCENTILE), 5.14.D3 (catalog-aware WindowExec sort elision) have shipped this session. Remaining: 5.14.B1-B2 (sketch fields in catalog + writer-side computation, so the APPROX UDFs can short-circuit instead of streaming), 5.14.C1-C6 (HTAP hot tier per [ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md) — the 3-month architectural moat), 5.14.D1-D2 (adaptive write-time multi-sort + query history). Detailed sub-item decomposition in TASK.md Phase 5.14.
2. **Phase 5.16 — Query insights (per-shape stats, scale-regression detection).** Stats system to identify bad query paths commonly used at scale — both for customers (self-service "your slowest queries" view) and for Basin's own product roadmap evidence. OSS side ships plan-shape hashing + per-shape p50/p95/p99 histograms + scale-dependent regression tracking; cloud side ships ingest pipeline + UI + cross-customer anonymized aggregates. Closes the feedback loop on whether our perf commits actually pay off in production, not just in benchmark. Detailed 8-item decomposition in TASK.md Phase 5.16. Bridges OSS ↔ cloud — see [basin-cloud-roadmap.md](./docs/basin-cloud-roadmap.md) for the cloud-side companion items.
3. **Phase 5.15 — Unified docs platform.** Each OSS repo (`basin`, future `basin-js`, future `basin-cli`) keeps its `docs/` as standard markdown with YAML frontmatter ([spec](./docs/frontmatter-spec.md)). `basin-cloud`'s webapp has `npm run dev:docs` which build-time-fetches each OSS repo and renders the union via Docusaurus or Mintlify. OSS side (5.15.A/B/C — frontmatter spec, migrate 24 existing docs, top-level index, CI gate) shipped in commit d5ffbe1; cloud side (5.15.E-I) deferred to the basin-cloud repo. Detailed 9-item decomposition in TASK.md Phase 5.15.
4. **Phase 0 customer interviews** — strategic, not engineering. Architecture is done; what's missing is paying customers.
3. ~~Engine `UPDATE` / `DELETE` support~~ — **shipped** (Iceberg copy-on-write).
4. ~~A4 coalesced metadata in catalog~~ — **shipped** (file-level `column_stats` + `Storage::read_paths`).
5. **pg_query migration (ADR 0014)** — adopt libpg_query as the canonical SQL parser, demoting sqlparser-rs to a transitional fallback and DataFusion-sql to executor-only. Phase 1 (in flight): pg_query parses every statement, drives dispatch for the 14 textual pre-screens in `executor.rs`, and rejects unsupported statement kinds with SQLSTATE 0A000. Phase 2: own `PgNode → DataFusion LogicalPlan` translator for SELECT. Phase 3: long-tail SQL (window funcs, GROUPING SETS, recursive CTEs, MERGE, LATERAL). Precedent: DuckDB, CockroachDB, Spanner-PG, YugabyteDB. Phase 1 is ~1 week of focused work; the full migration is a 1-quarter project that lands incrementally.
6. ~~Phase 5.11.A — expanded built-in function catalogue~~ — **shipped** (date/time, string, math, coalesce, aggregate; recursive-CTE + window verification pass).
7. ~~Phase 5.11.D — `CREATE MATERIALIZED VIEW` SQL surface~~ — **shipped** (CV DDL + `REFRESH MATERIALIZED VIEW` + `DROP MATERIALIZED VIEW`).
8. **B1 per-project secondary indexes** — biggest remaining point-query win. ~8 weeks. *Note (2026-05-19):* partially subsumed by Phase 5.14.A catalog blooms + 5.14.C hot tier for the OLTP `point_eq` shape; reassess scope once 5.14 lands.
9. **Phase 5.12 — Storage perf & Vortex** — **shipped** (50 perf commits in #161/#162; Vortex default 2026-05-18 per [ADR 0015](./docs/decisions/0015-vortex-storage-format.md); 88-shape `vortex_vs_parquet_smoke` battery green). Full Phase 5.12.A through 5.12.O ship-list in [TASK.md](./TASK.md) Phase 5.12.
10. ~~Phase 5.11.B — triggers via PL/pgSQL~~ — **superseded by [ADR 0012](./docs/decisions/0012-change-event-primitive.md) — see Phase 5.11.C reactors instead.** The change-event primitive (declarative lifecycle + SQL-bodied reactors) covers ~95% of trigger use cases without committing to a PL/pgSQL parser / interpreter. Already shipped in Phase 5.11.B (declarative lifecycle) and Phase 5.11.C (reactors).
11. ~~Phase 5.11.C — PL/pgSQL stored procedures (subset)~~ — **superseded by [ADR 0012](./docs/decisions/0012-change-event-primitive.md) — see Phase 5.11.C reactors instead.** Basin's procedure surface is `LANGUAGE sql` (planning-time inlining) + `CALL` procedures (multi-statement bodies), already shipped in Phase 5.11.D/E/F. PL/pgSQL with `IF`/`LOOP`/variables / `EXCEPTION` blocks / cursor-driven loops is explicit non-goal per ADR 0012.
12. **Phase 6 — production hardening** (multi-region read replicas, cross-shard 2PC, point-in-time restore extensions, branching/forking GC) — multi-month. Cloud-platform items (BYO-bucket, BYO-key, Stripe billing) live in the separate hosted-cloud repo.

---

## 1 — Extended pgwire protocol (~2 weeks) — **shipped 2026-05-01**

The single biggest "is it usable" blocker. Without this, every popular
Postgres driver fails on `Parse` with `0A000`. Now resolved.

- [x] `basin-engine`: prepared-statement API
      (`prepare(sql) -> StatementHandle`, `bind(handle, params)`,
      `execute_bound(bound)`, `describe_statement(handle)`,
      `close_statement(handle)`)
- [x] Parameter placeholder parsing (`$1`, `$2`, …) via a custom forward
      scanner that respects string-literal and comment context
- [x] Parameter type inference for INSERT VALUES + `WHERE col OP $N`
      patterns; unresolved placeholders default to TEXT
- [x] `basin-router`: full Parse / Bind / Describe / Execute / Sync /
      Close handler replacing the `0A000` fast-fail
- [x] `RowDescription` from cached statement, not from execution result
- [x] Binary parameter decoding for INT2/4/8, FLOAT4/8, BOOL, BYTEA, TEXT
- [x] Binary result-row encoding for the same type set
      (`encode_batches_with_formats`)
- [x] Smoke test: `tokio_postgres`'s default extended-query API
      (`client.query`, `client.prepare`+`query`, `client.execute`)
      runs end-to-end against two concurrent projects — 10/10 pass
- [ ] Smoke test: `asyncpg.fetch` works (Python) — deferred, low risk
      given tokio-postgres works
- [x] PoC dashboard updated: `extended_protocol` viability card

## 2 — Durable catalog (~1 week) — **shipped 2026-04-30**

Today the in-memory catalog evaporates on process restart. Parquet stays
on disk but is unindexed — effectively orphaned. Production Basin needs
the catalog persisted before it can be a database, not just a demo.

- [x] Pick the catalog backend: chose **Postgres-backed schema** (rather
      than full Iceberg-REST / Lakekeeper) for v1. Faster to ship, same
      trait surface, and the future Lakekeeper path is a drop-in
      replacement (the `RestCatalog` stub is preserved for that lane).
- [x] Implement `basin-catalog::PostgresCatalog` against the chosen
      backend; same trait surface as `InMemoryCatalog`
- [x] `basin-server`: `BASIN_CATALOG=postgres://...` env switch (default
      stays `memory` so existing tests don't break)
- [x] Atomic `append_data_files` via `SELECT ... FOR UPDATE`; optimistic
      concurrency conflict mapped to `BasinError::CommitConflict`
- [x] Cross-restart smoke test: insert via server-1, drop server-1,
      bring up server-2 against the same schema and TempDir, confirm
      rows survive
- [x] Dashboard: `durable_catalog` viability card

## 3 — WAL + fast write acks (Phase 2, ~2 months) — **v0.1 shipped 2026-05-01**

Today inserts are ~4–5× behind Postgres because the write path is
synchronous: Arrow → Parquet → ZSTD → object_store → catalog commit. The
architectural answer is a Raft-backed WAL with sub-5ms acks; Parquet
flush moves to background compaction. Closes the only wedge-relevant
metric where Basin loses to PG.

- [x] `basin-wal`: file-backed single-node v0.1 (Raft is v0.2). Append
      keyed by `(project_id, partition)`, monotonic LSN per partition
- [x] Batched flush to object storage every 200 ms or 1 MB
- [x] Recovery: list segments and replay on `Wal::open`
- [x] Bench: **57k writes/sec debug, 954k writes/sec release** —
      well above the 10k/sec spec target
- [ ] Chaos test (deferred to Raft v0.2 — single-node has no peers)
- [x] Compactor (in basin-shard): WAL → Parquet → atomic catalog
      commit → WAL truncate
- [ ] Engine integration: route INSERT through WAL+shard so the
      dashboard's insert-latency card flips green (in flight)

## 4 — Shard owners + eviction (Phase 3, ~3 months) — **v0.1 shipped 2026-05-01**

Fixes the noisy-neighbor 42× p99 degradation surfaced on the dashboard.
Enables genuinely many-project scale (every project has their own in-mem
state; idle projects evict). First time we can measure cold-start
latency.

- [x] `basin-shard`: in-process map of `(project_id, partition) → in-mem state`
- [x] Lazy load project state from WAL + Parquet on first request
- [x] Idle eviction (default 5 min) with metrics on evictions
- [x] Read path: in-RAM tail merged with Parquet base; predicate eval on tail
- [x] Background compactor: WAL → Parquet → catalog commit → WAL truncate
- [x] Per-partition `RwLock`; outer map lock held only for lookup/insert
- [ ] `basin-placement`: `(project, partition) → owner` map, etcd/FDB backed
- [ ] Consistent hashing with virtual nodes
- [ ] Fast failover: reassign shards within seconds on owner unreachable
- [ ] On-disk `last_compacted_lsn` marker so cold load doesn't re-replay
      already-compacted ranges (today: WAL truncate prevents duplicates,
      but we still scan the truncated remainder)
- [ ] Engine integration: route INSERT through shard owner instead of
      synchronous Parquet write — turns `compare_postgres` insert row from
      red to green
- [ ] Bench: cold start < 200 ms, hot point-lookup < 1 ms
- [ ] Dashboard: noisy-neighbor card flips green; cold-start card lights up

## 5c — Connection pooler (`basin-pool`) (~1 week) — scoped 2026-05-01

A thin native pooler that caches `ProjectSession` objects by
`(project_id, client_id)` and reuses them across short-lived client
connections (Lambda, Cloud Run, the per-request lifecycle).

Pgbouncer specifically does **not** work for Basin (its transaction-
pooling mode rewrites session state Basin doesn't have); a native
pooler is both smaller and better-fitted. See
[ADR 0007](./docs/decisions/0007-connection-pooling.md).

- [ ] `basin-pool` crate scaffold; `PoolConfig` (max sessions, idle TTL,
      per-project cap)
- [ ] LRU cache keyed on `(ProjectId, client_id)` with idle eviction
- [ ] `pgwire` accept loop checks the pool first, opens a fresh
      session only on miss
- [ ] Per-project cap so one project's burst can't starve others
- [ ] Metrics: hit rate, miss rate, evictions, sessions resident
- [ ] Smoke test: 1000 short-lived connections cycle through 10
      pool slots without `Engine::open_session` being called more
      than ~10 times

## 5b — Multi-region read replicas (~2-3 months) — added 2026-04-30

Founder direction expanded scope beyond the original wedge. See
[ADR 0004](./docs/decisions/0004-multi-region-read-replicas.md) — eventual-
consistent cross-region read replicas, region-local writes, no cross-region
2PC. Implementation order is in the ADR.

- [ ] `basin-region` crate scaffold; `Region` type + routing decision
- [ ] `BASIN_REGION` env var on `basin-server`
- [ ] Region column on project catalog rows
- [ ] pgwire-to-pgwire forwarder from replica regions to primary
- [ ] `PostgresCatalog::connect` accepts `read_only` for replica regions
- [ ] Postgres logical-replication setup docs
- [ ] S3 CRR bucket-config docs
- [ ] Dashboard: replication-lag and read-locality cards

## 5 — ORM compatibility (~2 weeks after #1) — **7/7 patterns 2026-05-01**

The "show me" demo for design-partner sales. If real ORM-shaped queries
run green against Basin, the door opens for adoption conversations.

- [x] Survey 7 representative ORM patterns through `tokio-postgres`
      (multi-row INSERT with params, prepared-stmt reuse, mixed-type
      WHERE, NULL params, `LIMIT $1`, single-quote escape, BYTEA
      round-trip). All 7 pass.
- [x] Add `LIMIT $N` / `OFFSET $N` placeholder type inference (Int64);
      drivers refused to bind `i64` to text-typed placeholders before
- [x] Add `BYTEA` column type — DDL parse, INSERT literal coercion,
      result-row encoding (text + binary), Arrow `Binary` round-trip
      through DataFusion's separate Arrow version
- [ ] Drive a real ORM (Diesel / SeaORM / Prisma) end-to-end against
      `basin-server`; document any new gaps
- [ ] Demo recording: 30s screencast running the app
- [x] Dashboard: `orm_compat` viability card (currently 1.0 / bar 0.85)

---

## Definition of done for the roadmap

A small SaaS team picks Basin, points their existing Prisma + Postgres
app at it, signs up 100 projects in trial, and reports back that:

1. Their existing app works without changes (compat).
2. Storage cost dropped by ≥10× vs the Postgres or Neon they came from.
3. Project onboarding/deletion is fast and per-project isolated.
4. Their team didn't have to learn a new database to use it.

When that conversation happens with a real customer, Basin is "real."
Until then, every line of code should answer the question: does this
move us closer to that conversation?
