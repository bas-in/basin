# Basin — wedge-deepening roadmap

The five things that turn Basin from "interesting PoC" into "developers can
ship a multi-tenant SaaS on it." All directly serve the wedge customer
(multi-tenant SaaS with audit-log workloads).

This is **not** a plan to beat Neon at Postgres-app workloads or Turso at
the edge. The brief is explicit: stay in the wedge until paying customers
ask for adjacent expansion. See [`TASK.md`](./TASK.md) for the full Phase
0–7 build plan; this file is the prioritized next-six-months slice.

Legend: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` deferred

---

## Roadmap status — checkpoint 2026-05-01

**The full WEDGE 1–5 + 5c slice is shipped.** All five items below are at
v0.1 with passing benchmark coverage. Workspace tests: **216 / 216 passing,
0 failures**. The dashboard stories are honest — every red card has a
documented architectural reason (single-process limit, etc.) and a future
trigger to revisit.

Three additional crates landed alongside the wedge by founder direction
and now ship as part of the open-source bundle (Phase 5.10 in
[`TASK.md`](./TASK.md)): `basin-auth` (identity, ADR 0005), `basin-rest`
(PostgREST equivalent, ADR 0006), `basin-pool` (per-tenant connection
pool, ADR 0007). All three are wired into `basin-server` behind opt-in
env vars; defaults preserve the original PoC behaviour.

**What's next, in priority order (engine / open-source repo only;
hosted-cloud product lives in a separate repo):**

1. **Phase 0 customer interviews** — strategic, not engineering. Architecture is done; what's missing is paying customers.
2. ~~Engine `UPDATE` / `DELETE` support~~ — **shipped** (Iceberg copy-on-write).
3. ~~A4 coalesced metadata in catalog~~ — **shipped** (file-level `column_stats` + `Storage::read_paths`).
4. **pg_query migration (ADR 0014)** — adopt libpg_query as the canonical SQL parser, demoting sqlparser-rs to a transitional fallback and DataFusion-sql to executor-only. Phase 1 (in flight): pg_query parses every statement, drives dispatch for the 14 textual pre-screens in `executor.rs`, and rejects unsupported statement kinds with SQLSTATE 0A000. Phase 2: own `PgNode → DataFusion LogicalPlan` translator for SELECT. Phase 3: long-tail SQL (window funcs, GROUPING SETS, recursive CTEs, MERGE, LATERAL). Precedent: DuckDB, CockroachDB, Spanner-PG, YugabyteDB. Phase 1 is ~1 week of focused work; the full migration is a 1-quarter project that lands incrementally.
5. **Phase 5.11.A — expanded built-in function catalogue** — date/time, string, math, coalesce, aggregate. Gates triggers + PL/pgSQL. ~3 weeks. The smaller "drop in your existing PG schema" win and a hard prerequisite for everything else in 5.11.
6. **Phase 5.11.D — `CREATE MATERIALIZED VIEW` SQL surface** — drop the `cv_glue` stub. ~1 week, lands once 5.11.A is in.
7. **B1 per-tenant secondary indexes** — biggest remaining point-query win. ~8 weeks.
8. **Phase 5.12 — SmithDB-inspired storage optimizations (lands after SQL compat clears 400 ✅).** LangChain's [SmithDB](https://www.langchain.com/blog/introducing-smithdb) is a Rust + DataFusion + Vortex + object-store database that ships LangSmith's agent-trace workload in production. The architecture is a cousin of Basin's; the four patterns below are battle-tested under the same constraints we operate within. Land them in this order:
   - **5.12.A — time-tiered compaction.** Replace the compactor's uniform schedule with SmithDB-style time-tiered policy: recent partitions compact often, older partitions settle into larger files. Matches Basin's audit-log target workload exactly. ~1 week. Single file: `crates/basin-shard/src/compactor.rs`.
   - **5.12.B — late materialization for big payloads.** Split JSONB and large-text columns from "core" columns at row-group write time so point queries that don't project the payload skip the body entirely. SmithDB does this for trace bodies; Basin's audit-log workload has the same shape (small core + big JSON). ~2 weeks. Touches the Parquet writer in `basin-storage` and the planner's projection pushdown. Compounds with A4 catalog stats + cluster-by for ~10× point-query latency improvement on payload-heavy tables.
   - **5.12.C — Vortex as opt-in storage format ([proposed ADR 0015](./docs/decisions/0015-vortex-storage-format.md)).** `CREATE TABLE … WITH (basin.file_format = 'vortex')` writes Vortex instead of Parquet for that table. Parquet stays default — preserves Iceberg / DuckDB / Athena / Spark read-compat for default tables. Per-table opt-in keeps the migration surface bounded. SmithDB validates Vortex at production scale (100× faster random access vs Parquet, 10-20× faster scans). ~3 weeks. New module: `crates/basin-storage/src/vortex_format.rs` implementing the same `FileFormat` trait Parquet does.
   - **5.12.D — read-from-writer's-cache extension.** Basin already merges the in-RAM WAL tail with the Parquet base on read. SmithDB extends this further: when a query lands on the same shard owner that's currently writing, scan directly from the writer's local cache (skip even the catalog round-trip for the freshest writes). ~1 week. Touches `basin-shard` read path.
9. **Phase 5.11.B — triggers** — `CREATE TRIGGER`, row + statement, `NEW`/`OLD`/`TG_OP`, recursive guard. ~2 months. **High priority but an explicit wedge expansion** — re-confirm with Phase 0 customer signal before committing.
10. **Phase 5.11.C — PL/pgSQL stored procedures (subset)** — `CREATE FUNCTION` + tree-walker interpreter. ~2 months. Ships alongside 5.11.B because triggers consume the same interpreter.
11. **Phase 6 — production hardening** (multi-region read replicas, cross-shard 2PC, point-in-time restore extensions, branching/forking GC) — multi-month. Cloud-platform items (BYO-bucket, BYO-key, Stripe billing) live in the separate hosted-cloud repo.

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
      runs end-to-end against two concurrent tenants — 10/10 pass
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
      keyed by `(tenant_id, partition)`, monotonic LSN per partition
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
Enables genuinely many-tenant scale (every tenant has their own in-mem
state; idle tenants evict). First time we can measure cold-start
latency.

- [x] `basin-shard`: in-process map of `(tenant_id, partition) → in-mem state`
- [x] Lazy load tenant state from WAL + Parquet on first request
- [x] Idle eviction (default 5 min) with metrics on evictions
- [x] Read path: in-RAM tail merged with Parquet base; predicate eval on tail
- [x] Background compactor: WAL → Parquet → catalog commit → WAL truncate
- [x] Per-partition `RwLock`; outer map lock held only for lookup/insert
- [ ] `basin-placement`: `(tenant, partition) → owner` map, etcd/FDB backed
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

A thin native pooler that caches `TenantSession` objects by
`(tenant_id, client_id)` and reuses them across short-lived client
connections (Lambda, Cloud Run, the per-request lifecycle).

Pgbouncer specifically does **not** work for Basin (its transaction-
pooling mode rewrites session state Basin doesn't have); a native
pooler is both smaller and better-fitted. See
[ADR 0007](./docs/decisions/0007-connection-pooling.md).

- [ ] `basin-pool` crate scaffold; `PoolConfig` (max sessions, idle TTL,
      per-tenant cap)
- [ ] LRU cache keyed on `(TenantId, client_id)` with idle eviction
- [ ] `pgwire` accept loop checks the pool first, opens a fresh
      session only on miss
- [ ] Per-tenant cap so one tenant's burst can't starve others
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
- [ ] Region column on tenant catalog rows
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
app at it, signs up 100 tenants in trial, and reports back that:

1. Their existing app works without changes (compat).
2. Storage cost dropped by ≥10× vs the Postgres or Neon they came from.
3. Tenant onboarding/deletion is fast and per-tenant isolated.
4. Their team didn't have to learn a new database to use it.

When that conversation happens with a real customer, Basin is "real."
Until then, every line of code should answer the question: does this
move us closer to that conversation?
