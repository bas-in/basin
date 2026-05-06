# Basin — Build Task List

Bucket-native, multi-tenant, Postgres-compatible database. Phased build per the
project brief. Every box should be small enough that a single PR closes it.

Legend: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` deferred / out of scope

---

## Local PoC milestone — reached 2026-04-30

A single-process Basin server speaks pgwire end-to-end. CREATE TABLE / INSERT
/ SELECT round-trip via `psql`; tenant data lands as Parquet under
`/tenants/{ulid}/...`; a multi-tenant smoke test proves cross-tenant
isolation across the real TCP path. Binary is `services/basin-server`.

This collapses Phase 2/3/4 production layers (Raft WAL, distributed shard
owners, placement service, full pgwire protocol) into a single in-process
pipeline (`basin-router → basin-engine → basin-storage + basin-catalog`).
It is not the production architecture — it is the smallest demoable wedge.
See `README.md` "Try the PoC" for usage.

---

## Phase 0 — Validate the wedge (1 month)

- [ ] Identify 20 candidate companies in the three target segments
      (multi-tenant SaaS, agent platforms, audit-heavy fintech)
- [ ] Run 20 customer interviews on the wedge question
- [ ] Sign 3–5 named design partners willing to try the alpha
- [ ] Write the PRD: pains, target ICP, non-goals, success metrics
- [ ] Decide go/no-go. Record the decision and the evidence in `docs/phase-0.md`

## Phase 1 — Storage substrate (2–3 months)

- [x] Cargo workspace + crate skeletons (`crates/basin-*`, `services/*`)
- [x] `basin-common`: `TenantId`, `PartitionKey`, error enum, telemetry init
- [x] `basin-storage`: write Arrow `RecordBatch` → Parquet under
      `/tenants/{id}/tables/{table}/data/yyyy/mm/dd/{ulid}.parquet`
- [x] `basin-storage`: read with predicate + projection pushdown
- [x] `basin-storage`: pluggable `object_store` backend (local fs, S3, R2, GCS
      — backend is whatever `dyn ObjectStore` the caller passes)
- [~] `basin-catalog`: Iceberg REST client (Lakekeeper-compatible)
      — trait shape locked, `RestCatalog` is a stub. Wire up in Phase 2/3.
- [x] `basin-catalog`: atomic `append_data_files` commits, snapshot listing
      (in-memory; same semantics will apply to the REST impl)
- [x] Per-tenant prefix enforcement at the storage API boundary
- [x] Integration test: 1M rows × 100 tenants, round-trip + cross-tenant isolation
      (`tests/integration/tests/phase1_substrate.rs`, runs in <2s)
- [ ] Bench: predicate pushdown reduces bytes read by ≥ 10× for selective scans
      (see also: viability suite — `tests/integration/tests/viability_README.md`)

## Phase 2 — WAL service (2 months) — **v0.1 shipped**

- [-] Pick Raft library — deferred to v0.2; v0.1 ships single-node WAL,
      same `Wal` trait so the swap is a backend change
- [x] `basin-wal`: file-backed single-node WAL, append keyed by
      `(tenant_id, partition)` with monotonic per-partition LSN
- [x] Batched flush to object storage every 200 ms or 1 MB
- [x] Recovery: list segments and replay on `Wal::open`
- [x] Bench: **57k writes/sec debug, 954k writes/sec release** (well over
      the 10k spec target)
- [-] Chaos test (deferred to Raft v0.2 — single-node has no peers to kill)

## Phase 3 — Shard owners (2–3 months) — **v0.1 shipped**

- [x] `basin-shard`: in-process map `(tenant, partition) → in-mem state`
- [x] Lazy load tenant state from WAL + Parquet on first request
- [x] Idle eviction (default 5 min) with metrics
- [x] Write path: WAL append → ack → in-mem state update
- [x] Read path: in-RAM tail merged with Parquet base; predicate eval on tail
- [x] Background compactor: WAL → Parquet → catalog commit → WAL truncate
- [-] `basin-placement` — deferred to v0.2; in-process map works for v0.1
- [-] Consistent hashing — same
- [-] Fast failover — same
- [x] Bench: hot single-row INSERT 22.3k/sec (full pipeline). Cold-start
      latency benchmark deferred (eviction works but not surfaced as a
      dashboard card yet).

## Phase 4 — Routers and SQL (3–4 months) — **mostly shipped**

- [x] `basin-router`: pgwire v3 simple + extended query (Parse/Bind/Describe/
      Execute/Close/Sync), binary + text formats for INT/FLOAT/BOOL/BYTEA/TEXT
- [x] SQL parsing + planning via DataFusion (with point-query fast path
      bypassing DataFusion for `WHERE col = literal`)
- [-] RLS predicate injection — structural per-tenant prefix isolation
      satisfies the wedge claim; in-row RLS deferred
- [-] User-defined `CREATE POLICY` — deferred (RLS uses prefix isolation)
- [-] Multi-shard fan-out + result merging — single-process today;
      Phase 3 v0.2 adds it
- [-] Single-shard transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) — deferred
- [x] Postgres types: int, bigint, text, **bytea**, **boolean**, float8, vector(N)
      — TIMESTAMPTZ partial; jsonb/uuid/numeric deferred
- [-] Indexes: btree / hash — deferred (predicate pushdown + HNSW vector
      cover the wedge)
- [-] Foreign keys — deferred
- [x] **Real ORM compat verified**: 7/7 representative ORM patterns pass via
      `tokio-postgres`'s default extended-query API
- [x] `psql` connects and runs full SQL workload
- [-] `pgx` (Go) / `asyncpg` (Python) full smoke — extended-query landed,
      these ride on it; explicit smoke tests are a follow-up

## Phase 5 — Analytical path (1–2 months)

- [ ] `basin-analytical`: pool reading Iceberg directly via DuckDB or DataFusion
- [ ] Planner heuristic to route analytical queries off the OLTP path
- [ ] Bench: 10 TB Iceberg scan completes in seconds via DuckDB

## Phase 5.5 — Sharding axes beyond per-tenant (1–3 months)

The primary sharding axis is per-tenant prefix (already structural). At
scale, four secondary axes show up; each gets its own work item with
explicit tests.

- [ ] **Within-tenant time-based partitioning**: `CREATE TABLE … PARTITION
      BY RANGE (ts)` writes new files under
      `tenants/{id}/tables/{t}/year=YYYY/month=MM/...`. Reader prunes
      partitions for time-range predicates. Iceberg `PartitionKey` is
      already plumbed; only the SQL surface + path layout + pruner pass
      are missing. Test: 1M-row scan with `WHERE ts BETWEEN ...` reads
      one partition's bytes, not all.
- [ ] **Compute sharding (router → shard owners)**: hash tenant_id →
      shard_id, route pgwire connections to the owning shard's process.
      Each shard owns the in-memory state for its tenants. All shards
      share the same R2 bucket. Test: 4-shard cluster on localhost,
      tenant requests land on the right shard 100% of runs, restart
      survival, hot-tenant rebalance.
- [ ] **Tiered storage (hot/cold)**: per-table age policy moves
      cold Parquet files to a cheaper tier (S3 IA, R2 Infrequent
      Access). Reader transparently fetches from whichever tier the
      catalog points at. Compactor enforces the policy. Test: insert
      data, mark age threshold, confirm files move + reads still work.
- [ ] **Within-tenant whale handling (sub-shard)**: a 100×-larger
      tenant gets pinned to a dedicated shard owner with bigger
      compute. Cheap because data stays in shared R2. Folds into the
      compute-sharding work — same router, just pinned mapping.

## Phase 5.7 — Point-query latency: caching + indexes + hot tier (3-6 months)

The critical path to "Postgres-replacement" credibility on point queries
without giving up the prefix-isolation wedge. Three sub-phases by
risk/effort, ordered to ship value early.

**A. Quick wins (4 weeks, high leverage, no architectural shift):**
- [ ] **A1 NVMe disk cache** — local SSD LRU between RAM and S3.
      ~50ms cold S3 fetch → ~100µs warm SSD read. Mirrors Snowflake /
      Databricks / ClickHouse Cloud architecture.
- [ ] **A2 Parquet page cache (RAM)** — LRU of decoded data pages.
      Already have footer cache; this extends to pages. Hot point query
      hits at <1ms.
- [ ] **A3 Bloom filters in Parquet footer** — opt-in per table; turns
      80%+ of "might be here" row-group scans into structural skips.
- [ ] **A4 Coalesced metadata in catalog** — keep row-group stats in
      the catalog, not the footer. Cuts cold-query round-trips from
      ~5 to ~2.

**B. Indexing + clustering (8 weeks, real Postgres-class point queries):**
- [ ] **B1 Per-tenant secondary indexes** — B-tree mapping
      `(table, indexed_col) → (file, row_group, row)`. Stored as a
      separate per-tenant file. Cached in RAM. `CREATE INDEX` SQL.
- [ ] **B2 Range-partitioned / Z-ordered files** — `CLUSTER BY` on
      `CREATE TABLE` physically sorts data so related rows live in the
      same file. Combined with A3 bloom filters, point queries hit
      one file.
- [ ] **B3 Per-table row-group sizing** — smaller row groups (4k rows)
      for point-heavy tables; trade scan throughput for seek
      granularity.

**C. Hot tier (6-8 weeks, the architectural commitment):**
- [ ] **C1 In-memory hot ring** — per-tenant ring buffer for recent
      writes (last 5 min or 100k rows, whichever first). Flushes to
      Parquet on threshold or timer. Reads check ring first, fall
      through to Parquet. Solves "90% of reads are on last week of
      data" for audit-log / event-store / time-series shapes.
- [ ] **C2 Embedded RocksDB hot tier (alt path)** — for tenants with
      larger hot working sets that don't fit in RAM. Optional, gated
      by tenant config.

Decision points:
- A1+A2+A3 alone may be enough to ship sub-10ms warm point queries —
  measure first, then commit to B/C only if needed.
- C is the architectural shift; only commit when a real customer
  workload demands it (multi-week effort, opens new ops surface).

## Phase 5.8 — pg_cron + http extension SQL surface (3 weeks)

- [ ] **basin-cron** — pg_cron-compat scheduler. `cron.schedule(name,
      schedule, sql)` etc. Per-tenant `cron.job` + `cron.job_run_details`.
      Background runner per shard.
- [ ] **basin-net** — pg_net + http extension SQL surface.
      `http_get`/`http_post` (sync), `net.http_*` (async). Per-tenant
      rate limit + URL allowlist (SSRF guard) + body cap + timeout.
      Combined with basin-cron = full "scheduled HTTP work" without
      Edge Functions.

## Phase 5.6 — Row-level security (1 month)

- [ ] `CREATE POLICY` SQL surface (Postgres-compatible syntax).
- [ ] Catalog stores per-tenant per-table policies.
- [ ] Engine injects predicate filters for `current_user` /
      `current_role` into every query plan; bypassable only by table
      owner.
- [ ] Tests: same-table queries return different rows for different
      authenticated principals; tenant isolation invariant holds when
      RLS is enabled.

## Phase 6 — Production hardening (3–4 months)

- [ ] Multi-region: regional WAL + S3 cross-region replication
- [ ] Catalog replication strategy chosen and implemented
- [ ] Point-in-time restore via Iceberg snapshots
- [ ] Branching / forking via copy-on-write catalog metadata
- [ ] Cross-shard 2PC
- [ ] Connection pooling, rate limiting, cost-based query rejection
- [ ] BYO-bucket: customer S3 + IAM role, platform writes into theirs
- [ ] BYO-key: customer KMS, platform never sees plaintext
- [ ] Per-tenant + per-query + per-shard + per-WAL telemetry
- [ ] Stripe billing integration: active hours, ops, storage

## Phase 7 — Launch (ongoing)

- [ ] Onboard the Phase 0 design partners
- [ ] Customer dashboard + CLI + docs
- [ ] Open beta after 3–6 months of design partner usage
- [ ] GA when uptime + perf + DX are all genuinely good

---

## Cross-cutting (start now, never finish)

- [ ] Per-tenant metrics from day one (ops/s, p50/p99, RAM, S3 IO, active hours)
- [ ] OpenTelemetry traces wired through router → shard → WAL
- [ ] Cross-tenant fuzz tests (find a bug → file a P0)
- [ ] Bug bounty program before public beta
- [ ] Security review at each phase boundary

## Critical rules (from the brief — re-read before scope-creep)

- Don't build Raft, the SQL parser, the table format, or the analytical engine.
- The WAL is the durability boundary, **not** S3.
- Cold start under 200 ms or hobbyists pick Turso.
- One leaked row across tenants and the project dies.
- If you start implementing distributed 2PC + MVCC + a SQL planner from scratch,
  stop — you've drifted from the wedge.
