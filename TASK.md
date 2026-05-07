# Basin — Build Task List

Bucket-native, multi-tenant, Postgres-compatible database. Phased build per the
project brief. Every box should be small enough that a single PR closes it.

**Scope:** this file is the **core DB / open-source** roadmap — pgwire /
SQL / storage / catalog / query engine / multi-tenancy / caches /
indexes / WAL / compactor / vector search / Postgres-extension
equivalents. The customer-facing **cloud platform** (identity, REST,
V8 edge functions, BYO-bucket / BYO-key, Stripe billing, customer
dashboard) lives in [`CLOUD_ROADMAP.md`](./CLOUD_ROADMAP.md). Don't
blur the line — we keep the core engine's scope discipline by keeping
the platform's open boxes out of this file.

**Postgres-extension equivalents we ship natively** (not via upstream
`.so` loading — see [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)):

| Postgres extension | Basin crate | Section | v0.1 status |
|---|---|---|---|
| `pg_cron` (scheduler) | `basin-cron` | Phase 5.8 | ✅ shipped |
| `pg_net` + `http` (HTTP from SQL) | `basin-net` | Phase 5.8 | ✅ shipped |
| `PostGIS` (subset: points + distance + dwithin + contains) | `basin-geo` | Phase 5.9 | ✅ shipped |
| `pg_trgm` (fuzzy text) | `basin-trgm` | Phase 5.9 | ✅ shipped |
| `TimescaleDB` continuous aggregates | `basin-cv` | Phase 5.9 | ✅ shipped |
| `pgcrypto` + `uuid-ossp` (digests, bcrypt, UUIDs) | engine UDFs | Phase 5.9 | ✅ shipped |
| `pg_vector` | native `vector(N)` + HNSW | Phase 4 / vector-search | ✅ shipped |

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
- [x] Bench: predicate pushdown reduces bytes read by ≥ 10× for selective
      scans — `tests/integration/tests/viability_predicate_pushdown.rs`
      asserts `full_scan_bytes / point_query_bytes >= 10x` and emits the
      ratio to the dashboard. (see also: viability suite — `tests/integration/tests/viability_README.md`)

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
- [x] RLS predicate injection — shipped in Phase 5.6, see below
- [x] User-defined `CREATE POLICY` — shipped in Phase 5.6, see below
- [x] Multi-shard fan-out + result merging — router → shard-owner protocol
      shipped (consistent hashing, 28% max load); cross-shard JOIN deferred
- [-] Single-shard transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) — deferred
- [x] Postgres types: int, bigint, text, **bytea**, **boolean**, float8,
      vector(N), **jsonb**, **uuid**, **timestamptz**
      — numeric still deferred
- [-] Indexes: btree / hash — deferred (predicate pushdown + HNSW vector
      cover the wedge)
- [-] Foreign keys — deferred
- [x] **Real ORM compat verified**: 7/7 representative ORM patterns pass via
      `tokio-postgres`'s default extended-query API
- [x] `psql` connects and runs full SQL workload
- [-] `pgx` (Go) / `asyncpg` (Python) full smoke — extended-query landed,
      these ride on it; explicit smoke tests are a follow-up

## Phase 5 — Analytical path (1–2 months) — **v0.1 shipped**

- [x] `basin-analytical`: pool reading Iceberg directly via DuckDB
      (4.6× faster than DataFusion on 1M-row aggregates; LocalFS-only,
      S3 via DuckDB httpfs deferred to v0.2)
- [x] Planner heuristic to route analytical queries off the OLTP path
      (aggregate / GROUP BY / `/*+ analytical */` hint)
- [-] Bench: 10 TB Iceberg scan — deferred (1M-row covers the wedge)

## Phase 5.5 — Sharding axes beyond per-tenant (1–3 months)

The primary sharding axis is per-tenant prefix (already structural). At
scale, four secondary axes show up; each gets its own work item with
explicit tests.

- [x] **Within-tenant time-based partitioning**: `CREATE TABLE … PARTITION
      BY RANGE (ts)`; reader prunes partitions for time-range predicates.
- [x] **Compute sharding (router → shard owners)**: consistent hashing
      tenant_id → shard_id; 28% max load skew measured; restart survival.
      Cross-shard JOIN deferred.
- [x] **Tiered storage (hot/cold)**: `ALTER TABLE … SET cold_after = N`;
      compactor moves files between tiers on a sweep. R2 Infrequent
      Access wired in.
- [x] **Within-tenant whale handling (sub-shard)**: tenant pinning via
      `BASIN_TENANT_PINS=ulid:idx,...` env var; pinned tenants always
      land on the configured shard endpoint regardless of consistent
      hash. v0.2 will move pins into the catalog so they survive cluster
      restart and can be edited at runtime. Original line preserved
      below for context:
      **Within-tenant whale handling (sub-shard)**: a 100×-larger
      tenant gets pinned to a dedicated shard owner with bigger
      compute. Cheap because data stays in shared R2. Folds into the
      compute-sharding work — same router, just pinned mapping.

## Phase 5.7 — Point-query latency: caching + indexes + hot tier (3-6 months)

The critical path to "Postgres-replacement" credibility on point queries
without giving up the prefix-isolation wedge. Three sub-phases by
risk/effort, ordered to ship value early.

**A. Quick wins (4 weeks, high leverage, no architectural shift):**
- [x] **A1 NVMe disk cache** — LRU on local SSD; ~50ms cold S3 fetch →
      ~100µs warm SSD read. **101× speedup measured.** Default-on.
- [x] **A2 Parquet page cache (RAM)** — LRU of decoded RecordBatches;
      <1ms warm hits. **7.24× speedup measured.** Default-on.
- [x] **A3 Bloom filters in Parquet footer** — opt-in per table via
      `ALTER TABLE … SET BLOOM FILTERS ON (col)`; ~80% of nonexistent-id
      queries become row-group skips.
- [x] **A4 Coalesced metadata in catalog** — file-level coalesced
      stats (min / max / null counts per column) lifted from the
      Parquet footer into `DataFileRef::column_stats`; `Storage::read_paths`
      reads only catalog-pruned paths so a fully-out-of-range predicate
      completes with zero object-store IO (no LIST, no HEAD, no footer
      GET). v0.1 prunes at file granularity; row-group-level coalesced
      stats deferred to v0.2 (subsumed by B1 secondary indexes).
      **Unblocks tightening `s3_scaling_perf_stack` bars back to
      ≥5× p50 / <3000ms p99 once a separate measurement run lands —
      bars not moved here pending that.**

**B. Indexing + clustering (8 weeks, real Postgres-class point queries):**
- [ ] **B1 Per-tenant secondary indexes** — B-tree mapping
      `(table, indexed_col) → (file, row_group, row)`. Stored as a
      separate per-tenant file. Cached in RAM. `CREATE INDEX` SQL.
      **Flagged as the biggest remaining point-query win in CAPABILITIES.md.**
- [ ] **B2 Range-partitioned / Z-ordered files** — `CLUSTER BY` on
      `CREATE TABLE` physically sorts data so related rows live in the
      same file. Combined with A3 bloom filters, point queries hit
      one file.
- [x] **B3 Per-table row-group sizing** — `ALTER TABLE … SET row_group_rows = N`
      ships; small row groups for point-heavy tables.

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

## Phase 5.8 — pg_cron + http extension SQL surface (3 weeks) — **v0.1 shipped**

- [x] **basin-cron** — `cron.schedule(name, schedule, sql)` +
      `cron.unschedule` + `cron.job` + `cron.job_run_details`.
      Background runner per shard. SQL surface (`cron_glue`) lands in v0.2.
- [x] **basin-net** — sync `http_get`/`http_post`; async `net.http_post`
      with `net._http_response` table. Per-tenant URL allowlist (DENY-ALL
      default), 10 req/s rate limit (burst 30), 10 MiB body cap, 30s timeout.
      SQL surface lands in v0.2.

## Phase 5.6 — Row-level security (1 month) — **v0.1 shipped**

- [x] `CREATE POLICY` SQL surface (Postgres-compatible syntax).
- [x] Catalog stores per-tenant per-table policies.
- [x] Engine injects predicate filters for `current_user` /
      `current_role` at the logical-plan layer.
- [x] Tests: same-table queries return different rows for different
      authenticated principals; cross-tenant leak invariant verified.

## Phase 5.9 — Postgres-extension equivalents (ongoing) — **v0.1 shipped**

Same SQL semantics, native Rust crates, no upstream extension loading
(per ADR 0002). All ship Rust API + integration test in v0.1; SQL
surface lands in v0.2 once engine planner is extended to register
the corresponding `ScalarUDF`s.

- [x] **basin-cv** — TimescaleDB continuous-aggregate equivalent. `CvSpec`
      + `CvRefresher::tick`; per-tenant materialization. v0.1 full
      re-execution; incremental refresh deferred to v0.2.
- [x] **basin-trgm** — pg_trgm equivalent. `similarity`,
      `word_similarity`, `extract`. v0.1 brute-force; GIN trigram index
      deferred to v0.2.
- [x] **basin-geo** — PostGIS subset. `Point`, `Box2d`, `ST_MakePoint`,
      `ST_X`, `ST_Y`, `ST_Distance` (Haversine WGS84), `ST_DWithin`,
      `ST_Contains`. No `LINESTRING`/`POLYGON`/spatial index in v0.1.
- [x] **JSONB type** — Arrow `LargeBinary` + field metadata
      `BASIN_TYPE=JSONB`; canonical-form normalization on insert; pgwire OID 3802.
- [x] **UUID type** — Arrow `FixedSizeBinary(16)` + field metadata
      `BASIN_TYPE=UUID`; pgwire OID 2950 with canonical hyphenated text.
- [x] **pgcrypto / uuid-ossp UDFs** — `digest` (md5/sha1/sha224/256/384/512),
      `encode`/`decode` (hex/base64/escape), `crypt` (bcrypt),
      `gen_salt('bf')`, `gen_random_uuid()`, `uuid_generate_v4()`.

## Phase 6 — Production hardening (3–4 months)

- [ ] Multi-region: regional WAL + S3 cross-region replication
- [ ] Catalog replication strategy chosen and implemented
- [ ] Point-in-time restore via Iceberg snapshots
- [ ] Branching / forking via copy-on-write catalog metadata
- [ ] Cross-shard 2PC
- [x] Connection pooling (✅ ADR 0007), rate limiting (✅ pgwire side:
      `BASIN_PGWIRE_RATE_LIMIT_QPS=100` token-bucket per tenant via
      `governor`, mapped to SQLSTATE 53400; basin-net side ✅), cost-based
      query rejection (✅ v0.1: `BASIN_QUERY_COST_LIMIT_ROWS=N` rejects
      single-table SELECTs that estimate above the cap with SQLSTATE
      54000; multi-FROM / JOIN / sub-query / explicit-LIMIT pass through
      unchecked. v0.2 will use A4 catalog `ColumnStats` for selectivity-
      aware estimates on multi-table shapes.)
- [ ] Per-tenant + per-query + per-shard + per-WAL telemetry

> Cloud-platform hardening items (BYO-bucket, BYO-key, Stripe billing)
> moved to [`CLOUD_ROADMAP.md`](./CLOUD_ROADMAP.md).

## Phase 7 — Launch (ongoing)

- [ ] Onboard the Phase 0 design partners
- [ ] CLI (`basinctl`) + engineering docs (the customer-facing dashboard
      moved to [`CLOUD_ROADMAP.md`](./CLOUD_ROADMAP.md))
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
