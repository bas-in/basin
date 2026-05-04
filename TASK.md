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
