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

## Phase 2 — WAL service (2 months)

- [ ] Pick Raft library (`openraft` vs `tikv/raft-rs`); record the decision
- [ ] `basin-wal`: 3-node Raft group, append keyed by `(tenant_id, partition)`
- [ ] Batched flush to S3 every 200 ms or 1 MB
- [ ] Recovery: replay WAL segments from S3 on cold boot
- [ ] Bench: 10k writes/sec sustained, p99 < 5 ms, no data loss under node kill
- [ ] Chaos test: kill leader mid-batch, verify no committed write is lost

## Phase 3 — Shard owners (2–3 months)

- [ ] `basin-shard`: process holding `(tenant_id, partition) → in-mem state`
- [ ] Lazy load tenant state from WAL + Parquet on first request
- [ ] Idle eviction (default 5 min) with metrics on evictions
- [ ] Write path: WAL append → ack → in-mem state update
- [ ] Read path: point-lookup by primary key from RAM
- [ ] Background compactor: WAL segments → Parquet, atomic catalog commit
- [ ] `basin-placement`: `(tenant, partition) → owner` map, etcd/FDB backed
- [ ] Consistent hashing with virtual nodes; per-tenant overrides
- [ ] Fast failover: reassign shards within seconds on owner unreachable
- [ ] Bench: cold start < 200 ms, hot point-lookup < 1 ms

## Phase 4 — Routers and SQL (3–4 months)

- [ ] `basin-router`: pgwire v3 (auth, query, prepared statements, COPY)
- [ ] SQL parsing + planning via DataFusion
- [ ] RLS predicate injection: `tenant_id = $current_tenant` always
- [ ] User-defined `CREATE POLICY` parsing matching Postgres syntax
- [ ] Multi-shard fan-out + result merging
- [ ] Single-shard transactions (BEGIN/COMMIT/ROLLBACK)
- [ ] Postgres types: int, bigint, text, timestamptz, jsonb, uuid, numeric, bytea
- [ ] Indexes: btree, hash
- [ ] Foreign keys (single-shard only)
- [ ] Compatibility: pick one ORM (Prisma) and run its test suite green
- [ ] Connect with `psql`, `pgx` (Go), `asyncpg` (Python) — full smoke

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
