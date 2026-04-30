<p align="center">
  <img src="./basin.svg" alt="Basin" width="180">
</p>

<h1 align="center">Basin</h1>

<p align="center">
  <strong>Bucket-native, multi-tenant, Postgres-compatible.</strong><br>
  One database that scales smoothly from a 10 MB hobbyist tenant to a 100 TB enterprise tenant — without a tier wall, a forced migration, or a rewrite.
</p>

<p align="center">
  <a href="./WEDGE.md"><img alt="status: pre-alpha" src="https://img.shields.io/badge/status-pre--alpha-orange?style=flat-square"></a>
  <a href="./benchmark/RESULTS.md"><img alt="benchmark: 13/14 passing" src="https://img.shields.io/badge/benchmark-13%2F14_passing-brightgreen?style=flat-square"></a>
  <a href="./CAPABILITIES.md"><img alt="capabilities: matrix" src="https://img.shields.io/badge/capabilities-matrix-blue?style=flat-square"></a>
  <a href="./docs/decisions/"><img alt="decisions: ADR-tracked" src="https://img.shields.io/badge/decisions-ADR--tracked-blueviolet?style=flat-square"></a>
  <a href="./LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey?style=flat-square"></a>
</p>

---

## What it is

Basin looks like Postgres to your application — pgwire on the wire, SQL in your editor, your existing ORM Just Works — but stores its data as Parquet files in object storage with a thin compute layer in front for transactional speed. **Tenants are first-class:** every table is implicitly partitioned by tenant, every byte on disk lives under a per-tenant prefix, and idle tenants cost essentially nothing.

| | Basin | Postgres / Neon | Supabase | Turso |
|---|---|---|---|---|
| **Multi-tenant** at scale (mostly-idle tenants) | First-class | per-project pricing | shared DB, logical RLS | many DBs, no shared SQL |
| **Storage cost** for log-shaped data | **11–20× smaller** than CSV / heap | baseline | baseline | baseline |
| **Per-tenant IAM isolation** | Bucket prefix is the boundary | not really | not really | per-DB |
| **Tenant deletion** | `O(file count)`, sub-second | `DELETE` or `DROP DATABASE` | `DELETE` | `DROP DATABASE` |
| **Postgres compatibility** | "compatible enough" — psql, ORMs, prepared statements | full | full | none (SQLite) |
| **Cold start** | targeting < 200 ms | seconds | instant (shared) | < 50 ms |
| **Best for** | many tenants, audit-heavy data, BYO-bucket compliance | one big DB | one big DB | edge / many tiny DBs |

If your workload is a single Postgres app, **use Postgres**. If your workload is at the edge, **use Turso**. If your workload is many tenants where storage cost and isolation dominate — keep reading.

## Try the proof-of-concept

```sh
# 1. Start the server.
BASIN_BIND=127.0.0.1:5433 \
BASIN_DATA_DIR=/tmp/basin \
BASIN_TENANTS='alice=*,bob=*' \
cargo run -p basin-server

# 2. Connect with any Postgres driver — psql, tokio-postgres, asyncpg, JDBC.
psql -h 127.0.0.1 -p 5433 -U alice
```

```sql
CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL);
INSERT INTO events VALUES (1, 'hello'), (2, 'world');
SELECT * FROM events WHERE id = 2;

-- Native vector search, no extension required:
CREATE TABLE docs (id BIGINT, embedding vector(384));
INSERT INTO docs VALUES (1, '[0.01, 0.02, ...]');
SELECT id FROM docs ORDER BY embedding <-> '[...]' LIMIT 10;
```

The data lands as Parquet on disk under `/tmp/basin/tenants/{ulid}/...` — one prefix per tenant, by design. Persist tables across restarts by pointing the catalog at Postgres:

```sh
BASIN_CATALOG=postgres://localhost:5432/postgres cargo run -p basin-server
```

## What the benchmarks say

The full report regenerates on every test run: see [`benchmark/RESULTS.md`](./benchmark/RESULTS.md) and the live dashboard at [`benchmark/index.html`](./benchmark/index.html) (just open it; no server needed).

| Claim | Bar | Measured |
|---|---|---|
| Audit-log Parquet ≥ 10× smaller than CSV | ≥ 10× | **20.22×** |
| Idle tenants stay cheap (1 000 in one process) | < 500 KiB / tenant | **0.77 KiB / tenant** |
| Selective query reads ≪ full scan | ≥ 10× reduction | **46.8×** |
| Tenant deletion is fast (1 000 files) | < 1 000 ms | **34 ms** |
| Cross-tenant isolation holds under load | 0 leaks / 2 000 ops | **0 leaks** |
| 10 M-row point query stays bounded | < 1 000 ms | **31 ms** |
| Native vector search beats brute-force | ≥ 1.3× speed-up | **1.72×** |
| Postgres extended-query protocol works | ratio = 1.0 | **10 / 10 ORM patterns pass** |
| Catalog survives process restart | 1 / 1 | **rows recovered** |

**Honest losses to fix:**

- Insert latency: ~3–5× behind Postgres today. The synchronous Parquet path is the cause; the **WAL** (Phase 2, in flight) makes inserts ack on quorum-disk in milliseconds.
- Single-process noisy-neighbor: a tenant doing full scans degrades a quiet tenant's p99. The **shard owner** (Phase 3, in flight) puts each tenant on its own slice with eviction.

Both are well-understood. Neither is "Basin is wrong" — they are "Basin's full architecture isn't built yet."

## Architecture

Four layers, each with one job:

```
   pgwire clients  (any Postgres driver)
          │
          ▼
   Routers (stateless)        parses SQL, applies RLS, routes by tenant
          │
          ▼
   Shard owners (stateful)    in-memory state for many tenants per process,
          │                   eviction on idle
          ▼
   Regional WAL (Raft)        ordered, durable; flushes to S3 every ~200 ms
          │
          ▼
   Object storage + catalog   /tenants/{id}/... — Parquet + Iceberg metadata
```

The full read / write paths and the durability rule (the WAL is the durability boundary, not S3) are in [`docs/architecture.md`](./docs/architecture.md).

## Project layout

```
crates/
  basin-common      shared types, errors, telemetry
  basin-storage     Parquet + object_store under tenant prefixes
  basin-catalog     Iceberg-style catalog (in-memory + Postgres-backed)
  basin-engine      DataFusion SQL execution, per-tenant sessions
  basin-router      pgwire v3 (simple + extended query)
  basin-vector      native HNSW vector search
  basin-wal         (Phase 2) write-ahead log
  basin-shard       (Phase 3) stateful shard owner
  basin-placement   (Phase 3) (tenant, partition) → owner mapping
  basin-analytical  (Phase 5) DuckDB / DataFusion against Iceberg directly
services/
  basin-server      single-process PoC binary
benchmark/          dashboard + auto-generated RESULTS.md
docs/
  architecture.md   the four-layer stack, in detail
  decisions/        ADRs — every "no" with the trigger that would change our mind
  sql-compatibility.md
tests/integration/  cross-crate smoke + viability + scaling tests
```

## Status

| Phase | Description | Status |
|---|---|---|
| **0** | Validate the wedge — customer interviews, design partners | open |
| **1** | Storage substrate — Parquet on object_store, Iceberg-style catalog | done |
| **2** | WAL service — Raft-backed, sub-5 ms write acks | in flight |
| **3** | Shard owners — per-tenant state, eviction, compactor | in flight |
| **4** | Routers + SQL — pgwire, RLS, multi-shard fan-out | extended-query done; rest pending |
| **5** | Analytical path — DuckDB / DataFusion direct on Iceberg | open |
| **6** | Production hardening — multi-region, BYO-bucket, BYO-key, billing | scoped (ADRs) |
| **7** | Launch | open |

Full plan: [`TASK.md`](./TASK.md). Six-month wedge slice: [`WEDGE.md`](./WEDGE.md).

## Build and test

```sh
cargo build --workspace                            # debug build
cargo test  --workspace                            # full test suite

cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py                        # regenerate RESULTS.md + dashboard
```

## What Basin is **not**

Per [ADR 0001](./docs/decisions/0001-single-region-only.md) and [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md):

- Not a Postgres replacement for high-frequency single-tenant OLTP — use Postgres.
- Not a data lake — it can feed one (Parquet on S3 is the substrate), but the analytical path is a separate code path.
- Not embedded — use SQLite.
- Not globally strongly-consistent — use Spanner / CockroachDB.
- Not a Postgres extension host — `pg_vector` is replaced by [native vector search](./docs/decisions/0003-native-vector-search.md); PostGIS / TimescaleDB / Citus are out of scope until a paying customer requires them.

## License

Licensed under the **Apache License, Version 2.0**. See [`LICENSE`](./LICENSE).

Contributions welcome. The project is opinionated about scope (see [`docs/decisions/`](./docs/decisions/)); please open an issue before writing a PR that adds surface area.
