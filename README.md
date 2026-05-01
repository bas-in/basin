<p align="center">
  <img src="./basin.svg" alt="Basin" width="180">
</p>

<h1 align="center">Basin</h1>

<p align="center">
  <strong>The multi-tenant Postgres-compatible database that lives on object storage.</strong><br>
  Per-tenant isolation, 10× cheaper storage than Postgres, single architecture from one tenant to a hundred thousand.
</p>

<p align="center">
  <a href="./WEDGE.md"><img alt="status: pre-alpha" src="https://img.shields.io/badge/status-pre--alpha-orange?style=flat-square"></a>
  <a href="./benchmark/RESULTS.md"><img alt="benchmarks" src="https://img.shields.io/badge/benchmarks-15%2F16_passing-brightgreen?style=flat-square"></a>
  <a href="./benchmark/RESULTS.md"><img alt="vs Postgres: disk 12.5x smaller" src="https://img.shields.io/badge/vs_postgres-disk_12.5%C3%97_smaller-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS.md"><img alt="vs Postgres: 10x more conns" src="https://img.shields.io/badge/vs_postgres-10%C3%97_more_conns-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS.md"><img alt="vs Postgres: 47x less RAM/conn" src="https://img.shields.io/badge/vs_postgres-47%C3%97_less_RAM%2Fconn-blue?style=flat-square"></a>
  <a href="./CAPABILITIES.md"><img alt="capabilities" src="https://img.shields.io/badge/capabilities-matrix-blue?style=flat-square"></a>
  <a href="./docs/decisions/"><img alt="decisions" src="https://img.shields.io/badge/decisions-ADR_tracked-blueviolet?style=flat-square"></a>
  <a href="./LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey?style=flat-square"></a>
</p>

---

## Why Basin

Multi-tenant SaaS built on Postgres hits a wall: per-project pricing destroys the unit economics, RLS isolation is logical-only, audit-log retention costs more than the application revenue. Basin keeps the **Postgres wire protocol and SQL** your apps already speak, but stores every byte as ZSTD-compressed Parquet under a **per-tenant bucket prefix**. Idle tenants cost essentially nothing. Active tenants get sub-millisecond writes through a Raft-batched WAL. Tenant deletion is a prefix delete, not a `DELETE` cascade.

One architecture from a 10 MB hobbyist tenant to a 100 TB enterprise tenant. No tier wall. No forced migrations. No "you've outgrown us" conversation.

---

## The numbers (vs Postgres 18, same workload, same hardware)

### Storage and query performance

| | Basin | Postgres | Result |
|---|---|---|---|
| **On-disk size** (1 M audit-log rows) | **8.1 MiB** | 96.5 MiB | **Basin wins 12.5×** |
| **Compression vs CSV** (audit logs) | **20.22×** smaller | ~1× | Basin wins decisively |
| **Point query p50** (no index either side) | **4.04 ms** | 14.08 ms | **Basin wins 3.48×** |
| **Insert 1 M rows** (100 × 10k batches) | **2.41 s** | 2.61 s | **Basin wins 1.09×** |
| **Single-row INSERT throughput** (full pipeline) | **22,300 / sec** | n/a | 0.045 ms per insert |
| **Predicate pushdown** (1-of-1M point query) | **46.8×** byte reduction | requires an index | Basin wins without an index |

### Server lifecycle and concurrency

| | Basin | Postgres | Result |
|---|---|---|---|
| **Connections held under 1,000-conn flood** | **1,000** | 100 (hard wall) | **Basin wins 10×, structural** |
| **Refused connections under flood** | **0** | 900 | Basin doesn't refuse a single one |
| **RAM per held-open connection** | **165 KiB** | 7,895 KiB | **Basin wins 47.7×** |
| **Connection-accept latency p50** | **1.17 ms** | 1.37 ms | Basin wins 1.17× |

The connection-scaling and RSS results are the **structural** advantage of being a from-scratch Rust-on-tokio server vs Postgres's fork-per-connection model. They don't shrink as the workload grows; they get more dramatic.

### Multi-tenant lifecycle

| | Basin | Postgres | Result |
|---|---|---|---|
| **Idle-tenant RAM cost** (1,000 tenants) | **1.2 KiB / tenant** | ~10 MB / tenant (one DB each) | **Basin wins ~10,000×** |
| **Schema migration** `ADD COLUMN` (100K rows, no default) | **0.04 ms** (metadata-only) | 1.45 ms | Basin wins ~37× |
| **Tenant deletion** (100K rows / 100 files, local FS) | 4.77 ms | 3.47 ms | PG wins 0.73× — honest, see below |
| **Cross-tenant isolation under 2,000 mixed ops** | **0 leaks** | n/a | Structural via bucket prefix |
| **Noisy-neighbor p99 degradation** | **2.27×** | n/a | Passes the < 5× bar |

> **Honest mixed result on tenant deletion at 100K rows on local FS:** PG's `DROP SCHEMA CASCADE` is a few catalog rows and an `unlink`; Basin's `O(file count)` deletion does 100 separate `delete()` calls. The wedge claim — bucket-prefix delete vs vacuum / extent walks — surfaces at scale (multi-GB tenants on S3), not on a small tmpfs table. Reported as-measured; the dashboard tells the full story.

Full live dashboard: open [`benchmark/index.html`](./benchmark/index.html) directly (no server needed).
Auto-regenerated Markdown report: [`benchmark/RESULTS.md`](./benchmark/RESULTS.md).

---

## Quick tour

Boot the server:

```sh
BASIN_BIND=127.0.0.1:5433 \
BASIN_DATA_DIR=/tmp/basin \
BASIN_TENANTS='alice=*,bob=*' \
BASIN_CATALOG=postgres://localhost:5432/postgres \
cargo run -p basin-server
```

Connect with **any Postgres driver** — `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, your existing ORM:

```sh
psql -h 127.0.0.1 -p 5433 -U alice
```

Run real SQL:

```sql
-- Standard tables, standard SQL.
CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL);
INSERT INTO events VALUES (1, 'hello'), (2, 'world');
SELECT * FROM events WHERE id = 2;

-- Native vector search, no pg_vector required.
CREATE TABLE docs (id BIGINT, embedding vector(384));
INSERT INTO docs VALUES (1, '[0.01, 0.02, ...]');
SELECT id FROM docs ORDER BY embedding <-> '[...]' LIMIT 10;
```

Confirm the data hit object storage under the tenant prefix:

```sh
find /tmp/basin/tenants -name '*.parquet'
# /tmp/basin/tenants/01HABCD…/tables/events/data/2026/05/01/01HEFG….parquet
# /tmp/basin/tenants/01HABCD…/tables/docs/data/...
```

That's a real bucket-native database. Per-tenant prefix isolation is the IAM boundary; one bucket policy revokes all access to a tenant's data even if every other layer is bypassed.

---

## Architecture

Four layers, each with one job:

```
   pgwire clients  (any Postgres driver — psql, tokio-postgres, asyncpg, JDBC)
          │
          ▼
   Routers (stateless)        parses SQL, applies RLS, routes by tenant
          │
          ▼
   Shard owners (stateful)    in-memory state for many tenants per process,
          │                   eviction on idle, lazy load from WAL + Parquet
          ▼
   Regional WAL (Raft)        durable in milliseconds; flushes to S3 every ~200 ms
          │
          ▼
   Object storage + catalog   /tenants/{id}/... Parquet + Iceberg-style metadata
                              local FS, S3, R2, GCS — same code, different bucket
```

The full architecture document is in [`docs/architecture.md`](./docs/architecture.md). Every "no" we've recorded is in [`docs/decisions/`](./docs/decisions/).

**Built on:** Apache Arrow · Apache Iceberg (table format) · Apache Parquet · Apache DataFusion (SQL planner) · Tokio · pgwire-rs · openraft (planned for the WAL's distributed v0.2). Pure Rust, `#![forbid(unsafe_code)]` across every crate.

---

## What you can do today

- **Multi-tenant Postgres-compatible SQL** — pgwire v3, simple + extended query protocol. Works with `psql`, `tokio-postgres`, `asyncpg`, Diesel, SeaORM, every ORM that speaks Postgres.
- **CRUD** — `CREATE TABLE`, `INSERT`, `SELECT` with `WHERE` / `ORDER BY` / `LIMIT`, `SHOW TABLES`. Prepared statements with parameter bind.
- **Native vector search** — `vector(N)` column type, `<->` / `<#>` / `<=>` operators, HNSW index per Parquet segment. No `pg_vector` extension required.
- **Per-tenant isolation** — bucket prefix is the IAM boundary; tested under concurrent load with zero cross-tenant leaks.
- **Durable catalog** — Postgres-backed; tables and snapshots survive process restart.
- **Cheap retention** — ZSTD-1 compression on Parquet, 12.5× smaller than Postgres heap on audit-log data.
- **Fast inserts** — Raft-batched WAL, ~0.056 ms per single-row INSERT through the full pipeline.
- **Cheap idle tenants** — 1.2 KiB of RAM per provisioned tenant in the catalog.

The full capability matrix (with what's planned and what's deferred): [`CAPABILITIES.md`](./CAPABILITIES.md).

---

## Status

| Phase | Description | Status |
|---|---|---|
| **0** | Validate the wedge — customer interviews, design partners | open |
| **1** | Storage substrate — Parquet on object_store, Iceberg-style catalog | shipped |
| **2** | WAL service — Raft-backed, sub-5 ms write acks | v0.1 shipped (single-node; Raft is v0.2) |
| **3** | Shard owners — per-tenant state, eviction, compactor | v0.1 shipped (in-process; placement service is v0.2) |
| **4** | Routers + SQL — pgwire, RLS, multi-shard fan-out | extended-query shipped; RLS pending |
| **5** | Analytical path — DuckDB / DataFusion direct on Iceberg | open |
| **6** | Production hardening — multi-region, BYO-bucket, BYO-key, billing | scoped (ADRs 0001, 0004) |
| **7** | Launch | open |

Six-month wedge slice: [`WEDGE.md`](./WEDGE.md). Full plan: [`TASK.md`](./TASK.md).

---

## Project layout

```
crates/
  basin-common      shared types, errors, telemetry
  basin-storage     Parquet + object_store under tenant prefixes
  basin-catalog     Iceberg-style catalog (in-memory + Postgres-backed)
  basin-wal         file-backed WAL (Raft-backed in v0.2)
  basin-shard       in-process shard owner with WAL → Parquet compactor
  basin-engine      DataFusion SQL execution, per-tenant sessions
  basin-router      pgwire v3 (simple + extended query)
  basin-vector      native HNSW vector search
  basin-placement   (Phase 3 v0.2) (tenant, partition) → owner mapping
  basin-analytical  (Phase 5) DuckDB / DataFusion against Iceberg directly
services/
  basin-server      single-process binary
benchmark/          dashboard + auto-regenerated RESULTS.md
docs/
  architecture.md   the four-layer stack, in detail
  decisions/        ADRs — every "no" with the trigger that would change our mind
  sql-compatibility.md
tests/integration/  cross-crate viability + scaling + Postgres comparisons
```

---

## Build and test

```sh
# Workspace build:
cargo build --workspace
cargo test  --workspace

# Run the benchmark suite + regenerate dashboard / RESULTS.md:
cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py

# Then open the dashboard (no server required):
open benchmark/index.html
```

---

## How Basin compares

### vs Postgres / Aurora / RDS

Postgres is the right answer for high-frequency single-tenant OLTP. **Basin is not trying to be Postgres.** Where Basin wins: workloads with many tenants where idle ones outnumber active ones by 100× and storage cost dominates. Basin's per-tenant prefix isolation is structural; Postgres's RLS is logical and easy to misconfigure.

### vs Neon

Neon is serverless Postgres with branching — terrific for **single-DB workloads** that want copy-on-write forks. Per-project pricing means N tenants = N projects = N × $19/mo minimum. Basin's per-active-tenant-hour model is two orders of magnitude cheaper at 100+ tenants. Basin doesn't try to match Neon's branching depth or its full Postgres surface — Basin is for the multi-tenant case where Neon's pricing breaks.

### vs Supabase

Supabase is "BaaS in a box" — Postgres + Auth + Edge Functions + Storage + Realtime. Basin is **just the database layer**, designed to sit alongside or behind Supabase / Neon / RDS rather than replace them. Multi-tenant SaaS that has outgrown one Supabase project's per-tenant economics can point their tenant data at Basin via pgwire while keeping Supabase Auth and Functions. (Auth + REST are scoped in ADRs 0005/0006 if Basin ever goes the BaaS route, but it's not the current direction.)

### vs Turso / libSQL

Turso is the right answer for **edge-distributed apps** with many tiny SQLite-class databases. Basin is for centralized multi-tenant SaaS with **shared Postgres SQL across tenants**, ZSTD-compressed object-store retention, and per-tenant compliance isolation that SQLite-per-edge can't naturally provide.

### vs ClickHouse / DuckDB / data-warehouse

ClickHouse and DuckDB are analytical engines — phenomenal at OLAP scans, not designed for transactional point reads or per-row inserts. Basin's Phase 5 analytical path uses DuckDB directly **against the same Iceberg tables**; the OLTP path on the shard owners is what makes the per-row workload work. Same data substrate, two read paths.

### Where Basin is *not* the answer

Per the ADRs:

- **Single-tenant high-frequency OLTP** → Postgres / Aurora / Neon
- **Edge / local-first** → Turso / libSQL / Cloudflare D1
- **Geospatial primary store** → PostGIS
- **Embedding-only workload** → dedicated vector DB (Qdrant, Pinecone) — but Basin's native `vector(N)` works fine *alongside* tabular data
- **Embedded SQLite-class library** → SQLite
- **Globally strongly-consistent writes across regions** → Spanner / CockroachDB

---

## Who should use Basin

If your workload is **one of**:

- A multi-tenant SaaS where most tenants are mostly idle and storage cost matters
- An audit-heavy or compliance-heavy product (fintech, healthcare, security tooling) with strict per-tenant data residency
- An AI-agent platform with many users storing embeddings + transactional rows alongside each other
- A document / event log platform where writes are cheap and analytical reads are occasional

…then Basin's wedge probably matches your shape. If you're a single-app developer building a side project, **use Postgres**. The wedge math doesn't apply to you.

---

## Keywords for search

Basin is a **multi-tenant Postgres-compatible database** designed for **bucket-native object storage**, with **per-tenant isolation**, **native vector search** (HNSW), **ZSTD-compressed Parquet** storage, an **Apache Iceberg-style catalog**, a **Raft-backed WAL**, and **pgwire** protocol support that works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, and any other Postgres driver. Basin compares to **Postgres**, **Neon**, **Supabase**, **Turso**, **PlanetScale**, and **CockroachDB** for the multi-tenant SaaS, audit-log, and AI-agent platform use cases. Self-hostable, **Apache-2.0** licensed, written in **Rust**.

---

## License

**Apache-2.0** — see [`LICENSE`](./LICENSE).

Contributions welcome. The project is opinionated about scope ([`docs/decisions/`](./docs/decisions/)) — open an issue before writing a PR that adds new surface area. The OSS code is the database; commercial cloud orchestration lives in a separate private repo and never affects what OSS users get.
