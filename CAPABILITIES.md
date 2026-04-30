# Basin — capabilities

Honest, public-facing description of what Basin does today, what's planned,
and what's not on the roadmap. If you're evaluating Basin for a real
workload, this is the right page to read first.

Cross-references: [`TASK.md`](./TASK.md) is the full Phase 0–7 build plan,
[`WEDGE.md`](./WEDGE.md) is the prioritized next-six-months slice,
[`docs/decisions/`](./docs/decisions/) records every "no" with the trigger
that would change our mind.

Status legend: ✅ shipped · 🛠 in progress · ◻️ planned · 🚫 not on roadmap.

---

## Wire protocol

| Capability | Status | Notes |
|---|---|---|
| pgwire v3 | ✅ | startup + cleartext-password auth + simple query |
| Simple query (`Q` message) | ✅ | what `psql` types into the prompt |
| Extended query (`Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Sync`) | ✅ | full Parse/Bind/Describe/Execute/Close/Sync; `tokio-postgres::query` works |
| Binary parameter / result format | ✅ | INT2/4/8, FLOAT4/8, BOOL, BYTEA, TEXT |
| `COPY FROM STDIN` / `COPY TO STDOUT` | ◻️ | bulk import / export |
| TLS | ◻️ | rustls behind a feature flag |
| `LISTEN` / `NOTIFY` | 🚫 | no pub/sub today |
| Replication protocol | 🚫 | not the right shape for object-store storage |

## SQL surface

| Capability | Status | Notes |
|---|---|---|
| `CREATE TABLE` | ✅ | int{2,4,8}, text/varchar, boolean, double, vector(N) |
| `INSERT … VALUES` (single + multi-row) | ✅ | string-quoted vector literals supported |
| `SELECT` with `WHERE` (single table) | ✅ | DataFusion-planned; predicate pushdown to Parquet |
| `SHOW TABLES` | ✅ | per-tenant scoped |
| `ORDER BY` / `LIMIT` | ✅ | full DataFusion support |
| Joins (single-shard) | 🛠 | DataFusion handles them; not yet exercised in tests |
| `UPDATE` / `DELETE` | ◻️ | requires row-level deletes; Iceberg v2 supports this |
| Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) | ◻️ | single-shard only when shipped |
| Prepared statements with parameter bind | ✅ | shipped with extended-query protocol |
| Foreign keys | ◻️ | single-shard only when shipped |
| Stored procedures / triggers | 🚫 | rebuild-Aurora trajectory |
| Materialized views | ◻️ | natural fit on Iceberg snapshots |

## Types

| Type | Status |
|---|---|
| `BIGINT` / `INTEGER` / `SMALLINT` | ✅ |
| `TEXT` / `VARCHAR` | ✅ |
| `BOOLEAN` | ✅ |
| `DOUBLE PRECISION` / `FLOAT8` | ✅ |
| `vector(N)` | ✅ |
| `TIMESTAMPTZ` | 🛠 |
| `JSONB` | ◻️ |
| `UUID` | ◻️ |
| `NUMERIC` (arbitrary precision) | ◻️ |
| `BYTEA` | ✅ |
| `INTERVAL`, `MONEY`, `XML`, geometric | 🚫 |

## Multi-tenancy

| Capability | Status | Notes |
|---|---|---|
| Per-tenant bucket prefix isolation | ✅ | structural, enforced at storage API |
| Connection → tenant via username | ✅ | pluggable resolver |
| Per-tenant snapshots | ✅ | Iceberg-style atomic appends |
| RLS within a tenant (`CREATE POLICY`) | ◻️ | post extended-query |
| BYO-bucket | ◻️ | customer's S3 + IAM role |
| BYO-key (KMS) | ◻️ | platform never sees plaintext |
| Tenant deletion (`O(file_count)`) | ✅ | bucket prefix delete |
| Tenant branching / fork | ◻️ | catalog metadata copy |

## Storage

| Capability | Status | Notes |
|---|---|---|
| Parquet under `tenants/{id}/...` | ✅ | ZSTD-1 compression, 65k row groups |
| Predicate pushdown | ✅ | row-group statistics + page index |
| Projection pushdown | ✅ | DataFusion-driven |
| Pluggable `object_store` (S3, R2, GCS, local FS) | ✅ | the workspace dep handles all four |
| Iceberg-style catalog (in-memory) | ✅ | atomic appends, optimistic concurrency |
| Iceberg-style catalog (durable) | ✅ | Postgres-backed; survives restart |
| WAL (Raft-backed, 5ms acks) | ◻️ | Phase 2 — closes the insert-latency gap |
| Background compactor | ◻️ | merges small files; needed before vector search latency is ideal |
| Iceberg REST catalog (Lakekeeper compatibility) | ◻️ | trait shape locked, server impl deferred |

## Query execution

| Capability | Status | Notes |
|---|---|---|
| OLTP path via DataFusion | ✅ | per-tenant `SessionContext` |
| Analytical path via DuckDB / DataFusion direct on Iceberg | ◻️ | Phase 5 |
| Cross-shard query merging | 🚫 | single-process today; Phase 3 work |
| Cost-based query rejection | ◻️ | "this query would cost $1k; reject" |

## Vector search

| Capability | Status | Notes |
|---|---|---|
| `vector(N)` column type | ✅ | Arrow `FixedSizeList<Float32>` |
| Distance ops `<->`, `<#>`, `<=>` | ✅ | rewritten to UDF calls |
| L2 / cosine / dot UDFs | ✅ | DataFusion `ScalarUDF` |
| HNSW index sidecar (`*.hnsw`) | ✅ | bincode on disk, per Parquet file |
| `Storage::vector_search` fast path | ✅ | k-merge across segments |
| Automatic planner routing of `ORDER BY x <-> $1 LIMIT k` | ◻️ | currently brute-force unless caller invokes `vector_search` directly |
| IVF-flat indexes | 🚫 | HNSW is enough for first 1B vectors per tenant |
| `pg_vector` wire-protocol compat | 🚫 | see [ADR 0003](./docs/decisions/0003-native-vector-search.md) |

## Postgres extension breadth

| Capability | Status | Notes |
|---|---|---|
| Extension support generally | 🚫 | see [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md) |
| `pg_vector` | 🚫 | replaced by **native vector search** ([ADR 0003](./docs/decisions/0003-native-vector-search.md)) |
| PostGIS (geospatial) | 🚫 | use a dedicated geo DB or sidecar PG |
| TimescaleDB / Citus | 🚫 | use a dedicated TS / sharded PG |
| `pgcrypto`, `uuid-ossp` (small functions) | ◻️ | implementable as built-ins case-by-case if ORM-blocking |

## Multi-region / global

| Capability | Status | Notes |
|---|---|---|
| Single-region | ✅ | the wedge customer's posture |
| S3 cross-region replication of data | ◻️ | "free" via bucket-level configuration |
| Eventual-consistent cross-region read replicas | ◻️ | scoped in [ADR 0004](./docs/decisions/0004-multi-region-read-replicas.md), build planned |
| Cross-region 2PC / strong consistency | 🚫 | see [ADR 0001](./docs/decisions/0001-single-region-only.md) — Spanner-class, deferred until paid |

## Operations

| Capability | Status | Notes |
|---|---|---|
| Per-tenant metrics (ops/s, p50/p99, RAM, S3 IO) | 🛠 | minimal today |
| OpenTelemetry traces | 🛠 | wired through router → engine → storage |
| Structured logs (`tracing` JSON) | ✅ | format selectable at startup |
| Connection pooling at the edge | ◻️ | post extended-query |
| Rate limiting | ◻️ | per-tenant throttles |
| Bring-your-own-bucket | ◻️ | Phase 6 |
| Bring-your-own-key (KMS) | ◻️ | Phase 6 |
| Stripe billing integration | ◻️ | Phase 6 |

## What we're not building, and what to use instead

If your workload requires …

- **High-frequency single-tenant OLTP** → use Postgres or Aurora.
- **Globally consistent cross-region writes** → use Spanner / CockroachDB.
- **Edge / local-first apps** → use Turso (libSQL) or Cloudflare D1.
- **Geospatial primary store** → use PostGIS.
- **Embeddings as the *only* workload** → use a dedicated vector DB (Qdrant, Pinecone, Weaviate, pg_vector on Postgres).
- **Embedded SQLite-class library** → use SQLite.

Basin's wedge is multi-tenant SaaS with audit-log workloads where storage
cost and per-tenant isolation dominate. If your shape doesn't match, the
above are honest recommendations.

---

*Last updated: 2026-04-30. This file is hand-maintained; PRs welcome.*
