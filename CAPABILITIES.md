# Basin — capabilities

Honest, public-facing description of what Basin does today, what's planned,
and what's not on the roadmap. If you're evaluating Basin for a real
workload, this is the right page to read first.

Cross-references: [`TASK.md`](./TASK.md) is the full Phase 0–7 core-DB
build plan, [`WEDGE.md`](./WEDGE.md) is the prioritized next-six-months
slice, [`CLOUD_ROADMAP.md`](./CLOUD_ROADMAP.md) is the customer-facing
cloud-platform roadmap (identity, REST, V8 edge functions, BYO-bucket /
BYO-key, Stripe billing, dashboard), [`docs/deployment.md`](./docs/deployment.md)
is the production cloud architecture guide, [`docs/decisions/`](./docs/decisions/)
records every "no" with the trigger that would change our mind.

Status legend: ✅ shipped · 🛠 in progress · ◻️ planned · 🚫 not on roadmap.

Coverage: every ✅ row above is exercised by [`tests/integration/tests/feature_coverage.rs`](./tests/integration/tests/feature_coverage.rs) (or its named cross-reference in that file's audit comment). Security invariants verified by [`tests/integration/tests/security.rs`](./tests/integration/tests/security.rs).

---

## Wire protocol

| Capability | Status | Notes |
|---|---|---|
| pgwire v3 | ✅ | startup + cleartext-password auth + simple query |
| Simple query (`Q` message) | ✅ | what `psql` types into the prompt |
| Extended query (`Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Sync`) | ✅ | full Parse/Bind/Describe/Execute/Close/Sync; `tokio-postgres::query` works |
| Binary parameter / result format | ✅ | INT2/4/8, FLOAT4/8, BOOL, BYTEA, TEXT, JSONB, UUID, FixedSizeBinary. JSONB / UUID parameter binding is native — `ParameterDescription` advertises OID 3802 / 2950 for INSERT / UPDATE / SELECT-WHERE slots whose target column carries `BASIN_TYPE=JSONB` / `BASIN_TYPE=UUID` metadata, and the Bind decoder understands both wire formats (JSONB v1: leading `0x01` version byte + canonical-form JSON; UUID: 16 raw RFC 4122 bytes). asyncpg / pgx / tokio-postgres encode `uuid.UUID` / `dict` / `serde_json::Value` directly without string coercion. |
| `COPY FROM STDIN` / `COPY TO STDOUT` | ✅ | CSV format only (RFC 4180-ish, comma-delimited; `WITH (FORMAT CSV [, HEADER true])`); BINARY / custom DELIMITER / NULL spec / column-list / file-path variants rejected with SQLSTATE 42601. COPY-IN imports row-by-row INSERTs against the engine; on a mid-stream error we drain to `CopyDone` before responding so the connection stays usable. Drives both simple-query and the extended-query path so `tokio_postgres::copy_in` / `copy_out` and `psql \copy` both work. |
| TLS | ✅ | rustls (aws-lc-rs) on the pgwire listener; static PEM cert + key via `BASIN_TLS_CERT_PATH` / `BASIN_TLS_KEY_PATH`. SSLRequest answered `'S'`/`'N'` and the socket wrapped before pgwire startup. mTLS / OCSP / cert rotation / ALPN selection deferred to v0.2. |
| `LISTEN` / `NOTIFY` | 🚫 | no pub/sub today |
| Replication protocol | 🚫 | not the right shape for object-store storage |

## SQL surface

| Capability | Status | Notes |
|---|---|---|
| `CREATE TABLE` | ✅ | int{2,4,8}, text/varchar, boolean, double, vector(N), JSONB, UUID, BYTEA |
| `INSERT … VALUES` (single + multi-row) | ✅ | string-quoted vector / JSONB / UUID literals supported |
| `SELECT` with `WHERE` (single table) | ✅ | DataFusion-planned; predicate pushdown to Parquet |
| `SHOW TABLES` | ✅ | per-tenant scoped |
| `ORDER BY` / `LIMIT` | ✅ | full DataFusion support |
| Joins (single-shard) | 🛠 | DataFusion handles them; not yet exercised in tests |
| `UPDATE` / `DELETE` | ✅ | Copy-on-write Iceberg v2. Single-scan partition; `replace_data_files` with optimistic concurrency on both catalog backends; physical deletion of replaced Parquet files. |
| `ALTER TABLE` | ✅ | `ADD COLUMN`, `SET cold_after`, `SET cold_age_column`, `SET BLOOM FILTERS ON`, `SET row_group_rows`, `RESET row_group_rows`, `CLUSTER BY (...)`, `RESET CLUSTER BY`, `ENABLE/DISABLE ROW LEVEL SECURITY`, `CREATE POLICY`, `DROP POLICY` |
| `CREATE MATERIALIZED VIEW … WITH (basin.continuous, …)` | 🛠 | Rust API ships via `basin-cv`; SQL surface is in `cv_glue` stub |
| `CREATE POLICY` (RLS) | ✅ | predicate injection at logical-plan layer; cross-tenant leak invariant verified |
| Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) | ◻️ | single-shard only when shipped |
| Prepared statements with parameter bind | ✅ | shipped with extended-query protocol |
| Foreign keys | ◻️ | single-shard only when shipped |
| Stored procedures / triggers | 🚫 | rebuild-Aurora trajectory |

## Types

| Type | Status | Notes |
|---|---|---|
| `BIGINT` / `INTEGER` / `SMALLINT` | ✅ | |
| `TEXT` / `VARCHAR` | ✅ | |
| `BOOLEAN` | ✅ | |
| `DOUBLE PRECISION` / `FLOAT8` | ✅ | |
| `vector(N)` | ✅ | Arrow `FixedSizeList<Float32>` |
| `BYTEA` | ✅ | Arrow `LargeBinary` |
| `JSONB` | ✅ | Arrow `LargeBinary` + field metadata `BASIN_TYPE=JSONB`; canonical-form normalization on insert; pgwire OID 3802 |
| `UUID` | ✅ | Arrow `FixedSizeBinary(16)` + field metadata `BASIN_TYPE=UUID`; pgwire OID 2950 with canonical hyphenated text + 16-byte binary |
| `POINT` (geospatial) | 🛠 | `basin-geo` crate ships Rust API; SQL surface (column type `POINT`) deferred to v0.2 via `geo_glue` stub |
| `TIMESTAMPTZ` | ✅ | Arrow `Timestamp(Microsecond, "UTC")` |
| `NUMERIC` (arbitrary precision) | ◻️ | |
| `INTERVAL`, `MONEY`, `XML`, geometric (LINESTRING/POLYGON) | 🚫 | |

## Multi-tenancy

| Capability | Status | Notes |
|---|---|---|
| Per-tenant bucket prefix isolation | ✅ | structural, enforced at storage API |
| Connection → tenant via username | ✅ | pluggable resolver |
| Per-tenant snapshots | ✅ | Iceberg-style atomic appends |
| Per-tenant fairness (Semaphore) | ✅ | cap=16; default-on |
| Row-Level Security | ✅ | `ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` with `current_user`-aware predicates injected at logical-plan layer; cross-tenant leak invariant tested |
| BYO-bucket | ◻️ | customer's S3 + IAM role |
| BYO-key (KMS) | ◻️ | platform never sees plaintext |
| Tenant deletion (`O(file_count)`) | ✅ | `Storage::delete_tenant` is catalog-first paths + parallel orphan LIST + bulk DeleteObjects + drop_namespace; LocalFS 4ms, R2 ~1.4-2.2s |
| Table fork (catalog COW) | ✅ | `Catalog::fork_table(tenant, src, dst)` clones a table's metadata + snapshot history into a new sibling within the same tenant, sharing data files by reference. Diverges on next commit. v0.2 adds cross-tenant fork with refcount-aware GC. |
| Within-tenant time partitioning | ✅ | `CREATE TABLE … PARTITION BY RANGE (ts)`; partition pruning |
| Tiered storage (hot/cold) | ✅ | `ALTER TABLE … SET cold_after = N`; compactor moves files between tiers |
| Whale-tenant pinning | ✅ | `BASIN_TENANT_PINS=ulid:idx,...` pins a tenant to a specific shard endpoint regardless of consistent hash; v0.2 moves pins into the catalog so they survive restart |

## Storage

| Capability | Status | Notes |
|---|---|---|
| Parquet under `tenants/{id}/...` | ✅ | ZSTD-1 compression, 65k row groups (configurable per table) |
| Predicate pushdown | ✅ | row-group statistics + page index |
| Projection pushdown | ✅ | DataFusion-driven |
| Bloom filters in Parquet footer | ✅ | per-column opt-in via `ALTER TABLE … SET BLOOM FILTERS ON (col)`; turns ~80% of nonexistent-id queries into row-group skips |
| Coalesced metadata in catalog (Phase 5.7 A4) | ✅ | file-level `column_stats` (min / max / null per column) on every committed `DataFileRef`; `Storage::read_paths` skips LIST + per-file footer fetch when the catalog stats prove the predicate prunes the file. Row-group-level coalesced stats deferred to v0.2 (B1). |
| Per-table row-group sizing | ✅ | `ALTER TABLE … SET row_group_rows = N`; small row groups for point-heavy tables |
| Cluster-by physical sort (Phase 5.7 B2) | ✅ | `Catalog::set_cluster_columns` configures per-table cluster columns; the writer `lexsort`s every batch by those columns before Parquet flush so related rows live in the same row group / file. Combined with A3 bloom + A4 catalog stats, point queries on the cluster columns prune to one file in the common case. SQL: `CREATE TABLE … CLUSTER BY (...)` / `ALTER TABLE … CLUSTER BY (...)` / `ALTER TABLE … RESET CLUSTER BY`. |
| Pluggable `object_store` (S3, R2, GCS, local FS, MinIO, SeaweedFS) | ✅ | the workspace dep handles all |
| **NVMe disk cache** | ✅ | LRU on local SSD; ~50ms cold S3 fetches → ~100µs warm SSD reads. Default-on. 101× speedup measured. |
| **Parquet page cache (RAM)** | ✅ | LRU of decoded RecordBatches; <1ms warm hits. Default-on. 7.24× speedup measured. |
| HTTP/2 toggle for S3 client | ✅ | `S3Config::http2_only`; useful on AWS S3 / R2 over HTTPS |
| Iceberg-style catalog (in-memory) | ✅ | atomic appends, optimistic concurrency |
| Iceberg-style catalog (durable) | ✅ | Postgres-backed; survives restart. Multi-region replication direction: single-writer global PG with regional read replicas via PG logical replication — see [ADR 0010](./docs/decisions/0010-catalog-replication.md). |
| Point-in-time restore (catalog level) | 🛠 | `Catalog::rollback_to_snapshot(tenant, table, snapshot_id)` truncates history to ≤ target and rewinds the head pointer. InMemory + Postgres impls. v0.2 adds physical file GC for orphaned post-rollback files; cross-DML rollback waits on soft-delete (also v0.2). |
| Per-tenant fair-share scheduler | 🛠 | architectural primitive shipped (cap=16); v0.2 EDF deferred — see [ADR 0008](./docs/decisions/0008-noisy-neighbor-fairness.md) |
| WAL (Raft-backed, 5ms acks) | ◻️ | Phase 2 — closes the insert-latency gap |
| Background compactor | 🛠 | merges small files; tier sweep + cold-data move shipped |
| Iceberg REST catalog (Lakekeeper compatibility) | ◻️ | trait shape locked, server impl deferred |
| Per-tenant secondary indexes (B-tree) | ◻️ | Phase 5.7 B1 — biggest remaining point-query win for true random ids |

## Query execution

| Capability | Status | Notes |
|---|---|---|
| OLTP path via DataFusion | ✅ | per-tenant `SessionContext` |
| Analytical path via DuckDB on Iceberg | ✅ | `basin-analytical` v0.1 — 4.6× faster than DataFusion on 1M-row aggregates |
| Engine routes analytical SQL automatically | ✅ | aggregate / GROUP BY / `/*+ analytical */` hint heuristic |
| Cross-shard query merging | 🛠 | router → shard-owner protocol shipped (consistent hashing, 28% max load); cross-shard JOIN deferred |
| Cost-based query rejection | ✅ | v0.1: `BASIN_QUERY_COST_LIMIT_ROWS=N` rejects single-table SELECTs that estimate above the row cap with PG SQLSTATE 54000 (`program_limit_exceeded`). Default off. Multi-FROM / JOIN / sub-query / explicit-LIMIT shapes pass through unchecked; v0.2 uses A4 catalog `ColumnStats` for selectivity-aware estimates on those. |

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

## Postgres-extension equivalents

ADR 0002 says "no upstream extensions". But the most common ones are
covered natively, as Basin-flavored crates with the same SQL semantics:

| Postgres extension | Basin equivalent | Status | Notes |
|---|---|---|---|
| `pg_vector` | native `vector(N)` + HNSW | ✅ | [ADR 0003](./docs/decisions/0003-native-vector-search.md) |
| `pg_cron` | **`basin-cron`** | ✅ | `cron.schedule(name, schedule, sql)` + `cron.unschedule` + `cron.job` + `cron.job_run_details`. SQL surface lands when cron_glue is wired (v0.2). |
| `pg_net` + `http` | **`basin-net`** | ✅ | sync `http_get` / `http_post`; async `net.http_post` with `net._http_response` table. Per-tenant URL allowlist (DENY-ALL default), 10 req/s rate limit, 10 MiB body cap, 30s timeout. SQL surface lands in v0.2. |
| `pgcrypto` (digest, encode, crypt, gen_salt) | native UDFs | ✅ | `digest` (md5/sha1/sha224/sha256/sha384/sha512), `encode`/`decode` (hex/base64/escape), `crypt` (bcrypt), `gen_salt('bf')` |
| `uuid-ossp` | native UDFs + `UUID` type | ✅ | `gen_random_uuid()`, `uuid_generate_v4()`, canonical hyphenated text + 16-byte binary on the wire |
| `PostGIS` (subset) | **`basin-geo`** | ✅ | `Point`, `Box2d`, `ST_MakePoint`, `ST_X`, `ST_Y`, `ST_Distance` (Haversine WGS84), `ST_DWithin`, `ST_Contains`. No `LINESTRING`/`POLYGON`/spatial index in v0.1 — see crate. |
| `pg_trgm` | **`basin-trgm`** | ✅ | `similarity`, `word_similarity`, `extract` (trigram set). v0.1 brute-force; GIN trigram index deferred to v0.2. |
| `TimescaleDB` continuous aggregates | **`basin-cv`** | ✅ | `CvSpec` + `CvRefresher::tick`; refresh_interval enforced; per-tenant materialization. v0.1 full re-execution; incremental refresh deferred. |
| `TimescaleDB` hypertables | within-tenant time partitioning | ✅ | `CREATE TABLE … PARTITION BY RANGE (ts)` |
| `pg_stat_statements` | OTEL traces | ✅ | per-query spans exported via `BASIN_OTLP_ENDPOINT` |
| `Citus` (sharding) | basin-router consistent-hash | ✅ | sharding is structural via per-tenant prefix |
| `pg_partman` | Iceberg manifest | ✅ | partition lifecycle is just snapshot evolution |
| `hstore` | use `JSONB` | ✅ | JSONB is the modern replacement |
| `citext` | not yet | ◻️ | one-line built-in if ORM-blocking |
| `postgres_fdw` / `dblink` | use basin-net for HTTP, no native FDW | 🚫 | foreign-PG queries not on roadmap |
| `plpython3u` / `plperl` (alt languages) | 🚫 | rebuild-Aurora trajectory |
| Loadable `.so` extensions | 🚫 | [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md) |

The Basin-flavored crates above (basin-cron, basin-net, basin-geo, basin-trgm, basin-cv)
follow the same staging pattern: ship a Rust API + integration test in v0.1,
defer the SQL surface to v0.2 once the engine planner is extended to register
the corresponding `ScalarUDF`s and parse the relevant `WITH (…)` options.

## Multi-region / global

| Capability | Status | Notes |
|---|---|---|
| Single-region | ✅ | the wedge customer's posture |
| Multi-region by deployment (one cluster per region) | ✅ | works today; document at [`docs/deployment.md`](./docs/deployment.md) |
| `region` field on `TenantMetadata` | ◻️ | 1-day Phase 1 add to make region-pinning explicit |
| S3 cross-region replication of data | ◻️ | "free" via bucket-level configuration on AWS S3 / R2 |
| Eventual-consistent cross-region read replicas | ◻️ | scoped in [ADR 0004](./docs/decisions/0004-multi-region-read-replicas.md), build planned |
| Cross-region 2PC / strong consistency | 🚫 | see [ADR 0001](./docs/decisions/0001-single-region-only.md) — Spanner-class, deferred until paid |

## Operations

| Capability | Status | Notes |
|---|---|---|
| Per-tenant metrics (ops/s, p50/p99, RAM, S3 IO) | ✅ | `Engine::tenant_counters(&TenantId) -> TenantCountersSnapshot` returns ops/bytes_read/bytes_written/errors + ring-window p99 ms estimate; registry shared across engine + storage + WAL |
| OpenTelemetry traces | ✅ | wired through router → engine → shard → storage; OTLP export available via `BASIN_OTLP_ENDPOINT` |
| Structured logs (`tracing` JSON) | ✅ | format selectable at startup |
| Connection pooling (`basin-pool`) | ✅ | Native `TenantSession` cache; per-tenant cap; LRU eviction. Wired into `basin-server` behind `BASIN_POOL_ENABLED=1`. See [ADR 0007](./docs/decisions/0007-connection-pooling.md). |
| Rate limiting (basin-net side) | ✅ | per-tenant 10 req/s sustained, burst 30; URL allowlist; body cap; timeout |
| Rate limiting (pgwire side) | ✅ | per-tenant token-bucket via `governor` (same crate as basin-net). Default off; `BASIN_PGWIRE_RATE_LIMIT_QPS=100` enables 100 qps sustained / 300 burst with bucket-empty mapped to Postgres SQLSTATE `53400` (`configuration_limit_exceeded`). Per-tenant overrides + catalog-driven config deferred to v0.2. |
| Bring-your-own-bucket | ◻️ | Phase 6 |
| Bring-your-own-key (KMS) | ◻️ | Phase 6 |
| Stripe billing integration | ◻️ | Phase 6 |

## Auth and REST API

| Capability | Status | Notes |
|---|---|---|
| `basin-auth` (signup, signin, magic-link, password reset, email verify, JWT, refresh) | ✅ | Requires SMTP at startup (fail-fast). Postgres-backed `auth.users`. JWT issued + verified per request. `BASIN_AUTH_ENABLED=1`. See [ADR 0005](./docs/decisions/0005-auth-system.md). |
| `basin-rest` (PostgREST-compatible HTTP layer) | ✅ | `GET`/`POST`/`PATCH`/`DELETE` on `/rest/v1/<table>`. Bearer-JWT auth via `basin-auth`. `BASIN_REST_ENABLED=1` (requires auth). See [ADR 0006](./docs/decisions/0006-rest-api-layer.md). |
| Pgwire JWT auth (`user` parameter carries bearer token) | ✅ | When auth is enabled, both pgwire and REST honor JWT. Static tenant map continues to work as fallback. |
| Per-tenant pgwire connection URLs | ✅ | `POST /admin/v1/tenants` returns `postgres://<tenant_user>:<password>@host:5433/<db>`. Password is bcrypt-validated on every pgwire startup; mismatch returns SQLSTATE `28P01` with a uniform "invalid pgwire credentials" message (no user-existence leak). Rotate via `POST /admin/v1/tenants/{user}/rotate`; old password invalidates immediately. List via `GET /admin/v1/tenants/{tenant_id}/credentials` (no plaintext, no hash). All admin endpoints gated on `claims.is_admin == true`. The `BASIN_TENANTS=alice=*` static-resolver path is preserved for back-compat demos. Cross-tenant isolation under per-tenant URLs is integration-tested (UNION + CTE bypass attempts both blocked); within-tenant RLS still applies. |
| API-key tokens (long-lived, revocable) | ✅ | `POST /auth/v1/api-keys` issues; `GET` lists; `DELETE` revokes. sha256 lookup + bcrypt verify; `Authorization: Bearer <key>` works on REST and (via `ApiKeyTenantResolver`) pgwire. |
| Real PostgREST (Haskell) sitting in front of Basin | 🚫 | needs `pg_catalog` / `information_schema` — 2–4 month slog with ongoing maintenance. Building basin-rest natively is ~3 weeks instead. |

## Comparison shape vs Supabase / Neon / Postgres

See [`docs/deployment.md`](./docs/deployment.md) for the production cloud
architecture rationale. TL;DR:

- **Single instance per region, not per customer.** Basin's whole wedge is
  that Postgres-as-a-Service vendors charge per project because Postgres
  can't multi-tenant cheaply. Basin can. ~$0.10–$0.20 per tenant per
  month all-in for a 100 MB / 10k-tenant workload.
- **Cloudflare R2 + Fly.io is the recommended cloud.** Zero egress, ~5–30 ms
  RTT in-metro, no surprise bills.
- **Multi-region today** = deploy one Basin cluster per region. No DB
  changes required. Read-replica / cross-region writes are Phase 6 work.

## What we're not building, and what to use instead

If your workload requires …

- **High-frequency single-tenant OLTP** → use Postgres or Aurora.
- **Globally consistent cross-region writes** → use Spanner / CockroachDB.
- **Edge / local-first apps** → use Turso (libSQL) or Cloudflare D1.
- **Geospatial primary store with full PostGIS** (LINESTRING, POLYGON, R-tree) → use real PostGIS or sidecar PG.
- **Embeddings as the *only* workload** → use a dedicated vector DB (Qdrant, Pinecone, Weaviate, pg_vector on Postgres).
- **Embedded SQLite-class library** → use SQLite.
- **Stored procedures / triggers / pl/* languages** → use Postgres.

Basin's wedge is multi-tenant SaaS with audit-log workloads where storage
cost and per-tenant isolation dominate. If your shape doesn't match, the
above are honest recommendations.

---

*Last updated: 2026-05-07. This file is hand-maintained; PRs welcome.*
