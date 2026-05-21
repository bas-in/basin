<p align="center">
  <img src="./basin.svg" alt="Basin" width="180">
</p>

<h1 align="center">Basin</h1>

<p align="center">
  <strong>Cheap Postgres on object storage.</strong><br>
  A Postgres-compatible database that stores every byte as Vortex-compressed
  columnar files on S3 (Parquet is first-class selectable), served by a single
  Apache DataFusion engine. Up to ~24× smaller on disk, 47× less RAM
  per connection, and spinning up a new project is a bucket-prefix away — no new VM,
  no per-DB minimum bill.
</p>

<p align="center">
  <a href="https://github.com/bas-in/basin/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/bas-in/basin/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/bas-in/basin/releases"><img alt="latest release" src="https://img.shields.io/github/v/release/bas-in/basin?include_prereleases&style=flat-square"></a>
  <a href="./CHANGELOG.md"><img alt="changelog" src="https://img.shields.io/badge/changelog-keep--a--changelog-blue?style=flat-square"></a>
  <a href="./WEDGE.md"><img alt="status: pre-alpha" src="https://img.shields.io/badge/status-pre--alpha-orange?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="tests passing" src="https://img.shields.io/badge/tests-passing-brightgreen?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: disk 12.5x smaller" src="https://img.shields.io/badge/vs_postgres-disk_12.5%C3%97_smaller-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: point query 3.5x faster" src="https://img.shields.io/badge/vs_postgres-point_3.5%C3%97_faster-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: 47x less RAM/conn" src="https://img.shields.io/badge/vs_postgres-47%C3%97_less_RAM%2Fconn-blue?style=flat-square"></a>
  <a href="./CAPABILITIES.md"><img alt="capabilities" src="https://img.shields.io/badge/capabilities-matrix-blue?style=flat-square"></a>
  <a href="./docs/sql-support.md"><img alt="SQL support matrix" src="https://img.shields.io/badge/SQL_support-matrix-blue?style=flat-square"></a>
  <a href="./LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey?style=flat-square"></a>
</p>

---

## Why Basin

**Storage is cheap because it lives on S3-compatible object storage.** Every table is columnar under a project prefix in your bucket of choice (Tigris, AWS S3, MinIO, local FS). The **default on-disk format is Vortex** (BtrBlocks compression cascade): **~1.95× smaller than ZSTD Parquet** on the audit-log dataset and ~24× smaller than a Postgres heap — object storage at $0.015–$0.02/GB/mo means what costs $25/mo on Postgres-class block storage is well under $1/mo on Basin. The Vortex default is **correctness-verified**: a Vortex⇆Parquet differential harness asserts byte-identical results across the full 88-shape SQL battery (point / range / aggregate / GROUP BY / ORDER BY+LIMIT / IS NULL / windows / joins / set ops / DELETE / UPDATE / NULLIF / IS DISTINCT FROM / CTE / subquery / multi-file), plus the full SQL-support matrix and ORM-compat suite. **Release-mode perf vs Parquet** (`vortex_vs_parquet_smoke`, identical data, median-of-5, LocalFS): Vortex wins on **the majority of analytical shapes** including the huge `aggregate_full` metadata fast path (~15-40× via catalog stats, scaling up with data), four_way_join (~1.6×), three_way_join (~1.3×), inner_join (~1.4×), string_eq (~1.4×), is_null, math_chain, distinct_on, rollup, and percentile shapes. **Per-file catalog blooms** ([Phase 5.14.A](./TASK.md)) just flipped `point_eq` from a loss to a win at every scale (~1.05× at 100k, where it was ~0.49× pre-bloom). Remaining residuals — window-frame shapes, OR/IN/IS DISTINCT predicates, exact `COUNT(DISTINCT)`, raw substring-fn shapes — are documented as upstream-Vortex tracks or architectural (HTAP hot-tier in [Phase 5.14.C](./docs/decisions/0016-htap-hot-tier-architecture.md) closes the OLTP point-read floor on TB-scale tables). **Parquet stays a first-class selectable format** via `CREATE TABLE … WITH (basin.file_format = 'parquet')` (also the Iceberg / Athena / Spark interchange path). See [ADR 0015](./docs/decisions/0015-vortex-storage-format.md) and [ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md).

**Projects are essentially free to create.** A new project is a new bucket prefix. No fork-per-connection. No provisioned VM. No per-DB pricing minimum. Idle projects cost only their bytes. Spin up one project for a side app, or ten thousand for a SaaS — same architecture, same binary.

**Compute is light.** Pure-Rust async server. **165 KiB** of RAM per held-open connection vs Postgres's 7.9 MiB — 47× less. 1,000 concurrent connections held without refusal where Postgres caps at 100. The structural advantage comes from being a from-scratch tokio server, not a forking daemon.

**Real Postgres on the wire.** Pgwire v3, simple + extended query, TLS (rustls), `COPY FROM STDIN` / `COPY TO STDOUT`, prepared statements with binary parameters (native JSONB, UUID, BYTEA, and the now-correct NUMERIC varlena + ARRAY list-element binary wire formats). Works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM — every Postgres driver. Your ORM doesn't know it isn't talking to Postgres — and a PG-oracle differential harness (`tests/integration/tests/differential_pg.rs`) proves it by running identical SQL against Basin *and* a real PostgreSQL instance and failing the build on any cell-level divergence.

**Object storage unlocks time travel.** Tables are Apache Iceberg snapshots; rollback to any prior snapshot is a metadata write. Forks are zero-copy. Point-in-time restore is a `rollback_to_snapshot` call. No WAL archive to manage, no base-backup-plus-replay dance.

**One engine, one binary.** A single Apache DataFusion engine handles transactional point reads *and* analytical scans. Native vector search (`vector(N)` + `<->` / `<#>` / `<=>`, HNSW per file segment — no `pg_vector` install). Analytical flexibility via Vortex (default) / Parquet projection + predicate pushdown, catalog-statistics file pruning (footer fetch skipped when the predicate prunes the file), per-file bloom filters on `basin.sort_by` columns (skips the file open entirely for absent literals), and incremental pre-aggregation (`CREATE MATERIALIZED VIEW … WITH (basin.continuous)`). Approximate-cardinality aggregates (`APPROX_COUNT_DISTINCT`) and approximate-quantile (`APPROX_PERCENTILE`) UDFs sit alongside their exact counterparts for dashboard workloads. Signup / JWT / refresh-token auth and a PostgREST-shape HTTP API are part of the same server. `pg_cron`, `pg_net`, `pg_trgm`, `PostGIS` subset, `TimescaleDB`-style continuous aggregates, `pgcrypto`, `uuid-ossp` — all native crates, no extension install.

---

## The numbers (vs Postgres 18, 1 M audit-log rows, LocalFS, no index either side)

| Dimension | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| **On-disk size** | 7.72 MiB | 96.51 MiB | **12.5× smaller** |
| **Compression vs CSV** | — | — | **20.22× smaller than CSV** |
| **Point-query p50** (cold, unindexed) | 0.18 ms | 15.06 ms | **83× faster** |
| **RAM per held-open connection** | 121 KiB | 7,658 KiB | **63× less** |
| **Connections under 1,000-conn flood** | 1,000 held / 0 refused | 99 held / 901 refused | **10× more, structural** |
| **ADD COLUMN** (100 K rows, no default) | 0.20 ms | 7.37 ms | **36× faster** |

All figures are integration-test measurements (random working sets, cold-p99 bars, warm/cold separation). Basin has no B-tree index on the id column; Postgres has none either. These are substrate comparisons on wedge-shaped audit-log data — not a claim of universal superiority.

For methodology, absolute numbers, real-S3 results, caveats, and reproduction steps, see [`benchmark/BENCHMARKS.md`](./benchmark/BENCHMARKS.md).

Full live dashboard: [`benchmark/index_localfs.html`](./benchmark/index_localfs.html) (open directly, no server needed). Real-cloud results: [`benchmark/index_real.html`](./benchmark/index_real.html).

### Think a benchmark is unfair? Tell us.

Every card is generated by an integration test in [`tests/integration/tests/`](./tests/integration/tests/) — read the source and tell us where we're wrong. **File a [Benchmark Methodology issue][bench-issue]** and we'll either fix the methodology, soften the headline claim, or explain why we think the test is fair. Decisions are logged in [`docs/decisions/`](./docs/decisions/).

[bench-issue]: ../../issues/new?template=benchmark_methodology.yml

---

## Quickstart

**Want to skip the build and start querying right now?**
See the [5-Minute Docker Quickstart](./docs/quickstart-docker.md) — one
`docker run` command, no Rust toolchain required.

**Ready to go deeper?**
The [Getting Started / Tutorial](./docs/tutorial.md) walks you through
CRUD, auth, RLS policies, the REST API, a React/Vite frontend snippet,
and the first-deployment path — about 15 minutes end-to-end.

---

Install basin, point it at a data dir, run. No external object store is needed
for local development.

```sh
BASIN_DATA_DIR=/tmp/basin cargo run -p basin-server
```

That gives you pgwire on `127.0.0.1:5433`, durable WAL + Parquet under
`/tmp/basin/`, and a volatile in-memory catalog for fast local iteration.
Set `BASIN_CATALOG=postgres://...` for restart-safe metadata.

The full production-shaped boot layers WAL, shard owner, connection pool,
JWT auth, and REST in one process:

```sh
BASIN_BIND=127.0.0.1:5433 \
BASIN_CATALOG=postgres://postgres@127.0.0.1:5432/postgres \
BASIN_DATA_DIR=/tmp/basin \
BASIN_WAL_DIR=/tmp/basin/wal \
BASIN_PROJECTS='alice=*,bob=*' \
BASIN_SHARD_ENABLED=1 \
BASIN_POOL_ENABLED=1 \
BASIN_AUTH_ENABLED=1 \
  BASIN_AUTH_JWT_SECRET=$(openssl rand -hex 32) \
  BASIN_AUTH_SMTP_HOST=smtp.example.com BASIN_AUTH_SMTP_PORT=587 \
  BASIN_AUTH_SMTP_USERNAME=u BASIN_AUTH_SMTP_PASSWORD=p \
  BASIN_AUTH_SMTP_FROM=noreply@example.com BASIN_AUTH_SMTP_TLS=starttls \
BASIN_REST_ENABLED=1 BASIN_REST_BIND=127.0.0.1:5434 \
cargo run -p basin-server
```

`BASIN_PROJECTS` is the project-list env var — name is historical, projects in the
public API. Required vars for production-shaped durability: `BASIN_BIND`,
`BASIN_CATALOG=postgres://...`, `BASIN_DATA_DIR` or `BASIN_STORAGE_BACKEND`,
`BASIN_WAL_DIR`, `BASIN_PROJECTS`, and `BASIN_AUTH_ENABLED` (if you want auth).
Everything else is optional.

To run the same binary against object storage, set
`BASIN_STORAGE_BACKEND=s3|tigris` plus the S3-compatible endpoint, bucket,
region, and credentials documented by `basin-storage`.

Connect with **any Postgres driver**:

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

Confirm the data hit object storage under the project prefix:

```sh
find /tmp/basin/projects -name '*.vortex'   # default format
# /tmp/basin/projects/01HABCD…/tables/events/data/2026/05/01/01HEFG….vortex
# Tables created with WITH (basin.file_format='parquet') write *.parquet instead.
```

That's a real bucket-native database. The prefix is the IAM boundary; one bucket policy revokes all access to a project's data even if every other layer is bypassed.

---

## Architecture

Four layers, each with one job:

```
   pgwire clients  (any Postgres driver — psql, tokio-postgres, asyncpg, JDBC)
          │
          ▼
   Routers (stateless)        parses SQL, applies RLS, routes by project
          │
          ▼
   Shard owners (stateful)    in-memory state for many projects per process,
          │                   eviction on idle, lazy load from WAL + Parquet
          ▼
   WAL                        durable append path; flushes to object storage
          │
          ▼
   Object storage + catalog   /projects/{id}/... Vortex (default) or Parquet + Iceberg-style metadata
                              local FS, S3, Tigris (S3-compatible) — same binary, different bucket
```

The full architecture document is in [`docs/architecture.md`](./docs/architecture.md). Every "no" we've recorded is in [`docs/decisions/`](./docs/decisions/).

**Built on:** Apache Arrow · Apache Iceberg (table format) · Vortex (default columnar format, LFAI incubation) · Apache Parquet (opt-in, interchange) · Apache DataFusion (SQL planner) · Tokio · pgwire-rs · openraft (single-process Raft WAL simulation today; cross-process distributed WAL is v0.2). Pure Rust, `#![forbid(unsafe_code)]` across every crate.

Basin's query engine is built on [Apache DataFusion](https://datafusion.apache.org/), the open-source SQL query engine from the Apache Software Foundation. Basin does not fork DataFusion — every query plan runs through upstream operators with Basin-shaped rules layered on top (RLS injection, project isolation, partition pruning).

---

## What you can do today

- **Postgres-compatible SQL** — pgwire v3, simple + extended query protocol, **TLS** (rustls), **`COPY FROM STDIN`/`COPY TO STDOUT`** (CSV). Works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, any Postgres ORM. A curated ORM/driver-compat suite plus a PG-oracle differential harness (`differential_pg.rs`) gate every release against a real PostgreSQL.
- **CRUD + DDL** — `CREATE TABLE` (incl. `CREATE TABLE AS … WITH NO DATA`), multi-row `INSERT`, `SELECT`, `UPDATE`, `DELETE` (Iceberg copy-on-write), `ON CONFLICT DO NOTHING / DO UPDATE` upsert, `ALTER TABLE … CLUSTER BY (…) / SET BLOOM FILTERS ON / SET row_group_rows / SET cold_after / ENABLE ROW LEVEL SECURITY / CREATE POLICY`, `SHOW TABLES`. Prepared statements with parameter bind (text + binary, including native JSONB / UUID and correct NUMERIC / ARRAY binary wire formats).
- **Honest enforcement, not silent no-ops** — `CREATE UNIQUE INDEX` actually enforces uniqueness, `VARCHAR(n)`/`CHAR(n)` length is enforced, RLS `WITH CHECK` is enforced on write, `TABLESAMPLE` actually samples, advisory locks are real, and unsupported `CREATE TRIGGER` / `MERGE` honest-reject with a SQLSTATE instead of silently doing nothing. A wave of silent-corruption CRITICALs surfaced by the differential harness were fixed.
- **Expanded SQL surface** — JSONPath (`jsonb_path_query`, `@?`, `@@`, `jsonb_path_query_array`); JSONB mutators (`jsonb_set`/`insert`/`strip_nulls`/`pretty`/`typeof`); `json_build_object`/`json_build_array`; INET/CIDR containment; `regexp_match`/`matches`/`split_to_array`/`split_to_table`, `format`, `encode`/`decode`; datetime `age`/`to_char`/`to_date`/`date_bin`; window `IGNORE NULLS`; `SAVEPOINT` / `ROLLBACK TO`; data-modifying CTEs; correlated + `LATERAL` joins (incl. `CROSS JOIN LATERAL generate_series`); bounded full-text search (`tsvector`/`tsquery`/`@@`); ordered-set aggregates (`percentile_disc`, `mode() WITHIN GROUP`); range/multirange arithmetic; real transaction semantics (deferred commits, `ROLLBACK` undo, SAVEPOINT stack, aborted state).
- **Time travel** — Iceberg-style snapshots. `Catalog::rollback_to_snapshot(project, table, snapshot_id)` rewinds; `Catalog::fork_table(project, src, dst)` clones a table's metadata + snapshot history into a new sibling that diverges on next commit. Zero data copy until divergence.
- **Native vector search** — `vector(N)` + `<->` / `<#>` / `<=>` operators, HNSW per file segment. No `pg_vector`.
- **Postgres-extension equivalents** — `pg_cron` (basin-cron), `pg_net` + `http` (basin-net), `pg_trgm` (basin-trgm), `PostGIS` subset (basin-geo), `TimescaleDB` continuous aggregates (basin-cv), `pgcrypto` + `uuid-ossp` UDFs.
- **Auth + REST in the OSS bundle** — basin-auth (signup, JWT, refresh-token rotation, email-link login, per-project API keys) + basin-rest (PostgREST-shape CRUD, cursor pagination + NDJSON streaming, OpenAPI 3.0 schema generation at `GET /rest/v1/_openapi.json`). **`auth.uid()`**, **`auth.role()`**, **`auth.jwt()`** SQL session functions let you write Supabase-style RLS policies.
- **Per-project connection URLs** — `POST /admin/v1/projects` returns `postgres://<user>:<password>@host:5433/<db>`. Password bcrypt-validated on every pgwire startup; mismatch → SQLSTATE `28P01`. Rotate via `POST /admin/v1/projects/{user}/rotate`.
- **Durable catalog** — Iceberg-style catalog backed by Postgres when `BASIN_CATALOG=postgres://...`; tables, snapshots, project credentials, and `basin-auth`'s identity tables survive process restart.
- **Cheap retention** — Vortex (default, ~1.95× smaller than ZSTD Parquet) or Parquet, ~24× smaller than Postgres heap on audit-log data; per-file catalog `column_stats` + per-file bloom filters on `basin.sort_by` columns skip footer fetches and file opens when the predicate prunes the file.
- **Analytical path** — a single DataFusion engine with Vortex/Parquet projection + predicate pushdown, catalog-statistics file pruning, per-file blooms, and incremental continuous materialized views. Approximate-cardinality and approximate-quantile UDFs (`APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`) sit alongside exact counterparts for dashboard workloads. Heavy scans use stateless pooled compute over shared object storage — elastic scale-out without a second engine.
- **Multi-schema isolation (phase A)** — `SchemaName` / `QualifiedTableName` types, a schema-aware in-memory *and* Postgres-backed catalog, a `basin_schemas` table, and `CREATE/DROP SCHEMA` + cross-schema queries with differential coverage. Phases B–E (full name resolution / search_path semantics / wider DDL) are still in progress — see Status.
- **Operations** — connection pooling, per-project pgwire rate limiting (token-bucket via `governor`), cost-based query rejection (`BASIN_QUERY_COST_LIMIT_ROWS`), per-project counters (ops / bytes_read / bytes_written / errors / p99), OpenTelemetry traces wired through router → engine → shard → storage → WAL.

The full capability matrix (with what's planned and what's deferred): [`CAPABILITIES.md`](./CAPABILITIES.md). The fine-grained per-syntax matrix derived from automated tests: [`docs/sql-support.md`](./docs/sql-support.md).

---

## PostgreSQL SQL support

Basin parses and runs real PostgreSQL syntax. The exact per-statement matrix is auto-generated by [`tests/integration/tests/sql_support_matrix.rs`](./tests/integration/tests/sql_support_matrix.rs) and lives in [`docs/sql-support.md`](./docs/sql-support.md) — it re-runs on every `cargo test`, so the numbers stay honest.

> Basin is Postgres-*compatible*, not actual Postgres. **~91% of common PG syntax runs end-to-end (~94% excluding deliberately out-of-scope features)** on the default config; see [`docs/sql-support.md`](./docs/sql-support.md) for the full per-fragment breakdown and known gaps.

**At a glance** (Default config — the sqlparser-rs front-end that ships in v0.1; the matrix tests **697 fragments** across three parser configurations):

| Total | ✅ end-to-end | 🛠 runtime gap | 📜 planner gap | ❌ parser gap | 🚫 design-excluded |
|---|---|---|---|---|---|
| 691 | **629** (91.0% of all · **94.3%** of the 667 non-excluded) | 1 | 24 | 13 | 24 |

The header line of [`docs/sql-support.md`](./docs/sql-support.md) reports **697 fragments tested / 1905 green across all three configurations**; the table above is the Default column that ships in v0.1.

**Intentionally out of scope (🚫 design-excluded).** `LISTEN/NOTIFY/UNLISTEN`, `CREATE TRIGGER` / `CREATE CONSTRAINT TRIGGER`, `CREATE OPERATOR`, composite `CREATE TYPE … AS (…)`, and exotic Postgres-only types (multirange, `OID`, `REGCLASS`, `BIT`, `PG_LSN`). The rationale is in [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md). Note: server-side `PREPARE/EXECUTE`, `DECLARE CURSOR`/`FETCH`/`MOVE`, `VACUUM`/`ANALYZE`/`CLUSTER`, `CREATE EXTENSION`, and `DROP TABLE` now run end-to-end — they were previously listed as design-excluded and have been de-classified.

**Remaining real gaps (v0.2 targets):** a handful of parser-refused exotic DDL (`CREATE AGGREGATE` / `RULE` / `EVENT TRIGGER` / `PUBLICATION` / `SUBSCRIPTION` / `COLLATION`), planner-rejected exotic column types, and long-tail window-frame / set-op forms. Every red row in the matrix links its exact failure mode to the planner / parser / executor layer that owns it.

> **See [`docs/sql-support.md`](./docs/sql-support.md) for the full per-statement matrix** — every row links its exact failure mode to the planner / parser / executor layer that owns it.

---

## Status

| Phase | Description | Status |
|---|---|---|
| **0** | Validate the wedge — customer interviews, design partners | **open** (the gate; engineering is mature enough to need customer signal next) |
| **1** | Storage substrate — Vortex (default) / Parquet on object_store, Iceberg-style catalog | **shipped** |
| **2** | WAL service — sub-5 ms write acks | **v0.1 shipped** (single-node; Raft is v0.2) |
| **3** | Shard owners — per-project state, eviction, compactor | **v0.1 shipped** (in-process; placement service is v0.2) |
| **4** | Routers + SQL — pgwire v3, extended query, TLS, COPY, native JSONB / UUID binding | **shipped** — real single-shard transaction semantics (deferred commits, `ROLLBACK` undo, `SAVEPOINT` stack, aborted state) landed; cross-shard 2PC remains v0.2 (ADR 0011) |
| **4.5** | PostgreSQL SQL-compatibility push — silent-corruption CRITICAL fixes, JSONPath / JSONB-mutating / INET-CIDR / regexp / datetime function families, correct NUMERIC + ARRAY binary wire formats, PG-oracle differential harness (`differential_pg.rs`) | **shipped** — Default config at ~91% / ~94% non-excluded (629/667); long-tail exotic-DDL parser gaps remain v0.2 |
| **5** | Analytical path — single DataFusion engine, Vortex/Parquet pushdown + per-file bloom + catalog pruning, continuous pre-aggregation, `APPROX_COUNT_DISTINCT`/`APPROX_PERCENTILE` UDFs | **v0.1 shipped** |
| **5.0a** | Vortex storage format — ~1.95× smaller than ZSTD Parquet; `aggregate_full` ~15–40× via catalog-stats metadata path; per-file blooms flip `point_eq` from a loss to a win at every scale; majority of analytical shapes win in release-mode smoke | **shipped as the DEFAULT** ([ADR 0015](./docs/decisions/0015-vortex-storage-format.md)), zero-regression vs Parquet baseline (Vortex⇆Parquet differential + full SQL-matrix + ORM-compat green). Parquet first-class per-table via `WITH (basin.file_format='parquet')` (Iceberg / Athena / Spark interchange path). HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) is Phase 5.14.C — closes the residual OLTP point-read floor on TB-scale tables. |
| **5.14** | Durable Basin moat — per-file catalog blooms (shipped), `APPROX_COUNT_DISTINCT` + `APPROX_PERCENTILE` UDFs (shipped), catalog-aware `WindowExec` sort-elision (shipped), HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md), in progress), adaptive multi-sort + query history (in progress). The 3-month investment that is **not** subsumed by upstream Vortex / DataFusion improvements. | **in flight** — A/B3/D3 sub-items shipped 2026-05-19; HTAP C1-C6 next |
| **5.15** | Unified docs platform — OSS-repo markdown with YAML frontmatter ([spec](./docs/frontmatter-spec.md)), `basin-cloud` webapp consumes via `npm run dev:docs` build-time fetch | **OSS side shipped** (5.15.A/B/C, frontmatter spec + 24-doc migration + top-level index + CI gate); `basin-cloud` webapp side (5.15.E–I) deferred to that repo |
| **5.5** | Sharding axes — partitioning, compute sharding, tiered storage | **shipped** |
| **5.6** | RLS with `CREATE POLICY` (UNION / CTE coverage) | **shipped** |
| **5.7** | Caches + bloom + A4 catalog stats + B2 cluster-by + B3 row-group sizing | **shipped**; B1 secondary indexes is the biggest open perf win (~8 weeks) |
| **5.8** | `pg_cron` + `pg_net` SQL surfaces | **shipped** |
| **5.9** | Postgres-extension equivalents (basin-geo / -trgm / -cv, JSONB, UUID, pgcrypto) | **shipped** |
| **5.10** | Identity + REST (basin-auth, basin-rest, OpenAPI, pagination, streaming, API keys, refresh rotation, per-project connection URLs, **`auth.uid()` / `auth.role()` / `auth.jwt()`** session functions) | **shipped** |
| **5.11** | Multi-schema isolation | **phase A shipped** — `SchemaName`/`QualifiedTableName` types, schema-aware in-memory + Postgres catalog, `basin_schemas` table, `CREATE/DROP SCHEMA` + cross-schema queries with differential coverage. **Phases B–E in progress** — full qualified-name resolution, `search_path` semantics, wider schema-scoped DDL |
| **6** | Production hardening | **partial** — telemetry / pooling / rate-limit / cost-rejection / catalog-PITR / fork shipped; multi-region (ADR 0009), catalog replication (ADR 0010), cross-shard 2PC (ADR 0011) all locked architecturally and gated on customer demand |
| **6.x** | SQL long-tail (still pending) | **planned** — `COPY FROM STDIN` ergonomics, server-side `PREPARE/EXECUTE` over text protocol edge cases, `LISTEN/NOTIFY`, plpgsql `DO` blocks, full `MERGE`, exotic types (multirange / `BIT` / `OID` / `REGCLASS`), and the parser-refused exotic DDL forms |
| **7** | Launch | gated on Phase 0 |

Six-month wedge slice: [`WEDGE.md`](./WEDGE.md). Full plan: [`TASK.md`](./TASK.md). Decision log: [`docs/decisions/`](./docs/decisions/).

---

## How Basin compares

### vs Postgres / Aurora / RDS

Postgres is the right answer for high-frequency single-project OLTP. **Basin is not trying to be Postgres.** Where Basin wins: workloads where storage cost matters, where you have multiple isolated databases (one per environment, one per customer, one per region), or where you want object-storage economics with relational semantics.

### vs Neon

Neon is serverless Postgres with branching — terrific for **single-DB workloads** that want copy-on-write forks. Basin matches the branching story (Iceberg forks are zero-copy too) but stores data on plain S3 rather than a managed page server, which is cheaper to operate per project and per GB. Neon's per-project minimum scales with project count (O(provisioned pool)); Basin's per-project cost is O(bytes), so many isolated projects stay cheap.

### vs Supabase

Supabase is "BaaS in a box" — Postgres + Auth + Edge Functions + Storage + Realtime. Basin covers the same SQL + Auth + REST surface in one binary, with `auth.uid()` / `auth.role()` / `auth.jwt()` working identically. Where Basin differs is the data-layer economics: Vortex/Parquet on S3 instead of Postgres heap on block storage. Multi-project SaaS that has outgrown Supabase's per-project pricing can migrate the database to Basin via pgwire and keep Supabase Auth, Edge Functions, and Realtime for the parts of the stack they handle well — or run entirely on Basin using basin-auth and basin-rest. Edge Functions / Realtime / Storage are out of scope per ADRs 0005/0006.

### vs Turso / libSQL

Turso is the right answer for **edge-distributed apps** with many tiny SQLite-class databases. Basin is for centralized apps that want **Postgres SQL on cheap object storage** — ZSTD compression, time travel via Iceberg, and a real wire-protocol Postgres surface that ORMs already speak.

### vs ClickHouse / DuckDB / data-warehouse

ClickHouse and DuckDB are analytical engines — phenomenal at OLAP scans, not designed for transactional point reads or per-row inserts. Basin handles analytics on a single DataFusion engine: Vortex (default) / Parquet projection + predicate pushdown skips data before the object-store GET, catalog-statistics file pruning + per-file blooms on `basin.sort_by` columns skip irrelevant files entirely, and incremental continuous materialized views make expensive aggregations nearly free at query time. Approximate-cardinality / quantile UDFs (`APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`) cover the dashboard-workload shapes. Stateless pooled compute over shared object storage adds elastic scale-out for the residual heavy scans. The HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) is what closes the OLTP point-read floor on TB-scale tables — the missing piece that lets Basin span both OLAP and OLTP without a second engine.

### Where Basin is *not* the answer

Per the ADRs:

- **Single-project high-frequency OLTP** → Postgres / Aurora / Neon
- **Edge / local-first** → Turso / libSQL / Cloudflare D1
- **Geospatial primary store** → PostGIS
- **Embedding-only workload** → dedicated vector DB (Qdrant, Pinecone) — but Basin's native `vector(N)` works fine *alongside* tabular data
- **Embedded SQLite-class library** → SQLite
- **Globally strongly-consistent writes across regions** → Spanner / CockroachDB

---

## Use cases

- **Multi-environment apps** — dev / staging / prod / per-region as cheap projects on one cluster. See [`docs/multi-project.md`](./docs/multi-project.md) for the multi-project SaaS story (per-customer-project isolation, noisy-neighbor scheduler, RLS with `auth.uid()`, cost math at 10k projects).
- **Audit / event logs** — Vortex BtrBlocks compression (~1.95× smaller than ZSTD Parquet, ~24× smaller than Postgres heap) makes append-mostly workloads dramatically cheaper than Postgres heap.
- **AI agent / RAG platforms** — native `vector(N)` + HNSW alongside transactional rows in the same database.
- **Document / activity stores** — write-cheap, analytical-read-occasionally workloads where Vortex/Parquet pushdown pruning, catalog blooms, and continuous pre-aggregation keep analytical queries fast without a second engine.
- **Projected SaaS** — one Basin cluster replaces hundreds of separate Postgres / Neon / Supabase projects with their per-project minimums. See [`docs/multi-project.md`](./docs/multi-project.md).

If you're a single-app developer building a side project, **use Postgres or the Free tier above**. The cost math doesn't show its full effect until you have multiple projects or multi-GB tables.

---

## Ecosystem

Basin (this repo) is the data plane. Three sibling repos sit around it:

- **[`bas-in/basin-cloud`](https://github.com/bas-in/basin-cloud)** — control plane and dashboard (Go + Vite/JSX SPA, Apache-2.0). Manages orgs, projects, billing; runs Basin engines on Fly Machines per project. Operators who want a managed UI use it. Operators running a single self-hosted engine do not — basin-server alone is sufficient.
- **[`bas-in/basin-cli`](https://github.com/bas-in/basin-cli)** — operator daily-driver (Go, Apache-2.0, stdlib-only). `basin login`, `basin projects list`, `basin sql run`, release artefacts are Sigstore-signed. Talks to basin-cloud's `/v1/*` API.
- **[`bas-in/basin-js`](https://github.com/bas-in/basin-js)** — TypeScript SDK (MIT). Supabase-shaped `createClient(url, anonKey)` that talks **directly** to a Basin engine (pgwire + REST), not through basin-cloud. Browser, Node, Deno, Bun, Cloudflare Workers. [`jsr:@bas-in/basin-js`](https://jsr.io/@bas-in/basin-js) and [`npm:@bas-in/basin-js`](https://www.npmjs.com/package/@bas-in/basin-js).
- **Planned client SDKs** — basin-py, basin-rs, basin-go, basin-dart, basin-swift, basin-kotlin. All will follow the same engine-direct shape as basin-js.

**Licensing rationale.** Server-side projects (basin engine, basin-cloud, basin-cli) are Apache-2.0 to carry the patent grant operators expect from infrastructure. Client SDKs (basin-js and future siblings) are MIT to match the norm of the SDK ecosystems they sit in.

---

## Project layout

```
crates/
  basin-common      shared types, errors, telemetry
  basin-storage     Vortex (default) / Parquet + object_store under project prefixes
  basin-catalog     Iceberg-style catalog (in-memory + Postgres-backed durable)
  basin-wal         file-backed WAL (Raft-backed in v0.2)
  basin-shard       in-process shard owner with WAL → Vortex/Parquet compactor
  basin-engine      single DataFusion engine — point reads + analytical pool, per-project sessions
  basin-router      pgwire v3 (simple + extended query)
  basin-vector      native HNSW vector search
  basin-placement   (Phase 3 v0.2) (project, partition) → owner mapping
services/
  basin-server      single-process binary
benchmark/          dashboard + auto-regenerated RESULTS_localfs.md
docs/
  architecture.md   the four-layer stack, in detail
  multi-project.md  the multi-project SaaS story (per-project isolation, scheduler, cost math)
  decisions/        ADRs — every "no" with the trigger that would change our mind
  sql-compatibility.md  hand-written compatibility narrative (planner / catalog scope)
  sql-support.md    auto-generated per-syntax matrix (sql_support_matrix.rs)
tests/integration/  cross-crate viability + scaling + Postgres comparisons
```

---

## Build and test

```sh
# Workspace build:
cargo build --workspace
cargo test  --workspace

# Run the benchmark suite + regenerate dashboard / RESULTS_localfs.md:
cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py

# Then open the dashboard (no server required):
open benchmark/index_localfs.html
```

---

## License

**Apache-2.0** — see [`LICENSE`](./LICENSE).

Contributions welcome. The project is opinionated about scope ([`docs/decisions/`](./docs/decisions/)) — open an issue before writing a PR that adds new surface area. The OSS code is the database; commercial cloud orchestration lives in a separate private repo and never affects what OSS users get.

---

## Keywords for search

Basin is a **Postgres-compatible database on object storage**, with **Vortex** (default columnar, LFAI) and **Apache Parquet** (opt-in, interchange) storage, an **Apache Iceberg** catalog, a file-backed WAL with a Raft WAL simulation toward distributed v0.2, **native vector search** (HNSW), per-file **catalog bloom filters** for point-query pruning, **HTAP hot tier** on the roadmap ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)), and **pgwire** protocol support that works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, and any other Postgres driver. Basin compares to **Postgres**, **Neon**, **Supabase**, **Turso**, **PlanetScale**, **Aurora**, **ClickHouse**, **SingleStore**, **DuckDB**, and **CockroachDB** for cheap-storage SaaS, audit-log, RAG / vector, HTAP, and multi-project use cases. Self-hostable, **Apache-2.0** licensed, written in **Rust**.
