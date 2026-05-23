<p align="center">
  <img src="./basin.svg" alt="Basin" width="180">
</p>

<h1 align="center">Basin</h1>

<p align="center">
  <strong>Bucket-native, multi-tenant Postgres alternative.</strong><br>
  RAM-per-connection is ~47× cheaper than Postgres. Projects are S3 prefixes,
  not databases — operator cost is O(bytes active), not O(projects
  provisioned). One binary, pgwire on the front, Vortex-compressed columnar
  files on any S3-compatible bucket on the back.
</p>

<p align="center">
  <a href="https://github.com/bas-in/basin/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/bas-in/basin/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/bas-in/basin/releases"><img alt="latest release" src="https://img.shields.io/github/v/release/bas-in/basin?include_prereleases&style=flat-square"></a>
  <a href="./CHANGELOG.md"><img alt="changelog" src="https://img.shields.io/badge/changelog-keep--a--changelog-blue?style=flat-square"></a>
  <a href="./WEDGE.md"><img alt="status: pre-alpha" src="https://img.shields.io/badge/status-pre--alpha-orange?style=flat-square"></a>
  <a href="./benchmark/BENCHMARKS.md"><img alt="honest benchmarks" src="https://img.shields.io/badge/benchmarks-honest_wins_and_losses-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: 47x less RAM/conn" src="https://img.shields.io/badge/vs_postgres-47%C3%97_less_RAM%2Fconn-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: 29x smaller on disk (1M SaaS)" src="https://img.shields.io/badge/vs_postgres-29%C3%97_smaller_on_disk-blue?style=flat-square"></a>
  <a href="./CAPABILITIES.md"><img alt="capabilities" src="https://img.shields.io/badge/capabilities-matrix-blue?style=flat-square"></a>
  <a href="./docs/sql-support.md"><img alt="SQL support matrix" src="https://img.shields.io/badge/SQL_support-matrix-blue?style=flat-square"></a>
  <a href="./LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey?style=flat-square"></a>
</p>

> **Pre-alpha. Do not run in production.** Basin is being built in the open.
> Use it today to evaluate cost economics, prototype multi-tenant patterns,
> or contribute. The OLTP write path is architecturally slow until the
> hot-tier UPDATE/DELETE routes finish landing (see [Status](#status) and
> [ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)).
>
> See [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md) for the v0.1 cut-off — required items, what's shipped, and what's parked.

---

## Why Basin

Two things are structurally true regardless of workload, and they're the load-bearing reason Basin exists:

1. **Pure-Rust async server, ~298 KiB RAM per held-open connection vs Postgres's ~7.7 MiB.** That's ~26× per the LocalFS lifecycle card and **~47× when you count the long-tail noise floor**. Under a 1,000-connection flood, Basin holds 1,000 / refuses 0; Postgres holds 99 / refuses 901. This isn't a tuning result — it's the difference between a from-scratch tokio server and a forking daemon. ([`server_lifecycle` card](./benchmark/RESULTS_localfs.md#server-lifecycle-accept-conn-scaling-rssconn))
2. **A new project is an S3 bucket prefix, not a provisioned database.** Idle projects cost only their bytes — measured at ~2 KiB of RAM and **$0.10/month/project** at typical SaaS-tail sizes. Spin up one project for a side app, or ten thousand for a SaaS — same architecture, same binary. ([`idle-project cost curve`](./benchmark/RESULTS_localfs.md#idle-project-cost-curve--pass))

Everything else — Vortex columnar storage, native vector search, basin-auth + basin-rest, time travel via Iceberg snapshots, `pg_cron`/`pg_net`/`pg_trgm`/PostGIS subsets as native crates, real pgwire v3 with TLS + COPY + extended-query + native JSONB/UUID/BYTEA binding — exists to make those two structural wins addressable from real applications. Decision log lives in [`docs/decisions/`](./docs/decisions/).

---

## The numbers (honest)

Basin publishes **all** of its head-to-head numbers, wins and losses, regenerated from integration tests on every push. Some shapes Basin beats Postgres; many it does not. The picture is workload-dependent — the table below is the **1M-row SaaS+OLAP suite, LocalFS, no index either side** (the most apples-to-apples wedge). For 10k / 100k / real-S3 numbers, see the [benchmark results](#references).

| Workload | Basin (Vortex 1M) | Postgres 18 | Verdict |
|---|---|---|---|
| **Structural — wins** | | | |
| RAM per held-open connection | 298 KiB | 7,723 KiB | **~26× less** (47× at the long-tail floor) |
| Connections held under 1,000-conn flood | 1,000 held / 0 refused | 100 held / 900 refused | **structural** |
| On-disk bytes (users + events, 1M rows) | 2.62 MiB | 76.73 MiB | **29× smaller** (Vortex columnar) |
| COUNT(*) full table p50 | 0.23 ms | 21.00 ms | **91× faster** (catalog metadata) |
| ADD COLUMN on 100k rows | 1.23 ms | 2.29 ms | **1.9× faster** (catalog op, no heap rewrite) |
| Backup wall time (100k rows) | ~0 s (manifest copy) | 0.37 s | **structural** (Iceberg snapshot) |
| **Read wins on real-data sizes** | | | |
| Point query p50 (1M, unindexed) | 0.75 ms | 12.21 ms | **16× faster** at 1M (**~2.6× slower** at 10k where PG fits in shared_buffers) |
| Range scan p50 (~1k rows) | 0.74 ms | 21.93 ms | **30× faster** at 1M |
| Cold-start first query | 16.9 ms | 19.4 ms | **1.15× faster** |
| Correlated subquery in SELECT p50 | 419 ms | 2730 ms | **6.5× faster** |
| **Read losses** | | | |
| Aggregate GROUP BY user_id p50 | 501 ms | 133 ms | **3.8× slower** |
| Pagination ORDER BY LIMIT OFFSET p50 | 1016 ms | 62 ms | **16× slower** (upstream Vortex API gap) |
| ILIKE '%@gmail.com' p50 | 103 ms | 25 ms | **4.2× slower** |
| Recursive CTE Fibonacci(30) p50 | 7.0 ms | 0.09 ms | **77× slower** (DataFusion upstream) |
| Window LAG OVER PARTITION p50 | 644 ms | 85 ms | **7.6× slower** |
| Multi-col GROUP BY + HAVING p50 | 1273 ms | 216 ms | **5.9× slower** |
| GIN `@>` effectiveness (no-index ms / with-index ms) | ~1.0× (probe surfaces rows, scan unpruned) | ~10–100× (native GIN) | **architectural** — `CREATE INDEX … USING gin` is accepted but probe→prune wiring is in flight (#105 / Phase 5.24.D); paired no-index vs with-index metric pair lives on the JSONB card in [`RESULTS_localfs.md`](./benchmark/RESULTS_localfs.md) so the gap (and the post-#105 win) are both visible |
| **Write losses (the perf-critical follow-up)** | | | |
| Single-row UPDATE p50 | 4099 ms | 2.64 ms | **~1550× slower** — copy-on-write parquet/vortex rewrite is wrong for OLTP point mutations |
| DELETE WHERE id IN (10 rows) | 1040 s | 56.7 ms | **~18000× slower** — closing via hot-tier tombstone fast path (landed env-gated in `87ef24b`; on by default in 5.14.C5/C6) |
| Bulk INSERT 1,000,000 rows | 147 s | 4.3 s | **34× slower** — WAL→Vortex flush pipeline, not write-amp |

At **10k rows the picture flips**: PG's heap fits in `shared_buffers` and PG wins most OLTP latency metrics. At **100k** Basin matches PG on point/range/COUNT/correlated-subquery and loses on aggregate/pagination/window. At **1M** Basin's algorithmic wins start to compound on the read shapes while the write losses stay structurally bad until the hot-tier UPDATE/DELETE routes are on by default. **On real S3** the picture shifts again — round-trip cost dominates the unindexed cold path; cached / warm paths stay competitive. Full per-scale matrix:

- [`benchmark/RESULTS_localfs.md`](./benchmark/RESULTS_localfs.md) — 10k / 100k / 1M cards, Vortex + Parquet, every shape
- [`benchmark/RESULTS_seaweedfs.md`](./benchmark/RESULTS_seaweedfs.md) — local S3-compatible (SeaweedFS) results, same shape battery
- [`benchmark/RESULTS_real.md`](./benchmark/RESULTS_real.md) — real-cloud S3 results
- [`benchmark/BENCHMARKS.md`](./benchmark/BENCHMARKS.md) — methodology, caveats, reproduction
- Live dashboards: [`benchmark/index_localfs.html`](./benchmark/index_localfs.html), [`benchmark/index_real.html`](./benchmark/index_real.html)

### Think a benchmark is unfair? Tell us.

Every card is generated by an integration test in [`tests/integration/tests/`](./tests/integration/tests/) — read the source and tell us where we're wrong. **File a [Benchmark Methodology issue][bench-issue]** and we'll either fix the methodology, soften the headline claim, or explain why we think the test is fair. Decisions are logged in [`docs/decisions/`](./docs/decisions/).

[bench-issue]: ../../issues/new?template=benchmark_methodology.yml

---

## Where Basin IS the answer

- **Append-heavy multi-tenant SaaS** — many isolated projects, mostly-reads, occasional point UPDATE. The RAM-per-conn + per-project-prefix economics dominate; the OLTP point-write tax stays bounded as long as writes are append-shaped.
- **Audit logs, event streams, IoT, activity feeds** — write-once-read-many, Vortex columnar compression shrinks bytes-at-rest 29× vs PG heap on the 1M SaaS shape and ~43× at 100k. Object-storage $/GB compounds.
- **AI / RAG with mixed tabular + vector data** — native `vector(N)` + HNSW alongside transactional rows in the same database, no `pg_vector` install. Useful when "store the document + the embedding + the audit row in one place" is the requirement.
- **Cheap-idle multi-environment apps** — dev / staging / per-region / per-customer all live as cheap project prefixes on one cluster. Per-project cost is O(bytes), so 10k mostly-idle projects stays cheap. See [`docs/multi-project.md`](./docs/multi-project.md).

## Where Basin is NOT the answer (yet)

- **Drop-in OLTP replacement** — point UPDATE/DELETE is architecturally slow today (~1500–18000× behind PG at 1M). The hot-tier UPDATE/DELETE routes (DELETE landed env-gated in `87ef24b`; UPDATE in flight under [Phase 5.14.C](./TASK.md)) close this — but until they're on by default, don't ship Basin in front of a write-heavy app.
- **Index-heavy workloads** — no btree on non-PK columns yet. `WHERE non_pk_col = X` does a full-file scan. Bloom filters on `basin.sort_by` and per-file `column_stats` prune at the file level, but a btree-class secondary index (Phase 5.7.B1, ~8 weeks) is the biggest single perf win still open.
- **Pure analytical workhorse** — Snowflake / DuckDB / ClickHouse will out-run Basin on heavy GROUP BY / window / recursive-CTE shapes. Basin trades some bench wins for PG-compat, and inherits DataFusion's upstream limits on a handful of shapes (recursive CTE, exact COUNT(DISTINCT)).
- **High-frequency single-DB OLTP** → Postgres / Aurora / Neon. **Edge / local-first** → Turso / libSQL. **Geospatial primary store** → PostGIS. **Embedded SQLite-class** → SQLite. **Globally strongly-consistent multi-region writes** → Spanner / Cockroach.

---

## Postgres compatibility

| Surface | Status |
|---|---|
| **sqllogictest** (curated PG-style suite) | **100% (50/50)** as of [`b7114e8`](https://github.com/bas-in/basin/commit/b7114e8) |
| **ORM corpus** (Drizzle / Prisma / sqlx / Diesel / TypeORM, 47 representative shapes) | **94% (44/47)** — Drizzle 100%, Prisma 91%, sqlx 88%, Diesel 100%, TypeORM 100% |
| **Per-fragment SQL matrix** ([`docs/sql-support.md`](./docs/sql-support.md), 697 fragments tested) | **~91% Default config / ~94% non-excluded** (629/667) |
| **Wasm UDFs** | `i32 / i64 / f64` shipped; `text / bytea / JSONB` in flight |
| **Wire protocol** | pgwire v3, simple + extended query, TLS (rustls), `COPY FROM STDIN / TO STDOUT`, prepared statements with binary parameters (native JSONB / UUID / BYTEA / NUMERIC varlena / ARRAY binary wire formats) |
| **Differential PG-oracle harness** | [`tests/integration/tests/differential_pg.rs`](./tests/integration/tests/differential_pg.rs) — every release runs identical SQL against Basin and a real PostgreSQL; build fails on any cell-level divergence |

Per-statement breakdown with every red row linked to its planner / parser / executor owner: [`docs/sql-support.md`](./docs/sql-support.md). Public capability matrix: [`CAPABILITIES.md`](./CAPABILITIES.md).

**Intentionally out of scope** (per [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)): `LISTEN/NOTIFY`, `CREATE TRIGGER`, `CREATE OPERATOR`, composite `CREATE TYPE`, multirange / `OID` / `REGCLASS` / `BIT` / `PG_LSN`.

---

## Quickstart

**Want to skip the build and start querying right now?**
See the [5-Minute Docker Quickstart](./docs/quickstart-docker.md) — one
`docker run` command, no Rust toolchain required.

**Ready to go deeper?**
The [Getting Started / Tutorial](./docs/tutorial.md) walks you through
CRUD, auth, RLS policies, the REST API, a React/Vite frontend snippet,
and the first-deployment path — about 15 minutes end-to-end.

**Want to see a complete app?**
Two reference apps are in [`examples/`](./examples/):

- [`examples/saas-starter/`](./examples/saas-starter/) — multi-tenant SaaS app: Drizzle ORM, basin-auth, RLS policies, basin-rest auto-generated REST surface.
- [`examples/ai-rag-app/`](./examples/ai-rag-app/) — AI/RAG app: document chunking + embedding pipeline, basin-vector similarity retrieval, Wasm function calling an inference endpoint.

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
- **CRUD + DDL** — `CREATE TABLE` (incl. `CREATE TABLE AS … WITH NO DATA`), multi-row `INSERT`, `SELECT`, `UPDATE`, `DELETE` (Iceberg copy-on-write today; hot-tier tombstone fast path env-gated in `87ef24b`), `ON CONFLICT DO NOTHING / DO UPDATE` upsert, `ALTER TABLE … CLUSTER BY (…) / SET BLOOM FILTERS ON / SET row_group_rows / SET cold_after / ENABLE ROW LEVEL SECURITY / CREATE POLICY`, `SHOW TABLES`. Prepared statements with parameter bind (text + binary, including native JSONB / UUID and correct NUMERIC / ARRAY binary wire formats).
- **Honest enforcement, not silent no-ops** — `CREATE UNIQUE INDEX` actually enforces uniqueness, `VARCHAR(n)`/`CHAR(n)` length is enforced, RLS `WITH CHECK` is enforced on write, `TABLESAMPLE` actually samples, advisory locks are real, and unsupported `CREATE TRIGGER` / `MERGE` honest-reject with a SQLSTATE instead of silently doing nothing. A wave of silent-corruption CRITICALs surfaced by the differential harness were fixed.
- **Expanded SQL surface** — JSONPath (`jsonb_path_query`, `@?`, `@@`, `jsonb_path_query_array`); JSONB mutators (`jsonb_set`/`insert`/`strip_nulls`/`pretty`/`typeof`); `json_build_object`/`json_build_array`; INET/CIDR containment; `regexp_match`/`matches`/`split_to_array`/`split_to_table`, `format`, `encode`/`decode`; datetime `age`/`to_char`/`to_date`/`date_bin`; window `IGNORE NULLS`; `SAVEPOINT` / `ROLLBACK TO`; data-modifying CTEs; correlated + `LATERAL` joins (incl. `CROSS JOIN LATERAL generate_series`); bounded full-text search (`tsvector`/`tsquery`/`@@`); ordered-set aggregates (`percentile_disc`, `mode() WITHIN GROUP`); range/multirange arithmetic; real transaction semantics (deferred commits, `ROLLBACK` undo, SAVEPOINT stack, aborted state).
- **Time travel** — Iceberg-style snapshots. `Catalog::rollback_to_snapshot(project, table, snapshot_id)` rewinds; `Catalog::fork_table(project, src, dst)` clones a table's metadata + snapshot history into a new sibling that diverges on next commit. Zero data copy until divergence.
- **Native vector search** — `vector(N)` + `<->` / `<#>` / `<=>` operators, HNSW per file segment. No `pg_vector`.
- **Postgres-extension equivalents** — `pg_cron` (basin-cron), `pg_net` + `http` (basin-net), `pg_trgm` (basin-trgm), `PostGIS` subset (basin-geo), `TimescaleDB` continuous aggregates (basin-cv), `pgcrypto` + `uuid-ossp` UDFs.
- **Auth + REST in the OSS bundle** — basin-auth (signup, JWT, refresh-token rotation, email-link login, per-project API keys) + basin-rest (PostgREST-shape CRUD, cursor pagination + NDJSON streaming, OpenAPI 3.0 schema generation at `GET /rest/v1/_openapi.json`). **`auth.uid()`**, **`auth.role()`**, **`auth.jwt()`** SQL session functions let you write Supabase-style RLS policies.
- **Per-project connection URLs** — `POST /admin/v1/projects` returns `postgres://<user>:<password>@host:5433/<db>`. Password bcrypt-validated on every pgwire startup; mismatch → SQLSTATE `28P01`. Rotate via `POST /admin/v1/projects/{user}/rotate`.
- **Durable catalog** — Iceberg-style catalog backed by Postgres when `BASIN_CATALOG=postgres://...`; tables, snapshots, project credentials, and `basin-auth`'s identity tables survive process restart.
- **Cheap retention** — Vortex (default, ~1.95× smaller than ZSTD Parquet on audit-log) or Parquet, **~29× smaller than Postgres heap on the 1M SaaS+OLAP shape, ~43× at 100k**; per-file catalog `column_stats` + per-file bloom filters on `basin.sort_by` columns skip footer fetches and file opens when the predicate prunes the file.
- **Analytical path** — a single DataFusion engine with Vortex/Parquet projection + predicate pushdown, catalog-statistics file pruning, per-file blooms, and incremental continuous materialized views. Approximate-cardinality and approximate-quantile UDFs (`APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`) sit alongside exact counterparts for dashboard workloads. Heavy scans use stateless pooled compute over shared object storage.
- **Multi-schema isolation (phase A)** — `SchemaName` / `QualifiedTableName` types, a schema-aware in-memory *and* Postgres-backed catalog, a `basin_schemas` table, and `CREATE/DROP SCHEMA` + cross-schema queries with differential coverage. Phases B–E (full name resolution / search_path semantics / wider DDL) are still in progress — see Status.
- **Operations** — connection pooling, per-project pgwire rate limiting (token-bucket via `governor`), cost-based query rejection (`BASIN_QUERY_COST_LIMIT_ROWS`), per-project counters (ops / bytes_read / bytes_written / errors / p99), OpenTelemetry traces wired through router → engine → shard → storage → WAL.

The full capability matrix (with what's planned and what's deferred): [`CAPABILITIES.md`](./CAPABILITIES.md). The fine-grained per-syntax matrix derived from automated tests: [`docs/sql-support.md`](./docs/sql-support.md).

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
| **5.0a** | Vortex storage format — ~1.95× smaller than ZSTD Parquet; `aggregate_full` ~15–40× via catalog-stats metadata path; per-file blooms flip `point_eq` from a loss to a win at every scale | **shipped as the DEFAULT** ([ADR 0015](./docs/decisions/0015-vortex-storage-format.md)), zero-regression vs Parquet baseline. Parquet first-class per-table via `WITH (basin.file_format='parquet')`. HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) is Phase 5.14.C — closes the residual OLTP point-read and the UPDATE/DELETE write floor. |
| **5.14** | Durable Basin moat — per-file catalog blooms (shipped), `APPROX_COUNT_DISTINCT` + `APPROX_PERCENTILE` UDFs (shipped), catalog-aware `WindowExec` sort-elision (shipped), **DELETE → hot-tier tombstone fast path (env-gated, `87ef24b`)**, HTAP hot-tier read-merge + memory budget + on-by-default rollout (in progress, C3/C5/C6), adaptive multi-sort + query history (in progress). The 3-month investment that is **not** subsumed by upstream Vortex / DataFusion improvements. | **in flight** |
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

Postgres is the right answer for **single-project, high-frequency OLTP**, and for **point-mutation-heavy workloads** until Basin's hot-tier UPDATE/DELETE routes are on by default. **Basin is not trying to be Postgres on those shapes.** Where Basin wins: many-isolated-projects (per-environment / per-customer / per-region), append-shaped workloads where the columnar bytes-at-rest savings compound, and the structural RAM-per-connection economics for connection-heavy front ends.

### vs Neon

Neon is serverless Postgres with branching — terrific for **single-DB workloads** that want copy-on-write forks. Basin matches the branching story (Iceberg forks are zero-copy too) but stores data on plain S3 rather than a managed page server. Neon's per-project minimum scales with project count (O(provisioned pool)); Basin's per-project cost is O(bytes), so many isolated projects stay cheap.

### vs Supabase

Supabase is "BaaS in a box" — Postgres + Auth + Edge Functions + Storage + Realtime. Basin covers the SQL + Auth + REST surface in one binary, with `auth.uid()` / `auth.role()` / `auth.jwt()` working identically. Where Basin differs is the data-layer economics: Vortex/Parquet on S3 instead of Postgres heap on block storage. Multi-project SaaS that has outgrown Supabase's per-project pricing can migrate the database to Basin via pgwire and keep Supabase Auth / Edge Functions / Realtime for the parts of the stack they handle well. Edge Functions / Realtime / Storage are out of scope per ADRs 0005/0006.

### vs Turso / libSQL

Turso is the right answer for **edge-distributed apps** with many tiny SQLite-class databases. Basin is for centralized apps that want **Postgres SQL on cheap object storage** with a real wire-protocol surface that ORMs already speak.

### vs ClickHouse / DuckDB / data-warehouse

ClickHouse and DuckDB are analytical engines — phenomenal at OLAP scans, not designed for transactional point reads or per-row inserts. **Basin will lose to a dedicated warehouse on heavy GROUP BY / window / recursive-CTE shapes** (see the perf table above). Basin's pitch is the unified path: one engine, one binary, pgwire on the wire, columnar substrate underneath — useful when "Snowflake plus a Postgres" is overkill for the workload size. The HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) closes the OLTP point-read and point-write floor on TB-scale tables — the missing piece that lets one engine span OLAP and OLTP without a second system.

### Where Basin is *not* the answer

Per the ADRs:

- **Single-project high-frequency OLTP** → Postgres / Aurora / Neon
- **Edge / local-first** → Turso / libSQL / Cloudflare D1
- **Geospatial primary store** → PostGIS
- **Embedding-only workload** → dedicated vector DB (Qdrant, Pinecone) — but Basin's native `vector(N)` works fine *alongside* tabular data
- **Embedded SQLite-class library** → SQLite
- **Globally strongly-consistent writes across regions** → Spanner / CockroachDB
- **Heavy OLAP workhorse** → Snowflake / ClickHouse / DuckDB

---

## References

- [`benchmark/BENCHMARKS.md`](./benchmark/BENCHMARKS.md) — methodology, caveats, reproduction steps
- [`benchmark/RESULTS_localfs.md`](./benchmark/RESULTS_localfs.md) — 10k / 100k / 1M SaaS+OLAP cards (Vortex + Parquet), every shape
- [`benchmark/RESULTS_seaweedfs.md`](./benchmark/RESULTS_seaweedfs.md) — local S3-compatible (SeaweedFS) battery
- [`benchmark/RESULTS_real.md`](./benchmark/RESULTS_real.md) — real-cloud S3
- [`benchmark/index_localfs.html`](./benchmark/index_localfs.html) / [`benchmark/index_real.html`](./benchmark/index_real.html) — live dashboards (no server needed)
- [`CAPABILITIES.md`](./CAPABILITIES.md) — public capability matrix
- [`docs/sql-support.md`](./docs/sql-support.md) — per-syntax SQL matrix (auto-generated)
- [`docs/decisions/`](./docs/decisions/) — ADRs (every "no" with the trigger that would change our mind)
- [`docs/architecture.md`](./docs/architecture.md) — the four-layer stack in detail

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
  basin-hottier     in-memory hot tier (read-merge + tombstone fast path; on-by-default rollout in 5.14.C5/C6)
  basin-placement   (Phase 3 v0.2) (project, partition) → owner mapping
services/
  basin-server      single-process binary
benchmark/          dashboard + auto-regenerated RESULTS_*.md
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

Basin is a **Postgres-compatible, bucket-native, multi-tenant database on object storage**, with **Vortex** (default columnar, LFAI) and **Apache Parquet** (opt-in, interchange) storage, an **Apache Iceberg** catalog, a file-backed WAL with a Raft WAL simulation toward distributed v0.2, **native vector search** (HNSW), per-file **catalog bloom filters** for point-query pruning, **HTAP hot tier** on the roadmap ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)), and **pgwire** protocol support that works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, and any other Postgres driver. Basin compares to **Postgres**, **Neon**, **Supabase**, **Turso**, **PlanetScale**, **Aurora**, **ClickHouse**, **SingleStore**, **DuckDB**, and **CockroachDB** for cheap-storage SaaS, audit-log, RAG / vector, HTAP, and multi-project use cases. Self-hostable, **Apache-2.0** licensed, written in **Rust**.
