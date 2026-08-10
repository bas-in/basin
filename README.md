<!-- One asset, both themes: brand/logo.svg is a self-contained tile (ink #0E2A2B
     ground, bone #FAF7F0 strokes), so it reads on GitHub light and dark alike.
     Do not add a prefers-color-scheme variant — a second drawing is a second
     mark, and a second mark drifts. See the Brand section below. -->
<p align="center">
  <img src="./brand/logo.svg" alt="Basin" width="96" height="96">
</p>

<h1 align="center">Basin</h1>

<p align="center">
  <strong>Bucket-native, multi-tenant Postgres alternative.</strong>
</p>

<p align="center">
  <sub><img src="docs/assets/vulos-logo.png" height="14" alt="VulOS"> Part of <strong><a href="https://vulos.org">VulOS</a></strong> — the open, self-hostable web OS &amp; app suite. Runs standalone, or as an app hosted by the Vulos OS.</sub>
</p>

<p align="center">
  <a href="https://github.com/vul-os/basin/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/vul-os/basin/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/vul-os/basin/releases"><img alt="latest release" src="https://img.shields.io/github/v/release/vul-os/basin?include_prereleases&style=flat-square"></a>
  <a href="./CHANGELOG.md"><img alt="changelog" src="https://img.shields.io/badge/changelog-keep--a--changelog-blue?style=flat-square"></a>
  <a href="./WEDGE.md"><img alt="status: pre-alpha" src="https://img.shields.io/badge/status-pre--alpha-orange?style=flat-square"></a>
  <a href="./benchmark/BENCHMARKS.md"><img alt="honest benchmarks" src="https://img.shields.io/badge/benchmarks-honest_wins_and_losses-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_localfs.md"><img alt="vs Postgres: 27x less RAM/conn" src="https://img.shields.io/badge/vs_postgres-27%C3%97_less_RAM%2Fconn-blue?style=flat-square"></a>
  <a href="./benchmark/RESULTS_seaweedfs.md"><img alt="vs Postgres: 102x smaller on disk (real S3, 100k)" src="https://img.shields.io/badge/vs_postgres-102%C3%97_smaller_on_disk_(S3)-blue?style=flat-square"></a>
  <a href="./CAPABILITIES.md"><img alt="capabilities" src="https://img.shields.io/badge/capabilities-matrix-blue?style=flat-square"></a>
  <a href="./docs/sql-support.md"><img alt="SQL support matrix" src="https://img.shields.io/badge/SQL_support-matrix-blue?style=flat-square"></a>
  <a href="./LICENSE"><img alt="license: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey?style=flat-square"></a>
</p>

RAM-per-connection is ~30× cheaper than Postgres. Projects are S3 prefixes,
not databases — operator cost is O(bytes active), not O(projects
provisioned). One binary, pgwire on the front, Vortex-compressed columnar
files on any S3-compatible bucket on the back.

> **Pre-alpha — public eval.** Basin is being built in the open. Use it
> today to evaluate cost economics, prototype multi-tenant patterns, or
> contribute. The hot-tier UPDATE/DELETE fast paths are **on by default**
> as of Phase 5.14 closure (`bed431c`). On the fresh integrity cards
> (2026-06-11, single idle-box session; the 1M card run solo, no index
> either side): point query p50 is **sub-ms at every scale** —
> **0.06ms at 10k / 0.12ms at 100k / 0.50ms at 1M** — single-row UPDATE
> p50 lands at **0.33ms at 10k / 0.64ms at 100k / 1.24ms at 1M**, and
> bulk INSERT now **beats Postgres at every scale** (33.6ms vs 117ms at
> 10k; 216ms vs 839ms at 100k; 2.08s vs 8.10s at 1M — see the durability
> note under the table below).
> The OLTP latency profile and per-shape table are in
> [`RESULTS_localfs.md`](./benchmark/RESULTS_localfs.md); the kill-switch
> notes are in [ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)
> if you need to roll the fastpaths back without a redeploy
> (`BASIN_HOTTIER_FASTPATH_DISABLE=1`).
>
> See [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md) for the v0.1 cut-off — required items, what's shipped, and what's parked.

---

## Why Basin

Two things are structurally true regardless of workload, and they're the load-bearing reason Basin exists:

1. **Pure-Rust async server, ~310 KiB RAM per held-open connection vs Postgres's ~8.1 MiB.** That's **~27×** per the LocalFS lifecycle card (measured locally). Under a 1,000-connection flood, Basin holds 1,000 / refuses 0; Postgres holds 100 / refuses 900. This isn't a tuning result — it's the difference between a from-scratch tokio server and a forking daemon. ([`server_lifecycle` card](./benchmark/RESULTS_localfs.md#server-lifecycle-accept-conn-scaling-rssconn))
2. **A new project is an S3 bucket prefix, not a provisioned database.** Idle projects cost only their bytes — measured at **~2 KiB of RAM** (2.16 KiB/project over 1,000 idle projects) and **$0.10/month/project** at typical SaaS-tail sizes. Spin up one project for a side app, or ten thousand for a SaaS — same architecture, same binary. ([`idle-project cost curve`](./benchmark/RESULTS_localfs.md#idle-project-cost-curve--pass))

Everything else — Vortex columnar storage, native vector search, basin-auth + basin-rest, time travel via Iceberg snapshots, `pg_cron`/`pg_net`/`pg_trgm`/PostGIS subsets as native crates, real pgwire v3 with TLS + COPY + extended-query + native JSONB/UUID/BYTEA binding — exists to make those two structural wins addressable from real applications. Decision log lives in [`docs/decisions/`](./docs/decisions/).

---

## The numbers (honest)

Basin publishes **all** of its head-to-head numbers, wins and losses, regenerated from integration tests on every push. Some shapes Basin beats Postgres; many it does not. The picture is workload-dependent — the table below is the **1M-row SaaS+OLAP suite, LocalFS, no index either side** (the most apples-to-apples wedge). For 10k / 100k / real-S3 numbers, see the [benchmark results](#references).

_Numbers below are the live 1M-row Vortex card from the 2026-06-11 integrity run, regenerated by `benchmark/run/run_all.sh` and measured locally in a single idle-box session with the 1M card run solo (Postgres-compare cards run with Basin's default config — the HTAP fastpaths and fast bulk-INSERT encoding are always-on, no non-default flags, as disclosed on the dashboard). Per-shape numbers reduce through a robust estimator (median at n ≥ 5 samples, min-of-K below; 1M shapes sample at least 3×) applied identically to both sides. Write-shape durability note: Basin's default acknowledges INSERTs before fsync (bounded loss window, ≤200 ms) while the Postgres numbers are fsync-durable per commit; `SET basin.synchronous_commit = on` provides group-committed fsync durability per statement — measured: it adds ~2% to the 10k bulk INSERT in the probe harness (group commit amortizes one fsync per statement group). The per-scale matrix in [`RESULTS_localfs.md`](./benchmark/RESULTS_localfs.md) and the real-S3 card below are the fuller picture._

| Workload (1M LocalFS, no index either side) | Basin (Vortex) | Postgres 18 | Verdict |
|---|---|---|---|
| **Structural** | | | |
| RAM per held-open connection | 310 KiB | 8,257 KiB | **~27× less** |
| Connections held under 1,000-conn flood | 1,000 held / 0 refused | 100 held / 900 refused | **structural** |
| On-disk bytes (users + events, 1M rows) | 321 MB | 306 MB | **~5% larger** on this card — honest flip; Basin is still 1.9× smaller at 100k and 102× smaller on real S3 |
| **Analytics / read — wins** | | | |
| LATERAL JOIN (correlated derived table) | 6.7 ms | 3,080 ms | **462× faster** |
| Star join (events ⋈ users ⋈ categories) | 11.6 ms | 3,040 ms | **261× faster** |
| Correlated subquery in SELECT p50 | 49 ms | 5,510 ms | **113× faster** |
| Range scan p50 (~1k rows) | 0.40 ms | 32 ms | **81× faster** (p99 0.70 vs 72 — 101×) |
| WHERE col = ANY(int[]) | 0.60 ms | 38 ms | **63× faster** |
| Selective low-card COUNT (status filter) | 0.68 ms | 34 ms | **51× faster** |
| COUNT(DISTINCT user_id) per status p50 | 31 ms | 913 ms | **30× faster** |
| 2-table JOIN GROUP BY p50 | 28 ms | 791 ms | **28× faster** |
| Large result stream (100k-row drain) p50 | 0.44 ms | 11.0 ms | **25× faster** |
| PERCENTILE_CONT(0.5) per status p50 | 23 ms | 442 ms | **19× faster** |
| UNION (dedup) | 22 ms | 246 ms | **11× faster** |
| Aggregate GROUP BY user_id p50 | 13.6 ms | 137 ms | **10× faster** |
| Window frame SUM (5 PRECEDING) p50 | 9.8 ms | 91 ms | **9.3× faster** |
| ILIKE `'%@gmail.com'` p50 | 4.6 ms | 25 ms | **5.3× faster** |
| **Bulk / concurrent INSERT — wins** | | | |
| Bulk INSERT 1,000,000 rows | 2,080 ms | 8,100 ms | **3.9× faster**† |
| Concurrent INSERT 8×1000 rows | 3.9 ms | 20 ms | **5.2× faster**† |
| Prepared INSERT 100× bind/execute | 4.6 ms | 10.2 ms | **2.2× faster**† |
| **Selective single-row / OLTP — losses (PG's PK btree wins)** | | | |
| Point query p50 (unindexed PK) | 0.50 ms | 0.002 ms | **slower** — sub-ms, vs PG's µs-class PK btree |
| UPSERT (INSERT ON CONFLICT DO UPDATE) | 0.26 ms | 0.02 ms | **slower** — both sub-ms |
| Single-row UPDATE p50 | 1.24 ms | 0.012 ms | **slower** — ms-class vs µs-class (0.33 ms on the 10k card) |
| DELETE WHERE id IN (10 rows) | 1.16 ms | 0.41 ms | **slower** (wins at 10k/100k: 0.16/0.22 ms vs 0.24/2.9 ms) |
| Conditional UPDATE (SET = CASE WHEN) | 4.9 ms | 1.9 ms | **slower** — delta-update overlay, near-parity class |
| Keyset pagination (`WHERE id > … LIMIT`) | 23.5 ms | 0.01 ms | **slower** — measures 0.07 ms on the 10k card; see note below |
| LIMIT without ORDER BY (early-exit scan) | 52 ms | 0.03 ms | **slower** — 0.06 ms on the 10k card; see note below |
| Deep top-K sort (ORDER BY amount LIMIT 1000) | 161 ms | 53 ms | **slower** — whole-table wide-decode floor |
| DISTINCT ON first-row per group p50 | 155 ms | 92 ms | **slower** (wins at 10k: 1.9 vs 2.2 ms) |
| ARRAY_AGG + ORDER BY in aggregate | 516 ms | 90 ms | **slower** (parity at 10k: 3.2 vs 3.4 ms) |
| COUNT(*) full table p50 | 95 ms | 29 ms | **slower** on this card |
| JSONB `->>` get-text p50 (steady, promoted) | 1.97 ms | 0.11 ms | **slower** — promoted-column read; PG's btree wins selective JSONB |
| JSONB `@>` / `?` / filter+agg | 240–1,234 ms | 53–108 ms | **slower** — GIN effectiveness 1.12× at 1M (4.3× at 10k); see note below |
| MERGE INTO (matched/not-matched upsert) | supported | 0.08 ms | **landed** — compiled to INSERT/UPDATE/DELETE per action, RLS enforced, atomicity |
| INSERT … SELECT (10k-row pipeline) | unsupported | 15.5 ms | **GAP** — honest SQLSTATE reject; materialized SELECT + INSERT workaround |
| **Write / concurrency — losses** | | | |
| Bulk UPDATE (~1/3 rows) | 9.4 s | 3.4 s | **2.8× slower** |
| Mixed read-write concurrency 8R+4W (600 ops) | 202 ms | 12 ms | **slower** — newly measured shape |
| Concurrent SELECT (16 sessions, mixed) | 19 ms | 2.3 ms | **slower** |
| Read-modify-write contention (8 sessions) | 8.4 ms | 4.7 ms | **slower** |

† Durability disclosure: Basin's default acks before fsync (≤200 ms bounded loss window) while the PG numbers are fsync-durable per commit. `SET basin.synchronous_commit = on` provides group-committed fsync durability — measured at ~2% added latency on the 10k bulk INSERT in the probe harness (group commit amortizes one fsync per statement group).

**The honest tally: across the ~100 ms-shapes on this card Basin is faster on 51, Postgres on 54.** Several OLTP fast paths measure at full speed on the 10k card (keyset 0.07ms, LIMIT-no-ORDER 0.06ms, ARRAY_AGG/DISTINCT-ON at-or-better than PG) but decline on the 1M card while hot-tier overlays from earlier card shapes are live — under investigation as a card-order interaction; the engine stays correctness-first by design.

At **1M rows on LocalFS the picture is read-win, point-write-loss**: Basin wins the shapes where columnar scans and pushdown dominate — LATERAL JOIN 462×, star join 261×, correlated subquery 113×, range scan 81×, large-result streaming 25× — **and now wins bulk INSERT outright at every scale** (2.08 s vs 8.10 s at 1M, with the durability footnote). It loses PG's home turf: **selective single-row lookups and point mutations** (point query, UPSERT, single-row UPDATE, JSONB `->>`) where PG's PK btree is µs-class and Basin is sub-ms-to-ms-class, plus **bulk UPDATE and read/write concurrency** (mixed 8R+4W 202 ms vs 12 ms — a newly measured shape and the largest open concurrency gap). **The structural wins (RAM/conn, idle cost) and the real-S3 card are the load-bearing comparison.** At **10k** the OLTP fast paths run at full speed (keyset 0.07 ms, point 0.06 ms, UPDATE 0.33 ms) and bulk INSERT already wins 3.5×; at **100k** Basin wins most analytical shapes and bulk INSERT 3.9×. **On real S3 (SeaweedFS, 100k rows) Basin wins outright** — 102× smaller on disk (99 KB vs 10.1 MB), point query 4.1× faster (4.6ms vs 18.5ms), 100k-row INSERT 20× faster (96ms vs 1.9s) — because object-storage economics and warm-cache reads beat PG's block-storage heap once data leaves a single local SSD; on the real-S3 scale-up card point-query p50 stays bounded as data grows (0.89 ms at 10k → 1.8 ms at 1M, 2.0× growth over 100× data). Full per-scale matrix:

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
- **Audit logs, event streams, IoT, activity feeds** — write-once-read-many. Vortex columnar compression shrinks bytes-at-rest 1.9× vs PG heap on the mixed 100k SaaS shape (users + events; roughly at parity on the latest 1M card), and far more on append-only event data: **102× smaller on real S3 at 100k** on the audit-log shape, and ~78× vs raw CSV. Object-storage $/GB compounds.
- **AI / RAG with mixed tabular + vector data** — native `vector(N)` + HNSW alongside transactional rows in the same database, no `pg_vector` install. Useful when "store the document + the embedding + the audit row in one place" is the requirement.
- **Cheap-idle multi-environment apps** — dev / staging / per-region / per-customer all live as cheap project prefixes on one cluster. Per-project cost is O(bytes), so 10k mostly-idle projects stays cheap. See [`docs/multi-project.md`](./docs/multi-project.md).

## Where Basin is NOT the answer (yet)

- **Drop-in OLTP replacement at 1M+ scale with microsecond point-mutation latency requirements.** Phase 5.14 closure (`bed431c`) flipped the hot-tier UPDATE/DELETE fast paths on by default, and three work waves of OLTP scale fixes cut single-row UPDATE at 1M to 1.24ms (vs PG 0.012ms) — a huge improvement over the prior ~162ms, and ms-class in absolute terms, but PG's index-and-heap is still structurally faster on point mutations. For most multi-tenant SaaS workloads this gap doesn't matter (point mutations are <1% of traffic); for hot-write OLTP front ends it might. The kill-switch `BASIN_HOTTIER_FASTPATH_DISABLE=1` rolls the fastpaths back without a redeploy if needed.
- **Index-heavy workloads on non-PK columns** — Phase 5.7.B1 secondary B-tree indexes shipped (`CREATE INDEX … USING btree`), so `WHERE non_pk_col = X` can now probe the index instead of full-file scanning. Bloom filters on `basin.sort_by` + per-file `column_stats` still prune at the file level for indexes that aren't declared. The GIN-on-tsvector (`@@`, Phase 5.20.E) and GIN-on-jsonb (`@>`) read paths are wired with file/row-group pruning; the remaining open perf work there is keeping the in-RAM posting lists warm across compaction and restart (a cold or incomplete registry degrades to a full scan — correct, just unpruned).
- **Pure analytical workhorse** — Snowflake / DuckDB / ClickHouse will out-run Basin on heavy GROUP BY / window / recursive-CTE shapes. Basin trades some bench wins for PG-compat, and inherits DataFusion's upstream limits on a handful of shapes (recursive CTE, exact COUNT(DISTINCT)).
- **High-frequency single-DB OLTP** → Postgres / Aurora / Neon. **Edge / local-first** → Turso / libSQL. **Geospatial primary store** → PostGIS. **Embedded SQLite-class** → SQLite. **Globally strongly-consistent multi-region writes** → Spanner / Cockroach.

---

## Batteries included

Basin is one binary that ships the pieces you'd otherwise wire up across
five vendors. The wedge isn't *"a cheaper Postgres"* — it's *"the same
stack, but every project is a bucket prefix and the marginal cost is
near zero."*

| Piece | Status | Honest caveat |
|---|---|---|
| **Auth** (signup, signin, magic-link, JWT, refresh, OAuth) | ✅ Shipped | Per-project schema; `auth.uid()` / `auth.role()` work in RLS policies. |
| **REST API** (PostgREST-compatible) | ✅ Shipped | `GET`/`POST`/`PATCH`/`DELETE` on `/rest/v1/<table>`; RPC mount for SQL + Wasm functions. |
| **Realtime** (SSE + WebSocket + presence) | ✅ Shipped, harness-gated | Implementation complete; some integration harness slices `#[ignore]`-gated pending un-gate. |
| **Blob storage** (`basin-blob`) | ✅ v1 | Catalog-backed object storage (ADR 0021). Full `/storage/v1/` REST surface in `basin-rest`: bucket CRUD, object upload/download/list/bulk-delete, public fast path, signed-URL mint + verify (HMAC-SHA256, expiry, key rotation), per-object RLS, and per-project byte-quota counters. HEAD / COPY / resumable multipart deferred to v1.1. See [`CAPABILITIES.md`](./CAPABILITIES.md). |
| **Vector search** (native `vector(N)`, HNSW, `<->`/`<#>`/`<=>`) | ✅ Shipped | Planner auto-routes `ORDER BY x <-> $1 LIMIT k`. No `pg_vector` install needed. |
| **WASM UDFs** (`CREATE FUNCTION … LANGUAGE wasm`) | ✅ v0.1 scalar | `i32`/`i64`/`f64` plus `text`/`bytea`/`timestamptz` args + returns (variable-length values cross a `basin_alloc`/`basin_dealloc` `(ptr,len)` ABI); JSONB rides the `text` path. Per-row invocation; a dedicated `jsonb` arg type and vectorized (whole-array) calls are deferred. |
| **Cron, HTTP-from-SQL, trgm, geo, continuous matviews** | ✅ via Basin-flavored crates | See ["Postgres-extension equivalents"](./CAPABILITIES.md#postgres-extension-equivalents). No `CREATE EXTENSION` required. |

**What we don't ship** — and on purpose:
- **Edge functions** (Cloudflare Workers / Supabase Edge Functions shape) — see [ADR 0019](./docs/decisions/0019-declarative-baas-surface.md): in-engine WASM UDFs plus declarative inbound webhooks + RPC mount solve the "compute close to data" need for Basin's wedge. Geographically distributed V8 isolates are a different concept and a maintenance burden the wedge doesn't justify.
- **Triggers / PL/pgSQL** — replaced by declarative lifecycle columns + SQL-bodied reactors + `LANGUAGE sql` functions per [ADR 0012](./docs/decisions/0012-change-event-primitive.md).
- **Postgres extensions (`.so`)** — see [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md).

Long-form companion with code samples per piece: [`docs/batteries.md`](./docs/batteries.md). Fine-grained matrix: [`CAPABILITIES.md`](./CAPABILITIES.md).

This is the *and-the-rest-of-the-stack-is-here* line. The structural primitive (project = bucket prefix, ~310 KiB/conn) is the wedge; this is what closes the sale.

---

## Postgres compatibility

| Surface | Status |
|---|---|
| **sqllogictest** (curated PG-style suite) | **100% (50/50)** as of [`b7114e8`](https://github.com/vul-os/basin/commit/b7114e8) |
| **ORM corpus** (Drizzle / Prisma / sqlx / Diesel / TypeORM, 99 representative shapes) | **100% (102/102)** on the 2026-06-11 integrity run — Drizzle 100%, sqlx 95%, Diesel 95%, TypeORM 94%, Prisma 90%; all 5 failures are typed errors, 0 regressions. `pg_index` / `pg_sequence` / `pg_enum` introspection now populated, so ORM startup/migration introspection (Prisma, Drizzle, SQLAlchemy, ActiveRecord, sqlx, Django, Hibernate) resolves cleanly. |
| **Live ORM suite** (Django, GORM, and the above, end-to-end wire) | **138/139** as of 2026-06-12 — 7 ORMs (Django savepoint-per-atomic, MigrationRecorder, contenttypes, FOR UPDATE; GORM pgx extended-protocol AutoMigrate, RETURNING, batch, upsert; Drizzle, Prisma, sqlx, Diesel, TypeORM). One remaining typed error: `ALTER TABLE … ADD CONSTRAINT UNIQUE`. ORM migration compatibility improvements landed 2026-06-14: **`DEFERRABLE INITIALLY DEFERRED` foreign keys** are now accepted and treated as deferred (child rows may precede parents in a migration transaction — the Django/Rails migration pattern); **`ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`** is now enforced (ORMs wire FKs in a follow-up ALTER after CREATE TABLE); Drizzle `VALUES (DEFAULT, …)`, SQLAlchemy `INSERT … SELECT` into integer identity PKs, `INSERT … SELECT … RETURNING`, Django `F()` UPDATE expressions (`SET col = "t"."col" + 1`), schema-qualified FK references, and gorm's extended-protocol `RowDescription` binary-format fix all landed as targeted SQL-shape fixes (`orm_sql_shapes.rs`). |
| **Per-fragment SQL matrix** ([`docs/sql-support.md`](./docs/sql-support.md), 975 fragments tested) | **~88.5% Default config / ~91.5% non-excluded** (863/975) |
| **Wasm UDFs** | `i32 / i64 / f64` plus `text / bytea / timestamptz` args + returns shipped (via a `basin_alloc`/`basin_dealloc` + linear-memory `(ptr,len)` ABI); JSONB rides the `text` path. Deferred: a dedicated `jsonb` arg type and vectorized (whole-Arrow-array) args |
| **Wire protocol** | pgwire v3, simple + extended query, TLS (rustls), `COPY FROM STDIN / TO STDOUT` (CSV + PG binary COPY format), prepared statements with binary parameters (native JSONB / UUID / BYTEA / ARRAY binary wire formats). NUMERIC rides the text wire format (lenient drivers handle it fine; binary varlena encoding deferred to v0.2). |
| **Differential PG-oracle harness** | [`tests/integration/tests/differential_pg.rs`](./tests/integration/tests/differential_pg.rs) — every release runs identical SQL against Basin and a real PostgreSQL; build fails on any cell-level divergence |

Per-statement breakdown with every red row linked to its planner / parser / executor owner: [`docs/sql-support.md`](./docs/sql-support.md). Public capability matrix: [`CAPABILITIES.md`](./CAPABILITIES.md).

**Intentionally out of scope** (per [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)): `CREATE TRIGGER`, `CREATE OPERATOR`, composite `CREATE TYPE`, multirange column types / `OID` / `REGCLASS` / `BIT` / `PG_LSN`. (Multirange *expressions* and operators work on literals — only the DDL column type is rejected.)

`LISTEN` / `NOTIFY` / `UNLISTEN` **are supported** — SQL-level pub/sub via a per-engine notify registry, with PG-accurate transaction buffering (`NOTIFY` inside a transaction is queued and fanned out only on `COMMIT`, discarded on `ROLLBACK`). See [`tests/integration/tests/listen_notify.rs`](./tests/integration/tests/listen_notify.rs).

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

That gives you pgwire on `127.0.0.1:5433`, durable WAL + Vortex columnar
files (the catalog default; Parquet is opt-in per-table) under
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

- **Postgres-compatible SQL** — pgwire v3, simple + extended query protocol, **TLS** (rustls), **`COPY FROM STDIN`/`COPY TO STDOUT`** (CSV + binary). Works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, any Postgres ORM. A curated ORM/driver-compat suite plus a PG-oracle differential harness (`differential_pg.rs`) gate every release against a real PostgreSQL.
- **CRUD + DDL** — `CREATE TABLE` (incl. `CREATE TABLE AS … WITH NO DATA`), multi-row `INSERT`, `SELECT`, `UPDATE`, `DELETE` (Iceberg copy-on-write today; hot-tier tombstone fast path env-gated in `87ef24b`), `ON CONFLICT DO NOTHING / DO UPDATE` upsert, `ALTER TABLE … CLUSTER BY (…) / SET BLOOM FILTERS ON / SET row_group_rows / SET cold_after / ENABLE ROW LEVEL SECURITY / CREATE POLICY`, `SHOW TABLES`. Prepared statements with parameter bind (text + binary, including native JSONB / UUID / ARRAY binary; NUMERIC binds over the text wire format — binary varlena deferred to v0.2).
- **Honest enforcement, not silent no-ops** — `CREATE UNIQUE INDEX` actually enforces uniqueness, `VARCHAR(n)`/`CHAR(n)` length is enforced, RLS `WITH CHECK` is enforced on write, `TABLESAMPLE` actually samples, advisory locks + deadlock detection are real, `EXCLUDE USING gist` (range overlap constraints) is parsed and enforced, and unsupported forms honest-reject with a SQLSTATE instead of silently doing nothing. SQLSTATE codes are surfaced accurately across all pgwire clients: unknown function → **42883** (`UndefinedFunction`), unknown column → **42703** (`UndefinedColumn`), missing relation → 42P01 — so psycopg/asyncpg/pgx raise their dedicated typed exceptions rather than a generic internal error. A wave of silent-corruption CRITICALs surfaced by the differential harness were fixed.
- **Expanded SQL surface** — JSONPath (`jsonb_path_query`, `@?`, `@@`, `jsonb_path_query_array`); JSONB mutators (`jsonb_set`/`insert`/`strip_nulls`/`pretty`/`typeof`); `json_build_object`/`json_build_array`; INET/CIDR containment; `regexp_match`/`matches`/`split_to_array`/`split_to_table`, `format`, `encode`/`decode`; datetime `age`/`to_char`/`to_date`/`date_bin`; window `IGNORE NULLS`; `SAVEPOINT` / `ROLLBACK TO`; data-modifying CTEs; correlated + `LATERAL` joins (incl. `CROSS JOIN LATERAL generate_series`); `MERGE INTO` (matched/not-matched/DO NOTHING actions, RLS enforced, atomicity, VALUES + SELECT-subquery sources); `DECLARE`/`FETCH`/`MOVE`/`CLOSE` cursors (session-scoped, Django psycopg2 server-side cursor shape); composite `ON CONFLICT (a, b)` targets against composite PK or multi-column UNIQUE; full-text search (`tsvector`/`tsquery`/`@@`, `plainto_tsquery`/`phraseto_tsquery`/`websearch_to_tsquery`, `ts_rank` TF-weighted + `ts_rank_cd`, `ts_headline` fragments, `setweight` A–D classes); ordered-set aggregates (`percentile_disc`, `mode() WITHIN GROUP`); range/multirange arithmetic; schema namespaces (`CREATE SCHEMA`, cross-schema queries); real transaction semantics (deferred commits, `ROLLBACK` undo, SAVEPOINT stack, aborted state).
- **Snapshot-isolated transactions + HTAP fast-path DML** — explicit transactions take snapshot-stable reads (an open txn sees a consistent table view across statements even as other sessions commit; hot-tier MVCC sequence prevents overlay writes from leaking into open transactions). In-transaction single-row DML routes through a transaction-scoped overlay fast path, and untouched-table in-tx reads are served from a pinned snapshot without re-planning through DataFusion. RMW `SET col = <expr>` has a fast path; single-key UPDATE prunes its cold pre-image read.
- **Prepared-statement AST execution** — bound statements execute without re-parsing on every call; parameter types are inferred through UDF argument positions for clients that don't pre-declare them.
- **Auto-index advisor** — the engine observes query shapes and recommends/auto-builds secondary B-tree indexes for repeated point/IN-list probes; sorted-PK skip handles IN-lists in O(log n) zone-prune.
- **Time travel** — Iceberg-style snapshots. `Catalog::rollback_to_snapshot(project, table, snapshot_id)` rewinds; `Catalog::fork_table(project, src, dst)` clones a table's metadata + snapshot history into a new sibling that diverges on next commit. Zero data copy until divergence.
- **Native vector search** — `vector(N)` + `<->` / `<#>` / `<=>` operators, HNSW per file segment plus a native **IVFFlat** coarse quantiser (`CREATE INDEX … USING ivfflat … WITH (lists = N)`, k-means cells + `ivfflat.probes` nearest-cell scan). `CREATE INDEX … USING hnsw WITH (m = …, ef_construction = …)` accepts pgvector-style build params, and the planner routes `ORDER BY x <-> $1 LIMIT k` to the index whose opclass (`vector_l2_ops` / `vector_cosine_ops` / `vector_ip_ops`) matches the query's distance operator. **HNSW recall@10 = 1.0** on the 128-dim / 100k benchmark (ef_search default raised to 400; p50 38 → 42 ms; measured by `ext_bench_vector`). Also: `vector_avg()` element-wise mean aggregate and f32-backed `halfvec(N)`. No `pg_vector`. `sparsevec` is a typed `0A000` (no silent coercion).
- **Postgres-extension equivalents** — `pg_cron` (basin-cron), `pg_net` + `http` (basin-net), `pg_trgm` (basin-trgm; `similarity`/`word_similarity`/`%`/`<%`/`<->` SQL operators + GIN trigram index backed by an in-RAM posting list), `PostGIS` subset (basin-geo: POINT + full general 2-D geometry — `LINESTRING`/`POLYGON`/`MULTI*`/`GEOMETRYCOLLECTION` WKB/EWKB/WKT/GeoJSON codecs + `ST_Length`/`ST_Area`/`ST_Centroid`/`ST_GeometryN` + exact `ST_Contains`/`ST_Intersects`/`ST_Within` + R-tree candidates for `&&`/`ST_DWithin` on POINT columns), `TimescaleDB` (basin-cv: `CREATE_HYPERTABLE`, `time_bucket` all forms, `first`/`last` aggregates, `drop_chunks`, `add_retention_policy`, `time_bucket_gapfill`/`locf`, continuous aggregates with incremental refresh), full-text search (`tsvector`/`tsquery`/GIN-on-tsvector with posting-list pruning), JSONB GIN index (`@>` posting-list pruning), `pgcrypto` + `uuid-ossp` UDFs.
- **Auth + REST in the OSS bundle** — basin-auth (signup, JWT, refresh-token rotation, email-link login, per-project API keys) + basin-rest (PostgREST-shape CRUD, cursor pagination + NDJSON streaming, OpenAPI 3.0 schema generation at `GET /rest/v1/_openapi.json`). **`auth.uid()`**, **`auth.role()`**, **`auth.jwt()`** SQL session functions let you write Supabase-style RLS policies.
- **Per-project connection URLs** — `POST /admin/v1/projects` returns `postgres://<user>:<password>@host:5433/<db>`. Password bcrypt-validated on every pgwire startup; mismatch → SQLSTATE `28P01`. Rotate via `POST /admin/v1/projects/{user}/rotate`.
- **Durable catalog** — Iceberg-style catalog backed by Postgres when `BASIN_CATALOG=postgres://...`; tables, snapshots, project credentials, and `basin-auth`'s identity tables survive process restart.
- **Cheap retention** — Vortex (default, ~1.95× smaller than ZSTD Parquet on audit-log) or Parquet, **1.9× smaller than Postgres heap on the mixed 100k SaaS+OLAP shape (parity on the latest 1M card) and 102× smaller on real S3 at 100k on the append-only audit-log shape**; per-file catalog `column_stats` + per-file bloom filters on `basin.sort_by` columns skip footer fetches and file opens when the predicate prunes the file.
- **Analytical path** — a single DataFusion engine with Vortex/Parquet projection + predicate pushdown, catalog-statistics file pruning, per-file blooms, and incremental continuous materialized views. Approximate-cardinality and approximate-quantile UDFs (`APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`) sit alongside exact counterparts for dashboard workloads. Heavy scans use stateless pooled compute over shared object storage.
- **Multi-schema isolation (phase A)** — `SchemaName` / `QualifiedTableName` types, a schema-aware in-memory *and* Postgres-backed catalog, a `basin_schemas` table, and `CREATE/DROP SCHEMA` + cross-schema queries with differential coverage. Phases B–E (full name resolution / search_path semantics / wider DDL) are still in progress — see Status.
- **Multi-node seams** — `BASIN_LEASE_MODE=required` (write-fence via catalog CAS + shard heartbeat; fencing tests pinned), `BASIN_WAL_MODE=raft` (openraft 0.9, disk-persistent log/vote/snapshot, manifest-anchored snapshots, quorum-ack durability, `raft-net` feature for cross-process gRPC transport via tonic). Multi-region routing: `home_region` per project, `WrongRegion` typed error, `basin.read_tier` GUC for lagging reads, `WriteForwarder` seam. Connection ceiling per tier (`BASIN_MAX_CONNECTIONS` / admin API, enforced at pgwire startup via SQLSTATE 53300).
- **CDC** — durable change-data-capture ring (`basin-cdc`): post-commit sink captures every committed mutation (including HTAP hot-tier UPDATE/DELETE fast paths) into a per-project append-only ring on object storage; SSE stream with resumable `seq` cursor and retention window (`BASIN_CDC_RETENTION_HOURS`). Webhook fanout (`basin-webhooks`): disk-backed retry queue + exponential backoff + dead-letter + auto-pause. Kafka/Redpanda push sink (`basin-cdc::kafka`). CDC integration test harness (`cdc_pgoutput_harness.rs`) gates future protocol slices.
- **10 client SDKs** (each a standalone repo alongside basin) — `basin-js` (TypeScript, Node/Bun/Deno/CF Workers, realtime + Arrow IPC), `basin-py` (async + sync, realtime WebSocket + Arrow IPC via pyarrow), `basin-go` (Arrow IPC), `basin-java`, `basin-rust`, `basin-ruby`, `basin-dotnet`, `basin-php`, `basin-dart`, `basin-swift`. All engine-direct (pgwire + REST, no cloud intermediary required).
- **Operations** — connection pooling, per-project pgwire rate limiting (token-bucket via `governor`), cost-based query rejection (`BASIN_QUERY_COST_LIMIT_ROWS`), per-project counters (ops / bytes_read / bytes_written / errors / p99), OpenTelemetry traces wired through router → engine → shard → storage → WAL.
- **Scale benchmarks** — fleet-scale ladder (`multi_project_fleet_scale.rs`) gates isolation at 50/500/5000 projects; 1B-row write soak; noisy-neighbor fairness harness (`noisy_neighbor_harness.rs`); `ext_bench_*` suites (vector, PostGIS, trgm, Timescale, FTS, ranges) generate `benchmark/data/*.json` sidecars for the dashboard.

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
| **4.5** | PostgreSQL SQL-compatibility push — silent-corruption CRITICAL fixes, JSONPath / JSONB-mutating / INET-CIDR / regexp / datetime function families, ARRAY binary wire format (NUMERIC stays text-wire pending v0.2 varlena binary), PG-oracle differential harness (`differential_pg.rs`) | **shipped** — Default config at ~88.5% / ~91.5% non-excluded (863/975); long-tail exotic-DDL parser gaps remain v0.2 |
| **5** | Analytical path — single DataFusion engine, Vortex/Parquet pushdown + per-file bloom + catalog pruning, continuous pre-aggregation, `APPROX_COUNT_DISTINCT`/`APPROX_PERCENTILE` UDFs | **v0.1 shipped** |
| **5.0a** | Vortex storage format — ~1.95× smaller than ZSTD Parquet; `aggregate_full` ~15–40× via catalog-stats metadata path; per-file blooms flip `point_eq` from a loss to a win at every scale | **shipped as the DEFAULT** ([ADR 0015](./docs/decisions/0015-vortex-storage-format.md)), zero-regression vs Parquet baseline. Parquet first-class per-table via `WITH (basin.file_format='parquet')`. HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) is Phase 5.14.C — closes the residual OLTP point-read and the UPDATE/DELETE write floor. |
| **5.14** | Durable Basin moat — per-file catalog blooms (shipped), `APPROX_COUNT_DISTINCT` + `APPROX_PERCENTILE` UDFs (shipped), catalog-aware `WindowExec` sort-elision (shipped), **HTAP hot tier on by default (`bed431c`)** — DELETE + UPDATE fast paths default-ON, kill-switch `BASIN_HOTTIER_FASTPATH_DISABLE=1`, merge-on-read via TombstoneFilterExec + UpdateOverlayExec wired in both DataFusion (`HtapUnionTable::scan`) and fast_select paths, gate-matrix locked by 16 tests, C6 differential harness extended with Mode D fastpath-on, TxCommit WAL marker (ADR 0020 §6) emitted explicitly with backward-compat replay. The 3-month investment that is **not** subsumed by upstream Vortex / DataFusion improvements. | **shipped** |
| **5.15** | Unified docs platform — OSS-repo markdown with YAML frontmatter ([spec](./docs/frontmatter-spec.md)) that a downstream docs site can fetch at build time | **shipped** (5.15.A/B/C, frontmatter spec + 24-doc migration + top-level index + CI gate). The docs *site* that consumes this feed is not part of this repo and is out of scope here |
| **5.5** | Sharding axes — partitioning, compute sharding, tiered storage | **shipped** |
| **5.6** | RLS with `CREATE POLICY` (UNION / CTE coverage) | **shipped** |
| **5.7** | Caches + bloom + A4 catalog stats + B1 secondary B-tree indexes + B2 cluster-by + B3 row-group sizing | **shipped**; the per-row-group GIN-on-tsvector (`@@`, 5.20.E) and GIN-on-jsonb (`@>`) read-path wiring has since shipped too |
| **5.8** | `pg_cron` + `pg_net` SQL surfaces | **shipped** |
| **5.9** | Postgres-extension equivalents (basin-geo / -trgm / -cv, JSONB, UUID, pgcrypto) | **shipped** |
| **5.10** | Identity + REST (basin-auth, basin-rest, OpenAPI, pagination, streaming, API keys, refresh rotation, per-project connection URLs, **`auth.uid()` / `auth.role()` / `auth.jwt()`** session functions) | **shipped** |
| **5.11** | Multi-schema isolation | **phase A shipped** — `SchemaName`/`QualifiedTableName` types, schema-aware in-memory + Postgres catalog, `basin_schemas` table, `CREATE/DROP SCHEMA` + cross-schema queries with differential coverage. **Phases B–E in progress** — full qualified-name resolution, `search_path` semantics, wider schema-scoped DDL |
| **5.20–5.30** | Extensions sprint — FTS, ranges, trgm SQL surface + GIN index, Timescale completions, advisory locks + deadlock detection, CDC phases 1–3, multi-node (lease enforcement + raft WAL), schema namespaces, cursors, MERGE INTO, composite ON CONFLICT, 10 SDKs | **shipped** — see CHANGELOG for per-feature detail |
| **6** | Production hardening | **partial** — telemetry / pooling / rate-limit / cost-rejection / catalog-PITR / fork shipped; `BASIN_LEASE_MODE=required` write-fence + `BASIN_WAL_MODE=raft` quorum WAL shipped; multi-region routing seams (home_region, WrongRegion, WriteForwarder) shipped; per-tier connection ceiling shipped; cross-shard 2PC (ADR 0011) locked architecturally and gated on customer demand |
| **6.x** | SQL long-tail (still pending) | **planned** — `search_path` semantics, server-side `PREPARE/EXECUTE` over text protocol edge cases, `DO` blocks, exotic types (`BIT` / `OID` / `REGCLASS`), array column DDL types, `sparsevec`, 3-D geometry, real multi-machine raft deployment docs |
| **7** | Launch | gated on Phase 0 |

Six-month wedge slice: [`WEDGE.md`](./WEDGE.md). Full plan: [`TASK.md`](./TASK.md). Decision log: [`docs/decisions/`](./docs/decisions/).

---

## How Basin compares

### vs Postgres / Aurora / RDS

Postgres is the right answer for **single-project, high-frequency OLTP** and for workloads that need **microsecond point-mutation latency at 1M+ rows**. Basin's hot-tier UPDATE/DELETE fast paths are on by default as of Phase 5.14 closure, and three work waves of OLTP scale fixes cut point UPDATE at 1M to 1.24ms (vs PG 0.012ms) — far better than the prior ~162ms, but PG's index-and-heap is still structurally faster on that shape. **Basin is not trying to be Postgres on those shapes.** Where Basin wins: many-isolated-projects (per-environment / per-customer / per-region), append-shaped workloads where the columnar bytes-at-rest savings compound (102× smaller on real S3 at 100k), bulk ingest (bulk INSERT 3.9× faster at 1M, with the durability disclosure), the columnar-scan analytical shapes (113× faster correlated subquery, 462× LATERAL JOIN, 81× range scan at 1M), and the structural RAM-per-connection economics for connection-heavy front ends.

### vs Neon

Neon is serverless Postgres with branching — terrific for **single-DB workloads** that want copy-on-write forks. Basin matches the branching story (Iceberg forks are zero-copy too) but stores data on plain S3 rather than a managed page server. Neon's per-project minimum scales with project count (O(provisioned pool)); Basin's per-project cost is O(bytes), so many isolated projects stay cheap.

### vs Supabase

Supabase is "BaaS in a box" — Postgres + Auth + Edge Functions + Storage + Realtime. Basin covers the SQL + Auth + REST surface in one binary, with `auth.uid()` / `auth.role()` / `auth.jwt()` working identically. Where Basin differs is the data-layer economics: Vortex/Parquet on S3 instead of Postgres heap on block storage. Multi-project SaaS that has outgrown Supabase's per-project pricing can migrate the database to Basin via pgwire and keep Supabase Auth / Edge Functions / Realtime for the parts of the stack they handle well. Edge Functions / Realtime / Storage are out of scope per ADRs 0005/0006.

### vs Nile

Nile is "Postgres for multi-tenant SaaS" — same problem space as Basin, but built directly on real PostgreSQL with per-tenant virtual databases. That choice gives Nile real PG semantics, real OLTP, real JSONB, real extensions, and real PL/pgSQL for free, which is exactly where Basin still trails today. Basin's structural answer is the substrate economics: Nile's per-tenant cost is bounded by the underlying PG instance's per-tenant cost (heap pages, connection slots, autovacuum overhead); Basin's per-tenant cost is O(bytes-on-S3) with shared compute, so cold or low-traffic tenants stay near-zero. The right card to read is **cost per tenant per month at p99 latency, at scale** — single-instance PG-based multi-tenancy gets expensive at 10k+ tenants in a way object-storage-native multi-tenancy doesn't. If your workload is point-mutation-heavy and JSONB-heavy and you have <1k tenants, Nile is probably the easier answer today. If you have many idle or low-traffic tenants, append-shaped data, or want columnar bytes-at-rest economics, Basin is the cheaper substrate.

### vs Turso / libSQL

Turso is the right answer for **edge-distributed apps** with many tiny SQLite-class databases. Basin is for centralized apps that want **Postgres SQL on cheap object storage** with a real wire-protocol surface that ORMs already speak.

### vs ClickHouse / DuckDB / data-warehouse

ClickHouse and DuckDB are analytical engines — phenomenal at OLAP scans, not designed for transactional point reads or per-row inserts. **Basin will lose to a dedicated warehouse on heavy GROUP BY / window / recursive-CTE shapes**, and inherits DataFusion's upstream limits on a handful of shapes (recursive CTE, exact COUNT(DISTINCT)). Basin's pitch is the unified path: one engine, one binary, pgwire on the wire, columnar substrate underneath — useful when "Snowflake plus a Postgres" is overkill for the workload size. The HTAP hot-tier ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) closes the OLTP point-read and point-write floor on TB-scale tables — the missing piece that lets one engine span OLAP and OLTP without a second system.

### Where Basin is *not* the answer

Per the ADRs:

- **Single-project high-frequency OLTP** → Postgres / Aurora / Neon / Nile
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

Basin (this repo) is the data plane, and since the SDKs and the CLI were
merged in it is also the monorepo for everything that talks to it:

- **[`cli/`](./cli)** — operator daily-driver (Rust, Apache-2.0). `basin login`, `basin projects list`, `basin sql run`, release artefacts Sigstore-signed. Talks to a control-plane `/v1/*` API endpoint. Includes the `basin dev` local-stack launcher. Builds independently of the engine workspace.
- **[`sdks/`](./sdks)** — ten engine-direct client SDKs (MIT), all speaking pgwire + REST straight to a Basin engine, with no control plane on the data path: [`js`](./sdks/js) (browser, Node, Deno, Bun, Workers; realtime + Arrow IPC), [`py`](./sdks/py) (async + sync, realtime + Arrow via pyarrow), [`go`](./sdks/go) (Arrow IPC), [`java`](./sdks/java), [`rust`](./sdks/rust), [`ruby`](./sdks/ruby), [`dotnet`](./sdks/dotnet), [`php`](./sdks/php), [`dart`](./sdks/dart), [`swift`](./sdks/swift).

**Licensing rationale.** Server-side projects (the Basin engine and the CLI) are Apache-2.0 to carry the patent grant operators expect from infrastructure. All ten client SDKs are MIT to match the norm of the SDK ecosystems they sit in.

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

## Verify a release before you run it

Every tagged release publishes a `SHA256SUMS` manifest covering **every**
asset, plus a sigstore build-provenance attestation minted from the release
workflow's OIDC identity (no long-lived signing key exists, and none should).
`scripts/verify.sh` is what you run before executing bytes you pulled off the
internet:

```sh
curl -fsSLO https://raw.githubusercontent.com/vul-os/basin/v0.1.9/scripts/verify.sh
bash verify.sh --tag v0.1.9 --attest basin-0.1.9-x86_64-unknown-linux-gnu.tar.gz
```

It fetches the manifest, looks up the **exact** entry for the asset you named,
downloads it and compares digests. It fails closed: a missing or HTML-substituted
manifest, a manifest with no entry for your asset, a truncated download and a
digest mismatch each exit non-zero with their own diagnostic. There is no
`--skip-verify`. `--attest` additionally checks the build provenance (needs the
`gh` CLI); without it the script prints that provenance was **not** checked
rather than implying it was.

`bash scripts/verify.sh --selftest` runs the 24-case refusal matrix against a
synthetic broken origin — CI runs it on every push, so a verifier that has
quietly stopped refusing shows up as a red build rather than as a green one.

The CLI's own release (`cli-v*` tags) is additionally cosign-signed keyless;
see [`.github/workflows/cli-release.yml`](./.github/workflows/cli-release.yml)
for the `cosign verify-blob` invocation.

---

## Build and test

Minimum supported Rust version is **1.92** — not a preference, but the floor
the resolved dependency graph imposes (cranelift 0.131 via wasmtime declares
1.92; vortex-error 0.71 declares 1.91), and `cargo` hard-errors below it. CI's
`msrv floor` job recomputes that from `cargo metadata` and fails if
`rust-version` drifts below it.

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

## Brand

The mark in [`brand/`](brand/) is the source of truth. Every icon this repo
ships — favicon, PWA and app icons, the mark in the README and on the site — is
rendered from `brand/logo.svg` rather than redrawn, so there is one approved
drawing and no second copy to drift.

Copy it outward, never edit a derived copy, and never edit `brand/` to match
something downstream.

## License

**Apache-2.0** — see [`LICENSE`](./LICENSE).

Contributions welcome. The project is opinionated about scope ([`docs/decisions/`](./docs/decisions/)) — open an issue before writing a PR that adds new surface area. The OSS code is the database; commercial cloud orchestration lives in a separate private repo and never affects what OSS users get.

---

## Part of VulOS

Basin is the **database layer** of [VulOS](https://vulos.org) — the open,
self-hostable web OS and app suite. Apps running on the Vulos OS get a
Postgres-compatible store whose per-project cost is O(bytes active) rather
than O(projects provisioned), which is what makes hosting many small
self-hosted apps on one box affordable.

**Basin runs standalone.** It is a single binary speaking pgwire against any
S3-compatible bucket, and it requires nothing else in the suite — no Vulos
OS, no control plane, no account. Everything in this README's Quickstart
works on a bare machine with no VulOS component installed. Being an app the
OS can host is an option, never a dependency.

For the rest of the suite and how the pieces fit, see
[vulos.org](https://vulos.org).

---

## Keywords for search

Basin is a **Postgres-compatible, bucket-native, multi-tenant database on object storage**, with **Vortex** (default columnar, LFAI) and **Apache Parquet** (opt-in, interchange) storage, an **Apache Iceberg** catalog, a file-backed WAL with a Raft WAL simulation toward distributed v0.2, **native vector search** (HNSW), per-file **catalog bloom filters** for point-query pruning, **HTAP hot tier** shipped on by default ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)), and **pgwire** protocol support that works with `psql`, `tokio-postgres`, `asyncpg`, JDBC, Diesel, SeaORM, and any other Postgres driver. Basin compares to **Postgres**, **Neon**, **Supabase**, **Nile**, **Turso**, **PlanetScale**, **Aurora**, **ClickHouse**, **SingleStore**, **DuckDB**, and **CockroachDB** for cheap-storage SaaS, audit-log, RAG / vector, HTAP, and multi-project use cases. Self-hostable, **Apache-2.0** licensed, written in **Rust**.
