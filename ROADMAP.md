# Roadmap

**Basin is pre-alpha.** There has been no v0.1 release. The tags that exist
(`v0.1.1` … `v0.1.10`) are development snapshots, not a stable line — the
pre-1.0 contract is that minor versions can break the public API and patch
versions are bug-fix only. Use Basin today to evaluate the cost economics,
to prototype multi-tenant patterns, or to contribute; do not put a business
on it yet.

This file answers three questions: what works now, what is being worked on,
and what we have decided not to build. It is deliberately short. The
detailed sources it is drawn from are:

- [`CAPABILITIES.md`](./CAPABILITIES.md) — the per-feature matrix, with a
  status on every row and an honest list of parity gaps
- [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md) — the v0.1 cut-off: what must
  be green before the cut, what is parked, and the trigger that unparks it
- [`docs/TASK.md`](./docs/TASK.md) — the full phase-by-phase build plan
- [`docs/decisions/`](./docs/decisions/) — one ADR per load-bearing "no",
  each recording the trigger that would change our mind
- [`CHANGELOG.md`](./CHANGELOG.md) — what actually shipped, and when

---

## What works today

The engine is mature; the product is not. Enough works that Basin runs real
multi-tenant workloads end to end:

- **pgwire v3**, simple and extended query, TLS, `COPY FROM STDIN` /
  `COPY TO STDOUT`, binary JSONB / UUID / NUMERIC / ARRAY wire formats.
  Ordinary Postgres drivers connect without a shim.
- **Storage on any S3-compatible bucket.** Vortex is the default file format
  ([ADR 0015](./docs/decisions/0015-vortex-storage-format.md)); Parquet is
  first-class per-table. A project is a bucket prefix, so per-project cost is
  O(bytes), not O(projects provisioned).
- **One engine for OLTP and OLAP** — DataFusion, plus the HTAP hot tier
  ([ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md)) whose
  UPDATE/DELETE fast paths are on by default, per-file catalog blooms and
  column stats, and the metadata-only aggregate path.
- **Real single-shard transactions** — `BEGIN` / `COMMIT` / `ROLLBACK` and a
  `SAVEPOINT` stack, with deferred commits and aborted-state semantics.
- **Multi-project isolation** — bucket-prefix separation, RLS via
  `CREATE POLICY`, per-project EDF fairness
  ([ADR 0008](./docs/decisions/0008-noisy-neighbor-fairness.md)), per-project
  connection ceilings and rate limits.
- **Multi-node ingest** — durable object-store catalog with leases and
  partition routing ([ADR 0023](./docs/decisions/0023-leases-and-partition-routing.md)),
  transparent per-partition write forwarding, quorum WAL under
  `BASIN_WAL_MODE=raft`.
- **The batteries** — `basin-auth` (identity), `basin-rest` (a PostgREST
  equivalent), `basin-pool`, `basin-realtime` (SSE + WebSocket + presence),
  `basin-blob` (object storage), native vector search with HNSW, CDC, Wasm
  UDFs, and native equivalents for `pg_cron`, `pg_net`, `pg_trgm`, `pgcrypto`
  and TimescaleDB continuous aggregates — no `.so` loading
  ([ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)).
- **Ten client SDKs**, a CLI, and a 5-minute Docker quickstart.

Per-feature status, including everything that is partial, is in
[`CAPABILITIES.md`](./CAPABILITIES.md). Per-shape performance, including the
shapes where Basin loses to Postgres, is in
[`benchmark/BENCHMARKS.md`](./benchmark/BENCHMARKS.md).

---

## Being worked on — the v0.1 cut

These are the items [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md) lists as
required-and-not-yet-green. Nothing gets called v0.1 until they are.

| Item | Where it stands |
|---|---|
| **WAL records for hot-tier DELETE and UPDATE** | The fast paths are default-on, but `MutationKind::Delete` / `Update` are not yet written to the WAL and replayed, so tombstones are lost if the engine restarts before the next compaction. This is a crash-safety gap, and it is the most load-bearing item on this list. |
| **Real-cloud read-shape parity** | The LocalFS cards are green. The real-S3 cards (cold start, point query, range scan, bulk LIST/DELETE) need a sweep before the cut; today's gaps are documented as workload caveats rather than measured-and-closed. |
| **ORM corpus ≥ 95% per ORM** | Drizzle, Prisma, sqlx, Diesel and TypeORM each have a recorded corpus plus a live harness under [`testing/orm-live/`](./testing/orm-live/). Prisma's `json_agg` rewrite is the largest remaining gap. |
| **sqllogictest PG-port slice** | The curated set is at 100%; the wider PostgreSQL-port slice still has to be confirmed. |
| **Dogfooding the catalog** | Basin's own control-plane catalog runs on Neon, not on Basin. It is the highest-visibility credibility gap in the project and it is a v0.1 gate. |
| **Wasm UDF type surface** | Scalar numerics plus `text` / `bytea` / `timestamptz` have landed. Still open: a first-class `jsonb` argument type, and vectorised whole-array invocation — execution is per-row today. |

Two things that are *not* on this list, and are honest about why: **Phase 0
customer interviews** are the real gate on everything (the architecture is
far ahead of the demand signal), and **CI health** — see the CHANGELOG's
unreleased section, which names the gates that were passing while verifying
nothing and the ones that are still red.

---

## After v0.1 — likely, not promised

Documented ideas with an obvious pull, listed so they are not lost. None of
these is a commitment.

- B-tree indexes on non-PK columns beyond the shipped foundation, and a
  trigram/inverted index to close `ILIKE '%suffix'`.
- A binary JSONB format. `serde_json::from_slice` per row dominates the
  JSONB extract path; this is why Postgres wins that shape by 5–10×.
- Scan-side TopK pushdown, which closes the 5–19× pagination gap. Blocked
  upstream in Vortex.
- Cross-region read replicas
  ([ADR 0004](./docs/decisions/0004-multi-region-read-replicas.md)) and
  cross-shard JOIN at the router layer.
- Physical GC for files orphaned by a rollback, and cross-DML rollback.
- The Wasm Component Model migration.
- `pg_dump` ingest. Export already works.

---

## Explicitly out of scope

Each of these is an ADR, not an omission — the reasoning and the trigger
that would reopen it are written down.

| Not building | Use instead | Why |
|---|---|---|
| Loading upstream Postgres `.so` extensions | Basin's native equivalents | [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md) |
| `CREATE TRIGGER`, PL/pgSQL, PL/Python, PL/Perl | Declarative lifecycle columns, SQL-bodied reactors, `LANGUAGE sql` functions, `CALL` procedures — ~95% of the surface | [ADR 0012](./docs/decisions/0012-change-event-primitive.md) |
| Globally consistent cross-region writes, cross-region 2PC | Spanner, CockroachDB | [ADR 0001](./docs/decisions/0001-single-region-only.md), [ADR 0011](./docs/decisions/0011-cross-shard-2pc.md) |
| Full PostGIS — constructive ops, `GEOMETRY` DDL beyond `POINT` | Real PostGIS, or a sidecar Postgres | `basin-geo` ships 2-D codecs, measures and exact predicates only |
| `pg_vector` wire compatibility, `sparsevec`, vector-`bit` | Basin's native `vector(N)` + HNSW, or a dedicated vector DB | [ADR 0003](./docs/decisions/0003-native-vector-search.md) |
| GRANT / REVOKE / a full role model | RLS policies, which are the v0.1 isolation primitive | Post-v0.1, no ADR yet |
| `ALTER TABLE … RENAME`, composite types, `GENERATED … VIRTUAL`, `AS OF` time travel | — | Rejected at parse time with SQLSTATE `0A000`; catalogued under "Documented exclusions" in [`CAPABILITIES.md`](./CAPABILITIES.md) |

Several crates are **frozen** rather than dropped — `basin-geo`,
`basin-trgm`, `basin-cron`, `basin-net`, `basin-webhooks`,
`basin-autoscale`, and `basin-fn` beyond its current type surface. They
work, they are documented, and they get no new investment. The single
condition that unfreezes any of them is in
[`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md#acceptance-criteria-for-unparking):
a named customer asks for it *and* has a contract pending. The bar is
deliberately high — scope discipline is the hardest part of this project,
and the ADRs are the artifact that makes "no" stick.

Basin's wedge is multi-project SaaS with audit-log-shaped workloads, where
storage cost and per-project isolation dominate. If your shape does not
match that, the recommendations above are meant sincerely.

---

## Build phases

The phase numbering used throughout the codebase, the CHANGELOG and
[`docs/TASK.md`](./docs/TASK.md).

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

---

## How this file is maintained

A roadmap that drifts is worse than none, so this one holds no facts of its
own. Every claim above is sourced from a document that is maintained as part
of the work:

- an item moves out of **Being worked on** when its box is ticked in
  [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md), which requires a commit SHA
  cited on the same line — cite-on-close is the contract there;
- an item moves into **Explicitly out of scope** only with an ADR;
- an item leaves it only via the unparking trigger.

If this file and one of those disagrees, the other one is right, and this
one is a bug.
