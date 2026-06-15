# Basin v0.1 — Scope Cut-Off

**Status:** Pre-alpha. Target release: TBD. This doc is the source of truth
for what ships in v0.1 vs what's parked. The companion docs are
[`README.md`](../README.md) (positioning), [`CAPABILITIES.md`](../CAPABILITIES.md)
(feature matrix), [`WEDGE.md`](../WEDGE.md) (wedge-deepening roadmap),
[`benchmark/BENCHMARKS.md`](../benchmark/BENCHMARKS.md) (full numbers), and
[`decisions.md`](../decisions.md) (wave-by-wave engineering log).

> Audience: maintainers, contributors, and review agents. Anyone proposing
> new work should be able to map the work to one bullet under "Required for
> v0.1" — otherwise the work is post-v0.1 and needs a wedge-customer trigger.

---

## What ships in v0.1 (THE WEDGE)

Basin v0.1 is a **cheap multi-project Postgres-compatible store on object
storage** optimized for the read-dominant SaaS audit-log / event-stream /
multi-project analytics workload. Single-region, single-shard transactions,
honest perf, honest gaps.

The structural moats are:
- **~12-24× smaller on disk** than Postgres (Vortex / BtrBlocks cascade on S3)
- **~47-63× less RAM per held-open connection** (from-scratch tokio server,
  not a forking daemon)
- **Per-project cost is O(bytes)** — a new project is a bucket prefix, not a
  VM, not a Postgres DB, not a per-DB pricing minimum
- **One engine for OLTP point reads and OLAP scans** (DataFusion + HTAP hot
  tier + per-file catalog blooms / stats)

If a feature is not on the path to making one of those four moats real to a
paying wedge customer, it waits for v0.2+.

---

## Required for v0.1 (must be GREEN before cut)

Each item links to the canonical tracking doc; "(shipped: <sha>)" cites
the commit when the item is closed. Cite-on-close is the contract — items
without a commit hash are not yet done.

### Engine / transactions
- [x] Single-shard transactions: `BEGIN` / `COMMIT` / `ROLLBACK` + `SAVEPOINT`
      (shipped: `f4127e9` real tx semantics — defer-commits-in-tx, ROLLBACK
      undo, SAVEPOINT stack, aborted state; `92aa0d0` SAVEPOINT rollback
      end-to-end). Cross-shard transactions are PARKED.
- [x] DELETE hot-tier route (env-gated, default OFF) — closes the
      single-row / bulk DELETE-WHERE-IN cliff (shipped: `87ef24b`,
      microbench 46.97 ms → 1.31 ms = 35.8× speedup).
- [x] DELETE hot-tier route **always-on** — SHIPPED (2026-05-25).
      `hottier_fastpath_enabled` defaults ON (the `BASIN_HOTTIER_*` env
      vars are kill-switches now); read-path tombstone suppression landed
      (`TombstoneFilterExec` + `HtapUnionTable::scan` merge-on-read, and
      the filter-pushdown passthrough so selective reads still prune).
- [x] UPDATE hot-tier route — SHIPPED (default-on via the same
      `hottier_fastpath_enabled` gate; UPDATE overlay merge-on-read).
- [ ] WAL `MutationKind::Delete` + `Update` records + replay (closes
      crash-safety gap surfaced in `decisions.md` 2026-05-23 DELETE
      hot-tier entry; today tombstones are lost on engine restart before
      next compaction).

### Storage / format / cache
- [x] Vortex default since 2026-05-18 ([ADR 0015](./decisions/0015-vortex-storage-format.md));
      88-shape `vortex_vs_parquet_smoke` battery green; differential harness
      asserts byte-identical results vs Parquet across the full battery.
- [x] HTAP hot tier (memtable + WAL markers + read-path merge); Phase
      5.14.C1–C6 shipped (`57d2ae11`…`9e107ef`).
- [x] Per-file catalog blooms + column stats; Phase 5.14.A (`ae6460a`…`c5739a6`).
- [x] FileMetadataCache + VortexFooterCache wired (`d26a92d`, `f5c01ef`).
- [x] NVMe disk cache + Parquet page cache default-on (101× / 7.24× speedups
      measured).

### Real-cloud perf parity (read shapes)
- [ ] Real-S3 perf parity for read shapes (cold_start, point query, range
      scan, bulk LIST/DELETE) — `benchmark/RESULTS_localfs.md` is green;
      `benchmark/index_real.html` real-cloud cards need a sweep before cut.
      Today's known gaps are documented as workload caveats, not silent
      regressions.

### PG-compatibility
- [x] pg_query (libpg_query) as canonical parser frontend; unsupported
      statements rejected with SQLSTATE 0A000 before sqlparser sees them
      ([ADR 0014](./decisions/0014-pg-query-as-canonical-parser.md)).
- [ ] sqllogictest curated suite ≥ 95% (current: `b7114e8` brought
      jsonb_udf BinaryView/Utf8View → **100% on the curated set**;
      framework in `tests/integration/tests/sqllogictest_pg.rs`). Confirm
      the full PG-port slice before cut.
- [ ] ORM corpus ≥ 95% per ORM:
  - Drizzle — `6bd6d60` array-params keystone landed; corpus expansion in
    `faa5ae2` (corpus 79% → ~89%); push to ≥ 95%
  - Prisma — `pg_get_serial_sequence` shim (`a0bc33a`) landed; #93 Prisma
    `json_agg` rewriter still in flight
  - sqlx — exercised via `tests/integration/tests/orm_compat.rs`
  - Diesel — exercised via `tests/integration/tests/orm_compat.rs`
  - TypeORM — `7469c09` qualified-column WHERE under bound protocol
    landed; verify the rest of the corpus

### Dogfooding
- [ ] basin-cloud catalog runs on Basin itself. Cloud control plane
      currently uses Neon — this is the highest-visibility credibility gap
      and is a v0.1 gate.

### Wasm UDFs
- [x] `LANGUAGE wasm` execution path (Phase 5.11.J, `fa65bcd`).
- [ ] Wasm UDF text / bytea / JSONB type surface (in flight as #90).
      Today's path is i64-only — text/bytea/JSONB args + returns are
      required before v0.1 cut.

### Honest positioning
- [x] Honest README with per-shape benchmarks (just rewritten this session
      by the parallel README agent — see git history).
- [x] Per-shape benchmark cards public — `benchmark/index_localfs.html` +
      `benchmark/RESULTS_localfs.md`; real-cloud cards in
      `benchmark/index_real.html`.
- [x] Performance residuals catalogued under
      [CAPABILITIES.md → "Performance residuals (won't chase further)"](../CAPABILITIES.md#performance-residuals-wont-chase-further).
- [x] Documented exclusions (CAPABILITIES.md § "Documented exclusions
      (5.22.E)") cover every SQLSTATE-0A000 rejection.

---

## Shipped already (don't touch — v0.1 substrate)

These are the load-bearing pieces that already exist and don't need any
further investment for v0.1. Cite by commit / phase per `decisions.md`.

- **pgwire v3** (simple + extended, TLS via rustls, COPY FROM STDIN /
  COPY TO STDOUT, binary JSONB / UUID / NUMERIC / ARRAY wire formats).
- **Durable Postgres-backed catalog** (Phase 5.7 A4 + B1; cross-restart
  smoke test green).
- **WAL** (`LocalWal` production today; `RaftWal` openraft single-process
  3-node cluster simulation; cross-process Raft networking is v0.2 per
  `crates/basin-wal/RAFT.md`).
- **Shard owners + eviction** (Phase 3 v0.1 shipped 2026-05-01).
- **Per-project bucket-prefix isolation + per-project EDF fairness**
  ([ADR 0008](./decisions/0008-noisy-neighbor-fairness.md)).
- **basin-realtime** (SSE + WebSocket + presence + filter pushdown +
  per-project memory budget; Phase 5.11.R1–R6 fully shipped).
- **basin-auth + basin-rest + basin-pool** (Phase 5.10 open-source bundle;
  ADRs 0005 / 0006 / 0007 / 0013).
- **basin-blob** (object storage; ADR 0021).
- **basin-iceberg-rest** (Lakekeeper-compatible REST catalog).
- **Vector search** (HNSW per-file sidecar; planner routing of
  `ORDER BY x <-> $1 LIMIT k`).
- **Query Insights** (Phase 5.16.A–D; `basin_stat_statements` + OTLP).
- **Cloud-side observability** (5-commit wave 2026-05-23: central email,
  EXPLAIN viewer, slow-query alerts, Fly autoscale alerts, plan-diff
  alerts).
- **Migration-tool compat matrix** (Phase 5.25; see
  [`docs/migration-tools.md`](./migration-tools.md)).
- **5-minute Docker quickstart** (Phase 5.31.E + 5.32.E,
  `4482ede` + sourced from real Dockerfile in `e4b79f6`).

---

## What's PARKED for v0.1 (post-v0.1 / never)

Each entry: name + brief rationale + the unparked-when condition.

### Crates to freeze (no new investment)

| Crate | Rationale | Unpark when |
|---|---|---|
| `basin-geo` | PostGIS-equivalent stub. Real PG-class spatial isn't on the wedge path. | A wedge customer asks AND pays for it. |
| `basin-trgm` | pg_trgm equivalent; v0.1 ships brute-force `similarity` / `word_similarity`. GIN trigram index = post-v0.1. | A wedge customer asks AND pays. |
| `basin-cron` | Scheduled jobs in SQL. Use external scheduler (cron-on-fly, GH Actions, etc.) for now. SQL surface already shipped (`a091c04`); no new investment. | A wedge customer asks AND pays. |
| `basin-net` | `net.http_*`. Use an external functions service (Cloudflare Worker, Lambda, etc.) for now. SQL surface already shipped (`a091c04`); no new investment. | A wedge customer asks AND pays. |
| `basin-fn` (Wasm) | Keep #90's text/bytea/JSONB surface as-is once landed. **No further investment** in Component Model migration, SDK polish, or new examples. | Component Model becomes a wedge-customer ask. |
| `basin-autoscale` | Stays a planned feature per the autoscale memory note, but no v0.1 perf investment beyond what's already merged. | basin-cloud reaches scale where manual scaling hurts. |
| `basin-webhooks` | Outbound webhooks layer. Use basin-realtime + a customer-side worker pattern for v0.1. | A wedge customer asks AND pays. |

### Features to freeze (no new investment for v0.1)

- **Continuous materialized views beyond what already ships.** `basin-cv`
  incremental refresh for `date_trunc` / `time_bucket` shapes is shipped;
  no new shape coverage in v0.1.
- **Range types beyond what already ships.** All six range types + GIST
  interval-tree probe wired in 5.24.D (`decisions.md` 2026-05-22), and
  `EXCLUDE USING gist` exclusion constraints on range columns are enforced
  (sentinel-CHECK). Still out: range-of-records, `EXCLUDE USING gist` on
  geometry.
- **FTS GIN wiring expansion.** `to_tsvector` + `@@` ship correctness-only;
  `ts_headline`, weighted vectors (`setweight`), language configs beyond
  `english` / `simple` are post-v0.1.
- **Multi-region replication.** Multi-region by deployment works today;
  cross-region read replicas + eventual-consistent S3 CRR are scoped in
  [ADR 0004](./decisions/0004-multi-region-read-replicas.md) but PARKED.
- **Cross-shard JOINs / distributed transactions.** Cross-shard JOIN
  deferred; cross-region 2PC explicitly out per
  [ADR 0001](./decisions/0001-single-region-only.md).
- **`CREATE TRIGGER` / PL/pgSQL.** Explicitly ruled out per
  [ADR 0012](./decisions/0012-change-event-primitive.md). Basin replaces
  the surface with declarative lifecycle + SQL-bodied reactors +
  `LANGUAGE sql` functions + `CALL` procedures (~95% coverage).
- **New storage formats** beyond Vortex / Parquet, and Vortex tuning beyond
  #92's 2-pass Fast/Best encoding.
- **`ALTER TABLE … RENAME TO` / `RENAME COLUMN`.** Rejected at parse time
  in v0.1.
- **Composite types** (`CREATE TYPE … AS (...)`). Use JSONB.
- **`GENERATED ALWAYS AS … VIRTUAL`.** Only `STORED` ships.
- **`SELECT … AS OF SNAPSHOT n` / `AS OF TIMESTAMP ts`.** Parser support
  deferred to `basin-analytical` v0.2.
- **`pg_dump` / `basin dump` parser-side imports.** Export side ships;
  ingest of foreign `pg_dump` output is post-v0.1.
- **GRANT / REVOKE / role-based access control.** RLS policies
  (`CREATE POLICY`) are the v0.1 isolation primitive; full role model is
  post-v0.1.

### Realtime — maintenance mode

- **Keep what's shipped** (basin-realtime crate: SSE + WebSocket +
  presence + filter pushdown + per-project memory budget; Phase 5.11.R1–R6).
- **Don't add:** broadcast channels, room presence v2, multi-region
  realtime fan-out, or any new transport. Documented surface stays stable.
- **Position:** "Realtime is in maintenance mode for v0.1; SSE row-change
  subscribe + WS multiplex + presence are stable; no extensions planned."

---

## Acceptance criteria for unparking

A frozen item gets unfrozen only when one of these triggers fires:

1. **A named wedge customer requests it AND has a paid contract pending.**
2. **A production-blocking dependency forces it** (rare).

The bar is intentional. The wedge is "cheap multi-project Postgres on S3
for SaaS analytics." Anything not on the path to a paying wedge customer
waits for v0.2+.

When the trigger fires, the unparking commit must:
- Cite this doc + the customer / blocker that fired the trigger.
- Move the item from this doc's PARKED list into a v0.2 scope doc (or
  inline into TASK.md's active phases).
- Update [CAPABILITIES.md](../CAPABILITIES.md) status from 🚫 / ◻️ → 🛠.

---

## What v0.2 likely includes (informal — not a commitment)

These are post-v0.1 ideas already documented in `decisions.md` or TASK.md
that have an obvious wedge-customer pull. Listed here so they're not lost
when v0.1 ships; **not promises**.

- **B-tree indexes on non-PK columns.** Foundation shipped (Phase 5.7 B1,
  `33a8162`); expansion is post-v0.1.
- **Trigram / inverted index for `ILIKE %suffix`.** `basin-trgm` v0.2 GIN
  trigram index is the closer.
- **JSONB binary format / simd-json.** Architectural finding from
  `decisions.md` 2026-05-23 (JSONB UDF perf entry): `serde_json::from_slice`
  per-row dominates. PG wins on JSONB keyed access because of its binary
  format. Closes the 5-10× JSONB extract gap.
- **Scan-side TopK pushdown.** Upstream Vortex blocker (#91 no-fix);
  closes the 5-19× pagination gap. Waits on upstream Vortex PR per the
  upstream-PRs-parked memory.
- **Multi-region read replicas** ([ADR 0004](./decisions/0004-multi-region-read-replicas.md)).
- **WASM Component Model migration.**
- **Cross-shard JOIN** at the router layer.
- **Physical file GC for orphaned post-rollback files** + cross-DML
  rollback (soft-delete).
- **Row-group-level coalesced stats** (Phase 5.7 A4 B1 follow-up).
- **`pg_dump` ingest path** (today: export-only + skipped-feature
  annotations).

---

## How this doc gets updated

- **When an item under "Required for v0.1" ships:** flip `[ ]` → `[x]` and
  cite the commit SHA on the same line. Don't drop the item — the history
  is the audit trail.
- **When an item under "PARKED" gets a customer trigger:** see
  "Acceptance criteria for unparking" above.
- **When a new feature proposal lands:** map it to a required-v0.1 bullet
  here. If it doesn't map, it's post-v0.1 and the proposing PR needs to
  cite this doc + a customer trigger.
- **Cadence:** review this doc weekly during pre-cut hardening; review at
  each ADR landing thereafter.

---

## Linked docs

- [`README.md`](../README.md) — honest perf + IS / IS-NOT positioning
- [`CAPABILITIES.md`](../CAPABILITIES.md) — full per-feature matrix (✅ /
  🛠 / ◻️ / 🚫)
- [`WEDGE.md`](../WEDGE.md) — wedge-deepening roadmap (the next-six-months
  prioritized slice)
- [`benchmark/BENCHMARKS.md`](../benchmark/BENCHMARKS.md) — full per-shape
  numbers
- [`decisions.md`](../decisions.md) — wave-by-wave engineering log
- [`docs/decisions/`](./decisions/) — ADRs (0001 through 0026)
- [`docs/sql-support.md`](./sql-support.md) — fine-grained per-syntax
  matrix from automated tests
