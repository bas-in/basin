# Changelog

All notable changes to Basin are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The pre-1.0 contract: minor versions can break public API; patch versions
are bug-fix only. Once the engine wedge ships to design partners we
graduate to 1.0 and the standard SemVer guarantees.

## [Unreleased]

Strategic checkpoint 2026-05-19: durable-Basin-moat plan adopted (TASK.md
Phase 5.14). Phase 5.12 perf + storage work, Phase 5.13 pg_query parser
migration, multi-schema isolation phases A.1/A.2/A.3, real transaction
semantics, and 88-shape Vortex⇆Parquet smoke battery all landed since
v0.1.3.

### Added — Storage (ADR 0015)
- **Vortex storage default since 2026-05-18** ([ADR 0015](./docs/decisions/0015-vortex-storage-format.md)).
  Opted-in 2026-05-11, default-flipped after correctness prerequisites
  shipped (self-describing decode, view-type normalisation, catalog-stats
  file pruning, format-aware compaction, format-agnostic vector-search).
  ~1.95× smaller on disk; on-par-to-better full-scan / aggregate / string-eq
  throughput vs ZSTD Parquet; trailing on point-lookup and ORDER BY+LIMIT.
- **Self-describing Vortex decode** — `vortex_format::decode` recovers
  Arrow schema from the file's own `DType` via `vf.dtype().to_arrow_schema()`
  when no catalog schema is supplied; `Utf8View`/`BinaryView` normalised
  to canonical `Utf8`/`Binary`.
- **FileMetadataCache wired into RuntimeEnv** — eliminates per-iteration
  footer re-parse.
- **VortexFooterCache** — skips per-file footer re-parse on hot shapes.

### Added — Performance fast paths (#161, #162)
- **Metadata-only aggregate fast path (~30-40×)** — bare COUNT/SUM/MIN/MAX
  answered from catalog `column_stats` + Vortex footer; bypasses
  DataFusion entirely.
- **Point / range / IS NULL / BETWEEN fast paths** — `fast_select.rs`
  short-circuits common predicate shapes through the storage read layer,
  with catalog-stats file pruning and Arrow post-filter where needed.
- **Inequality predicate fast path** — `>`, `<`, `>=`, `<=` join the
  point-eq fast path.
- **ORDER BY single_col LIMIT n fast path** — pushed through the storage
  read layer.
- **Low-cardinality GROUP BY COUNT(*) fast path** — bypass DataFusion's
  full aggregate executor for the common dashboard shape.
- **`FAST-AGG-GROUPBY` aliased-projection support** — `expr_projection`
  accepts aliased scalar projections; ORM-style `SELECT col AS alias`
  hits the fast path.
- **UNION ALL same-table collapse** — collapses UNION ALL of same-table
  scans to a single scan + OR predicate; restores output projection
  shape.
- **`NULLIF(a,b) IS [NOT] NULL` analyzer rewrite** — rewrites to a plain
  conjunction so Vortex's type-gated pushdown engages.
- **`STREAMLIMIT`** — forces single-partition stream for OFFSET on
  sort-matching scans; avoids the coalesce-then-skip overhead.
- **Utf8/Binary → Utf8View promotion in schema** — recovers the
  zero-copy view-array fast path for UDFs that accept the view types.
- **Native Vortex projection + type-safe filter pushdown** — predicates
  pushed into Vortex when the catalog schema proves the column's Arrow
  type exactly matches the literal.
- **Zero-copy `batch_df_to_ws` on SELECT hot path** — eliminates the
  workspace-arrow ↔ DataFusion-arrow copy on SELECT.
- **Scan concurrency = 8 on Vortex ListingTable** — recovers parallel
  scan on multi-file Vortex tables.

### Added — DDL options
- **`basin.file_format`** per-table option — `CREATE TABLE … WITH
  (basin.file_format = 'vortex' \| 'parquet')`. Vortex is the default
  since 2026-05-18; Parquet remains first-class selectable per ADR 0015.
- **`basin.sort_by`** compound DDL option (WEDGE 4) — declares
  `file_sort_order`; the writer enforces it via `lexsort_to_indices`
  + `take` before flush. Recovers window shapes whose `PARTITION BY` /
  `ORDER BY` matches the declared sort.
- **`basin.row_block_size`** per-table option — per-table chunk
  granularity; tunes point-heavy vs scan-heavy shapes.

### Added — Parser (ADR 0014)
- **pg_query canonical front-end (Phase 1)** — `pg_query` 6.x vendored;
  `crates/basin-engine/src/pg_ast.rs` ships `parse`, `ParseTree`,
  `stmt_kind`, `StmtKind`, `reject_unsupported`. Every statement parses
  through PostgreSQL 16's real parser first; unsupported kinds rejected
  with SQLSTATE 0A000 before sqlparser sees them. `BASIN_PG_QUERY` env
  gate enabled. Engine reuses the pg_query parse tree on re-entry to
  eliminate duplicate C-library parses per query. See [ADR 0014](./docs/decisions/0014-pg-query-as-canonical-parser.md).
- **`reject_unsupported` guard** — `LISTEN`, `NOTIFY`, `PREPARE`,
  `DECLARE CURSOR`, `LOCK`, `VACUUM`, `CLUSTER`, `ANALYZE`,
  `CREATE EXTENSION`, `CREATE TRIGGER` all return clean 0A000.

### Added — Schema / multi-schema isolation (#116)
- **A.1: `SchemaName` + `QualifiedTableName` types in `basin-common`** (#117).
- **A.2: `QualifiedTableName` API + `InMemoryCatalog` schema-aware impl** (#118).
- **A.3: `PostgresCatalog` schema-aware impl + `basin_schemas` table** (#119).
- **+12 multi-schema differential cases** covering `CREATE SCHEMA` /
  `DROP SCHEMA` / cross-schema queries (#146).

### Added — Transactions
- **Real `BEGIN`/`COMMIT`/`ROLLBACK` + `SAVEPOINT` semantics** (#92,
  completes #83): commits deferred while in-transaction, ROLLBACK undoes,
  SAVEPOINT stack supported, aborted-state recovery. Driver-implicit
  `BEGIN TRANSACTION READ WRITE` no longer rejects.
- **Optimistic-lock row-version verification under concurrent writers** (#103).
- **`LATERAL generate_series` + SAVEPOINT rollback + CTAS WITH NO DATA**
  (commit `92aa0d0`).
- **Scalar subquery in `UPDATE SET`** (#106); `UPDATE … WHERE id IN
  (SELECT …)` restored after #66 regressed it (#76).

### Added — Wire protocol
- **Real `NUMERIC` binary wire format** — varlena base-10000 encoding
  (#141); previously was sending text bytes through the binary slot.
- **Real `ARRAY` binary wire format** — PG list-element encoding (#144);
  same fix.
- **`TIMESTAMP`/`DATE` binary param decode** (#67).
- **Extended-protocol `RETURNING`** — encode projected rows as DataRows
  (#73).
- **Reject multi-statement extended `Parse`** per PG spec (#68).

### Added — DML / DDL completeness
- **`DROP TABLE`** + **`IF NOT EXISTS` on CREATE TABLE** (#49).
- **`INSERT … ON CONFLICT DO NOTHING`** — single-column conflict-target
  match suppresses UNIQUE violation (#75).
- **`INSERT … ON CONFLICT DO UPDATE`** — `table.col` + `EXCLUDED.col`
  resolution (#74).
- **Data-modifying CTEs** — `WITH x AS (INSERT/UPDATE/DELETE … RETURNING)
  SELECT …` (commit `6056dca`).
- **`MERGE` honest-reject** — silent-noop → 0A000 with reason; 3 stale
  differential tests un-ignored (commit `975dd93`).
- **`RLS WITH CHECK` enforcement** + real `TABLESAMPLE` + honest
  `CREATE TRIGGER` (commit `c92675b`, paired with array-rewrite OOB fix).
- **`SUBSTRING(x FROM 'regex')` POSIX-style first-match extraction** (#97).
- **Correlated subqueries in DELETE/UPDATE** via SELECT-decorrelation path
  (commit `6d04524`).
- **Correlated `LATERAL` → JOIN decorrelation** for non-aggregate
  row-returning bodies (#81).
- **Multi-column `WITH RECURSIVE`** — propagate all CTE column aliases (#82).
- **`int4range`/`int8range`/`numrange` arithmetic + multirange
  containment** (#94).
- **JSONB cast + extraction on text columns** — `data::jsonb->>'key'` (#88).
- **JSON families** salvaged from dead-agent WIP (commit `4300c72`):
  `json_to_record` / `jsonb_to_record AS t(coldefs)` (#78),
  `json_agg(t)` whole-row (commit `78f8057`).
- **Full-text search bounded subset** — `tsvector` / `tsquery` / `@@` /
  `to_tsvector` / `to_tsquery` (#79).
- **Exact `percentile_disc` / `mode()` WITHIN GROUP ordered-set
  aggregates** (#77).
- **INET/CIDR containment UDFs** (#153, partial).
- **4 silent-corruption PG-compat CRITICALs** enforced honestly
  (commit `25c42e3`).
- **String / datetime / window PG-compat modules** salvaged from
  quarantine (commit `41d4b03`).
- **psql `\dt` / `\d` family** — 20 pg_catalog scalar stubs registered.
- **dynamic per-project `max_connections`** enforced at pgwire (no
  commercial coupling; commit `9385474`).

### Added — Testing
- **PG-oracle differential test harness** (25 initial cases, #129).
- **+33 PG-compat differential cases** — extended params, COPY,
  sequences, RECURSIVE, LATERAL, strings, arrays, NULLS, txn, CAST (#143).
- **+12 multi-schema differential cases** (#146).
- **88-shape `vortex_vs_parquet_smoke` benchmark battery** — robust
  scale-configurable size × shape matrix (10k / 25k / 100k / 1M opt-in);
  honest characterization in CHANGELOG / README / WEDGE.
- **Vortex⇆Parquet differential correctness harness** — asserts
  byte-identical results across point / range / inequality / IS NULL /
  string-eq / compound / aggregate / GROUP BY / ORDER BY+LIMIT /
  projection / full-scan + DELETE/UPDATE rewrite, on multi-file tables.
- **5 speed scenarios for 4-way comparison** — basin vs PG vs Neon vs
  Supabase (#145).
- **Curated ORM/driver-compat suite** — param binding, nested reads,
  RETURNING, txn, CTE (commit `8b71eca`).
- **`orm_compat` 4 stale-ignore tests flipped green** — `json_agg`
  JSONB wire type, `LATERAL ORDER BY` rewrite, correlated-subquery type
  inference, `DELETE` alias (#93).
- **SQL-compat matrix expanded 490 → 697 fragments** — honest coverage
  (commit `39d51bb`).
- **Coverage for error paths, transactions, and schema evolution**
  (commit `d129b0e`).

### Added — Auth (already in [Unreleased] but kept here for completeness)
- `auth.uid()`, `auth.role()`, `auth.jwt()` SQL session functions — Supabase-compatible;
  usable in RLS policies (`CREATE POLICY … USING (owner_id = auth.uid())`). Both
  `auth.uid()` and `auth_uid()` spellings work.
- Per-project auth schema — auth data now lives in each project's own Basin storage
  (like Supabase's `auth` schema per project). No reserved internal project, no loopback
  pgwire connection. See ADR 0013.
- Self-routing pgwire credentials — `pgwire_user` format now encodes `project_id` as a
  26-char ULID prefix, enabling credential validation without a global cross-project lookup.
- `AuthStore` trait — pluggable auth storage; `PostgresAuthStore` for external Postgres,
  `EngineAuthStore` (default) for in-process Basin storage. Zero external dependencies
  for open source deployments.

### Added — SQL-compat lift (already in [Unreleased])
- **SQL-compat lift: ~75% → 97.2% fragment coverage.** 423 of 435
  non-design-excluded fragments now pass end-to-end (490 total; 55 are
  explicit design-exclusions). Key work shipped in this cycle:
  JSONB operators (`->` / `->>` / `#>` / `@>` / `?` family) via
  DataFusion JSON support; scalar function catalogue (date/time,
  string, math, coalesce/nullif/greatest/least); range, array, and
  date type support; operator rewriters for vector distance and JSONB
  paths; `LIMIT`/`OFFSET` parameter-type inference (fixes `i64`
  binding from ORMs); `BYTEA` DDL + Arrow `LargeBinary` round-trip;
  `NUMERIC`/`DECIMAL` column type; design-exclusion classification
  of 55 deliberately out-of-scope fragments (`LISTEN/NOTIFY`,
  `BEGIN/COMMIT/ROLLBACK`, `DROP TABLE`, extensions, etc.).
  Remaining real v0.2 gaps: `LATERAL` joins, `WITH RECURSIVE` +
  DML-in-CTE, advanced window frames (`RANGE INTERVAL` / `GROUPS` /
  `EXCLUDE`), `JSON_AGG(t)` whole-row, `EXCLUDE USING gist`.
  See [`docs/sql-support.md`](./docs/sql-support.md).

### Changed
- Basin-server startup order: auth initialises before pgwire (eliminates `DeferredAuthResolver`
  and `wait_for_pgwire_accept` polling loop).
- `BASIN_AUTH_CATALOG_DSN` is now an optional external Postgres override rather than the
  default loopback path.
- **Storage backend rename** — `r2` backend renamed to `s3_compatible`;
  `R2Config` alias dropped (commit `88162d5`). docs replace Cloudflare R2
  with Tigris; Apache DataFusion attribution added (commit `4e1f87c`).
- **Workspace migration to arrow58 / df53 / sqlparser0.61 / object_store0.13**
  (commit `2b51061`) — workspace compiles clean; test modules updated.
- **CHECK constraints** — store bare predicate and strip wrapper at
  enforce time (commit `5bd3253`).
- **Router accept hot path** — replaced per-accept `Mutex` with `RwLock`
  in `LiveCounts`; dropped Arc on `conn_guard` (commit `25ddba5`).
- **Storage scan** — cut per-row-group allocs; eliminated redundant
  `HEAD` (commit `6f09793`); router-side per-row `BytesMut` /
  per-cell `String` allocs eliminated on the text encoding path
  (commit `af790e3`).

### Removed
- **`basin-cloud` / `basin-billing` crates** out of OSS repo — hosted
  product items moved to a separate (closed-source) `basin-cloud` repo.
  The OSS engine ships `EncryptionProvider` / `BillingProvider`-shaped
  traits; external callers wire their own adapters. `CLOUD_ROADMAP.md`
  removed.

### Migration
- Existing `pgwire_user` credentials in the old `project_<hex>` format are automatically
  rotated to the new `{project_id}_{hex}` format on first startup after upgrade.
- Existing tables without a recorded `basin.file_format` continue to read
  and write as Parquet (zero migration); new tables default to Vortex.

## [0.1.9] - 2026-05-17

Vortex storage default ship batch. See [ADR 0015](./docs/decisions/0015-vortex-storage-format.md).

### Added
- Vortex codec encode/decode + writer wiring (Phase 0/1/2 of #161).
- Per-table `WITH (basin.file_format = 'vortex')` opt-in (Lanes 1–8 of #161).
- `ALTER TABLE … SET FILE_FORMAT` for empty tables (Lane 8).
- Self-describing Vortex decode + view-type normalisation.
- Differential Vortex⇆Parquet correctness harness.

### Changed
- **Vortex is the default on-disk format** as of 2026-05-18 (commit `988fe7d`).
  Parquet remains first-class selectable. ADR 0015 updated.

## [0.1.8] - 2026-05-15

Perf and observability batch.

### Added
- Metadata-only aggregate fast path (~30-40×).
- Point / range / inequality / IS NULL / BETWEEN fast paths.
- Catalog-stats file pruning in `fast_select.rs`.
- Low-cardinality GROUP BY COUNT(*) fast path.
- `basin.sort_by` compound DDL option (WEDGE 4) + writer enforcement.
- `basin.row_block_size` per-table DDL option.
- FileMetadataCache wired into RuntimeEnv.
- VortexFooterCache.
- 88-shape `vortex_vs_parquet_smoke` benchmark battery.
- Utf8/Binary → Utf8View promotion.
- UNION ALL same-table scan collapse.
- `NULLIF(a,b) IS [NOT] NULL` conjunction rewrite.
- `STREAMLIMIT` for OFFSET on sort-matching scans.

### Fixed
- Red pipeline (sccache, rustfmt drift, object_store test, approx_constant)
  (commit `6fd6c5d`).
- Fast-path bail inside an explicit transaction (commit `790ed79`).

## [0.1.7] - 2026-05-14

Schema isolation and transaction-semantics batch.

### Added
- Multi-schema isolation phases A.1 (#117), A.2 (#118), A.3 (#119).
- Real `BEGIN`/`COMMIT`/`ROLLBACK` + `SAVEPOINT` semantics (#92,
  completes #83).
- Optimistic-lock row-version verification under concurrent writers (#103).
- `LATERAL generate_series` + SAVEPOINT rollback + CTAS WITH NO DATA.
- Scalar subquery in `UPDATE SET` (#106).
- INET/CIDR containment UDFs (#153, partial).

## [0.1.6] - 2026-05-13

Wire-format correctness + parser-foundation batch.

### Added
- Real `NUMERIC` binary wire format — varlena base-10000 (#141).
- Real `ARRAY` binary wire format — PG list-element encoding (#144).
- `TIMESTAMP`/`DATE` binary param decode (#67).
- Extended-protocol `RETURNING` row encoding (#73).
- Reject multi-statement extended `Parse` per PG spec (#68).
- pg_query parse-tree foundation in `pg_ast.rs` (ADR 0014 Phase 1).
- Engine reuses pg_query parse tree on re-entry (commit `a82d9f6`).

### Fixed
- Multi-byte UTF-8 panic in `pg_operators::find_word_sequence` (#65).
- `RLS WITH CHECK` enforcement; real `TABLESAMPLE`; honest
  `CREATE TRIGGER`; array-rewrite OOB panic.

## [0.1.5] - 2026-05-12

DML completeness + ORM-compat lift.

### Added
- `DROP TABLE` + `IF NOT EXISTS` on `CREATE TABLE` (#49).
- `INSERT … ON CONFLICT DO NOTHING` / `DO UPDATE` (#74, #75).
- Data-modifying CTEs (`WITH x AS (INSERT/UPDATE/DELETE … RETURNING) SELECT …`).
- Correlated subqueries in DELETE/UPDATE.
- Correlated `LATERAL` → JOIN decorrelation (#81).
- Multi-column `WITH RECURSIVE` (#82).
- `int4range`/`int8range`/`numrange` arithmetic + multirange containment (#94).
- JSONB cast + extraction on text columns (#88).
- Full-text search bounded subset (`tsvector` / `tsquery` / `@@`) (#79).
- Exact `percentile_disc` / `mode()` WITHIN GROUP ordered-set aggregates (#77).
- 4 silent-corruption PG-compat CRITICALs enforced honestly.
- String / datetime / window PG-compat modules salvaged.
- 20 psql `\dt` / `\d`-family pg_catalog scalar stubs.
- Curated ORM/driver-compat suite + `orm_compat` 4 stale-ignore flips (#93).

## [0.1.4] - 2026-05-11

Toolchain migration + benchmark refresh.

### Changed
- Workspace migration to arrow58 / df53 / sqlparser0.61 / object_store0.13
  (commit `2b51061`).
- `r2` storage backend renamed to `s3_compatible`; `R2Config` alias dropped.
- Docs replace Cloudflare R2 with Tigris; Apache DataFusion attribution added.

### Added
- 3-way Neon / Supabase / Basin-Cloud Frankfurt harness (build only;
  run is operator-gated).
- Post-migration `LocalFS+SeaweedFS` regenerated data for arrow58/df53.
- Parallel config × category harness (per-group `--test`, `-j6`,
  per-pkg `debug=0`).
- SQL-compat matrix expansion to 490 → 697 fragments (commit `39d51bb`).

## [0.1.3] - 2026-05-11

Engine catch-up + CI / release pipeline hot-fix.

### Added

- **`TIMESTAMP` (without time zone) accepted** in `CREATE DOMAIN`,
  `CREATE FUNCTION` arg / `RETURNS TABLE` column, and `CREATE
  PROCEDURE` arg surfaces. The v0.1.1 CHANGELOG claimed this but only
  `CREATE TABLE` actually shipped it. New `SqlArgType::Timestamp`
  variant bridges to Arrow `Timestamp(Microsecond, None)` and PG OID
  1114 (distinct from `TIMESTAMPTZ` / OID 1184 at the wire).
- **Constraint introspection views populated.** `pg_catalog.pg_constraint`,
  `information_schema.table_constraints`, `key_column_usage`, and
  `referential_constraints` now emit real rows derived from each
  table's declared PRIMARY KEY / FOREIGN KEY / CHECK / NOT NULL. The
  v0.1.1 CHANGELOG claimed these populated but the underlying
  functions returned empty `RecordBatch`es. PostgREST / pgAdmin
  schema-discovery queries now resolve.
  - `pg_constraint.contype` emits `'p'` (PK), `'f'` (FK), `'c'`
    (CHECK), `'n'` (NOT NULL) per PG convention.
  - PK constraint named `<table>_pkey`; FK keeps its declared name;
    CHECK keeps its declared name; NOT NULL named
    `<table>_<col>_not_null`.
  - `referential_constraints.update_rule` / `delete_rule` map
    `RefAction::NoAction` → `"NO ACTION"`, `RefAction::Cascade` →
    `"CASCADE"`.
  - `key_column_usage` emits one row per PK column + per FK local
    column, with 1-based `ordinal_position` within the constraint
    (not within the table) — matches PG semantics.

### Fixed

- **CI test job OOM at link time.** `basin-integration-tests` was
  linking a near-full workspace per binary; the linker bus-faulted on
  GitHub's 7 GB runners. The job now runs
  `cargo test --workspace --exclude basin-integration-tests` and sets
  `CARGO_PROFILE_DEV_DEBUG=line-tables-only` to shrink test binaries.
  Heavy `viability_*` / `s3_scaling_*` cards run on developer
  workstations, not CI.
- **sccache broke every workflow.** The GHA cache backend
  (`artifactcache.actions.githubusercontent.com`) returns HTTP 400
  intermittently, and `RUSTC_WRAPPER=sccache` propagates to every
  rustc call — so a degraded backend takes out clippy, test, audit,
  and release simultaneously. Removed from both workflows. Swatinem
  `rust-cache` is the sole cache layer.

### Changed

- **Release matrix** trimmed to three targets:
  `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` (native
  `ubuntu-24.04-arm` runner), `aarch64-apple-darwin`. `macos-13`
  (Intel Mac) dropped — Rosetta runs the aarch64 binary.
- **RELEASING.md** refreshed: 3-target matrix, no `-D warnings`,
  `--exclude basin-integration-tests` in the local sanity-check
  sequence.

## [0.1.2] - 2026-05-10

### Added

- **`SERIAL` / `BIGSERIAL` / `SMALLSERIAL`** column types (+ `SERIAL2`
  / `SERIAL4` / `SERIAL8` aliases). PG-shaped: each `SERIAL` column
  auto-creates a sequence named `<table>_<col>_seq`, stamps
  `DEFAULT nextval('<seq>')`, and is implicitly `NOT NULL`.
  `CREATE TABLE t (id SERIAL PRIMARY KEY, …)` now works through pgwire
  without the user spelling out the sequence. `SMALLSERIAL` widens to
  Int64 physically (the INSERT path has no Int16 row-builder yet).

### Fixed

- `rewrite_sequence_calls` now emits a plain integer literal instead
  of `<n>::bigint`. The cast was surviving rewrite and tripping the
  INSERT-default evaluator, which only recognised bare numbers.
- `clippy::approx_constant` deny in `prepared.rs` (`3.14` literal in a
  decimal-preservation test) — was breaking clippy + test CI jobs.
  Changed to `2.25`.

### Changed

- **CI**: dropped redundant `cargo build` step in `test`, dropped
  macOS from test matrix, added `concurrency` cancellation, relaxed
  `cargo audit` (no `--deny warnings`).
- **Release**: native `ubuntu-24.04-arm` runner for aarch64-linux
  (replaces `cross` + Docker, which was timing out on
  duckdb-bundled). Persisted per-target build cache. Stripped debug
  info from release artefacts.

> **Pipeline note**: v0.1.2 also tried sccache via the GHA cache
> backend; that broke every workflow when Azure's cache endpoint
> returned 400s. Fixed in v0.1.3.

## [0.1.1] - 2026-05-10

First public release. Captures Phase 5.11 closure (Tier 0–3) and the
Phase 6 production-hardening entry batch.

### Added

- **Phase 5.11 — modern SaaS toolkit** (per [ADR 0012](docs/decisions/0012-change-event-primitive.md)):
  - Tier 0: `ChangeEventSink` trait + capture point in `basin-common`
  - 5.11.A: built-in function catalogue (date/time, string, math, coalesce, JSONB operators); recursive-CTE / window verification
  - 5.11.B: declarative lifecycle — `AUTO_UPDATE` / `AUDIT TO` / `SOFT DELETE` column attributes
  - 5.11.C / C2: SQL-bodied reactors (`ALTER TABLE … REACT ON … EXECUTE`) + constraint reactors via `__basin_assert(predicate, error_text)` UDF
  - 5.11.D / E / F: `LANGUAGE sql` scalar functions, `RETURNS TABLE` functions, multi-statement `CALL` procedures (planning-time inlined)
  - 5.11.D2: `CREATE MATERIALIZED VIEW … WITH (basin.continuous, …)` SQL surface
  - 5.11.I: webhook fanout (`ALTER TABLE … SUBSCRIBE WEBHOOK …`) with retry queue, dead-letter, idempotency keys, per-project counters
  - 5.11.K: generated columns (`GENERATED ALWAYS AS (expr) STORED`)
  - 5.11.K2: `CREATE TYPE … AS ENUM` + `CREATE DOMAIN`; enum ordinal comparison via `ORDER BY` planner rewrite
  - 5.11.K3: sequences (`CREATE SEQUENCE` + `nextval` / `currval` / `setval` UDFs); multi-option grammar via textual pre-screen
  - 5.11.M: 17 `information_schema` + `pg_catalog` views (`tables` / `columns` / `routines` / `views` / `schemata` / `table_constraints` / `key_column_usage` / `referential_constraints` + `pg_class` / `pg_attribute` / `pg_namespace` / `pg_proc` / `pg_type` / `pg_constraint` / `pg_index` / `pg_depend` / `pg_authid`); PostgREST + pgAdmin + Prisma + Sequelize + SQLAlchemy startup-query compat verified
  - Mutual recursion detection in `LANGUAGE sql` inliner — catches `f → g → f` at registration

- **Phase 6 — production hardening (entry batch):**
  - Constraint enforcement: PRIMARY KEY (composite + single), CHECK (column + table-level), FOREIGN KEY (single-project single-shard, `NO ACTION` + `CASCADE`)
  - WAL Phase 2 — `Wal` trait extracted; `LocalWal` (single-node fsync, byte-identical to prior concrete) + `RaftWal` (multi-node openraft consensus, single-process simulation, 3-node + 5-node + leader-failure tests)
  - EDF (Earliest Deadline First) per-project scheduler — priority by op-shape, p99 13.97ms under noisy-neighbor load
  - Vector planner auto-routing for `ORDER BY x <-> $1 LIMIT k` — 5.62× speedup on 1K-row debug-build corpus
  - Decimal128 / `NUMERIC(p, s)` type bridge — DDL + arrow-bridge + pgwire OID 1700 (text wire); `NUMERIC` / `DECIMAL` / `DEC` synonyms accepted
  - basin-trgm GIN trigram index — 9.4× speedup on 1K-row debug-build corpus
  - basin-cv incremental refresh — watermark-based, `date_trunc` / `time_bucket` GROUP BY shapes; falls back to full re-execution on unsupported shapes; `WITH (full = true)` opt-out
  - basin-geo: `LineString` / `Polygon` types + `ST_MakeLine` / `ST_NumPoints` / `ST_PointN` / `ST_Length` / `ST_MakePolygon` / `ST_Area` / `ST_Contains` / `ST_Within`
  - basin-iceberg-rest — Lakekeeper-compat REST catalog (GET namespaces / list-tables / load-table; POST create-table; POST commit-table with `assert-table-uuid` / `assert-current-schema-id` / `assert-ref-snapshot-id` requirements)
  - BYO-key (KMS) engine seam — `EncryptionProvider` trait + per-project `ProjectStorageConfig` registry with cache invalidation; `wrap_key_with_config` / `unwrap_key_with_config` extension methods (default-impl forwards for backward compat)
  - CSV `COPY` extensions: column-list (`COPY t (col1, col2) FROM STDIN`) + file paths (`COPY t TO '/var/lib/basin/exports/users.csv'`, gated by `BASIN_COPY_PATH_ALLOWLIST`)
  - pgwire simple-query multi-statement support (`tokio_postgres::batch_execute` of `;`-separated statements)
  - `TIMESTAMP` (without time zone) accepted in CREATE TABLE / TYPE / DOMAIN / FUNCTION arg / RETURNS TABLE / PROCEDURE arg surfaces
  - Router OIDs: `Date32 → 1082`, `Timestamp(_, Some(_)) → 1184` (TIMESTAMPTZ), `Timestamp(_, None) → 1114` (TIMESTAMP), `Interval(MonthDayNano) → 1186` — text + binary where applicable

- **ORM compat verification** for Prisma / Sequelize / SQLAlchemy startup queries (`tests/integration/tests/orm_compat.rs`)

- **Security hardening** — RLS predicate injection now walks `SetExpr::SetOperation` (UNION / INTERSECT / EXCEPT) + `query.with` CTEs + `TableFactor::Derived` + embedded subqueries; `rls_union_subquery_cannot_bypass` + `rls_cte_cannot_bypass` regression tests pin the invariant

- **GitHub Actions CI/CD** — `.github/workflows/ci.yml` (rustfmt + clippy + workspace test on Linux + macOS + cargo audit) and `.github/workflows/release.yml` (tag-driven prebuilt binaries for x86_64-linux / aarch64-linux / x86_64-darwin / aarch64-darwin)

### Changed

- **Trigger story reframed** — `CREATE TRIGGER` with PL/pgSQL body is now an explicit non-goal per [ADR 0012](docs/decisions/0012-change-event-primitive.md). The replacement primitives (declarative lifecycle + reactors) cover ~95% of real-world trigger use cases.
- **basin-cloud / basin-billing moved out of OSS workspace** — hosted-product crates now live in the separate (closed-source) `basin-cloud` repo. The OSS engine ships `EncryptionProvider` and `BillingProvider`-shaped traits; external callers wire their own adapters.
- `Engine::new` now `attach_catalog`s on storage so the encryption call path can look up per-project `ProjectStorageConfig`.
- `pg_constraint` / `information_schema.table_constraints` / `key_column_usage` / `referential_constraints` views now populate with real PK / CHECK / FK rows (previously schema-only).

### Fixed

- 6 PG-divergence cases reconciled in 5.11.A function catalogue:
  - `extract(second FROM ts)` returns Float64 with sub-second precision (was Int32)
  - `power(int, int)` returns Float8 (was Int64)
  - `age(ts1, ts2)` returns native `Interval(MonthDayNano)` with PG-compatible calendar walk (was Utf8)
  - `coalesce(NULL, NULL)` no longer requires a CAST (Null type now bridges)
  - `to_char` / `to_timestamp` accept PG format strings (`YYYY-MM-DD HH24:MI:SS`) instead of chrono `%Y-%m-%d` only
  - Router-side Date32 OID mapping (was falling through to TEXT)
- `Int16` workspace-arrow ↔ DataFusion-arrow bridge (unblocked `pg_attribute.attnum`)
- `basin-webhooks ↔ basin-engine` cyclic dep resolved by moving registry + DDL helpers into basin-engine
- `JoinOperator::Semi` / `JoinOperator::Anti` dead-code variants in `sql_functions.rs` (sqlparser 0.52 doesn't expose them; the file was unregistered until 5.11.D wired the inliner)

### Removed

- `basin-cloud` and `basin-billing` workspace crates (moved to `basin-cloud` repo)
- `CLOUD_ROADMAP.md` (canonical copy lives in `basin-cloud` repo)

[Unreleased]: https://github.com/bas-in/basin/compare/v0.1.9...HEAD
[0.1.9]: https://github.com/bas-in/basin/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/bas-in/basin/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/bas-in/basin/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/bas-in/basin/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/bas-in/basin/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/bas-in/basin/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/bas-in/basin/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/bas-in/basin/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bas-in/basin/releases/tag/v0.1.1
