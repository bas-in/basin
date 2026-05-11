# Changelog

All notable changes to Basin are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The pre-1.0 contract: minor versions can break public API; patch versions
are bug-fix only. Once the engine wedge ships to design partners we
graduate to 1.0 and the standard SemVer guarantees.

## [Unreleased]

### Added
- `auth.uid()`, `auth.role()`, `auth.jwt()` SQL session functions — Supabase-compatible;
  usable in RLS policies (`CREATE POLICY … USING (owner_id = auth.uid())`). Both
  `auth.uid()` and `auth_uid()` spellings work.
- Per-tenant auth schema — auth data now lives in each tenant's own Basin storage
  (like Supabase's `auth` schema per project). No reserved internal tenant, no loopback
  pgwire connection. See ADR 0013.
- Self-routing pgwire credentials — `pgwire_user` format now encodes `tenant_id` as a
  26-char ULID prefix, enabling credential validation without a global cross-tenant lookup.
- `AuthStore` trait — pluggable auth storage; `PostgresAuthStore` for external Postgres,
  `EngineAuthStore` (default) for in-process Basin storage. Zero external dependencies
  for open source deployments.

### Changed
- Basin-server startup order: auth initialises before pgwire (eliminates `DeferredAuthResolver`
  and `wait_for_pgwire_accept` polling loop).
- `BASIN_AUTH_CATALOG_DSN` is now an optional external Postgres override rather than the
  default loopback path.

### Migration
- Existing `pgwire_user` credentials in the old `tenant_<hex>` format are automatically
  rotated to the new `{tenant_id}_{hex}` format on first startup after upgrade.

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
  - 5.11.I: webhook fanout (`ALTER TABLE … SUBSCRIBE WEBHOOK …`) with retry queue, dead-letter, idempotency keys, per-tenant counters
  - 5.11.K: generated columns (`GENERATED ALWAYS AS (expr) STORED`)
  - 5.11.K2: `CREATE TYPE … AS ENUM` + `CREATE DOMAIN`; enum ordinal comparison via `ORDER BY` planner rewrite
  - 5.11.K3: sequences (`CREATE SEQUENCE` + `nextval` / `currval` / `setval` UDFs); multi-option grammar via textual pre-screen
  - 5.11.M: 17 `information_schema` + `pg_catalog` views (`tables` / `columns` / `routines` / `views` / `schemata` / `table_constraints` / `key_column_usage` / `referential_constraints` + `pg_class` / `pg_attribute` / `pg_namespace` / `pg_proc` / `pg_type` / `pg_constraint` / `pg_index` / `pg_depend` / `pg_authid`); PostgREST + pgAdmin + Prisma + Sequelize + SQLAlchemy startup-query compat verified
  - Mutual recursion detection in `LANGUAGE sql` inliner — catches `f → g → f` at registration

- **Phase 6 — production hardening (entry batch):**
  - Constraint enforcement: PRIMARY KEY (composite + single), CHECK (column + table-level), FOREIGN KEY (single-tenant single-shard, `NO ACTION` + `CASCADE`)
  - WAL Phase 2 — `Wal` trait extracted; `LocalWal` (single-node fsync, byte-identical to prior concrete) + `RaftWal` (multi-node openraft consensus, single-process simulation, 3-node + 5-node + leader-failure tests)
  - EDF (Earliest Deadline First) per-tenant scheduler — priority by op-shape, p99 13.97ms under noisy-neighbor load
  - Vector planner auto-routing for `ORDER BY x <-> $1 LIMIT k` — 5.62× speedup on 1K-row debug-build corpus
  - Decimal128 / `NUMERIC(p, s)` type bridge — DDL + arrow-bridge + pgwire OID 1700 (text wire); `NUMERIC` / `DECIMAL` / `DEC` synonyms accepted
  - basin-trgm GIN trigram index — 9.4× speedup on 1K-row debug-build corpus
  - basin-cv incremental refresh — watermark-based, `date_trunc` / `time_bucket` GROUP BY shapes; falls back to full re-execution on unsupported shapes; `WITH (full = true)` opt-out
  - basin-geo: `LineString` / `Polygon` types + `ST_MakeLine` / `ST_NumPoints` / `ST_PointN` / `ST_Length` / `ST_MakePolygon` / `ST_Area` / `ST_Contains` / `ST_Within`
  - basin-iceberg-rest — Lakekeeper-compat REST catalog (GET namespaces / list-tables / load-table; POST create-table; POST commit-table with `assert-table-uuid` / `assert-current-schema-id` / `assert-ref-snapshot-id` requirements)
  - BYO-key (KMS) engine seam — `EncryptionProvider` trait + per-tenant `TenantStorageConfig` registry with cache invalidation; `wrap_key_with_config` / `unwrap_key_with_config` extension methods (default-impl forwards for backward compat)
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
- `Engine::new` now `attach_catalog`s on storage so the encryption call path can look up per-tenant `TenantStorageConfig`.
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

[Unreleased]: https://github.com/bas-in/basin/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/bas-in/basin/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/bas-in/basin/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bas-in/basin/releases/tag/v0.1.1
