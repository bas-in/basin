# Basin — migration-tool compatibility

Consolidated reference for using standard Postgres migration tools against Basin.
Each tool has a structured integration test in
`tests/integration/tests/migration_tool_<tool>.rs` that exercises the real CLI
against an in-process Basin server.

## Compatibility matrix

<!-- MIGRATION_MATRIX_BEGIN
     This section is currently authored from test-file analysis (Phase 5.25.G).
     See "CI-regeneration contract" below for the intended automation path. -->

| Tool | Supported | Caveats | Verified-on-version |
|------|-----------|---------|---------------------|
| Flyway | untested (needs flyway CLI in CI) | `SET search_path` must be accepted as no-op; `pg_advisory_lock(N)` must return a no-op row; `SHOW transaction_isolation` must be handled; `ALTER TABLE … ADD COLUMN … DEFAULT …` required by V4/V6/V9; composite `PRIMARY KEY (…, …)` required by V3/V8. Use `prisma migrate deploy` or `prisma db push` as a lighter alternative for CI. | — |
| golang-migrate | untested (needs golang-migrate CLI in CI) | `pg_advisory_lock(N)` / `pg_advisory_unlock(N)` must return no-op rows (highest-risk blocker); `SET search_path` must be accepted; `SELECT current_schema()` must return a scalar; `CREATE TABLE IF NOT EXISTS schema_migrations` must work (✅ shipped). Tracking table stores only the latest version, not a history row per migration. | — |
| Diesel | untested (needs diesel CLI + libpq in CI) | `__diesel_schema_migrations` created via `CREATE TABLE IF NOT EXISTS` (✅ shipped); `SHOW search_path` must be handled; `SET search_path` must be accepted; `pg_advisory_lock` present in some Diesel CLI versions; `JSONB` column type in DDL and wire encoding required (✅ shipped); `ALTER TABLE ADD COLUMN` required by migration 4 (✅ shipped). One row inserted per applied migration. | — |
| sqlx | untested (needs sqlx-cli in CI) | `_sqlx_migrations` created via `CREATE TABLE IF NOT EXISTS` (✅ shipped); `BYTEA` column for checksum (✅ shipped); `TIMESTAMPTZ` column for `installed_on` (✅ shipped); `SET search_path` must be accepted; `pg_advisory_lock` present in some sqlx-cli versions. Compile-time `query!` macro checking not exercised (requires live DB at build time or `sqlx-data.json` snapshot). `migrate revert` exits non-zero for up-only migrations by design. | — |
| Prisma | untested (needs Node ≥ 18 + prisma CLI in CI) | Must use `prisma db push` or `prisma migrate deploy` — `prisma migrate dev` requires `CREATE DATABASE` for its shadow-database mechanism, which Basin does not support. `pg_catalog` introspection heavily used: `pg_class`, `pg_attribute`, `pg_namespace`, `pg_constraint`, `pg_index`, `pg_enum`, `pg_type` (Basin coverage Phase 5.11.M — some edges may be incomplete). `SERIAL` / `autoincrement()` default reflection via `pg_attribute.atthasdef` required. FK rows in `pg_constraint` required for relation metadata. `_prisma_migrations` table created only by `migrate deploy`, not by `db push`. | — |

<!-- MIGRATION_MATRIX_END -->

## Per-tool notes

### Flyway

- **Fixture**: `tests/integration/fixtures/migration-tool-scaffold/flyway/sql/` — V1–V10 migrations covering users, teams, posts, comments, tags, post_tags, audit_log.
- **Tracking table**: `flyway_schema_history` (one row per applied migration, `success BOOLEAN`).
- **CI install**:
  ```yaml
  - name: Install Flyway
    run: |
      wget -qO- https://download.red-gate.com/maven/release/com/redgate/flyway/flyway-commandline/10.21.0/flyway-commandline-10.21.0-linux-x64.tar.gz \
        | tar xz -C /opt
      echo "/opt/flyway-10.21.0" >> $GITHUB_PATH
  ```
- **Run test**: `cargo test -p basin-integration-tests --test migration_tool_flyway -- --ignored`

### golang-migrate

- **Fixture**: `tests/integration/fixtures/migration-tool-scaffold/golang-migrate/` — 5 up/down migration pairs.
- **Tracking table**: `schema_migrations` (stores only the latest version: `version BIGINT`, `dirty BOOLEAN`).
- **CI install**:
  ```yaml
  - name: Install golang-migrate
    run: |
      curl -L https://github.com/golang-migrate/migrate/releases/download/v4.18.1/migrate.linux-amd64.tar.gz \
        | tar xz -C /usr/local/bin
  ```
- **Run test**: `cargo test -p basin-integration-tests --test migration_tool_golang_migrate -- --ignored`

### Diesel

- **Fixture**: `tests/integration/fixtures/migration-tool-scaffold/diesel/migrations/` — 4 migration directories including a JSONB column.
- **Tracking table**: `__diesel_schema_migrations` (one row per applied migration, `version VARCHAR(50) PRIMARY KEY`).
- **CI install**:
  ```yaml
  - name: Install Diesel CLI
    run: |
      sudo apt-get install -y libpq-dev
      cargo install diesel_cli --no-default-features --features postgres
  ```
- **Run test**: `cargo test -p basin-integration-tests --test migration_tool_diesel -- --ignored`

### sqlx

- **Fixture**: `tests/integration/fixtures/migration-tool-scaffold/sqlx/` — 5 up-only migration files.
- **Tracking table**: `_sqlx_migrations` (`version BIGINT`, `description TEXT`, `installed_on TIMESTAMPTZ`, `success BOOLEAN`, `checksum BYTEA`, `execution_time BIGINT`).
- **CI install**:
  ```yaml
  - name: Install sqlx-cli
    run: cargo install sqlx-cli --no-default-features --features native-tls,postgres
  ```
- **Run test**: `cargo test -p basin-integration-tests --test migration_tool_sqlx -- --ignored`

### Prisma

- **Fixture**: `tests/integration/fixtures/migration-tool-scaffold/prisma/schema.prisma` — 4 models (User, Team, Post, Comment with relations).
- **Tracking table**: none created by `db push`; `_prisma_migrations` is created only by `prisma migrate deploy`.
- **Limitation**: `prisma migrate dev` is unsupported because it requires `CREATE DATABASE` for shadow-database diffing. Use `prisma db push` (development) or `prisma migrate deploy` (production).
- **CI install**:
  ```yaml
  - name: Install Prisma CLI
    run: npm install -g prisma
  ```
- **Run test**: `cargo test -p basin-integration-tests --test migration_tool_prisma -- --ignored`

## CI-regeneration contract

### Current state (Phase 5.25.G)

The matrix above is **authored from static test-file analysis** — the tool CLIs are not installed in the dev environment or current CI image, so the tests all skip cleanly (they are `#[ignore]`'d and perform a runtime binary check before doing anything). The "Verified-on-version" column is "—" until a CI run completes with the CLIs installed.

### Intended mechanism

The spec (5.25.G) calls for this matrix to be *generated from test results, never hand-edited*, regenerated on every green CI run. The planned mechanism:

1. **A `migration_matrix` generator** — a small test or script (`tests/integration/tests/migration_matrix.rs` or a shell wrapper) that:
   - Runs each tool test (`--ignored`) against a Basin server.
   - Parses the structured JSON/text output produced by each test's print statements.
   - Emits the Markdown table above, replacing the `MIGRATION_MATRIX_BEGIN … MIGRATION_MATRIX_END` sentinel block in this file.
2. **A CI job** in `.github/workflows/ci.yml` (or a dedicated `migration-compat.yml`) that:
   - Installs each tool CLI (see per-tool CI install snippets above).
   - Runs `cargo test -p basin-integration-tests --test migration_tool_flyway --test migration_tool_golang_migrate --test migration_tool_diesel --test migration_tool_sqlx --test migration_tool_prisma -- --ignored`.
   - Runs the `migration_matrix` generator to refresh this file.
   - Commits the updated `docs/migration-tools.md` back to the branch, or fails the job if the committed matrix is stale.

This pattern mirrors the `sql-support.md` convention: run
`cargo test -p basin-integration-tests --test sql_support_matrix` to refresh
that matrix; the equivalent here will be
`cargo test -p basin-integration-tests --test migration_matrix`.

<!-- TODO(5.25.G): Implement migration_matrix generator once tool CLIs are
     added to the CI image. The contract above is the spec; the sentinels
     MIGRATION_MATRIX_BEGIN / MIGRATION_MATRIX_END delimit the block to replace. -->

### Structured result contract (for the generator)

Each per-tool test already prints machine-parseable status lines. The generator should capture:

```
[<tool>] PASSED          → Supported = "yes"
[<tool>] SKIP — …        → Supported = "untested (needs <tool> CLI in CI)"
panic: …                 → Supported = "with-caveats" or "no" depending on which assertion failed
```

The tool version is printed by each availability-check command (e.g. `flyway -version`, `migrate -version`, `diesel --version`, `sqlx --version`, `prisma --version`) and should be captured and stored as the "Verified-on-version" value.

---

*Regeneration: run `cargo test -p basin-integration-tests --test migration_matrix` once tool CLIs are in CI. Matrix is currently authored from test-file analysis (Phase 5.25.G, 2026-05-21).*
