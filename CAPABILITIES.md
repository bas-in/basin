# Basin — capabilities

Honest, public-facing description of what Basin does today, what's planned,
and what's not on the roadmap. If you're evaluating Basin for a real
workload, this is the right page to read first.

Cross-references: [`TASK.md`](./TASK.md) is the full Phase 0–7 core-DB
build plan, [`WEDGE.md`](./WEDGE.md) is the prioritized next-six-months
slice, [`docs/deployment.md`](./docs/deployment.md) is the production
deployment architecture guide, [`docs/decisions/`](./docs/decisions/)
records every "no" with the trigger that would change our mind.

Status legend: ✅ shipped · 🛠 in progress · ◻️ planned · 🚫 not on roadmap.

Coverage: every ✅ row above is exercised by [`tests/integration/tests/feature_coverage.rs`](./tests/integration/tests/feature_coverage.rs) (or its named cross-reference in that file's audit comment). Security invariants verified by [`tests/integration/tests/security.rs`](./tests/integration/tests/security.rs).

> **For the fine-grained per-syntax matrix derived from automated tests, see [`docs/sql-support.md`](./docs/sql-support.md).**

---

## Wire protocol

| Capability | Status | Notes |
|---|---|---|
| pgwire v3 | ✅ | startup + cleartext-password auth + simple query |
| Simple query (`Q` message) | ✅ | what `psql` types into the prompt; multi-statement bodies (semicolon-separated DDLs / SELECTs) are split at the router and dispatched in order, one `RowDescription`/`DataRow`/`CommandComplete` group per statement and one trailing `ReadyForQuery`. Powers `tokio_postgres::batch_execute` and `psql -f setup.sql`. Mid-batch failure stops dispatch (PG's non-transactional semicolon-batch behaviour); earlier successes stand. |
| Extended query (`Parse`/`Bind`/`Describe`/`Execute`/`Close`/`Sync`) | ✅ | full Parse/Bind/Describe/Execute/Close/Sync; `tokio-postgres::query` works |
| Binary parameter / result format | ✅ | INT2/4/8, FLOAT4/8, BOOL, BYTEA, TEXT, JSONB, UUID, FixedSizeBinary. JSONB / UUID parameter binding is native — `ParameterDescription` advertises OID 3802 / 2950 for INSERT / UPDATE / SELECT-WHERE slots whose target column carries `BASIN_TYPE=JSONB` / `BASIN_TYPE=UUID` metadata, and the Bind decoder understands both wire formats (JSONB v1: leading `0x01` version byte + canonical-form JSON; UUID: 16 raw RFC 4122 bytes). asyncpg / pgx / tokio-postgres encode `uuid.UUID` / `dict` / `serde_json::Value` directly without string coercion. |
| `COPY FROM STDIN` / `COPY TO STDOUT` | ✅ | CSV format only (RFC 4180-ish, comma-delimited; `WITH (FORMAT CSV [, HEADER true])`); BINARY / custom DELIMITER / NULL spec / column-list / file-path variants rejected with SQLSTATE 42601. COPY-IN imports row-by-row INSERTs against the engine; on a mid-stream error we drain to `CopyDone` before responding so the connection stays usable. Drives both simple-query and the extended-query path so `tokio_postgres::copy_in` / `copy_out` and `psql \copy` both work. |
| TLS | ✅ | rustls (aws-lc-rs) on the pgwire listener; static PEM cert + key via `BASIN_TLS_CERT_PATH` / `BASIN_TLS_KEY_PATH`. SSLRequest answered `'S'`/`'N'` and the socket wrapped before pgwire startup. mTLS / OCSP / cert rotation / ALPN selection deferred to v0.2. |
| `LISTEN` / `NOTIFY` | 🚫 | no pub/sub today |
| Replication protocol | 🚫 | not the right shape for object-store storage |

## SQL surface

| Capability | Status | Notes |
|---|---|---|
| Parser: libpg_query (ADR 0014, Phase 1) | 🛠 | `pg_query` (libpg_query vendored via Rust crate) is now the canonical SQL parse + statement-classification frontend. Every incoming statement is parsed by the real PostgreSQL 16 parser; unsupported kinds (LISTEN, NOTIFY, VACUUM, CREATE TRIGGER, BEGIN/COMMIT/ROLLBACK, etc.) are rejected with SQLSTATE 0A000 before sqlparser sees them. sqlparser-rs remains a transitional fallback for SELECT/DML/DDL node bodies during Phase 1. Textual pre-screens in `executor.rs` are being replaced by typed AST matches in Agents 2–4; Agent 5 makes the pg_query path unconditional. See [ADR 0014](./docs/decisions/0014-pg-query-as-canonical-parser.md). |
| `CREATE TABLE` | ✅ | int{2,4,8}, text/varchar, boolean, double, vector(N), JSONB, UUID, BYTEA |
| `INSERT … VALUES` (single + multi-row) | ✅ | string-quoted vector / JSONB / UUID literals supported |
| `SELECT` with `WHERE` (single table) | ✅ | DataFusion-planned; predicate pushdown to Parquet |
| `SHOW TABLES` | ✅ | per-tenant scoped |
| `ORDER BY` / `LIMIT` | ✅ | full DataFusion support |
| Joins (single-shard) | 🛠 | DataFusion handles them; not yet exercised in tests |
| `UPDATE` / `DELETE` | ✅ | Copy-on-write Iceberg v2. Single-scan partition; `replace_data_files` with optimistic concurrency on both catalog backends; physical deletion of replaced Parquet files. |
| `ALTER TABLE` | ✅ | `ADD COLUMN`, `SET cold_after`, `SET cold_age_column`, `SET BLOOM FILTERS ON`, `SET row_group_rows`, `RESET row_group_rows`, `CLUSTER BY (...)`, `RESET CLUSTER BY`, `ENABLE/DISABLE ROW LEVEL SECURITY`, `CREATE POLICY`, `DROP POLICY` |
| `GENERATED ALWAYS AS (expr) STORED` columns | ✅ | Phase 5.11.K. Expression evaluated on INSERT and re-evaluated on every UPDATE row. Direct writes rejected with SQLSTATE 42601. VIRTUAL deferred to v0.2. Self-reference rejected at registration. |
| `CREATE MATERIALIZED VIEW … WITH (basin.continuous, …)` | ✅ | Rust API ships via `basin-cv`; SQL surface lives in `basin-engine`'s `cv_ddl` module. Pre-screen lifts `WITH (basin.continuous, refresh_interval = '<duration>')` out before sqlparser, then dispatches to `Catalog::set_continuous_aggregate`. `REFRESH MATERIALIZED VIEW <name>` and `DROP MATERIALIZED VIEW <name>` round out the surface. |
| `CREATE TYPE … AS ENUM`, `CREATE DOMAIN` | ✅ | Phase 5.11.K2. Enums stored as `Utf8` + `BASIN_ENUM_TYPE` metadata; domains as base type + `BASIN_DOMAIN` metadata + CHECK enforced at write. `ALTER TYPE … ADD VALUE` append-only (matches PG). DROP cascade rejected if column references the type. `ORDER BY` / range comparisons on enum columns now follow PG declaration-order (planner rewrites the column reference to a `CASE`-on-ordinal expression at plan time). |
| `SELECT … AS OF SNAPSHOT n` / `AS OF TIMESTAMP ts` (time-travel) | ◻️ | Routing design lives in [`docs/architecture.md`](./docs/architecture.md) §7 — any `AS OF` clause forces the analytical path because shard owners only hold the current state. Parser support deferred to `basin-analytical` v0.2. |
| `CREATE POLICY` (RLS) | ✅ | predicate injection at logical-plan layer; cross-tenant leak invariant verified |
| `information_schema` + `pg_catalog` views (17) | ✅ | Phase 5.11.M complete. Shipped: `information_schema.tables`/`columns`/`routines`/`views`/`schemata`/`table_constraints`/`key_column_usage`/`referential_constraints` + `pg_catalog.pg_class`/`pg_attribute`/`pg_namespace`/`pg_proc`/`pg_type`/`pg_constraint`/`pg_index`/`pg_depend`/`pg_authid`. PostgREST / pgAdmin / Prisma / Sequelize / SQLAlchemy startup-query compat verified by `tests/integration/tests/postgrest_pgadmin_compat.rs` and `tests/integration/tests/orm_compat.rs`. `pg_constraint` / `table_constraints` / `key_column_usage` / `referential_constraints` populate with real PK/CHECK/FK rows from the constraint-enforcement work. `pg_depend` surfaces continuous-matview → source-table edges + function → arg/return type edges. `pg_authid` surfaces the calling tenant as a single role. `pg_index` populates when 5.7 B1 secondary indexes ship. |
| Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) | ◻️ | single-shard only when shipped. Drivers that implicitly send `BEGIN TRANSACTION READ WRITE` around prepared-statement bulk inserts (e.g. `lib/pq` with `tx.PrepareContext` → `stmt.ExecContext` loop) get rejected with `unsupported in PoC: BEGIN TRANSACTION READ WRITE`. Rewrite as multi-row `INSERT … VALUES (a,b),(c,d),…` until Phase 5. |
| `DROP TABLE [IF EXISTS]` | ◻️ | rejected with `unsupported in PoC: DROP TABLE …` (SQLSTATE XX000). DDL drop ships in Phase 5 alongside transactions. Use the dashboard's Tables editor (`/v1/projects/:ref/tables/:name` DELETE on the cloud) until then. |
| `CREATE TABLE IF NOT EXISTS` | 🛠 | the `IF NOT EXISTS` clause is parsed but **not honoured**; basin returns `catalog error: table … already exists` when the table exists. Wrap in a per-table existence check or swallow the "already exists" error script-side. Idempotent CREATE ships in Phase 5. |
| `INSERT … ON CONFLICT DO {NOTHING,UPDATE}` | 🛠 | clause is parsed but **silently ignored** in v0.1 — the INSERT runs as if the ON CONFLICT clause weren't there. Real conflict handling lands when secondary indexes ship (Phase 5.7 B1). |
| `UPDATE … SET col = <expression>` | 🛠 | RHS must be a literal or a single bind parameter (`SET name = $1`). Expressions like `SET name = name \|\| ' (updated)'` or `SET count = count + 1` get rejected with `expected literal of type Utf8 …`. Compute the new value client-side. |
| `UPDATE … WHERE col IN (SELECT …)` / `WHERE EXISTS (SELECT …)` | 🛠 | subquery in WHERE rejected with `WHERE clause not representable in v0.1`. Materialise the inner SELECT client-side and pass IDs as a bind-parameter list. |
| `SELECT *` Arrow projection on specific column-shapes | 🛠 | occasionally fails with `expected '32' at position N; got 'M'` on tables that mix `BIGSERIAL PK + TEXT UNIQUE + TIMESTAMPTZ DEFAULT` (root-caused to Arrow type-projection mismatch on the wire). Workaround: list columns explicitly. Tracked for v0.2. |
| psql `\dt` / `\d`-family meta-commands | ✅ | Phase 5.11.N: 20 pg_catalog scalar stubs registered — `pg_table_is_visible`, `pg_get_userbyid`, `pg_get_function_arguments/result/identity_arguments`, `pg_get_expr`, `pg_get_indexdef`, `format_type`, `pg_get_constraintdef`, `pg_total_relation_size`, `pg_table_size`, `pg_relation_size`, `obj_description`, `col_description`, `pg_get_partkeydef`, `pg_encoding_to_char`, `current_schema`, `current_schemas`, `has_table_privilege`, `has_schema_privilege`. Each registered under both bare and `pg_catalog.`-qualified names. Note: `current_schemas(bool)` returns the PG array-literal string `{pg_catalog,public}` rather than a true `text[]` because Basin's df↔ws arrow bridge does not yet support List types. |
| Prepared statements with parameter bind | ✅ | shipped with extended-query protocol |
| Foreign keys | ✅ | single-tenant single-shard. `REFERENCES users(id)` (column-level) and table-level `FOREIGN KEY (col) REFERENCES users(id)` both supported. `ON DELETE NO ACTION` (default; rejects parent DELETE if children exist) and `ON DELETE CASCADE` (recursive child DELETE via existing engine path). `SET NULL` / `SET DEFAULT` / `RESTRICT` rejected at CREATE. Cross-tenant FKs structurally blocked by tenant-scoped `Catalog::load_table`. Referenced columns must be PK (multi-column UNIQUE not yet shipped). |
| Triggers (declarative lifecycle + SQL-bodied reactors) | ✅ | **Reframed away from PL/pgSQL per [ADR 0012](./docs/decisions/0012-change-event-primitive.md).** Declarative lifecycle (Phase 5.11.B): `AUTO_UPDATE` columns, `AUDIT TO <table>` table option, `SOFT DELETE` columns with `INCLUDE DELETED` opt-out — covers ~75% of `audit_*` and `updated_at` trigger use cases. SQL-bodied reactors `ALTER TABLE … REACT ON {INSERT\|UPDATE\|DELETE} [WHEN (…)] EXECUTE <sql>` (Phase 5.11.C) — Tier 0 sink trait + capture point in `crates/basin-common::events`; pre-commit dispatch with `NEW`/`OLD`/`TG_OP`/`TG_TABLE_NAME` substitution; reactor failure rolls back the source mutation. Constraint reactors `REACT ON INSERT CONSTRAINT (predicate)` (Phase 5.11.C2) via the `__basin_assert(predicate, error_text)` UDF (works around DataFusion's eager CASE-eval); SQLSTATE 23514 on violation. `DROP REACTOR <name> ON <table>`. PL/pgSQL parser / interpreter explicitly out of scope. |
| User-defined functions (`CREATE FUNCTION … LANGUAGE sql`) | 🛠 | **Reframed away from PL/pgSQL per [ADR 0012](./docs/decisions/0012-change-event-primitive.md).** Basin's path is `LANGUAGE sql` functions, planning-time inlined into the call site (same trick PG uses for `LANGUAGE sql`). Catalog API (`Catalog::register_sql_function`) + planner inliner ✅ shipped (Phase 5.11.D foundations); `CREATE FUNCTION` SQL surface ✅ shipped (5.11.D). `RETURNS TABLE(col1 type1, …)` ✅ shipped (5.11.E) — table-position calls are inlined as derived sub-queries with `LATERAL` emitted when call-site args reference outer columns. `CALL` procedures (5.11.F) extend the same machinery. Out of scope: PL/pgSQL with `IF`/`LOOP`/variables, `EXCEPTION` blocks, cursor-driven loops; PG `RETURNS SETOF type` (single-column SRF; deferred to v0.2 if it surfaces). |
| `CREATE PROCEDURE` / `CALL` / `DROP PROCEDURE` | ✅ | Phase 5.11.F. `LANGUAGE sql` only (`LANGUAGE plpgsql` rejected with SQLSTATE 0A000 per ADR 0012). Multi-statement body — body statements drawn from `INSERT` / `UPDATE` / `DELETE` / `SELECT` / `CREATE TABLE` / `DROP TABLE`; nested `CALL` rejected at registration in v0.1. Call-site arguments substituted into the body before each statement runs through the standard engine pipeline (RLS rewrites, sequence rewrite, function inliner all apply). Sequential best-effort execution: a mid-procedure failure leaves prior statements committed — single-shard transactions land in Phase 5 and will tighten this. `CREATE PROCEDURE` recognised via textual pre-screen (sqlparser 0.52's native `Statement::CreateProcedure` parses only T-SQL `AS BEGIN … END`); `CALL` and `DROP PROCEDURE` use sqlparser's native AST nodes. |
| PG built-in functions (string, date/time, math, JSONB) | ✅ | Phase 5.11.A shipped via `basin-engine`'s ScalarUDF registry. Pre-existing: `digest`, `encode`/`decode`, `crypt`, `gen_random_uuid`, `gen_salt`, vector ops. New in 5.11.A: `now`, `current_timestamp`, `current_date`, `date_trunc`, `age` (returns native `interval`), `extract` (sub-second precision for `second` via Float64), `to_char` / `to_timestamp` (PG format strings, not chrono), `lower`/`upper`/`substring`/`trim`/`length`/`position`/`replace`/`regexp_replace`/`||`, `abs`/`ceil`/`floor`/`round`/`power` (always Float8)/`sqrt`/`mod`/`%`, `coalesce`/`nullif`/`greatest`/`least`/`is distinct from`. JSONB operators (`->`/`->>`/`#>`/`@>` etc) via DataFusion's JSON support. 6 PG-divergence cases reconciled; one residual flagged: Float64 vs PG `numeric` for sub-ULP `extract(second)` (closes when the Decimal128 arrow-bridge ships). |
| Extended `to_char` picture strings | ✅ | Datetime: `CC` (century), `Q` (quarter), `W` (week-in-month), `J` (Julian day), `DY` (abbreviated weekday), `Month`/`Day` (full names), `TZ` (timezone abbrev), `MS` (milliseconds), `FM` fill-mode prefix. Numeric: `9`/`0` digit placeholders, `.` decimal, `,`/`G` group separator, `D` decimal separator, `S` sign, `$` currency, `XXX` hex upper-case, `EEEE` scientific notation, `FM` fill-mode. Registered as `to_char(numeric, text)` overloads alongside the existing timestamp overload. |
| `to_date(text, format)` | ✅ | New UDF returning `Date32`; accepts the same PG picture strings as `to_char` / `to_timestamp`. Round-trips correctly with `to_char(date, 'YYYY-MM-DD')`. |
| `convert(bytea, src_encoding, dst_encoding)` | ✅ | Three-argument bytea→bytea encoding conversion. v0.1 supports UTF-8 → UTF-8 (identity pass-through); non-UTF-8 encodings raise an execution error. |
| `length(bytea)` | ✅ | Byte-length overload for `Binary` input (distinct from DF's `length(text)` which counts characters). |
| `bit_length(text)` / `octet_length(text)` | ✅ | DataFusion builtins; verified in `format_encoding` integration test. |
| `overlay(str PLACING new FROM start [FOR len])` | ✅ | DataFusion builtin SQL syntax; verified in `format_encoding` integration test. |
| `convert_from` / `convert_to` | ✅ | UTF-8 encode/decode UDFs (`basin-engine::string_dt_udf`); verified round-trip in `format_encoding` integration test. |
| `encode`/`decode` all formats | ✅ | `hex`, `base64`, `escape` verified in `format_encoding` integration test (round-trips + MD5 hash hex encoding). |
| `translate(str, from, to)` | ✅ | Per-character substitution UDF; char deletion when `to` is shorter than `from`. Verified in `format_encoding` test. |
| `quote_literal` / `quote_nullable` / `quote_ident` | ✅ | String-escaping UDFs; `quote_nullable` handles `NULL` input → `'NULL'` literal. Verified in `format_encoding` test. |
| `format(fmt, args...)` | ✅ | printf-style: `%s` (plain), `%I` (quoted ident), `%L` (quoted literal), `%%` (literal percent). Verified in `format_encoding` test. |
| `position(sub IN str)` | ✅ | SQL keyword form (DataFusion builtin); 1-indexed, 0 for not-found. Verified in `format_encoding` test. |

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
| `TIMESTAMP` (without time zone) | ✅ | Arrow `Timestamp(Microsecond, None)`; pgwire OID 1114; surfaces in `information_schema.columns.data_type` as `"timestamp without time zone"` |
| `NUMERIC` / `DECIMAL` | ✅ | Arrow `Decimal128(p, s)` (1 ≤ p ≤ 38, 0 ≤ s ≤ p). DDL accepts `NUMERIC`, `NUMERIC(p)`, `NUMERIC(p, s)`, `DECIMAL(...)` synonym. Wire format: text only (binary numeric encoding is varlena-shaped and deferred to v0.2; lenient drivers handle text fine). pgwire OID 1700; `information_schema.columns.data_type` = `"numeric"`. |
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
| BYO-key (KMS) | ✅ | Engine seam fully wired. `EncryptionProvider` trait in `basin-storage::encryption` + `Storage::attach_encryption_provider` (opt-in, default `None` = plaintext) + per-tenant `TenantStorageConfig` registry persisted via catalog (`Storage::set_tenant_storage_config`) + cache-invalidation on update. `wrap_key_with_config` / `unwrap_key_with_config` extension methods route per-tenant CMK refs (default-impl forwards to `wrap_key`/`unwrap_key` for backward compat). Writer envelope-encrypts the Parquet body with a fresh per-file AES-256-GCM data key + persists the wrapped key as a `<path>.wrapped` sidecar; reader transparently unwraps. **External callers plug in their own KMS adapter** — the OSS engine ships only the trait + per-tenant config + envelope-encryption hooks. |
| Tenant deletion (`O(file_count)`) | ✅ | `Storage::delete_tenant` is catalog-first paths + parallel orphan LIST + bulk DeleteObjects + drop_namespace; LocalFS ~4ms, high-latency S3-compatible store ~1.4-2.2s |
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
| Pluggable `object_store` in `basin-server` | ✅ | `BASIN_STORAGE_BACKEND=local|r2|s3|tigris` wires the runnable binary to local FS or S3-compatible object stores. The storage crate still accepts any `dyn ObjectStore` for embedding/tests. |
| **NVMe disk cache** | ✅ | LRU on local SSD; ~50ms cold S3 fetches → ~100µs warm SSD reads. Default-on. 101× speedup measured. |
| **Parquet page cache (RAM)** | ✅ | LRU of decoded RecordBatches; <1ms warm hits. Default-on. 7.24× speedup measured. |
| HTTP/2 toggle for S3 client | ✅ | `S3Config::http2_only`; useful on AWS S3 / Tigris / R2 over HTTPS |
| Iceberg-style catalog (in-memory) | ✅ | atomic appends, optimistic concurrency |
| Iceberg-style catalog (durable) | ✅ | Postgres-backed; survives restart. Multi-region replication direction: single-writer global PG with regional read replicas via PG logical replication — see [ADR 0010](./docs/decisions/0010-catalog-replication.md). |
| Point-in-time restore (catalog level) | 🛠 | `Catalog::rollback_to_snapshot(tenant, table, snapshot_id)` truncates history to ≤ target and rewinds the head pointer. InMemory + Postgres impls. v0.2 adds physical file GC for orphaned post-rollback files; cross-DML rollback waits on soft-delete (also v0.2). Project-wide variants (`list_snapshots_project_wide`, `diff_snapshots`, `rollback_to_snapshot_project_wide`) shipped for Migration Manager v0.2. |
| Per-tenant fair-share scheduler | ✅ | EDF (Earliest Deadline First) shipped on top of the cap=16 primitive. Priority by op-shape: HEAD/list/small-range/DELETE/COPY/full-GET → High (5ms deadline); PUT/multipart/large-range (≥256KiB) → Low (1000ms deadline). `CONSECUTIVE_DISPATCH_CAP` prevents one tenant from monopolising all 16 slots. Fairness obs: tenant B's 100 sequential HEADs against tenant A's 1000 bulk-ops → p50 5.83ms / p99 13.97ms. See [ADR 0008](./docs/decisions/0008-noisy-neighbor-fairness.md). |
| WAL (local/object-store backed) | ✅ | `LocalWal` is the production path today and `basin-server` can mirror the S3-compatible storage backend for WAL object storage. `RaftWal` implements the same trait with openraft in a single-process simulation cluster (3-node + leader-failure + quorum-commit tests). Cross-process networking and persisted Raft state are v0.2 work, not a current production durability boundary — see `crates/basin-wal/RAFT.md`. |
| Background compactor | 🛠 | merges small files; tier sweep + cold-data move shipped |
| Iceberg REST catalog (Lakekeeper compatibility) | ✅ | `basin-iceberg-rest` crate ships GET (namespaces, list-tables, load-table), POST `create-table`, POST `commit-table` (mapped Iceberg requirements: assert-table-uuid via UUIDv5(`tenant/table`), assert-current-schema-id, assert-ref-snapshot-id; mapped updates: add-snapshot + set-current-snapshot via existing `Catalog::append_data_files` optimistic-concurrency), and DELETE table. Drop into `basin-server` to expose. Other commit actions (add-schema, set-default-spec, remove-snapshots, set-properties, etc) return structured 501; `register-table` and overwrite-style commits explicitly v0.2. |
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
| Automatic planner routing of `ORDER BY x <-> $1 LIMIT k` | ✅ | Detected at planning time (single-table FROM, single ORDER BY of `<col> <distance_op> <literal_or_param>`, vector column, constant LIMIT, ASC direction). Routes to `Storage::vector_search` fast path with optional pushdown of column-equality WHERE predicates (over-fetch + post-filter). Falls back to brute-force on JOINs / set ops / unbounded LIMIT / unsupported predicate shapes — correctness preserved. Distance ops `<->` (L2), `<=>` (cosine), `<#>` (dot product) all routed. ~5.6× speedup on 1K-row debug-build corpus. |
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
| `TimescaleDB` continuous aggregates | **`basin-cv`** | ✅ | `CvSpec` + `CvRefresher::tick`; refresh_interval enforced; per-tenant materialization. **Incremental refresh shipped** for the `date_trunc(_, col)` / `time_bucket(_, col)` GROUP BY shape — only re-aggregates rows newer than the watermark plus the last partial bucket. Bodies without a detectable time-bucket fall back to full re-execution. `REFRESH MATERIALIZED VIEW … WITH (full=true)` opt-out for explicit full rebuild. SQL surface ships in `basin-engine::cv_ddl`. |
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
| S3 cross-region replication of data | ◻️ | "free" via bucket-level configuration on AWS S3, Tigris, R2, etc. |
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
| Bring-your-own-bucket | 🚫 | hosted-product concern; out of scope for this OSS workspace |
| Bring-your-own-key (KMS) | ✅ | Engine seam complete (see Multi-tenancy table). Per-tenant CMK routing via `TenantStorageConfig` + `wrap_key_with_config`. External callers wire their own KMS adapter — OSS engine ships the seam, not the adapter. |
| Billing integration | 🚫 | hosted-product concern; out of scope for this OSS workspace |

## Auth and REST API

| Capability | Status | Notes |
|---|---|---|
| `basin-auth` (signup, signin, magic-link, password reset, email verify, JWT, refresh) | ✅ | Requires SMTP at startup (fail-fast). Auth tables live in each tenant's own storage namespace under the `basin_auth` schema prefix — no reserved internal tenant, no loopback pgwire connection, no separate Postgres required. Auth state replicates automatically with tenant storage. JWT issued + verified per request. `BASIN_AUTH_ENABLED=1`. Optional override `BASIN_AUTH_CATALOG_DSN=<dsn>` points auth at a separate external Postgres instance for blast-radius isolation; not the default path. See [ADR 0005](./docs/decisions/0005-auth-system.md) and [ADR 0013](./docs/decisions/0013-auth-per-tenant-schema.md). |
| `auth.uid()` / `auth.role()` / `auth.jwt()` session functions | ✅ | SQL functions in the `auth` schema, populated from JWT claims at connection open. Use in RLS policies: `CREATE POLICY "own rows" ON items FOR ALL USING (owner_id = auth.uid())`. Both `auth.uid()` (schema-qualified) and `auth_uid()` (underscore) spellings accepted — the executor rewrites the schema-dot form before DataFusion sees it. Anonymous sessions return `NULL` / `'anon'` matching Supabase behaviour. |
| Per-tenant auth schema (ADR 0013) | ✅ | Auth data lives in each tenant's own Basin storage namespace (`basin_auth` schema prefix). No reserved internal tenant. No loopback pgwire connection. `EngineAuthStore` default; `PostgresAuthStore` available for external-Postgres override. |
| Self-routing pgwire credentials | ✅ | `pgwire_user` format `{tenant_id}_{hex}` (26-char ULID prefix). Credential validation parses tenant_id directly from the user field — no global cross-tenant lookup table. API keys embed the same tenant prefix. Old-format credentials (`tenant_<hex>`) are automatically migrated on first startup after upgrade. |
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
- **Fly.io + Tigris is the managed cloud.** Zero egress, ~5–30 ms
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
- **PL/Python, PL/Perl, or other alt-language stored procedures** → use Postgres. Basin's PL/pgSQL subset (Phase 5.11) covers the trigger / audit-row use case, not the embedded-Python data-science use case.

Basin's wedge is multi-tenant SaaS with audit-log workloads where storage
cost and per-tenant isolation dominate. If your shape doesn't match, the
above are honest recommendations.

---

*Last updated: 2026-05-12. This file is hand-maintained; PRs welcome.*
