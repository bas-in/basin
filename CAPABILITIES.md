# Basin — capabilities

Honest, public-facing description of what Basin does today, what's planned,
and what's not on the roadmap. If you're evaluating Basin for a real
workload, this is the right page to read first.

Cross-references: [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md) is the
v0.1 cut-off (what ships in v0.1, what's parked, unparking triggers),
[`TASK.md`](./TASK.md) is the full Phase 0–7 core-DB build plan,
[`WEDGE.md`](./WEDGE.md) is the prioritized next-six-months slice,
[`docs/deployment.md`](./docs/deployment.md) is the production deployment
architecture guide, [`docs/decisions/`](./docs/decisions/) records every
"no" with the trigger that would change our mind.

> **v0.1 scope status.** Every row below carries a per-feature status (✅ /
> 🛠 / ◻️ / 🚫). For the higher-order question — "is this on the v0.1 cut
> path or PARKED for v0.2+?" — read [`docs/V0_1_SCOPE.md`](./docs/V0_1_SCOPE.md).
> Crates flagged as frozen (basin-geo, basin-trgm, basin-cron, basin-net,
> basin-fn, basin-webhooks) and features flagged as maintenance-mode
> (basin-realtime extensions, FTS expansion, citext rewrite, etc.) are
> stable but receive no further v0.1 investment.

**Get started:** [5-Minute Docker Quickstart](./docs/quickstart-docker.md) · [Getting Started / Tutorial](./docs/tutorial.md) · [Multi-tenant SaaS sample app](./examples/saas-starter/) · [AI/RAG sample app](./examples/ai-rag-app/)

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
| `COPY FROM STDIN` / `COPY TO STDOUT` | ✅ | CSV format (`WITH (FORMAT CSV [, HEADER true] [, DELIMITER 'c'] [, NULL 's'] [, QUOTE 'c'] [, ESCAPE 'c'])`) **and the PG binary COPY format** (`WITH (FORMAT BINARY)` — `PGCOPY` header / length-prefixed fields / `0xFFFF` trailer, reusing the pgwire binary param+result codecs; int2/4/8, float4/8, bool, text, bytea, jsonb, uuid, timestamp[tz], date, numeric). Column lists (`COPY t (a, b) FROM STDIN` — reorder + DEFAULT/NULL fill like INSERT) and double-quoted identifiers (the `sqlx::PgCopyIn` shape `COPY "users" (id, email) FROM STDIN`) are supported, as are query-source `COPY (SELECT …) TO STDOUT` and env-gated server-side file paths. COPY-IN streams through a batched Arrow ingest fast path; on a mid-stream error we drain to `CopyDone` before responding so the connection stays usable. Drives both simple-query and the extended-query path so `tokio_postgres::copy_in` / `copy_out` and `psql \copy` both work. Unsupported-in-binary column types (vectors, intervals, arrays) reject with `0A000` naming the column; `FORMAT text` rejects with `42601`. |
| TLS | ✅ | rustls (aws-lc-rs) on the pgwire listener; static PEM cert + key via `BASIN_TLS_CERT_PATH` / `BASIN_TLS_KEY_PATH`. SSLRequest answered `'S'`/`'N'` and the socket wrapped before pgwire startup. mTLS / OCSP / cert rotation / ALPN selection deferred to v0.2. |
| `LISTEN` / `NOTIFY` / `UNLISTEN` | ✅ | SQL-level pub/sub via a per-engine notify registry (`notify_registry.rs`), dispatched in `executor.rs`. PG-accurate transaction buffering: `NOTIFY` inside a transaction is queued and fanned out only on `COMMIT`, discarded on `ROLLBACK`; channel names are case-insensitive; `pg_listening_channels()` reflects session state. Covered by `tests/integration/tests/listen_notify.rs`. (For change-data fan-out to external clients, basin-realtime SSE + WebSocket is the complementary higher-level surface.) |
| Realtime SSE (`GET /realtime/v1/sse/:project/:table`) | ✅ | Phase 5.11.R2 (`800d6d2`). Row-change stream over Server-Sent Events per table; bearer-JWT auth; compatible with `EventSource`. |
| Realtime WebSocket (`GET /realtime/v1/ws/:project`) | ✅ | Phase 5.11.R3 (`10858d8`). Multiplexed subscribe/unsubscribe over a single WS connection; JSON envelope with `type`, `table`, `payload`. |
| Presence channels (track/untrack/presence_state/presence_diff) | ✅ | Phase 5.11.R4 (`cdf3f99`). Per-project presence rooms; `track`, `untrack`, `presence_state`, `presence_diff` messages over the WS channel. |
| Subscriber-side filter pushdown | ✅ | Phase 5.11.R5. `subscribe_filtered` predicate evaluated in ≤ 50 µs at the sink before bytes hit the wire. |
| Per-project realtime memory budget | ✅ | Phase 5.11.R6. `O(bytes)` per-project `BudgetTracker`; back-pressure on over-budget projects without affecting others. |
| Replication protocol | 🚫 | not the right shape for object-store storage |

## SQL surface

| Capability | Status | Notes |
|---|---|---|
| Parser: libpg_query (ADR 0014, Phase 1) | 🛠 | `pg_query` (libpg_query vendored via Rust crate) is now the canonical SQL parse + statement-classification frontend. Every incoming statement is parsed by the real PostgreSQL 16 parser; unsupported kinds (VACUUM, CREATE TRIGGER, CLUSTER, REINDEX, etc.) are rejected with SQLSTATE 0A000 before sqlparser sees them. sqlparser-rs remains a transitional fallback for SELECT/DML/DDL node bodies during Phase 1. Textual pre-screens in `executor.rs` are being replaced by typed AST matches in Agents 2–4; Agent 5 makes the pg_query path unconditional. See [ADR 0014](./docs/decisions/0014-pg-query-as-canonical-parser.md). |
| `CREATE TABLE` | ✅ | int{2,4,8}, text/varchar, boolean, double, vector(N), JSONB, UUID, BYTEA |
| `INSERT … VALUES` (single + multi-row) | ✅ | string-quoted vector / JSONB / UUID literals supported. Multi-row literal INSERTs route through a fast VALUES scanner that bypasses full AST parsing; the scanner admits int/float/text/bool plus **JSONB documents** (canonicalized identically to the slow path), **timestamp literals** (`803d8a5`), and type-matching **`::jsonb` / `::timestamp` / `::timestamptz` suffix casts** on string literals (`749ab7b` — the shape the published bulk-INSERT benchmark sends), declining to the slow path on any uncertainty — byte-level fast-vs-slow equivalence is pinned by `values_fast_ingest.rs`. |
| `SELECT` with `WHERE` (single table) | ✅ | DataFusion-planned; predicate pushdown to Parquet |
| `SHOW TABLES` | ✅ | per-project scoped |
| `ORDER BY` / `LIMIT` | ✅ | full DataFusion support |
| Joins (single-shard) | 🛠 | DataFusion handles them; not yet exercised in tests |
| `UPDATE` / `DELETE` | ✅ | Copy-on-write Iceberg v2. Single-scan partition; `replace_data_files` with optimistic concurrency on both catalog backends; physical deletion of replaced Parquet files. Rewrites encode with the Fast cascade and skip before/after event capture when no sinks / audit / generated columns / RETURNING consume it (`7934575`) — background compaction keeps the Best cascade. |
| `ALTER TABLE` | ✅ | `ADD COLUMN`, `SET cold_after`, `SET cold_age_column`, `SET BLOOM FILTERS ON`, `SET row_group_rows`, `RESET row_group_rows`, `CLUSTER BY (...)`, `RESET CLUSTER BY`, `ENABLE/DISABLE ROW LEVEL SECURITY`, `CREATE POLICY`, `DROP POLICY` |
| `GENERATED ALWAYS AS (expr) STORED` columns | ✅ | Phase 5.11.K. Expression evaluated on INSERT and re-evaluated on every UPDATE row. Direct writes rejected with SQLSTATE 42601. VIRTUAL deferred to v0.2. Self-reference rejected at registration. |
| `CREATE MATERIALIZED VIEW … WITH (basin.continuous, …)` | ✅ | Rust API ships via `basin-cv`; SQL surface lives in `basin-engine`'s `cv_ddl` module. Pre-screen lifts `WITH (basin.continuous, refresh_interval = '<duration>')` out before sqlparser, then dispatches to `Catalog::set_continuous_aggregate`. `REFRESH MATERIALIZED VIEW <name>` and `DROP MATERIALIZED VIEW <name>` round out the surface. |
| `CREATE TYPE … AS ENUM`, `CREATE DOMAIN` | ✅ | Phase 5.11.K2. Enums stored as `Utf8` + `BASIN_ENUM_TYPE` metadata; domains as base type + `BASIN_DOMAIN` metadata + CHECK enforced at write. `ALTER TYPE … ADD VALUE` append-only (matches PG). DROP cascade rejected if column references the type. `ORDER BY` / range comparisons on enum columns now follow PG declaration-order (planner rewrites the column reference to a `CASE`-on-ordinal expression at plan time). |
| `SELECT … AS OF SNAPSHOT n` / `AS OF TIMESTAMP ts` (time-travel) | ◻️ | Routing design lives in [`docs/architecture.md`](./docs/architecture.md) §7 — any `AS OF` clause forces the analytical path because shard owners only hold the current state. Parser support deferred to `basin-analytical` v0.2. |
| `CREATE POLICY` (RLS) | ✅ | predicate injection at logical-plan layer; cross-project leak invariant verified |
| `information_schema` + `pg_catalog` views (17) | ✅ | Phase 5.11.M complete. Shipped: `information_schema.tables`/`columns`/`routines`/`views`/`schemata`/`table_constraints`/`key_column_usage`/`referential_constraints` + `pg_catalog.pg_class`/`pg_attribute`/`pg_namespace`/`pg_proc`/`pg_type`/`pg_constraint`/`pg_index`/`pg_depend`/`pg_authid`. PostgREST / pgAdmin / Prisma / Sequelize / SQLAlchemy startup-query compat verified by `tests/integration/tests/postgrest_pgadmin_compat.rs` and `tests/integration/tests/orm_compat.rs`. `pg_constraint` / `table_constraints` / `key_column_usage` / `referential_constraints` populate with real PK/CHECK/FK rows from the constraint-enforcement work. `pg_depend` surfaces continuous-matview → source-table edges + function → arg/return type edges. `pg_authid` surfaces the calling project as a single role. `pg_index` populates with real index rows (5.7 B1 shipped, `33a8162`), and `pg_sequence` + `pg_enum` are now populated (`30ea4f3`) — closing the introspection surface ORMs (Prisma, Drizzle, SQLAlchemy, ActiveRecord, sqlx, Django, Hibernate) probe at startup / migration time. Wire-level ORM flow compatibility is gated by `tests/integration/tests/orm_compat.rs` (2026-06-11 run: Drizzle 100%, sqlx/Diesel 95%, TypeORM 94%, Prisma 90%; 94/99 overall, all failures typed errors, 0 regressions). |
| Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) | ✅ | Single-shard, snapshot-isolated. An explicit transaction takes **snapshot-stable reads** (`530ec82`): repeated SELECTs of the same data inside one transaction return the same answer even as other sessions commit. The hot-tier MVCC sequence (`9f5b7f0`) prevents another session's overlay writes from leaking into an open transaction. Memtable entries keep **MVCC version chains** (`8d9fc2d`): a pinned snapshot read keeps being served its own version across any number of subsequent overwrites by other sessions — closing the prior single-version residual where a second overwrite could push a pinned reader back to the cold pre-image. In-tx single-row DML routes through a **transaction-scoped overlay fast path** (`62e5011`); untouched-table in-tx reads are served from a pinned snapshot without re-planning through DataFusion (`f5d1d6f`). Cross-shard 2PC is v0.2 (ADR 0011). **Caveat:** a cold in-transaction UPDATE that has not been touched by a fast path still routes through the cold catalog-commit path; the snapshot-stable view is established at the table's *first* in-tx read (a concurrent overlay committed before that first read is visible, after it is not). |
| `DROP TABLE [IF EXISTS]` | ✅ | catalog deletes the table; physical files removed by `Storage::delete_table`. Verified by `drop_table_removes_existing_table` in `crates/basin-engine/src/lib.rs`. `IF EXISTS` swallows the not-found case. |
| `CREATE TABLE IF NOT EXISTS` | ✅ | `if_not_exists: true` honoured; second CREATE is a no-op and returns `CREATE TABLE` with no error. Verified by `create_table_if_not_exists_creates_when_absent` / `create_table_if_not_exists_noop_when_exists` in `crates/basin-engine/src/lib.rs`. |
| `INSERT … ON CONFLICT DO {NOTHING,UPDATE}` | ✅ | `DO NOTHING` suppresses UNIQUE violations on the conflict-target match (#75); `DO UPDATE SET … = EXCLUDED.col` resolves `table.col` + `EXCLUDED.col` references (#74). v0.1 coverage limited to single-column conflict targets that map to a UNIQUE / PRIMARY KEY constraint; multi-column composite conflict targets are still partial (tighten in v0.2 with composite index support). |
| `UPDATE … SET col = <expression>` | 🛠 | RHS must be a literal or a single bind parameter (`SET name = $1`). Expressions like `SET name = name \|\| ' (updated)'` or `SET count = count + 1` get rejected with `expected literal of type Utf8 …`. Compute the new value client-side. |
| `UPDATE … WHERE col IN (SELECT …)` / `WHERE EXISTS (SELECT …)` | 🛠 | subquery in WHERE rejected with `WHERE clause not representable in v0.1`. Materialise the inner SELECT client-side and pass IDs as a bind-parameter list. |
| `SELECT *` Arrow projection on specific column-shapes | 🛠 | occasionally fails with `expected '32' at position N; got 'M'` on tables that mix `BIGSERIAL PK + TEXT UNIQUE + TIMESTAMPTZ DEFAULT` (root-caused to Arrow type-projection mismatch on the wire). Workaround: list columns explicitly. Tracked for v0.2. |
| psql `\dt` / `\d`-family meta-commands | ✅ | Phase 5.11.N: 20 pg_catalog scalar stubs registered — `pg_table_is_visible`, `pg_get_userbyid`, `pg_get_function_arguments/result/identity_arguments`, `pg_get_expr`, `pg_get_indexdef`, `format_type`, `pg_get_constraintdef`, `pg_total_relation_size`, `pg_table_size`, `pg_relation_size`, `obj_description`, `col_description`, `pg_get_partkeydef`, `pg_encoding_to_char`, `current_schema`, `current_schemas`, `has_table_privilege`, `has_schema_privilege`. Each registered under both bare and `pg_catalog.`-qualified names. Note: `current_schemas(bool)` returns the PG array-literal string `{pg_catalog,public}` rather than a true `text[]` because Basin's df↔ws arrow bridge does not yet support List types. |
| Prepared statements with parameter bind | ✅ | shipped with extended-query protocol. Bound statements **execute without re-parsing** on each call (`333861b`) — the parsed/planned form is cached against the statement name. Parameter types are inferred through UDF argument positions (`38d7d0e`) for clients (e.g. drivers that send untyped `Parse`) that don't pre-declare them. |
| Foreign keys | ✅ | single-project single-shard. `REFERENCES users(id)` (column-level) and table-level `FOREIGN KEY (col) REFERENCES users(id)` both supported. `ON DELETE NO ACTION` (default; rejects parent DELETE if children exist) and `ON DELETE CASCADE` (recursive child DELETE via existing engine path — **caveat:** the multi-level cascade-recursion shape is recorded as a gap in the differential bench and is being hardened). `SET NULL` / `SET DEFAULT` / `RESTRICT` rejected at CREATE. Cross-project FKs structurally blocked by project-scoped `Catalog::load_table`. Referenced columns must be PK (multi-column UNIQUE not yet shipped). |
| Triggers (declarative lifecycle + SQL-bodied reactors) | ✅ | **Reframed away from PL/pgSQL per [ADR 0012](./docs/decisions/0012-change-event-primitive.md).** Declarative lifecycle (Phase 5.11.B): `AUTO_UPDATE` columns, `AUDIT TO <table>` table option, `SOFT DELETE` columns with `INCLUDE DELETED` opt-out — covers ~75% of `audit_*` and `updated_at` trigger use cases. SQL-bodied reactors `ALTER TABLE … REACT ON {INSERT\|UPDATE\|DELETE} [WHEN (…)] EXECUTE <sql>` (Phase 5.11.C) — Tier 0 sink trait + capture point in `crates/basin-common::events`; pre-commit dispatch with `NEW`/`OLD`/`TG_OP`/`TG_TABLE_NAME` substitution; reactor failure rolls back the source mutation. Constraint reactors `REACT ON INSERT CONSTRAINT (predicate)` (Phase 5.11.C2, `59e8a10`) via the `__basin_assert(predicate, error_text)` UDF (works around DataFusion's eager CASE-eval); SQLSTATE 23514 on violation. `DROP REACTOR <name> ON <table>`. PL/pgSQL parser / interpreter explicitly out of scope. |
| User-defined functions (`CREATE FUNCTION … LANGUAGE sql`) | ✅ | **Reframed away from PL/pgSQL per [ADR 0012](./docs/decisions/0012-change-event-primitive.md).** Basin's path is `LANGUAGE sql` functions, planning-time inlined into the call site (same trick PG uses for `LANGUAGE sql`). Catalog API (`Catalog::register_sql_function`) + planner inliner ✅ shipped (Phase 5.11.D foundations); `CREATE FUNCTION` SQL surface ✅ shipped (5.11.D). `RETURNS TABLE(col1 type1, …)` ✅ shipped (5.11.E) — table-position calls are inlined as derived sub-queries with `LATERAL` emitted when call-site args reference outer columns. `CALL` procedures (5.11.F) extend the same machinery. Out of scope: PL/pgSQL with `IF`/`LOOP`/variables, `EXCEPTION` blocks, cursor-driven loops; PG `RETURNS SETOF type` (single-column SRF; deferred to v0.2 if it surfaces). |
| `CREATE FUNCTION … LANGUAGE wasm` (WASM UDFs) | ✅ | Phase 5.11.J (`fa65bcd`). Wasmtime-backed execution (epoch-interrupted, CPU-deadline + memory-capped per call); `.wasm` module registered in catalog; call-site dispatch via `ScalarUDF` bridge. Same `CREATE FUNCTION` / `DROP FUNCTION` DDL surface as `LANGUAGE sql`. Arg + return types: `i32` / `i64` / `f64` natively, plus `text` / `bytea` / `timestamptz` over a `basin_alloc`/`basin_dealloc` linear-memory `(ptr,len)` ABI (JSONB rides the `text` path). Per-row invocation; vectorized args and a first-class `jsonb` type are deferred — see caveat below. |
| `CREATE PROCEDURE` / `CALL` / `DROP PROCEDURE` | ✅ | Phase 5.11.F. `LANGUAGE sql` only (`LANGUAGE plpgsql` rejected with SQLSTATE 0A000 per ADR 0012). Multi-statement body — body statements drawn from `INSERT` / `UPDATE` / `DELETE` / `SELECT` / `CREATE TABLE` / `DROP TABLE`; nested `CALL` rejected at registration in v0.1. Call-site arguments substituted into the body before each statement runs through the standard engine pipeline (RLS rewrites, sequence rewrite, function inliner all apply). Sequential best-effort execution: a mid-procedure failure leaves prior statements committed — single-shard transactions land in Phase 5 and will tighten this. `CREATE PROCEDURE` recognised via textual pre-screen (sqlparser 0.52's native `Statement::CreateProcedure` parses only T-SQL `AS BEGIN … END`); `CALL` and `DROP PROCEDURE` use sqlparser's native AST nodes. |
| PG built-in functions (string, date/time, math, JSONB) | ✅ | Phase 5.11.A shipped via `basin-engine`'s ScalarUDF registry. Pre-existing: `digest`, `encode`/`decode`, `crypt`, `gen_random_uuid`, `gen_salt`, vector ops. New in 5.11.A: `now`, `current_timestamp`, `current_date`, `date_trunc`, `age` (returns native `interval`), `extract` (sub-second precision for `second` via Float64), `to_char` / `to_timestamp` (PG format strings, not chrono), `lower`/`upper`/`substring`/`trim`/`length`/`position`/`replace`/`regexp_replace`/`||`, `abs`/`ceil`/`floor`/`round`/`power` (always Float8)/`sqrt`/`mod`/`%`, `coalesce`/`nullif`/`greatest`/`least`/`is distinct from`. JSONB operators (`->`/`->>`/`#>`/`@>` etc) via DataFusion's JSON support. 6 PG-divergence cases reconciled; one residual flagged: Float64 vs PG `numeric` for sub-ULP `extract(second)` (closes when the Decimal128 arrow-bridge ships). |
| PostgreSQL array functions and operators | ✅ | Phase 5.11.Q. DataFusion 44 ships `datafusion-functions-nested` with `nested_expressions` enabled by default, providing: constructors (`ARRAY[…]` 1-D, `make_array(make_array(…))` nested); accessors/size (`array_length`, `cardinality`, `array_ndims`, `array_dims`); subscript via `array_element(arr, n)` (1-based); slice via `array_slice(arr, lo, hi)`; mutators `array_append`, `array_prepend`, `array_cat` / `array_concat`, `array_remove`, `array_replace`, `array_position`, `array_positions`; membership `array_has` (= ANY semantics), `array_has_all` (= ALL / @> semantics), `array_has_any` (&& overlap semantics); operators `@>` / `<@` type-discriminated (List types → `array_has_all`; range types handled separately); set-returning `unnest`; `array_to_string` / `string_to_array`. `array_repeat` is the DataFusion equivalent of PG `array_fill`. Gaps: `array_lower` / `array_upper` have no native UDF (always 1 / `array_length` for 1-based arrays); array column types (`TEXT[]`, `INT[]`) in `CREATE TABLE` DDL are not yet supported in the arrow schema bridge. Verified by `tests/integration/tests/array_fns.rs`. |
| JSONB GIN indexing (`CREATE INDEX … USING gin`) | 🛠 | Phase 5.19. `CREATE INDEX … USING gin (col)` and `CREATE INDEX … USING gin (col jsonb_path_ops)` parse and persist the opclass in the catalog. `crates/basin-engine/src/index_probe.rs` maintains an in-RAM posting list per `(table, col)` and implements containment probe (`@>`, `<@`) with posting-list file pruning; the dedup-(term, file) rework (`9d197ce`) made the posting lists survive 1M-row backfills. **Measured (2026-06-11 integrity run):** GIN effectiveness is restored at 10k — `@>` drops 10.8 ms → 2.52 ms with the index (4.3× effectiveness, ≈ PG's 2.35 ms with GIN). At 1M, effectiveness is 1.12× (269 → 240 ms) pending the card-order overlay-interaction fix — while hot-tier overlays from earlier card shapes are live the prune declines, correctness-first by design. |
| Full-text search (`tsvector` / `tsquery` / `@@`) | 🛠 | Phase 5.20. `TSVECTOR` and `TSQUERY` column types (Arrow `Utf8` + `BASIN_TYPE` sidecar metadata); `to_tsvector([config,] text)` with Snowball English stemming + stop-word filtering; `to_tsquery`, `plainto_tsquery`, `phraseto_tsquery`; `@@` match operator (lowered to `tsvector_match_udf`); `ts_rank` / `ts_rank_cd` (simplified deterministic score — matched-distinct-lexemes / vector-length, NOT PG cover-density). **Caveats:** `ts_rank_cd` reuses the simplified `ts_rank` formula; `ts_headline`, weighted vectors (`setweight`, A–D classes), and language configs beyond `english`/`simple` are out of scope. GIN-on-tsvector (5.20.E, wired): `CREATE INDEX … USING gin (tsvector_col)` populates an in-RAM lexeme posting list (`crates/basin-storage/src/index/gin_tsvector.rs`) on INSERT and via a CREATE INDEX backfill over pre-existing files (the backfill settles any live hot-tier overlay first); `@@` probes it structurally — `&`/`<->` intersect candidate file sets, `|` unions them, `!`/unknown lexemes decline to a full scan — with file/row-group pruning and a provably-empty short-circuit, both gated on no-live-overlay + per-file completeness (any un-indexed live file, e.g. after posting-budget eviction or compaction, degrades that table to a full scan — correct, just unpruned; the budget knob is `BASIN_GIN_POSTING_BUDGET`, shared with the JSONB GIN registry). `to_tsquery` lexemes are Snowball-stemmed through the same pipeline as `to_tsvector` (PG parity: `to_tsquery('english','runs')` matches a `running` document; raw `::tsquery` casts stay unstemmed). End-to-end harness in `tests/integration/tests/fts_harness.rs` (all slices live, including adversarial 5.20.E pruning tests). |
| `SET statement_timeout` / `lock_timeout` / `idle_in_transaction_session_timeout` | 🛠 | Phase 5.28. All three GUCs are implemented and compile. `statement_timeout`: wired end-to-end — per-session override via `SET statement_timeout = '500ms'`, read by `session_statement_timeout()` in every `execute()` call; SQLSTATE `57014` on breach. `lock_timeout`: GUC stored in `SessionState::lock_timeout`; cooperative cancellation primitive in `crates/basin-shard/src/lock_wait.rs` (`bounded_lock_wait`) maps expiry → SQLSTATE `55P03`. `idle_in_transaction_session_timeout`: GUC stored in `SessionState`; background `SessionReaper` task in `crates/basin-engine/src/session_reaper.rs` flags idle-in-txn sessions → SQLSTATE `25P03`. **Caveat:** end-to-end integration harness (`tests/integration/tests/timeout_trio_harness.rs`) tests are all `#[ignore]`-gated — implementations merged and compile but the full harness slices are pending un-ignore after verification. |
| `EXPLAIN ANALYZE` (real per-node timing) + `pg_stat_activity` + `pg_locks` | ✅ | Phase 5.23. `EXPLAIN ANALYZE <stmt>` executes the query via DataFusion's `.explain(verbose, analyze=true)` and reports real per-node actual-time and actual-rows through the standard `QUERY PLAN` column. `pg_stat_activity`: live `TableProvider` backed by a process-wide `ConnectionRegistry`; per-project scoped — a user sees only their own project's sessions. `pg_locks`: live `TableProvider` backed by `LockRegistry`; per-project scoped — surfaces current held/waiting locks for the calling project. Both views registered under `pg_catalog.*` and accessed transparently from pgAdmin / DataGrip / pgcli. |
| `basin_project_usage` view | ✅ | Per-project live view of `ProjectCounterRegistry` counters (`bytes_read_total`, `bytes_written_total`, `class_a_ops_total`, `class_b_ops_total`, `cpu_seconds_total`, `snapshot_at`). Per-project scoped — a session sees exactly one row (its own); cross-project counters are never visible. Operators `SELECT * FROM basin_project_usage` to read the calling project's usage. v0.1 is observability-only — no engine-enforced hard quotas. External enforcement (cron drops lease, throttles writes) reads this view to decide. See `crates/basin-engine/src/project_usage_view.rs` and `docs/operators/quotas.md`. |

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
| `UUID` | ✅ | Arrow `FixedSizeBinary(16)` + field metadata `BASIN_TYPE=UUID`; pgwire OID 2950 with canonical hyphenated text + 16-byte binary. Fast-select predicate coercion fixed (`d0f0f5e`): `Utf8` UUID literals coerced to `FixedSizeBinary(16)` — UUID point-lookups work natively. |
| `POINT` (geospatial) | 🛠 | `basin-geo` crate ships Rust API; SQL surface (column type `POINT`) deferred to v0.2 via `geo_glue` stub |
| `TIMESTAMPTZ` | ✅ | Arrow `Timestamp(Microsecond, "UTC")` |
| `TIMESTAMP` (without time zone) | ✅ | Arrow `Timestamp(Microsecond, None)`; pgwire OID 1114; surfaces in `information_schema.columns.data_type` as `"timestamp without time zone"` |
| `DATE` | ✅ | Arrow `Date32`; pgwire OID 1082 (text + binary). INSERT/UPDATE coercion (`258b8ff`) accepts `'YYYY-MM-DD'`, `DATE '…'` and `'…'::DATE` literals. Residual: `WHERE d IN (SELECT …)` on a DATE column is rejected (IN-subquery literal-list rewrite). |
| `NUMERIC` / `DECIMAL` | ✅ | Arrow `Decimal128(p, s)` (1 ≤ p ≤ 38, 0 ≤ s ≤ p). DDL accepts `NUMERIC`, `NUMERIC(p)`, `NUMERIC(p, s)`, `DECIMAL(...)` synonym. Wire format: text only (binary numeric encoding is varlena-shaped and deferred to v0.2; lenient drivers handle text fine). pgwire OID 1700; `information_schema.columns.data_type` = `"numeric"`. |
| Range types (`int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange`) | 🛠 | Phase 5.24. All six range types implemented as JSON-encoded structs (Arrow `Utf8` with `BASIN_TYPE=INT4RANGE` etc. sidecar). Constructors (`int4range(lo, hi [, bounds])`) and operators (`@>`, `<@`, `&&`, `<<`, `>>`, `-|-`, `+`, `*`, `-`) registered as DataFusion scalar UDFs in `crates/basin-engine/src/range_udf.rs`; infix operator syntax rewritten to UDF calls via `rewrite_range_operators`. Accessors: `lower`, `upper`, `isempty`, `lower_inc`, `upper_inc`, `lower_inf`, `upper_inf`. **Caveat (landing):** a sibling agent is landing this work now; index-probe wiring (interval-tree-style fast overlap scan) may be pending at the time of this writing. Correctness is preserved — unindexed range queries fall back to full DataFusion scan. End-to-end harness in `tests/integration/tests/range_types_harness.rs` (`#[ignore]`-gated pending engine wiring). |
| `citext` (case-insensitive text) | 🛠 | Phase 5.30. `CITEXT` column type implemented: Arrow `Utf8` + `BASIN_TYPE=CITEXT` metadata; case-folding via `citext_fold()` in `crates/basin-common/src/types/citext.rs`. Comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) wired as DataFusion ScalarUDFs (`citext_eq`, `citext_ne`, `citext_lt`, etc.) in `crates/basin-engine/src/operators/citext_cmp.rs`. UNIQUE constraint enforcement folds the key at index time via `crates/basin-storage/src/index/btree_citext.rs`. **Caveat:** full WHERE-clause rewrite (auto-folding `WHERE email = 'FOO@BAR.COM'` against a citext column without explicit `::citext` cast) is deferred; callers must use `citext_eq(col, $1)` or explicit cast syntax until the planner optimizer rule lands. End-to-end harness in `tests/integration/tests/citext_harness.rs` (`#[ignore]`-gated). |
| `INTERVAL`, `MONEY`, `XML`, geometric (LINESTRING/POLYGON) | 🚫 | |

## Multi-project

| Capability | Status | Notes |
|---|---|---|
| Per-project bucket prefix isolation | ✅ | structural, enforced at storage API |
| Connection → project via username | ✅ | pluggable resolver |
| Per-project snapshots | ✅ | Iceberg-style atomic appends |
| Per-project fairness (Semaphore) | ✅ | cap=16; default-on |
| Row-Level Security | ✅ | `ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` with `current_user`-aware predicates injected at logical-plan layer; cross-project leak invariant tested |
| BYO-bucket | ◻️ | customer's S3 + IAM role |
| BYO-key (KMS) | ✅ | Engine seam fully wired. `EncryptionProvider` trait in `basin-storage::encryption` + `Storage::attach_encryption_provider` (opt-in, default `None` = plaintext) + per-project `ProjectStorageConfig` registry persisted via catalog (`Storage::set_project_storage_config`) + cache-invalidation on update. `wrap_key_with_config` / `unwrap_key_with_config` extension methods route per-project CMK refs (default-impl forwards to `wrap_key`/`unwrap_key` for backward compat). Writer envelope-encrypts the Parquet body with a fresh per-file AES-256-GCM data key + persists the wrapped key as a `<path>.wrapped` sidecar; reader transparently unwraps. **External callers plug in their own KMS adapter** — the OSS engine ships only the trait + per-project config + envelope-encryption hooks. |
| Project deletion (`O(file_count)`) | ✅ | `Storage::delete_project` is catalog-first paths + parallel orphan LIST + bulk DeleteObjects + drop_namespace; LocalFS ~4ms, high-latency S3-compatible store ~1.4-2.2s |
| Table fork (catalog COW) | ✅ | `Catalog::fork_table(project, src, dst)` clones a table's metadata + snapshot history into a new sibling within the same project, sharing data files by reference. Diverges on next commit. v0.2 adds cross-project fork with refcount-aware GC. |
| Within-project time partitioning | ✅ | `CREATE TABLE … PARTITION BY RANGE (ts)`; partition pruning |
| Tiered storage (hot/cold) | ✅ | `ALTER TABLE … SET cold_after = N`; compactor moves files between tiers |
| Whale-project pinning | ✅ | `BASIN_PROJECT_PINS=ulid:idx,...` pins a project to a specific shard endpoint regardless of consistent hash; v0.2 moves pins into the catalog so they survive restart |

## Storage

| Capability | Status | Notes |
|---|---|---|
| Parquet under `projects/{id}/...` | ✅ | ZSTD-1 compression, 65k row groups (configurable per table) |
| Predicate pushdown | ✅ | row-group statistics + page index |
| Projection pushdown | ✅ | DataFusion-driven |
| Bloom filters in Parquet footer | ✅ | per-column opt-in via `ALTER TABLE … SET BLOOM FILTERS ON (col)`; turns ~80% of nonexistent-id queries into row-group skips |
| Coalesced metadata in catalog (Phase 5.7 A4) | ✅ | file-level `column_stats` (min / max / null per column) on every committed `DataFileRef`; `Storage::read_paths` skips LIST + per-file footer fetch when the catalog stats prove the predicate prunes the file. Row-group-level coalesced stats deferred to v0.2 (B1). |
| Per-table row-group sizing | ✅ | `ALTER TABLE … SET row_group_rows = N`; small row groups for point-heavy tables |
| `basin.file_format` per-table option | ✅ | `CREATE TABLE … WITH (basin.file_format = 'vortex' \| 'parquet')` selects the on-disk format at create time; persisted in catalog `TableMetadata.file_format`. Vortex is the default since 2026-05-18 per [ADR 0015](./docs/decisions/0015-vortex-storage-format.md); Parquet remains a first-class selectable format for Iceberg / Athena / Spark / DuckDB read-compat. Single format per table — mixed-format provider deferred. `ALTER TABLE … SET FILE_FORMAT` ships via Vortex Lane 8 for empty tables. |
| `basin.sort_by` compound DDL option (WEDGE 4) | ✅ | `CREATE TABLE … WITH (basin.sort_by = 'col1, col2')` declares a `file_sort_order` the writer enforces (`lexsort_to_indices` + `take` before flush). Window shapes whose `PARTITION BY` / `ORDER BY` match the declared sort recover scan-as-presorted plans; smoke shapes against fact tables opted into `basin.sort_by='id'` (see commit `00107eb`). Compound multi-column form supported. |
| `basin.row_block_size` per-table option | ✅ | `CREATE TABLE … WITH (basin.row_block_size = N)` sets per-table chunk granularity for Vortex / Parquet writes; tunes point-heavy vs scan-heavy shapes (commit `dc8cd96`). |
| Vortex storage backend (default since 2026-05-18) | ✅ | BtrBlocks cascade + `.with_compact()`; **1.95×** smaller on disk and on-par-to-better full-scan / aggregate / string-eq throughput vs ZSTD Parquet. Self-describing decode: `vortex_format::decode` recovers Arrow schema from the file's own `DType` (`vf.dtype().to_arrow_schema()`); `Utf8View`/`BinaryView` normalised to canonical `Utf8`/`Binary`. Differential `vortex_parquet_differential` harness asserts byte-identical results across point / range / inequality / IS NULL / string-eq / compound / aggregate / GROUP BY / ORDER BY+LIMIT / projection / full-scan plus DELETE/UPDATE rewrite on multi-file tables. **Trailing on point-lookup latency** (≈0.65× after catalog-stats file pruning) and `ORDER BY … LIMIT` (≈0.38×); native vortex-datafusion execution still maturing. See [ADR 0015](./docs/decisions/0015-vortex-storage-format.md). |
| FileMetadataCache wired into RuntimeEnv | ✅ | Eliminates per-iteration footer re-parse; warm shapes hit cached Vortex / Parquet footers instead of re-decoding. Commit `d26a92d`. |
| VortexFooterCache | ✅ | Skips per-file footer re-parse on hot shapes; complements FileMetadataCache. Commit `f5c01ef`. |
| Cluster-by physical sort (Phase 5.7 B2) | ✅ | `Catalog::set_cluster_columns` configures per-table cluster columns; the writer `lexsort`s every batch by those columns before Parquet flush so related rows live in the same row group / file. Combined with A3 bloom + A4 catalog stats, point queries on the cluster columns prune to one file in the common case. SQL: `CREATE TABLE … CLUSTER BY (...)` / `ALTER TABLE … CLUSTER BY (...)` / `ALTER TABLE … RESET CLUSTER BY`. |
| Pluggable `object_store` in `basin-server` | ✅ | `BASIN_STORAGE_BACKEND=local|s3|tigris` wires the runnable binary to local FS or S3-compatible object stores. The storage crate still accepts any `dyn ObjectStore` for embedding/tests. |
| **NVMe disk cache** | ✅ | LRU on local SSD; ~50ms cold S3 fetches → ~100µs warm SSD reads. Default-on. 101× speedup measured. |
| **Parquet page cache (RAM)** | ✅ | LRU of decoded RecordBatches; <1ms warm hits. Default-on. 7.24× speedup measured. |
| HTTP/2 toggle for S3 client | ✅ | `S3Config::http2_only`; useful on AWS S3 / Tigris over HTTPS |
| Iceberg-style catalog (in-memory) | ✅ | atomic appends, optimistic concurrency |
| Iceberg-style catalog (durable) | ✅ | Postgres-backed; survives restart. Multi-region replication direction: single-writer global PG with regional read replicas via PG logical replication — see [ADR 0010](./docs/decisions/0010-catalog-replication.md). |
| Point-in-time restore (catalog level) | 🛠 | `Catalog::rollback_to_snapshot(project, table, snapshot_id)` truncates history to ≤ target and rewinds the head pointer. InMemory + Postgres impls. v0.2 adds physical file GC for orphaned post-rollback files; cross-DML rollback waits on soft-delete (also v0.2). Project-wide variants (`list_snapshots_project_wide`, `diff_snapshots`, `rollback_to_snapshot_project_wide`) shipped for Migration Manager v0.2. |
| Per-project fair-share scheduler | ✅ | EDF (Earliest Deadline First) shipped on top of the cap=16 primitive. Priority by op-shape: HEAD/list/small-range/DELETE/COPY/full-GET → High (5ms deadline); PUT/multipart/large-range (≥256KiB) → Low (1000ms deadline). `CONSECUTIVE_DISPATCH_CAP` prevents one project from monopolising all 16 slots. Fairness obs: project B's 100 sequential HEADs against project A's 1000 bulk-ops → p50 5.83ms / p99 13.97ms. See [ADR 0008](./docs/decisions/0008-noisy-neighbor-fairness.md). |
| WAL (local/object-store backed) | ✅ | `LocalWal` is the production path today and `basin-server` can mirror the S3-compatible storage backend for WAL object storage. `RaftWal` implements the same trait with openraft in a single-process simulation cluster (3-node + leader-failure + quorum-commit tests). Cross-process networking and persisted Raft state are v0.2 work, not a current production durability boundary — see `crates/basin-wal/RAFT.md`. |
| Background compactor | 🛠 | merges small files; tier sweep + cold-data move shipped |
| Iceberg REST catalog (Lakekeeper compatibility) | ✅ | `basin-iceberg-rest` crate ships GET (namespaces, list-tables, load-table), POST `create-table`, POST `commit-table` (mapped Iceberg requirements: assert-table-uuid via UUIDv5(`project/table`), assert-current-schema-id, assert-ref-snapshot-id; mapped updates: add-snapshot + set-current-snapshot via existing `Catalog::append_data_files` optimistic-concurrency), and DELETE table. Drop into `basin-server` to expose. Other commit actions (add-schema, set-default-spec, remove-snapshots, set-properties, etc) return structured 501; `register-table` and overwrite-style commits explicitly v0.2. |
| Per-project secondary indexes (B-tree) | ✅ | Phase 5.7 B1 (`33a8162`). `CREATE INDEX <name> ON <table> (<col>)` — fast-select probe path; `pg_index` view populates with real rows. Biggest point-query win for true random ids. **Caveat:** the in-RAM index is legitimately incomplete (built async, FIFO-evicted, blind to rows predating it), so a probe MISS means "unknown", not "empty" — a miss falls through to the pruned cold read so an auto-built index can never make a live row invisible (soundness fix in `f5d1d6f`). |
| Auto-index advisor | ✅ | `c953d83`. The engine observes repeated point / IN-list probe shapes and recommends (and can auto-build) a secondary B-tree index for the hot column; `sorted-PK skip` resolves IN-lists against a sorted PK in O(log n) zone-prune without a full scan. See `crates/basin-engine/src/index_advisor.rs` + `tests/integration/tests/index_advisor.rs`. |

## Query execution

| Capability | Status | Notes |
|---|---|---|
| OLTP path via DataFusion | ✅ | per-project `SessionContext`; single-engine architecture (DuckDB removed) |
| Metadata-only fast paths (aggregate / point / range / IS NULL / BETWEEN / low-card GROUP BY) | ✅ | Bare `COUNT/SUM/MIN/MAX` and most filter shapes bypass DataFusion entirely, answering from catalog `column_stats` + Vortex footer when the predicate prunes to one file or the aggregate has no GROUP BY. Measured **~15-40×** wins on the metadata aggregates; ~1.3-1.7× on join shapes. See [ADR 0015](./docs/decisions/0015-vortex-storage-format.md) and `benchmark/vortex_vs_parquet_smoke`. |
| Native Vortex execution path | ✅ | `vortex-datafusion` 0.70 `VortexFormat` implementing the DataFusion 53 `FileFormat` trait. Native projection + type-safe filter pushdown; predicates pushed only when the catalog schema proves Arrow-type-exact match with the literal (Vortex panics uncatchably on mixed-DType compare). Decode-then-filter retained as the fallback. |
| Engine routes analytical SQL automatically | ✅ | aggregate / GROUP BY / `/*+ analytical */` hint heuristic |
| Cross-shard query merging | 🛠 | router → shard-owner protocol shipped (consistent hashing, 28% max load); cross-shard JOIN deferred |
| Cost-based query rejection | ✅ | v0.1: `BASIN_QUERY_COST_LIMIT_ROWS=N` rejects single-table SELECTs that estimate above the row cap with PG SQLSTATE 54000 (`program_limit_exceeded`). Default off. Multi-FROM / JOIN / sub-query / explicit-LIMIT shapes pass through unchecked; v0.2 uses A4 catalog `ColumnStats` for selectivity-aware estimates on those. |
| HTAP hot tier (memtable + WAL markers) | ✅ | Phase 5.14.C1–C6 (`57d2ae11`…`9e107ef`). Per-project/table `MemTable` absorbs writes; `RaftWal`-marker flush loop snapshots to Vortex non-blocking; read-path merge gives HTAP semantics. `GlobalPressureScheduler` picks largest-first flush. PK-ordered read-merge handles INSERT-SELECT/DEFAULT through the hot tier. `ALTER PROJECT … SET memtable_cap = N` controls per-project memory budget (per-version accounting since `8d9fc2d`). MemTable entries carry MVCC version chains so pinned snapshot readers survive concurrent overwrites; chains drain whole at flush acknowledgement. 88-shape differential confirms hot/cold/split modes produce byte-identical results. |
| Catalog bloom filters (per-file point-eq miss pruning) | ✅ | Phase 5.14.A (`ae6460a`…`c5739a6`). Per-file bloom filters stored in `DataFileRef.bloom_filters`; fixed-seed hasher closes false-negative correctness bug (5.14.A4). Complements per-column Parquet page bloom filters. |
| HLL / t-digest sketches + `APPROX_COUNT_DISTINCT` / `APPROX_PERCENTILE` | ✅ | Phase 5.14.B1–B5 (`1c2387d`, `696cadb`, `fc00a41`, `4935115`). Writer-side per-column HLL and t-digest sketches stored in `DataFileRef`; `APPROX_COUNT_DISTINCT(col)` aggregate UDF (HLL); `APPROX_PERCENTILE(col, p)` aggregate UDF (t-digest). Differential tested at 1 M rows across file counts. |
| Query-shape insights + `basin_stat_statements` view | ✅ | Phase 5.16.A–D (`33ae73f`…`94db513`). Canonical per-shape hash (xxh3_64); per-shape HDR histogram registry (bounded LRU, O(bytes) per project); scale-dependent regression tracking (per-(shape, log2-bucket) histograms + `top_regressions`); `basin_stat_statements` SQL view + OTLP export. |

### OLTP latency profile (1M rows, LocalFS, no index)

After three work waves of OLTP scale fixes (transaction-scoped overlay, RMW
fast paths, pruned cold pre-image reads, write-through PK row cache, pinned
in-tx reads, S4 residency, delta updates, the pre-parse INSERT path),
single-row operations at 1M rows are **sub-ms-to-ms-class** and bulk INSERT
beats Postgres outright at every scale — numbers from the 2026-06-11
integrity run (single idle-box session; the 1M card run solo), no index on
either side (PG numbers in parens; 10k-card values cited where the scales
differ):

| Operation | Basin p50 | Notes |
|---|---|---|
| **Bulk INSERT (one statement stream)** | **2.08 s at 1M (PG 8.10 s — Basin 3.9× faster); 33.6 ms at 10k (PG 117 ms — 3.5× faster)** | pre-parse classifier + literal-VALUES scanner + statement-affine WAL striping; was 262 s three waves ago. Durability note: the default acks before fsync (≤200 ms loss window) while PG's number is fsync-durable; `SET basin.synchronous_commit = on` closes the gap with group-committed fsync — **measured: ~2% added latency on the 10k bulk INSERT in the probe harness** (group commit amortizes one fsync per statement group) |
| Point query (PK `=`) | 0.50 ms at 1M / 0.06 ms at 10k | PG's PK btree is ~2 µs; protocol+catalog floor analysis in the µs-read design |
| UPSERT (`ON CONFLICT DO UPDATE`) | ~0.26 ms (PG 0.02) | |
| Single-row UPDATE | 1.24 ms at 1M / 0.33 ms at 10k (PG 0.01) | hot-overlay fast path |
| Conditional UPDATE (`SET … CASE`, no WHERE) | ~4.9 ms (PG 1.9) | delta updates: overlay writes, background-reconciled; was ~208 ms |
| Read-modify-write contention (8 sessions) | ~8.4 ms / op (PG 4.7) | was ~80 ms |
| Point query + FK hydrate (events ⋈ users) | 0.40 ms at 1M / 0.10 ms at 10k | point-join fast path |
| Keyset pagination (`WHERE id > … ORDER BY LIMIT`) | 0.07 ms at 10k / 23.5 ms at 1M | full speed at 10k after stripe-merge compaction; declines on the 1M card while hot-tier overlays from earlier card shapes are live — under investigation as a card-order interaction, correctness-first by design |
| LIMIT without ORDER BY (early-exit scan) | 0.06 ms at 10k / 52 ms at 1M | same card-order interaction as keyset |
| `SELECT … FOR UPDATE` + UPDATE (one txn) | ~2.1 ms (PG 1.6) | first-touch reads pin-and-serve; FOR UPDATE locks are advisory (optimistic concurrency) |
| Concurrent SELECT (16 sessions, mixed) | ~19 ms (PG 2.3) | open concurrency gap |
| Mixed read-write concurrency 8R+4W (600 ops) | ~202 ms (PG 12) | newly measured shape; the largest open concurrency gap |
| Concurrent point reads (C=16, storage layer) | ~7,900 qps | was ~218 qps before the unfiltered-cache serving fix |

Work-counter CI gates (`scale_invariants.rs`, `file_count_scaling.rs`,
`1b76c7d`) assert these primitives do **bounded work** regardless of table size
— a fresh point read opens ≤2 files / decodes ≤2 chunks at any scale, repeat
reads do zero cold work, DELETE is tombstone-only, and `files_opened` stays
constant as file count grows. These are scale-invariant guarantees, not
wall-clock numbers.

#### Known OLTP / correctness gaps still open

- ~~FK `ON DELETE CASCADE` recursion~~ and ~~`DELETE … WHERE id BETWEEN`~~ —
  CLOSED 2026-06-11: BETWEEN deletes desugar through the cold rewrite's existing
  range atoms; cascade was already fully implemented and is now pinned by tests
  (delete_features.rs). Cross-table cascade is not transactionally atomic under
  auto-commit (each child DELETE commits separately) — documented semantics.
- ~~Read-own-insert tail flush~~ — CLOSED 2026-06-11 (S4 age-based residency):
  an auto-commit INSERT ≤ 128 rows (`BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS`)
  writes through to the hot tier as a CLEAN entry and the point read answers
  from memory — zero file opens, zero rows decoded, no forced tail flush
  (`row_tier_residency.rs`). Retained rows survive the tail flush for
  `BASIN_HOTTIER_RETAIN_SECS` (default 300 s; `0` = kill switch restoring the
  old flush+read behavior) under a per-project clean-byte cap
  (`BASIN_HOTTIER_RETAIN_CAP`). Bulk loads (> 128 rows) and composite-PK
  tables keep the one-flush-then-cold path.
- **Cold in-tx UPDATE under a savepoint** routes through the cold catalog-commit
  path (the fast path is disabled when a savepoint is in scope).
- **`hottier_differential` ordering divergences** — a handful of merge-on-read
  shapes can return rows in a different order than the PG oracle (set-equal but
  not sequence-equal); these are tracked as ordering-only divergences, not
  value divergences.

### Performance residuals (won't chase further)

These are shapes where Vortex underperforms Parquet by single-digit-percent-to-30% in the smoke matrix. Documented here as known limitations with concrete workarounds. Further engineer-time invested in chasing these would return marginal customer value compared with the surface pivot (basin-cli, basin-js, WebSocket subscriptions, basin-cloud webapp).

| Shape | Ratio (parquet_ms / vortex_ms) | Root cause | Workaround |
|---|---|---|---|
| `substring_concat` | ~0.64× | Vortex string codec maturity — substring + concat operators not yet fused | Pre-materialise the concatenated column with a generated column or a computed view |
| `like_prefix` | ~0.72× | Prefix-LIKE doesn't push down through Vortex's string codec optimally | Declare `WITH (basin.sort_by='col')` to enable bloom-filter + zone-map prefix pruning; or maintain a trigram index (`basin-trgm`) |
| `string_chain_funcs` | ~0.80× | Chained `LOWER`/`UPPER`/`TRIM`/`REPLACE` incur per-batch dispatch cost; Vortex doesn't fuse the chain | Compute once in a generated column |
| `or_predicate` / `deep_or_chain` | 0.62× / 0.71× | OR / IN with many disjuncts hits Vortex's `filter_evaluation` threshold and falls back to row-scan | Split into `UNION ALL` with separate predicates per branch; or rewrite as `WHERE col IN (val1, val2, …)` if disjuncts share a column — IN list pushdown is better than OR-of-equals |
| Window-frame execution residuals beyond Phase 5.14.D3 | varies | Some uncommon frame specifications (`RANGE`-vs-`ROWS`, complex `PARTITION BY` chains) don't benefit from sort-aware metadata | Match `basin.sort_by` to the `PARTITION BY` column when feasible |
| Long-tail shapes at 0.85–0.95× | varies | Within measurement noise. Won't chase further | None — perf is within ±15% of Parquet, normal cross-codec variance |
| `inner_join` @ 100k row scale (one observed at 0.71×) | 0.71× | Appears to be system-load noise; not reproduced in clean reruns | If reproducibly slow, file an issue with EXPLAIN ANALYZE output |

**Vortex version cadence.** Basin tracks Vortex 0.70 as of 2026-05-19. Routine version bumps to 0.71+ are expected to improve several residuals "for free" — see Vortex release notes per upgrade.

### Limitations we ARE actively fixing

- **Point queries at TB-scale** — Phase 5.14.C HTAP hot tier ✅ shipped (C1–C6; memtable + flush loop + read-merge + memory budget). Further tuning in v0.2.
- **Query observability** — Phase 5.16 Query Insights ✅ shipped (A–D; shape hash + histograms + regression tracking + `basin_stat_statements` + OTLP export).
- **UUID fast-select predicate** — ✅ fixed (`d0f0f5e`): `Utf8` UUID literals are now coerced to `FixedSizeBinary(16)` in the fast-select predicate path; UUID point-lookups work without string coercion.
- **Window frames with complex `PARTITION`** — Phase 5.14.D3 (sort elision above `WindowAggExec`) shipped at `ee80b36`; 5.14.D4 multi-sort follow-up in flight

## Vector search

| Capability | Status | Notes |
|---|---|---|
| `vector(N)` column type | ✅ | Arrow `FixedSizeList<Float32>` |
| Distance ops `<->`, `<#>`, `<=>` | ✅ | rewritten to UDF calls |
| L2 / cosine / dot UDFs | ✅ | DataFusion `ScalarUDF` |
| HNSW index sidecar (`*.hnsw`) | ✅ | bincode on disk, per Parquet file. `CREATE INDEX … USING hnsw (col <opclass>) WITH (m = …, ef_construction = …)` accepts pgvector-style build params (`d5d99d4`); the declared opclass (`vector_l2_ops` / `vector_cosine_ops` / `vector_ip_ops`) is persisted so the planner only routes a query to the index whose opclass matches the query's distance operator. |
| `Storage::vector_search` fast path | ✅ | k-merge across segments |
| Automatic planner routing of `ORDER BY x <-> $1 LIMIT k` | ✅ | Detected at planning time (single-table FROM, single ORDER BY of `<col> <distance_op> <literal_or_param>`, vector column, constant LIMIT, ASC direction). Routes to `Storage::vector_search` fast path with optional pushdown of column-equality WHERE predicates (over-fetch + post-filter). Falls back to brute-force on JOINs / set ops / unbounded LIMIT / unsupported predicate shapes — correctness preserved. Distance ops `<->` (L2), `<=>` (cosine), `<#>` (dot product) all routed; the route is gated on the candidate index's opclass matching the operator (`d5d99d4`), so a cosine-built HNSW index is not used for an L2 query. ~2.3× speedup measured on a 5K-row corpus (`viability_vector_search`, HNSW vs brute-force, identical top-10). |
| IVF-flat indexes | 🚫 | HNSW is enough for first 1B vectors per project |
| `pg_vector` wire-protocol compat | 🚫 | see [ADR 0003](./docs/decisions/0003-native-vector-search.md) |

## Postgres-extension equivalents

ADR 0002 says "no upstream extensions". But the most common ones are
covered natively, as Basin-flavored crates with the same SQL semantics:

| Postgres extension | Basin equivalent | Status | Notes |
|---|---|---|---|
| `pg_vector` | native `vector(N)` + HNSW | ✅ | [ADR 0003](./docs/decisions/0003-native-vector-search.md) |
| `pg_cron` | **`basin-cron`** | ✅ | `cron.schedule(name, schedule, sql)` + `cron.unschedule` + `cron.job` + `cron.job_run_details`. SQL call-site UDFs `cron.schedule` / `cron.unschedule` shipped (Phase 5.8.A, `a091c04`). |
| `pg_net` + `http` | **`basin-net`** | ✅ | sync `http_get` / `http_post`; async `net.http_post` with `net._http_response` table. Per-project URL allowlist (DENY-ALL default), 10 req/s rate limit, 10 MiB body cap, 30s timeout. SQL call-site UDFs `net.http_get` / `net.http_post` shipped (Phase 5.8.A, `a091c04`). |
| `pgcrypto` (digest, encode, crypt, gen_salt) | native UDFs | ✅ | `digest` (md5/sha1/sha224/sha256/sha384/sha512), `encode`/`decode` (hex/base64/escape), `crypt` (bcrypt), `gen_salt('bf')` |
| `uuid-ossp` | native UDFs + `UUID` type | ✅ | `gen_random_uuid()`, `uuid_generate_v4()`, canonical hyphenated text + 16-byte binary on the wire |
| `PostGIS` (subset) | **`basin-geo`** | ✅ | `Point`, `Box2d`, `ST_MakePoint`, `ST_X`, `ST_Y`, `ST_Distance` (Haversine WGS84), `ST_DWithin`, `ST_Contains`. **GeoJSON I/O**: `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` (`41639b8`), with constructor-expression INSERTs. **R-tree index**: row-exact envelope candidates (`a250891`) — `&&` bbox-overlap and `ST_DWithin` radius queries prune to candidate row groups via an in-memory R-tree before decoding; `&&` does a single-decode residual to avoid re-decoding each surviving row's WKB (`dd26766`). Measured: `ST_DWithin` ~1.7× faster than PostGIS GIST on the R-tree compare card, and ~28× faster than an unindexed Basin scan at 1M rows (`viability_large_spatial_dwithin`). **Loss:** `&&` bbox-count and KNN `<->` LIMIT still trail PostGIS GIST by ~150× — PG's GIST is structurally faster on those two shapes. No `LINESTRING`/`POLYGON` — see crate. |
| `pg_trgm` | **`basin-trgm`** | ✅ | `similarity`, `word_similarity`, `extract` (trigram set). v0.1 brute-force; GIN trigram index deferred to v0.2. |
| `TimescaleDB` continuous aggregates | **`basin-cv`** | ✅ | `CvSpec` + `CvRefresher::tick`; refresh_interval enforced; per-project materialization. **Incremental refresh shipped** for the `date_trunc(_, col)` / `time_bucket(_, col)` GROUP BY shape — only re-aggregates rows newer than the watermark plus the last partial bucket. Bodies without a detectable time-bucket fall back to full re-execution. `REFRESH MATERIALIZED VIEW … WITH (full=true)` opt-out for explicit full rebuild. SQL surface ships in `basin-engine::cv_ddl`. |
| `TimescaleDB` hypertables | within-project time partitioning | ✅ | `CREATE TABLE … PARTITION BY RANGE (ts)` |
| `pg_stat_statements` | OTEL traces | ✅ | per-query spans exported via `BASIN_OTLP_ENDPOINT` |
| `Citus` (sharding) | basin-router consistent-hash | ✅ | sharding is structural via per-project prefix |
| `pg_partman` | Iceberg manifest | ✅ | partition lifecycle is just snapshot evolution |
| `hstore` | use `JSONB` | ✅ | JSONB is the modern replacement |
| `citext` | native `citext` type | 🛠 | Phase 5.30. Type, comparison operators, and UNIQUE folding shipped (see Types table). Full WHERE-clause predicate rewrite deferred. No `CREATE EXTENSION citext;` needed — always available. |
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
| `region` field on `ProjectMetadata` | ◻️ | 1-day Phase 1 add to make region-pinning explicit |
| S3 cross-region replication of data | ◻️ | "free" via bucket-level configuration on AWS S3, Tigris, etc. |
| Eventual-consistent cross-region read replicas | ◻️ | scoped in [ADR 0004](./docs/decisions/0004-multi-region-read-replicas.md), build planned |
| Cross-region 2PC / strong consistency | 🚫 | see [ADR 0001](./docs/decisions/0001-single-region-only.md) — Spanner-class, deferred until paid |

## Operations

| Capability | Status | Notes |
|---|---|---|
| Per-project metrics (ops/s, p50/p99, RAM, S3 IO) | ✅ | `Engine::project_counters(&ProjectId) -> ProjectCountersSnapshot` returns ops/bytes_read/bytes_written/errors + ring-window p99 ms estimate; registry shared across engine + storage + WAL |
| OpenTelemetry traces | ✅ | wired through router → engine → shard → storage; OTLP export available via `BASIN_OTLP_ENDPOINT` |
| Structured logs (`tracing` JSON) | ✅ | format selectable at startup |
| Connection pooling (`basin-pool`) | ✅ | Native `ProjectSession` cache; per-project cap; LRU eviction. Wired into `basin-server` behind `BASIN_POOL_ENABLED=1`. See [ADR 0007](./docs/decisions/0007-connection-pooling.md). |
| Rate limiting (basin-net side) | ✅ | per-project 10 req/s sustained, burst 30; URL allowlist; body cap; timeout |
| Rate limiting (pgwire side) | ✅ | per-project token-bucket via `governor` (same crate as basin-net). Default off; `BASIN_PGWIRE_RATE_LIMIT_QPS=100` enables 100 qps sustained / 300 burst with bucket-empty mapped to Postgres SQLSTATE `53400` (`configuration_limit_exceeded`). Per-project overrides + catalog-driven config deferred to v0.2. |
| Bring-your-own-bucket | 🚫 | hosted-product concern; out of scope for this OSS workspace |
| Bring-your-own-key (KMS) | ✅ | Engine seam complete (see Multi-project table). Per-project CMK routing via `ProjectStorageConfig` + `wrap_key_with_config`. External callers wire their own KMS adapter — OSS engine ships the seam, not the adapter. |
| Billing integration | 🚫 | hosted-product concern; out of scope for this OSS workspace |

## Auth and REST API

| Capability | Status | Notes |
|---|---|---|
| `basin-auth` (signup, signin, magic-link, password reset, email verify, JWT, refresh) | ✅ | Requires SMTP at startup (fail-fast). Auth tables live in each project's own storage namespace under the `basin_auth` schema prefix — no reserved internal project, no loopback pgwire connection, no separate Postgres required. Auth state replicates automatically with project storage. JWT issued + verified per request. `BASIN_AUTH_ENABLED=1`. Optional override `BASIN_AUTH_CATALOG_DSN=<dsn>` points auth at a separate external Postgres instance for blast-radius isolation; not the default path. See [ADR 0005](./docs/decisions/0005-auth-system.md) and [ADR 0013](./docs/decisions/0013-auth-per-project-schema.md). |
| `auth.uid()` / `auth.role()` / `auth.jwt()` session functions | ✅ | SQL functions in the `auth` schema, populated from JWT claims at connection open. Use in RLS policies: `CREATE POLICY "own rows" ON items FOR ALL USING (owner_id = auth.uid())`. Both `auth.uid()` (schema-qualified) and `auth_uid()` (underscore) spellings accepted — the executor rewrites the schema-dot form before DataFusion sees it. Anonymous sessions return `NULL` / `'anon'` matching Supabase behaviour. |
| Per-project auth schema (ADR 0013) | ✅ | Auth data lives in each project's own Basin storage namespace (`basin_auth` schema prefix). No reserved internal project. No loopback pgwire connection. `EngineAuthStore` default; `PostgresAuthStore` available for external-Postgres override. |
| Self-routing pgwire credentials | ✅ | `pgwire_user` format `{project_id}_{hex}` (26-char ULID prefix). Credential validation parses project_id directly from the user field — no global cross-project lookup table. API keys embed the same project prefix. Old-format credentials (`project_<hex>`) are automatically migrated on first startup after upgrade. |
| `basin-rest` (PostgREST-compatible HTTP layer) | ✅ | `GET`/`POST`/`PATCH`/`DELETE` on `/rest/v1/<table>`. Bearer-JWT auth via `basin-auth`. `BASIN_REST_ENABLED=1` (requires auth). See [ADR 0006](./docs/decisions/0006-rest-api-layer.md). |
| RPC function mount (`POST /rest/v1/rpc/:fn`) | ✅ | Phase 5.11.L (`183c315`, ADR 0019). Invokes `LANGUAGE sql` and `LANGUAGE wasm` functions over HTTP; bearer-JWT auth; request body JSON → bind params; result JSON array response. |
| Pgwire JWT auth (`user` parameter carries bearer token) | ✅ | When auth is enabled, both pgwire and REST honor JWT. Static project map continues to work as fallback. |
| Per-project pgwire connection URLs | ✅ | `POST /admin/v1/projects` returns `postgres://<project_user>:<password>@host:5433/<db>`. Password is bcrypt-validated on every pgwire startup; mismatch returns SQLSTATE `28P01` with a uniform "invalid pgwire credentials" message (no user-existence leak). Rotate via `POST /admin/v1/projects/{user}/rotate`; old password invalidates immediately. List via `GET /admin/v1/projects/{project_id}/credentials` (no plaintext, no hash). All admin endpoints gated on `claims.is_admin == true`. The `BASIN_PROJECTS=alice=*` static-resolver path is preserved for back-compat demos. Cross-project isolation under per-project URLs is integration-tested (UNION + CTE bypass attempts both blocked); within-project RLS still applies. |
| API-key tokens (long-lived, revocable) | ✅ | `POST /auth/v1/api-keys` issues; `GET` lists; `DELETE` revokes. sha256 lookup + bcrypt verify; `Authorization: Bearer <key>` works on REST and (via `ApiKeyProjectResolver`) pgwire. |
| Real PostgREST (Haskell) sitting in front of Basin | 🚫 | needs `pg_catalog` / `information_schema` — 2–4 month slog with ongoing maintenance. Building basin-rest natively is ~3 weeks instead. |

## Comparison shape vs Supabase / Neon / Postgres

See [`docs/deployment.md`](./docs/deployment.md) for the production cloud
architecture rationale. TL;DR:

- **Single instance per region, not per customer.** Basin's whole wedge is
  that Postgres-as-a-Service vendors charge per project because Postgres
  can't multi-project cheaply. Basin can. ~$0.10–$0.20 per project per
  month all-in for a 100 MB / 10k-project workload.
- **Fly.io + Tigris is the managed cloud.** Zero egress, ~5–30 ms
  RTT in-metro, no surprise bills.
- **Multi-region today** = deploy one Basin cluster per region. No DB
  changes required. Read-replica / cross-region writes are Phase 6 work.

## What we're not building, and what to use instead

If your workload requires …

- **High-frequency single-project OLTP** → use Postgres or Aurora.
- **Globally consistent cross-region writes** → use Spanner / CockroachDB.
- **Edge / local-first apps** → use Turso (libSQL) or Cloudflare D1.
- **Geospatial primary store with full PostGIS** (LINESTRING, POLYGON, R-tree) → use real PostGIS or sidecar PG.
- **Embeddings as the *only* workload** → use a dedicated vector DB (Qdrant, Pinecone, Weaviate, pg_vector on Postgres).
- **Embedded SQLite-class library** → use SQLite.
- **PL/Python, PL/Perl, PL/pgSQL, or other alt-language stored procedures** → use Postgres. Per [ADR 0012](./docs/decisions/0012-change-event-primitive.md), Basin replaces the PG trigger / PL/pgSQL surface with declarative lifecycle columns + SQL-bodied reactors + `LANGUAGE sql` functions + `CALL` procedures (Phase 5.11.B/C/D/E/F) — that covers ~95% of real-world trigger / stored-procedure use cases without a PL/pgSQL parser or interpreter. The remaining ~5% (cursor-driven loops, `EXCEPTION` blocks, complex control flow) is an explicit non-goal.

Basin's wedge is multi-project SaaS with audit-log workloads where storage
cost and per-project isolation dominate. If your shape doesn't match, the
above are honest recommendations.

### Honest parity gaps

The features in the "Batteries included" matrix have these specific v0.1 limits — listed here so users can decide whether the gap blocks their workload:

- **WASM UDFs** support scalar `i32` / `i64` / `f64` plus `text` / `bytea` / `timestamptz` arguments and return values. Variable-length values cross the boundary over a `(ptr, len)` linear-memory ABI: the host calls the module's exported `basin_alloc` / `basin_dealloc`, writes UTF-8 / raw bytes into guest memory, and the guest packs its return as a `(ptr << 32) | len` `i64` (`len = -1` signals SQL `NULL`). JSONB is handled by declaring the argument as `text` and parsing the JSON document inside the module — there is no dedicated `jsonb` SQL arg type yet. Still deferred: a first-class `jsonb` arg type and **vectorized** (whole-Arrow-array) invocation — execution is currently per-row. See `tests/integration/tests/wasm_udf_types.rs`.
- **Realtime SSE / WebSocket / presence** is shipped (Phases 5.11.R2–R6) but the end-to-end integration harness has `#[ignore]`-gated slices pending un-ignore. Single-client smoke tests pass; cross-client soak coverage in flight.
- **Blob storage (`basin-blob`)** ships the full v1 surface (ADR 0021): the catalog-backed `BlobStore` plus the `/storage/v1/` REST routes in `basin-rest` — bucket create/get/delete, object upload/download/list/single+bulk-delete, an unauthenticated public-object fast path, signed-URL minting and verification (HMAC-SHA256 over `(project, bucket, path, expiry)`, constant-time verify, independent key rotation), per-object RLS enforcement, and per-project byte-quota counters (`bytes_written_total`). Deferred to v1.1: `HEAD` (metadata-only) and `COPY` object ops, resumable multipart / TUS uploads, image transforms, object versioning, and cross-project sharing. Cloud-side quota *enforcement* (rejecting writes past a limit), billing, and CDN integration are out of crate scope.
- **REST RPC mount** (`POST /rest/v1/rpc/:fn`) accepts JSON request bodies; binary / multipart request bodies (direct file upload through the RPC mount) are not wired — upload files through the `/storage/v1/object/...` routes instead.
- **Vector** — HNSW per-file segment + planner auto-route shipped; IVF-flat and `pg_vector` wire-format compat are explicit non-goals (see [ADR 0003](./docs/decisions/0003-native-vector-search.md)).

## Documented exclusions (5.22.E)

This section catalogues PostgreSQL features and behaviors Basin intentionally
does not support, or supports with material caveats. `pg_dump` output tags
each affected object with a `-- skipped: <feature>` comment so that
`psql`/`pg_restore` import attempts have actionable hints.

### SQL statements rejected at parse time (SQLSTATE 0A000)

| Statement / feature | Reason |
|---|---|
| `VACUUM` / `VACUUM ANALYZE` | Iceberg-style compaction is handled by the background compactor, not by VACUUM |
| `CREATE TRIGGER … EXECUTE FUNCTION` (PL/pgSQL triggers) | Basin replaces PL/pgSQL triggers with declarative lifecycle columns and SQL-bodied reactors (ADR 0012); PL/pgSQL interpreter is not shipped |
| `CLUSTER` (heap re-order) | Not meaningful for Parquet / Vortex object-store files; use `ALTER TABLE … CLUSTER BY (…)` for physical sort on write |
| `REINDEX` / `REINDEX CONCURRENTLY` | Index rebuild handled internally; no user-facing command |
| `LOCK TABLE` | Lock management is internal; advisory locks are used for optimistic concurrency (ADR 0026) |
| `COPY … FROM/TO file_path` without the env gates | Server-side file-path `COPY` is default-deny; requires `BASIN_COPY_ALLOW_FILE_PATHS=1` + `BASIN_COPY_PATH_ALLOWLIST` (and is CSV-only — `FORMAT BINARY` file paths rejected with SQLSTATE 42601) |
| `COPY … WITH (FORMAT text)` | PG's tab-delimited text COPY format (backslash escapes, `\N` NULLs) not implemented; use CSV or BINARY |
| `COPY … (FORMAT BINARY)` over exotic column types | Vectors / intervals / arrays have no binary COPY codec; rejected with SQLSTATE 0A000 naming the column. Scalar types (int2/4/8, float4/8, bool, text, bytea, jsonb, uuid, timestamp[tz], date, numeric) are supported |

### DDL features not supported

| Feature | Caveat / status |
|---|---|
| `CREATE EXTENSION` | No loadable `.so` extensions (ADR 0002). The common extensions are covered natively: `pgvector` → `vector(N)`, `citext` → native `CITEXT`, `pg_trgm` → `basin-trgm`, `pgcrypto` → native UDFs, `uuid-ossp` → native UDFs, `pg_cron` → `basin-cron`, `pg_net` → `basin-net`, `PostGIS` subset → `basin-geo`. `pg_dump` output emits `-- skipped: CREATE EXTENSION <name>` for any extension not in this list. |
| `CREATE LANGUAGE` / `CREATE PROCEDURAL LANGUAGE` | PL/pgSQL, PL/Python, PL/Perl and other procedural languages are not supported. Only `LANGUAGE sql` and `LANGUAGE wasm` are valid in `CREATE FUNCTION` / `CREATE PROCEDURE`. |
| `CREATE TRIGGER … EXECUTE FUNCTION` (PL/pgSQL body) | See above. Equivalent: `ALTER TABLE … REACT ON {INSERT\|UPDATE\|DELETE} EXECUTE <sql>`. |
| `ALTER TABLE … RENAME TO` / `ALTER TABLE … RENAME COLUMN` | Column and table renames not yet implemented; rejected at parse time in v0.1. |
| `CREATE TABLE … INHERITS (parent)` | Table inheritance not supported; use partitioning or separate tables. |
| `CREATE TYPE … AS (…)` composite types | Composite row types not supported; use JSONB for structured sub-objects. |
| `CREATE TABLE … (col TEXT[])` / `col INT[]` array column types | Array column types in `CREATE TABLE` DDL are not yet wired through the Arrow schema bridge. `ARRAY[…]` expressions and array functions work on existing columns; DDL for array-typed columns is rejected. |
| `CREATE INDEX … USING gist` (geometry, `EXCLUDE USING gist`) | GIST geo-index is not on the roadmap. `EXCLUDE USING gist` (e.g. for meeting-room scheduling) is not supported. Use Basin's interval-tree–backed range index (`USING gist` on range columns is mapped to the interval-tree probe for `tstzrange`/`daterange`; geometry spatial index is not available). |
| `CREATE SEQUENCE … CYCLE` / `OWNED BY` options | Sequences ship; `CYCLE` and `OWNED BY` options are not parsed and will be silently dropped or rejected. |
| `ALTER TABLE … SET TABLESPACE` | Tablespaces do not exist in Basin; rejected. |
| `COMMENT ON …` | Object comments are not stored or surfaced in `pg_catalog`. |
| `GRANT` / `REVOKE` / `REASSIGN OWNED` | Role-based access control is not shipped in v0.1. RLS policies (`CREATE POLICY`) are the isolation primitive. |
| `CREATE DATABASE` / `DROP DATABASE` | Projects are the Basin unit of isolation; `CREATE DATABASE` is unsupported. Rejected with SQLSTATE 0A000. |
| `CREATE SCHEMA <name>` for user schemas | User-defined schemas beyond `public` are not supported. Reserved schemas (`basin_auth`, `basin_blob`, `basin_cron`, `basin_net`, `basin_realtime`, `pg_catalog`, `information_schema`, `auth`, `storage`, `realtime`) are guarded against user DDL (SQLSTATE 42501). |

### Type caveats

| Type | Caveat |
|---|---|
| `NUMERIC` / `DECIMAL` — binary wire encoding | Text-format only on the pgwire wire (binary `NUMERIC` is varlena-shaped and deferred to v0.2). Drivers that request binary format for NUMERIC will receive text. |
| `INTERVAL` | Not supported as a column type. `age()` returns an `interval` expression value in query results, but `CREATE TABLE … (col INTERVAL)` is rejected. |
| `MONEY` | Not supported. |
| `XML` | Not supported. |
| `LINESTRING`, `POLYGON`, geometric types beyond `POINT` | Not supported. Only `POINT`-equivalent geometry via `basin-geo`; no `LINESTRING`/`POLYGON`/R-tree. |
| `citext` — implicit cast in WHERE | `WHERE email = 'FOO@BAR.COM'` against a `CITEXT` column does not automatically fold the literal. Callers must use an explicit `::citext` cast or the `citext_eq(col, $1)` UDF until the planner optimizer rule lands (v0.2). |

### Query / execution caveats

| Feature | Caveat |
|---|---|
| `WINDOW … EXCLUDE {CURRENT ROW\|GROUP\|TIES\|NO OTHERS}` | Not parsed by sqlparser 0.61 / DataFusion 53. Queries with an `EXCLUDE` clause on a window frame receive a clean parse error. Will close when sqlparser ships `WindowFrameExclusion`. |
| `UPDATE … SET col = <expr>` (non-literal RHS) | RHS must be a literal or a single bind parameter. Expressions like `SET count = count + 1` are rejected. Compute the new value client-side. |
| `UPDATE … WHERE col IN (SELECT …)` / `WHERE EXISTS (SELECT …)` | Subquery in WHERE is rejected in v0.1. Materialise the inner SELECT client-side. |
| Cross-shard `JOIN` | Joins across projects or across shards are not supported. The router ships consistent-hash shard routing; cross-shard JOIN is deferred to v0.2. |
| `SELECT … AS OF SNAPSHOT n` / `AS OF TIMESTAMP ts` | Time-travel query syntax deferred to `basin-analytical` v0.2. |
| `SELECT *` on mixed-type tables | May fail with an Arrow type-projection mismatch on tables that mix `BIGSERIAL PK + TEXT UNIQUE + TIMESTAMPTZ DEFAULT`. Workaround: list columns explicitly. |
| `GENERATED ALWAYS AS (expr) VIRTUAL` | Only `STORED` generated columns are implemented. `VIRTUAL` is deferred to v0.2. |
| `RETURNS SETOF type` (single-column SRF) | Not supported in `CREATE FUNCTION`. Use `RETURNS TABLE(col type, …)` instead. |
| Nested `CALL` inside a procedure body | Rejected at registration in v0.1. |
| `BEGIN` / `COMMIT` / `ROLLBACK` multi-statement transactions | Single-shard, snapshot-isolated transactions shipped (`f4127e9`, `530ec82`, `9f5b7f0`). Cross-shard transactions are not supported. A cold in-tx UPDATE that misses every fast path still routes through the cold catalog-commit path. |
| `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | Shipped (SAVEPOINT stack + `ROLLBACK TO`). **Caveat:** a `SAVEPOINT` opened over an in-transaction *cold* UPDATE pins that UPDATE on the cold catalog-commit path (the fast path is disabled when an active savepoint is in scope), so savepoint-wrapped cold UPDATEs are slower than the same UPDATE outside a savepoint. |

### `pg_dump` / `basin dump` output — skipped-feature annotations

Basin's `basin dump` output emits `-- skipped: <reason>` comments for the
following constructs whenever they appear in the schema being dumped:

- `-- skipped: CREATE EXTENSION <name>` — all extension statements
- `-- skipped: PL/pgSQL function <name>` — functions whose source contains PL/pgSQL bodies
- `-- skipped: CREATE TRIGGER <name>` — PL/pgSQL-bodied triggers (Basin reactors are emitted as `ALTER TABLE … REACT ON … EXECUTE …` instead)
- `-- skipped: GRANT/REVOKE` — privilege statements
- `-- skipped: COMMENT ON` — object comment statements
- `-- skipped: CREATE LANGUAGE` — procedural language declarations
- `-- skipped: TABLESPACE` references — any DDL referencing a named tablespace

When restoring a Basin dump into real PostgreSQL, these skipped objects will
need to be recreated manually. The `-- skipped:` annotations are stable across
dump format versions.



> Full per-tool notes, CI install snippets, fixture locations, and the CI-regeneration
> contract live in [`docs/migration-tools.md`](./docs/migration-tools.md).

Summary matrix (see linked doc for caveats and the regeneration plan):

| Tool | Supported | Caveats | Verified-on-version |
|------|-----------|---------|---------------------|
| Flyway | untested (needs flyway CLI in CI) | `pg_advisory_lock` no-op required; `SET search_path` / `SHOW transaction_isolation` must be handled; composite PKs; `ALTER TABLE … ADD COLUMN … DEFAULT …` | — |
| golang-migrate | untested (needs golang-migrate CLI in CI) | `pg_advisory_lock` / `pg_advisory_unlock` no-op required (highest-risk blocker); `SET search_path`; `SELECT current_schema()` | — |
| Diesel | untested (needs diesel CLI + libpq in CI) | `pg_advisory_lock` (some versions); `SHOW search_path`; JSONB DDL (✅); `__diesel_schema_migrations` CREATE IF NOT EXISTS (✅) | — |
| sqlx | untested (needs sqlx-cli in CI) | `BYTEA` / `TIMESTAMPTZ` in `_sqlx_migrations` (✅); `pg_advisory_lock` (some versions); compile-time `query!` not exercised; `migrate revert` exits non-zero for up-only migrations (by design) | — |
| Prisma | untested (needs Node ≥ 18 + prisma CLI in CI) | Must use `db push` / `migrate deploy` — `migrate dev` requires `CREATE DATABASE` (unsupported); heavy `pg_catalog` introspection (`pg_class`, `pg_attribute`, `pg_constraint`, `pg_index`, `pg_enum`, `pg_type`) | — |

---

*Last updated: 2026-05-29. This file is hand-maintained; PRs welcome.*
