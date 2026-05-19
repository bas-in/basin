# Basin — Build Task List

Bucket-native, multi-project, Postgres-compatible database. Phased build per the
project brief. Every box should be small enough that a single PR closes it.

**Scope:** this file is the **core DB / open-source** roadmap — pgwire /
SQL / storage / catalog / query engine / multi-project / caches /
indexes / WAL / compactor / vector search / Postgres-extension
equivalents / **basin-auth** (identity) / **basin-rest** (PostgREST
equivalent). The full open-source bundle a self-hoster gets when they
clone the repo. Hosted-product / control-plane / enterprise-auth
extensions are out of scope for this OSS roadmap.

**Postgres-extension equivalents we ship natively** (not via upstream
`.so` loading — see [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)):

| Postgres extension | Basin crate | Section | v0.1 status |
|---|---|---|---|
| `pg_cron` (scheduler) | `basin-cron` | Phase 5.8 | ✅ shipped |
| `pg_net` + `http` (HTTP from SQL) | `basin-net` | Phase 5.8 | ✅ shipped |
| `PostGIS` (subset: points + distance + dwithin + contains) | `basin-geo` | Phase 5.9 | ✅ shipped |
| `pg_trgm` (fuzzy text) | `basin-trgm` | Phase 5.9 | ✅ shipped |
| `TimescaleDB` continuous aggregates | `basin-cv` | Phase 5.9 | ✅ shipped |
| `pgcrypto` + `uuid-ossp` (digests, bcrypt, UUIDs) | engine UDFs | Phase 5.9 | ✅ shipped |
| `pg_vector` | native `vector(N)` + HNSW | Phase 4 / vector-search | ✅ shipped |

Legend: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` deferred / out of scope

---

## Local PoC milestone — reached 2026-04-30

A single-process Basin server speaks pgwire end-to-end. CREATE TABLE / INSERT
/ SELECT round-trip via `psql`; project data lands as Parquet under
`/projects/{ulid}/...`; a multi-project smoke test proves cross-project
isolation across the real TCP path. Binary is `services/basin-server`.

This collapses Phase 2/3/4 production layers (Raft WAL, distributed shard
owners, placement service, full pgwire protocol) into a single in-process
pipeline (`basin-router → basin-engine → basin-storage + basin-catalog`).
It is not the production architecture — it is the smallest demoable wedge.
See `README.md` "Try the PoC" for usage.

---

## Phase 0 — Validate the wedge (1 month)

- [ ] Identify 20 candidate companies in the three target segments
      (multi-project SaaS, agent platforms, audit-heavy fintech)
- [ ] Run 20 customer interviews on the wedge question
- [ ] Sign 3–5 named design partners willing to try the alpha
- [ ] Write the PRD: pains, target ICP, non-goals, success metrics
- [ ] Decide go/no-go. Record the decision and the evidence (target file `docs/phase-0.md` does not yet exist — create it as part of the go/no-go writeup).

## Phase 1 — Storage substrate (2–3 months)

- [x] Cargo workspace + crate skeletons (`crates/basin-*`, `services/*`)
- [x] `basin-common`: `ProjectId`, `PartitionKey`, error enum, telemetry init
- [x] `basin-storage`: write Arrow `RecordBatch` → Parquet under
      `/projects/{id}/tables/{table}/data/yyyy/mm/dd/{ulid}.parquet`
- [x] `basin-storage`: read with predicate + projection pushdown
- [x] `basin-storage`: pluggable `object_store` backend (local fs, S3, R2, GCS
      — backend is whatever `dyn ObjectStore` the caller passes)
- [x] `basin-server`: runtime storage backend selection
      (`BASIN_STORAGE_BACKEND=local|r2|s3|tigris`) for data files and WAL
      object storage, with separate root prefixes for data and WAL.
- [x] `basin-catalog`: Iceberg REST client (Lakekeeper-compatible)
      — `basin-iceberg-rest` crate ships GET (namespaces, list-tables,
      load-table), POST `create-table`, POST `commit-table` with
      Iceberg-shaped requirements (`assert-table-uuid`,
      `assert-current-schema-id`, `assert-ref-snapshot-id`) + add-snapshot
      / set-current-snapshot updates, and DELETE table.
      See CHANGELOG `[0.1.1]` Phase 6 entry batch.
- [x] `basin-catalog`: atomic `append_data_files` commits, snapshot listing
      (in-memory; same semantics will apply to the REST impl)
- [x] Per-project prefix enforcement at the storage API boundary
- [x] Integration test: 1M rows × 100 projects, round-trip + cross-project isolation
      (`tests/integration/tests/phase1_substrate.rs`, runs in <2s)
- [x] Bench: predicate pushdown reduces bytes read by ≥ 10× for selective
      scans — `tests/integration/tests/viability_predicate_pushdown.rs`
      asserts `full_scan_bytes / point_query_bytes >= 10x` and emits the
      ratio to the dashboard. (see also: viability suite — `tests/integration/tests/viability_README.md`)

## Phase 2 — WAL service (2 months) — **v0.1 shipped**

- [-] Pick Raft library — openraft simulation is in-tree, but production
      cross-process Raft WAL remains v0.2; v0.1 ships single-node/object-store
      WAL behind the same `Wal` trait so the swap is a backend change
- [x] `basin-wal`: file-backed single-node WAL, append keyed by
      `(project_id, partition)` with monotonic per-partition LSN
- [x] Batched flush to object storage every 200 ms or 1 MB
- [x] Recovery: list segments and replay on `Wal::open`
- [x] Bench: **57k writes/sec debug, 954k writes/sec release** (well over
      the 10k spec target)
- [-] Chaos test (deferred to Raft v0.2 — single-node has no peers to kill)

## Phase 3 — Shard owners (2–3 months) — **v0.1 shipped**

- [x] `basin-shard`: in-process map `(project, partition) → in-mem state`
- [x] Lazy load project state from WAL + Parquet on first request
- [x] Idle eviction (default 5 min) with metrics
- [x] Write path: WAL append → ack → in-mem state update
- [x] Read path: in-RAM tail merged with Parquet base; predicate eval on tail
- [x] Background compactor: WAL → Parquet → catalog commit → WAL truncate
- [-] `basin-placement` — deferred to v0.2; in-process map works for v0.1
- [-] Consistent hashing — same
- [-] Fast failover — same
- [x] Bench: hot single-row INSERT 22.3k/sec (full pipeline). Cold-start
      latency benchmark deferred (eviction works but not surfaced as a
      dashboard card yet).

## Phase 4 — Routers and SQL (3–4 months) — **mostly shipped**

- [x] `basin-router`: pgwire v3 simple + extended query (Parse/Bind/Describe/
      Execute/Close/Sync), binary + text formats for INT/FLOAT/BOOL/BYTEA/TEXT
- [x] SQL parsing + planning via DataFusion (with point-query fast path
      bypassing DataFusion for `WHERE col = literal`)
- [x] RLS predicate injection — shipped in Phase 5.6, see below
- [x] User-defined `CREATE POLICY` — shipped in Phase 5.6, see below
- [x] Multi-shard fan-out + result merging — router → shard-owner protocol
      shipped (consistent hashing, 28% max load); cross-shard JOIN deferred
- [-] Single-shard transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) — deferred
- [x] Postgres types: int, bigint, text, **bytea**, **boolean**, float8,
      vector(N), **jsonb**, **uuid**, **timestamptz**
      — numeric still deferred
- [-] Indexes: btree / hash — deferred (predicate pushdown + HNSW vector
      cover the wedge)
- [-] Foreign keys — deferred
- [x] **Real ORM compat verified**: 7/7 representative ORM patterns pass via
      `tokio-postgres`'s default extended-query API
- [x] `psql` connects and runs full SQL workload
- [-] `pgx` (Go) / `asyncpg` (Python) full smoke — **deferred (2026-05-19)**.
      Scaffolding ships: `tests/integration/python/smoke.py`,
      `tests/integration/go/smoke.go`, `tests/integration/tests/smoke_pgx.rs`,
      `smoke_asyncpg.rs`. Go side rides on `pgx/v5`. Python side hits an
      asyncpg quirk where after a `DataError` on JSONB / UUID encoding,
      `conn.close()` hangs. tokio-postgres coverage (7/7 ORM patterns + the
      curated ORM/driver-compat suite at commit `8b71eca`) already demonstrates
      the extended-protocol surface end-to-end across native drivers; the
      pgx / asyncpg-specific tests are deferred to v0.2 (fix is `asyncio.wait_for(..., timeout=2)`
      + `DataError` catch on close).

## Phase 5 — Analytical path (1–2 months) — **v0.1 shipped**

- [x] `basin-analytical`: stateless DataFusion pool reading Iceberg directly
      from object storage; Vortex scan layer with projection/predicate
      zone-map pushdown (skip chunks before the object-store GET);
      catalog-statistics file pruning; incremental continuous pre-aggregation
      via `CREATE MATERIALIZED VIEW … WITH (basin.continuous)`;
      surgical custom DataFusion physical operators where benchmarks prove a gap;
      stateless pooled compute enables elastic scale-out over shared object storage
- [x] Planner heuristic to route analytical queries off the OLTP path
      (aggregate / GROUP BY / `/*+ analytical */` hint)
- [-] Bench: 10 TB Iceberg scan — deferred (1M-row covers the wedge)

## Phase 5.5 — Sharding axes beyond per-project (1–3 months)

The primary sharding axis is per-project prefix (already structural). At
scale, four secondary axes show up; each gets its own work item with
explicit tests.

- [x] **Within-project time-based partitioning**: `CREATE TABLE … PARTITION
      BY RANGE (ts)`; reader prunes partitions for time-range predicates.
- [x] **Compute sharding (router → shard owners)**: consistent hashing
      project_id → shard_id; 28% max load skew measured; restart survival.
      Cross-shard JOIN deferred.
- [x] **Tiered storage (hot/cold)**: `ALTER TABLE … SET cold_after = N`;
      compactor moves files between tiers on a sweep. Provider-level
      infrequent-access lifecycle rules wired in.
- [x] **Within-project whale handling (sub-shard)**: project pinning via
      `BASIN_PROJECT_PINS=ulid:idx,...` env var; pinned projects always
      land on the configured shard endpoint regardless of consistent
      hash. v0.2 will move pins into the catalog so they survive cluster
      restart and can be edited at runtime. Original line preserved
      below for context:
      **Within-project whale handling (sub-shard)**: a 100×-larger
      project gets pinned to a dedicated shard owner with bigger
      compute. Cheap because data stays in shared object storage. Folds into the
      compute-sharding work — same router, just pinned mapping.

## Phase 5.7 — Point-query latency: caching + indexes + hot tier (3-6 months)

The critical path to "Postgres-replacement" credibility on point queries
without giving up the prefix-isolation wedge. Three sub-phases by
risk/effort, ordered to ship value early.

**A. Quick wins (4 weeks, high leverage, no architectural shift):**
- [x] **A1 NVMe disk cache** — LRU on local SSD; ~50ms cold S3 fetch →
      ~100µs warm SSD read. **101× speedup measured.** Default-on.
- [x] **A2 Parquet page cache (RAM)** — LRU of decoded RecordBatches;
      <1ms warm hits. **7.24× speedup measured.** Default-on.
- [x] **A3 Bloom filters in Parquet footer** — opt-in per table via
      `ALTER TABLE … SET BLOOM FILTERS ON (col)`; ~80% of nonexistent-id
      queries become row-group skips.
- [x] **A4 Coalesced metadata in catalog** — file-level coalesced
      stats (min / max / null counts per column) lifted from the
      Parquet footer into `DataFileRef::column_stats`; `Storage::read_paths`
      reads only catalog-pruned paths so a fully-out-of-range predicate
      completes with zero object-store IO (no LIST, no HEAD, no footer
      GET). v0.1 prunes at file granularity; row-group-level coalesced
      stats deferred to v0.2 (subsumed by B1 secondary indexes).
      **Unblocks tightening `s3_scaling_perf_stack` bars back to
      ≥5× p50 / <3000ms p99 once a separate measurement run lands —
      bars not moved here pending that.**

**B. Indexing + clustering (8 weeks, real Postgres-class point queries):**
- [ ] **B1 Per-project secondary indexes** — B-tree mapping
      `(table, indexed_col) → (file, row_group, row)`. Stored as a
      separate per-project file. Cached in RAM. `CREATE INDEX` SQL.
      **Flagged as the biggest remaining point-query win in CAPABILITIES.md.**
- [x] **B2 Range-partitioned / Z-ordered files** — physically sorts data
      so related rows live in the same file; combined with A3 bloom
      filters, point queries hit one file. **v0.1 shipped end-to-end:**
      catalog `TableMetadata.cluster_columns` + `Catalog::set_cluster_columns`
      (InMemory + Postgres; fork_table copies it); engine reads the spec
      from catalog at write time; storage writer's `WriteOptions.cluster_columns`
      drives `lexsort_to_indices` + `take` so the Parquet file is
      physically sorted before flush. Empty cluster-column list preserves
      the pre-B2 path byte-equivalently. SQL surface: `CREATE TABLE …
      CLUSTER BY (col, …)`, `ALTER TABLE … CLUSTER BY (col, …)`, and
      `ALTER TABLE … RESET CLUSTER BY` all map to `set_cluster_columns`
      via the same regex-strip pre-screen the rest of Basin's extension
      DDL uses.
- [x] **B3 Per-table row-group sizing** — `ALTER TABLE … SET row_group_rows = N`
      ships; small row groups for point-heavy tables.

**C. Hot tier (6-8 weeks, the architectural commitment):**
- [ ] **C1 In-memory hot ring** — per-project ring buffer for recent
      writes (last 5 min or 100k rows, whichever first). Flushes to
      Parquet on threshold or timer. Reads check ring first, fall
      through to Parquet. Solves "90% of reads are on last week of
      data" for audit-log / event-store / time-series shapes.
- [ ] **C2 Embedded RocksDB hot tier (alt path)** — for projects with
      larger hot working sets that don't fit in RAM. Optional, gated
      by project config.

Decision points:
- A1+A2+A3 alone may be enough to ship sub-10ms warm point queries —
  measure first, then commit to B/C only if needed.
- C is the architectural shift; only commit when a real customer
  workload demands it (multi-week effort, opens new ops surface).

## Phase 5.8 — pg_cron + http extension SQL surface (3 weeks) — **v0.1 shipped**

- [x] **basin-cron** — `cron.schedule(name, schedule, sql)` +
      `cron.unschedule` + `cron.job` + `cron.job_run_details`.
      Background runner per shard. SQL surface (`cron_glue`) lands in v0.2.
- [x] **basin-net** — sync `http_get`/`http_post`; async `net.http_post`
      with `net._http_response` table. Per-project URL allowlist (DENY-ALL
      default), 10 req/s rate limit (burst 30), 10 MiB body cap, 30s timeout.
      SQL surface lands in v0.2.

## Phase 5.6 — Row-level security (1 month) — **v0.1 shipped**

- [x] `CREATE POLICY` SQL surface (Postgres-compatible syntax).
- [x] Catalog stores per-project per-table policies.
- [x] Engine injects predicate filters for `current_user` /
      `current_role` at the logical-plan layer.
- [x] Tests: same-table queries return different rows for different
      authenticated principals; cross-project leak invariant verified.

## Phase 5.9 — Postgres-extension equivalents (ongoing) — **v0.1 shipped**

Same SQL semantics, native Rust crates, no upstream extension loading
(per ADR 0002). All ship Rust API + integration test in v0.1; SQL
surface lands in v0.2 once engine planner is extended to register
the corresponding `ScalarUDF`s.

- [x] **basin-cv** — TimescaleDB continuous-aggregate equivalent. `CvSpec`
      + `CvRefresher::tick`; per-project materialization. v0.1 full
      re-execution; incremental refresh deferred to v0.2.
- [x] **basin-trgm** — pg_trgm equivalent. `similarity`,
      `word_similarity`, `extract`. v0.1 brute-force; GIN trigram index
      deferred to v0.2.
- [x] **basin-geo** — PostGIS subset. `Point`, `Box2d`, `ST_MakePoint`,
      `ST_X`, `ST_Y`, `ST_Distance` (Haversine WGS84), `ST_DWithin`,
      `ST_Contains`. No `LINESTRING`/`POLYGON`/spatial index in v0.1.
- [x] **JSONB type** — Arrow `LargeBinary` + field metadata
      `BASIN_TYPE=JSONB`; canonical-form normalization on insert; pgwire OID 3802.
- [x] **UUID type** — Arrow `FixedSizeBinary(16)` + field metadata
      `BASIN_TYPE=UUID`; pgwire OID 2950 with canonical hyphenated text.
- [x] **pgcrypto / uuid-ossp UDFs** — `digest` (md5/sha1/sha224/256/384/512),
      `encode`/`decode` (hex/base64/escape), `crypt` (bcrypt),
      `gen_salt('bf')`, `gen_random_uuid()`, `uuid_generate_v4()`.

## Phase 5.10 — Identity + REST (open-source bundle) — **v0.1 shipped**

Auth + REST ship as part of the OSS bundle, the same shape Supabase
ships open-source. Hosted-product extensions (enterprise SSO,
BYO-bucket signed URLs, admin audit log, etc) are out of scope for
this OSS roadmap.

### basin-auth (identity) — ADR 0005

- [x] Postgres-backed user store, bcrypt password hashing, JWT
      issuance + verification (HS256), role/membership tables,
      `current_user`-aware project resolution.
- [x] `JwtProjectResolver`, `ApiKeyProjectResolver`, and
      `ProjectCredentialsResolver` auto-mount on pgwire when
      `BASIN_AUTH_ENABLED=1`; provisioned pgwire credentials are
      bcrypt-validated and static `BASIN_PROJECTS` users are limited to
      auth bootstrap/internal use instead of accepting arbitrary passwords.
- [x] REST endpoints for issue / verify / refresh.
- [x] Email-link login — project-agnostic `auth_magic_links` table +
      `AuthService::{request,consume}_email_link`; REST endpoints
      `POST /auth/v1/magic-link` (204 No Content; never confirms whether
      the email exists) and `POST /auth/v1/magic-link/consume`. Returns
      503 / `E_EMAIL_DISABLED` when SMTP is unconfigured.
- [x] Per-project API-key tokens (long-lived, revocable, separate from
      session JWTs) — `auth_api_keys` table + `AuthService::{issue,
      validate, revoke, list}_api_key`; `ApiKeyProjectResolver` stacks
      with the JWT resolver in `basin-server/src/main.rs`; REST
      endpoints `POST/GET /auth/v1/api-keys` and `DELETE /auth/v1/api-keys/{id}`.
- [x] Per-project pgwire connection URLs (the managed-Postgres feel) —
      `auth_project_credentials` table + `AuthService::{provision,
      validate, rotate, list}_project_credentials` (public methods on
      `AuthService`); `ProjectCredentialsResolver` extends the resolver
      trait with `resolve_credentials(user, password)` so the pgwire
      startup handler bcrypt-validates and rejects with SQLSTATE
      `28P01` on mismatch (uniform error — no user-existence leak).
      `Claims::is_admin` gates three new REST endpoints under
      `/admin/v1/projects/*`: provision returns `postgres://...`, rotate
      invalidates the old password, list emits descriptors only.
      Cross-project isolation under per-project URLs is integration-tested
      (UNION + CTE bypass blocked); within-project RLS still applies.
      `BASIN_PROJECTS=alice=*` remains a dev/bootstrap map when auth is
      disabled, and no longer acts as a passwordless customer fallback when
      auth is enabled.
      `BASIN_AUTH_PGWIRE_PUBLIC_HOST` env var configures the host:port
      embedded in the URL.
- [x] Auth schema uniqueness now relies on real table metadata (`UNIQUE`
      constraints plus catalog-persisted index declarations) instead of
      app-side scans only; the engine enforces insert/update duplicates with
      SQLSTATE `23505` semantics.
- [x] Auth consume/refresh security flows avoid SQL transactions and
      `FOR UPDATE`, using guarded single-statement updates so correctness
      no longer depends on unsupported engine transaction semantics.
- [x] Per-user session settings (timezone, language) read by the engine
      for `current_setting()` — `user_session_settings` table +
      `AuthService::{set,get}_session_setting{,s}` with hard-coded
      allowlist (`timezone`, `language`). Engine-side `current_setting()`
      consumer is a separate work item in basin-engine.
- [x] Refresh-token rotation + revocation list — refresh tokens are now
      JWTs (`aud=basin-refresh`, fresh `jti` per issuance). Each rotate
      records the prior `jti` in `auth_revoked_refresh_tokens`; replay of
      a rotated token returns 401 / `E_REVOKED_TOKEN`. Reuse-detection
      path: a presented-already-rotated token *plus* a yet-newer
      rotation in the same user's history triggers a `BLANKET:<user>`
      sentinel that invalidates every outstanding refresh JWT for that
      user. Stale rows are filtered out at lookup (`expires_at > now()`);
      a periodic GC daemon is deferred.
- [x] **Auth per-project schema** — migrate auth tables from reserved
      internal project to each project's own storage namespace; provisioned
      at project creation via `Engine::open_session_as`. See ADR 0013.
- [x] **`AuthStore` trait** — `basin-auth` defines trait + `PostgresAuthStore`;
      `EngineAuthStore` lives in `basin-server` (passed as `Arc<dyn AuthStore>`).
      `AuthService::with_store(cfg, Arc<dyn AuthStore>)` replaces `Mutex<Client>`.
- [x] **Self-routing credentials** — `pgwire_user` format → `{project_id}_{hex}`;
      API keys embed project prefix. Removes global credential lookup table.
- [x] **Remove loopback startup** — delete `DeferredAuthResolver`,
      `wait_for_pgwire_accept`, `INTERNAL_AUTH_PROJECT_ID` static injection.
      Auth starts before pgwire; `StackedProjectResolver` built directly.
- [x] **`auth.uid()` / `auth.role()` / `auth.jwt()`** — SQL session functions
      in the `auth` schema, set from JWT claims at connection time. Enables
      Supabase-style `CREATE POLICY … USING (user_id = auth.uid())`.
      Both `auth.uid()` (schema-dot) and `auth_uid()` (underscore) forms work;
      the executor rewrites the schema-dot form before DataFusion sees it.
      Anonymous sessions return `NULL` / `'anon'` matching Supabase behaviour.
      **Shipped. Supabase-compatible session functions. Both `auth.uid()` and
      `auth_uid()` spellings work.**
- [ ] **Conformance tests** — against `EngineAuthStore` and `PostgresAuthStore`
      (skip PG if unavailable): user uniqueness per project, cross-project same
      email, single-use tokens, refresh rotation, API key lifecycle,
      self-routing credential parsing.

### basin-rest (PostgREST equivalent) — ADR 0006

Requires `BASIN_AUTH_ENABLED=1` per ADR.

- [x] CRUD endpoints over the JWT-resolved project (GET / POST / DELETE)
- [x] Engine `UPDATE` / `DELETE` (Iceberg copy-on-write) — unblocks the
      REST `PATCH` codec
- [x] `PATCH` codec wired to engine `UPDATE` — `build_update_sql`
      produces the right SQL, hits the engine's Iceberg copy-on-write
      `UPDATE` path; `patch_round_trip` integration test asserts
      end-to-end. (lib.rs docstring previously said "501 until engine
      grows UPDATE" — stale; UPDATE shipped, PATCH lights up.)
- [x] Pagination cursors instead of `LIMIT`/`OFFSET` —
      `?cursor=<token>&limit=<n>` ships an opaque base64url JSON
      `{"after_id": <last_id>}` token; response wraps as
      `{rows, next_cursor}` when either param is present, bare array
      otherwise. v0.1 limitation: requires the table's first column to
      be `id BIGINT` (other shapes ignore the cursor). Asserted by
      `pagination_cursor_advances`.
- [x] Streaming responses for large result sets (chunked transfer) —
      `application/x-ndjson` over `Body::from_stream` when the result
      exceeds 1 MiB or 10 000 rows, or when `?stream=true`. Cursor token
      lands as a final `{"_basin_next_cursor":"…"}` NDJSON line.
      Asserted by `streaming_response_for_large_payload`.
- [x] OpenAPI / Swagger schema generation from catalog metadata —
      `GET /rest/v1/_openapi.json` returns a per-project OpenAPI 3.0.3
      spec built from `Catalog::list_tables` + `load_table`. Auth-gated
      so each project only sees its own tables. Type mapping covers
      every Arrow `DataType` Basin's DDL produces (incl. JSONB / UUID
      via the `BASIN_TYPE` field metadata, and `VECTOR(N)` via
      `FixedSizeList<Float32>`).

### pgwire JSONB / UUID parameter-binding wire-format fix

- [x] `StatementSchema` carries parallel `param_is_jsonb` / `param_is_uuid`
      flag vecs (populated from the source column's `BASIN_TYPE` field
      metadata at INSERT / UPDATE / SELECT-WHERE inference time). The
      pgwire layer surfaces OID 3802 / 2950 in `ParameterDescription`,
      and the Bind decoder strips the JSONB v1 `0x01` version byte and
      renders 16-byte UUID buffers as canonical hyphenated strings before
      handing them to the engine's text-substitution path. Asserted by
      `tests/integration/tests/jsonb_uuid_param_binding.rs` (native
      `tokio-postgres` `Uuid` + `serde_json::Value` round-trip) and the
      `decode_param_binary` unit tests. v0.2 follow-up: `WHERE id = $1`
      against a UUID column still fails inside `basin-storage`'s
      fast-select predicate evaluator because the pushed-down
      `ScalarValue::Utf8` is compared against `FixedSizeBinary(16)`;
      lift that limit and the smoke can re-enable the predicate-side
      round-trip.

## Phase 5.11 — Modern SaaS toolkit: SQL functions, declarative lifecycle, sink trait (~12-15 weeks committed)

**Wedge call: new SaaS only.** See [ADR 0012](./docs/decisions/0012-change-event-primitive.md).
Basin is the database new SaaS apps get *built on*, not where legacy PG
schemas migrate. This phase ships user-defined functions, declarative
lifecycle columns, expanded built-ins, enums, and a forward-compatible
**`ChangeEventSink` trait** that future triggers / webhooks / realtime
all plug into — without committing to any of those consumer features
upfront.

What this phase **does NOT** do: PL/pgSQL parser, PL/pgSQL interpreter,
`CREATE TRIGGER … EXECUTE FUNCTION` with a PL/pgSQL body, full PG
`LISTEN`/`NOTIFY` wire-protocol compat, WebSocket realtime, presence
channels. Those are explicit non-goals per ADR 0012; reactors and
webhooks ship in Tier 2 only when Phase 0 customer signal demands them;
WebSocket realtime stays deferred (same workspace, separate crate when
shipped — `crates/basin-realtime/`).

Tiered to keep the committed engineering scope honest:

- **Tier 0** (~3-5 days): `ChangeEventSink` trait + capture point. Zero
  consumers; engine byte-identical to today when no sinks attached.
- **Tier 1** (~12-15 weeks honest): function catalogue + JSONB ops +
  `LANGUAGE sql` scalar functions + declarative lifecycle + enums +
  `CREATE MATERIALIZED VIEW` SQL surface. The committed minimum.
- **Tier 2** (~14-18 weeks, customer-signal-driven): reactors,
  constraint reactors, webhooks (built-in sinks), `RETURNS TABLE`
  functions, `CALL` procedures, generated columns, sequences. Each
  ships independently as Phase 0 interviews show real pull.
- **Tier 3** (~9-12 weeks, larger asks): `information_schema` +
  `pg_catalog` views, WASM UDFs.
- **Deferred** (placeholder): `crates/basin-realtime` — WebSocket
  subscriptions as a `ChangeEventSink` impl; gated on ≥2 design
  partners explicitly asking and unable to bridge an existing realtime
  provider via webhooks.

Phase 0 customer interviews should run **in parallel with Tier 1** so
Tier 2 priorities are customer-driven, not imagined.

### Tier 0 — `ChangeEventSink` trait + capture point (~3-5 days, no deps) ✅ shipped

Forward-compat substrate. Tier 1 phases don't depend on this; Tier 2
phases (reactors + webhooks) do. Cheap to ship now so the executor
commit path doesn't get re-touched repeatedly.

- [x] `ChangeEvent { project, table, op, before, after, committed_at,
      seq, causation_user }` in `basin-common::events`. Stable public
      contract — adding fields fine, renaming breaking.
- [x] `ChangeEventSink` trait (async `publish(&ChangeEvent) -> Result<()>`)
      in `basin-common::events`.
- [x] `EventSinkRegistry` per-engine: separate `pre_commit:
      Vec<Arc<dyn ChangeEventSink>>` + `post_commit:
      Vec<Arc<dyn ChangeEventSink>>` lists. Pre-commit sinks run
      synchronously and abort the mutation on `Err`; post-commit sinks
      run fire-and-forget after the catalog commit succeeds.
- [x] `Engine::attach_pre_commit_sink` + `Engine::attach_post_commit_sink`.
- [x] Capture point in executor's INSERT/UPDATE/DELETE path — exactly
      once per committed mutation; serialized by the existing
      per-`(project, table)` snapshot ID ordering.
- [x] One trivial `TracingSink` (logs each event via `tracing::info!`)
      for debug demos; opt-in via env var, default off.
- [x] Test: zero-sink path is byte-identical to today (no allocation,
      no spawn); attached `TracingSink` records every committed event;
      a pre-commit sink that returns `Err` rolls back the mutation;
      a post-commit sink that returns `Err` does NOT roll back.

### Tier 1 — Ship now (~12-15 weeks honest)

Committed engineering. Independent of Tier 0; ships in parallel.
Customer-visible PG-compat upgrade with zero novel infrastructure.

#### 5.11.A — Built-in function catalogue + JSONB operators + recursive-CTE/window verification (~3-4 weeks) ✅ shipped

The single biggest customer-visible PG-compat win. JSONB operators
folded in because every modern SaaS schema uses them constantly.

- [x] Date/time: `now()`, `current_timestamp`, `current_date`,
      `date_trunc(unit, ts)`, `age(ts1, ts2)`, `extract(field FROM ts)`,
      `to_timestamp(text, fmt)`, `to_char(ts, fmt)`.
- [x] String: `lower`, `upper`, `substring(s FROM n FOR m)`, `trim`,
      `length`, `position`, `replace`, `regexp_replace`, `||` operator.
- [x] Math: `abs`, `ceil`, `floor`, `round`, `power`, `sqrt`, `mod`,
      `%` operator.
- [x] Coalesce / null-handling: `coalesce`, `nullif`, `greatest`,
      `least`, `is distinct from`.
- [x] Aggregate: `string_agg`, `array_agg`, `bit_and`/`bit_or`,
      `every`/`bool_and`/`bool_or`.
- [x] **JSONB operators**: `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`,
      `?|`, `?&` — wired through DataFusion's existing JSON support.
- [x] **Recursive-CTE + window-function verification pass**: DataFusion
      supports both; add an integration test row-by-row covering
      `WITH RECURSIVE` (employee-hierarchy classic), `ROW_NUMBER`,
      `RANK`, `DENSE_RANK`, `LAG`/`LEAD`, `SUM() OVER (PARTITION BY)`.
- [x] Test: a single integration test that exercises every function
      above against `tokio-postgres`'s default extended-query path; no
      panic, results match a real PG reference run committed alongside.

#### 5.11.D — `LANGUAGE sql` scalar functions (~3 weeks, depends on A) ✅ shipped (catalog API + planner inliner + `CREATE FUNCTION` / `DROP FUNCTION` / `ALTER FUNCTION … RENAME TO` SQL surface + mutual-recursion detection at registration)

The function primitive — body is a single SELECT, inlined at planning
time. Covers ~50% of all real-world function use cases. No interpreter,
no frame management, no security sandbox.

- [x] `CREATE FUNCTION name(args) RETURNS scalar LANGUAGE sql AS $$
      SELECT … $$` parser + catalog persistence (`functions` table).
- [x] Planning-time inlining: function call becomes a sub-query in the
      logical plan, with arguments substituted into the body.
- [x] Recursive function detection — reject (PG also rejects for
      `LANGUAGE sql`); recursion needs PL/pgSQL which is out of scope.
      Mutual-recursion detection at registration catches `f → g → f`.
- [x] `DROP FUNCTION`, `ALTER FUNCTION` (rename only).
- [x] Test: `display_name(users)` round-trips; functions composing
      built-ins from 5.11.A work; recursive function rejected.

#### 5.11.B — Declarative lifecycle (`AUTO_UPDATE`, `AUDIT TO`, `SOFT DELETE`) (~2 weeks) ✅ shipped

Covers ~75% of "trigger" use cases without parsing or interpreting
anything. Pure engine-native column behaviour. **Implements the writes
inline in the executor — does NOT depend on Tier 0.**

- [x] `CREATE TABLE foo (..., updated_at TIMESTAMPTZ AUTO_UPDATE)` —
      engine sets the column on every UPDATE row (DEFAULT-now semantics
      for INSERT already work).
- [x] `CREATE TABLE foo (..., AUDIT TO foo_audit)` — every committed
      mutation appends `(op, NEW or OLD, ts, causation_user)` to the
      audit table. Auto-creates the audit table on first reference.
- [x] `CREATE TABLE foo (..., deleted_at TIMESTAMPTZ SOFT DELETE)` —
      `DELETE FROM foo WHERE …` rewrites to `UPDATE foo SET deleted_at
      = now() WHERE …`; `SELECT` filters out non-NULL `deleted_at` by
      default unless caller opts in via `INCLUDE DELETED`.
- [x] Test: each declarative mode round-trips, independent of the
      others; AUDIT mode emits one row per mutation; SOFT DELETE
      round-trips via REST + pgwire.

#### 5.11.K2 — `CREATE TYPE … AS ENUM` + `CREATE DOMAIN` (~2 weeks) ✅ shipped

Reusable typed constraints. Every modern PG schema uses enums for
status columns; domains for reusable validations.

- [x] `CREATE TYPE order_status AS ENUM ('pending', 'paid', ...)` parser
      + catalog (`enum_types` table; one row per `(project, name,
      ordered_labels)`).
- [x] `CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)` parser +
      catalog.
- [x] Type-resolution path: catalog lookup before falling back to
      built-in PG types.
- [x] `ALTER TYPE … ADD VALUE` (append-only — PG enums are
      append-only too).
- [x] `DROP TYPE`, `DROP DOMAIN` (cascade required if any column uses
      the type).
- [x] Test: enum round-trip via REST + pgwire; comparison ordering
      matches declaration order; rejecting unknown enum value;
      domain `CHECK` enforced on INSERT.

#### 5.11.D2 — `CREATE MATERIALIZED VIEW` SQL surface (~1 week) ✅ shipped

Drop the existing `cv_glue` stub. Independent of the other 5.11 work;
the engine plumbing already exists in `basin-cv`.

- [x] `CREATE MATERIALIZED VIEW name AS query WITH (basin.continuous,
      refresh_interval = '5m', ...)` SQL surface → existing
      `Catalog::set_continuous_aggregate`.
- [x] `REFRESH MATERIALIZED VIEW name` SQL form → `CvRefresher::tick`
      one-shot.
- [x] `DROP MATERIALIZED VIEW`.
- [x] Test: SQL round-trip (CREATE → refresh → SELECT → DROP) against
      the engine; refresh-on-schedule wire-up survives a process restart.

### Tier 2 — Customer-signal-driven (~14-18 weeks, ship as Phase 0 demands)

Each independent. Each plugs into the Tier 0 trait. Order below is
suggested-priority; real order is whatever Phase 0 surfaces.

#### 5.11.C — SQL-bodied reactors (`REACT ON … EXECUTE`) (~2 weeks, depends on Tier 0 + 5.11.A) ✅ shipped (machinery: `ReactorSink` pre-commit + `register_reactor` catalog API + AST-level NEW/OLD/TG_OP substitution; ALTER TABLE SQL surface; constraint reactors via `__basin_assert` UDF for SQLSTATE 23514; `DROP REACTOR` parser)

The trigger primitive. `ReactorSink` implements `ChangeEventSink`,
attached as **pre-commit** so reactor failures abort the mutation.

- [x] `ALTER TABLE … REACT ON {INSERT|UPDATE|DELETE} [WHEN (predicate)]
      EXECUTE <sql_statement>` parser surface.
- [x] Reactor registry in catalog (`reactors` table; one row per
      `(project, table, name, ops, when_predicate, body)`).
- [x] `ReactorSink` impl: on event, evaluate `WHEN` predicate, run body
      via existing engine path with `NEW` / `OLD` / `TG_OP` /
      `TG_TABLE_NAME` substituted.
- [x] `DROP REACTOR`, `ALTER REACTOR` (rename only for v0.1).
- [x] Test: counter-denormalization reactor (parent.child_count++);
      audit-side reactor; reactor fails → mutation rolls back; reactor
      respects RLS (sees full row, downstream SELECT applies filter).

#### 5.11.C2 — Constraint-shaped reactors (`REACT … CONSTRAINT`) (~1 week, depends on 5.11.C)

Project-scoped invariant enforcement without a body. Covers "max 100
rows per project", "free-tier caps", "hierarchical depth limit".

- [ ] `ALTER TABLE … REACT ON INSERT CONSTRAINT (predicate)` —
      predicate evaluated against NEW + the current table state; if
      false, mutation aborts with SQLSTATE `23514 check_violation`.
- [ ] Test: cap-at-N rejection works; cap-at-N allows under the cap;
      constraint with subquery against a sibling table works.

#### 5.11.I — Webhook fanout (~4-5 weeks honest, depends on Tier 0) ✅ shipped (machinery: `crates/basin-webhooks` ships `WebhookSink` + retry queue + dead-letter + per-project counters/p99-latency observability; `ALTER TABLE … SUBSCRIBE WEBHOOK` / `UNSUBSCRIBE WEBHOOK` SQL surface; predicate evaluation via `predicate_eval` module against ChangeEvent JSON payload)

Replaces "trigger fires HTTP" with a retryable, idempotency-keyed
fanout. `WebhookSink` implements `ChangeEventSink`, attached as
**post-commit** with its own disk-backed retry queue. Lives in a new
`crates/basin-webhooks` workspace member; reuses `basin-net` for
the actual HTTP path.

- [x] `ALTER TABLE … SUBSCRIBE WEBHOOK TO '<url>' ON {INSERT|UPDATE|
      DELETE} [WHERE …]` parser + catalog persistence.
- [x] Disk-backed retry queue (`basin-wal` sidecar; idempotency-keyed
      so dupes don't double-process); worker drains the queue with
      exponential backoff to the configured URL via `basin-net`.
- [x] Dead-letter after `max_retries` (configurable, default 16);
      surface dead letters via a `webhook_dead_letters` table.
- [x] Reuses basin-net's URL allowlist + per-project rate limit + body
      cap + timeout — already tested.
- [x] Stale-subscription cleanup: customer endpoint down for > 24h →
      auto-pause subscription + audit log entry; resume requires
      explicit `RESUME WEBHOOK`.
- [x] Test: webhook fires on matching event; retries on transient HTTP
      failure; idempotency key dedupes after retry; dead-letter row
      created after max_retries; webhook does NOT fire when WHERE
      predicate is false; auto-pause kicks in after sustained failures.

#### 5.11.E — `LANGUAGE sql RETURNS TABLE` functions (~2 weeks, depends on 5.11.D) ✅ shipped

Multi-row return — function call becomes a derived table at planning
time. Same inlining trick as scalar functions.

- [x] `CREATE FUNCTION name(args) RETURNS TABLE(col1 type, ...)
      LANGUAGE sql AS $$ SELECT … $$` parser + catalog.
- [x] Planning-time inlining as a derived table.
- [x] Test: `recent_orders(uid)` round-trips; `SELECT * FROM
      recent_orders(...)` planning works alongside JOINs.

#### 5.11.F — Multi-statement `CALL` procedures (~2 weeks, depends on 5.11.D)

Multi-statement workflows for onboarding, archive, periodic tasks.
Sequence of SQL statements with parameter binding, no control flow.

- [ ] `CREATE PROCEDURE name(args) LANGUAGE sql AS $$ stmt1; stmt2;
      … $$` parser + catalog.
- [ ] `CALL name(args)` — runs each statement in order with arguments
      substituted; transactional once Phase 5 single-shard transactions
      ship; until then, best-effort sequential.
- [ ] Test: `CALL archive_project(t)` round-trip; multi-project isolation
      preserved through the call; failure mid-procedure leaves earlier
      statements applied (until single-shard transactions ship).

#### 5.11.K — Generated columns (`GENERATED ALWAYS AS … STORED`) (~2 weeks) ✅ shipped

Modern PG syntax for computed columns persisted at write time. Cleaner
than `LANGUAGE sql` functions for the simplest case.

- [x] `CREATE TABLE foo (..., full_name TEXT GENERATED ALWAYS AS
      (first_name || ' ' || last_name) STORED)` parser + catalog.
- [x] Engine evaluates the expression on every INSERT/UPDATE row;
      reads return the stored value.
- [x] Reject `INSERT`/`UPDATE` writing directly to a generated column
      with SQLSTATE `42601 syntax_error`.
- [-] `VIRTUAL` (computed-on-read) variant explicitly out of scope for
      v0.1 — STORED only.
- [x] Test: generated column round-trip; expression composing built-ins
      from 5.11.A; rejection of direct write.

#### 5.11.K3 — Sequences (`CREATE SEQUENCE`, `nextval`, `currval`) (~2 weeks) ✅ shipped (catalog API + scalar UDFs `nextval`/`currval`/`setval` + `CREATE SEQUENCE` / `DROP SEQUENCE` SQL surface + multi-option grammar via textual pre-screen — works around sqlparser 0.52's single-option limitation per the production fix)

Custom auto-increment, gap-tolerant counters. Most new SaaS uses ULID/
UUID, but a real slice still wants sequences for human-readable IDs.

- [x] `CREATE SEQUENCE name [START n] [INCREMENT n]` parser + catalog
      (`sequences` table; per-project; persisted current value).
- [x] `nextval(name)`, `currval(name)`, `setval(name, n)` functions.
- [x] `DEFAULT nextval('seq_name')` column default integration.
- [x] Concurrent-safety: per-`(project, sequence)` mutex around the
      increment; cached blocks of N for high-rate sequences (cache
      size from `WITH CACHE n` clause).
- [x] `DROP SEQUENCE` (cascade if columns reference it).
- [x] Test: 10 concurrent `nextval` calls produce 10 distinct values;
      cache flushed on engine restart (gap is acceptable, duplicate
      is not); cross-project isolation (project A's `nextval` doesn't
      touch project B's sequence).

### Tier 3 — Larger asks

#### 5.11.M — `information_schema` + `pg_catalog` read-only views (~6-8 weeks honest) ✅ shipped (17 views total: `information_schema.tables`/`columns`/`routines`/`views`/`schemata`/`table_constraints`/`key_column_usage`/`referential_constraints` + `pg_catalog.pg_class`/`pg_attribute`/`pg_namespace`/`pg_proc`/`pg_type`/`pg_constraint`/`pg_index`/`pg_depend`/`pg_authid`. PostgREST/pgAdmin/ORM (Prisma/Sequelize/SQLAlchemy) startup-query compat verified by integration tests. Once 5.11's PK+CHECK+FK enforcement landed, `pg_constraint`/`table_constraints`/`key_column_usage`/`referential_constraints` populate with real PK/CHECK/FK rows. `pg_index` populates when 5.7 B1 secondary indexes ship.)

The gate for proper PG-ecosystem tooling. Every introspecting tool
(PostgREST, pgAdmin, DataGrip, schema-migration tools, every ORM that
introspects) queries these. Without them, the "PG-compatible" claim
fails at first contact with real tooling. **PostgREST alone runs ~200
catalog queries on startup.**

- [x] `information_schema.tables`, `.columns`, `.key_column_usage`,
      `.table_constraints`, `.referential_constraints`, `.routines`,
      `.parameters` (subset), `.views`, `.schemata`.
- [x] `pg_catalog.pg_class`, `.pg_attribute`, `.pg_namespace`,
      `.pg_index`, `.pg_constraint`, `.pg_type`, `.pg_proc`, `.pg_depend`,
      `.pg_authid` (`pg_am` deferred — none of the bootstrap-query tooling
      we tested probes it).
- [x] Project-scoped: each project sees only its own objects in catalog
      views (RLS-style filter built into the view definition).
- [x] Tooling integration tests: PostgREST + pgAdmin + Prisma + Sequelize
      + SQLAlchemy startup queries verified by
      `tests/integration/tests/postgrest_pgadmin_compat.rs` +
      `tests/integration/tests/orm_compat.rs`.
- [x] Documented compat matrix: which PG-specific oid columns return
      stable values, which return NULL, which raise (see CAPABILITIES.md
      `information_schema` + `pg_catalog` row).

#### 5.11.J — WASM UDFs (~3-4 weeks, customer-gated)

Custom imperative computation as WebAssembly. Escape hatch for the
~5% of cases `LANGUAGE sql` can't express. Gated on Phase 0 customer
demand per ADR 0012's revisit clause — if customers haven't asked,
don't build.

- [ ] `CREATE FUNCTION name(args) RETURNS type LANGUAGE wasm AS '<base64
      bytes>'` parser + catalog persistence.
- [ ] `wasmtime` runtime per-call; sandboxed by construction.
- [ ] CPU + memory caps per invocation (deterministic shutdown on
      overrun).
- [ ] Test deferred — implement when shipped.

### Deferred — `crates/basin-realtime` (placeholder, same workspace)

WebSocket realtime + presence channels as `ChangeEventSink`
implementations. Lives in `crates/basin-realtime/` when it ships —
same shape as `crates/basin-cron/`, `crates/basin-net/`. The Tier 0
trait is the public seam; engine doesn't change.

**Gated on:** ≥2 design partners (Phase 0) explicitly asking for
server-pushed realtime updates that webhooks can't satisfy AND unable
to bridge an existing realtime provider (Pusher / Ably / Supabase
Realtime / their own WebSocket layer) to Basin's webhook fanout. See
ADR 0012's "Trigger to revisit" section.

If a separate repo later makes more sense (independent release cadence,
ecosystem signal), the workspace member becomes a separate repo via a
one-day `git mv` + new `Cargo.toml`. Both paths stay open; same-repo
default is just lower-friction at current scale.

### Decision trade-off (read before reopening this scope)

This phase **commits Basin to "new SaaS only"** as the trigger /
function / realtime story. Legacy PG migrations that depend on
hand-written PL/pgSQL can't drop in their existing schema unchanged
— they translate trigger bodies (mechanical for ~95% of real-world
cases per the schema audit; the other ~5% is a real porting cost the
customer bears).

The trade-offs:

- **Lose:** the "drop in any PG schema unchanged" claim. Customers
  with deeply legacy enterprise PG schemas are not Basin's wedge.
- **Win:** wedge clarity ("the multi-project DB designed for new
  SaaS"), bounded engineering scope, no permanent PL/pgSQL maintenance
  load, clean trait-shaped extensibility for future sinks, no novel
  realtime infrastructure shipped speculatively.

Tier 1 (~12-15 weeks honest) is a clear win regardless of Phase 0
signal. Tier 2 phases each ship independently as customers ask. Tier 3
is the bigger commitment — `information_schema` (5.11.M) is the
unblocker for proper PG-ecosystem tooling and worth the 6-8 weeks once
Tier 1 customers are in production. Reopen ADR 0012 only if both gating
conditions in its "Trigger to revisit" section are met.

## Phase 5.12 — Storage perf & Vortex (PR #161 / #162) — **shipped**

See [ADR 0015](./docs/decisions/0015-vortex-storage-format.md). Vortex
opted-in 2026-05-11, flipped to default 2026-05-18 with the correctness
prerequisites done. ~50 perf commits landed across #161 + #162. The
88-shape `vortex_vs_parquet_smoke` battery characterizes the win
honestly: ~15-40× on metadata-only aggregates, 1.3-1.7× on most join
shapes, on-par-to-better on scans, and explicitly trailing on
point-lookup latency (≈0.65×) and `ORDER BY … LIMIT` (≈0.38×).

- [x] **5.12.A** Vortex codec encode/decode + writer wiring — `vortex`
      0.70 + BtrBlocks cascade with `.with_compact()`. Files:
      `crates/basin-storage/src/vortex_format.rs`. Acceptance gate:
      Vortex round-trip integration test passes (commit `8b17e35`,
      `57a9152`, `15414d3`).
- [x] **5.12.B** Per-table `WITH (basin.file_format = 'vortex')` opt-in
      — Lanes 1–8 in commits `ff51efd` … `654c339`. Files:
      `crates/basin-catalog/src/metadata.rs` (`TableFileFormat`),
      `crates/basin-engine/src/executor.rs` (WITH-clause strip).
      Acceptance gate: end-to-end opt-in CREATE/INSERT/SELECT round-trip
      (`f8f7b13`).
- [x] **5.12.C** Vortex-default flip (2026-05-18) — `988fe7d`,
      `7dbe214`. Acceptance gate: zero net regressions vs the
      pre-existing Parquet baseline; `orm_compat` 19/19;
      `sql_support_matrix` green.
- [x] **5.12.D** Self-describing Vortex decode — `vortex_format::decode`
      recovers Arrow schema from the file's own `DType` when no catalog
      schema is supplied. `Utf8View`/`BinaryView` normalised to
      canonical `Utf8`/`Binary`. Commits `88459dd`, `e2a1d27`,
      `ba53254`.
- [x] **5.12.E** Differential Vortex⇆Parquet correctness harness — asserts
      byte-identical results across point / range / inequality / IS NULL /
      string-eq / compound / aggregate / GROUP BY / ORDER BY+LIMIT /
      projection / full-scan + DELETE/UPDATE rewrite, on multi-file tables.
      File: `tests/integration/tests/vortex_parquet_differential.rs`
      (commit `b99cbc6`). Acceptance gate: 0 differential rows.
- [x] **5.12.F** Metadata-only aggregate fast path (~30-40×) — bare
      `COUNT/SUM/MIN/MAX` bypass DataFusion and answer from Vortex
      footer / catalog `column_stats`. Files:
      `crates/basin-engine/src/fast_select.rs`. Commits `2e4610a`,
      `727b7b6`, `649e1dc`. Acceptance gate: `aggregate_full` shape goes
      from 1.2× to 38×.
- [x] **5.12.G** `basin.sort_by` compound DDL option (WEDGE 4) — declares
      `file_sort_order`; the writer enforces it via `lexsort_to_indices`
      + `take` before flush. Commits `905de49`, `00107eb`. Files:
      `crates/basin-storage/src/writer.rs`. Acceptance gate: window
      shapes whose `PARTITION BY` / `ORDER BY` match the declared sort
      recover scan-as-presorted plans.
- [x] **5.12.H** `basin.row_block_size` per-table DDL option — per-table
      chunk granularity. Commit `dc8cd96`. File:
      `crates/basin-engine/src/executor.rs`.
- [x] **5.12.I** FileMetadataCache wired into RuntimeEnv + VortexFooterCache —
      eliminates per-iteration footer re-parse. Commits `d26a92d`,
      `f5c01ef`.
- [x] **5.12.J** Utf8/Binary → Utf8View promotion in schema. Commit
      `8e471ef`. File: `crates/basin-engine/src/executor.rs`.
      **VIEWPROMOTE — retirement note:** redundant the moment
      DataFusion 54+ ships full Utf8View UDF support. Track upstream
      `apache/datafusion#<viewtype-udf>`; file an upstream issue
      cataloguing the UDF gaps Basin worked around; retire this rewrite
      when subsumed.
- [x] **5.12.K** UNION ALL of same-table scans → single scan + OR. Commit
      `47860f1` (+ `a2d9641` projection restore). File:
      `crates/basin-engine/src/executor.rs`. **UNIONSCAN — upstream
      candidate:** this is a clean logical-plan rewrite that has no
      Basin-specific dependency. File as a DataFusion logical optimizer
      RFC and retire when accepted upstream.
- [x] **5.12.L** `NULLIF(a,b) IS [NOT] NULL` → conjunction analyzer
      rewrite. Commits `218a5c6`, `6f32a08` (+ revert/restore noise).
      File: `crates/basin-engine/src/executor.rs`. **NULLIF rewrite —
      upstream candidate:** belongs in DataFusion's `SimplifyExpressions`
      analyzer pass; file an issue + PR upstream and retire the Basin
      copy when subsumed.
- [x] **5.12.M** Low-cardinality GROUP BY COUNT(*) fast path — commit
      `cd5b626`. File: `crates/basin-engine/src/fast_select.rs`.
- [x] **5.12.N** STREAMLIMIT — force single-partition stream for OFFSET on
      sort-matching scan. Commit `f7f1d9e`. File:
      `crates/basin-engine/src/executor.rs`. **STREAMLIMIT — retirement
      note:** retire when DataFusion's `LimitPushdown` handles
      single-partition streaming for sort-matching OFFSET. File the
      upstream issue describing the gap (`OFFSET N` with a presorted
      input that matches the requested order should not require a
      coalescing step).
- [x] **5.12.O** 88-shape `vortex_vs_parquet_smoke` benchmark battery —
      commits `fe9b37f`, `d00592c`, `19278fb`, `da83f0c`. File:
      `tests/integration/tests/vortex_vs_parquet_smoke.rs`. Acceptance
      gate: the smoke battery runs green at the configured scale
      (10k/25k/100k; 1M opt-in) and produces the per-shape ratio
      reported in CHANGELOG / WEDGE.

## Phase 5.13 — pg_query parser migration (ADR 0014)

See [ADR 0014](./docs/decisions/0014-pg-query-as-canonical-parser.md).
Three migration phases; Phase 1 in flight, Phase 2 starting, Phase 3
gated on Phase 2 completion.

- [x] **5.13.A** libpg_query as canonical front-end (Phase 1) — `pg_query`
      6.x vendored; `crates/basin-engine/src/pg_ast.rs` ships `parse`,
      `ParseTree`, `stmt_kind`, `StmtKind`, `reject_unsupported`. Every
      incoming statement parses through the real PostgreSQL 16 parser
      first; unsupported kinds rejected with SQLSTATE 0A000 before
      sqlparser sees them. `BASIN_PG_QUERY` env gate is on by default.
      Acceptance gate: every `tests/integration/tests/*` passes with
      the gate on; rejected statements emit clean 0A000 not opaque
      sqlparser errors. Files: `crates/basin-engine/src/pg_ast.rs`,
      `crates/basin-engine/src/executor.rs`.
- [~] **5.13.B** Typed AST matches replacing textual prescreens (Phase 2)
      — the 14 textual pre-screens in `executor.rs` migrate to `pg_ast`
      AST matches one at a time. Agents 2–4 split: (i) DDL pre-screens
      (`ALTER TYPE … ADD VALUE`, `CREATE DOMAIN`, Basin-specific
      `ALTER TABLE` extensions), (ii) DML / mutation pre-screens, (iii)
      function / procedure / reactor pre-screens. Acceptance gate per
      migration: pre-screen test suite passes with the AST match
      replacing the regex; no new diffs in `sql_support_matrix`.
      Files: `crates/basin-engine/src/executor.rs` (per-pre-screen),
      `crates/basin-engine/src/pg_ast.rs` (typed matches).
- [ ] **5.13.C** Unconditional pg_query path (Phase 3) — Agent 5 flips
      `BASIN_PG_QUERY` unconditional and removes the sqlparser
      front-end from the executor's hot path. sqlparser stays only for
      legacy module-internal node bodies until Phase 2's full translator
      lands. Acceptance gate: env var removed; sqlparser dependency
      gated to translator-only.

## Phase 5.15 — Unified docs platform (HIGHEST PRIORITY — blocks 5.14)

**Why first.** Contributor and customer onboarding both go through documentation.
Today docs sit in three places (`README.md`, `CAPABILITIES.md`, `docs/*.md` in
this repo) with no rendered surface. As OSS surface grows (future `basin-js`,
`basin-cli`) and `basin-cloud` ships its own admin docs, we need a single
rendered site that pulls each product's markdown into a unified UI without
forcing contributors to leave their own repo.

**Architecture.** Each OSS repo keeps its `docs/` as standard markdown with
YAML frontmatter. `basin-cloud`'s webapp runs `npm run dev:docs` (or its CI
equivalent) which build-time-fetches each OSS repo via `git clone --depth=1`,
copies `docs/` into `basin-cloud/webapp/content/oss/<product>/`, and renders
the union via Docusaurus or Mintlify. `basin-cloud` keeps its own
cloud-specific docs under `content/cloud/`, separate from imported OSS
content. Pin by git tag for versioned docs per OSS release.

Eight items, ordered for incremental shipping. Items 5.15.A–5.15.D land in
this OSS repo; 5.15.E–5.15.I land in `basin-cloud` (separate repo).

- [ ] **5.15.A** Frontmatter spec — define the YAML schema OSS markdown
      files use (`title`, `nav_section`, `sidebar_position`, `summary`,
      optional `version_since` and `version_until`). Files:
      `docs/frontmatter-spec.md` (new) + cross-reference in
      `docs/decisions/README.md`. Acceptance gate: schema documented;
      one ADR-eligible decision recorded ("why YAML frontmatter, not
      MDX or sidecar TOML").
- [ ] **5.15.B** Migrate existing Basin OSS docs to the frontmatter
      spec. Files: `docs/architecture.md`, `docs/deployment.md`,
      `docs/multi-project.md`, `docs/sql-compatibility.md`,
      `docs/sql-support.md`, `docs/scaling/*.md`, every ADR in
      `docs/decisions/*.md`. Acceptance gate: every `.md` under
      `docs/` parses with the spec; one CI check enforces it on every
      PR (`scripts/check-frontmatter.sh` or similar).
- [ ] **5.15.C** Top-level docs index — `docs/README.md` becomes the
      OSS-repo-internal navigation manifest, ordered by `nav_section` +
      `sidebar_position`. Used both by GitHub's directory rendering
      (humans browsing the OSS repo) and by basin-cloud's fetcher.
      Acceptance gate: every `.md` referenced; clicking any link works
      from `https://github.com/basin/basin/blob/main/docs/README.md`.
- [ ] **5.15.D** Stub repository skeletons for `basin-js` and
      `basin-cli` — even before either ships code, set up their
      `docs/` folder with placeholder frontmatter so basin-cloud's
      fetcher has a target. Files: separate repos (not this one).
      Acceptance gate: each repo's `docs/` has at least a
      `getting-started.md` with valid frontmatter; basin-cloud's
      fetcher can pull both without 404.
- [ ] **5.15.E** `basin-cloud` webapp: pick Docusaurus or Mintlify.
      Lives in the separate cloud repo. Acceptance gate: locally
      `npm run dev` renders a "hello world" docs page.
- [ ] **5.15.F** `basin-cloud` webapp: `npm run dev:docs` script —
      build-time fetch of each OSS repo's `docs/` into
      `webapp/content/oss/<product>/`. Files: cloud repo. Acceptance
      gate: fresh checkout + `npm install` + `npm run dev:docs` +
      `npm run dev` shows OSS docs at `/docs/basin/architecture` etc.
- [ ] **5.15.G** `basin-cloud` webapp: cloud-only docs namespace under
      `webapp/content/cloud/` — billing, dashboards, security,
      scaling-as-a-service. Acceptance gate: cloud docs render at
      `/docs/cloud/*` without colliding with imported OSS content.
- [ ] **5.15.H** Cross-product link resolver — `[[basin-js:auth/login]]`
      → canonical URL on the rendered site. Files: cloud repo
      (Docusaurus plugin or Mintlify config). Acceptance gate: a
      cross-reference from a Basin doc resolves to the right
      basin-js page in the rendered site.
- [ ] **5.15.I** CI sync — webhook on each OSS repo's `main` triggers
      basin-cloud rebuild; nightly cron as a safety net. Files:
      `.github/workflows/notify-cloud-docs.yml` in each OSS repo +
      basin-cloud receiver. Acceptance gate: merging a docs-only PR
      in this OSS repo causes the cloud rendered site to update
      within ~15 minutes.

**Out of scope for 5.15:** search (post-launch via Algolia or built-in),
versioned docs UI dropdown (post-launch — Docusaurus + Mintlify both
support; deferred until we cut v0.2), localisation, in-page edit links
back to the OSS repo (nice-to-have for contributor flow; track as
follow-up).

**Why before 5.14:** the storage / HTAP / sketches work in 5.14 will
generate new docs (API reference for new SQL surfaces like
`APPROX_COUNT_DISTINCT`, `WITH (basin.row_block_size = …)`, hot-buffer
operator notes). Landing 5.15 first means every new feature is
documented from day one in the place customers and contributors look.

## Phase 5.14 — Durable Basin moat: HTAP + catalog-driven optimization (next 3 months)

**Strategic framing (2026-05-19 stakeholder discussion):** Basin's
durable advantage is the **catalog + storage-orchestration + multi-tenant
layer**. The next 3 months invest there exclusively. Optimizer-rule
shims that will eventually be subsumed by upstream Vortex / DataFusion
are explicitly deprioritized — we file those as upstream PRs instead
(see retirement notes in Phase 5.12.J / .K / .L / .N).

Four items, ordered by independent shippability. Each has a file scope
and an acceptance gate so it can be picked up by a sonnet agent.

### 5.14.A — Catalog blooms (~1 week, no deps)

Per-file bloom filters in `DataFileRef.bloom_filters`, computed at write
time for `global_sort_order` columns and probed in
`fast_select.rs::execute_simple_select` before opening the Vortex file.
Targets the `point_eq` miss path — today a miss still pays catalog
metadata + footer open + decode.

Design spec is complete (was task #23 in an earlier sweep). Storage
layer lives in `crates/basin-catalog/src/metadata.rs` (`DataFileRef`).

- [ ] **5.14.A1** Add `bloom_filters: HashMap<ColumnName, SerializedBloom>`
      field to `DataFileRef`. Files: `crates/basin-catalog/src/metadata.rs`.
      Acceptance gate: round-trip serialise/deserialise via InMemory +
      Postgres impls.
- [ ] **5.14.A2** Writer-side bloom computation for declared
      `basin.sort_by` columns. Files: `crates/basin-storage/src/writer.rs`.
      Acceptance gate: every committed file with `basin.sort_by` set
      carries blooms for those columns.
- [ ] **5.14.A3** Probe in `execute_simple_select` before file open.
      Files: `crates/basin-engine/src/fast_select.rs`. Acceptance gate:
      the `point_eq` miss shape in `vortex_vs_parquet_smoke` drops file
      opens to zero (count via a perf counter exported through
      `Engine::project_counters`).
- [ ] **5.14.A4** Differential test: bloom false-negative rate is zero
      (a bloom that says "definitely not present" must never be wrong);
      bloom false-positive rate is bounded by configured target.
      Files: `tests/integration/tests/catalog_blooms.rs`. Acceptance
      gate: 1M-row 4-column bloom suite passes; FPR ≤ 1%.

**Acceptance gate (composite):** `point_eq` miss shape in
`vortex_vs_parquet_smoke` improves by ≥3× over today's baseline; the
differential harness confirms zero false-negatives.

### 5.14.B — HLL + t-digest sketches (~3-4 weeks, no deps on 5.14.A)

Per-file HyperLogLog (cardinality) and t-digest (quantiles) sketches
stored in catalog metadata. Powers `APPROX_COUNT_DISTINCT` UDF and
`APPROX_PERCENTILE` UDF that no upstream component can ever provide
for Basin — sketches are catalog-layer state, not query-engine state.

Closes `count_distinct` and `percentile_cont` gaps via an
approximation alternative. Sketches merge across files at query time.

- [ ] **5.14.B1** Add `hll_sketches: HashMap<ColumnName, SerializedHll>`
      and `tdigest_sketches: HashMap<ColumnName, SerializedTDigest>`
      to `DataFileRef`. Files: `crates/basin-catalog/src/metadata.rs`.
      Acceptance gate: round-trip serialise/deserialise via InMemory +
      Postgres impls.
- [ ] **5.14.B2** Writer-side sketch computation: HLL for every column,
      t-digest for numeric columns. Use `hyperloglog-rs` + `tdigest`
      crates (or established equivalents — pick at PR time). Files:
      `crates/basin-storage/src/writer.rs`. Acceptance gate: every
      committed file carries sketches.
- [ ] **5.14.B3** `APPROX_COUNT_DISTINCT(col)` UDF — merges HLLs from
      catalog-pruned files and returns estimate. Files:
      `crates/basin-engine/src/udfs/approx.rs` (new module).
      Acceptance gate: ≤2% error vs exact `COUNT(DISTINCT col)` on the
      88-shape battery.
- [ ] **5.14.B4** `APPROX_PERCENTILE(col, p)` UDF — merges t-digests
      and returns quantile. Files: `crates/basin-engine/src/udfs/approx.rs`.
      Acceptance gate: ≤1% absolute error vs exact `percentile_cont(p)`
      on the 88-shape battery.
- [ ] **5.14.B5** Differential test: 1M-row table; assert sketch-merge
      results within bounds across every file count from 1 to 100.
      Files: `tests/integration/tests/sketches.rs`. Acceptance gate:
      every shape within bounds.

**Acceptance gate (composite):** `count_distinct` and `percentile_cont`
gaps close via the `APPROX_*` alternative; correctness bounds (≤2%
HLL, ≤1% t-digest) honoured on the differential harness.

### 5.14.C — Row-format hot buffer for HTAP (~3 months on its own — the architectural moat)

The architectural commitment. LSM-style memtable for recent writes
(row-formatted, PK-indexed). Flushed to Vortex on threshold. Reads
merge memtable + Vortex. Closes the OLTP `point_eq` HIT path floor
and single-row UPDATE latency. **This is what differentiates Basin
from "Parquet + DataFusion in a bucket."**

Precedent: SingleStore (rowstore tier), ClickHouse `ReplacingMergeTree`,
Apache Pinot (real-time segments). All three couple a row-formatted
hot tier with a column-formatted cold tier and merge at read time.

Lives in a new crate `crates/basin-hottier/` (separate from `basin-shard`
because the lifecycle is different and the data structures don't share
much with the WAL→Parquet compactor).

- [ ] **5.14.C1** `MemTable` data structure — Arc-shared, lock-free reads,
      single-writer per-`(project, table)`. PK-indexed B-tree or
      skip-list; row-formatted Arrow `StructArray` slabs. Files:
      `crates/basin-hottier/src/memtable.rs` (new). Acceptance gate:
      benchmark `memtable_insert` ≥ 100k rows/s single-thread debug,
      ≥ 1M rows/s release.
- [ ] **5.14.C2** Write-path integration — engine INSERT/UPDATE/DELETE
      writes to memtable instead of WAL+Parquet for hot tier. WAL still
      durability-anchors. Files:
      `crates/basin-engine/src/executor.rs` (mutation path),
      `crates/basin-shard/src/lib.rs` (route hot vs cold). Depends on
      5.14.C1. Acceptance gate: `single_row_update_latency` p99 drops
      ≥10× vs today's Parquet-rewrite path.
- [ ] **5.14.C3** Read-merge: scan path merges memtable rows with the
      Vortex base. Tombstone resolution for DELETE; latest-version-wins
      for UPDATE per PK. Files:
      `crates/basin-hottier/src/merge.rs` (new),
      `crates/basin-engine/src/scan.rs` (integration). Depends on
      5.14.C1 + 5.14.C2. Acceptance gate: `point_eq` HIT shape in
      `vortex_vs_parquet_smoke` lands at p99 ≤ 2ms warm.
- [ ] **5.14.C4** Flush — threshold-driven (size + age) Vortex compaction
      from memtable. Atomic catalog commit; memtable rows GC'd post-commit.
      Files: `crates/basin-hottier/src/flush.rs` (new). Depends on
      5.14.C1+C2+C3. Acceptance gate: 1M-row mixed workload flushes
      without read-stall; no row loss verified by differential.
- [ ] **5.14.C5** Multi-tenant isolation: per-project memtable cap +
      shared heavy resource (the in-process slab allocator), cheap
      per-tenant primitive (counter + semaphore) — per [Multi-tenant
      isolation must scale](file:///Users/pc/.claude/projects/-Users-pc-code-exo-basin/memory/feedback_multitenant_isolation.md).
      Files: `crates/basin-hottier/src/registry.rs` (new). Depends on
      C1. Acceptance gate: per-project byte usage scales O(bytes), not
      O(pool); a 10k-tenant fuzz test fits in a 4GB heap.
- [ ] **5.14.C6** Differential harness vs Parquet-only path — every
      shape in `vortex_vs_parquet_smoke` runs identically when the
      hot tier is empty (no rows in memtable), and identically when
      everything is in memtable (all rows in memtable, no Vortex files
      yet). Files: `tests/integration/tests/hottier_differential.rs`.
      Acceptance gate: 0 differential rows.

**Acceptance gate (composite):** OLTP `point_eq` HIT p99 ≤ 2ms warm;
single-row UPDATE p99 ≤ 5ms; 10k-tenant fuzz fits in 4GB heap; 0
differential rows vs Parquet-only baseline.

### 5.14.D — Adaptive write-time multi-sort + catalog-aware WindowExec (~6 weeks combined, no deps)

Two write-time / planner pieces that rely on Basin's catalog as a
planning input that upstream cannot see. Closes `order_by_multi`,
`window_partition_sum`, and `lag_lead_window`.

- [ ] **5.14.D1** Query-history collector — record observed
      `ORDER BY` / `PARTITION BY` shapes per `(project, table)` in
      catalog metadata. Files:
      `crates/basin-catalog/src/query_history.rs` (new). Acceptance
      gate: top-N shapes per table queryable via internal API.
- [ ] **5.14.D2** Compaction-time multi-sort — compactor consults the
      query-history top-N and re-sorts the output Vortex file by the
      most common shape (currently the compactor only emits insertion
      order or the declared `basin.sort_by`). Files:
      `crates/basin-shard/src/compactor.rs`. Depends on 5.14.D1.
      Acceptance gate: `order_by_multi` shape in
      `vortex_vs_parquet_smoke` improves ≥2× after a compaction sweep.
- [ ] **5.14.D3** Catalog-aware `WindowExec` — custom DataFusion
      `ExecutionPlan` that consults `basin.sort_by` / discovered
      file-sort-order via the catalog and skips the full sort when the
      window's `PARTITION BY` matches. Files:
      `crates/basin-engine/src/physical/window_exec.rs` (new).
      Acceptance gate: `window_partition_sum` + `lag_lead_window` plans
      drop the `SortExec` step when sort-matching is detected, verified
      by `EXPLAIN ANALYZE`.
- [ ] **5.14.D4** Multi-sort + catalog-aware WindowExec differential —
      assert identical results vs the today-baseline plan. Files:
      `tests/integration/tests/catalog_window.rs`. Acceptance gate: 0
      differential rows on the relevant 88-shape battery slice.

**Acceptance gate (composite):** `order_by_multi` ≥2× faster after
compaction; `window_partition_sum` + `lag_lead_window` drop their
`SortExec`; 0 differential rows.

### Dependencies + sequencing

- **5.14.A** (blooms) can start immediately, no deps.
- **5.14.B** (sketches) can start immediately, no deps on A.
- **5.14.D** (multi-sort + WindowExec) can start immediately, no deps
  on A or B.
- **5.14.C** (hot tier) is the 3-month commitment — start once A is in
  and at least one of B or D is in motion, to avoid blocking the moat
  on the easier wins.

### Explicitly NOT in 5.14 (deprioritized per 2026-05-19 strategic call)

- Further tactical optimizer rules that look like 5.12.J / .K / .L / .N.
  Those go upstream as DataFusion / Vortex PRs; the Basin copy retires
  when subsumed.
- Any feature that does not depend on Basin's catalog + multi-tenant
  layer for correctness (i.e., anything that could equally well ship in
  upstream Vortex or upstream DataFusion).
- WebSocket realtime (still gated per ADR 0012 trigger).

## Phase 6 — Production hardening (3–4 months)

- [ ] Multi-region: regional WAL + S3 cross-region replication
- [~] Catalog replication strategy chosen and implemented — strategy
      chosen in [ADR 0010](./docs/decisions/0010-catalog-replication.md)
      (single-writer global Postgres + regional read replicas via PG
      logical replication); implementation phases tracked there. v0.1
      implementation deferred per the ADR's own milestone gating.
- [~] Point-in-time restore via Iceberg snapshots — v0.1 catalog-level
      `Catalog::rollback_to_snapshot(project, table, snapshot_id)` ships
      (InMemory + Postgres impls; truncates history to ≤ target,
      rewinds head pointer). v0.2 follow-up: physical file GC of
      orphaned post-rollback Parquet files (today the OLTP listing-based
      reader still sees them until a compactor sweep). Reads on
      APPEND-only history work after rollback once orphans are removed;
      crossing UPDATE/DELETE commits is unrecoverable in v0.1 because
      replaced files are physically deleted at commit (soft-delete is
      a v0.2 prerequisite for cross-DML rollback).
- [~] Branching / forking via copy-on-write catalog metadata — v0.1
      catalog-level `Catalog::fork_table(project, src, dst)` ships
      (InMemory + Postgres impls). New table inherits source's schema /
      snapshot history / partition spec / RLS / tier / bloom / row-group
      / CV settings; data files are *shared by reference* (no Parquet
      copy). v0.2: cross-project forking (needs refcount-aware GC).
- [x] Migration Manager v0.2 catalog ops shipped (project-wide list /
      diff / rollback; default fan-out + Postgres single-query
      optimisation).
- [ ] Cross-shard 2PC
- [x] Connection pooling (✅ ADR 0007), rate limiting (✅ pgwire side:
      `BASIN_PGWIRE_RATE_LIMIT_QPS=100` token-bucket per project via
      `governor`, mapped to SQLSTATE 53400; basin-net side ✅), cost-based
      query rejection (✅ v0.1: `BASIN_QUERY_COST_LIMIT_ROWS=N` rejects
      single-table SELECTs that estimate above the cap with SQLSTATE
      54000; multi-FROM / JOIN / sub-query / explicit-LIMIT pass through
      unchecked. v0.2 will use A4 catalog `ColumnStats` for selectivity-
      aware estimates on multi-table shapes.)
- [x] Per-project + per-query + per-shard + per-WAL telemetry — `basin_common::ProjectCounterRegistry` aggregates ops/bytes_read/bytes_written/errors + ring-window p99 latency per project; `Engine::project_counters(&ProjectId) -> ProjectCountersSnapshot` exposes a cheap snapshot. Storage writer/reader and WAL append are wired to bump per-project byte counters; engine `ProjectSession::execute` bumps op + latency + error.
- [~] BYO-key envelope-encryption hooks — `EncryptionProvider` trait shipped in `basin-storage::encryption`; `Storage::attach_encryption_provider` is the additive opt-in (default `None` = byte-for-byte plaintext path). Writer envelope-encrypts the Parquet body with a fresh per-file AES-256-GCM data key and persists the wrapped key as a `<path>.wrapped` sidecar; reader transparently unwraps. External callers plug their own KMS adapter into the trait — the OSS engine ships only the trait + envelope hooks.

> Hosted-product hardening items (BYO-bucket, Stripe billing,
> enterprise auth/REST extensions) are out of scope for this OSS
> roadmap. The trait + envelope-hook surface for BYO-key lives in
> basin-storage per the line above.

## Phase 7 — Launch (ongoing)

- [ ] Onboard the Phase 0 design partners
- [~] CLI (`basinctl`) + engineering docs — CLI ✅ shipped:
      `services/basinctl/` with `ping`, `projects`, `tables`, `query`,
      `version`. Engineering docs continuation: ADRs / architecture
      overview / operator runbook still open.
- [ ] Open beta after 3–6 months of design partner usage
- [ ] GA when uptime + perf + DX are all genuinely good

---

## Cross-cutting (start now, never finish)

- [x] Per-project metrics from day one (ops/s, p50/p99, RAM, S3 IO, active hours) — public `Engine::project_counters` API surfaces ops/bytes/errors + p99 ms estimate aggregated across engine + storage + WAL; viability test `tests/integration/tests/viability_per_project_counters.rs` asserts per-project byte isolation.
- [x] OpenTelemetry traces wired through router → shard → WAL — `#[tracing::instrument]` spans on every layer (router/engine/shard/storage::{read,write_batch,read_paths}/WAL append/flush/read_from/truncate); OTLP export available via `BASIN_OTLP_ENDPOINT`.
- [x] Cross-project fuzz tests (find a bug → file a P0) — `tests/integration/tests/fuzz_cross_project_isolation.rs` runs a seed-reproducible (`BASIN_FUZZ_SEED`) StdRng fuzzer of 1000 random query shapes across 8 projects, asserts every returned row carries the calling project's payload prefix, and verifies `TableName::new` rejects path-traversal inputs. No isolation breach found.
- [x] Feature-coverage + security suite — shipped at `tests/integration/tests/feature_coverage.rs` (one assertion per CAPABILITIES.md ✅ row, with audit comment cross-referencing the test that already covers each row) and `tests/integration/tests/security.rs` (OWASP-shaped pgwire SQL-injection probes through both simple and extended-bind paths, path-injection on `TableName`/`ProjectId`/`PartitionKey`, RLS bypass attempts via UNION/CTE, structural cross-project fork rejection, and pgwire rate-limit enforcement). All 12 security tests pass — `collect_table_refs_from_query` in `executor.rs` walks `SetExpr::SetOperation`, `query.with` CTEs, `TableFactor::Derived` subqueries, `TableFactor::NestedJoin`, and embedded subqueries (EXISTS/IN/scalar) so RLS predicate injection cannot be bypassed via UNION / CTE / subquery shapes.
- [ ] Bug bounty program before public beta
- [ ] Security review at each phase boundary

## Known v0.1 gaps — refreshed 2026-05-19

The original 2026-05-12 production-benchmark gap list is largely closed.
Everything tracked here is the **honest live set**; the canonical
per-syntax matrix is auto-generated from tests and lives at
[`docs/sql-support.md`](./docs/sql-support.md) — that is the source of
truth, this list is the human-readable summary.

**Shipped since 2026-05-12 (struck from the gap list):**

- ✅ `DROP TABLE [IF EXISTS]` — `drop_table_removes_existing_table` (task #49,
  commit `10288ed`).
- ✅ `CREATE TABLE IF NOT EXISTS` — `if_not_exists: true` honoured;
  `create_table_if_not_exists_creates_when_absent` /
  `create_table_if_not_exists_noop_when_exists` (task #49, commit `10288ed`).
- ✅ `INSERT … ON CONFLICT DO NOTHING` — single-column conflict-target match
  suppresses UNIQUE violation (#75, commit `b66403e`).
- ✅ `INSERT … ON CONFLICT DO UPDATE` — `table.col` + `EXCLUDED.col` resolution
  (#74, commit `a49e58a`).
- ✅ Real BEGIN/COMMIT/ROLLBACK + SAVEPOINT — `f4127e9` (#92, completes #83):
  defer commits in-tx, ROLLBACK undo, SAVEPOINT stack, aborted state. Driver-
  implicit `BEGIN TRANSACTION READ WRITE` no longer rejects.
- ✅ `UPDATE … SET col = <expression>` / `UPDATE … SET col = (SELECT …)` —
  expressions in assignment RHS, including scalar subqueries (#106, commit
  `3e619e0`); `UPDATE … WHERE id IN (SELECT …)` restored after #66 regressed it
  (#76, commit `0efaf93`).
- ✅ `pg_catalog` UDFs for meta-commands — 20 stubs shipped (`pg_table_is_visible`,
  `pg_get_userbyid`, `pg_get_function_arguments/result/identity_arguments`,
  `pg_get_expr`, `pg_get_indexdef`, `format_type`, `pg_get_constraintdef`,
  `pg_total_relation_size`, etc.). See CAPABILITIES.md psql `\dt` row.

**Remaining honest v0.2 gaps:**

- [ ] `LATERAL` joins — uncorrelated strip + nested-aggregate ORM rewrite
  shipped (commit `4faa5d7`); correlated decorrelation for non-aggregate row-
  returning bodies (#81, commit `6f7ab3c`); full advanced LATERAL still partial.
- [ ] `WITH RECURSIVE` + DML-in-CTE — multi-column RECURSIVE shipped (#82,
  commit `dae8765`); data-modifying CTEs shipped (commit `6056dca`); the
  combination of recursive + DML in one CTE chain still partial.
- [ ] Advanced window frames (`RANGE INTERVAL` / `GROUPS` / `EXCLUDE`).
- [ ] `EXCLUDE USING gist` — not on roadmap (geo-index dependency).
- [ ] `SELECT *` Arrow-projection failure on the rare `BIGSERIAL PK + TEXT +
      TEXT UNIQUE + TIMESTAMPTZ DEFAULT now()` shape — root-caused to a
      type-OID encoding mismatch on the wire (Arrow → pgwire row description);
      workaround is explicit-column SELECT.

**Live coverage:** see [`docs/sql-support.md`](./docs/sql-support.md) for the
full per-fragment matrix (latest test expansion to 697 fragments via commit
`39d51bb`).

## Critical rules (from the brief — re-read before scope-creep)

- Don't build Raft, the SQL parser, or the table format from scratch.
- The WAL is the durability boundary, **not** S3.
- Cold start under 200 ms or hobbyists pick Turso.
- One leaked row across projects and the project dies.
- If you start implementing distributed 2PC + MVCC + a SQL planner from scratch,
  stop — you've drifted from the wedge.
