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

> **Test-first phase ladder (Phase 5.19+):** every phase leads with
> `.A — test harness` that lands FIRST and reports RED against the
> current engine. Every implementation task references which named
> slice of the harness it flips from red → green. Agents picking up
> a sub-task in isolation know what "done" looks like by reading the
> slice description.

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
- [x] **B1 Per-project secondary indexes** — B-tree mapping
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
- [-] **C1 In-memory hot ring** — **SUBSUMED by Phase 5.14.C HTAP hot tier**
      (the `basin-hottier` crate: per-`(project,table)` memtable + WAL-backed
      write path + read-merge + flush + budget, all shipped 5.14.C1–C6). The
      ring-buffer design here is obsoleted by the LSM-style memtable that
      actually landed. Closed as superseded.
- [-] **C2 Embedded RocksDB hot tier (alt path)** — **NOT pursued.** ADR 0016
      explicitly rejected RocksDB for the hot tier (multi-project isolation
      constraint) in favour of the per-project `BTreeMap` memtable that
      shipped in 5.14.C. Closed as superseded.

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
- [x] **Conformance tests** (shipped `bca0a2b`; 4 new conformance functions in `basin-auth::store::conformance`; shared runner via `test-utils` feature flag; PostgresAuthStore graceful-skip when `BASIN_AUTH_TEST_POSTGRES_URL` unset; bug-fixes in `engine_auth_store.rs` for Vortex/UUID + alphabetical column order + StringViewArray) — against `EngineAuthStore` and `PostgresAuthStore`
      (skip PG if unavailable): user uniqueness per project, cross-project same
      email, single-use tokens, refresh rotation, API key lifecycle,
      self-routing credential parsing.

### basin-auth v2 — OAuth + MFA — ADR 0020

Lifts ADR 0005's OAuth + MFA deferral. Unblocks basin-js's 🔒
`signInWithOAuth` / `auth.mfa.*` stubs. All flows in OSS `basin-auth`;
cloud builds only provider-registration / factor UI.

#### 5.10.O — OAuth providers (presets + generic OIDC, ~2-3 weeks)

- [x] `auth.oauth_providers` table (per-project): provider, client_id,
      client_secret (encrypted via `EncryptionProvider`), scopes,
      redirect_uri, OIDC discovery/endpoints. `auth.identities` table
      (user_id, provider, provider_user_id). Also `auth.oauth_states`
      for in-flight PKCE state. Migrations in `schema.rs`.
- [x] Provider presets (Google, GitHub, Apple) with endpoint/scope
      defaults; a preset row needs only client_id + secret.
- [x] Generic OIDC via RFC 8414 discovery (`.well-known/openid-
      configuration`); explicit-endpoint fallback.
- [x] `GET /auth/v1/authorize?provider=&redirect_to=` — signed `state`
      (CSRF, HMAC-SHA256) + PKCE `code_challenge`; 302 to provider.
      Routed via `AuthService::begin_oauth_authorize`.
- [x] `GET /auth/v1/callback` — validate state, exchange code (PKCE
      verifier), fetch userinfo, link/create user, issue Basin JWT +
      refresh. Routed via `AuthService::handle_oauth_callback`.
- [x] Identity linking on provider-asserted **verified** email;
      `redirect_to` validated against per-project allowlist
      (open-redirect guard).
- [x] Crates: `hmac` (HMAC-SHA256 state signing), `reqwest` (OIDC
      discovery + token exchange + userinfo), `subtle` (constant-time
      comparison) — all already in workspace. Custom PKCE via `sha2` +
      `base64`. Note: used `hmac`+`reqwest` instead of the `oauth2` crate
      (same security model, lighter API surface).
- [x] Tests: `tests/integration/tests/oauth_flow.rs` — mock provider
      (axum HTTP server); full authorize→callback→JWT; CSRF-state
      rejection; PKCE S256; verified-email linking; open-redirect
      rejection. 17 tests, all pass.

#### 5.10.M — MFA: TOTP + WebAuthn/passkeys (~3-4 weeks) — ✅ complete

> Core landed (basin-auth `mfa.rs`, 123 unit tests green): factors/challenges/
> recovery-codes tables, `aal`/`amr` JWT claims + `auth.aal()`, enroll/verify/
> challenge/step-up endpoints, recovery codes. Integration tests fixed and green:
> `mfa_totp.rs` (11 tests) + `mfa_webauthn.rs` (7 tests). Fix: added
> `AuthStore + MfaStore` stub impls on `MfaCache` under `cfg(any(test,
> feature="test-utils"))` so `None::<&MfaCache>` satisfies the generic bounds.

- [x] `auth.mfa_factors` (id, user_id, type ∈ {totp, webauthn}, status,
      secret/credential — encrypted), `auth.mfa_challenges` (short TTL),
      `auth.mfa_recovery_codes` (argon2-hashed, single-use).
- [x] JWT carries `aal` (`aal1`/`aal2`) + `amr` (methods). New SQL
      session fn `auth.aal()` alongside `auth.uid()`/`role()`/`jwt()`.
- [x] Endpoints: `POST /auth/v1/factors` (enroll: TOTP secret+otpauth
      URI / WebAuthn creation challenge), `/factors/:id/verify`,
      `/factors/:id/challenge`, `/factors/:id/challenge/verify`
      (re-issues aal2 JWT), `DELETE /factors/:id` (requires aal2).
- [x] Recovery codes issued at first enrollment; single-use; rate-
      limited via existing `governor`.
- [x] Downgrade guard: once a factor is enrolled, JWT caps at aal1
      until a challenge succeeds; aal2-requiring RLS fails closed.
- [x] Crates: `totp-rs`/`oath` (RFC 6238), `webauthn-rs` (FIDO2).
- [x] Tests: `tests/integration/tests/mfa_totp.rs` (11) +
      `tests/integration/tests/mfa_webauthn.rs` (7) — enroll/verify/
      challenge/step-up; recovery-code single-use; TOTP replay
      rejection; aal2 RLS gating. All 18 green.

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
      `decode_param_binary` unit tests. ~~v0.2 follow-up: `WHERE id = $1`
      against a UUID column still fails inside `basin-storage`'s
      fast-select predicate evaluator because the pushed-down
      `ScalarValue::Utf8` is compared against `FixedSizeBinary(16)`;
      lift that limit and the smoke can re-enable the predicate-side
      round-trip.~~ **Resolved** — `ScalarValue::Utf8` is now coerced to
      16-byte `FixedSizeBinary` before comparison in
      `crates/basin-storage/src/predicate.rs`; regression tests added in
      `predicate::compound_tests` (`uuid_eq_literal_matches_row`,
      `uuid_eq_no_match_returns_all_false`, `uuid_eq_invalid_literal_is_error`,
      `uuid_eq_via_compound_predicate`). Fixed in commit d0f0f5e.

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
- **Tier 2** (~14-18 weeks): reactors, constraint reactors, webhooks
  (built-in sinks), `RETURNS TABLE` functions, `CALL` procedures,
  generated columns, sequences. Each ships independently; parallelise
  where the file scopes don't overlap.
- **Tier 3** (~9-12 weeks, larger asks): `information_schema` +
  `pg_catalog` views, WASM UDFs.
- **Tier 4** (~10-12 weeks): `crates/basin-realtime` — WebSocket +
  SSE + presence channels as `ChangeEventSink` impls (5.11.R series
  below). Committed to ship.

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

### Tier 2 — Built-in sinks + function/procedure surface (~14-18 weeks)

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

#### 5.11.C2 — Constraint-shaped reactors (`REACT … CONSTRAINT`) (~1 week, depends on 5.11.C) ✅ shipped (`cdf3f99` — async-frame stack-exhaustion fix via tokio::task::spawn in reactor_sink.rs; 5/5 reactor_constraint tests)

Project-scoped invariant enforcement without a body. Covers "max 100
rows per project", "free-tier caps", "hierarchical depth limit".

- [x] `ALTER TABLE … REACT ON INSERT CONSTRAINT (predicate)` —
      predicate evaluated against NEW + the current table state; if
      false, mutation aborts with SQLSTATE `23514 check_violation`.
- [x] Test: cap-at-N rejection works; cap-at-N allows under the cap;
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

#### 5.11.F — Multi-statement `CALL` procedures (~2 weeks, depends on 5.11.D) ✅ shipped (`59e8a10`; engine 9/9 + integration 5/5; transaction semantics via shipped BEGIN/SAVEPOINT machinery)

Multi-statement workflows for onboarding, archive, periodic tasks.
Sequence of SQL statements with parameter binding, no control flow.

- [x] `CREATE PROCEDURE name(args) LANGUAGE sql AS $$ stmt1; stmt2;
      … $$` parser + catalog.
- [x] `CALL name(args)` — runs each statement in order with arguments
      substituted; wrapped in an implicit BEGIN/COMMIT (or SAVEPOINT
      when inside an outer transaction) — all-or-nothing semantics via
      the Phase 5 single-shard transaction machinery.
- [x] Test: `CALL archive_project(t)` round-trip; multi-project isolation
      preserved through the call; failure mid-procedure rolls back earlier
      statements via implicit transaction.

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

#### 5.11.J — WASM UDFs (~3-4 weeks)

Custom imperative computation as WebAssembly. Escape hatch for the
~5% of cases `LANGUAGE sql` can't express. Committed to ship.
Pairs with ADR 0019's RPC mount (5.11.L) — once both land, the
`/rpc/<fn>` route transparently dispatches `LANGUAGE wasm` bodies
alongside `LANGUAGE sql`.

- [x] `CREATE FUNCTION name(args) RETURNS type LANGUAGE wasm AS '<base64
      bytes>'` parser + catalog persistence.
- [x] `wasmtime` runtime per-call; sandboxed by construction.
- [x] CPU + memory caps per invocation (deterministic shutdown on
      overrun).
- [x] Tests: `cargo test -p basin-engine wasm_udf` + integration suite pass.

#### 5.11.L — `POST /rest/v1/rpc/<fn>` mount (ADR 0019) ✅ shipped

PostgREST-style HTTP RPC surface over catalog UDFs. Tracked via ADR 0019.

- [x] Route `POST /rest/v1/rpc/:fn_name` wired in `crates/basin-rest/src/routes/rpc.rs`.
- [x] Auth: JWT / API-key bearer gate (identical to `/rest/v1/<table>`).
- [x] Body: JSON object → named args → positional call via catalog lookup;
      falls back to insertion order when function is not in catalog.
- [x] Response: scalar unwrap for single-row/single-column results; array
      of objects for `RETURNS TABLE`; `{"ok":true,"tag":…}` for empty.
- [x] Both `LANGUAGE sql` (5.11.D) and `LANGUAGE wasm` (5.11.J) dispatch
      transparently — engine routes by language; route just builds the SQL.
- [x] Integration tests: `rpc_sql_scalar_function`, `rpc_requires_auth`,
      `rpc_zero_arg_function` in `crates/basin-rest/src/tests.rs`.

#### Phase 5.11.W — Wasm functions: JS/TS authoring (~6-8 weeks)

The committed answer to "do you have edge functions?" per
[ADR 0019](./docs/decisions/0019-declarative-baas-surface.md)'s
"V8 never; WebAssembly is the function runtime" decision. Devs author
in TypeScript; the toolchain compiles to a Wasm component (Javy /
ComponentizeJS) at deploy time; it runs on the **already-shipped**
wasmtime runtime (5.11.J) and is invokable via the **already-shipped**
`/rpc/<fn>` mount (5.11.L) plus a new HTTP-handler shape. **No V8, no
isolate pool, no separate runtime.** Any Wasm-targeting language works;
JS/TS is the headline DX. Critical path: W1 → (W2 ∥ W3) → W4 → W7.

##### 5.11.W1 — Wasm component-model host ABI (~2 weeks, engine)

5.11.J runs Wasm as a *pure scalar UDF* (no side effects). W1 gives a
function-Wasm a host-import interface so it can do real work, on the
WASI Preview 2 / component model so any guest language targets one ABI.

- [ ] Host imports: `basin:fn/query` (run SQL under the caller's
      identity — RLS applies), `basin:fn/http` (outbound HTTP via
      `basin-net`: allowlist + rate-limit + body-cap + timeout already
      in place), `basin:fn/log` (tracing), `basin:fn/secret` (read a
      project secret, decrypted via `EncryptionProvider`).
- [ ] WIT interface definitions in `crates/basin-engine` (or a new
      `crates/basin-fn`); guest bindings generated from the WIT.
- [ ] Acceptance: a hand-written Rust→Wasm component calls
      `basin:fn/query("SELECT 1")` and `basin:fn/http(...)` and gets
      correct results through the host.

##### 5.11.W2 — HTTP-handler function shape `/fn/v1/:name` (~1.5 weeks, engine + basin-rest)

Beyond `/rpc/<fn>` (calls a function, serialises its return), add a
request→response handler ABI so a function receives an HTTP request
(method, headers, body) and returns a response — the edge-function
ergonomic shape, in Wasm.

- [ ] `basin:fn/handler` WIT export: `handle(request) -> response`.
- [ ] Mount `ANY /fn/v1/:name` in `basin-rest`; JWT/API-key gated like
      other routes; project-scoped.
- [ ] Acceptance: a JS function `export default (req) => Response.json({...})`
      compiled to Wasm responds correctly to a `curl` against
      `/fn/v1/<name>`.

##### 5.11.W3 — JS/TS→Wasm build pipeline in basin-cli (~1.5 weeks, basin-cli)

- [ ] `basin functions deploy ./fn.ts` runs ComponentizeJS/Javy →
      Wasm component targeting the W1 ABI → uploads + registers.
- [ ] `basin functions list` / `logs <name>` / `delete <name>`.
- [ ] Acceptance: round-trip — write a TS handler, `deploy`, invoke via
      `/fn/v1/<name>`, see logs via `functions logs`.

##### 5.11.W4 — `@basin/functions` authoring SDK (~1 week, basin-js)

- [ ] Typed bindings for the W1 host imports (`query` / `http` / `log`
      / `secret`); a project template; a local test harness that runs a
      function against a mock host without deploying.
- [ ] Acceptance: `import { query } from '@basin/functions'` type-checks;
      template `npm test` runs a handler against the mock host green.

##### 5.11.W5 — Per-invocation resource governance (~1 week, engine)

Extends 5.11.J's caps for the host-import + handler case (functions can
now block on host calls).

- [ ] Epoch-interrupt CPU cap, linear-memory cap, wall-clock timeout,
      per-project concurrency semaphore. Configurable via
      `BASIN_FN_*` env / per-project DDL.
- [ ] Acceptance: a function that spins / allocates / hangs is killed at
      the cap; a noisy project's function load doesn't starve others.

##### 5.11.W6 — Function catalog + lifecycle (`LANGUAGE javascript`) (~1 week, engine)

- [ ] `CREATE FUNCTION name(...) LANGUAGE javascript AS $$ … $$` sugar —
      stores the compiled Wasm component + source; `DROP` / `ALTER …
      RENAME`; version pin. (Compile happens client-side in W3; the
      engine stores + runs the component.)
- [ ] `basin:fn/query` host call is RLS-aware and `auth.aal()`-aware
      (ADR 0020) so MFA-gated logic works inside functions.
- [ ] Acceptance: catalog round-trip; RLS enforced on in-function
      queries; cross-project isolation verified.

##### 5.11.W7 — Differential + soak tests + docs (~1 week) ✅ shipped

- [x] Differential: a reference TS function and its hand-written SQL
      equivalent produce identical results across a battery of inputs.
      `tests/integration/tests/wasm_functions_differential.rs` covers
      scalar TEXT, scalar INT, RETURNS TABLE, and error propagation
      across the `basin:functions/query` host-trait path (the exact
      ABI a ComponentizeJS-produced guest takes). 4/4 green.
- [x] Soak: 100 projects × concurrent function invocations.
      `tests/integration/tests/wasm_functions_soak.rs` ships a short
      variant (100 projects × 50 invocations, ~seconds) covering the
      three invariants — bounded memory (per-project semaphore LRU
      cap holds; no leaked permits), caps honoured (per-project peak
      in-flight never exceeds `project_concurrency`), no cross-project
      interference (quiet-project p99 stays within 5× baseline under a
      noisy-project load; same shape as
      `basin-bench-harness::profiles::noisy_neighbor`). A 1-hour
      `#[ignore]` long variant is documented inline for releases.
      Short variant 2/2 green; long variant ignored in CI.
- [x] Docs: [`docs/functions.md`](./docs/functions.md) — authoring,
      host ABI, deploy, limits; explicit "this is Wasm, not V8" note
      linking [ADR 0019](./docs/decisions/0019-declarative-baas-surface.md).
      `BASIN_FN_*` cap matrix + audit cross-refs included.

### Tier 4 — `crates/basin-realtime` (~10-12 weeks)

WebSocket + SSE + presence channels as `ChangeEventSink`
implementations. New crate `crates/basin-realtime/` — same shape as
`crates/basin-cron/`, `crates/basin-net/`. The Tier 0 trait is the
public seam; engine doesn't change.

Implementation broken into file-scope-disjoint sub-tasks so multiple
can land in parallel. Critical path: R1 → R2 → (R3 || R4) → R7.

#### 5.11.R1 — Crate skeleton + trait wiring (~1 week) ✅ shipped (`4c10663`)

- [x] New `crates/basin-realtime/` with `Cargo.toml`, `src/lib.rs`,
      module stubs (`sse`, `ws`, `presence`, `filter`, `budget`).
      Workspace `Cargo.toml` `[workspace.members]` adds the crate.
- [x] `RealtimeSink` struct implementing `ChangeEventSink`
      (post-commit). Attaches via `Engine::attach_post_commit_sink`
      from `services/basin-server` behind the `realtime` Cargo
      feature (ADR 0018).
- [x] Per-project ring buffer (`DashMap<ChannelKey, broadcast::Sender>`
      keyed by `(project, table)`; capacity 1024 events default).
- [x] Replay cursor that consumes the existing webhook retry log
      (5.11.I) for catch-up after a client reconnect — same
      durable-log source for both webhooks and realtime.
- [x] Acceptance gate green: empty crate compiles; `RealtimeSink` attaches;
      a synthetic `ChangeEvent` published by the engine appears on
      the in-memory broadcast channel. 5/5 unit tests pass.

#### 5.11.R2 — SSE adapter (~2-3 weeks) ✅ shipped (`800d6d2`)

- [x] axum streaming response handler mounted at
      `/realtime/v1/sse/:project/:table` (path scoping mirrors
      `/in/<project>/<name>` from 5.11.N).
- [x] JWT auth identical to `basin-rest` (ADR 0006); RLS-equivalent
      filtering applied to the event stream so a subscriber only
      sees rows their policies permit (via `sse.rs::rls_permits` —
      causation_user ownership + admin/service_role bypass; seam for
      R5 predicate pushdown).
- [x] Heartbeat comment frames every 15s via axum `KeepAlive::new()`.
- [x] `Last-Event-Id` header support for replay-on-reconnect via the
      shared retry-log cursor.
- [x] Body cap + per-connection rate limit reuse basin-net knobs.
- [x] Acceptance gate: `curl -N -H "Authorization: Bearer …"
      .../realtime/v1/sse/proj/orders` receives JSON events on every
      INSERT/UPDATE/DELETE; reconnecting with `Last-Event-Id`
      replays missed events from the retry log.
- [x] Integration test:
      `tests/integration/tests/realtime_sse.rs` covering connect /
      replay / disconnect / RLS-filtering / cross-project-isolation.
      34 tests pass (R1's 5 + 6 new SSE + concurrent-agent additions).

#### 5.11.R3 — WebSocket adapter (~2 weeks on top of R2) ✅ shipped (`10858d8`)

- [x] axum WebSocket handler mounted at
      `/realtime/v1/ws/:project`. Single connection multiplexes
      multiple table subscriptions via subprotocol messages.
- [x] Subscribe / unsubscribe protocol messages (JSON-framed
      control plane on the same WS connection).
- [x] Bidirectional ping / pong for liveness; tokio-tungstenite's
      built-in support is sufficient.
- [x] Reuses R2's ring buffer + replay machinery; only the transport
      adapter differs.
- [x] Disconnect protocol: server-initiated close with reason code
      on auth failure / RLS denial / project deletion.
- [x] Acceptance gate: `wscat` connects, subscribes to two tables,
      receives interleaved events, unsubscribes one mid-stream
      without closing the connection.
- [x] Integration test: `tests/integration/tests/realtime_ws.rs`
      covering multi-subscription / unsubscribe / RLS / ping-pong /
      reconnect. 6/6 pass.

#### 5.11.R4 — Presence channels (~2 weeks) ✅ shipped (`cdf3f99`)

- [x] Per-project presence registry: `(project, channel) → Set<{
      client_id, metadata }>`. In-memory; ephemeral. `DashMap<(ProjectId,
      ChannelName), Arc<ChannelPresence>>`.
- [x] Presence protocol over WS: `track` / `untrack` /
      `presence_state` / `presence_diff` messages (Phoenix Channels shape).
- [x] Heartbeat-driven liveness: heartbeat refreshes TTL; missed
      heartbeats evict after 90s (`PresenceConfig` TTL=90s/sweep=30s;
      background eviction task).
- [x] Filter pushdown: registry-key isolation per `(project, channel)`.
- [x] Acceptance gate: two clients join the same channel; each sees
      the other in `presence_state`; one disconnects; the other
      receives a `presence_diff` with the leaver.
- [x] Integration test:
      `tests/integration/tests/realtime_presence.rs`. 64 basin-realtime tests pass.

#### 5.11.R5 — Subscription filter pushdown (~1 week) ✅ shipped (`94db513`)

- [x] Subscriber-side predicate (`WHERE` clause) evaluated at fanout
      time against the `ChangeEvent` JSON payload (same
      `predicate_eval` module 5.11.I uses for webhook subscriptions;
      `subscribe_filtered` → `FilteredReceiver`).
- [x] Predicate parsed at subscribe time; compiled once (`Arc<Expr>`); reused per
      event.
- [x] Acceptance gate: subscribing to `orders` with predicate
      `status = 'paid'` only delivers events where the new row's
      status is paid; bench shows ≤ 50µs predicate eval per event
      (observed: simple_eq ~64ns, AND ~104ns, IS NULL ~28ns). 15/15 tests.

#### 5.11.R6 — Multi-project memory budget (~1 week) ✅ shipped (`d1acaa0`)

- [x] Shared bounded buffer + per-project byte counter (`BudgetTracker`
      = `DashMap<ProjectId, ProjectBudget>`, lazy per-project alloc, single
      `AtomicU64` + `hard_cap` each; lock-free CAS `try_reserve`; RAII
      `BudgetGuard`). Pattern from `feedback_multitenant_isolation`: cost is
      O(bytes-in-flight) per project, not O(active_subscribers).
- [x] Per-project hard cap (default 16 MiB in-flight; configurable
      via `BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES`).
- [x] When a project's quota fills, new events drop into the durable
      retry log; in-memory fast path is best-effort.
- [x] Acceptance gate: 1k-project fuzz with one noisy project pushing
      10x events/sec — other projects' p99 delivery latency
      unaffected (sub-ms); noisy project sees `BUFFER_FULL` on its own
      subscription only. 8/8 budget tests + fuzz smoke.

#### 5.11.R7 — Differential harness + soak test (~1 week)

- [x] Differential: every shape in `change_event_smoke` runs three
      ways — no realtime, SSE subscriber, WS subscriber. Asserts the
      events delivered match the engine-emitted events exactly.
- [x] Soak: 1-hour run, 100 projects × 10 connections each, mixed
      INSERT/UPDATE/DELETE workload. Asserts no memory growth, no
      dropped events under quota, durable replay catches
      over-quota drops.
- [x] Files: `tests/integration/tests/realtime_differential.rs`,
      `tests/integration/tests/realtime_soak.rs`.

#### 5.11.R8 — basin-js client SDK channel API (~3-5 days) ✅ shipped in basin-js (T-025/028/029/030)

- [x] Mirror Supabase's channel API in `basin-js`:
      `basin.channel('orders').on('postgres_changes', { event: '*' },
      cb).subscribe()`. Maps to SSE for read-only subscriptions, WS
      when presence or filter changes mid-stream. (basin-js T-030
      `channel()` router; transports T-025 SSE / T-028 WS / T-029 presence.)
- [x] Reconnect-with-replay using `Last-Event-Id` automatically.
- [x] Acceptance gate: basin-js realtime suite green (SSE/WS/presence
      transports + channel routing). Lives in the basin-js repo.
- [x] Lives in basin-js repo; tracked here as the consumer-side
      acceptance gate for R1-R7.

If a separate repo later makes more sense (independent release
cadence, ecosystem signal), the workspace member becomes a separate
repo via a one-day `git mv` + new `Cargo.toml`. Both paths stay
open; same-repo default is just lower-friction at current scale.

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

## Phase 5.17 — Object storage (Supabase-style blobs) — ADR 0021

Catalog-backed blob storage. Objects are rows in `storage.objects`;
access control reuses the RLS engine (Phase 5.6); bytes live in the
same `object_store` the engine uses; signed URLs are HMAC over
`(project, bucket, path, expiry)`. New `basin-blob` crate behind the
`storage` Cargo feature (ADR 0018). Unblocks basin-js's 🔒
`storage.from(bucket)` stubs.

#### 5.17.A — Data model + crate skeleton (~1 week)

- [x] New crate `crates/basin-blob/` (workspace member; `storage`
      feature in `basin-server`).
- [x] `storage.buckets` (id, name, public, file_size_limit,
      allowed_mime_types[]) + `storage.objects` (id, bucket_id, path,
      size, mime_type, metadata jsonb, owner, created_at, updated_at,
      etag). Per-project, project-scoped.
- [x] Bytes path convention: `<project_prefix>/storage/<bucket>/<path>`
      in `object_store`. No second backend.
- [x] Acceptance: bucket + object rows round-trip through the catalog;
      object_store write/read of a small blob works.

#### 5.17.B — HTTP API in basin-rest (~1.5 weeks)

- [x] `POST/GET/DELETE /storage/v1/object/:bucket/:path*` (upload /
      download / single-delete; RLS-gated). `GET /storage/v1/object/public/…`
      fast path for public buckets. `POST /storage/v1/object/list/:bucket`.
      Bulk delete `DELETE /storage/v1/object/:bucket` with `{prefixes:[]}`
      body (basin-js `.remove(paths[])` maps here). Bucket CRUD under
      `/storage/v1/bucket`.
- [x] Body cap from `storage.buckets.file_size_limit`; MIME sniffed
      server-side against `allowed_mime_types[]`; path normalised
      (traversal guard).
- [x] Acceptance: `curl` upload then download round-trips; private
      bucket without JWT → 401; public bucket without JWT → 200.

#### 5.17.C — RLS integration (~3-5 days) ✅ shipped

- [x] `storage.objects` honours RLS policies (Phase 5.6) consulting
      `auth.uid()` / `auth.role()` / `auth.aal()`. Public buckets
      short-circuit RLS for reads.
- [x] Acceptance: a `CREATE POLICY … ON storage.objects` restricting by
      `owner = auth.uid()` correctly filters list/download across two
      authenticated users; cross-project isolation verified.

#### 5.17.D — Signed URLs (~3-5 days) ✅ shipped

- [x] `POST /storage/v1/object/sign/:bucket/:path*` mints an HMAC URL
      over `(project, bucket, path, expiry)` (`hmac`+`sha2`,
      constant-time verify via `subtle`). Time-boxed, no JWT needed.
- [x] Acceptance: signed URL grants access until expiry; rejected past
      TTL; tampered path/expiry → 403.

#### 5.17.E — Per-project bytes counter (~2-3 days) ✅ shipped

- [x] Bump the per-project byte counter (`ProjectCounterRegistry`) on
      store/delete so cloud can enforce quota + bill. Exposed via the
      same snapshot/OTLP path as other counters. basin-blob 31 tests green.

#### 5.17.F — Resumable uploads (TUS) — v1.1 fast-follow (~1 week)

- [ ] TUS protocol for large-file resumable uploads. Not v1; ship after
      A-E land and a real large-file use case appears.

> Cloud-side (basin-cloud repo): quota enforcement, billing on
> bytes-stored + egress, CDN in front of public reads, image transforms
> (resize/optimize — post-v1, possibly a WASM UDF). Per ADR 0021's
> OSS/cloud split.

## Phase 5.18 — System-schema namespacing — ADR 0022

Make the system namespaces real reserved schemas; user-defined schemas stay
flat-aliased to `public` (projects own project-membership). See
[ADR 0022](./docs/decisions/0022-system-schema-namespacing.md). Sequencing:
**A → B**, **A → C**, **D after A**, **E last**. 5.18.A must run SOLO in
basin-catalog (no other catalog agent concurrent).

- [x] **5.18.A — Catalog `(schema, table)` keying for reserved schemas.** Add a
      `schema` dimension to catalog object identity for the closed reserved set
      (`public` auth storage cron net realtime pg_catalog information_schema);
      `public` is default; unqualified + `public.` resolve identically to today.
      InMemory + Postgres impls + round-trip tests. **Foundation — blocks B/C/D/E.**
      Files: `crates/basin-catalog/src/**`.
- [ ] **5.18.B — Real `search_path` + qualified-name resolution.** Replace the
      cosmetic shim in `crates/basin-engine/src/schema_ddl.rs`: resolver walks
      `search_path` (default `public`, `pg_catalog` implicitly first); qualified
      reserved-schema names bind directly; user schemas alias to `public`.
      Depends on 5.18.A.
- [ ] **5.18.C — Migrate system namespaces off prefix hacks.** auth
      (`basin_auth_*` → `auth.*`), net (`_net_http_response` → `net.*`), cron
      (`cron.job` real), storage (formalize `storage.*`). Back-compat read/alias
      path + migration test per subsystem; MUST NOT regress existing
      auth/storage/cron/net suites. Depends on 5.18.A. Files: `crates/basin-auth`,
      `crates/basin-net`, `crates/basin-cron`, `crates/basin-blob`.
- [ ] **5.18.D — Honest introspection.** `information_schema.schemata` +
      `.tables.table_schema`, `pg_catalog.pg_namespace` + `pg_class.relnamespace`
      report real reserved-schema membership (extend Phase 5.11.M views).
      Depends on 5.18.A.
- [ ] **5.18.E — Differential + tooling test.** PG tooling
      (pgAdmin/Prisma/PostgREST) sees `auth.users` / `storage.objects` in the
      right schema; RLS-on-`storage.objects` still scopes; user `CREATE SCHEMA x`
      + `x.t` aliases to `public.t` (documented). Depends on A–D.

## Phase 5.19 — JSONB indexing (GIN-equivalent) (~3-4 weeks)

JSONB shipped in 5.11.A with `->`, `->>`, `#>`, `@>` operators —
without a path-aware index, any JSON-heavy workload full-scans.
This is the single biggest "they tried it and switched back" risk
for new-SaaS apps using JSONB document columns. Adds a GIN-style
index that probes containment, key existence, and path operators
without scanning every row.

**Test-first.** The differential + ORM-compat harness lands before
the implementation; every implementation task closes against this
harness.

- [ ] **5.19.A — Differential + ORM-compat test harness** (~1 week,
      lands first). Three layers:
      (1) **Differential correctness:** for a curated battery of
      JSONB queries (`@>`, `<@`, `?`, `?&`, `?|`, `->`, `->>`, `#>`),
      run with-index vs full-scan; assert identical rows on a 1M-row
      seeded table covering: nested objects (≤ 5 deep), arrays
      (mixed types), nulls inside JSONB, empty objects, very-large
      values (≥ 10 KiB), unicode keys.
      (2) **Performance gate:** assert ≥ 10× speedup with-index vs
      without on representative queries; p99 ≤ 5 ms on the indexed
      path vs ≥ 200 ms on full-scan. Bench in `benches/jsonb_gin.rs`.
      (3) **ORM compat:** Diesel, sqlx-migrate, and Prisma can each
      declare a GIN index via their migration DSL, the migration
      applies against Basin, the index is used (verified via
      `EXPLAIN`), and ORM-shaped queries (`.contains()`,
      `path::overlaps`, JSON path operators) round-trip.
      Files: `tests/integration/tests/jsonb_gin_differential.rs`,
      `tests/integration/tests/jsonb_gin_orm_compat.rs`,
      `benches/jsonb_gin.rs`.
      Acceptance: harness compiles and reports red against the
      current (no-index) engine — the harness exists *before* the
      implementation; every implementation task below flips a slice
      of the harness from red to green.
- [ ] **5.19.B — GIN index type + catalog DDL** (~1 week).
      `CREATE INDEX … USING gin (col)` + `CREATE INDEX … USING gin
      (col jsonb_path_ops)` parser + catalog persistence. Two operator
      classes: `jsonb_ops` (key+value indexing, supports more operators)
      and `jsonb_path_ops` (path hashes only, smaller + faster for `@>`).
      Files: `crates/basin-catalog/src/index.rs` (extend),
      `crates/basin-engine/src/ddl.rs` (parser).
      Acceptance: `CREATE INDEX i ON t USING gin (data jsonb_path_ops);`
      persists; `\d t` in psql shows the index; the ORM-compat slice
      of 5.19.A (migration-applies) flips green.
- [ ] **5.19.C — Containment probe** (`@>`, `<@`) (~1 week).
      Engine query planner uses the GIN index for containment when
      the query predicate matches the index operator class.
      Posting-list AND-merge for compound containment.
      Files: `crates/basin-engine/src/index_probe.rs` (new),
      `crates/basin-engine/src/planner.rs` (extend).
      Acceptance: containment-slice of 5.19.A flips green
      (differential clean + ≥ 10× speedup).
- [ ] **5.19.D — Key / path probe** (`?`, `?&`, `?|`, `->`, `->>`,
      `#>`) (~1 week). Index probe for key-existence and path
      operators when the column uses `jsonb_ops`.
      Files: `crates/basin-engine/src/index_probe.rs` (extend).
      Acceptance: key/path slice of 5.19.A flips green.
- [ ] **5.19.E — Index maintenance on UPDATE / DELETE** (~1 week).
      Copy-on-write aligned: GIN posting lists rebuilt for affected
      row groups on Iceberg snapshot commit. Soft-delete tombstones
      filtered at probe time.
      Files: `crates/basin-storage/src/index_maint.rs` (new),
      `crates/basin-engine/src/dml_mutate.rs` (extend).
      Acceptance: 10k UPDATE/DELETE workload against a GIN-indexed
      table; differential re-eval slice of 5.19.A stays green
      across the workload.

## Phase 5.20 — Full-text search (`tsvector` / `tsquery` / `@@`) (~3-4 weeks)

`basin-trgm` (shipped) covers trigram / typo-tolerant search —
different from FTS. Every content app reaches for
`to_tsvector('english', body) @@ to_tsquery('search')`. PG-style
stemming + ranking + GIN index. Without this, customers bolt on
Meilisearch/Typesense or leave.

**Test-first.** The FTS battery + ORM-compat lands first; each
implementation task flips a slice of the harness green.

- [ ] **5.20.A — FTS battery + ORM-compat test harness** (~1 week,
      lands first). Three layers:
      (1) **Type round-trip:** `tsvector` + `tsquery` round-trip
      through psql + binary pgwire (binary parameter binding);
      unicode + edge cases (empty, very long, exotic punctuation).
      (2) **Semantics:** stemming correctness for English (golden
      file: 200 input strings → expected lexeme outputs, sourced
      from a reference PG instance); `@@` operator matches PG
      semantics on a curated battery; `ts_rank` / `ts_rank_cd`
      ordering matches PG within float tolerance; GIN-indexed
      latency ≤ 10 ms vs full-scan ≥ 500 ms on a 1M-doc seed.
      (3) **ORM compat:** Prisma's `@@db.TsVector` / `searchableText`
      DSL, Diesel's `tsvector` type, and sqlx's `PgTsVector`
      bindings — for each, migration applies, INSERTs round-trip,
      queries (`@@` with named params) work.
      Files: `tests/integration/tests/fts_semantics.rs`,
      `tests/integration/tests/fts_orm_compat.rs`,
      `tests/integration/tests/fts_content_app.rs` (sample
      blog-app schema reproducing a golden result set),
      `benches/fts.rs`.
      Acceptance: harness compiles + reports red against the
      current engine.
- [ ] **5.20.B — `tsvector` + `tsquery` types** (~1 week). Catalog
      type registration; binary + text serialisation; Arrow
      representation. Files: `crates/basin-common/src/types/tsvector.rs`
      (new), `crates/basin-engine/src/types.rs` (register).
      Acceptance: type-round-trip slice of 5.20.A flips green.
- [ ] **5.20.C — `to_tsvector` / `to_tsquery` / `plainto_tsquery`
      UDFs with stemming** (~1 week). English stemming first
      (Snowball / `rust-stemmers`); pluggable language registry for
      future additions. Stop-word lists per language. Files:
      `crates/basin-engine/src/udfs/fts.rs` (new).
      Acceptance: stemming-golden-file slice of 5.20.A flips green.
- [ ] **5.20.D — `@@` operator + ranking** (`ts_rank`, `ts_rank_cd`)
      (~1 week). Lexeme-position-aware matching; ranking with
      weighting by document section (A/B/C/D). Files:
      `crates/basin-engine/src/operators/fts_match.rs` (new).
      Acceptance: @@ semantics + ranking slice of 5.20.A flips green.
- [ ] **5.20.E — GIN index on `tsvector`** (~1 week). Reuses the
      GIN infrastructure from 5.19. Posting lists per lexeme;
      probe at `@@` query time. Files:
      `crates/basin-storage/src/index/gin_tsvector.rs` (new).
      Acceptance: indexed-latency slice of 5.20.A flips green.
      **Depends on:** 5.19.B (shared GIN infra).

## Phase 5.21 — Logical CDC / `pgoutput` / replication slots (~4-6 weeks)

`ChangeEventSink` + webhooks (5.11.I) cover most "row changed → do
thing" cases, but Debezium / Materialize / Fivetran / Airbyte /
Estuary all expect a **logical-replication consumer protocol**.
This is what unlocks the data-platform integration story —
"drop-in for any tool that talks to Postgres" instead of "drop-in
for apps, adapters for data tools."

**Test-first.** A pgoutput-protocol conformance harness + real
Debezium + Fivetran integration tests land first; each
implementation task closes against this harness.

- [ ] **5.21.A — CDC protocol conformance + tool-integration test
      harness** (~1 week, lands first). Three layers:
      (1) **In-process consumer:** test-only Rust client that speaks
      pgwire's replication-protocol subset (`CREATE_REPLICATION_SLOT`,
      `START_REPLICATION`, ack messages); decodes pgoutput; asserts
      message types, field ordering, LSN monotonicity.
      (2) **Tool integration:** Debezium (Kafka Connect) + Fivetran
      (HVR / Postgres connector) — each spins up against a Basin
      instance via docker-compose; snapshot phase + streaming phase
      verified end-to-end. 10k mixed INSERT/UPDATE/DELETE produce
      identical row sets at the consumer (ordered, no dupes, no
      drops). Crash-restart: kill the consumer mid-stream; on
      restart, resumes from last-ack'd LSN with zero gaps.
      (3) **Replay & retention:** WAL retention respects
      min-confirmed across slots; consumers can replay from an
      arbitrary historical LSN within retention.
      Files: `tests/integration/tests/cdc_pgoutput_conformance.rs`,
      `tests/integration/tests/cdc_debezium_e2e.rs`,
      `tests/integration/tests/cdc_fivetran_e2e.rs`,
      `tests/integration/tests/cdc_replay_retention.rs`,
      `tests/integration/fixtures/debezium-compose.yml`,
      `tests/integration/fixtures/fivetran-compose.yml`.
      Acceptance: harness compiles + reports red against the current
      engine.
- [ ] **5.21.B — Replication slot pgwire commands** (~1 week).
      `CREATE_REPLICATION_SLOT <name> LOGICAL pgoutput`,
      `DROP_REPLICATION_SLOT <name>`, `START_REPLICATION SLOT <name>
      LOGICAL <start_lsn>` parsed at the pgwire layer. Catalog
      persistence of slot state (`name`, `plugin`, `start_lsn`,
      `confirmed_flush_lsn`, `consumer_id`). Files:
      `crates/basin-engine/src/pgwire/replication.rs` (new),
      `crates/basin-catalog/src/replication_slots.rs` (new).
      Acceptance: protocol-handshake slice of 5.21.A flips green.
- [ ] **5.21.C — `pgoutput` plugin: encode `ChangeEvent` → pgoutput
      messages** (~2 weeks). Translate Phase 5.11.G ChangeEventSink
      events into pgoutput wire format: `RELATION`, `BEGIN`, `INSERT`,
      `UPDATE`, `DELETE`, `COMMIT`. Tuple encoding (text + binary).
      Files: `crates/basin-engine/src/replication/pgoutput.rs` (new).
      Acceptance: in-process-consumer slice of 5.21.A flips green;
      Debezium snapshot+streaming slice begins decoding correctly.
- [ ] **5.21.D — LSN management + ack handling + replay** (~1 week).
      Monotonic per-project LSN sequence (already exists as
      `ChangeEvent.seq`). Consumer ack messages advance
      `confirmed_flush_lsn`; engine GCs WAL segments only past the
      min-confirmed across all active slots. Replay from arbitrary
      LSN. Files: `crates/basin-engine/src/replication/lsn.rs` (new),
      `crates/basin-wal/src/retention.rs` (extend).
      Acceptance: replay+retention slice of 5.21.A flips green
      (crash-restart resumes; WAL retention respects min-confirmed).
- [ ] **5.21.E — `CREATE PUBLICATION` SQL surface** (~1 week).
      `CREATE PUBLICATION p FOR ALL TABLES;`,
      `CREATE PUBLICATION p FOR TABLE t1, t2 WITH (publish='insert,update');`,
      `ALTER PUBLICATION`, `DROP PUBLICATION`. Per-project per-table
      filtering applied at the publisher side before encoding.
      Files: `crates/basin-engine/src/ddl.rs` (extend),
      `crates/basin-catalog/src/publications.rs` (new).
      Acceptance: publication-filter slice of 5.21.A flips green
      (cross-project + table-subset isolation verified by both the
      in-process consumer and the Debezium/Fivetran fixtures).

## Phase 5.22 — `pg_dump` / `pg_restore` compat (export side) (~2-3 weeks)

Two reasons: (a) buyer trust — "I can get my data out if I need to"
closes deals; (b) migration *into* Basin from existing PG is the
highest-leverage onboarding path. Engine already has snapshots so
"consistent point-in-time" semantics are free.

**Test-first.** A round-trip + cross-tool harness lands first;
each implementation task flips a slice of it green.

- [ ] **5.22.A — Round-trip + cross-tool + ORM-load test harness**
      (~1 week, lands first). Four layers:
      (1) **Basin → Basin round-trip:** 10-table seed schema with
      diverse types (JSONB, vector, text, timestamps, arrays,
      sequences, generated columns, enums); dump in both formats
      (plain + custom); restore to fresh Basin; diff schema + data;
      assert zero diff.
      (2) **Basin → Postgres cross-tool:** dump from Basin in both
      formats; load into real Postgres via `psql` (plain) +
      `pg_restore` (custom); confirm schema + row counts; flag any
      Basin-specific features that should be marked with
      `-- skipped: <feature>` comments and verify the comments are
      present.
      (3) **Postgres → Basin migration path:** dump from real
      Postgres of a representative SaaS schema (auth.users-like,
      orders/customers, JSONB documents); load into Basin via
      `basin restore`; confirm queries against the loaded data work.
      (4) **ORM-load compat:** after a dump + restore on a Basin
      instance, Diesel / sqlx / Prisma each connect, introspect, and
      run their standard test queries against the restored data.
      Files:
      `tests/integration/tests/pg_dump_basin_round_trip.rs`,
      `tests/integration/tests/pg_dump_cross_tool_pg.rs`,
      `tests/integration/tests/pg_dump_pg_to_basin_migration.rs`,
      `tests/integration/tests/pg_dump_orm_load_compat.rs`,
      `tests/integration/fixtures/pg-compose.yml`.
      Acceptance: harness compiles + reports red against the current
      engine.
- [ ] **5.22.B — Plain-text SQL dump format** (`pg_dump -F p`
      equivalent) (~1 week). Schema + data emitted as `CREATE TABLE`
      / `CREATE INDEX` / `COPY … FROM stdin` / data rows. Snapshot
      pinned at dump-start for consistency. Files:
      `crates/basin-engine/src/dump/plain.rs` (new).
      Acceptance: plain-format Basin→Basin round-trip slice of
      5.22.A flips green.
- [ ] **5.22.C — Custom binary format** (`pg_dump -F c` equivalent)
      (~1 week). Compressed, restorable with `pg_restore`. Header
      includes pg_dump version + Basin version markers. Files:
      `crates/basin-engine/src/dump/custom.rs` (new).
      Acceptance: custom-format + cross-tool slices of 5.22.A flip
      green (`pg_restore -l` lists objects; real PG accepts the
      dump modulo skipped features).
- [ ] **5.22.D — `basin dump` / `basin restore` CLI commands**
      (~3-5 days). basin-cli wraps the pgwire-side dump endpoint:
      `basin dump --project=<ref> --format=plain|custom -o dump.sql`,
      `basin restore --project=<ref> dump.sql`. Files:
      basin-cli `cmd_dump.go` + `cmd_restore.go`.
      Acceptance: end-to-end CLI slice of 5.22.A flips green;
      output is byte-for-byte stable across runs at the same
      snapshot.
- [ ] **5.22.E — Documented exclusions in `CAPABILITIES.md`**
      (~1 day). Note any PG features Basin can't dump losslessly
      (extensions, plpgsql); dump output flags them with
      `-- skipped: <feature>` comments so import-into-PG attempts
      have clear hints. Verified by the cross-tool slice of 5.22.A.

## Phase 5.23 — `EXPLAIN ANALYZE` polish + `pg_stat_activity` + `pg_locks` (~1-2 weeks)

`basin_stat_statements` is on the roadmap (5.16.D). The other two
views matter for ops + dev tooling that introspects (DataGrip /
pgAdmin / pgcli). Plus `EXPLAIN ANALYZE` correctness verification.

**Test-first.** Admin-view structural correctness + real-tooling
compat harness lands first.

- [ ] **5.23.A — Admin-view + tooling-compat test harness**
      (~3 days, lands first). Three layers:
      (1) **EXPLAIN ANALYZE correctness:** curated query battery
      (point lookup, full scan, join, aggregate, window, CTE);
      assert "actual rows" + "actual time" match reality;
      determinism across runs (no flapping above a tolerance).
      (2) **View shape:** `pg_stat_activity` + `pg_locks` return
      the expected columns + types vs PG reference; per-project
      RLS enforced (other projects' rows not visible).
      (3) **Real-tooling compat:** dockerised pgAdmin, DataGrip,
      and pgcli connect to a Basin instance and complete the
      following without errors: object browser populates, "Server
      activity" pane populates from `pg_stat_activity`, "Locks"
      pane populates from `pg_locks`.
      Files: `tests/integration/tests/explain_analyze_battery.rs`,
      `tests/integration/tests/admin_views_shape.rs`,
      `tests/integration/tests/admin_view_tooling_compat.rs`,
      `tests/integration/fixtures/pgadmin-compose.yml`,
      `tests/integration/fixtures/datagrip-compose.yml` (or
      headless pgcli script).
      Acceptance: harness compiles + reports red.
- [ ] **5.23.B — `EXPLAIN ANALYZE` verification + polish**
      (~3 days). Audit every executor node for accurate
      "actual rows" + "actual time" reporting; ensure parallel
      plans aggregate correctly; buffer/I-O stats where measurable.
      Files: `crates/basin-engine/src/executor.rs` (instrument).
      Acceptance: EXPLAIN-correctness slice of 5.23.A flips green.
- [ ] **5.23.C — `pg_stat_activity` view** (~3 days). Current
      connections, queries in flight, wait events, client IP, app
      name, query text (params redacted). Per-project RLS — a user
      only sees their own project's activity. Files:
      `crates/basin-engine/src/info_schema_provider.rs` (extend),
      `crates/basin-engine/src/connection_registry.rs` (new — or
      extend existing pool tracking).
      Acceptance: pg_stat_activity slice of 5.23.A flips green.
- [ ] **5.23.D — `pg_locks` view** (~3 days). Current lock holders +
      waiters. Engine has single-writer-per-shard semantics so the
      lock model is simple (writers serialise per partition);
      surface that accurately. Files:
      `crates/basin-engine/src/info_schema_provider.rs` (extend),
      `crates/basin-shard/src/lock_registry.rs` (new).
      Acceptance: pg_locks slice of 5.23.A flips green; tooling
      compat slice (pgAdmin/DataGrip/pgcli) all flip green together.

## Phase 5.24 — Range types + GIST-equivalent index (~2-3 weeks)

`int4range`, `tsrange`, `daterange` etc. — booking apps, scheduling
apps, "is X in range Y" queries. PG-idiomatic. Without ranges these
apps reach for two separate `lower` + `upper` columns and lose the
overlap-detection ergonomics.

**Test-first.** Range-types semantics + ORM compat + scheduling
integration harness lands first.

- [ ] **5.24.A — Range types + ORM-compat + scheduling test harness**
      (~1 week, lands first). Three layers:
      (1) **Type round-trip:** all six range types
      (`int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`,
      `daterange`) round-trip through psql + binary pgwire;
      inclusive/exclusive bounds (`[)`, `(]`, `[]`, `()`, `empty`)
      preserved.
      (2) **Operator semantics:** curated battery for every operator
      (`@>`, `<@`, `&&`, `<<`, `>>`, `-|-`, `+`, `*`, `-`) verified
      against a reference PG instance for equivalence; index-covered
      shapes meet ≤ 10 ms p99 latency vs full-scan ≥ 200 ms on a
      1M-row table.
      (3) **ORM compat + scheduling integration:** Diesel's
      range-type bindings, sqlx's `PgRange`, and Prisma's range
      support — migration applies, CRUD round-trips, overlap
      queries work via the ORM. Plus a booking-system schema
      (rooms × time-ranges) with a no-overlap exclusion constraint
      reproducing a golden conflict-rejection sequence.
      Files: `tests/integration/tests/range_types_semantics.rs`,
      `tests/integration/tests/range_orm_compat.rs`,
      `tests/integration/tests/range_scheduling.rs`,
      `benches/range_index.rs`.
      Acceptance: harness compiles + reports red.
- [ ] **5.24.B — Range type implementations** (~1 week).
      `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`,
      `daterange`. Binary + text serialisation; Arrow representation
      (struct of {lower, upper, lower_inc, upper_inc, empty}). Files:
      `crates/basin-common/src/types/range.rs` (new),
      `crates/basin-engine/src/types.rs` (register).
      Acceptance: type-round-trip slice of 5.24.A flips green.
- [ ] **5.24.C — Range operators** (~1 week). `@>`, `<@`, `&&`,
      `<<`, `>>`, `-|-`, `+`, `*`, `-`. Files:
      `crates/basin-engine/src/operators/range.rs` (new).
      Acceptance: operator-semantics slice of 5.24.A flips green.
- [ ] **5.24.D — GIST-equivalent index** (~1 week). True GIST is
      a deep build; ship a simpler alternative: pre-sort + interval-
      tree-style probe on a B-tree-of-(lower, upper) for the common
      overlap / contains query shapes. Falls back to full scan for
      uncovered query shapes (documented). Files:
      `crates/basin-storage/src/index/interval.rs` (new).
      Acceptance: index-latency slice of 5.24.A flips green
      (1M-row range table; overlap ≤ 10 ms vs full-scan ≥ 200 ms).

## Phase 5.25 — Migration-tool compat matrix (~3-5 days)

Verification + docs sprint. Flyway / golang-migrate / Diesel /
sqlx-migrate / Prisma should mostly "just work" via pgwire, but a
tested matrix in `CAPABILITIES.md` closes a real adoption objection.

**Test-first.** This whole phase is testing — but the shared
harness scaffolds first so each tool's verification slots in
consistently.

- [ ] **5.25.A — Shared migration-tool test scaffold** (~1 day,
      lands first). docker-compose fixture template + ephemeral
      Basin instance launcher + result-comparison helpers
      (introspection diff, ORM-query battery against the migrated
      schema). Each per-tool task below plugs into this scaffold.
      Files: `tests/integration/fixtures/migration-tool-scaffold/`,
      `tests/integration/tests/migration_tool_common.rs`.
      Acceptance: scaffold builds; a placeholder per-tool test
      compiles + skips correctly until its real implementation
      lands.
- [ ] **5.25.B — Flyway compat** (~1 day). Run Flyway's standard
      test migrations against Basin; document any gaps. Files:
      `tests/integration/tests/migration_tool_flyway.rs`,
      `CAPABILITIES.md` (row added).
      Acceptance: a 10-migration Flyway project applies cleanly;
      `flyway_schema_history` table populated correctly.
- [ ] **5.25.C — golang-migrate compat** (~1 day).
      `migrate up/down/version/force` against Basin. Files:
      `tests/integration/tests/migration_tool_golang_migrate.rs`.
- [ ] **5.25.D — Diesel compat** (CLI + ORM) (~1 day). `diesel
      migration run`, `diesel migration revert`, `diesel migration
      generate`; plus an ORM query battery against the migrated
      schema (CRUD round-trip; JSONB column queries via Diesel's
      `Jsonb` type). Files:
      `tests/integration/tests/migration_tool_diesel.rs`.
- [ ] **5.25.E — sqlx-migrate compat** (~1 day). `sqlx migrate run`,
      `sqlx migrate revert`; plus a compile-time-checked-query
      battery (`query!` / `query_as!` macros) against the migrated
      schema. Files:
      `tests/integration/tests/migration_tool_sqlx.rs`.
- [ ] **5.25.F — Prisma migrate compat** (~1 day). This is the most
      likely to find real gaps (Prisma introspects heavily); plus a
      Prisma Client query battery against the migrated schema. Files:
      `tests/integration/tests/migration_tool_prisma.rs`.
- [ ] **5.25.G — `CAPABILITIES.md` migration-tool matrix row**
      (~1 day). Table: tool × supported (yes/no/with-caveats) ×
      caveats column × verified-on-version column. Generated from
      the test results — never hand-edited; CI regenerates on every
      green run.

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
- [x] **5.13.B** Typed AST matches replacing textual prescreens (Phase 2)
      — the 14 textual pre-screens in `executor.rs` migrate to `pg_ast`
      AST matches one at a time. Agents 2–4 split: (i) DDL pre-screens
      (`ALTER TYPE … ADD VALUE`, `CREATE DOMAIN`, Basin-specific
      `ALTER TABLE` extensions), (ii) DML / mutation pre-screens, (iii)
      function / procedure / reactor pre-screens. Acceptance gate per
      migration: pre-screen test suite passes with the AST match
      replacing the regex; no new diffs in `sql_support_matrix`.
      Files: `crates/basin-engine/src/executor.rs` (per-pre-screen),
      `crates/basin-engine/src/pg_ast.rs` (typed matches).
- [x] **5.13.C** Unconditional pg_query path (Phase 3) — Agent 5 flips
      `BASIN_PG_QUERY` unconditional and removes the sqlparser
      front-end from the executor's hot path. sqlparser stays only for
      legacy module-internal node bodies until Phase 2's full translator
      lands. Acceptance gate: env var removed; sqlparser dependency
      gated to translator-only.

## Phase 5.16 — Query insights (per-shape stats; OSS data collection + cloud aggregation)

**Why.** Basin today exposes per-project counters (ops / bytes / p99 / errors)
and OpenTelemetry traces, but no **shape-aware** telemetry — there is no
"which query *shape* is killing me?" view.  pg_stat_statements is the
canonical reference; CockroachDB's Statements page is the polished version.
We need our own, with two properties Postgres doesn't have:

1. **Scale-dependent regression tracking** — track latency by table-size
   bucket so we surface shapes whose p99 grows monotonically with data.
   (This is the pattern the `point_eq` 0.94 → 0.85 → 0.49 release smoke
   exposed for Phase 5.14.A blooms.)
2. **Anonymised cross-customer aggregates** in basin-cloud — Basin engineers
   see "X% of customers run shape Y at >100GB table size" without seeing any
   specific customer's data.  Drives roadmap evidence, not guesswork.

OSS side ships the data-collection layer.  Cloud side
([basin-cloud-roadmap.md](./docs/basin-cloud-roadmap.md)) ships the ingest
pipeline + UI + cross-customer roll-ups.

Privacy invariant: literals are stripped at the LogicalPlan layer before
anything is persisted or exported.  Plan-shape hash is over operator tree
+ column refs + literal type slots, never literal values.

Privacy and anonymisation model: [ADR 0017](./docs/decisions/0017-query-shape-privacy.md).

- [x] **5.16.A** Plan-shape canonical hash (shipped `33ae73f`; 6 unit tests pass; xxh3_64 seeded with `basin_sketch::QUERY_SHAPE_SEED`; LITERAL_SLOT + DataType erasure proves cross-process stability).  **Locked decision
      (2026-05-19) per ADR 0017:** use `xxhash-rust` crate's
      `xxh3_64`, seeded with the fixed constant
      `BASIN_QUERY_SHAPE_SEED = 0xBA51_4145_7E11_5A95`.  Cross-version
      stable (required for the OSS → cloud → cross-project aggregate
      pipeline that follows in 5.16.D – H).  Reject `std::DefaultHasher`
      (unstable across Rust versions), `fnv1a-64` (too weak at cloud
      scale), `siphash13` (slower; reserved as fallback if
      xxhash-rust ever drops maintenance).
      Files: `crates/basin-engine/src/query_shape.rs` (new); hooks into
      `executor.rs::exec_select` and the fast-path entry points;
      `Cargo.toml` workspace adds `xxhash-rust = "0.8"`.
      Acceptance gate: `WHERE id = 5` and `WHERE id = 99` hash to the
      same shape; `WHERE id = $1` (parameterised) hashes to the same
      shape; `WHERE id = 5 AND k = 3` hashes to a different shape;
      benchmark `query_shape_hash` ≤ 5 µs/query on a representative
      LogicalPlan; **same plan hashes to the same value across two
      independent process invocations** (cross-process stability gate).
      Estimate: ~4 days.
- [x] **5.16.B** Per-shape rolling histograms (shipped `9026e95`; 5/5 query_stats tests; 500-shape memory bound verified ≤1.5 MiB/project; observe hook live in `exec_select`).  Files:
      `crates/basin-engine/src/query_stats.rs` (new); `QueryStatRegistry`
      with HDR histograms (use the `hdrhistogram` crate) for latency, rows
      scanned, files opened, bytes decoded, cache hits, fast-path-engaged
      enum, p50/p95/p99 readouts.  Per-project + per-table breakdown.
      Multi-project isolation pattern: shared registry + per-project
      bounded LRU of shapes (max 500 distinct shapes/project), counter
      O(bytes).
      Acceptance gate: 10k QPS workload → registry overhead ≤ 1% p99;
      memory bounded at 500 shapes × ~2 KiB histogram ≈ 1 MiB/project.
      Estimate: ~1 week. Depends on 5.16.A.
- [x] **5.16.C** Scale-dependent regression tracking (shipped `57a755f`; 8 log2 buckets <1k…≥1B; threshold default 1.3, configurable via `set_regression_threshold`; 7/7 query_stats tests; memory bound 10 MiB/project at 500 shapes × 13 histograms).  Files:
      `crates/basin-engine/src/query_stats.rs` (extend).  Bucket each
      query by `log2(table_row_count)` (8 buckets covering 1k → 1B rows)
      and store per-(shape, bucket) histograms.  Expose
      `top_regressions(project, table)` returning shapes whose ratio
      `bucket[N].p99 / bucket[N-1].p99` exceeds 1.3 (configurable).
      Acceptance gate: a synthetic test with a shape's latency growing
      monotonically with row count flags the shape correctly; a shape
      with flat latency does not.
      Estimate: ~1 week. Depends on 5.16.B.
- [x] **5.16.D** OTLP export schema (shipped `94db513`; 5/5 query_stats_export tests; SQL view `basin_stat_statements` with 9 columns; OTLP via tracing-opentelemetry bridge; 1k QPS smoke under 1s wall).  Files:
      `crates/basin-engine/src/query_stats_export.rs` (new); extends the
      existing OpenTelemetry pipeline with `basin.query_shape.*` metrics
      (Counter + Histogram per shape).  Plus a `basin_stat_statements`
      view exposed via SQL (Postgres-style) so self-hosted users see
      shape stats without standing up an OTLP collector.
      Acceptance gate: `SELECT * FROM basin_stat_statements` returns rows;
      OTLP export at 1k QPS does not block the query hot path.
      Estimate: ~1 week. Depends on 5.16.B + 5.16.C.

Sub-items 5.16.E – 5.16.H live in the basin-cloud repo
(see [`docs/basin-cloud-roadmap.md`](./docs/basin-cloud-roadmap.md)
Phase 5.16-cloud):

- 5.16.E — Cloud ingest pipeline (OTLP receiver → ClickHouse / VictoriaMetrics)
- 5.16.F — Cloud Query Insights UI (per-customer "your slowest queries" view)
- 5.16.G — Anti-pattern detection + suggestion engine
- 5.16.H — Anonymised cross-customer aggregates (k-anonymity ≥ 5, Basin engineers only)

ADR 0017 will record the privacy / anonymisation design.

**Acceptance gate (composite, OSS side):** at 10k QPS the `QueryStatRegistry`
overhead is ≤ 1% p99; per-shape stats survive a process restart via OTLP
re-export (in-memory only — restart loses pre-restart history, design choice);
`basin_stat_statements` view materialises in < 100 ms.

**Total OSS effort:** ~4 weeks.

## Phase 5.15 — Unified docs platform (OSS-side shipped; cloud-side at basin-cloud)

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

- [x] **5.15.A** Frontmatter spec (shipped `a3f7a26`; ADR 0021 records
      the YAML-vs-MDX-vs-TOML rationale; spec + ADR + README cross-ref).
- [x] **5.15.B** Migrate existing Basin OSS docs to the frontmatter spec
      (shipped `f9cf57a`; 35+ docs migrated; `scripts/check-frontmatter.sh`
      + `docs-frontmatter` CI job enforces on every PR).
- [x] **5.15.C** Top-level docs index (shipped `2ce0e7c`; 38 docs across
      8 nav_sections; `scripts/build-docs-index.sh` + Python helper
      regenerates from frontmatter; `docs-index` CI job blocks drift).
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
durable advantage is the **catalog + storage-orchestration + multi-project
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

- [x] **5.14.A1** Add `bloom_filters: BTreeMap<String, Vec<u8>>` (shipped `ae6460a`)
      field to `DataFileRef`. Files: `crates/basin-catalog/src/metadata.rs`.
      Acceptance gate: round-trip serialise/deserialise via InMemory +
      Postgres impls.
- [x] **5.14.A2** Writer-side bloom computation for declared (shipped `50b9306`)
      `basin.sort_by` columns. Files: `crates/basin-storage/src/writer.rs`.
      Acceptance gate: every committed file with `basin.sort_by` set
      carries blooms for those columns.
- [x] **5.14.A3** Probe in `execute_simple_select` before file open (shipped `50b9306`).
      Files: `crates/basin-engine/src/fast_select.rs`. Acceptance gate:
      the `point_eq` miss shape in `vortex_vs_parquet_smoke` drops file
      opens to zero (count via a perf counter exported through
      `Engine::project_counters`).
- [x] **5.14.A4** Differential test: bloom false-negative rate is zero (shipped `c5739a6` — CRITICAL: closed the fastbloom random-seed false-negative bug)
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

- [x] **5.14.B1** Hoist HLL and t-digest types to a new `basin-sketch`
      crate, then add `hll_sketches: BTreeMap<String, Vec<u8>>` and
      `tdigest_sketches: BTreeMap<String, Vec<u8>>` to `DataFileRef`
      (mirroring the bloom field pattern).  **Locked decision
      (2026-05-19):** sketches MUST live in their own crate because
      `basin-storage` (B2 writer-side) cannot depend on `basin-engine`
      (where the inline `Hll` and `TDigest` types currently live)
      without inverting the engine→storage dependency direction.  Both
      crates take a fresh dep on `basin-sketch`.  Wire format unchanged
      from the existing inline impls (HLL: 16 384 raw register bytes;
      t-digest: 8-byte LE count + 16 bytes per centroid).
      Files: `crates/basin-sketch/Cargo.toml` (new), `crates/basin-sketch/src/{lib.rs,hll.rs,tdigest.rs}` (new — hoist 350 LOC from
      `approx_count_distinct.rs` + `approx_percentile.rs`),
      `crates/basin-catalog/src/metadata.rs` (add fields),
      `crates/basin-storage/src/data_file.rs` (add fields),
      `crates/basin-engine/src/approx_count_distinct.rs` (re-export from
      basin-sketch), `crates/basin-engine/src/approx_percentile.rs`
      (same).  Acceptance gate: round-trip serialise/deserialise via
      InMemory + Postgres impls; existing UDF unit tests still pass.
      Shipped `d7b96c4`.
- [x] **5.14.B2** Writer-side sketch computation (shipped `696cadb`).  `WriteOptions::
      sketch_columns: Vec<String>` (defaults to `global_sort_order`,
      consistent with `bloom_columns`); writer's
      `compute_bloom_filters` companion `compute_sketches` runs in the
      same column-iteration pass.  Numeric-column detection: Arrow
      `DataType::Int*` / `UInt*` / `Float32` / `Float64` get t-digest;
      non-numeric only get HLL.  Files: `crates/basin-storage/src/writer.rs`,
      `crates/basin-engine/src/{executor,dml_mutate}.rs` (thread the
      `sketch_columns` field through, mirror the 5.14.A2 bloom
      threading).  Acceptance gate: every file committed for a table
      with `basin.sort_by` set carries sketches for those columns;
      writer overhead ≤ 5 % of total write time on a 100 k-row insert;
      `vortex_parquet_differential` continues to pass.  Depends on B1.
- [x] **5.14.B3** `APPROX_COUNT_DISTINCT(col)` UDF (shipped `fc00a41`; inline HLL — Phase 5.14.B1 hoists into basin-sketch; query-time fast path is B5)
      catalog-pruned files and returns estimate. Files:
      `crates/basin-engine/src/udfs/approx.rs` (new module).
      Acceptance gate: ≤2% error vs exact `COUNT(DISTINCT col)` on the
      88-shape battery.
- [x] **5.14.B4** `APPROX_PERCENTILE(col, p)` UDF (shipped `4935115`; inline t-digest — Phase 5.14.B1 hoists into basin-sketch; query-time fast path is B5)
      and returns quantile. Files: `crates/basin-engine/src/udfs/approx.rs`.
      Acceptance gate: ≤1% absolute error vs exact `percentile_cont(p)`
      on the 88-shape battery.
- [x] **5.14.B5** Differential test (shipped `6d8aafc` + tuning `df0b1a5`; `#[ignore]`; file_count=1+2 verified green with HLL err 0.27%–1.61%, t-digest max 0.59%): 1M-row table; assert sketch-merge
      results within bounds across every file count from 1 to 100.
      Files: `tests/integration/tests/sketches.rs`. Acceptance gate:
      every shape within bounds.

**Acceptance gate (composite):** `count_distinct` and `percentile_cont`
gaps close via the `APPROX_*` alternative; correctness bounds (≤2%
HLL, ≤1% t-digest) honoured on the differential harness.

### 5.14.C — Row-format hot buffer for HTAP (~8 weeks engineering — the architectural moat)

The architectural commitment.  Full design spec in
[ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md).
LSM-style memtable for recent writes (row-formatted, PK-indexed,
`parking_lot::RwLock<BTreeMap<RowKey, MemRowValue>>` per
`(project_id, table_name)`).  Flushed to Vortex on size / age /
scan-pressure threshold.  Reads merge memtable + Vortex.  Closes
the OLTP `point_eq` HIT path floor and single-row UPDATE latency.
**This is what differentiates Basin from "Parquet + DataFusion in
a bucket."**

Precedent: SingleStore (rowstore tier), ClickHouse `ReplacingMergeTree`,
Apache Pinot (real-time segments).  All three couple a row-formatted
hot tier with a column-formatted cold tier and merge at read time.

Lives in a new crate `crates/basin-hottier/`.

**Architecture summary (from ADR 0016):**

- Per-`(project, table)` `BTreeMap` memtable — NOT RocksDB (multi-project
  isolation constraint) or sled (durability redundancy with `basin-wal`).
  Crossbeam-skiplist `SkipMap` is the fallback if C1 benchmark fails.
- Multi-project: shared `MemTableRegistry` (one per process) + per-project
  `AtomicU64` counter + per-project `Semaphore`.  Per-project cost is
  O(bytes + counter + semaphore); idle projects cost zero.
- Memory budgets: project hard cap 256 MB, project soft cap 192 MB,
  per-table soft cap 16 MB, max age 60 s.  Largest-first flush
  scheduler.
- Non-blocking flush: write lock held only for snapshot clone + final
  GC; object-store I/O is lock-free.
- Transaction integration via `TxState::memtable_watermarks`; ROLLBACK
  truncates the memtable to the per-table watermark.  Requires WAL
  transaction-boundary markers (`BEGIN` / `ROLLBACK`) for correct
  crash recovery.

- [x] **5.14.C1** `MemTable` + `MemTableRegistry` (shipped `57dae11`; new `basin-hottier` crate skeleton) — ~1 week.
      `parking_lot::RwLock<BTreeMap<RowKey, MemRowValue>>` per
      `(project, table)`.  `RowKey` is a newtype over `Vec<u8>` with
      big-endian PK column encoding.  `MemRowValue::Row(Vec<u8>) |
      Tombstone`.  `MemTableRegistry` is a `DashMap<(ProjectId, TableName),
      Arc<MemTableEntry>>` with per-project `AtomicU64` byte counter +
      per-project `Semaphore`.
      Files: `crates/basin-hottier/Cargo.toml`, `crates/basin-hottier/src/lib.rs`,
      `crates/basin-hottier/src/memtable.rs`, `crates/basin-hottier/src/registry.rs`,
      `crates/basin-hottier/src/row_key.rs`.
      Acceptance gate: `cargo bench memtable_insert` ≥ 100k rows/s
      single-thread debug, ≥ 1M rows/s release; `cargo bench
      memtable_point_lookup` p99 ≤ 500 µs at 1M rows; range scan
      1k-row range ≤ 2ms at 1M rows.  If p99 lookup at 1M rows >
      500 µs, switch backing structure to `crossbeam-skiplist::SkipMap`
      before C2 starts.
- [x] **5.14.C2** Write-path integration + WAL transaction markers (shipped `a435ba8`; single-row INSERT/UPDATE/DELETE through memtable + `HtapUnionTable` read-merge + WAL markers; INSERT-SELECT and DEFAULT VALUES deferred to C3) — ~1.5 weeks.
      INSERT/UPDATE/DELETE route to memtable (after WAL append).
      `TxState::memtable_watermarks` for per-table rollback; extend
      `SavepointFrame` with the same.  Add `BEGIN` / `ROLLBACK`
      markers to WAL; teach `replay_wal_into` to suppress entries
      between matched pairs.
      Files: `crates/basin-engine/src/executor.rs`,
      `crates/basin-engine/src/dml_mutate.rs`,
      `crates/basin-engine/src/session.rs`,
      `crates/basin-shard/src/in_process.rs`,
      `crates/basin-wal/src/file_wal.rs`.
      Depends on C1.  Acceptance gate: `single_row_update_latency`
      p99 ≤ 5 ms (vs the existing ≥ 50ms Parquet copy-on-write path —
      ≥ 10× improvement); INSERT of 1 row ≤ 1 ms p99; ROLLBACK
      correctly reverts memtable rows; crash-replay test verifies
      rolled-back writes are NOT re-applied.
- [x] **5.14.C3** Read-merge path (shipped `795598e`; merge.rs 9/9 tests; INSERT-SELECT + DEFAULT VALUES wired through memtable; 88-shape smoke parity) — ~1.5 weeks.  `merge_scan(memtable_iter,
      vortex_iter) -> impl Iterator<Item=Row>` with PK-ordered merge,
      dedup-on-PK (memtable wins), tombstone suppression.
      `fast_select.rs::execute_simple_select` probes memtable first for
      point lookups.  Full-scan path in `executor.rs` calls the merge.
      Files: `crates/basin-hottier/src/merge.rs` (new),
      `crates/basin-engine/src/fast_select.rs`,
      `crates/basin-engine/src/executor.rs`.
      Depends on C1 + C2.  Acceptance gate: `point_eq` HIT shape in
      `vortex_vs_parquet_smoke` p99 ≤ 2ms warm (down from ~4ms
      current Vortex path); all 88 existing smoke shapes produce
      identical results with and without the hot tier populated.
- [x] **5.14.C4** Flush (shipped `9e107ef`; FlushTask + FlushBackend trait + 7-step non-blocking algorithm from ADR 0016; 12/12 flush unit tests; 14/14 shard tests; pause/resume exposed for C6) — ~2 weeks.  Background Tokio task (one per
      `MemTableRegistry`).  Triggers: size (table_memtable_soft_cap),
      age (memtable_max_age_secs), scan pressure (>100k rows on a
      full-scan).  Algorithm: snapshot under brief write lock →
      partition new/updated/tombstoned → write new rows via existing
      `write_batch_with_options` → apply updates/tombstones via
      existing `dml_mutate` copy-on-write → atomic catalog commit
      via `replace_data_files` + `append_data_files` (existing path,
      with `commit_with_retry`) → GC flushed rows → WAL truncation.
      Files: `crates/basin-hottier/src/flush.rs` (new),
      `crates/basin-shard/src/in_process.rs` (integrate flush loop).
      Depends on C1+C2+C3.  Acceptance gate: 1M-row mixed workload
      (50% INSERT, 25% UPDATE, 25% DELETE) flushes without read stall;
      no row loss verified by `vortex_parquet_differential`; flush
      duration ≤ 10s for 64 MB memtable on LocalFS.
- [x] **5.14.C5** Multi-project memory budget (shipped `12b2fc2`; 10k-project fuzz heap ≈1.86 GiB ≤ 4 GiB gate; ALTER PROJECT DDL for hard-cap override) — ~1 week.  Per-project
      hard cap (256 MB default, ALTER PROJECT configurable), soft cap
      (192 MB triggers background flush), per-table soft cap (16 MB).
      `Semaphore` back-pressure on hard cap.  Largest-first global
      flush scheduler when total process pressure rises.  Configurable
      via `MemTableConfig`.
      Files: `crates/basin-hottier/src/budget.rs` (new),
      `crates/basin-engine/src/lib.rs` (Engine-level config plumbing).
      Depends on C1.  Acceptance gate: 10k-project fuzz test (10k
      projects × 1k rows × 200 bytes/row = 2 TB total) — no project
      exceeds hard cap; total process heap ≤ 4 GB; per-project byte
      usage scales O(bytes) not O(active_projects).
- [x] **5.14.C6** Differential harness (shipped `04a691c`; 88 shapes × 3 modes = 264 sub-assertions all green; runtime ~13s; 6 shapes excluded uniformly due to SQL gaps not hot-tier issues) — ~1 week.  Every shape in
      `vortex_vs_parquet_smoke` runs in three modes: (1) hot tier empty
      (all rows in Vortex — baseline), (2) all rows in memtable (no
      Vortex files; flush suppressed), (3) split (half in each).
      Files: `tests/integration/tests/hottier_differential.rs`.
      Depends on C1 + C2 + C3.  Acceptance gate: 0 differential rows
      across all shapes × all three modes.

**Acceptance gate (composite):** OLTP `point_eq` HIT p99 ≤ 2 ms warm;
single-row UPDATE p99 ≤ 5 ms; 10k-project fuzz fits in 4 GB heap; 0
differential rows vs Vortex-only baseline.

**Total effort:** ~8 engineer-weeks.  Open risks and out-of-scope
items documented in [ADR 0016](./docs/decisions/0016-htap-hot-tier-architecture.md).

### 5.14.D — Adaptive write-time multi-sort + catalog-aware WindowExec (~6 weeks combined, no deps)

Two write-time / planner pieces that rely on Basin's catalog as a
planning input that upstream cannot see. Closes `order_by_multi`,
`window_partition_sum`, and `lag_lead_window`.

- [x] **5.14.D1** Query-history collector — shipped at commit
      `caa43e6` (Phase 5.14.D2 in the commit message is the
      *collector* per the opus 2026-05-19 decomposition; the open
      work below is the *compactor consumer*).  `QueryHistory` lives
      at `crates/basin-engine/src/query_history.rs`; executor records
      ORDER BY and GROUP BY shapes via `record_query_patterns`.
- [x] **5.14.D2** Compaction-time multi-sort (shipped `ddc9382`) — compactor consults the
      query-history `top_pattern` and re-sorts output files when
      threshold met (≥ 30 % share, ≥ 100 queries observed).
      **Locked priority decision (2026-05-19):** user-declared
      `CLUSTER BY` always wins by default — if `TableMetadata::
      cluster_columns` is non-empty, D2 does not override.  When
      `top_pattern` differs from `cluster_columns`, emit a structured
      event into the registry so Phase 5.16.G's anti-pattern engine
      surfaces it as a suggestion ("your queries would benefit from
      CLUSTER BY (c, d) — currently (a, b)").  Add a per-table opt-in
      `basin.adaptive_sort_override = true` DDL option (default
      `false`) for power users who want D2 to override their declared
      cluster.  Files: `crates/basin-shard/src/in_process.rs`
      (compactor — same function 5.14.C4 flush will touch, so land D2
      before C4 starts), `crates/basin-engine/src/ddl.rs` (parse the
      new `basin.adaptive_sort_override` WITH option, mirror the
      `basin.row_block_size` parser pattern),
      `crates/basin-catalog/src/metadata.rs` (`TableMetadata::
      adaptive_sort_override: bool` with `#[serde(default)]`).
      Depends on D1 (shipped).  Acceptance gate: `order_by_multi`
      shape in `vortex_vs_parquet_smoke` improves ≥ 2× after a
      compaction sweep on a table with `adaptive_sort_override=true`
      and no `cluster_columns`.
- [x] **5.14.D3** Catalog-aware `WindowExec` (shipped `ee80b36` + `1b86751`) — custom DataFusion
      `ExecutionPlan` that consults `basin.sort_by` / discovered
      file-sort-order via the catalog and skips the full sort when the
      window's `PARTITION BY` matches. Files:
      `crates/basin-engine/src/physical/window_exec.rs` (new).
      Acceptance gate: `window_partition_sum` + `lag_lead_window` plans
      drop the `SortExec` step when sort-matching is detected, verified
      by `EXPLAIN ANALYZE`.
- [x] **5.14.D4** Multi-sort + catalog-aware WindowExec differential —
      assert identical results vs the today-baseline plan. Files:
      `tests/integration/tests/catalog_window.rs`. Acceptance gate: 0
      differential rows on the relevant 88-shape battery slice.
      Shipped `2ce0e7c` (test bundled with docs commit); test green:
      `order_by_multi` 50 rows, `window_partition_sum` 250 rows,
      `lag_lead_window` 50 rows — all identical baseline/D2/D3;
      D2 `compactions_with_adaptive_sort = 1`; D3 SortExec elision
      confirmed for `lag_lead_window`.

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
- Any feature that does not depend on Basin's catalog + multi-project
  layer for correctness (i.e., anything that could equally well ship in
  upstream Vortex or upstream DataFusion).
- (Reserved for future entries.) WebSocket realtime moved to Tier 4
  (5.11.R series) — committed, no longer gated.

## Phase 6.SEC — Security P0/P1 fixes from 2026-05-21 audit **(BETA-BLOCKER)**

Audit: `docs/audits/2026-05-21-security-audit.md` (3 P0 / 5 P1 / 7 P2 / 17
test-gaps; commit landing alongside this section). **All P0s must land
before any beta** — two MFA crypto holes mean aal2 is currently security
theater; an unauthenticated inbound-webhook lets anyone trigger registered
SQL with their project ID.

Sequencing: P0s in parallel (different files), then P1s, then the
regression-test backfill in `tests/integration/tests/security.rs`.

- [x] **6.SEC.P0.1 — TOTP replay protection actually wired**
      (`crates/basin-auth/src/mfa.rs`). Replaced the `&HashSet::new()` no-op
      with the RFC 6238 §5.2 monotonic-counter rule: the highest accepted
      30-second step is persisted on the factor row (new `last_used_step`
      column, monotonic via `GREATEST`/`max`) and any code at or below it is
      rejected. Burned on both factor-verify and challenge-verify. Tests:
      `totp_used_code_rejected_on_replay`, `totp_next_window_code_still_accepted`,
      `mfa_cache_last_used_step_is_monotonic`.
- [x] **6.SEC.P0.2 — WebAuthn signature verification** (`mfa.rs`). Replaced
      the echo-only stub with real FIDO2 crypto (pure-Rust `p256` + `ciborium`,
      no openssl): registration parses the CBOR attestation object + COSE P-256
      key and binds clientData type/challenge/origin + rpIdHash; assertion
      verifies the ECDSA-P256 signature over `authData || sha256(clientDataJSON)`
      and enforces a strictly-advancing sign-counter (clone detection). Tests:
      `webauthn_forged_attestation_rejected`, `webauthn_valid_assertion_accepted`,
      `webauthn_bad_signature_rejected`, `webauthn_counter_regression_rejected`
      (+ `mfa_webauthn::webauthn_counter_regression_rejected` integration test
      driving a software P-256 authenticator).
- [x] **6.SEC.P0.3 — Inbound webhook authentication** (LAST P0 beta-blocker
      closed). Added `secret_hex` column to `InboundWebhookDef` + a
      `CREATE INBOUND WEBHOOK <name> [WITH SECRET '<hex>'] EXECUTE <body>`
      DDL clause; auto-generates a 32-byte random hex token when omitted
      and surfaces it as a single-row `secret` result set (only time the
      plaintext leaves the server). `POST /in/:project_id/:name` now
      requires `X-Basin-Signature: <hex>` = HMAC-SHA256(body, secret),
      verified in constant time via `subtle::ConstantTimeEq`. The
      ADR-0019-promised `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=1` env gate
      now exists and only widens the *unsigned* path (signed requests
      still go through full verification). Tests:
      `inbound_webhook_row_insert_signed`,
      `inbound_webhook_unsigned_rejected`,
      `inbound_webhook_bad_signature_rejected`,
      `inbound_webhook_unsigned_unknown_name_401`,
      `inbound_webhook_signed_unknown_name_401` (basin-rest) +
      `tests/integration/tests/inbound_webhook_auth.rs` (end-to-end
      round-trip incl. constant-time-compare smoke + debug-bypass scenario)
      + `verify_signature_*` unit tests for hex/length/ct-compare paths.
      **All 3 security P0s are now closed (P0.1 TOTP replay, P0.2 WebAuthn,
      P0.3 inbound webhook auth).**

- [ ] **6.SEC.P1.* — five P1 findings** detailed in the audit (OAuth
      identity-link via unverified email, signed-URL secret rotation
      missing, presence `client_id` impersonation, public-bucket alias
      gap, reserved-schema CREATE/DROP open). Each ~½–1 day; file:line in
      audit §4.
  - [x] **6.SEC.P1 (OAuth)** — account-takeover via unverified email closed.
        Auto-link to an existing account now requires the provider's honest
        `email_verified=true` claim (removed the unconditional GitHub
        override at `oauth.rs` callback); identity lookup is keyed by
        `(provider, provider_user_id)` so a `sub` reused across providers
        resolves to distinct identities. Tests: `oauth_flow.rs`
        (`unverified_email_does_not_link_to_existing_account`,
        `existing_identity_by_sub_logs_into_same_account`,
        `same_sub_across_providers_resolves_distinct_identities`) +
        `oauth.rs` unit tests for absent/false `email_verified`.
  - [x] **6.SEC.P1 (reserved-schema)** — user DDL targeting reserved system
        schemas now rejected with SQLSTATE 42501 (`permission denied for
        schema "<name>"`). Guard `guard_reserved_schema_for_user_ddl` in
        `crates/basin-engine/src/schema_ddl.rs` is invoked from every
        user-DDL entry point that accepts a schema-qualified name:
        `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE INDEX`,
        `DROP INDEX` (executor.rs); `CREATE FUNCTION`, `DROP FUNCTION`
        (function_ddl.rs); `CREATE POLICY`, `ALTER POLICY`, `DROP POLICY`
        (rls.rs). `public` and bare names pass through unchanged.
        Internal subsystems (basin-auth, basin-blob, basin-cron, basin-net,
        basin-realtime) write tables sqlx-direct against Postgres with
        prefixed names (`basin_auth_users`, etc.) — they don't flow
        through this SQL parser, so the guard does not affect them; their
        existing integration tests stayed green. New `BasinError::PermissionDenied`
        variant maps to SQLSTATE 42501 in `basin-router/src/error.rs`.
        Tests: `crates/basin-engine/tests/reserved_schema_ddl_guard.rs`
        (14 tests — CREATE TABLE in each reserved schema rejected, DROP
        TABLE rejected even with IF EXISTS, ALTER TABLE / CREATE INDEX
        guarded, `public.foo` + bare names + user `CREATE SCHEMA myapp`
        still work).

- [x] **6.SEC.T (#21) — 17 regression tests** in
      `tests/integration/tests/security.rs` from audit §6. Shipped P0/P1
      fixes get green tests asserting the FIX is in place (TOTP replay +
      monotonic counter, WebAuthn structural + counter + pointer guards,
      inbound webhook default-secure + sig-shape + bypass-env-name,
      presence forged client_id + metadata cap, OAuth unverified-email +
      same-sub-distinct-per-provider, recovery-code hash format, JWT
      alg:none reject, P2-6 pgwire-user routing hint safety, cross-project
      fuzz pointer). Remaining audit P1/P2 surfaces with no fix yet land
      as `#[ignore]`d "expected failing" tests documenting the gap
      (P1-1 WS filter, P1-3 redirect_to origin, P1-5 admin project binding,
      P2-1 signed-URL rotation, P2-3 OAuth state TTL, P2-4 MIME sniff,
      P2-5 reserved-schema DDL, P2-7 refresh-token compromise window).
      28 green + 8 ignored under `cargo test -p basin-integration-tests
      --test security`.

---

## Phase 6.X — Lease-based ownership + partition routing — ADR 0023 **(TOP PRIORITY)**

The architectural commitment that fixes hot-project pinning AND multi-instance
cap bypass in one shape. Driven by the noisy-neighbor audit
(`docs/audits/2026-05-21-noisy-neighbor-fairness.md`, 4 P0 / 6 P1 / 5 P2-P3).
Full design in [ADR 0023](./docs/decisions/0023-leases-and-partition-routing.md).

**Why TOP PRIORITY:** every per-project cap in OSS today (REST QPS, pgwire QPS,
HTAP memtable bytes, realtime `BUFFER_FULL`, Wasm semaphore, basin-net
outbound) is per-process — a project on N replicas gets `N×` the cap they
should. Hot projects pin one replica at 100% while siblings idle. The new
basin-cloud overage prices (`acb2f7c`: storage $0.010/GB-mo, compute
$0.018/CPU-hr) cannot bill correctly until this is fixed. Blocks any
multi-replica production deployment.

### Phase 6.P0 — single-instance mechanical fixes (parallel, ~1 day each)
Independent of 6.X; ship immediately while 6.X is being built. Each closes
an audit P0 single-instance gap on its own.

- [x] **6.P0.A — Statement-level wall-clock + CPU timeout** in the executor
      hot path. `BASIN_STATEMENT_TIMEOUT_MS` default (e.g. 30 000). Closes
      "any hostile/buggy query runs forever" today. Files:
      `crates/basin-engine/src/{executor,session}.rs`. Test: long-running
      aggregate is cancelled at the timeout; clean SQLSTATE 57014
      (`query_canceled`).
- [x] **6.P0.B — Catalog connection pool** — replaced `Mutex<Client>` in
      `crates/basin-catalog/src/postgres.rs` with `deadpool-postgres` (default
      pool size 16, override via `BASIN_CATALOG_PG_POOL_SIZE`). Every DDL no
      longer serializes every catalog read across all projects; multi-statement
      transactions still run on one checked-out connection so their semantics
      are preserved. Lease impl (`postgres.rs::LeaseRegistry`) reuses the
      same pool. Structural test (`pool_exposes_distinct_postgres_sessions`)
      verifies concurrent checkouts surface ≥4 distinct PG backend pids
      where the old shared `Client` could only ever surface one. Commits
      8977fa6 (refactor) + c36790a (test).
- [x] **6.P0.C — Wasm on a dedicated tokio runtime + wire W5 caps to real
      entrypoint** (combined with W5-followup #16). Five fixes in
      `crates/basin-fn`:
      1. `ComponentHarness::with_governance` + `run_with` re-added so every
         invocation routes through `FunctionGovernance::invoke_with_caps`
         (CPU epoch trap, per-Store `MemoryLimiter`, wall guard, per-project
         semaphore). Mirrored on `HandlerHarness::with_governance` +
         `handle_with`; `FunctionRuntime::with_caps_governance` plugs the W5
         caps into the W6 catalog-aware registry. Closes audit finding #1:
         the W5 caps had zero non-test callers.
      2. Epoch ticker spawned per governance instance — `set_epoch_deadline`
         now has teeth on the real path.
      3. `FunctionHost` carries an optional `MemoryLimiter`; installed onto
         every governance-wrapped `Store` via `Store::limiter(...)` so the
         linear-memory cap is per-invocation (not just engine-wide).
      4. New `WasmRuntime` owns a multi-thread `tokio::runtime::Runtime`
         sized via `BASIN_FN_WORKER_THREADS` (default 4) on a side thread
         so wasm-blocking no longer competes with axum / shard-mode
         executor / basin-net on the global pool.
      5. Per-project `DashMap<ProjectId, Arc<Semaphore>>` replaced by a
         bounded `Mutex<LruCache<...>>` (default 10 000 entries via
         `BASIN_FN_PROJECT_SEM_CAP`) — closes #16 (per-project cost is now
         O(bytes), not O(distinct projects ever seen)).
      Acceptance tests in `tests/governance_caps_test.rs`:
      `component_harness_run_with_cpu_cap_kills_spinner` (spinner via the
      real entrypoint),
      `dedicated_runtime_does_not_starve_main_runtime` (1k-soak +
      main-runtime canary stays < 500 ms),
      `per_project_semaphore_lru_evicts_at_scale` (10k cold projects → LRU
      cap holds).

### Phase 6.X — the architectural commitment (~6–10 weeks single-engineer)
Sequencing: **A → B**, **A → D**, **B → C**, **A–D → E + F**.

- [ ] **6.X.A — Lease table + leaseholder primitive (~2 wk).** New table
      `basin_catalog.partition_leases (project_id, partition_id, holder,
      epoch, granted_at, expires_at)`. `LeaseRegistry` trait + Postgres impl;
      acquire / renew / steal-on-expiry / release. Per-replica heartbeat
      loop (default 5 s renew, 15 s TTL). Epoch fencing token at the WAL
      append site so a dual-leaseholder (network partition) → loser's
      appends fail at WAL write time. **FOUNDATION — blocks B/C/D/E/F.**
      Files: `crates/basin-catalog/src/{leases,postgres}.rs` (new
      `leases.rs`), `crates/basin-wal/src/file_wal.rs` (epoch fence on
      append), `crates/basin-shard/src/in_process.rs` (heartbeat loop).
- [x] **6.X.B — Partition-level routing (~2 wk).** `ShardMap::shard_for(project)`
      → `LeaseAwareShardMap::owner_for(project, partition)` (ADR 0023).
      Router consults the `LeaseRegistry::owner_of(project, partition)` and
      caches the result with a configurable TTL
      (`BASIN_ROUTER_LEASE_CACHE_TTL_MS`, default 5000 ms). On cache miss the
      local replica falls back to `acquire` (v1 "any replica can become
      owner if free" rule). Default 1 partition per project preserves the
      pre-6.X.B back-compat path byte-equivalently (single
      `(project, "_default")` lease lives on whichever replica acquires
      first). Whales partition via `ALTER PROJECT <name> SET partitions = N`
      (persisted in `ProjectStorageConfig::provider_extras` under
      `"basin.partitions"`; 1 ≤ N ≤ 1024). Multi-partition test verifies
      4 partitions across 4 replicas distribute to 4 distinct shard owners.
      Files: `crates/basin-router/src/{sharding,lib}.rs`,
      `crates/basin-engine/src/{alter_project,executor}.rs`. **Depends on A.**
- [x] **6.X.C — Lease handoff under load (~1 wk).** A replica voluntarily
      yields a lease on coordinator request via `Shard::yield_partition`
      (`crates/basin-shard/src/in_process.rs::InProcessShard::yield_partition`):
      mark draining → pre-compaction WAL flush → drain in-memory tail to
      Parquet via the existing `compact_one` path → append a
      `Handoff { to_holder, at_epoch }` WAL marker (informational; replay
      no-op per `basin-wal::replay_wal`) → release the lease in the catalog
      → drop in-memory state. New writes against a draining partition
      surface the typed `BasinError::LeaseHandoffInProgress` error (router
      maps to SQLSTATE `40001` so drivers retry against the new owner after
      a fresh route resolution). **Measured p99 = 19 ms** across 10 runs
      (small-memtable 10-row case); 50-row end-to-end stall = 22 ms — well
      under the < 500 ms ADR target. Larger-memtable note: a multi-hundred-MB
      memtable is dominated by the cold-tier write step (object-storage PUT
      bandwidth, ~100 MB/s typical); operators should size partitions so
      single-partition memtable depth fits the target window, or accept a
      larger one-off stall. Tests: `in_process::tests::handoff_*`
      (unit, 4 tests) + `tests/integration/tests/lease_handoff.rs` (2
      end-to-end). **Depends on A + B.**
- [x] **6.X.D — Heartbeat budget reconciliation (~1 wk).** Per-replica
      heartbeat carries per-`(project, partition)` usage deltas; coordinator
      computes project totals and writes per-partition slice budgets into
      the heartbeat response. `basin-catalog::budgets` ships the foundation:
      `CapKind` enum (6 sites), `UsageDelta` / `ProjectBudget` /
      `SliceBudget`, `BudgetCoordinator` trait + `InMemoryBudgetCoordinator`,
      lock-free `SliceBudgetView` + token-bucket `SliceGate`. All six cap
      consumers wired: REST QPS (`basin-rest` authorize path), pgwire QPS
      (`basin-router::PgRateLimit::check_async`), memtable bytes
      (`basin-hottier::MemTableRegistry::try_reserve_bytes` uses
      `min(hard_cap, slice)`), realtime BUFFER_FULL
      (`basin-realtime::BudgetTracker::with_slice_view`), Wasm semaphore
      (`basin-fn::FunctionGovernance::invoke_with_caps` consumes one slice
      token before the per-project Semaphore), basin-net outbound
      (`basin-net::RateLimit::check_async`). Shard heartbeat
      (`InProcessShard::heartbeat_budgets`) pushes deltas on the same tick
      as the lease renew and fans the slice answer out to every registered
      view. Multi-replica acceptance test
      (`in_process::tests::multi_replica_heartbeat_aggregates_rest_qps_under_project_cap`):
      3 shards share one coordinator, project capped at 60 RestQps; after
      2 heartbeat rounds slice=20 each, aggregate admits=60 (not 3×60=180).
      Failure path: coordinator unreachable → views stay stale (safe).
      **Closes the multi-instance cap-bypass P0.** **Depends on A.**
- [x] **6.X.E — Failure-path hardening (~1–2 wk).** Replica-loss tests
      (kill mid-write, verify recovery within TTL); dual-leaseholder
      fencing test (force a partition; verify loser's WAL appends are
      rejected); network-partition simulator. **Depends on A–D.** Files:
      `tests/integration/tests/lease_failure_paths.rs` (new).
- [x] **6.X.F — Observability + ops (~1 wk).** OTLP-named lease metrics
      live in `basin_common::project_counters::LeaseMetrics`:
      `basin_lease_holdings_total{replica}` (gauge),
      `basin_lease_acquire_total{replica,result}` (counter),
      `basin_lease_renew_total{replica,result}` (counter),
      `basin_lease_handoff_duration_ms{replica}` (histogram; shape-only
      until 6.X.C lands), `basin_budget_over_cap_seconds_total{project,cap}`
      (counter), `basin_heartbeat_lag_ms{replica}` (histogram). Emission
      hooks added to `crates/basin-shard/src/in_process.rs` at the lease
      acquire / renew / heartbeat sites (additive, zero-cost back-compat
      when `lease_metrics` is `None`). Operator runbook at
      `docs/operators/lease-ownership.md` covers ownership-model
      summary + dashboard reading, manual-rebalance sample SQL, whale
      identification via byte-counter snapshot + `basin_stat_statements`,
      partition-count tuning, stuck-lease incident playbook (manual steal
      via catalog UPDATE), and the failure-mode matrix. Forced-yield DDL
      + auto-reshuffle on `SET partitions=N` documented as TODOs blocked
      on 6.X.C. Tests:
      `in_process::tests::lease_metrics_emit_acquire_renew_and_heartbeat_lag`,
      `lease_metrics_emit_failed_acquire_for_contested_lease`,
      `lease_metrics_decrement_holdings_on_lost_lease`,
      `no_lease_metrics_path_does_not_panic`, plus 6 unit tests in
      `basin_common::project_counters`. **Depends on A–D.**

> **Explicitly out of scope (or deferred) for 6.X v1**: Raft / Paxos lease
> consensus (Postgres CAS is sufficient); distributed counters on the hot
> path (heartbeat reconciliation does the job at lower cost); separate
> budget service (catalog Postgres is the only coordinator); cross-project
> budget aggregation (caps are per-project; org rollups are basin-cloud);
> auto-rebalance heuristics (ops trigger manually for v1; auto when a real
> customer needs it). All recorded in ADR 0023.

---

## Phase 6.TR — Type round-trip + UUID-in-Vortex workaround (ADR 0024) **(PRIORITY)**

Surfaced by the post-Phase-6.X triage (task #40). Two clusters that
share storage-layer plumbing; do them as one wave because C1 is gated
on C4 and the fix lives in the same files.

**Why this is priority:** UUID is the default primary-key type. Until
C1 closes, any table with a UUID column can't be written to Vortex —
the storage moat (ADR 0015) doesn't apply where customers most need it.
C4 unblocks not just UUID but the entire `BASIN_TYPE`-tagged logical-type
family (MONEY, INET, CIDR, MACADDR, MACADDR8, BIT, VARBIT) — 7 extra_types
tests + 3 UUID tests = 10 ignores removed in one wave.

- [ ] **6.TR.A — `BASIN_TYPE` field-level metadata round-trip** (cluster C4).
      Write path preserves field metadata into Vortex; read path restores
      it on the way back. The 7 `extra_types` tests already encode the
      desired behavior — they currently fail because the metadata is lost,
      so the reader can't tell `Decimal128` from `MONEY`, `FixedSizeBinary(6)`
      from `MACADDR`, etc. **Blocks 6.TR.B; pays for itself across MONEY,
      INET, CIDR, MACADDR, MACADDR8, BIT, VARBIT** beyond the UUID work.
- [ ] **6.TR.B — UUID-as-Decimal128 storage encoding** (cluster C1, ADR 0024).
      Single-boundary translation in basin-storage: `FixedSizeBinary(16) +
      BASIN_TYPE="uuid"` ↔ `Decimal128(38, 0) + BASIN_TYPE="uuid"` (BE
      unsigned magnitude). Engine, planner, pgwire, REST keep treating
      UUID as `FixedSizeBinary(16)` — the translation is hidden below the
      storage trait. Tag the conditional branches `TODO(adr-0024)` so the
      eventual upstream-Vortex removal is mechanical. Closes
      `jsonb_uuid_param_binding`, `smoke_pgx`, `viability_uuid`.
- [ ] **6.TR.C — Upstream Vortex `FixedSizeBinary(N)` encoder PR** (parallel
      track, no Basin-side blocking work). Open a PR to vortex-data/vortex
      adding the encoder; when it lands and we pin the new version,
      delete the 6.TR.B translation layer (the `BASIN_TYPE` sidecar from
      6.TR.A stays — it's still needed for the other logical types).

> **Explicitly out of scope for 6.TR:** changing engine/planner/pgwire to
> see Decimal128 instead of UUID — that would leak the storage detail
> across the codebase. The whole point of ADR 0024 is that the boundary
> is single-layer.

---

## Phase 6 — Production hardening (3–4 months)

- [x] **Subsystem Cargo feature flags + minimal pgwire-only build** (ADR 0018) —
      `auth`, `rest`, `webhooks`, `realtime`, `wasm-udf` gated in
      `services/basin-server/Cargo.toml`; `default = ["auth","rest","webhooks"]`;
      `minimal = []` (pgwire + engine + storage + catalog + WAL only).
      Both `cargo build -p basin-server` (full) and
      `cargo build -p basin-server --no-default-features` (minimal) verified clean.
- [ ] Multi-region: regional WAL + S3 cross-region replication
- [~] Catalog replication strategy chosen and implemented — strategy
      chosen in [ADR 0010](./docs/decisions/0010-catalog-replication.md)
      (single-writer global Postgres + regional read replicas via PG
      logical replication); implementation phases tracked there. v0.1
      implementation deferred per the ADR's own milestone gating.
- [~] Point-in-time restore via Iceberg snapshots — v0.1 catalog-level
      `Catalog::rollback_to_snapshot(project, table, snapshot_id)` ships
      (InMemory + Postgres impls; truncates history to ≤ target,
      rewinds head pointer). **Physical GC now shipped** via
      `Catalog::gc_orphaned_files(project, table)`: rollback accumulates
      discarded-snapshot paths in `TableState::gc_orphan_paths`; GC sweep
      cross-references the project-wide live set (respecting forks) and
      returns only paths with refcount 0 as orphaned. Verified by
      `tests/integration/tests/pitr_fork_gc.rs`. Remaining gap: soft-delete
      of replaced files (cross-DML rollback), which is a v0.2 prerequisite.
- [~] Branching / forking via copy-on-write catalog metadata — v0.1
      catalog-level `Catalog::fork_table(project, src, dst)` ships
      (InMemory + Postgres impls). New table inherits source's schema /
      snapshot history / partition spec / RLS / tier / bloom / row-group
      / CV settings; data files are *shared by reference* (no Parquet
      copy). **Cross-project fork safety now shipped**: `gc_orphaned_files`
      uses the project-wide live set (union of all tables' `live_data_files`)
      so a path shared between source and fork is never reported as orphaned
      while the fork holds a reference; `file_refcount(project, path)` returns
      the exact count. Integration test `pitr_fork_gc.rs` covers rollback →
      GC, fork-share safety, and cross-project isolation.
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
- [x] Per-project + per-query + per-shard + per-WAL telemetry — `basin_common::ProjectCounterRegistry` aggregates ops/bytes_read/bytes_written/errors + ring-window p99 latency per project; `Engine::project_counters(&ProjectId) -> ProjectCountersSnapshot` exposes a cheap snapshot. Storage writer/reader and WAL append are wired to bump per-project byte counters; engine `ProjectSession::execute` bumps op + latency + error. **Billing-meter audit (#15, `docs/audits/2026-05-21-billing-meter-gap.md`) closed engine-side:** blocker 1/3 + 2/3 (Class-A/B op counts) shipped in `8bd64b5`; blocker 3/3 (`cpu_micros_total` aggregating engine query elapsed + Wasm `ComponentHarness::run_with` elapsed) shipped this commit — the engine now meters every cost dimension the 2026-05-21 reprice priced. Only the basin-cloud time-integrated GB-month sampler (cloud-side, hole #1/#2) remains.
- [x] BYO-key envelope-encryption hooks — `EncryptionProvider` trait shipped in `basin-storage::encryption`; `Storage::attach_encryption_provider` is the additive opt-in (default `None` = byte-for-byte plaintext path). Writer envelope-encrypts the Parquet body with a fresh per-file AES-256-GCM data key and persists the wrapped key as a `<path>.wrapped` sidecar; reader transparently unwraps. Reference adapter ships in `basin-storage::encryption_static`: `StaticKeyEncryption` (caller-supplied 32-byte root key) + `EnvKeyEncryption` (`BASIN_ROOT_KEY` env var) — both use AES-256-GCM key-wrapping, dep-light (reuses workspace `aes-gcm` + `rand`), fully testable offline. 86 existing tests green; 15 new adapter tests cover round-trip, wrong-key auth failure, tampered sidecar rejection, and default (no-adapter) path unchanged.

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

- [x] `LATERAL` joins — uncorrelated strip + nested-aggregate ORM rewrite
  shipped (commit `4faa5d7`); correlated decorrelation for non-aggregate row-
  returning bodies (#81, commit `6f7ab3c`); compound-WHERE nested-agg shape
  (e.g. `WHERE fk = outer.pk AND extra_col = val`) closed via
  `parse_corr_predicate_with_extra_filters` + `split_and_conjuncts` helpers;
  ORDER BY + LIMIT shape closed via window-function rewrite.
  Advanced LATERAL gap-closure (v0.1 gap): `CROSS JOIN LATERAL (SELECT agg…)`
  now rewrites to `INNER JOIN (SELECT fk, agg GROUP BY fk) ON fk`; multi-table
  child body (`FROM a JOIN b ON …` inside LATERAL) now handled via
  `parse_corr_predicate_multi_table` which detects FK without knowing child alias
  in advance; both shapes green in `lateral_cross_join_aggregate` and
  `lateral_multi_table_child_aggregate` integration tests.
  Remaining partial: OR predicates in the correlated WHERE clause (`WHERE fk =
  outer.pk OR alt_fk = outer.pk`) — OR cannot be safely decorrelated to a simple
  GROUP BY FK; DataFusion's physical planner does not execute
  `OuterReferenceColumn` so these queries return a clear planning error rather
  than silently mis-executing (contract verified in `lateral_or_predicate_clean_error`
  test).
- [x] `WITH RECURSIVE` + DML-in-CTE (INSERT) — multi-column RECURSIVE shipped
  (#82, commit `dae8765`); data-modifying CTEs shipped (commit `6056dca`);
  `WITH RECURSIVE … INSERT INTO target SELECT * FROM cte` now supported via
  `exec_recursive_with_dml_body` (regression test `cte_recursive_feeding_insert`
  green). UPDATE and DELETE with a recursive source now supported: CTEs are
  materialized as MemTables via DataFusion then the DML runs against the
  MemTable (regression tests `cte_recursive_feeding_delete` and
  `cte_recursive_feeding_update` added to `set_ops_ctes.rs`).
- [x] Advanced window frames — `RANGE BETWEEN INTERVAL '…' PRECEDING / FOLLOWING` now supported
  natively via DataFusion 53 type-coercion (no pre-parse rewrite needed); regression test
  `range_interval_5min_preceding_sum` added to `tests/integration/tests/window_fns.rs`.
- [x] `GROUPS BETWEEN … PRECEDING/FOLLOWING` — DataFusion 53 supports `GROUPS` frames
  natively via `WindowFrameStateGroups`; no pre-parse rewrite required. Integration tests
  `groups_1_preceding_current_row` and `groups_unbounded_preceding_current_row` added to
  `tests/integration/tests/window_fns.rs` (24 tests total, all green).
- [x] `EXCLUDE {CURRENT ROW | GROUP | TIES | NO OTHERS}` — **not supported**: sqlparser 0.61
  (used by DataFusion 53) does not parse the `EXCLUDE` clause on window frames (it has no
  `WindowFrameExclusion` type in `WindowFrame`). Queries using `EXCLUDE` will receive a clean
  parse error from sqlparser — no silent mis-execution. This is a known limitation of the
  sqlparser/DataFusion stack; no workaround is possible without upgrading sqlparser to a future
  version that adds `WindowFrame.exclusion`. Documented as a limitation, not a v0.1 blocker.
- [ ] `EXCLUDE USING gist` — not on roadmap (geo-index dependency).

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
