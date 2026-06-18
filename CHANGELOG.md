# Changelog

All notable changes to Basin are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The pre-1.0 contract: minor versions can break public API; patch versions
are bug-fix only. Once the engine wedge ships to design partners we
graduate to 1.0 and the standard SemVer guarantees.

## 2026-06-18 — Fix: DELETE fast path lost a row count for an absent PK (data-loss correctness)

- **`DELETE FROM t WHERE pk = <absent>` no longer reports a phantom deletion or
  corrupts `COUNT(*)`.** The hot-tier DELETE fast path (single-column-PK
  `pk = lit` / `pk IN (lits)`) wrote one tombstone per *requested* PK and
  reported `keys.len()` as the affected-row count — without confirming the key
  resolved to a live row. A DELETE of a key that does not exist (e.g. one far
  above the populated range, or one already deleted) therefore reported
  `DELETE 1` and, because the metadata-aggregate `COUNT(*)` fast path subtracts
  one per live tombstone, dropped the reported row count by one even though no
  real row was removed. The fix resolves the requested PKs against the live row
  set first — tier precedence tx-overlay > shared memtable > cold, via the same
  PK-point-probe machinery the UPDATE fast path uses — and tombstones/counts
  **only** the keys that resolve to a live row. An absent-key DELETE now reports
  `DELETE 0` and leaves `COUNT(*)` unchanged; a present-key DELETE removes
  exactly that row; a repeated DELETE of an already-deleted key reports
  `DELETE 0`; an `IN`-list reports exactly the count of present keys. The
  affected-row tag now matches Postgres (rows that actually existed and were
  deleted), mirroring the UPDATE fast path which was already correct.
- The `DELETE … USING` join fast path was **not** affected (its keys come from a
  live read of the target table) and is unchanged. The UPDATE fast path was
  already correct (it reports only PKs that resolved to an existing row).
- Regression guard: `dml_extras::fast_path_absent_key_delete_update_oracle` runs
  a long mix of present/absent DELETEs and UPDATEs (below, within, above, and at
  the boundaries of the seeded range, plus repeated deletes) against an in-test
  `HashSet` oracle and asserts, after every statement, that the reported
  affected count, `COUNT(*)`, and the enumerated surviving key set all match the
  oracle. Correctness over speed — the fast path stays a point-probe (O(matching
  files), not a full scan).

## 2026-06-18 — Cold-read: background whole-file prefetch + higher storage scan fan-out

- **Disk-cache whole-file prefetch moved off the cold critical path.** The
  speculative whole-file promotion (LEVER 3) — which warms a small file's whole
  body into the cache after a ranged miss so later range reads become zero-RTT
  slices — previously ran *inline*: the cold read that triggered it blocked on a
  second, full-file GET before returning. For a single-pass scan that touches
  only a subset of columns this added a round-trip and transferred bytes the
  cold query never used; measured against a 30 ms-RTT object store it made a
  cold `count(distinct)` scan ~4× slower (≈408 ms) than serving the requested
  range directly. The promotion now runs in a **detached background task**: the
  cold read returns its requested range immediately, and the whole file warms
  asynchronously for subsequent reads. A new path-level dedup
  (`prefetch_inflight`) ensures the several distinct ranges a cold file-open
  fires near-simultaneously elect **one** background prefetch per file, not one
  per range. Same cold `count(distinct)` measured at ≈103 ms after the change.
  Files larger than `BASIN_DISK_CACHE_SPECULATIVE_BYTES` (default 4 MiB) are
  still never whole-file prefetched. Returned bytes are byte-identical to a
  direct ranged GET.
- **Higher default file-scan fan-out on the storage read path.** The
  table-wide reader (`read_paths_inner`) fetched + decoded files with a fixed
  concurrency of 4, serialising a cold multi-file scan into `ceil(n/4)` waves.
  Raised the default in-flight fan-out to 16 (override
  `BASIN_READ_FILE_CONCURRENCY`), keeping ordered (`buffered`) emission so the
  row order handed downstream is unchanged — only the fetch concurrency
  changes, never the result set or its order. The per-project concurrency
  permit pool remains the real ceiling.
- General across any cold scan of any table; not benchmark-shaped. No change
  to query results, the warm path, or default durability behaviour.

## 2026-06-18 — Cached libpg_query parse: lower warm per-statement floor

- The libpg_query parse that runs at the top of every statement
  (`pg_ast::parse`, the C-library PostgreSQL grammar reduction +
  protobuf build) is now memoized behind a process-global LRU
  (`pg_ast::parse_cached`, default 256 entries, overridable via
  `BASIN_ENGINE_PG_PARSE_CACHE_SIZE`). On a cache hit the executor gets a
  cheap `Arc<ParseTree>` pointer clone instead of re-reducing the grammar.
- Mirrors the existing sqlparser statement cache
  (`executor::parse_sql_cached`) exactly, including its safety argument: the
  parse tree is a pure, text-deterministic function of the SQL bytes (no
  session or catalog state enters it), so the same SQL hash yields the same
  tree and the cached `Arc` is read-only on every consumer
  (`stmts`, `stmt_kind`, `ctas_shape`, `reject_unsupported`). Errors are
  never cached; the recursive-descent depth guard still runs on the miss
  path. Process-global so a connection pool round-robining sessions keeps the
  cache warm across checkouts.
- Removes a measured ~30µs fixed cost per statement (~11% of the warm
  server-side point-lookup latency) for the repeat-shape OLTP workloads the
  small-scale point/range cards exercise, with no change to results,
  prepared-statement semantics, or the extended-query path. Measured locally
  on a LocalFS engine driving a warmed `id = $1` point lookup.

## 2026-06-18 — Catalog-driven file discovery: zero LIST RPCs on warm scans/writes

- `Storage::list_data_files` (and through it `list_data_files_with_stats`) and
  the storage-layer `read()` file-discovery path now serve the table's live
  data-file set straight from the attached catalog
  (`Snapshot::live_data_files`) instead of issuing object-store `LIST` RPCs
  against the hot `data/` and cold `cold/` tier prefixes. The catalog already
  holds the authoritative file set (path + size + row-count + column-stats,
  committed transactionally at write/compaction time), so a warm scan or write
  on a RAM-resident table now makes **zero** `LIST` round-trips.
- Eliminates a fixed ~2× object-store `LIST` per query (≈50 ms combined on
  intra-region S3/Tigris at ~25 ms/RTT) that previously hit every range scan,
  aggregate, `ORDER BY … LIMIT`, single-row `INSERT`, `UPDATE`, and `DELETE`
  — collapsing the scan/write latency floor toward the point-lookup floor.
  Measured locally against MinIO with per-op object-store tracing: range /
  full-agg / top-k / update / insert went from 2 `LIST`s each to 0.
- Strict round-trip elimination, not a visibility change: the catalog and a
  `LIST` enumerate the same physical files, and un-flushed rows are still
  tail-merged from the in-memory memtable on the read path. Falls back to the
  object-store `LIST` when no catalog is attached or the table is not
  catalog-known (schema-less callers, integration tests), so behaviour for
  those paths is unchanged.

## 2026-06-18 — Multi-region: `http-forward` write transport + receive endpoint (ADR 0009)

- `BASIN_WRITE_FORWARD_MODE=http-forward` now actually forwards a non-home
  auto-commit write to the project's home region over Fly 6PN HTTP and returns
  the home region's result. Previously the transport was a placeholder
  (`UnconfiguredForwardClient`) that failed loud, and no route received a
  forwarded statement — only `fly-replay` worked.
- New receive route `POST /internal/v1/forward` on the engine REST server: body
  `{project_id, current_user, sql}`. It opens a home-region session as that
  principal (so `current_user`/RLS resolve correctly), executes the statement,
  and returns the `ExecResult` as JSON (`{kind:"empty",tag}` or
  `{kind:"rows",ipc_b64}`, where rows are a base64 Arrow IPC stream preserving
  schema + values).
- Security / fail-closed: the route executes arbitrary SQL as an arbitrary
  principal, so it is **only mounted when `BASIN_FORWARD_SECRET` is set**, and
  every request must carry that secret in the `x-basin-forward-secret` header
  (constant-time compare; 401 on missing/mismatch). On Fly it is reachable only
  over the private 6PN `.internal` network. With no secret the route does not
  exist (404). The `http-forward` *client* is likewise only installed when the
  mode is `http-forward` AND peers (`BASIN_REGION_PEERS`) AND
  `BASIN_FORWARD_SECRET` are all configured; otherwise the fail-loud placeholder
  remains.
- `fly-replay` and `off` modes are unchanged; unset/empty mode or a missing
  secret/peers behaves identically to before.

## 2026-06-18 — Observability: `GET /metrics/inflight` exposes in-flight + latency for autoscaling

- New REST route `GET /metrics/inflight` returns a small JSON snapshot of
  engine-wide load: `inflight` (concurrency gauge of statements executing now),
  rolling-window `p50_micros`/`p99_micros` query-latency percentiles, `samples`,
  `window_secs` (default 10), plus `started_at`/`observed_at` (RFC3339 UTC) and a
  `goroutines` mirror of `inflight`. Previously this route 404'd, so an external
  autoscaler had no load signal.
- Backed by a process-global `inflight_metrics` module: an `AtomicI64` gauge with
  an RAII guard (decrements + records latency on drop, including error/unwind
  paths) and a mutex-guarded latency ring pruned to the window on read. The gauge
  wraps the pgwire/REST statement chokepoints (`ProjectSession::execute` and
  `execute_bound`) — one wrap per statement, no double-count. One atomic inc/dec +
  one mutex push per query; polled once per scrape.

## 2026-06-18 — Perf (OLTP): point/small INSERT prunes instead of rebuilding the PK set

- Single-row INSERT was ~200 ms+ because `enforce_pk_on_insert` on a sub-threshold
  table used the cached-set path, which rebuilds/re-reads the existing PK set —
  and since every INSERT changes the file set it cache-missed each time, re-reading
  the whole table per insert.
- Now: any INSERT with a small incoming batch (≤256 rows, the OLTP point/small-write
  case) takes the prune/stream path regardless of table size: **zone-map prune** on
  the first PK column (BIGINT for narrow/wide/composite alike) skips files whose
  range can't contain the key, and **per-file PK bloom prune** (single-column i64
  PKs) skips files whose bloom says the key is absent. A serial/above-range or
  random key therefore reads ~0 existing files. Shape-agnostic (single-i64,
  composite, text PKs all handled; composite/text fall back to zone-map prune).
  Correctness preserved (bloom false-positives only cause a needless read;
  false-negatives are impossible) — verified by tests/pk_streaming.rs.

## 2026-06-17 — Perf: keep Vortex footers/zone-maps RAM-resident (cold-read + point-lookup win)

- The Vortex footer cache was capped at 512 entries (LRU) while the Parquet meta
  cache holds 16k — so a many-file table re-fetched footers from the object store
  on every query, slowing cold OLAP and the point-lookup file-open. Bumped the
  default to 16,384 (≈ a few MB resident; footers are tiny) + added
  `BASIN_STORAGE_VORTEX_FOOTER_CACHE_CAP`. Now zone-map pruning never re-reads a
  footer from the bucket, so most point/range queries open exactly one data file
  — directly helps both OLAP cold reads and OLTP point lookups (the bloom-pruned
  single-file open is served from cache). Page cache is bumped per-deployment via
  `BASIN_PAGE_CACHE_MAX_BYTES` (engine default stays conservative for small OSS boxes).

## 2026-06-17 — Observability: shard tail-pressure warning (bounded-memory signal)

- A 10M-row wide COPY OOM-killed a 2 GB engine: the shard's in-memory tail
  (uncompacted batches) grows with ingest rate and, for wide rows, outran the
  time-ticked compactor. `write_batch_inner` now measures the resident tail
  bytes (cheap: `get_array_memory_size` over the few large tail batches) and
  WARNs when it crosses a 256 MiB soft cap — making "compaction not keeping up"
  observable to the operator/autoscaler. The proper fix (synchronous
  backpressure: drain the tail when over cap so the writer blocks on compaction
  throughput, keeping RAM `O(cap)` regardless of ingest rate) needs a refactor
  to let the per-partition handle invoke the shard compactor — tracked as a
  follow-up. Interim mitigation: 8 GB dev boxes.

## 2026-06-17 — Feat: per-project (per-tier) pgwire rate limiting

- The pgwire request rate limiter was per-project-keyed but with a single
  GLOBAL quota (`BASIN_PGWIRE_RATE_LIMIT_QPS`) — every project got the same cap,
  with no per-tier differentiation. Now per-project: the limiter carries an
  override map of dedicated token buckets and resolves each project's quota
  lazily from the catalog (`get_project_rate_limit_qps`) on first query, cached
  thereafter (one fetch per project per process; the hot-path check is an O(1)
  map read + bucket check — no added per-request catalog cost).
- Plumbing mirrors max-connections: `Catalog::{get,set}_project_rate_limit_qps`
  (default no-op; InMemoryCatalog stores it), `POST/GET
  /admin/v1/projects/:id/rate-limit` for the control plane to push per tier, and
  the global env qps becomes the default when no per-project value is set.
- Isolation verified by unit test: a project with a tight cap is throttled while
  a project without an override is unaffected (one project's cap cannot starve
  another) — the per-project noisy-neighbor guard at the request-rate dimension.

## 2026-06-17 — Feat: super-admin cross-project live usage analytics

- New `GET /admin/v1/usage` (admin-global, `require_admin` only): per-project
  live usage across ALL projects on the engine instance — ops, bytes r/w,
  Class-A/B object-store ops, CPU-micros, errors, p99-latency estimate — sorted
  by CPU-micros descending so the heaviest/largest projects surface first. The
  operator's "who's hot / who's big / who's erroring" view. Reuses the existing
  per-project atomic counters (O(1), no hot-path cost) + `auth.list_projects()`;
  read-only. The control plane fans this out across engine instances for a
  fleet-wide dashboard.

## 2026-06-17 — Fix: DROP TABLE purges object-store files (no ghost rows on re-create)

- `DROP TABLE` dropped the catalog row, hot-tier overlay, and shard tail but
  **left the table's cold data files in object storage**. A later
  `CREATE TABLE` of the SAME name then inherited those orphaned `.vortex`
  files — surfacing as ghost rows: spurious `duplicate key` errors on a fresh
  PK table, or inflated `count(*)` on a no-PK table. (The materialized-view
  drop path already deleted its files; regular DROP TABLE did not.)
- Fix: new `Storage::delete_table_prefix` deletes everything under
  `…/projects/{project}/tables/{table}/` (all tiers + index segments), called
  by `exec_drop_table` after the shard tail is cleared (so no in-flight
  compaction is still writing under the prefix). Verified: COPY 50k → DROP →
  re-CREATE same name → COPY 50k yields exactly 50k rows, no ghost duplicate.

## 2026-06-17 — Fix (major): pooled sessions now use the fast batched COPY path

- `PooledSessionWrapper` (the session the pgwire router uses whenever
  `BASIN_POOL_ENABLED=1` — the default dev/prod config) did not override
  `ingest_csv_batch`, so it hit the trait's default impl which returns
  `FeatureNotSupported`. The router then fell back to **per-row INSERT** for
  EVERY `COPY`, on every table, in the pooled configuration — the fast bulk
  path was effectively dead in production.
- Impact was severe and PK-amplified: measured on the shard+object-store path,
  bulk `COPY` ran at **~350 rows/s for a PRIMARY KEY table** (each row paying a
  full INSERT + constraint + WAL cycle) vs **~2.0M rows/s** once batched — a
  ~5900× regression. Large PK COPYs were slow enough to trip the edge proxy's
  connection timeout, surfacing to clients as "server closed the connection
  unexpectedly" mid-COPY.
- Fix: forward `ingest_csv_batch` from `PooledSessionWrapper` to the inner
  session (same delegation as every other `Session` method). COPY now takes the
  batched fast path under the pool. No-PK and PK COPY both ~2M+ rows/s locally.

## 2026-06-17 — Dev/test: opt-in plaintext-HTTP S3 endpoints (BASIN_STORAGE_ALLOW_HTTP)

- The S3-compatible backend hard-rejected non-HTTPS endpoints, which blocked
  pointing the engine at a local MinIO/RustFS dev server over `http://127.0.0.1`.
  `BASIN_STORAGE_ALLOW_HTTP=1` now permits a plaintext endpoint (sets
  object_store's `allow_http`); HTTPS stays mandatory by default so production
  credentials never traverse plaintext. Enables local S3-path reproduction/tests
  without TLS certs.

## 2026-06-17 — Fix: S3/Tigris custom endpoints use path-style (was dropping the bucket → 403)

- The S3-compatible storage backend forced virtual-hosted-style requests for all
  providers. With a custom endpoint that has no per-bucket subdomain (Tigris's
  `https://fly.storage.tigris.dev`, MinIO, Wasabi, …), object_store then emitted
  `{endpoint}/{key}` and **dropped the bucket** — the first object-key segment
  (`projects/…`) was misread as the bucket name, so every PUT 403'd
  (`AccessDenied`). Symptom: COPY/INSERT succeeded into the WAL/memtable but
  compaction to object storage failed on every tick, so `count(*)` stayed 0 —
  data never became durable/queryable.
- Now: a **custom endpoint → path-style** (`{endpoint}/{bucket}/{key}`), which
  Tigris/MinIO/Wasabi support and which keeps the bucket in the request. AWS S3
  proper (no custom endpoint) keeps virtual-hosted-style. Surfaced the first time
  the dev engine actually ran on Tigris (it had silently defaulted to local
  storage because `BASIN_STORAGE_BACKEND` was unset).

## 2026-06-17 — Perf/scale: bounded-memory PRIMARY KEY enforcement on bulk COPY

- `enforce_pk_on_insert` previously materialized the **entire** existing PK set
  in RAM (a `HashSet` of every key in the table) to detect collisions — `O(rows)`
  memory, which OOMs a large PK table (a 1B-row `BIGINT` PK needs ~8 GB+ just for
  the keys) and is the single blocker for scaling COPY to 1B rows on a small box.
- **New bounded path** (`enforce_pk_streaming`): above a row-count threshold the
  check sorts the incoming batch's keys once, prunes data files whose first
  PK-column zone-map `[min,max]` cannot overlap the batch's key range (reusing
  the storage layer's `evaluate_compound_for_pruning`), then streams each
  surviving file's PK column and binary-searches every existing key into the
  sorted batch. Peak memory is `O(batch)` + one file chunk, **not** `O(table)`.
  Correctness is byte-identical to the in-RAM path (same `UniqueViolation`,
  same intra-batch + NULL checks; pruning only ever skips files proven disjoint).
- For the common monotonic-key bulk load (serial ids, time order) zone-map
  pruning skips every prior file, so per-chunk cost collapses to the batch sort:
  measured **~25× faster** ingest at 200k (narrow PK 29k→793k r/s, wide PK
  11k→272k r/s) *and* flat per-chunk time (no superlinear growth) — bounded
  memory and faster.
- Threshold defaults to 2M existing rows; small tables keep the build-and-cache
  fast path (with cross-batch memoization) unchanged. Overridable via
  `BASIN_PK_STREAMING_MIN_ROWS` (tests force the streaming path at small scale).
- Tests: `crates/basin-engine/tests/pk_streaming.rs` gates correctness
  (clean load passes, cross-file + intra-batch dups rejected, no false positives);
  the shape-matrix harness confirms all shapes green on both paths.

## 2026-06-17 — Fix: SUM/MIN/MAX over NUMERIC/FLOAT route to DataFusion (correct type)

- The integer fast-aggregate path accumulates in i64; a SUM/MIN/MAX over a
  NUMERIC/FLOAT column previously errored "column is not Int64". The fast-SELECT
  gate now checks the aggregate column types against the table schema and bails
  to DataFusion for non-integer columns, which computes the sum with the correct
  output type. Integer aggregates keep the fast path. (Found by the shape-matrix
  harness — all shapes now green.)

## 2026-06-17 — Engine: shape-matrix test harness + COPY timestamptz + integer SUM/MIN/MAX

- **New** `crates/basin-engine/tests/shape_matrix.rs`: a fast, in-process,
  scalable engine bench over a matrix of table shapes (narrow/wide, PK/no-PK,
  JSONB/NUMERIC/TIMESTAMPTZ, composite-PK) × operations (bulk COPY, point
  lookup, range, agg, group-by), timing each and surfacing per-shape perf
  cliffs + correctness gaps before they reach the cloud benchmark. Scales
  toward the 1B-row target via `BASIN_SHAPE_ROWS`.
- **Fix (COPY):** `parse_timestamp_micros` now accepts Postgres' default
  `timestamptz` text form `YYYY-MM-DD HH:MM:SS[.ffffff]±HH[:MM]` (space
  separator, `+00`-style offset). Previously COPY of *any* timestamptz column
  failed to parse, which over pgwire surfaced as the COPY connection closing.
- **Fix (aggregates):** the integer fast-aggregate path widens Int8/16/32 → Int64,
  so `SUM/MIN/MAX` over INT/SMALLINT columns work (`SUM(int)`→bigint, matching
  Postgres) instead of erroring "column is not Int64". (NUMERIC/FLOAT in this
  fast path are still pending — tracked by the shape-matrix harness.)

## 2026-06-17 — Perf: O(n) bulk COPY into a PRIMARY KEY table (was O(n²))

- COPY into a table with a PRIMARY KEY was pathologically slow (<~25 rows/s on
  an object-store backend) because every committed chunk added a data file and
  `enforce_pk_on_insert` then re-scanned **all** prior files' PK columns from
  cold storage for the next chunk — O(files²) cold reads. The PK-set cache only
  helped when the file set was unchanged, so a chunked bulk load missed it every
  chunk. The cache now records the file paths its set was built from and, when
  the file set has only GROWN (the bulk-append case), seeds from the cached set
  and scans only the newly-added files — O(files) total. Correctness is
  unchanged: cached-subset ∪ new-files = full scan; compaction (files replaced)
  falls back to a full rebuild; tombstone-active paths still bypass the cache.

## 2026-06-17 — Fix: bounded-memory COPY (no per-batch Expr blow-up / OOM)

- `copy_ingest::apply_column_defaults_to_batch` materialised `n_rows × n_cols`
  sqlparser `Expr` values for **every** column-list COPY — even when all columns
  were supplied and nothing needed a default. On a large batch this was a ~100×
  memory blow-up that OOM-killed `basin-server` mid-COPY; the client saw the
  connection drop as an unexpected EOF. Added a fast path: when no *unlisted*
  column needs a DEFAULT/SERIAL/identity value, return the batch unchanged and
  skip the allocation entirely (the dominant COPY shape). Correctness is
  identical — that path previously spliced the supplied columns straight back.

## 2026-06-17 — Fix: serial sequences resume past recovered data after restart

- On restart the engine recovers committed rows from its durable WAL + object
  store, but the in-memory catalog's SERIAL/identity sequence cursor reset to
  its start — so `nextval` re-handed-out an id that already existed in the
  recovered data, causing a spurious `duplicate key (id)=(N)` on the next insert
  (surfaced as pgwire-credential provisioning failures after an engine restart).
- WAL replay (`replay_wal_into`) now realigns each serial/identity sequence to
  `MAX(recovered id)` — combining the replayed tail (authoritative for the
  newest, highest ids) with cold column-stats (covers a fully-compacted table) —
  so the sequence resumes exactly where the durable data left off. Recovered
  entirely from Basin's own WAL/storage; no external dependency.

## 2026-06-17 — Fix: `COPY` now fills column DEFAULTs (incl. `SERIAL`) for omitted columns

- A column-list `COPY t (a, b) FROM …` that omitted a `NOT NULL` column backed
  by a `DEFAULT`/`SERIAL`/`BIGSERIAL` wrongly failed with
  `COPY: column "id" cannot be NULL and has no default`. Root cause: the router
  resolved the table schema via a `SELECT * LIMIT 0` prepare, and DataFusion
  strips per-field metadata off projection output — so `BASIN_COLUMN_DEFAULT`
  (the marker behind a column default / serial sequence) was invisible at the
  protocol layer, both for the pre-validation and for the schema handed to the
  ingest path. Postgres fills serial defaults during `COPY`; Basin now does too.
- Engine (`copy_ingest::exec_copy_from_batch`) resolves the **catalog** schema
  (metadata intact) instead of trusting the router-supplied one, so the
  DEFAULT-fill pass evaluates `nextval(...)` for omitted serial columns.
- Router (`select_copy_in_columns`) drops the metadata-blind NOT-NULL
  pre-validation; authoritative NOT-NULL + DEFAULT enforcement is the engine
  ingest path's job (it fills defaults or raises a genuine null violation).

## 2026-06-17 — Fix: control-plane admin token may administer any project

- `/admin/v1/projects/:id/{max-connections,placement,snapshot,restore,fork}`
  gated on the JWT's `project_id` matching the path project exactly, so the
  single deploy-time control-plane token (scoped to `INTERNAL_AUTH_PROJECT_ID`)
  could create projects (unscoped route) but got `403` on every per-project
  admin call. `assert_admin_for_path_project` now treats an `is_admin` token
  scoped to `INTERNAL_AUTH_PROJECT_ID` as a super-admin able to operate on any
  project — security-neutral, since such a token is already all-powerful via the
  unscoped provisioning route.

## 2026-06-17 — Fix: TCP keepalive + nodelay on accepted pgwire connections

- A freshly-accepted pgwire socket now gets `SO_KEEPALIVE` (30s idle / 10s
  interval) and `TCP_NODELAY`. Keepalive stops an L4 proxy in front of the
  engine (e.g. Fly's edge) from silently reaping a SQL connection that goes
  quiet between statements; nodelay removes Nagle latency on the small messages
  a session exchanges. Best-effort — a platform that rejects an option leaves
  the socket usable.

## 2026-06-15 — Fix: `@@` rewriter no longer mangles SQL comments

- The jsonb-path `@@` text-rewriter (`rewrite_binary_op_to_fn` in
  `jsonb_path_udf.rs`) skipped string literals + quoted identifiers but **not**
  `--` / `/* … */` comments, so an `@@` inside a comment (e.g.
  `SELECT 1 -- a @@ b`) was lowered to a function call with operand capture
  crossing the comment boundary → a parse/execute error. It now skips comment
  spans verbatim, mirroring the comment-aware tsvector rewriter (`find_at_at`).
- Un-ignores `fts_at_at::at_at_inside_line_comment_is_ignored` (now a green
  regression guard).

## 2026-06-15 — Docs: correct stale capability claims (binary NUMERIC wire, citext WHERE-fold)

- `CAPABILITIES.md` no longer advertises two already-shipped features as deferred:
  - **Binary `NUMERIC` wire encoding** is implemented (`encode_numeric_binary`,
    wired into the Decimal128 result branch + ParameterDescription) — the old
    "text-only, deferred to v0.2" caveat was stale.
  - **citext `WHERE`/`ORDER BY` auto-folding** is implemented (`CitextAnalyzerRule`,
    registered in `session.rs`) — the old "must use `citext_eq` / explicit cast"
    caveat was stale. (End-to-end harness remains `#[ignore]`-gated.)
- Doc-only sync verified against current code; no behaviour change.

## 2026-06-15 — Hot-tier DELETE/UPDATE fast path admits single-column B-tree indexes

- DELETE/UPDATE **by PK** on a table whose secondary indexes are all GIN or
  **single-column B-tree** now routes to the hot-tier overlay fast path
  (tombstone / override) instead of a cold copy-on-write rewrite + index
  rebuild. Previously any non-GIN index forced the cold path, so the 1M-row
  bulk-DELETE / point-mutation shapes on indexed tables paid the full rewrite.
- Sound because both read consumers are now overlay-aware: `fast_select`'s
  secondary-index allowlist probe DECLINES while a hot overlay is live
  (`table_has_live_overlay`), so a value HIT never prunes to a cold-file set a
  live tombstone/override could escape; and `materialize_overlay_for_table`
  re-registers the replacement file's B-tree locations on drain (mirroring the
  cold CoW `maintain_btree_secondary_on_replace`), so pruning re-engages with a
  complete index once the overlay settles.
- GIST / vector (hnsw) and multi-column / expression B-tree still decline
  (their readers have no overlay guard). Adversarial oracle — an indexed value
  spanning a rewritten row, a surviving same-file row, and an untouched file —
  `tests/integration/tests/btree_overlay_delete.rs`.

## 2026-06-15 — Cloud control-plane: list projects

- **`GET /admin/v1/projects`** enumerates all provisioned project ids (distinct
  `project_id` over the credential store, new `AuthStore::list_projects` +
  `AuthService::list_projects`). Admin-global (`require_admin`; not project-
  scoped). Dashboard table-stakes — there was previously no way to enumerate
  projects over HTTP. Pinned by `admin_list_projects_enumerates`.

## 2026-06-15 — Cloud control-plane: deprovision (DELETE) a project

- **`DELETE /admin/v1/projects/:id`** fully deprovisions a project: deletes its
  object-store bytes (`Storage::delete_project`), drops its catalog rows —
  tables, snapshots, namespace (`Catalog::drop_namespace`), and removes its
  pgwire credentials (new `AuthService::delete_project_credentials`, idempotent
  per-credential). Admin-scoped; returns 204. A serverless product must let you
  tear a project down (cost control, self-serve, GDPR) — the teardown primitives
  existed but were unrouted; this wires them. Pinned by
  `admin_delete_project_deprovisions` (provision → credentials present → delete →
  credentials gone, against the live PG-backed harness).

## 2026-06-15 — Cloud control-plane: list project tables

- **`GET /admin/v1/projects/:id/tables`** returns a project's table names as
  JSON (`catalog.list_tables`) — a control-plane schema browser. Admin-scoped,
  read-only. Pinned by `admin_project_tables_lists_tables`.

## 2026-06-15 — Cloud control-plane: per-project usage endpoint

- **`GET /admin/v1/projects/:id/usage`** returns a project's cumulative
  billing-dimension counters as JSON — `ops_total`, `bytes_read/written_total`,
  Class-A/Class-B object-store ops, `cpu_micros_total`, `errors_total`, and a p99
  latency estimate. Admin-scoped (`require_admin` + project-scoped token).
  Previously these counters (`Engine::project_counters`) were only readable via a
  pgwire session (`SELECT * FROM basin_project_usage`); the control plane now has
  a direct HTTP readout for billing/dashboard use. First of several cloud routes
  exposing already-built engine/catalog capabilities. Pinned by
  `admin_functions_control.rs::admin_project_usage_returns_counters`.

## 2026-06-15 — COUNT(*) stays O(files) after a hot-tier UPDATE/DELETE

- **`SELECT COUNT(*)` no longer falls back to a full table scan when the table
  has a live hot-tier overlay** (a fast-path UPDATE or DELETE earlier in the
  session). The metadata-only count path was gated off by *any* tombstone or
  update entry, so a `COUNT(*)` after a bulk UPDATE+DELETE scanned every file
  (~98 ms at 1M vs Postgres ~34 ms — a published loss). It now corrects the
  catalog row-count by the exact overlay delta: an UPDATE override replaces a
  counted row 1:1 (no change) and each tombstone removes one counted row, so
  `COUNT(*) = Σrow_count − tombstone_count` — computed in O(files), turning the
  loss into a ~30× win. Value aggregates (MIN/MAX/SUM/COUNT(col)) can't be
  derived from counters once a row is tombstoned, so they correctly decline the
  shortcut and run the overlay-aware scan. Pinned by
  `count_star_metadata_shortcut.rs` (delete-adjusts, combined update+delete,
  and a MAX-with-tombstone decline-and-correct test).

## 2026-06-14 — Multi-array UNNEST in INSERT … SELECT (Django bulk_create)

- **`INSERT INTO t (…) SELECT * FROM UNNEST((ARRAY[…])::T1[], (ARRAY[…])::T2[])`
  now inserts the zipped rows.** This parallel multi-array `UNNEST` (Postgres
  zips the arrays element-wise) is what Django's `bulk_create` emits. DataFusion
  accepts the syntax but executes it to **zero rows** for the literal-array form
  (its element-wise `List<Utf8>→List<T>` cast nullifies the arrays), so the
  whole bulk insert silently inserted nothing. A pre-parse rewrite turns the
  literal-array `UNNEST` into a `(VALUES (CAST(v0 AS T1), CAST(w0 AS T2)), …) AS
  __unnest(col1, col2, …)` table, which DataFusion plans natively; shorter
  arrays are NULL-padded (PG semantics). Fires only on 2+ literal-`ARRAY`
  arguments — single-array and `UNNEST(column)` forms are untouched. Combined
  with the INSERT-SELECT RETURNING fix, the full `bulk_create` shape works
  end-to-end. Pinned by `pg_operators` unit tests + `orm_sql_shapes.rs`.

## 2026-06-14 — Enum columns advertise their own type OID (Prisma)

- **A user-enum result column now advertises the enum's own Postgres type OID in
  `RowDescription` instead of TEXT (25).** Prisma / node-pg map columns to their
  declared enum by that OID; reporting TEXT made Prisma fail to map enum fields.
  The stable per-enum OID already existed (`pg_type.oid` / `pg_enum.enumtypid`);
  it is now (a) computed in the Postgres user-object range `[16384, u32::MAX)` so
  it is a valid wire OID that matches the catalog rows a client introspects, and
  (b) reattached to result-column field metadata in both the simple-query
  (`exec_select`) and extended-protocol Describe (`probe_schema`) paths —
  DataFusion strips the metadata through planning, the same problem the
  `json_agg` annotation already solves. The pgwire layer reads the marker and
  advertises the enum OID (typlen 4). Single-table SELECTs; joins fall back to
  TEXT (unchanged). Pinned by `field_description_surfaces_enum_oid_from_metadata`
  (router) and `enum_column_select_carries_oid_metadata` (engine).

## 2026-06-14 — ORM SQL-shape fixes (Drizzle, SQLAlchemy, Django, gorm)

- **Array `&&` overlap with a parenthesized/cast operand** now rewrites to
  `arrays_overlap(...)` instead of being mangled by the range-operator rewriter.
  Django's ArrayField `__overlap` emits `"t"."tags" && (ARRAY['a','b'])::varchar(50)[]`;
  the array-operator rewrite now (a) consumes the trailing `::type[]` cast on a
  parenthesized operand, (b) recognizes an embedded `ARRAY[` as an array, and
  (c) extracts a double-quoted compound identifier (`"t"."col"`) as the LHS.

- **`(expr AT TIME ZONE 'tz')::date`** (Django `__date` lookups) now plans. The
  backward LHS scan in the `AT TIME ZONE` rewriter stopped at whitespace/comma
  but not `(`, so a parenthesized LHS like `("t"."col" AT TIME ZONE 'tz')`
  swallowed the leading paren and produced `at_time_zone(("t"."col", 'tz'))` — a
  single struct argument that failed coercion. The scan now also stops at `(`.

Five fixes for SQL shapes real ORMs emit, each validated by a toolchain-free
regression test (`tests/integration/tests/orm_sql_shapes.rs`) plus, for the
wire-protocol fix, a router unit test:

- **`INSERT INTO t (id, …) VALUES (default, …)` on a SERIAL/identity column now
  draws the next sequence value** instead of inserting NULL (which violated the
  NOT NULL identity column). The explicit `DEFAULT` keyword path checked the
  column's DEFAULT-expression text but not its backing identity sequence.
  (Drizzle emits this shape; unblocks its insert/RETURNING cascade.)
- **`INSERT INTO t (…) SELECT …` into a narrow integer identity PK** (e.g.
  SQLAlchemy `Mapped[int]` → INTEGER) now casts the synthesized i64 identity
  values to the column's declared type instead of failing with an Int64-vs-Int32
  schema mismatch.
- **`INSERT … SELECT … RETURNING`** now emits the projected RETURNING rows. The
  INSERT-SELECT path previously returned only an `INSERT 0 N` tag, so Django's
  `bulk_create` (`INSERT … SELECT * FROM UNNEST(…) RETURNING id`) saw no rows
  ("no results to fetch"). The written batch — which already carries synthesized
  identity values — is now projected through the same RETURNING machinery as the
  VALUES path.
- **Django `F()` UPDATE expressions** — `SET col = "t"."col" + 1` — now resolve:
  the table qualifier on the SET right-hand side is stripped (it is evaluated
  over a temp table), fixing "No field named t.col". The strip recurses through
  `CASE` expressions too, so Django `bulk_update` (`SET col = CASE WHEN
  "t"."id" = … THEN … ELSE "t"."col" END`) resolves as well.
- **Schema-qualified FK references** (`REFERENCES "public"."u" (id)`) are now
  accepted in both `CREATE TABLE` and `ALTER TABLE … ADD CONSTRAINT` by taking
  the bare referenced-table name (Basin FKs are single-project).
- **Extended-protocol `RowDescription` now advertises the actual result format
  codes** (the Bind result-format codes) instead of a hardcoded text `0`. A
  client that decodes by the advertised format (pgx/gorm reads `fd.Format`)
  previously misread binary `int8` bytes as text and failed `strconv.ParseInt`;
  any binary-format result column over the extended protocol was affected.

## 2026-06-14 — DEFERRABLE INITIALLY DEFERRED foreign keys (ORM migrations)

- **`FOREIGN KEY … DEFERRABLE INITIALLY DEFERRED` is now accepted and treated as
  deferred.** Postgres validates such FKs at `COMMIT`, not at the statement, so a
  transaction may legally insert a child row before its parent — exactly what
  Django/Rails migrations emit (create both tables, then insert seed rows in
  dependency-free order inside one transaction). The prior `ALTER ADD FK`
  enforcement change checked every FK per-row at insert, which rejected this
  legal ordering and rolled the migration back. The FK's `INITIALLY DEFERRED`
  characteristic is now parsed (both `CREATE TABLE` and `ALTER ADD CONSTRAINT`)
  and recorded on the catalog `ForeignKeyDef` (`initially_deferred`,
  `#[serde(default)]` so existing catalog payloads are unaffected); deferred FKs
  skip per-row enforcement while **immediate FKs are still enforced**. Commit-time
  validation of deferred FKs remains future work and is documented as such.

## 2026-06-14 — ALTER TABLE ADD CONSTRAINT … FOREIGN KEY (ORM migrations)

- **`ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY …` is now accepted and enforced.**
  ORMs create tables first and wire up foreign keys in a follow-up `ALTER`
  (Django `AddField`, Rails), usually inside the same migration transaction — so
  rejecting the FK addition rolled back every column the migration added in that
  transaction too (the array/enum/`version` columns), failing a large fraction of
  the live-ORM suite for an unrelated reason. The FK is now registered in catalog
  metadata exactly as `CREATE TABLE` does and enforced on subsequent writes by
  the existing FK machinery (`enforce_fk_on_insert`, cascade/no-action on parent
  delete). Backfill validation of existing child rows is deferred (the migration
  case adds the FK to a freshly-created, empty table). `ADD PRIMARY KEY` after
  creation remains deferred (its NOT NULL backfill is out of scope). Source:
  `crates/basin-engine/src/alter.rs`; covered by
  `type_ddl::alter_add_foreign_key_registers_and_enforces`.

## 2026-06-14 — enum values stamped as a cast INSERT correctly (`'USER'::"Role"`)

- **Cast-wrapped string literals coerce for enum / text columns.** ORMs (Django
  et al.) stamp enum INSERT values as an explicit cast — `'USER'::"Role"`
  (`CAST('USER' AS "Role")`, sometimes nested through `::TEXT`) — which was
  rejected with "expected string literal, got CAST(…)". The string coercion now
  peels CAST wrappers around a string literal (user enums are stored as Utf8, so
  the cast target is irrelevant); enum **label validation still fires** through
  the cast, and `'x'::text` / `'x'::varchar` casts also resolve. Source:
  `crates/basin-engine/src/dml.rs` (`coerce_string_ref`); covered by
  `type_ddl::enum_insert_with_cast_label`.

## 2026-06-14 — explicit `DEFAULT` keyword in INSERT values

- **`INSERT INTO t (…, c) VALUES (…, DEFAULT)` applies the column default.** The
  bare `DEFAULT` keyword in a value position was rejected by the value coercers
  ("expected string literal, got DEFAULT"); ORMs emit it for not-null-with-default
  columns they don't set. `apply_column_defaults` now replaces a `DEFAULT` marker
  with the column's DEFAULT expression (or NULL when the column has none), for
  both the column-list and no-column-list INSERT forms. Source:
  `crates/basin-engine/src/executor.rs`; covered by
  `coverage_txn_schema::insert_explicit_default_keyword_applies_column_default`.

## 2026-06-14 — HNSW vector search recall@10 0.60 → 1.00 (ef_search default)

- **Vector ANN search now sets a usable query-time `ef_search`.** The HNSW index
  left `instant-distance`'s default `ef_search` (100), which measured
  **recall@10 ≈ 0.60** on the 128-dim / 100k benchmark — a silent quality hole
  (queries missed ~40% of true nearest neighbours). Query latency here is
  row-fetch-dominated, so a larger candidate list is nearly free: raising the
  default to **400** lifts **recall@10 to 1.00** with p50 only 38 → 42 ms
  (0.60→0.80 at 200, 1.00 at 400). Per-index override via
  `HnswIndexBuilder::with_ef_search`. Source: `crates/basin-vector/src/index.rs`;
  measured by `ext_bench_vector`.

## 2026-06-14 — array columns render in the PostgreSQL `{…}` text form on the wire

- **Reading an array column returns PG array text `{a,b}` / `{}`.** A `List` /
  `LargeList` / `FixedSizeList` result column previously fell through to a Debug
  rendering, so array-aware clients (psycopg, lib-pq, node-postgres) failed to
  parse it ("array does not start with '{'"). Array cells now render as the
  PostgreSQL curly-brace text form: NULL elements as the `NULL` token, text
  elements double-quoted + escaped when ambiguous (empty / looks-like-`NULL` /
  contains a delimiter/brace/quote/backslash/whitespace), numerics bare, booleans
  as `t`/`f`. Completes the array round-trip with the INSERT-side parsing.
  Source: `crates/basin-router/src/types.rs` (`render_cell` / `format_pg_array_text`);
  covered by `types::tests::pg_array_text_formatting`.

## 2026-06-14 — INSERT array values in the PostgreSQL curly-brace text form

- **`'{a,b}'::T[]` / `'{}'::T[]` array literals INSERT into array columns.**
  Drivers (Django ArrayField, libpq array output) emit the PostgreSQL
  curly-brace text form for array values, not just the `ARRAY[...]` constructor.
  The INSERT array-coercion path now parses that form (`parse_pg_array_literal`
  + `extract_array`): unquoted / double-quoted (with `\"`,`\\` escapes) elements,
  the unquoted `NULL` token as a null element, embedded commas inside quotes,
  empty `{}`, and multi-byte UTF-8. Each element reuses the per-type element
  coercion, so `'{1,2}'::int[]` and `'{a,b}'::text[]` both work. Also fixes the
  `'{}'` empty-array rewrite (it was mis-lifted to `make_array('')`, a one-empty-
  string array). Source: `crates/basin-engine/src/{dml,pg_operators}.rs`; covered
  by `array_fns::insert_pg_curly_array_literals` + `dml::tests::pg_array_literal_parses_all_forms`.

## 2026-06-14 — ALTER TABLE ADD COLUMN … DEFAULT v NOT NULL (ORM migrations)

- **`ALTER TABLE ADD COLUMN … DEFAULT v NOT NULL` is now allowed.** The canonical
  Django/Rails migration shape (`ADD COLUMN version INTEGER DEFAULT 0 NOT NULL`)
  was rejected outright ("NOT NULL not supported"); since drivers wrap a
  migration in one transaction, that rejection rolled back every other column
  added in the same migration (e.g. an adjacent `ADD COLUMN tags VARCHAR[]`),
  cascading to a large fraction of the live-ORM suite. NOT NULL is now admitted
  WHEN a DEFAULT is present (the default supplies the value); a bare
  `ADD COLUMN x T NOT NULL` with no default stays rejected. Source:
  `crates/basin-engine/src/alter.rs`.
- **String-literal defaults coerce to numeric column types.** Drivers stamp
  integer/float defaults as quoted strings (`DEFAULT '0'`); the INSERT
  default-application path now coerces a `SingleQuotedString` to the target
  INTEGER / DOUBLE column (PostgreSQL's implicit cast — a non-numeric string
  still errors). This also makes `INSERT … VALUES ('5')` into a numeric column
  work, matching PG. Source: `crates/basin-engine/src/dml.rs` (`coerce_i64` /
  `coerce_f64`). Covered by `schema_alter_add_column_not_null_with_default_allowed`.

## 2026-06-14 — system functions in FROM position (`SELECT * FROM current_schema()`)

- **`SELECT * FROM current_schema()` / `current_database()` / `version()` resolve.**
  Drivers (typeorm's `PostgresQueryRunner` connect) call these nullary system
  functions in FROM position, treating them as set-returning table functions.
  Basin's FROM-clause dispatch only resolves registered UDTFs and rejected them
  with `UndefinedFunction`, aborting the connection. The bare whole-statement
  probe is now rewritten to the equivalent scalar projection
  `SELECT <fn>() AS <fn>` (same single-row result PostgreSQL returns).
  Conservative / correct-or-noop — only the exact bare form is rewritten.
  Source: `crates/basin-engine/src/pg_operators.rs` (`rewrite_system_fn_from_table`);
  covered by `orm_driver_connect.rs`.

## 2026-06-14 — pgwire reports a PG-compatible server_version (ORM connectivity fix)

- **Startup `server_version` is now a real PostgreSQL version ("15.0").**
  pgwire's `DefaultServerParameterProvider` defaults `server_version` to its OWN
  crate version (`env!("CARGO_PKG_VERSION")`, e.g. "0.28.0") in the startup
  ParameterStatus. Version-gating clients read that as "PostgreSQL 0.28" and
  refuse to connect outright — Django: *"PostgreSQL 14 or later is required
  (found 0.2800)"* — which cascades to every later driver in a connection pool.
  Basin now overrides the startup `server_version` to "15.0", matching the value
  `pg_settings.server_version` already reports. Live-ORM Django connect goes
  REGRESSION → PASS. Regression test: `protocol::tests::startup_server_version_is_pg_compatible`
  (asserts major ≥ 14 and that we do not ship pgwire's crate-version default).
  Source: `crates/basin-router/src/protocol.rs`.
- **`pg_catalog.pg_type` now exposes `typarray` / `typelem` / `typdelim`.**
  psycopg (and SQLAlchemy on it) loads its type cache at connect time with a
  query selecting those columns; their absence aborted the connection ("column
  typarray does not exist") and cascaded to the whole SQLAlchemy suite. Basin
  does not model distinct array-type OIDs, so `typarray`/`typelem` are 0 and
  `typdelim` is the standard comma. Source: `crates/basin-catalog/src/info_schema.rs`.
- **`version()` / `pg_catalog.version()` implemented.** psycopg / SQLAlchemy call
  it at connect time to read the server banner; its absence aborted the
  connection ("function pg_catalog.version does not exist"). Returns a
  PostgreSQL-15-style banner consistent with the advertised `server_version`.
  Registered under both the bare and `pg_catalog.`-qualified names. Source:
  `crates/basin-engine/src/pg_scalar_aliases.rs`.
- **`SHOW <var>` always returns a row (was empty for unknown vars).** PostgreSQL's
  `SHOW` returns exactly one row; Basin returned an empty result (no row) for any
  variable it didn't special-case, so drivers that FETCH the value — psycopg /
  SQLAlchemy `get_isolation_level` via `SHOW transaction isolation level` — raised
  "no results to fetch" and aborted the connection. `SHOW transaction_isolation`
  now returns "read committed", and every other unknown `SHOW <var>` returns a
  single empty-valued row keyed by the variable name. The operative interceptor
  is `noop_accept` (Basin classifies `SHOW` via the pg_query parser into
  `StmtKind::VariableShow` and short-circuits there, before the sqlparser
  executor — which in any case cannot parse multi-word forms like `SHOW
  TRANSACTION ISOLATION LEVEL`): it now emits the row instead of an empty
  result. Regression test: `orm_driver_connect.rs` exercises the psycopg/
  SQLAlchemy connect statement sequence end-to-end. Source:
  `crates/basin-engine/src/{noop_accept,executor}.rs`.
- **Nullary function-call result columns are named PostgreSQL-style.**
  `SELECT version()` / `now()` / `current_schema()` (and schema-qualified
  `pg_catalog.version()`) now name their output column after the function
  (`version`, `now`, `current_schema`) instead of DataFusion's `version()` with
  parens. Drivers that read connect-probe results BY COLUMN NAME broke otherwise
  — node-postgres / typeorm's `getVersion()` does
  `result[0].version.replace(...)` and crashed on `undefined`. Scoped to nullary
  `()` calls, so `count(*)` / `max(x)` are unchanged. Source:
  `crates/basin-engine/src/executor.rs`.

## 2026-06-14 — Cold DELETE write-amp: GIN-only tombstone fast path + FK-cascade re-read skip

- **GIN-only tables now take the DELETE tombstone fast path.** A DELETE on a
  table whose only secondary index is `USING gin` (jsonb containment / tsvector
  FTS) previously fell to the cold copy-on-write rewrite — a full-file rewrite
  plus GIN posting rebuild — because the fast-path gate declined any table with
  a secondary index. It now admits GIN-only tables (mirroring the UPDATE twin):
  the read-path overlay guards are tombstone-aware (`table_has_live_overlay`
  counts `tombstone_count`, so the GIN posting-probe `Empty` short-circuit and
  `apply_gin_pruning_for_query` fall back to the overlay-aware TombstoneFilter
  scan while a tombstone is live, and `materialize_overlay_for_table` purges +
  rebuilds the postings on drain). The point `pk IN (…)` DELETE and the
  `DELETE … USING` join DELETE now share one `delete_fastpath_table_eligible`
  gate, and the USING path encodes the join's matched PKs straight to tombstones
  — skipping the `pk IN (v1..vN)` predicate string build + full DELETE re-parse
  that made a high-cardinality non-indexed join-delete quadratic. Oracle:
  `gin_overlay_delete.rs` (routing + adversarial `@>` containment-read +
  post-drain completeness, point and USING shapes). Source: `dml_mutate.rs`.
- **Cold DELETE skips the FK-cascade re-read when nothing references the
  target.** The cold copy-on-write DELETE rebuilt the deleted-PK set by
  re-reading every rewritten and dropped data file — a second full pass over the
  rewritten rows — unconditionally for any table with a primary key, purely to
  feed parent-side FK cascade / NO ACTION enforcement. For the common table that
  nothing references by foreign key that pass is pure waste. It is now gated on
  a cheap `fks_referencing` catalog probe (no file I/O): no inbound FK → skip the
  re-read and the cascade check. Regression:
  `cold_point_delete_btree_is_pruned_and_correct` (btree-indexed table, scattered
  point-IN delete, exact-row + pruning asserts).
- Known limitation (unchanged): a DELETE of scattered rows from a large table
  carrying a **non-GIN** (btree / GIST / vector) secondary index still takes the
  cold rewrite of the files holding the matched PKs — those index read paths
  have no overlay-emptiness guard, so a tombstone could serve a stale row.
  Matching Postgres's in-place delete there needs overlay-aware btree/GIST
  maintenance (tracked).

## 2026-06-14 — Multi-node raft hardening (mTLS transport, chaos-under-load drills, snapshot catch-up, per-region group routing, deploy harness)

- **Mutual TLS for the raft transport (`BASIN_RAFT_TLS_CERT/KEY/CA`).** The
  tonic transport now runs over rustls with mutual authentication when the
  three TLS vars are set: each node presents a cluster-CA-signed leaf and
  requires its peers to do the same — confidentiality plus peer auth, so a node
  that cannot prove cluster membership is rejected at the handshake. All three
  vars are required together (a partial config is a startup error; no silent
  plaintext fallback). With TLS on, bare `host:port` peers are dialed over
  `https://` automatically. `BASIN_RAFT_TLS_DOMAIN` (default `basin-raft`) is
  the verification SAN. Default remains plaintext for a private cluster network.
  This removes the documented "plaintext only" caveat. Verified end-to-end by
  `crates/basin-wal/tests/raft_net_tls.rs` (a 3-node TLS cluster elects +
  replicates). Source: `crates/basin-wal/src/raft_net/tls.rs`.
- **Chaos-under-load failover drills.** New drills in
  `raft_failover_drills.rs`: kill the leader WHILE a sustained write stream is
  in flight (the stream resumes on the new leader; every acked write survives —
  the strongest zero-loss proof); slow/lagging follower does not stall the 2/3
  quorum commit; partition + heal under continuous load. The `AckedSet`
  zero-acked-write-loss invariant is asserted in every drill.
- **Snapshot-streaming follower catch-up.** A drill proves a follower that
  misses the log-purge window (`record_flush_watermark` snapshots + purges)
  catches up via `install_snapshot` and ends with the identical committed write
  set. The WAL snapshot is self-contained (full applied state machine + manifest
  pointer); the `catalog_snapshot_id` it carries is the seam the engine layer
  uses to rebuild flushed table state from S3 (documented boundary).
- **Per-region raft group routing.** One independent raft group per region
  (no cross-region quorum). `basin_common::raft_group_for(home_region)` makes
  the group-selection decision explicit and testable: a project's `home_region`
  selects which region's group owns its writes; the engine region gate routes a
  non-home write to the forwarder or rejects it (`WrongRegion`). Cross-region
  replication of the underlying data is by S3 CRR at the bucket layer.
- **Deployment harness + runbook.** `scripts/raft-cluster-smoke.sh` brings up a
  real 3-node cluster (three `basin-server` processes, distinct ports/dirs,
  `BASIN_WAL_MODE=raft`), waits for readiness, and runs a smoke (write to
  leader, read from follower, kill leader, verify the pre-failover write
  survived). `docs/runbooks/failover.md` gains the mTLS setup, the new drills,
  and the harness pointer; `docs/deployment.md` documents the mTLS env matrix.

## 2026-06-14 — Program-wide feature summary (extensions, SQL surface, multi-node, CDC, SDKs, CLI, storage, scale)

This entry summarises the capabilities that landed across the current program
and are now on main. Per-commit detail lives in the individual dated entries
that follow. Grouped by area.

### Extensions

- **`pg_trgm` — full SQL surface + GIN trigram index.** `similarity`, `word_similarity`, `show_trgm` registered as DataFusion UDFs. Operators: `%` (similarity threshold), `<%` (word-similarity threshold), `<->` (distance). GUC overrides via `SET pg_trgm.similarity_threshold` / `SET pg_trgm.word_similarity_threshold`. GIN trigram index (`CREATE INDEX … USING gin (col gin_trgm_ops)`): in-RAM posting list seeded at INSERT + CREATE INDEX backfill; `%` probes prune to candidate files using the conservative shared-trigram bound, then re-evaluate `similarity()` on survivors. Differential correctness pinned against unindexed twin: `trgm_gin_index.rs`. SQL conformance: `trgm_sql_conformance.rs`.

- **Range types — full operator set + multirange + GIST EXCLUDE constraints.** All six range types (`int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange`) as JSON-encoded structs with `BASIN_TYPE` sidecar. Full operator set: `@>`, `<@`, `&&`, `<<`, `>>`, `&<`, `&>`, `-|-`, `+`, `*`, `-`, `range_merge`. Accessors: `lower`, `upper`, `isempty`, `lower_inc`, `upper_inc`, `lower_inf`, `upper_inf`. Multirange: `int4multirange` construction and `@>` containment. Empty-range semantics (canonical half-open normalization). `EXCLUDE USING gist (col WITH &&)` on range columns: parsed by `extract_exclude_using_gist`, stored as sentinel CHECK, enforced at INSERT via the exclusion enforcer. Conformance: `range_conformance.rs`.

- **Full-text search — complete surface shipped.** `TSVECTOR`/`TSQUERY` column types. `to_tsvector` with Snowball English stemming + stop-words. `to_tsquery` (Snowball-stemmed, PG parity), `plainto_tsquery`, `phraseto_tsquery`, `websearch_to_tsquery`. `@@` match operator. `ts_rank` (TF-weighted). `ts_rank_cd` (simplified cover-density). `ts_headline` fragments (`MaxFragments`, `<b>…</b>` wrapping). `setweight` (A–D class annotation). `strip()`, `tsvector_length`. GIN-on-tsvector: in-RAM lexeme posting list + CREATE INDEX backfill (settles live overlay first); `@@` probes structurally (`&`/`<->` intersect, `|` unions, `!`/unknown decline to full scan); provably-empty short-circuit; per-file completeness guard. `to_tsquery` stems through the same pipeline as `to_tsvector` so probe lexemes and `@@` re-evaluation agree. Full conformance + adversarial pruning harness: `fts_conformance.rs`.

- **TimescaleDB — full surface.** `create_hypertable`, `time_bucket` (all forms: epoch-aligned + `origin`-aligned), `first(value, ts)` / `last(value, ts)` aggregates, `drop_chunks`, `add_retention_policy`, `run_retention_policy`, `time_bucket_gapfill` + `locf` carry-forward, continuous aggregates (`CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous, …)`, `refresh_continuous_aggregate`, `add_continuous_aggregate_policy`, incremental refresh), `timescaledb_information.chunks` + `timescaledb_information.hypertables` catalog views, `compress_chunk` DDL accepted. Conformance: `timescale_conformance.rs` + `timescale_completions.rs` + `timescale_caggs.rs` + `timescale_gapfill.rs`.

- **pgvector — complete surface.** HNSW (`CREATE INDEX USING hnsw WITH (m, ef_construction)`; opclass-matched routing). Native IVFFlat coarse quantiser (`CREATE INDEX USING ivfflat WITH (lists = N)`; k-means cells; `ivfflat.probes`). `vector_avg(v)` element-wise mean aggregate (NULL-skipping, GROUP BY, dimension-mismatch error). `halfvec(N)` f32-backed round-trip. `sparsevec` typed `0A000`. Conformance: `vector_conformance.rs` (17 groups).

- **PostGIS — general 2-D geometry SQL surface.** `LINESTRING`/`POLYGON`/`MULTI*`/`GEOMETRYCOLLECTION` as WKB/EWKB/WKT/GeoJSON values. Full codec set: `ST_GeomFromText`, `ST_GeomFromGeoJSON`, `ST_GeomFromWKB`, `ST_AsText`, `ST_AsGeoJSON`, `ST_AsEWKB`. Accessors + measures: `ST_GeometryType`, `ST_NumPoints`, `ST_NumGeometries`, `ST_GeometryN`, `ST_PointN`, `ST_StartPoint`/`ST_EndPoint`/`ST_ExteriorRing`, `ST_Length`/`ST_Area`/`ST_Perimeter` (planar + WGS84 geography), `ST_Centroid`, `ST_Envelope`, `ST_Buffer`. Exact topology predicates: `ST_Intersects`/`ST_Contains`/`ST_Within`/`ST_Disjoint`/`ST_Crosses`/`ST_Touches`/`ST_Overlaps` via `geo` crate. Only `POINT` is a native column DDL type; only `POINT` has an R-tree index. Conformance: `postgis_conformance.rs`.

- **JSONB GIN index.** `CREATE INDEX … USING gin (col)` / `gin (col jsonb_path_ops)`: in-RAM posting list, dedup-(term, file) storage for 1M-row backfill survival. `@>`, `<@` posting-list file pruning. Per-file completeness guard. `BASIN_GIN_POSTING_BUDGET` shared with FTS GIN. Harness: `jsonb_index_harness.rs` + `ext_bench_jsonb_gin.rs`.

- **Advisory locks + deadlock detection.** `pg_advisory_lock`/`pg_advisory_unlock`/`pg_advisory_unlock_all`/`pg_try_advisory_lock` (session-scoped); xact-scoped variants auto-released at COMMIT/ROLLBACK. Reentrancy (N locks need N unlocks). Project isolation. Wait-for-graph deadlock detector — ABBA cycles broken promptly with SQLSTATE 40P01. Lock-wait timeout → SQLSTATE 55P03. Disconnect releases all held session locks. ORM tooling compatibility (Prisma, sqlx, Diesel, golang-migrate). Pinned: `advisory_locks.rs`.

### SQL surface

- **`MERGE INTO` (Postgres 15+).** Each WHEN action compiled to INSERT/UPDATE/DELETE through the normal pipeline. Actions: WHEN MATCHED THEN UPDATE/DELETE/DO NOTHING, WHEN NOT MATCHED THEN INSERT. First-match-wins. Duplicate-target SQLSTATE 21000. Atomicity (failing action rolls back the whole MERGE). RLS enforced on every action. VALUES and SELECT-subquery sources. Command tag `MERGE N`. Pinned: `merge_into.rs`.

- **Composite `ON CONFLICT` targets.** `ON CONFLICT (a, b)` against composite PK or multi-column UNIQUE. DO UPDATE, DO NOTHING, EXCLUDED refs. ON CONFLICT ON CONSTRAINT by name (including implicit `<table>_pkey`). Mismatch → SQLSTATE 42P10. NULL semantics per PG (NULLs in composite key do not conflict). Mixed conflict/no-conflict rows. Pinned: `composite_on_conflict.rs`.

- **`DECLARE`/`FETCH`/`MOVE`/`CLOSE` cursors.** Session-scoped, materialised at DECLARE. FETCH direction variants (NEXT, PRIOR, FIRST, LAST, ALL, ABSOLUTE, RELATIVE, N). MOVE (position-only). CLOSE c / CLOSE ALL. Past-end → 0 rows. Unknown cursor → SQLSTATE 34000. WITH HOLD → SQLSTATE 0A000. Django/psycopg2 server-side cursor pattern. `BASIN_CURSOR_MAX_ROWS` cap. Pinned: `cursor_lifecycle.rs` + `cursor_extended.rs`.

- **`EXCLUDE USING gist` constraint on range columns.** Parsed (`extract_exclude_using_gist`), stripped from DDL, encoded as sentinel CHECK, enforced at INSERT time by the exclusion enforcer. Meeting-room / resource-booking overlap prevention. Part of range-types work (5.24.F).

- **Schema namespaces (phase A).** `CREATE SCHEMA` / `DROP SCHEMA` / `CREATE SCHEMA IF NOT EXISTS`. Qualified `schema.table` names in DML/DDL. Schema-aware in-memory + Postgres catalog. `basin_schemas` table. Cross-schema queries with differential coverage. `search_path` semantics and wider schema-scoped DDL in phases B–E. Pinned: `schema_namespaces.rs` + `schema_ddl.rs` + `schema_e2e.rs`.

### Multi-node

- **`BASIN_LEASE_MODE=required` write-fence.** Catalog CAS + shard heartbeat writer-lease. Happy path: acquire → heartbeat renew (not a regrant) → writes flow. Fencing: lost lease → in-flight and fresh handles refuse writes (`BasinError::LeaseNotHeld`); reads continue; replica recovers by re-acquiring at a higher epoch. Writes refused under a foreign lease (reads unaffected). Off mode: pre-existing CommitConflict behaviour unchanged. Pinned: `lease_mode_required.rs` + `lease_failure_paths.rs` + `lease_handoff.rs`.

- **`BASIN_WAL_MODE=raft` — quorum-replicated WAL.** openraft 0.9. Disk-backed log (`raft.log`) + vote (`raft.meta`) + manifest-anchored snapshot (`raft.snapshot`). WAL durability boundary = quorum ack (one consensus round amortised over the group-commit batch). `durable_lsn` advances on raft commit index. Fail-closed backpressure: cannot-reach-quorum → SQLSTATE 40001 (`RaftNoQuorum`). `raft-net` feature: cross-process gRPC transport via tonic, lazy-dial HTTP/2 channel per peer, backoff on transport failure. `BASIN_RAFT_BIND` / `BASIN_RAFT_PEERS` / `BASIN_RAFT_BOOTSTRAP` / `BASIN_NODE_ID`. Raft leadership supersedes `BASIN_LEASE_MODE`. Cluster status logged at startup. V1 plaintext; mTLS documented follow-up.

- **Multi-region routing seams.** `home_region` field on `ProjectMetadata`. Writes to a non-home replica → typed retryable `WrongRegion` (SQLSTATE 40001). `basin.read_tier = 'lagging'` allows reads from non-home replica. `WriteForwarder` seam for proxy-side write routing. Pinned: `multiregion_homing.rs`.

- **Per-project connection ceiling.** Hard ceiling enforced at pgwire startup (SQLSTATE 53300). `CatalogConnectionLimitProvider` reads from catalog; fail-closed (no stored ceiling → 25, Free tier). Admin API `POST /admin/v1/projects/:id/max-connections`. RAII guard; lowering never kills existing connections.

### CDC

- **Durable CDC ring (Phase 1).** `basin-cdc::CdcRingWriter` as a post-commit `ChangeEventSink`. Every committed mutation (including HTAP hot-tier UPDATE/DELETE fast paths and in-tx mutations drained at COMMIT) appended to a per-project append-only ring on object storage. Committed-only, commit-ordered (`seq` monotonic). Size/time-window batching. Retention window (`BASIN_CDC_RETENTION_HOURS`, default 24h, max 168h). `cursor_expired` frame on stale consumer.

- **SSE stream (Phase 1).** `GET /cdc/v1/sse/:project` with resumable `seq` cursor. Bearer-JWT auth.

- **Webhook fanout (Phase 2).** `basin-webhooks`: disk-backed retry queue keyed by `(subscription_id, project, table, seq)` SHA-256 idempotency key. Exponential backoff (1s → 5min cap), `max_retries`, dead-letter file. Auto-pause after `auto_pause_after` (default 24h); resume explicit. Per-project rate limiting via `basin-net` token bucket.

- **Kafka/Redpanda push sink (Phase 3).** `basin-cdc::kafka` delivery worker draining the CDC ring into a Kafka/Redpanda topic.

### SDKs

- **10 client SDKs in `sdk/`.** `basin-js` (TypeScript/Node/Bun/Deno/CF Workers; realtime SSE + WebSocket; Arrow IPC), `basin-python` (async + sync; realtime via websockets extra; Arrow IPC via pyarrow extra), `basin-go` (Arrow IPC via arrow-go/v18), `basin-java`, `basin-rust`, `basin-ruby`, `basin-dotnet`, `basin-php`, `basin-dart`, `basin-swift`. All engine-direct (pgwire + REST); no cloud intermediary required.

### CLI

- **`basin-cli` (`bas-in/basin-cli`, Go).** `basin login`, `basin projects list`, `basin sql run`, `basin dev` local-stack launcher. Release artefacts Sigstore-signed. Talks to basin-cloud `/v1/*` API.

### Storage

- **Vortex is the default** (since 2026-05-18, commits ≤ 3787db0). Zero regression vs Parquet baseline. Catalog-persisted file stats (`column_stats` on every committed `DataFileRef`). Whole-file stats pruning (`Storage::read_paths` skips LIST + footer fetch when catalog stats prove the predicate prunes the file). Per-file bloom filters in `DataFileRef.bloom_filters`. `FileMetadataCache` + `VortexFooterCache` eliminate per-iteration footer re-parse.

- **S3 cold-path levers.** `BASIN_STORAGE_BACKEND=s3|tigris`. NVMe disk cache (LRU; cold S3 fetches → warm SSD reads). Parquet page cache (LRU RecordBatches). HTTP/2 toggle for S3 client.

### Scale benchmarks

- **Fleet-scale isolation ladder** (`multi_project_fleet_scale.rs`): per-project isolation gates at 50 / 500 / 5000 projects with proportional noisy load. Victim p99 ratio pinned flat as fleet grows.
- **1B-row write soak** (hypertable soak gate in `hypertable_harness.rs`, gated for provisioned hardware).
- **Noisy-tenant fairness** (`noisy_neighbor_harness.rs`): per-project GIN posting budget partition + PkRowCache waterline + reconciler round-robin.
- **Extension benchmark suites**: `ext_bench_vector.rs`, `ext_bench_postgis.rs`, `ext_bench_trgm.rs`, `ext_bench_timescale.rs`, `ext_bench_fts.rs`, `ext_bench_ranges.rs` (+ `_ext` variants for size-ladder sidecars) generate `benchmark/data/*.json` for the dashboard.

### Honest gaps recorded

The following items from the "what landed this program" brief are **partial or absent** on main at time of writing, and are noted honestly:

- **`gen types` multi-language codegen** lives in the separate `basin-cli` repo (not this checkout) and now covers all 10 SDK languages — noted here for cross-reference. (TimescaleDB `interpolate()` shipped — linear fill between non-NULL buckets, see the Extensions section above.)
- **CDC pgoutput / logical replication protocol**: `cdc_pgoutput_harness.rs` is a test-first gate; all slices `#[ignore]`-gated. Debezium/Fivetran e2e requires external connectors and is integration-env-gated.
- **Hypertable auto-partition (5.29.B–F)**: `hypertable_harness.rs` slices are `#[ignore]`-gated; `create_hypertable` DDL is accepted and the basic conformance group (conformance.rs) is live, but the full auto-partition / chunk-exclusion / ORM-compat slices are pending.
- **`sparsevec(N)` / vector `bit`**: typed `0A000` by design (ADR 0003). Not planned for v0.1.
- **3-D geometry / `ST_Buffer`/`ST_Union` constructive ops**: not on roadmap for v0.1.
- **`search_path` semantics** (schema namespace phases B–E): phase A (CREATE SCHEMA + cross-schema qualified names) is live; unqualified resolution across schemas is deferred.
- **`array column DDL types`** (`TEXT[]`, `INT[]` in CREATE TABLE): not yet wired through the Arrow schema bridge; `ARRAY[…]` expressions and array functions work on existing columns.
- **gen-types (SDK codegen)**: not found in `sdk/` at time of writing; omitted from SDK listing.
## 2026-06-14 — Point-op latency: bind-direct UPDATE, FK/reactor flag cache, prewarm flag

### Changed

- **Bind-direct UPDATE.** A prepared `UPDATE t SET col = $N WHERE pk = $M`
  whose template triggers no string-rewrite pipeline now binds the AST-fast
  route: at Execute the cached parsed statement is cloned, its SET-value /
  WHERE placeholders are substituted with the decoded params, and the
  resulting `Statement::Update` is dispatched straight through
  `dispatch_parsed_statement` — eliminating the per-Execute whole-statement
  re-parse the text route paid. The statement still flows through `exec_update`
  verbatim (every gate, the hot-tier overlay fast path, and the
  change-event/sink capture inside `hot_tier_update_by_pk`), so the bind path
  is byte-for-byte identical to the normal path, including error parity.
  Mirrors the existing AST-fast INSERT / point-SELECT routes.
- **Single-row UPDATE/DELETE FK + reactor flag cache.** The hot-tier
  fast-path gate consulted `fks_referencing` + `list_reactors` as two uncached
  awaited catalog round-trips on every statement (~120µs on the warm OLTP
  loop). They are now served from a per-session, catalog-epoch-validated
  `dml_flags_cache` (one warm `Mutex` lock on the steady state), refetched on
  any catalog mutation — FK / reactor DDL bumps the epoch and a same-statement
  eager clear covers within-statement visibility on the epoch-0 backends. The
  cached verdict is identical to the inline calls'.

### Added

- **`BASIN_PREWARM_PROVIDERS` (default off).** When set to `1`, the first cold
  table-meta load in a session fires a fire-and-forget task that reads the
  table's per-file stats/footers into the process-wide caches, so a follow-up
  cold SELECT skips the per-file footer fetch (cold ~3.4→~2.3ms; the steady
  warm path is unaffected). At most one warm per (session, table); a no-op when
  the flag is unset. Covered by a structural cold-vs-warm test (not wall-clock).

## 2026-06-13 — PostGIS general 2-D geometry SQL surface

### Added

- **General 2-D geometry types in SQL.** `LINESTRING`, `POLYGON` (with holes),
  `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, and `GEOMETRYCOLLECTION` are
  now first-class values across the `ST_*` surface, backed by variable-length
  WKB/EWKB/WKT/GeoJSON codecs in `basin-geo::geometry` (over `geo_types`).
  Constructors `ST_GeomFromText` / `ST_GeomFromGeoJSON` / `ST_GeomFromWKB` and
  output codecs `ST_AsText` / `ST_AsGeoJSON` / `ST_AsEWKB` accept and emit every
  type. Accessors/measures: `ST_GeometryType`, `ST_NumPoints`,
  `ST_NumGeometries`, `ST_GeometryN`, `ST_PointN`, `ST_StartPoint`/`ST_EndPoint`/
  `ST_ExteriorRing`, `ST_Length`/`ST_Area`/`ST_Perimeter` (planar + WGS84
  `geography` variants), `ST_Centroid`, `ST_Envelope`. Predicates
  `ST_Intersects` / `ST_Contains` / `ST_Within` use the `geo` crate's exact
  planar topology traits — not bbox approximations. The fixed-21-byte `POINT`
  fast path is unchanged; only `POINT` has a native column DDL type and an
  R-tree index, so general geometries ride as `BYTEA`/WKB values.
- Conformance: `tests/integration/tests/postgis_conformance.rs` grows a
  general-geometry group (WKT/GeoJSON/WKB/EWKB round-trips, hand-computed
  `ST_Length`/`ST_Area`/`ST_Centroid`/`ST_Envelope`, multipart accessors,
  exact point-in-polygon and segment-intersection truth cases, NULL/empty
  handling, invalid-WKB typed errors). 58 tests, 0 ignored.

### Fixed

- `ST_AsGeoJSON` emitted object keys alphabetically (`coordinates` before
  `type`) on the general path because the geometry was round-tripped through a
  `serde_json::Value` (BTreeMap); the envelope is now assembled type-first to
  match PostGIS.
- `ST_NumPoints` resolved to the POINT-only UDF and always returned 1; it now
  counts every vertex for `LINESTRING`/`POLYGON`/`MULTI*` (a `POINT` still
  reports 1).
- `ST_GeometryN` / `ST_PointN` rejected `Int64` index literals (sqlparser's
  default integer width) with a coercion error; both now accept `Int32` and
  `Int64`, mirroring `ST_SetSRID` / `ST_Transform`.

## 2026-06-13 — Native IVFFlat, vector_avg aggregate, f32-backed halfvec

### Added

- **Native IVFFlat vector index.** `basin-vector::IvfFlatIndex` implements a
  real coarse quantiser: deterministic Lloyd k-means partitions vectors into
  `lists` cells, and a query probes the `probes` nearest cells (default 1,
  matching pgvector's `ivfflat.probes`) then exact-ranks the gathered
  candidates. `CREATE INDEX … USING ivfflat (col <opclass>) WITH (lists = N)`
  is accepted and the `lists` knob round-trips through the catalog opclass.
  Approximate by design — candidates are exact-ranked so the returned rows are
  exactly ordered among those retrieved; recall is a quality metric that rises
  with `probes`, not a correctness gate (same ANN tradeoff as HNSW). Measured
  recall@10 on a seeded clustered set: probes=1 ≈ 0.90, probes=4 = 1.00,
  all-cells = 1.00 (exhaustive → exact). Unit-tested in `basin-vector`
  (`ivfflat::tests`); SQL surface + brute-force equality pinned in
  `vector_conformance.rs` (group 15).
- **`vector_avg(v)` aggregate.** Element-wise mean of `vector(N)` values, with
  PostgreSQL aggregate semantics: NULL inputs skipped, zero non-NULL rows →
  NULL, dimension mismatch is a hard error. f64 accumulation, f32 result,
  GROUP BY supported. Registered as `vector_avg` only (not `avg`, which would
  shadow numeric AVG in DataFusion's name-keyed UDAF registry).
- **`halfvec(N)` column type (f32-backed).** Accepted and round-trips like
  `vector(N)`; stored with the same `FixedSizeList<Float32>` layout so every
  vector path (INSERT coercion, distance ops, HNSW/IVFFlat routing,
  `vector_dims`/`vector_norm`/`vector_avg`) works unchanged. Precision note:
  Basin keeps full f32 precision rather than pgvector's f16 truncation, so
  distances are at least as accurate. Native f16 on-disk storage (to halve
  segment size) is deferred to avoid a storage-format break. `sparsevec(N)`
  remains a typed `0A000` (no silent dense coercion).

## 2026-06-13 — Change events from hot-tier UPDATE/DELETE fast paths (CDC/realtime)

### Fixed

- **Hot-tier UPDATE/DELETE fast paths now dispatch change events.** The
  point-mutation fast paths (`hot_tier_update_by_pk`, `hot_tier_delete_by_pk`)
  and in-transaction hot mutations drained at COMMIT never dispatched
  post-commit change events, so the realtime websocket and the CDC ring were
  blind to hot-tier UPDATE/DELETE — only INSERT and cold copy-on-write
  UPDATE/DELETE fired. Now every committed hot-tier mutation reaches the
  post-commit sinks: UPDATE (single-table fast path AND `UPDATE … FROM`) emits
  before/after through the shared overlay-write seam; DELETE captures
  before-images before the tombstone write. In-transaction mutations buffer
  per-tx and dispatch in commit order at COMMIT, with ROLLBACK discarding the
  buffer. All capture is gated on a post-commit CDC/realtime sink being
  attached (the always-on pre-commit reactor sink does not trigger it), so the
  zero-CDC-sink OLTP hot path reads no extra rows, projects no extra
  `UPDATE … FROM` join columns, and consumes no event sequence numbers — the
  1M-row UPDATE benchmarks are unaffected.

## 2026-06-13 — UPDATE … FROM is set-oriented (kill per-row quadratic)

### Changed

- **`UPDATE … FROM` is now set-oriented and scale-invariant in target table
  size.** It previously ran the join SELECT once, then issued a full
  `UPDATE … WHERE pk = X` SQL statement per matched row — each inner statement
  re-read the matched row's pre-image and bumped the hot epoch, forcing a
  provider-cache rebuild over every live file (an O(M·F) blow-up: ~150s at 10k
  rows, ~51min at 1M). Now ONE join projects every matched target row's full
  post-image (each SET column as its RHS expression evaluated against the
  joined pre-image, every other column carried unchanged), keyed by the target
  PK; matched `(key, post-image)` pairs are deduped by PK (last-occurrence-wins,
  preserving the old loop's multi-match behaviour) and written in ONE batched
  overlay write. On memtable-budget decline the post-images drain to cold in
  budget-bounded chunks through the existing narrowed-merge + index-maintenance
  path — never a per-row loop. A new scale-invariance gate
  (`update_from_scaling_gate`) pins a fixed 200-row `UPDATE … FROM` /
  `DELETE … USING` over a GIN-indexed table at 10k and 100k rows to
  `t(100k)/t(10k) ≤ 3`.

## 2026-06-12 — Per-project pgwire connection ceiling (#28b)

### Added

- **Per-project pgwire connection ceiling.** A hard ceiling on simultaneously
  open pgwire connections per project, enforced at the pgwire startup handler:
  a new connection is refused with SQLSTATE 53300 (`too many connections for
  project (ceiling reached)`) once the live count reaches the ceiling.
  `CatalogConnectionLimitProvider` reads the ceiling from the catalog on every
  new connect; fail-closed — a project with no stored ceiling gets 25 (the Free
  tier). The ceiling is persisted (new `project_max_connections` catalog table /
  in-memory map) and set via the admin route `POST
  /admin/v1/projects/:id/max-connections` (admin JWT, project_id claim must
  match the path); `GET` reads it back. It is a CEILING, not a reservation:
  lowering it never kills existing connections — only new admits are refused
  once live >= ceiling (a RAII guard decrements the live count on
  disconnect/drop).

## 2026-06-12 — Multi-node: quorum-replicated WAL (`BASIN_WAL_MODE=raft`)

### Added

- **Cross-process raft transport over tonic/gRPC (`raft-net` feature).** With
  the `raft-net` feature built (`cargo build -p basin-server --features
  raft-net`; requires `protoc`), `RaftWal` nodes talk over a real wire protocol
  instead of the in-process simulation, so a cluster runs across separate
  processes / hosts. Each RPC (append_entries / vote / install_snapshot) carries
  the openraft message as a serde_json-encoded opaque payload — the same codec
  the disk log frames entries with, so wire and log agree on entry encoding. The
  tonic network factory dials lazily, reuses one HTTP/2 channel per peer
  (evicting on transport failure and on openraft backoff so a peer that
  restarted at the same address re-resolves), bounds every RPC by
  `min(openraft hard_ttl, BASIN_RAFT_RPC_TIMEOUT_MS)`, and backs off
  exponentially. Error mapping is liveness-correct: connect / timeout /
  unknown-peer become Unreachable, malformed bytes become NetworkError, and a
  peer that answered with a raft error becomes RemoteError. v1 is plaintext on
  an assumed-private cluster network; mTLS is the documented production
  follow-up. `basin-server` selects the tonic factory when `BASIN_RAFT_BIND` /
  `BASIN_RAFT_PEERS` are configured and starts the transport server on the bind
  addr; without the feature, raft mode uses the in-process Sim network.
- **Quorum-replicated WAL durability (`BASIN_WAL_MODE=raft`).** Off by default
  (`local` mode is byte-identical to today). In raft mode the WAL durability
  boundary becomes a **quorum ack** instead of a local fsync: a WAL append
  batch is proposed as one openraft log entry, so a single consensus round plus
  local fsync is amortised over the whole group-commit batch, and `durable_lsn`
  advances on the raft commit index. Backpressure is fail-closed and typed — a
  write that cannot reach quorum blocks then fails with the retryable
  `RaftNoQuorum` (SQLSTATE 40001), never a silent partial ack.
- **Manifest-anchored raft snapshots.** A raft snapshot is a small manifest
  pointer (catalog snapshot id + per-`(project, partition)` flushed watermark),
  not a data copy: S3 and the catalog already hold the rows. After the compactor
  commits, `record_flush_watermark` stamps the watermark, snapshots, and purges
  the raft log up to it, so the local log stays bounded by the un-flushed window.
- **Server cluster wiring + leader fence.** `BASIN_WAL_MODE=raft` selects the
  `RaftWal` backend for the shard, parsing `BASIN_NODE_ID` / `BASIN_RAFT_BIND` /
  `BASIN_RAFT_PEERS` (`id@host:port`) and bootstrapping (`BASIN_RAFT_BOOTSTRAP=1`
  on one node) or joining. Raft leadership is the write fence and supersedes the
  writer lease: a non-leader write is refused before the raft round-trip with the
  typed retryable not-leader error (SQLSTATE 40001) carrying a leader hint; with
  `BASIN_LEASE_MODE=required` also set, raft wins and the lease is logged as
  subsumed. Raft mode without `BASIN_SHARD_ENABLED=1` / bind / peers is a
  refuse-to-start error. Cluster status (node id, role, term, commit index,
  peers) is logged at startup via `RaftWal::cluster_status`.

## 2026-06-12 — SQL compat: ALTER ADD UNIQUE, typed timestamptz binds, 42883/42703

### Added

- **`ALTER TABLE … ADD CONSTRAINT <name> UNIQUE (cols)`** — the post-hoc
  multi-column UNIQUE addition Django's schema editor emits for
  `unique_together` (previously a 42601 reject). The engine
  backfill-validates the existing rows with a projection-pushdown distinct
  scan (duplicates reject the ALTER with SQLSTATE 23505,
  `could not create unique constraint …`), registers the named
  `UniqueConstraint` in the catalog, and subsequent INSERT / UPDATE
  enforcement rides the same machinery as a CREATE-time `UNIQUE (cols)`.
  Unnamed `ADD UNIQUE (col)` synthesises the PG-convention
  `<table>_<col>_key` name; `DROP CONSTRAINT` already removed UNIQUE
  entries and still does.
- **Typed TIMESTAMPTZ bind parameters** — extended-protocol timestamptz
  binds (tokio-postgres `SystemTime` / `chrono::DateTime<Utc>`, binary or
  text format) decode to a dedicated `ScalarParam::Timestamptz`
  (Unix-epoch micros) and render as a canonical `'…+00'::timestamptz`
  literal through the bind-direct INSERT fast path, the AST substitution,
  and the text route — microsecond-lossless on all three.

### Fixed

- **Missing-function / missing-column SQLSTATEs** — planning errors for an
  unknown function now surface as 42883 (`function <name> does not exist`)
  and for an unknown column as 42703 (`column "<name>" does not exist`)
  instead of the XX000 internal bucket, mirroring the existing 42P01
  missing-relation seam (psycopg/asyncpg raise their dedicated
  `UndefinedFunction` / `UndefinedColumn` classes on these codes).

## 2026-06-11 — FTS: GIN-on-tsvector pruning hardened (5.20.E), to_tsquery stemming

### Added

- **GIN-on-tsvector CREATE INDEX backfill** — `CREATE INDEX … USING gin
  (tsvector_col)` now backfills the FTS lexeme posting list over
  pre-existing live files (after settling any live hot-tier overlay), so an
  index created after data exists actually prunes instead of never reaching
  full coverage. Accepts Utf8 / LargeUtf8 / Utf8View readback encodings and
  records true row-group ordinals.
- **`to_tsquery` Snowball stemming** — `to_tsquery([config,] text)` stems
  each lexeme through the same pipeline as `to_tsvector` (PG parity:
  `to_tsquery('english','runs')` now matches a document containing
  `running`). Raw `::tsquery` casts remain unstemmed, matching PG. The GIN
  probe consumes the identical canonical form, so probe lexemes and `@@`
  re-evaluation can never disagree.
- **Adversarial FTS pruning harness** (`fts_harness.rs` slice 6) — stem
  consistency (incl. a `simple`-config trap), OR-unions-files, NOT/phrase
  soundness, live-overlay vetoes for both fast paths,
  CREATE-INDEX-over-overlay settle, incomplete-coverage degradation, and a
  read-counter test pinning that a 3-file layout reads ≤ 1 file when pruned
  and all 3 on the unprunable shape.

### Fixed

- **Structural tsquery probe (wrong-results class)** — the FTS posting-list
  probe previously AND-merged every lexeme atom regardless of the query's
  boolean structure: `'cat' | 'dog'` over files each holding only one of
  the lexemes intersected to ∅ and short-circuited to zero rows;
  `'cat' & !'dog'` pruned to the files containing `dog`. The probe now
  parses the canonical tsquery and evaluates it — `&`/`<->` intersect file
  sets, `|` unions them, `!` and never-indexed lexemes decline to a full
  scan.
- **FTS Empty short-circuit + pruning now carry the GIN safety contract** —
  both are gated on no-live-overlay (O(1) counters) and per-file
  completeness (every live file indexed), mirroring the JSONB GIN guards;
  in-transaction SELECTs decline FTS pruning entirely. Posting-budget
  eviction un-marks only the affected files (per-file degradation instead
  of whole-column), file paths are interned, and the insert path records
  true row-group ordinals (previously everything landed on row-group 0 — a
  false-negative risk for batches larger than one row-group).
- **Config-aware probe canonicalisation** — a `plainto_tsquery('simple', …)`
  probe was canonicalised with the English stemmer, which could prune the
  files holding the real (unstemmed) matches; the config argument is now
  threaded through, and non-literal configs decline to a full scan.

## 2026-06-11 — COPY: sqlx PgCopyIn shape + binary COPY format

### Added

- **Binary COPY** — `COPY … FROM STDIN / TO STDOUT WITH (FORMAT BINARY)`
  implements the PG binary COPY format (19-byte `PGCOPY` header, per-tuple
  `i16` field count + `i32`-length-prefixed fields, `0xFFFF` trailer). Both
  directions reuse the existing pgwire binary codecs (`decode_param_binary`
  for COPY-IN fields, the binary `DataRow` field encoder for COPY-OUT), so
  COPY and extended-protocol binds can't drift. Supported column types:
  int2/4/8, float4/8, bool, text, bytea, jsonb, uuid, timestamp[tz], date,
  numeric; anything else (vectors, intervals, arrays) rejects with a clean
  `0A000` naming the column. CSV-only options (`HEADER` / `DELIMITER` /
  `NULL` / `QUOTE` / `ESCAPE`) in BINARY mode and BINARY file paths reject
  with `42601`. Mid-stream errors drain to `CopyDone` exactly like CSV.
- **Quoted identifiers in COPY** — `COPY "users" (id, email) FROM STDIN`
  (the verbatim `sqlx::PgCopyIn` statement) now parses; previously failed
  with `expected table name or '(' after COPY`. Quoted names outside the
  bare-identifier charset are still rejected (they would be re-rendered
  into SQL unquoted). The ORM corpus drives the sqlx COPY shape through the
  real CopyIn sub-protocol and classifies it `Ok`.
- **Modern option syntax** — `COPY t FROM STDIN (FORMAT CSV)` (parenthesised
  options without `WITH`, PG ≥ 9.0) and the legacy `WITH BINARY` shorthand.
- **COPY ingest fast path** — `DATE` columns and fractional-second
  timestamps (`…15:09:26.535897`) now parse in the batched Arrow ingest
  path (previously errored; fractional timestamps are what binary COPY-IN
  decodes to).

## 2026-06-12 — compatibility deep-cuts: 42P01, Django/GORM, GIN-overlay unlock, promoted-column fixes

- **Missing tables answer 42P01** (`a49b51f`): ORM migration harnesses
  branch on undefined_table to bootstrap their tracker tables; the exact
  code and PG-shaped message are now pinned at the wire. DISCARD/RESET
  pool-reset statements are accepted with correct tags.
- **ORM corpus: 7 ORMs, 138/139** — Django (savepoint-per-atomic,
  MigrationRecorder, contenttypes, FOR UPDATE) and GORM (pgx
  extended-protocol AutoMigrate, RETURNING, batch, upsert) join Drizzle,
  Prisma, sqlx, Diesel, TypeORM. The one typed error is a named gap:
  ALTER TABLE … ADD CONSTRAINT UNIQUE.
- **GIN-only tables ride the overlay UPDATE path** (`c50c07f`): the three
  documented blockers are closed, so jsonb_set on a GIN-indexed table
  writes overlays instead of rewriting files (~99ms → overlay-class ms at
  1M, to be measured); the reconciler's materialize now rebuilds GIN/FTS
  registries for replacement files so pruning re-engages after drains.
- **Promoted-column fixes** (two more real bugs): every fast-select
  fallback handed DataFusion un-rewritten SQL (hard plan error whenever
  promotion was active but the fast path declined), and the backfill
  sweep dropped PK blooms from every swept file while never blooming the
  shadow column. Program correctness tally: eighteen.

## 2026-06-11 — integrity benchmark run (provenance: single idle-box session, 1M solo)

All Postgres-compare cards (10k / 100k / 1M), the ORM corpus card, and the
real-S3 (SeaweedFS) cards were regenerated in one evening session on an
otherwise-idle box, with the timing-fragile 1M card run solo. README and
CAPABILITIES numbers are rewritten verbatim from these JSONs.

- **The honest tally: across the ~100 ms-shapes on this card Basin is
  faster on 51, Postgres on 54** (the three unsupported/failed shapes —
  MERGE, INSERT…SELECT, PG's hour-bucket failure — excluded from the count).
- **Headline wins**: bulk INSERT now beats Postgres at every scale —
  33.6 ms vs 117 ms at 10k (3.5×), 216 ms vs 839 ms at 100k (3.9×),
  **2.08 s vs 8.10 s at 1M (3.9×)**. LATERAL JOIN 6.7 ms vs 3,080 ms
  (462×), star join 11.6 ms vs 3,040 ms (261×), correlated subquery 49 ms
  vs 5,510 ms (113×), range scan 0.40 ms vs 32 ms (81×), large result
  stream (100k-row drain) 0.44 ms vs 11.0 ms (25×). Point query p50
  0.06 ms at 10k / 0.50 ms at 1M; single-row UPDATE 0.33 ms at 10k /
  1.24 ms at 1M.
- **Honest flags**: the new mixed read-write concurrency shape (8R+4W,
  600 ops) measures **202 ms vs PG 12 ms** at 1M — the largest open
  concurrency gap; 16-session concurrent SELECT remains a loss (19 ms vs
  2.3 ms). Several OLTP fast paths measure at full speed on the 10k card
  (keyset 0.07 ms, LIMIT-no-ORDER 0.06 ms, ARRAY_AGG / DISTINCT ON
  at-or-better than PG, GIN `@>` effectiveness 4.3×) but decline on the
  1M card while hot-tier overlays from earlier card shapes are live
  (keyset 23.5 ms, LIMIT 52 ms, ARRAY_AGG 516 vs 90 ms, GIN effectiveness
  1.12×) — under investigation as a card-order interaction;
  correctness-first by design. Also flipped to losses on this 1M card:
  on-disk bytes (321 MB vs PG 306 MB, ~5% larger; Basin still 1.9×
  smaller at 100k and 102× smaller on real S3), COUNT(*) full table
  (95 ms vs 29 ms), and DELETE WHERE id IN (1.16 ms vs 0.41 ms; still a
  win at 10k/100k).
- **Durability delta measured**: `SET basin.synchronous_commit = on` adds
  **~2%** to the 10k bulk INSERT in the probe harness — group commit
  amortizes one fsync per statement group. The default remains async ack
  with the documented ≤200 ms loss window, disclosed wherever a write win
  is published.
- **ORM corpus card regenerated**: 94/99 shapes ok (Drizzle 24/24, sqlx
  18/19, Diesel 18/19, TypeORM 15/16, Prisma 19/21) — all 5 failures are
  typed errors, 0 regressions.
- **Real-S3 card (SeaweedFS, 100k)**: 102× smaller on disk (99 KB vs
  10.1 MB), point query 4.6 ms vs 18.5 ms (4.1×), 100k-row INSERT 96 ms
  vs 1.9 s (20×); on the scale-up card point-query p50 stays bounded as
  data grows (0.89 ms at 10k → 1.8 ms at 1M, 2.0× growth over 100× data).

## 2026-06-11 — S4 age-based residency: read-own-insert with zero file opens

- **Fixed: warm point reads collapsed ~30× by the unfiltered-decode cache**
  (`6bb462a`, `259a145`): two compounding defects, found via a scaling
  differential against June-5 data. First, every cache hit took an exclusive
  page-cache shard lock to promote LRU recency, and all point reads on a
  file share one cache key — a lock convoy (fixed: read-lock peek with
  opportunistic promotion). Second and dominant: a filtered read that missed
  the shared entry performed the whole-file unfiltered decode (~20ms on 1M
  rows) to populate it, and an over-budget entry never admits, so every
  query repeated the decode; even on a hit, serving meant filtering the
  full cached decode per query instead of the zone-map-pruned path. Filtered
  reads are now serve-only consumers, row-gated
  (`BASIN_UNFILTERED_SERVE_MAX_ROWS`, default 65536); unfiltered scans keep
  populating and serving at any size. Measured solo (1M-row point reads):
  C=1 42.7 → 1,255 qps; C=16 218 → 7,851 qps. The concurrency cliff bar was
  re-derived from measured reality at the same time (`f39b2d5`): non-collapse
  ≥ 0.7 at C=64 instead of a ≥ 1.5× headroom that never passed any recorded
  run on this hardware, including the run that introduced it.
- **GIN consumers stop trusting the index past what it can prove**
  (`33b820c`-class): the containment/key-existence Empty short-circuits and
  the pruned-table re-registrations could drop overlay-modified rows, and a
  stale posting set pointing only at replaced files could return zero rows
  even after the overlay drained. Both now require live-overlay counters at
  zero AND per-file completeness; CREATE INDEX settles overlays before
  backfilling.
- **DISTINCT ON lowering trimmed** (~2× est.): ON columns project from the
  group keys and per-column first_value orders by only the non-key tail —
  bit-identical winners, half the comparator work.
- **Deep top-K verdict**: TopK was never broken — the residual is the
  whole-table wide-decode floor (Vortex declines dynamic-filter pushdown);
  plan shape and correctness now pinned, late-materialization design
  documented as the follow-up.
- **Concurrent reads stop queueing on the catalog** (`2cf6e21`): the table
  map's Mutex serialized every SELECT's load_table; 16 concurrent readers
  spent ~14 of their 17ms in the lock queue. Reads now share an RwLock.
- **COUNT(\*) metadata gate is O(1)** (`d5e8eff`): the tombstone check
  cloned the whole memtable (a 1M-entry clone per query under S4
  retention); it now reads the maintained counter.
- **Ordered ARRAY_AGG ~3-5× faster** (`d9f1065`): DataFusion's
  order-sensitive accumulator allocates per row; a delegating UDAF
  Arc-buffers batches and sorts once, byte-compatible output.
- **GIN works at scale + three wrong-results fixes** (`9d197ce`): posting
  lists store deduped (term, file) pairs against a 5M budget instead of
  per-row entries against 500k — a 1M-row backfill no longer evicts itself
  into uselessness (measured effectiveness was 1.03×, i.e. none) — pruning
  degrades per file instead of all-or-nothing, and `@>` evaluates over raw
  canonical bytes instead of building a serde tree per row. Fixed: `<@`
  acceleration was unsound (could prune away correct superset documents),
  nested-object needles emitted non-necessary terms (same effect), and the
  jsonb-posting sidecar silently dropped rows from over-budget files.
- **Unordered LIMIT early-exits** (`46ba20b`): SELECT … LIMIT k drove every
  candidate file eagerly (49.6ms at 1M for LIMIT 100); files now open one
  at a time with the remaining limit pushed down, stopping at k rows.
- **Batched multi-row upserts; in-tx literal INSERT fast path** (`ecfa6a8`):
  one IN-list conflict probe + one CASE UPDATE riding the overlay path
  replaces per-row probe+UPDATE recursion (50-row upsert ~5.4ms → sub-ms
  expected); BEGIN; INSERT ×100; COMMIT stops parse-cache-missing every
  statement. The overlay eligibility checker also honors its documented
  simple-CASE contract — caught by the new zero-replacement-files gate.
- **Benchmark coverage: eight new head-to-head shapes** (`a18374a`-class):
  UPDATE…FROM, DELETE…USING, INSERT…SELECT, MERGE (honest GAP), 1000-row
  upsert, 100k result streaming, prepared bind-direct latency, mixed
  read-write concurrency; plus bulk-upsert/LIMIT/GIN scale gates and an
  ignored 10M smoke run.
- **Durability is now a knob, not an asterisk** (`652f278`):
  `SET basin.synchronous_commit = on` makes INSERT acks wait for a
  group-committed, genuinely fsynced WAL segment (one fsync wakes every
  writer coalesced within `BASIN_WAL_COMMIT_DELAY_MS`, default 2 ms);
  the default stays async with the documented ≤200 ms loss window. The
  old claim that local PUTs were fsync-durable was false (write+rename,
  no sync) and every such doc is rewritten.
- **Prepared-statement INSERTs reach the fast paths** (`a6b1f7c`): prepared
  literal INSERTs skip the per-Execute AST clone/substitution/re-render and
  route through the tuple scanner; parameterized INSERTs build batches
  directly from bound wire values via a Parse-time bind plan. Fixes a
  pre-existing bug where binary-format timestamp parameters failed to parse
  at all.
- **Keyset pagination short-circuits disjoint PK runs** (`6a67d64`): pages
  open at most ~2 files on the layouts statement-affine striping and
  stripe-merge produce, instead of every file ahead of the cursor.
- **Fixed: btree secondary-index registry went stale on CoW replaces**
  (`0f58882`): index probes are authoritative file allowlists, and
  UPDATE/DELETE rewrites never re-registered replacement files — queries on
  indexed values could silently return nothing. All four replace sites now
  purge + re-register + flush the sidecar. The delta-update gate keeps
  declining indexed tables, now with the overlay-visibility proof documented
  and an adversarial GIN/btree suite pinning it.
- **Stripe-merge compaction** (`92668f4`): a background tick merges cold
  files with overlapping PK zone maps into strictly fewer files with
  disjoint ranges, so keyset pagination prunes to 1–2 file opens instead of
  opening every write stripe (the ~24 ms residual at 1M). Conservative
  gates (single-column PK as effective sort order, decodable stats,
  256 MiB input cap), optimistic-concurrency commit with liveness
  re-validation, index re-registration via the tail-compaction helpers.
- **Measured: bulk INSERT now beats Postgres** — 10k rows: 36.9 ms vs PG
  126.5 ms (3.4×); 1M rows: 2.05 s vs PG 9.62 s (4.7×), down from 262 s
  three waves ago. The stack: literal-VALUES scanner + `::jsonb` admission +
  statement-affine striping + the pre-parse path below (the rewrite-pipeline
  scan over the multi-MB statement turned out to be the dominant hidden
  cost). Disclosure: Basin acks before fsync (≤200 ms loss window); the PG
  numbers are fsync-durable — the synchronous-commit knob is designed, not
  yet shipped. Conditional UPDATE holds near-parity at both scales (4.5–4.8
  ms vs PG 3.3–3.7); FOR UPDATE is even; RMW contention 8.6 ms vs 4.0.
- **Pre-parse fast path for literal INSERTs** (`180c49c`): plain
  `INSERT INTO t [(cols)] VALUES (…)` statements skip the double
  whole-statement parse (libpg_query dispatch parse + a sqlparser AST whose
  tuple expressions were discarded unused), the depth scan, and the rewrite
  pipeline — an O(prefix) classifier routes raw SQL through the tuple
  scanner into the shared INSERT tail. Auto-commit only; RETURNING /
  ON CONFLICT / CTEs / transactions / hypertables decline to the normal
  path with equivalence pinned.
- **Background overlay reconciler** (`4bbdbe8`): overlay UPDATE/DELETE rows
  drain via a periodic sweep (`BASIN_OVERLAY_RECONCILE_SECS`, default 5 s)
  instead of waiting for a conflicting cold operation — bounding read-tax
  and durability exposure now that delta updates can park 10k overrides.
- **Fixed: overlay suppression hole on narrow fast-path reads** (`92dfe90`):
  aggregate and non-PK-projection fast reads decode a minimal column set,
  and suppression of overridden cold rows silently no-ops when the PK
  column is absent — `SELECT SUM(v)` with outstanding overlay updates
  returned old + new values summed. The read projection now includes the
  PK while an overlay is live; the gate that should have caught it was
  strengthened and mutation-verified.
- **Delta updates: overlay UPDATEs to 10,000 keys, including no-WHERE shapes**
  (`ba049f5`): the 64-key small-bulk cap becomes `BASIN_DELTA_UPDATE_MAX_KEYS`
  (default 10k) behind a stage-then-reserve memory guard that declines to the
  cold copy-on-write path with zero partial state when the hot-tier budget is
  exhausted. The matching probe now carries full pre-image rows (no second
  cold read per statement), and UPDATEs without a WHERE clause route through
  it too — the conditional-UPDATE shape becomes overlay writes instead of a
  whole-file rewrite. A 200-key UPDATE at 100k rows is gated to write zero
  replacement files, mirroring DELETE's tombstone-only invariant.
- **Statement-affine write striping** (`d509a29`): an INSERT statement writes
  its whole batch to one WAL stripe (chosen per session) instead of slicing
  across all 8 — the slices ran sequentially anyway, so a statement paid 8
  encodes/locks/files for zero parallelism. Cross-session fan-out is
  preserved; a statement's PKs now co-locate in one file, helping bloom
  pruning.
- **Fixed: page-cache representation collision on semantic types**
  (`648c114`): schema-less reads (CoW rewrite pre-images, CV refresh) cached
  raw physical batches — UUID disguised as Decimal256, POINT as LargeBinary —
  under the same key catalog-aware scans serve verbatim. Cache keys now carry
  the populating read's schema-awareness; a cross-class regression test fails
  without the split, and the restamp tail is proven idempotent.
- **Hot-tier UPDATE fast path: batched expression eval + overlay memo**
  (`081387e`, `98fa44f`): multi-key expression UPDATEs evaluate once over a
  concatenated pre-image batch instead of building a DataFusion
  SessionContext per key (the structural reason the small-bulk cap sat at
  64), and the decoded update overlay is memoized per table keyed by
  (epoch, update-count) — sound across the flush ack's no-epoch-bump
  re-tagging — with override rows appended as one batch per scan instead of
  one single-row batch each. Groundwork for delta updates at higher
  cardinality.
- **Retention is now enforced** (`bcef9d1`): the shard background loop
  gained a clean-retention sweep (`BASIN_HOTTIER_SWEEP_SECS`, default 30 s)
  calling `enforce_clean_budgets` — previously nothing in production drove
  the retention window. The never-wired hottier FlushTask production
  plumbing is decommissioned (wiring it would have resurrected committed
  DELETEs — its tombstone application was a stub); the shard compactor is
  the sole durable flusher, stated explicitly.
- **WAL zero-copy payloads** (`390bce9`): WAL records hold refcounted
  payload bytes (wire format unchanged, pinned by a fixture test), the ack
  path drops its per-stripe payload memcpy, and segment framing + the
  object-store PUT moved outside the partition lock — concurrent appenders
  no longer stall behind an in-flight flush. PUT-failure semantics are
  byte-for-byte preserved.

- **Read-own-insert is now memory-served** — an auto-commit INSERT of up to
  128 rows (`BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS`) writes through to the
  hot tier as CLEAN entries the moment the shard WAL acks it, and the
  canonical point SELECT (`WHERE pk = …`) answers from that entry **before**
  the shard tail flush, the catalog load, or any cold file open: the
  previously documented "read-own-insert tail flush" gap is CLOSED. Bulk
  loads (> 128 rows), counter-keyed / composite-PK rows, and
  `BASIN_HOTTIER_RETAIN_SECS=0` (the kill switch — byte-for-byte the old
  behavior) skip the write-through. Pinned by `row_tier_residency.rs`
  (work-counter gates: `files_opened == 0`, `rows_decoded == 0`, catalog
  snapshot unmoved).
- **Retention now affects engine reads** — flush acknowledgement keeps acked
  rows readable as CLEAN entries instead of dropping them
  (`BASIN_HOTTIER_RETAIN_SECS`, default 300 s; per-project clean-byte cap
  `BASIN_HOTTIER_RETAIN_CAP`): a point read of a just-flushed or just-updated
  row stays a memory hit. Engine ack sites thread per-key MVCC seqs into
  `mark_flushed` (overlay materialize; COMMIT-promoted HTAP rows are acked
  clean once the COMMIT's own cold file lands, eliminating their
  double-flush), `commit_replace` clears a table's clean rows after every
  copy-on-write rewrite (stale-residency guard), ALTER TABLE
  materializes-then-clears and TRUNCATE drops the table's hot tier outright,
  and the hard-cap fallback now evicts CLEAN bytes before the legacy
  drop-largest-project last resort. Read paths gate hot-overlay checks on the
  new O(1) tombstone/update counters, the auto-commit memtable fallback skips
  clean rows (fixes a non-PK-Eq under-return when a retained row matched),
  and the promoted shadow-column fast path engages with clean-only residency
  instead of falling back to DataFusion.

## 2026-06-11 — memtable MVCC version chains; fast VALUES scanner: JSONB + timestamps

- **MVCC version chains in the memtable** (`8d9fc2d`): hot-tier entries keep
  oldest-first version chains instead of latest-only. A pinned snapshot read
  keeps being served its own version across any number of subsequent
  overwrites by other sessions — previously a second overwrite could push a
  pinned reader back to the cold pre-image. Chains drain whole at flush
  acknowledgement; per-version bytes feed the existing memory budget. This is
  the S4 row-tier MVCC kernel; age-based residency lands next. Pinned by the
  new `row_tier_mvcc.rs` suite.
- **Fast VALUES scanner admits JSONB and timestamp literals** (`803d8a5`):
  multi-row literal INSERTs on schemas with JSONB / timestamp columns (the
  common events-table shape) now take the fast scanner instead of falling
  back to full AST parsing + per-row coercion. JSONB documents are
  canonicalized through the same encoder as the slow path; byte-level
  fast-vs-slow equivalence on a 10k-row benchmark-shaped table is pinned by
  `values_fast_ingest.rs`.
- **Robust benchmark estimator** (`7ae4a1a`): per-shape card numbers reduce
  through median (n ≥ 5 samples) or min-of-K (below), and 1M-row shapes are
  floored at 3 samples — single-shot 1M numbers could previously swing 3× on
  scheduling noise. Both the Postgres and Basin sides use the same estimator.
- **DATE (Date32) DML** (`258b8ff`): DATE columns were readable but not
  writable — batch_from_rows had no Date32 arm. INSERT accepts
  `'YYYY-MM-DD'` / `DATE '…'` / `'…'::DATE` literals; UPDATE wiring goes
  through the shared scalar-assignment path so cold, hot-overlay and tx
  fast-path UPDATEs all coerce dates. Residual: `WHERE d IN (SELECT …)` on
  a DATE column still errors.
- **Cold-UPDATE overhead cuts** (`7934575`): expression-RHS assignments no
  longer force before/after event capture when nothing consumes it (sinks /
  audit / generated columns / RETURNING still do), and copy-on-write
  UPDATE/DELETE rewrites encode replacement files with the Fast cascade
  (compaction keeps Best). Targets the no-WHERE conditional-UPDATE shape,
  previously ~208ms on a 500-row table.
- **Bulk-INSERT ablation probe** (`02258ea`): measured (not estimated)
  decomposition of the 10k-row bulk INSERT cost — PK / JSONB / FK / floor
  variants. The benchmark shape has no FK and runs one-shot on a fresh
  engine, ruling out FK checks and O(N-cold) PK scans as explanations.
  Measured split at 10k rows (release, min-of-3): ~339ms total, PK
  enforcement ~1ms, JSONB canonicalization ~43ms, write floor
  (WAL/stripe/batch build) ~292ms, linear in rows, no growth with
  existing data.
- **Fast VALUES scanner: `::jsonb` / `::timestamp` suffix casts**
  (`749ab7b`): the probe revealed the published benchmark sends
  `'<json>'::jsonb` payloads, which the scanner declined — the JSONB fast
  path never engaged on the real statement. Type-matching suffix casts on
  string literals are now admitted (`::jsonb` into JSONB, `::timestamp` /
  `::timestamptz` into timestamp columns, byte-identical to the slow
  path's cast peeling); `::text` and all other tags still decline because
  the slow path rejects or differs on them. Function-style `CAST(...)`
  still declines.
- **S4 row tier, phase 2 groundwork** (`53b36a7`, `3db6ac9`): memtable
  entries carry clean/dirty state with seq-aware flush acknowledgement
  (fixing a latent version-loss where the flush ack dropped chains by key
  including versions written after the flush snapshot, plus a freed-bytes
  accounting mismatch), O(1) overlay counters, oldest-first clean
  eviction, and retention budgets (`BASIN_HOTTIER_RETAIN_SECS`, default
  300s; `BASIN_HOTTIER_RETAIN_CAP`, default 32 MiB; 0 = today's
  drain-at-flush). Engine read/write paths don't call the new ack yet, so
  production behavior is unchanged; the residency read path (read-own-
  insert with zero file opens) lands next.

## 2026-06-10 — OLTP scale + correctness wave; HTAP / ORM / extensions roadmap

Two work waves landed since `origin/main`: (1) OLTP scale fixes + correctness
hardening that cut single-row write/read latency to ms-class at 1M rows, and
(2) HTAP / ORM / extension surface expansion. All published numbers below are
measured locally; the 1M LocalFS card runs on a shared box and is
timing-sensitive — the structural cards (RAM/conn, disk, idle cost) and the
real-S3 (SeaweedFS) card are the load-bearing comparison.

### Engine — transactions and MVCC
- **Snapshot-stable reads inside explicit transactions** (`530ec82`): repeated
  SELECTs of the same data inside one transaction return the same answer even as
  other sessions commit.
- **Hot-tier MVCC sequence** (`9f5b7f0`): another session's overlay writes no
  longer leak into an open transaction.
- **Transaction-scoped overlay** (`62e5011`): in-tx single-row DML takes the
  fast path through a per-transaction overlay.
- **Pinned in-tx fast reads** (`f5d1d6f`): untouched-table reads inside a
  transaction execute against a pinned snapshot without re-planning through
  DataFusion. Fixed two read-path soundness holes — a pinned read no longer
  re-discovers the current head (off-by-one in COUNT), and a secondary-index
  probe MISS now falls through to the pruned cold read instead of being treated
  as "definitely empty" (an auto-built index could previously hide a live row).

### Engine — OLTP fast paths (always-on)
- Fast paths flipped **always-on** — removed `BASIN_PK_ROW_CACHE` +
  `BASIN_FAST_BULK_INSERT` opt-ins (`c2cdf60`, #203).
- RMW `SET col = <expr>` fast path; overlay point-read correctness fix
  (`7571af2`). `UPDATE … RETURNING` routed through the hot-tier fast path
  (`d57bcfd`).
- Single-key UPDATE prunes its cold pre-image read (`f0dded8`, #212); UPDATE
  read-before-write consults the PK row cache (`7d3e765`); cold materialize
  deferred to the cold path with a gated INSERT-tail flush (`c11c04e`, #205).
- Keyset + point-join fast paths, index backfills, in-tx gates (`f2876ea`).
- Write-through `PkRowCache` on INSERT commit (`f40f2a2`); `has_pending_tail`
  O(resident-partitions) no-flush tail probe (`3a0406e`).
- `cluster_columns` defaults to the single-column PK for point-query pruning
  (`54198fd`, #204).
- Result: single-row UPDATE at 1M dropped from ~162 ms to ~9 ms; point query
  ~1.4 ms; UPSERT ~0.6 ms; RMW ~11 ms/op; point-join ~2.8–4.6 ms; keyset
  ~5–18 ms; `SELECT … FOR UPDATE` + UPDATE ~21 ms.

### Engine — correctness
- Schema evolution: pre-ALTER files now read, aggregate, and update correctly
  (`8ebb764`).
- Promoted GIN/JSONB shadow-column reads + in-tx pin safety — 4 differential GAP
  shapes recovered (`3180da3`); promoted-column sweep re-seals GIN row-group
  summaries (`715530e`).
- GIN `@>` serves aggregates; upsert checks go through the dispatcher
  (`4daefdc`); multi-row `INSERT … ON CONFLICT DO UPDATE` (`501a757`);
  Binary/LargeBinary normalized at overlay and rewrite boundaries (`b3eb863`).

### ORM compatibility — introspection complete
- `pg_index` populated; `pg_sequence` and `pg_enum` added (`30ea4f3`) — ORM
  startup/migration introspection now resolves for Prisma, Drizzle, SQLAlchemy,
  ActiveRecord, sqlx, Django, Hibernate. Wire-level ORM flow gate added
  (`b746552`). Corpus now **96/99 (97%)**: Drizzle 100%, Prisma 100%, sqlx 95%,
  Diesel 95%, TypeORM 94%.

### Prepared statements
- Execute bound statements without re-parsing (`333861b`); infer parameter types
  through UDF argument positions (`38d7d0e`).

### Vector / pgvector compat
- HNSW `WITH (m, ef_construction)` build params + opclass-matched index routing
  (`d5d99d4`) — a cosine-built index is not used for an L2 query.

### PostGIS / geo
- `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` + constructor-expression INSERTs
  (`41639b8`); R-tree row-exact envelope candidates (`a250891`); single-decode
  residual for `&&` overlap (`dd26766`). `ST_DWithin` ~1.7× faster than PostGIS
  GIST; `&&` count + KNN still trail GIST by ~150×.

### Storage / read path
- Auto-index advisor + sorted-PK skip for IN-lists (`c953d83`); exact O(log n)
  IN-list zone prune (`ce67b8a`); push Utf8 equality into Vortex + minimal
  aggregate read set (`5c2dbdd`); stats-prune catalog-less reads + cached Vortex
  file stats (`e352716`); vectorized predicate eval + real-size decode-cache
  admission (`fc62061`); per-session provider + head-probe caches (`89c218e`);
  shared unfiltered decode across point lookups of a file (`12de75f`).
- WAL: payload clone + clock read moved outside the partition lock (`25b2df1`).

### Test infrastructure
- Scale-invariant **work-counter** CI gates (`1b76c7d`): per-query
  `files_opened` / `rows_decoded` / `bytes_fetched` counters with design-derived
  bounds at 100k, plus `file_count_scaling.rs` asserting `files_opened` stays
  constant as file count grows — these gate indefinite scaling on *work*, not
  wall-clock.

### Known gaps recorded this wave
- FK `ON DELETE CASCADE` multi-level recursion and `DELETE … WHERE id BETWEEN`
  on a scratch table (`basin: unsupported` in the differential bench).
- Read-own-insert costs one O(1) tail flush+read until the row tier lands.
- Cold in-tx UPDATE under an active savepoint routes through the cold
  catalog-commit path.
- `hottier_differential` ordering-only divergences on a few merge-on-read shapes.

## 2026-05-25 — Selective-read pushdown + benchmark fairness

### Engine — filter pushdown through the merge-on-read path
- `TombstoneFilterExec` now implements DataFusion's physical filter-pushdown API
  as a transparent passthrough (`gather_filters_for_pushdown` /
  `handle_child_pushdown_result`). Tombstone suppression commutes with a row
  filter, so a selective predicate is pushed through it into the cold
  Vortex/Parquet scan. Previously the predicate was stuck above it and the cold
  scan read every row — a selective `WHERE id < 100` on a table with DELETE
  tombstones was a full-table scan. Root-cause fix for the JSONB-`->>`-at-1M
  regression: `payload->>'k' WHERE id < 100` at 1M rows drops from ~45-88 ms to
  ~1.5-2.8 ms (scan reads ~100 rows / 14 KB instead of 1 M rows / 144 MB).
  Correctness pinned by `tombstone_filter_pushdown.rs` + the htap/overlay/delete
  suites + the PG result oracle (`9d9e040`).
- `HtapUnionTable` propagates filter pushdown to its cold child (delegates to the
  cold provider, capped at `Inexact` since the union also merges the hot tier)
  (`055b289`).
- Promoted-column backfill sweep rewrites ALL cold files (not just the
  compaction tail) so an auto-promoted JSONB shadow column is materialised
  across the whole live file set; `backfill_promoted_columns` accepts
  LargeBinary/Binary/Utf8/LargeUtf8 source columns (`1313e04`).
- Parquet page-index `RowSelection` prunes within a surviving row group
  (sub-row-group pruning for point/IN probes on Parquet tables) (`d3bda33`).

### Benchmark harness — steady-state fairness
- Small-sample suites now take a symmetric untimed warm-up before timing on
  BOTH engines, and `median()` returns a true median (averages the two middle
  values for even N) instead of the upper element. The prior combination
  reported `max(cold_first_read, warm)` as "p50", surfacing Basin's one-time
  cold-file open while Postgres's just-seeded data was hot in `shared_buffers`.
  `large_in_list_100` at 1M reads its true warm latency (~1 ms) instead of a
  ~23 ms cold artefact. The JSONB steady-state suite awaits the async
  auto-promotion before sweeping so it can't lose the race (`1320d0a`).

## 2026-05-21 — Phase 6.X+ wave

### Phase 6.X — Lease-based ownership (ADR 0023)
- Lease table, `LeaseRegistry`, heartbeat, and WAL epoch fence establish the
  ownership primitive for partition routing (`acf7929`).
- Partition-level routing via `LeaseRegistry` wired into the router accept path
  (`eadebbe`).
- Voluntary lease handoff under load with <500 ms p99 stall (`d85e893`).
- Heartbeat-budget tick + multi-replica acceptance test land Phase 6.X.D
  (`fd88b13`).
- Slice gates: `MemtableBytes` cap (`8ca80ba`), `WasmConcurrency` cap
  (`e5ca5b8`), per-project `RestQps` gate in the authorize path (`bb34b9e`).
- Heartbeat-reconciled budgets foundation — cap consumers wired across catalog,
  router, net, and realtime (`49dd734`).
- Lease observability metrics + operator runbook (Phase 6.X.F) (`af63d22`).
- Lease failure-path regression suite: replica loss, dual-leaseholder fence,
  network partition, budget-coordinator unreach, handoff-mid-write atomicity
  (`dcad853`).

### Phase 6.SEC — Security P0/P1
- Real TOTP replay protection + WebAuthn signature verification close
  beta-blocker P0 items (`94a2e14`).
- OAuth auto-link now requires a verified email and a `(provider, sub)` identity
  key, closing the account-takeover vector (`216fda8`).
- Reserved-schema DDL rejected with SQLSTATE 42501 (`2566baa`).
- Public-bucket alias bypass closed — anonymous alias path routes through the
  full RLS chain (`f1ed678`).
- Presence `client_id` bound to the authed session; metadata size capped
  (`1a4e046`).
- 17 audit-driven regression tests for shipped P0/P1 fixes; remaining P2s
  marked `#[ignore]` with rationale (`1fc84dd`).

### Phase 6.P0 — Production hardening
- Statement-level wall-clock timeout → SQLSTATE 57014 (`efda53e`).
- Catalog `deadpool-postgres` connection pool replaces `Mutex<Client>`,
  eliminating the noisy-neighbor connection bottleneck (`8977fa6`).
- Wasm function runtime: governance wired into the real entrypoint, epoch
  ticker, dedicated runtime, semaphore LRU (`e3b0feb`).

### Cloud-side gates
- 12 OAuth providers added: Microsoft, GitLab, Slack, Discord, Apple,
  Twitter/X, Bitbucket, Notion, Spotify, Twitch, LinkedIn, Figma (`b87c9ee`).
- BYO bucket support + per-project Class-A/B usage counters (`d620281`,
  `8bd64b5`).
- `basin-autoscale` crate and daemon (T-096) (`246b678`).
- Per-subscriber realtime budget cap enforced (`7003e76`).

### Engine bug fixes
- `RETURNS TABLE` parsed correctly from sqlparser 0.53+ `DataType::Table` shape
  (`235c1f8`).
- UNION ALL scan-collapse skipped when all branch predicates are identical
  (`94530d0`).
- `SET`/`SHOW search_path` deferred to real executor in `noop_accept` (`68bae0f`).
- `CommitConflict` from UPDATE/DELETE propagated to the router for re-evaluation
  (`ebac48d`).
- Secondary-index helper `first_string` skips empty batches (`29e2263`).

### Test infrastructure
- Noisy-neighbor + fairness permanent regression harness (#22) (`31bc2a7`).
- `sql_syntax_fuzz` — graceful-handling assertions across 119 broad SQL shapes
  (`4705544`).
- Wasm-functions differential + soak harness (Phase 5.11.W7 capstone)
  (`b832961`).
- Lease failure-path hardening suite (Phase 6.X.E) (`dcad853`).

### Documentation
- Operator runbooks for storage, realtime, wasm-functions, session-pool
  (`5f98af0`).
- ADR 0024 + Phase 6.TR plan: UUID-as-Decimal128 storage encoding (workaround
  for Vortex `FixedSizeBinary` gap) (`a79e710`).
- ADR 0023 leases + partition routing (`073e4aa`).

### Known limitations
- **Secondary-index point-query returns wrong row** — `create_index_roundtrip_point_query`
  and 24 related binaries are `#[ignore]`'d pending resolution of the #40
  cluster (indexed-lookup bug, not blocked on infra).
- **`BASIN_TYPE` metadata round-trip** — type metadata is silently dropped on
  Vortex round-trip; `extra_types` test flagged as a real engine bug (Phase
  6.TR not yet shipped).
- **Format-encoding edge cases** — datetime, hex, and bytea format-encoding
  bugs flagged in `format_encoding`; `@@` comment-rewriter interference in FTS
  flagged in `fts_at_at`; all awaiting #40 triage queue.
- **`TABLESAMPLE BERNOULLI`** — marked as a real behavior change vs PostgreSQL
  baseline; not yet fixed.
- **Performance regression test** — `viability_perf_stack` `#[ignore]`'d with
  measured data: root cause identified but fix not yet landed.

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
- **Catalog-stats cold-path footer skip (A4 wiring)** — the cold-path
  lister (`list_data_files_with_stats`) now seeds each file's
  `(row_count, column_stats)` from the catalog `DataFileRef` the engine
  already persists at flush/compact time, and SKIPS the per-file Parquet
  footer range-GET / Vortex tail-GET entirely when those stats are present.
  On an S3 backend (~10 ms RTT/file) the footer round-trips dominate cold
  latency; eliminating them collapses the per-query footer fan-out to an
  in-RAM catalog lookup. Files written before A4 (empty catalog stats) fall
  back to the footer path per-file — a strict optimisation, never a
  correctness regression. The catalog-stats prune is byte-identical to the
  footer-stats prune (differential-tested in `read_stats_pruning`).

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
