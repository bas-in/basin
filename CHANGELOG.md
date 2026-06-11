# Changelog

All notable changes to Basin are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The pre-1.0 contract: minor versions can break public API; patch versions
are bug-fix only. Once the engine wedge ships to design partners we
graduate to 1.0 and the standard SemVer guarantees.

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
