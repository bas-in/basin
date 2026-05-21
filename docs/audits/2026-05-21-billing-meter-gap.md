# Billing-meter gap analysis — engine counters vs cloud cost model

**Date:** 2026-05-21
**Scope:** every cost dimension priced by `basin-cloud/billing_model/` vs every per-project counter exported by the OSS engine.
**Trigger:** the 2026-05-21 reprice (basin-cloud commit `acb2f7c`) added two
new overage SKUs — `OVERAGE_USD_PER_GB_MONTH` (storage) and
`COMPUTE_OVERAGE_USD_PER_CPU_SECOND` (compute) — neither of which has an
engine-side meter that can be reconciled to a customer invoice.

Sources audited:
- `basin-cloud/billing_model/{model,scenarios,competitors,generate,real_data}.py`
- `basin-cloud/billing_model/proposed_pricing.md`
- `crates/basin-common/src/telemetry.rs` (the only per-project counter surface)
- All `record_*` / `bump_*` call sites under `crates/`

---

## 1. Executive summary — the 3 holes that block T-104 / Phase 4 billing

1. **CPU-seconds per project — not measured at all.**
   **✅ CLOSED (2026-05-21, this commit).** A new monotonic
   `cpu_micros_total: AtomicU64` was added to `ProjectCounters`
   (`crates/basin-common/src/telemetry.rs`) and is bumped on two sites:
   (a) the engine's per-query exec-completion site
   (`crates/basin-engine/src/lib.rs` — right after `record_latency_ms`)
   with `started.elapsed().as_micros()`, and (b) the Wasm
   `ComponentHarness::run_with` wrapper
   (`crates/basin-fn/src/harness.rs`) so per-invocation wall-clock
   on the dedicated wasm runtime is also attributed. Surfaced through
   `ProjectCountersSnapshot.cpu_micros_total` so the OTLP /
   `basin-cloud` aggregator picks it up for free. Per-project
   aggregation + cross-project isolation verified by
   `basin_engine::tests::cpu_micros_total_aggregates_per_project`,
   `governance_caps_test::component_harness_run_with_bumps_cpu_micros_per_project`,
   and `basin_common::telemetry::tests::cpu_micros_counter_is_per_project_and_monotonic`.
   *Original gap text, for reference:* The reprice introduces
   `COMPUTE_OVERAGE_USD_PER_CPU_SECOND = 0.000005`
   (`basin-cloud/billing_model/model.py:78`) as the structural margin line.
   The engine knew `elapsed_ms` per query but only fed it into a 128-slot
   p99 latency ring; the per-project SUM was never aggregated.

2. **Tigris Class-A / Class-B op counts — not measured per project.**
   **✅ CLOSED (2026-05-21, this commit).** Two new monotonic counters
   `class_a_ops_total` (PUT, multipart-complete, COPY, DELETE) and
   `class_b_ops_total` (GET, HEAD, LIST) were added to
   `ProjectCounters` and bumped on every successful RPC in
   `ProjectScopedStore::{put_opts, put_multipart_opts, get_opts, list,
   list_with_delimiter, copy_opts, delete_stream}`
   (`crates/basin-storage/src/concurrency.rs`). Surfaced through the
   existing `ProjectCountersSnapshot` so the OTLP / `basin-cloud`
   aggregator picks them up for free. Cross-project isolation verified
   in `concurrency::tests::cross_project_isolation_class_a_b`.
   *Original gap text, for reference:* The reprice's gross margin claim
   depends on `basin_ops_cost` accurately reflecting
   `ProjectCounterRegistry` PUT/GET counts
   (`basin-cloud/billing_model/model.py:152-173`); we now attribute
   every wire-level Class-A/B op to a project.

3. **Logical-GB-month integration — derivable but not stored.** Billing
   needs *time-integrated* GB-months, not the live high-water mark exposed
   by `bytes_written_total - bytes_deleted_total`
   (`crates/basin-common/src/telemetry.rs:80-81, 122, 128`). Today nothing
   snapshots the registry on a wall-clock cadence; the catalog only stores
   `quota.current_value` (`basin-cloud/billing_model/real_data.py:146-156`),
   which is a point-in-time gauge. A customer who held 100 GB for 29 days
   then deleted to 0 GB would be billed as if they stored 0 GB for the
   whole month.

---

## 2. Full gap matrix

| # | Cost dimension (cloud model)                                  | Unit              | Engine counter today                                                                                                                       | Gap → action                                                                                                                                                                |
|---|---------------------------------------------------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | Storage — `R2_STORAGE_USD_PER_GB_MONTH` (`model.py:23`)       | GB-month          | ⚠️ `bytes_written_total` is a gauge (`telemetry.rs:81`); `record_bytes_deleted` keeps it live (`telemetry.rs:128`). NO time-integration.    | Add `bytes_stored_seconds_total: AtomicU64` updated by a periodic registry sampler in `basin-cloud`. New module: `basin-cloud/src/usage_sampler.rs`.                         |
| 2 | Storage overage — `OVERAGE_USD_PER_GB_MONTH=0.010` (`model.py:67`) | GB-month     | Same as #1                                                                                                                                  | Same fix; the overage uses the identical meter, just compared against `bundled_storage_gb` per tier (`scenarios.py:84-89`).                                                  |
| 3 | Tigris Class-A PUTs — `R2_CLASS_A_USD_PER_M_OPS=50.0` (`model.py:24`) | count       | ✅ `ProjectCounters::class_a_ops_total` (`telemetry.rs`) bumped from `ProjectScopedStore::{put_opts, put_multipart_opts, copy_opts, delete_stream}` on success.                | DONE (2026-05-21). Exported via `ProjectCountersSnapshot.class_a_ops_total`; no new export surface needed.                                                                  |
| 4 | Tigris Class-B GETs — `R2_CLASS_B_USD_PER_M_OPS=5.0` (`model.py:25`) | count        | ✅ `ProjectCounters::class_b_ops_total` (`telemetry.rs`) bumped from `ProjectScopedStore::{get_opts, list, list_with_delimiter}` on success.                                  | DONE (2026-05-21). Exported via `ProjectCountersSnapshot.class_b_ops_total`. HEAD-shaped GETs (`GetOptions::head`) are billed identically to body GETs.                     |
| 5 | Tigris egress bytes (Fly-internal = $0 today)                 | bytes             | ❌ Not measured.                                                                                                                            | Future-cost: add `egress_bytes_total: AtomicU64`; bump in `ProjectScopedStore::get_opts` from `GetResult.meta.size`. Multi-region makes this non-zero (see §5).              |
| 6 | Compute — `FLY_*_USD_PER_MONTH` pool costs (`model.py:33-37`) | $ / pool-month    | ✅ `ProjectCounters::cpu_micros_total` bumped after every `ProjectSession::execute` (`crates/basin-engine/src/lib.rs`) and every `ComponentHarness::run_with` (`crates/basin-fn/src/harness.rs`).                | DONE (2026-05-21). Exported via `ProjectCountersSnapshot.cpu_micros_total`. Pool-flat cost is now attributable to a project by summing per-project CPU.                       |
| 7 | Compute overage — `COMPUTE_OVERAGE_USD_PER_CPU_SECOND=0.000005` (`model.py:78`) | CPU-second | ✅ Same counter as #6.                                                                                                                       | DONE (2026-05-21). The reprice's margin story is now backable by a real meter. basin-cloud sums `cpu_micros_total / 1e6` to bill against the SKU.                              |
| 8 | Active connection-seconds (per-tier conn caps, `proposed_pricing.md:101-103`) | conn·s     | ⚠️ `basin-pool` exposes `resident_sessions` + `resident_per_project` as point-in-time gauges (`crates/basin-pool/src/stats.rs:44-50`). No accumulation. | Add `conn_seconds_total: AtomicU64` bumped on session checkin (release) by elapsed-since-checkout. Wire from `basin-pool::pooled_session::PooledSession::drop`.            |
| 9 | WAL bytes per project                                         | bytes (cumulative)| ✅ `LocalWal::append` bumps `bytes_written_total` + `record_op` (`crates/basin-wal/src/lib.rs:393-394`).                                    | None. Already counted, but shared with table-data writes — no way to split. Document the limitation if WAL becomes its own SKU.                                             |
| 10 | Hottier resident bytes (memtable)                            | bytes (gauge)     | ⚠️ Tracked internally by `MemTableRegistry` per `(project, table)` (`crates/basin-hottier/src/registry.rs:6-32`), but NOT re-exported through `ProjectCounters`. | Add `hottier_resident_bytes: AtomicU64` mirror on `ProjectCounters`; update in `MemTableRegistry::try_reserve_bytes` and `release_bytes`.                                 |
| 11 | Compaction CPU                                               | CPU-second        | ❌ `Shard::compact_one` (`crates/basin-shard/src/in_process.rs:448`) does background work with no project-side accounting.                  | Wrap `compact_one` body in `Instant::now()` + `tc.record_cpu_ms()`. Background CPU is a real margin leak today.                                                             |
| 12 | Function (`basin-fn`) wall-clock                              | CPU-second        | ✅ Wasm invocation wall-clock attributed via `ComponentHarness::run_with` measuring `started.elapsed()` around `invoke_with_caps` and bumping `cpu_micros_total` on the optional `ProjectCounterRegistry` attached via `ComponentHarness::attach_project_counters`.                                                                                                                       | DONE (2026-05-21). Same SKU as engine query CPU (#6/#7) — single billing counter; no need for a separate `fn_cpu_seconds_total`.                                            |
| 13 | Catalog Postgres bytes (`CATALOG_PG_*`, `model.py:41-43`)     | GB                | Out of engine scope — lives in basin-cloud's Postgres.                                                                                       | Not present in tree (and shouldn't be); `basin-cloud` sums `pg_database_size()` directly. Note here for completeness.                                                       |
| 14 | Stripe / Paystack fees (`model.py:46-47, 100-104, 176-199`)  | $ per txn         | Out of engine scope — payments live in basin-cloud.                                                                                          | Not present in tree (correct). No engine work.                                                                                                                              |
| 15 | HTTP egress to customer (REST/SSE response bytes)            | bytes             | ❌ Not measured. `basin-rest::routes::data::ndjson_stream` materialises a `Vec<u8>` before chunking (`crates/basin-rest/src/routes/data.rs:164-198`) — exact bytes known. | Future-cost. Add `http_egress_bytes_total: AtomicU64`; bump in `render_get_response` and `ndjson_stream`. Free on Fly→client today.                                          |
| 16 | Realtime / SSE messages sent                                 | count             | ❌ Not measured. `crates/basin-realtime/src/sse.rs:286-330` streams broadcast events without per-project accounting.                        | If the cloud ever bills realtime separately, add `realtime_events_total`. Today this is folded into compute-seconds via the SSE handler's tokio task.                        |
| 17 | Cache hit-rate inputs to `READ_CACHE_HIT_RATE` (`model.py:92`) | ratio            | ⚠️ `PageCacheCounters` exists (`page_cache.rs:122-138`) but is process-wide, not per-project; `DiskCacheCounters` similarly (`disk_cache.rs:303-311`). | Not strictly billing-critical — the constant is a modeling assumption, not a charge. But to *validate* the model's 90 % assumption we need per-project hit/miss snapshots.   |
| 18 | Free-tier active-project cap (`FREE_MAX_ACTIVE_PROJECTS=25`, `scenarios.py:28`) | count | ⚠️ Project lifecycle lives in basin-cloud catalog (`real_data.py:140-142`). Engine doesn't know "active" vs "paused".                       | Cloud-side check (proposed_pricing.md:290-296 already says "SELECT COUNT(*) … status != 'paused'"). No engine meter needed.                                                  |

Legend: ✅ exported per project · ⚠️ exists internally but not per-project / not in `ProjectCounters` · ❌ not measured

---

## 3. Per-hole code paths (file:line + the additive bump call to add)

Every fix below is **additive** — none mutates an existing counter or
breaks ABI. Implementation order matches §6 sizing.

### Hole #7 (and #6, #12) — CPU-seconds — ✅ CLOSED (2026-05-21)

Implementation as landed:
- New counter `cpu_micros_total: AtomicU64` on `ProjectCounters`
  (`crates/basin-common/src/telemetry.rs`) plus `record_cpu_micros(u64)`
  method, mirrored to `ProjectCountersSnapshot.cpu_micros_total`.
- Engine bump point: `crates/basin-engine/src/lib.rs` — right after
  `record_latency_ms(elapsed_ms)`, bumps with
  `started.elapsed().as_micros()`.
- WASM fn bump point: `crates/basin-fn/src/harness.rs` —
  `ComponentHarness::run_with` measures wall-clock around
  `invoke_with_caps` and, when an optional `ProjectCounterRegistry` is
  attached via `ComponentHarness::attach_project_counters`, bumps
  `cpu_micros_total` for the invoking project. The bump fires even on
  guest error/trap because a trapping guest still consumed CPU.

Still open as a defence-in-depth follow-up (NOT a billing blocker):
- Compaction bump point: `crates/basin-shard/src/in_process.rs`
  (`compact_one`). Background CPU is a real margin leak today but is
  not directly billed against `COMPUTE_OVERAGE_USD_PER_CPU_SECOND` —
  it's amortised into the fixed-fee SKU. Tracked separately as a
  v0.2 attribution-accuracy item.

### Holes #3, #4 — Class-A / Class-B op counts

- New counters: `class_a_ops_total` and `class_b_ops_total` on `ProjectCounters`
  (`telemetry.rs:78-84`).
- Bump sites in `ProjectScopedStore`
  (`crates/basin-storage/src/concurrency.rs`):
  - `put_opts` (line 90), `put_multipart_opts` (line 105) → Class-A
  - `get_opts` (line 122), `list` (line 145), `list_with_delimiter` (line 170), `copy_opts` (line 179) → Class-B (or its own LIST class)
- `ProjectScopedStore` already holds `self.project: ProjectId`
  (`concurrency.rs:78`) — the registry needs to be plumbed in via the same
  `attach_project_counters` channel `Storage` uses (`lib.rs:408`).

### Hole #1, #2 — Storage GB-month integration

- This is a **cloud-side**, not engine-side, job. The engine already exposes
  the live gauge via `ProjectCounterRegistry::snapshot`
  (`telemetry.rs:225-231`). A new daemon in basin-cloud should:
  1. Tick every N minutes (5-min default; bill resolution is monthly).
  2. Call `Engine::project_counters(project)`
     (`crates/basin-engine/src/lib.rs:357`) for every active project.
  3. Multiply `bytes_written_total` by the tick duration; accumulate into
     basin-cloud's `usage_ledger` table.
- No engine code change required for this hole. Document in
  `basin-cloud/billing_model/README.md` as the consumer contract.

### Hole #5, #15 — Egress bytes (future-cost)

- Object-store egress: `concurrency.rs:122` (`get_opts`) — `GetResult` has
  `meta.size` accessible before stream consumption.
- HTTP egress to client: `crates/basin-rest/src/routes/data.rs:158-160`
  (`estimate_bytes`) already computes wire size. Bump there:
  `tc.record_http_egress_bytes(estimate_bytes(&rows_value) as u64);`
- For `ndjson_stream` (`data.rs:164-198`): bump after `buf` is sized
  (`buf.len() as u64`).

### Hole #8 — Connection-seconds

- New counter: `conn_micros_total: AtomicU64` on `ProjectCounters`.
- Bump site: `crates/basin-pool/src/pooled_session.rs` `Drop` impl — record
  `checkout_at.elapsed()` against the project's counter. The pool already
  has `project_id` per session (`crates/basin-pool/src/stats.rs:39-41`).

### Hole #10 — Hottier resident bytes

- The byte counter exists at `crates/basin-hottier/src/registry.rs:34-36`
  inside `MemTableEntry`. Mirror it to `ProjectCounters` by adding a
  `set_hottier_resident_bytes(u64)` (gauge-style) and calling it from
  `MemTableRegistry::try_reserve_bytes` / `release_bytes`.

---

## 4. Orphaned counters (engine exports, model doesn't use)

These are exported by `ProjectCounters` today but **not consumed by any
cost line in `basin-cloud/billing_model/`**. All are fine to keep — they're
operational telemetry — but flagged for completeness.

| Counter                                | Defined at                                        | Used by cloud model? |
|----------------------------------------|---------------------------------------------------|----------------------|
| `ops_total`                            | `telemetry.rs:79, 116-118`                        | No — it conflates SQL and WAL appends (`basin-wal/src/lib.rs:393`, `basin-engine/src/lib.rs:645`). Cloud has `requests_per_day` as a model input, not a billable. |
| `errors_total`                         | `telemetry.rs:82, 145-147`                        | No. Useful for SLO dashboards.                                                                                                                            |
| `latency_p99_ms_estimate`              | `telemetry.rs:88-89, 149-157, 162-170`            | No. p99 is an SLO signal; not a cost line. (The model's `READ_CACHE_HIT_RATE` is a separate assumption.)                                                  |
| `PoolStats::{hits, misses, evictions}` | `crates/basin-pool/src/stats.rs:10-13, 47-49`     | No. Useful for pool-tuning, not billing.                                                                                                                  |
| `PageCacheCountersSnapshot`            | `crates/basin-storage/src/page_cache.rs:122-138`  | No (process-wide, not per-project anyway).                                                                                                                |
| `DiskCacheCounters`                    | `crates/basin-storage/src/disk_cache.rs:303-311`  | No (process-wide).                                                                                                                                        |
| `basin-webhooks` per-tenant latency    | `crates/basin-webhooks/src/telemetry.rs:127, 263` | No (separate subsystem; webhooks aren't a billed SKU today).                                                                                              |

---

## 5. Future-cost flags (modeled as $0, won't be in multi-region)

`basin-cloud/billing_model/model.py:21-22` carries the comment:
> Tigris-to-Fly egress is $0 within Fly's network.

This is **only true today** because basin-cloud is single-region (PRG).
When the cloud goes multi-region the following lines become non-zero:

1. **Tigris cross-region replication egress.** Tigris bills `$0.02/GB`
   replication between regions. Not modeled at all
   (`model.py:23-25` has no replication line). Engine must have
   `egress_bytes_total` ready before the first secondary region ships —
   adding it after the fact means losing 1 month of attribution.

2. **Fly cross-machine egress.** Fly bills `$0.02/GB` for region-to-region
   egress. The engine's `Engine::route_to_leader` /
   `basin-shard::follower` paths would carry this. Today both are zero
   because every shard is on the same machine — but the `basin-shard`
   follower API (`crates/basin-shard/src/follower.rs`) is built precisely
   for the future where they aren't.

3. **R2/Tigris API to non-Fly clients.** HTTP egress to customer browsers /
   SDKs is `$0` on Fly's outbound bandwidth allowance (until the org-wide
   monthly cap). Past that it's `$0.02/GB`. The model treats this as $0
   (no `OVERAGE_USD_PER_GB_EGRESS` constant) — hole #15 must be plumbed
   before crossing Fly's allowance.

4. **Realtime fanout multiplier.** SSE / WebSocket fanout
   (`crates/basin-realtime/src/sse.rs:286-330`) multiplies one DB write
   into N connected-client emits. Today billed via compute-seconds only;
   in multi-region this turns into bytes-on-the-wire AND cross-region
   broker hops. Hole #16 should land before this becomes a cost line.

**Bottom line:** four egress flavors are modeled as $0 today and will
break the unit economics the day multi-region ships. The corresponding
engine meters (#5, #15, #16) are 1-day fixes; deferring them costs us
1 month of bill-vs-model reconciliation when they go live.

---

## 6. Sizing

### 1-day fixes (1 engineer, half-day each)

- **#3, #4** — Class-A / Class-B op counters in `ProjectScopedStore`.
  ✅ DONE (2026-05-21). Two new `AtomicU64`s in `ProjectCounters` +
  bump sites in `crates/basin-storage/src/concurrency.rs` covering
  put_opts, put_multipart_opts, get_opts, list, list_with_delimiter,
  copy_opts, delete_stream. Cross-project isolation covered by
  `concurrency::tests::cross_project_isolation_class_a_b`.
- **#6, #7** — CPU-seconds. ✅ DONE (2026-05-21). One bump after
  `record_latency_ms` in `crates/basin-engine/src/lib.rs`, plus the new
  `cpu_micros_total` field + `record_cpu_micros` method on
  `ProjectCounters`. Test
  `basin_engine::tests::cpu_micros_total_aggregates_per_project`.
- **#12** — Function CPU-seconds. ✅ DONE (2026-05-21). Bump in
  `ComponentHarness::run_with` in `crates/basin-fn/src/harness.rs`
  (wrap `invoke_with_caps` in `Instant::now()` and call
  `counters.for_project(&project).record_cpu_micros(...)` on return).
  Test
  `governance_caps_test::component_harness_run_with_bumps_cpu_micros_per_project`.
- **#5, #15** — Egress byte counters (future-cost insurance). Three
  bump sites total (concurrency.rs + data.rs render path).

### 1-week fixes (design + tests)

- **#8** — Connection-seconds. Needs the pool's `Drop`-time accumulator
  + a test that simulates checkout/checkin. Easy on its own; the week
  buys regression coverage for the pool's eviction interaction.
- **#10** — Hottier resident bytes mirror. Straightforward, but the
  semantics of "live" bytes overlap with `bytes_written_total` and need a
  written invariant + tests so the cloud aggregator doesn't double-count.
- **#11** — Compaction CPU. Needs a separate `compaction_cpu_seconds`
  bucket so background work is attributable but distinguishable from
  query CPU; otherwise the SKU's gross margin story (`proposed_pricing.md:101-122`) gets muddled.

### New collector infra (~2 weeks, basin-cloud not basin)

- **#1, #2** — Storage-bytes-time integration. The engine side is done;
  basin-cloud needs:
  1. A sampler daemon that polls `Engine::project_counters` on a fixed
     cadence (proposed 5 min) and writes to a new `usage_samples` table.
  2. A monthly-rollup job that integrates samples into GB-months.
  3. Idempotent late-arrival handling (engine restarts reset gauges).
  4. Schema migration: extend `quota` (currently `point-in-time current_value`)
     into a time-series surface or add a new `usage_ledger` table.

### Total sizing

- **Engine-side changes to unblock the 2026-05-21 reprice:** ~5 engineer-days
  (#3, #4, #6, #7, #12 — all 1-day fixes).
- **Engine-side coverage for the *full* model:** add ~5 more days for #8,
  #10, #11.
- **basin-cloud collector to actually invoice:** ~2 weeks for the sampler
  + rollup + schema work in §6.3.
- **Multi-region readiness (egress meters):** included in the 1-day
  bucket above; sequencing matters more than effort.

**Net:** 2 engineer-weeks of basin work + 2 engineer-weeks of basin-cloud
work to fully back the new overage SKUs. The 5-day engine subset is the
true unblocker for Phase 4 / T-104 billing.
