---
title: "Noisy-neighbor / fairness audit — single-instance and load-balanced"
nav_section: meta
sidebar_position: 62
summary: "Read-only fairness audit: how one hot project degrades its neighbours on a single instance and behind a load balancer, and where the accounting gaps are."
date: 2026-05-21
scope: read-only
related:
  - docs/audits/2026-05-21-billing-meter-gap.md
  - docs/scaling/read-replicas.md
  - docs/scaling/shard-rebalance.md
---

# Noisy-Neighbor & Multi-Project Fairness Audit

Wedge invariant under test: *one project's behaviour cannot take down another*.
This audit walks every per-project resource axis and probes whether (a) a single
hostile/buggy project on one server can degrade others, and (b) when N
load-balanced replicas share per-project state, hot-project pinning or
cross-replica cap evasion break the invariant.

All findings are read-only — no source modified.

---

## 1. Executive summary

### Top 3 single-instance fairness gaps

1. **`BASIN_QUERY_COST_LIMIT_ROWS` is opt-in & disabled by default**
   (`crates/basin-engine/src/cost_check.rs:42-47`, `:59-63`). With the default
   the planner accepts unbounded `SELECT * FROM huge_table`. There is **no
   per-query wall-clock or CPU timeout** on the executor (`grep statement_timeout`
   → nothing); the only executor-side cap is the *planning-time row estimate*
   for single-table SELECTs. Multi-FROM / JOIN / sub-query / CTE shapes
   `return Ok(None)` (`cost_check.rs:78,82,88,93,99,103,110`) and pass
   *unchecked*. A project can monopolise a DataFusion executor thread with a
   cartesian self-join for as long as it takes — every other project sharing
   the runtime starves. **P0.**

2. **Catalog Postgres is a single `Mutex<Client>`, not a pool**
   (`crates/basin-catalog/src/postgres.rs:57-60`). Every DDL, every
   `load_table`, every `next_event_seq`, the per-query cost-check
   `estimate_query_rows`, and the `cost_check` round-trip itself
   (`executor.rs:1144-1149`) serialises through this one mutex. One slow
   `ALTER TABLE` from project A holds the lock for the duration of the PG
   round-trip and stalls *every* read-path catalog hit for every other project.
   Comment at `postgres.rs:54-56` admits this and defers the pool. **P0.**

3. **Pgwire connection limiter has no default ceiling**
   (`crates/basin-router/src/connection_limit.rs:67-75`). The only built-in
   `ConnectionLimitProvider` is `NoConnectionLimit` which always returns
   `None` ("unlimited"). OSS / self-hosted projects get no cap. A
   `BASIN_PGWIRE_RATE_LIMIT_QPS=0` (the default,
   `crates/basin-router/src/rate_limit.rs:112-114`) leaves QPS unlimited too.
   One project can open 10k pgwire sockets and DOS the accept loop / file
   descriptors. **P0.**

### Top 3 multi-instance (load-balanced) gaps

1. **Hot-tier memtable is single-writer per `(project, table)` and not
   replicated across replicas.** `crates/basin-hottier/src/registry.rs:149`
   keys on `(ProjectId, TableName)`; HTAP write-path lands in the memtable of
   the shard owner that gets routed. Combined with `basin-router`'s
   `ShardMap` (`crates/basin-router/src/sharding.rs:46-94`) which always
   picks `endpoints[hash(project) % N]`, *all* writes for one project go to
   one process — a write-heavy whale pins one replica at 100% CPU while
   sibling replicas sit cold. Whale-pinning (`sharding.rs` "Whale pinning")
   exists but is a manual operator escape hatch, not a balancer. **P0.**

2. **Per-project caps are local — a project can multiply their budget by N
   replicas.** Every per-project counter (memtable bytes
   `crates/basin-hottier/src/registry.rs:111-115`, realtime BUFFER_FULL
   `crates/basin-realtime/src/budget.rs:160-186`, REST QPS
   `crates/basin-rest/src/server.rs:40,56,67`, pgwire QPS
   `crates/basin-router/src/rate_limit.rs:44`, wasm semaphore
   `crates/basin-fn/src/governance.rs:232,302-309`, basin-net outbound
   `crates/basin-net/src/guards.rs:150-178`, connection count
   `crates/basin-router/src/connection_limit.rs:93-95`) is **per-process,
   `DashMap` / `Arc<...>` keyed on `ProjectId`**. There is no
   cross-replica coordinator. A project whose REST QPS cap is 100/sec can
   round-robin across 4 replicas and sustain 400/sec. **P0** for any cap
   that influences billing or pricing tiers; cosmetic for the others.

3. **Catalog Postgres is shared across all replicas.** Every replica connects
   to the same `tokio_postgres::Client` mutex pattern (one per replica). A
   noisy project pushing DDL or `SELECT ... cost_check` adds load directly to
   the shared PG that everyone uses. Plus, since the mutex is per-replica,
   there's no global concurrency bound on Postgres connections from the
   fleet — N replicas × the underlying tokio_postgres concurrency means PG
   itself becomes the bottleneck under fanout DDL. **P1.**

---

## 2. Per-axis matrix

Legend: ✅ enforced, ⚠️ partial / advisory / opt-in, ❌ no enforcement, n/a not
applicable.

| # | Axis | Limiter file:line | Default | Unit | Scales w/ projects? | Single-instance | Multi-instance | Gap |
|---|------|-------------------|---------|------|--------------------|-----------------|----------------|-----|
| 1 | **Pgwire QPS / project** | `crates/basin-router/src/rate_limit.rs:36-39, 56-92` | 100/s sustained, 300 burst — **disabled** when `BASIN_PGWIRE_RATE_LIMIT_QPS=0` (default, `:112-114`) | per-`ProjectId` token bucket via `governor::keyed::DefaultKeyedStateStore` | O(active projects) — one bucket per project (lazy) | ⚠️ opt-in | ❌ per-replica | Default = off; per-replica buckets, no global aggregation |
| 1 | **Pgwire concurrent connections / project** | `crates/basin-router/src/connection_limit.rs:67-75, 218-242` | `NoConnectionLimit` → **unlimited** | per-`ProjectId` `AtomicU32` (`:93-95`) | O(active projects) | ❌ no default cap; provider is a stub | ❌ per-replica | OSS-only `NoConnectionLimit` provider; no cloud `ConnectionLimitProvider` shipped (docstring `:21-39`) |
| 1 | **Pgwire per-IP rate** | (none in router) — `crates/basin-router/src/lib.rs:343-377` captures `peer: SocketAddr` for tracing only | n/a | n/a | n/a | ❌ not present | ❌ not present | An attacker can spray from one IP without any IP-level throttle at pgwire |
| 2 | **REST QPS / project** | `crates/basin-rest/src/server.rs:40, 56, 67, 290-295`; `crates/basin-rest/src/lib.rs:132, 151` | 100/s × 60 = 6000/min sustained, **per-project** | per-`ProjectId.to_string()` via `basin_auth::rate_limit::PerKey::per_minute` | O(active projects) | ✅ on-by-default | ❌ per-replica | Per-replica only; N replicas × 100/s = N × cap |
| 2 | **REST body cap** | `crates/basin-rest/src/server.rs:90, 195`; `crates/basin-rest/src/lib.rs:124, 147` | 1 MiB | per-request (axum `DefaultBodyLimit`) | n/a | ✅ | ✅ stateless | OK |
| 2 | **REST per-IP / per-endpoint** | (none in basin-rest) | n/a | n/a | n/a | ❌ not present | ❌ not present | Auth-flow IP limiter exists (`crates/basin-auth/src/flows/signin.rs:27`) but only for `signup/signin/reset/magic`; data routes (`/rest/v1/:table`) have no IP throttle |
| 2 | **Auth flow IP/email rate** | `crates/basin-auth/src/lib.rs:141-142, 246-247, 254-255`; uses `crates/basin-auth/src/rate_limit.rs:25-30` | `rate_limit_per_ip_per_min`: caller-supplied; tests use 1000 (`crates/basin-auth/src/config.rs:60`, `:195`) | per-key (`PerKey`) string `signin:{project}` etc. | O(unique IPs × projects) — same as PerKey | ⚠️ requires explicit config | ❌ per-replica | Cap is **per-replica**, key is a `String` |
| 3 | **Query row-cost cap** | `crates/basin-engine/src/cost_check.rs:42-47, 71-117, 123-132`; `crates/basin-engine/src/executor.rs:1142-1153` | unset → **disabled** (`:59-63`) | per-query rows (planning estimate, single-table SELECT only) | n/a | ⚠️ opt-in & narrow | ⚠️ opt-in & narrow | JOIN / sub-query / CTE / DML pass unchecked (`cost_check.rs:78,82,88,93,99,110`); **no wall-clock or CPU timeout** anywhere in executor |
| 3 | **Statement timeout** | (none) — `grep statement_timeout query_timeout` → no matches | n/a | n/a | n/a | ❌ not present | ❌ not present | A project can hold a DataFusion worker indefinitely; full SAT-shape join, recursive CTE, ill-bounded window aggregate |
| 4 | **HTAP project memtable budget** | `crates/basin-hottier/src/budget.rs:27-50, 80-108`; `crates/basin-hottier/src/registry.rs:111-128, 211-261` | hard cap 256 MiB, soft cap 192 MiB, table soft 16 MiB (`budget.rs:27-44`) | per-`ProjectId` `AtomicU64` + `Arc<Semaphore>` (`registry.rs:111-128`) | O(active projects) | ✅ on-by-default; CAS-correct (`registry.rs:220-238`) | ⚠️ per-process — see #4 below | Bursting project correctly returns `HardCapReached` (`registry.rs:216-218`); but the **global** scheduler (`budget.rs:228-312`) is also stateless per-process — no cross-replica view |
| 4 | **HTAP global pressure** | `crates/basin-hottier/src/budget.rs:200, 228-312` | 4 GiB total across all projects | shared scheduler — `pick_flush_candidates` (`:279-303`) | O(active projects) at scheduler invocation | ✅ largest-first works | ⚠️ per-process | Replicas don't see each other's totals; under skew one replica may flush aggressively while another sits below threshold |
| 5 | **Realtime per-project budget** | `crates/basin-realtime/src/budget.rs:52, 86-135, 160-186, 197-213` | 16 MiB in-flight per project (`:52`) | per-`ProjectId` `AtomicU64` in `DashMap` (`:160-186`); single shared broadcast `ChannelRegistry` (`crates/basin-realtime/src/lib.rs:97-100`) | O(active projects) | ✅ CAS-correct (`:98-116`); BUFFER_FULL → durable retry-log per ADR | ❌ per-replica | Cap is **per-replica**; a project publishing through N replicas multiplies their cap by N |
| 5 | **Realtime broadcast ring** | `crates/basin-realtime/src/lib.rs:64, 113-114` | 1024 events / `(project, table)` `tokio::sync::broadcast` channel | per-`(project, table)` channel — shared across subscribers | O(active `(project, table)` pairs) | ⚠️ noisy publisher fills *their own* ring, lagged subs get `RecvError::Lagged` (correct isolation); but the **DashMap entry** is never evicted unless `prune()` is called (`:159-167`) | ⚠️ per-replica | Channels persist forever; a project creating 10k tables leaks 10k entries per replica until `prune()` runs. Comment says "TODO(R4): wire into basin-presence so presence signals trigger prune" — not wired today |
| 5 | **Presence per-project** | `crates/basin-realtime/src/presence.rs:71-87` | heartbeat_ttl 90s, eviction 30s | per-`(project, channel)` map (lazy) | O(active channels) — **no per-project max** | ❌ no per-project cap on # of presence entries; a project tracking 100k clients in one channel grows unbounded until heartbeat-evicted | ❌ per-replica | grep `max_subscriptions max_channels MAX_PER_PROJECT MAX_CLIENTS max_clients` in `basin-realtime/` → no matches |
| 6 | **Wasm functions concurrency** | `crates/basin-fn/src/governance.rs:14, 52-54, 232, 302-309, 343-379` | 16 concurrent invocations per project (`DEFAULT_PROJECT_CONCURRENCY`) | per-`ProjectId` `Arc<Semaphore>` in `DashMap` | O(active projects) | ✅ semaphore enforces fair acquire | ❌ per-replica | Cap is per-process; replicate ×N |
| 6 | **Wasm CPU / memory / wall** | `crates/basin-fn/src/governance.rs:44-50, 67-78, 140-179, 207-211, 343-379` | 50 epoch ticks (~5 s), 64 MiB, 10 s wall | per-`Store` (epoch deadline + ResourceLimiter + `tokio::time::timeout`) | n/a | ✅ all three caps; memory cap is engine-wide trap not per-store (`:202-211`) | ✅ stateless | OK. **However**: wasm runs on `tokio::task::spawn_blocking` (`:359`) — the blocking pool is shared across *all* projects; 16 × N projects exceeding blocking pool size starves wasm globally. See wasm audit (cross-ref §6) |
| 7 | **Webhooks retry queue / backlog** | `crates/basin-webhooks/src/queue.rs:1-15, 117-123, 168-202`; `crates/basin-webhooks/src/worker.rs:314-323`; `crates/basin-webhooks/src/config.rs:25-32` | unbounded; `auto_pause_after` 24h; `worker_tick` 250ms; `max_retries` = `sub.max_retries` (per-subscription, no global cap visible at `:314-317`) | **one shared `queue.jsonl`** for the whole process (`queue.rs:5-12`), one in-memory `VecDeque`; one worker per process (`worker.rs:17-22`) | shared — flat file grows with total event volume | ❌ no per-project backlog cap; a project generating 1M failing webhooks fills the disk and starves the single worker tick-loop for everyone | ❌ shared per replica | Per-project fairness is delegated to `basin-net`'s 10/s token bucket only. The queue file and `VecDeque` are shared. Worker comment `worker.rs:17-22` admits this is by design |
| 8 | **Storage / object byte-counter** | `crates/basin-blob/src/store.rs:264-302, 352-381`; `crates/basin-common/src/telemetry.rs:78-145, 191-225` | none — counter only, no enforcement | per-`ProjectId` `AtomicU64` (`telemetry.rs:80,194-195`) | O(active projects) | ⚠️ accounting only — `put_object` bumps `bytes_written_total`, **no quota check** | ❌ per-replica counter; no global aggregation; no rate cap at all | A project uploading 100 GiB/sec only trips object-store rate-limits (S3-side), not anything in Basin |
| 8 | **Storage per-bucket size limit** | `crates/basin-blob/src/store.rs:367-373`; `crates/basin-blob/src/model.rs:105-106, 148-151` | `file_size_limit: None` (unlimited) per bucket | per-object | n/a | ⚠️ opt-in per bucket | ✅ stateless check | Operator must set; default is unlimited |
| 8 | **REST upload body cap** | `crates/basin-rest/src/lib.rs:147` (1 MiB default `max_body_bytes`) | 1 MiB | per-request | n/a | ✅ but tight — applies to *all* routes incl. `upload_object`; multipart >1 MiB rejected at proxy layer | ✅ stateless | A 1 MiB cap on storage is restrictive; if operator raises it for `/storage/v1/object/...` there's no per-route override (`server.rs:90, 195` is global) |
| 9 | **WAL append rate** | `crates/basin-wal/src/file_wal.rs:42, 57, 67-70, 111-129` | shared object_store; per-`(project, PartitionKey)` `Arc<Mutex<PartitionState>>`; one `Inner` per process (`:62-70`); shared flush task (`:75-78, 100-105`) | per-`(project, partition)` mutex for buffer; flush task is single per process | O(active partitions) | ⚠️ writers in different partitions parallelise (`:65-68` comment); but **the flush task is one tokio task that flushes serially** (`:131-141, 144-150`) — a project with 1000 partitions all dirty makes every other project's flush wait | ⚠️ per-replica | Plus: object_store backing (S3) is shared per replica; SDK throttles apply globally |
| 10 | **Compactor** | `crates/basin-shard/src/lib.rs:50-51, 73-74`; `crates/basin-shard/src/in_process.rs:684-741` | 30s interval; **single background loop per process** | shared per-process loop (`in_process.rs:718-741`) | n/a — work is FIFO over project map | ⚠️ A project with 10k tables makes one tick-pass take longer; quiet projects' WAL → Parquet drain delayed proportionally | ⚠️ per-replica | No per-project quota; no concurrent compaction lanes |
| 10 | **Flush task (hot-tier)** | `crates/basin-hottier/src/flush.rs:269`; `crates/basin-shard/src/in_process.rs:684-720` | per-`ShardConfig.flush_tick_interval` (5s); **one task per process** | shared per-process | n/a | ⚠️ same shape as compactor | ⚠️ per-replica | Largest-first scheduling helps but still serial |
| 11 | **Catalog Postgres connections** | `crates/basin-catalog/src/postgres.rs:57-91` | **one `tokio_postgres::Client` per `PostgresCatalog`**, behind `Mutex<Client>` (`:58`) | shared per process | n/a — one client total | ❌ single mutex serialises every multi-statement transaction; per-query cost-check adds one round-trip per simple-query (`executor.rs:1144-1149`) | ❌ each replica has its own single client; no fleet pool sizing | Docstring `:50-56` admits "For read-heavy workloads we could split into a pool later." A project doing 1000 DDLs/min holds the lock 1000× ; everyone else stalls behind |
| 12 | **OTLP export buffer** | `crates/basin-engine/src/query_stats_export.rs:109-122, 144-190` | 15s tick (`:117-122`); emits `tracing::info!` per row | shared per-process exporter task; per-row work `O(distinct (shape, project, table))` | O(active query shapes) | ⚠️ A project generating millions of unique shape_hashes (e.g. by injecting literals — see `query_stats.rs` for shape hashing) inflates the registry; per-tick walk grows; one project can starve the export thread's CPU budget | ⚠️ per-replica | Each replica exports its own snapshot; collector receives N× the volume |
| 13 | **basin-cron schedules** | `crates/basin-cron/src/runner.rs:104-117, 271-316` | `PER_MINUTE_RATE_LIMIT = 60` runs/project/60s sliding window (`:117`); **single tick-loop per process** (`:170-186`) | per-`ProjectId` `VecDeque<DateTime>` (`:111-112`) | O(active projects) | ✅ rate-limit at 60/min/project; ❌ no cap on number of schedules per project — project can `cron.schedule` 10k jobs, each tick walks them all (`:184-186` then `tick_project`) | ❌ per-replica | grep `max_jobs max_schedules max_cron` in `basin-cron/` → no matches. Plus: tick loop calls `engine.exec` for each due job sequentially (`:318-326`) — a slow job blocks the next project's job for the same tick |
| 14 | **basin-net outbound HTTP** | `crates/basin-net/src/guards.rs:141-194` | 10 req/s sustained, 30 burst per project (`:163-166`); 10 MiB body cap (`:47`); 30 s timeout (`:48-49`) | per-`ProjectId` token bucket via `governor::keyed`; allowlist per-project in `RwLock<HashMap<ProjectId, HashSet<String>>>` (`:78-82`) | O(active projects × hostnames) | ✅ rate, body, timeout all enforced; ✅ allowlist defaults to DENY-ALL (`crates/basin-net/src/lib.rs:49-53`) | ❌ per-replica | A project calling external API can do 10/s × N replicas. Allowlist edit is async + per-process (`:88-103`) — write on replica-A is not seen on replica-B until reconciled |

### Axes that exist but are not in the original 14

- **Per-IP rate limit at pgwire** — **not present** in router. Only the
  auth-flow PerKey on REST has per-IP shape, and it's `signin:{project}` /
  `signup:{project}` etc., never a raw IP.
- **Per-route REST body cap** — only one global `DefaultBodyLimit`
  (`crates/basin-rest/src/server.rs:90, 195`). Tightening `/storage/upload`
  while loosening `/rest/v1/:table` would require splitting the router.
- **OTLP cardinality budget** — none. A project generating unbounded
  `shape_hash` values (e.g. literals not parameterised) inflates the
  registry without bound.

---

## 3. Adversarial scenarios

| # | Scenario | What breaks | Blast radius | Suggested fix |
|---|----------|-------------|--------------|---------------|
| A | Project runs `SELECT * FROM huge_table CROSS JOIN huge_table` | `cost_check` returns `None` for JOIN (`crates/basin-engine/src/cost_check.rs:88-93`); query runs to completion or OOM; no wall-clock timeout | All projects on same process — DataFusion worker tied up, memory pressure global | (i) Add statement-level wall-clock timeout in `executor.rs` around `df.collect()` and the spawn_blocking branch (`:4541-4565`); (ii) extend `cost_check` to estimate joins via column-cardinality stats |
| B | Project opens 10k pgwire sockets | `ConnectionLimiter::unlimited()` admits all (`crates/basin-router/src/connection_limit.rs:228-232`); accept-loop spawns 10k tasks, FDs exhaust | Whole process — pgwire dies; REST may survive | Wire a default ConnectionLimitProvider that reads `BASIN_PGWIRE_MAX_CONNS_PER_PROJECT` env or a project-config table; default 200 |
| C | Project does 100k INSERTs/s to one table | Memtable hard cap fires (`crates/basin-hottier/src/registry.rs:216-218`) → `HardCapReached`; subsequent writes return error. ✅ Other projects OK on memory. **But** the WAL flush task is shared (`crates/basin-wal/src/file_wal.rs:131-141`) — quiet projects' flushes queue behind | All projects' WAL durability latency degraded | Bound flush concurrency per project / add per-partition flush parallelism |
| D | Project publishes 1M realtime events/sec | `BudgetTracker::try_reserve` returns `BufferFull` cleanly (`crates/basin-realtime/src/budget.rs:98-116`); events routed to webhook retry log per ADR | **Realtime ring OK** — but webhook queue is shared `queue.jsonl` (`crates/basin-webhooks/src/queue.rs:5-12`); failed deliveries fill the queue and the single worker (`crates/basin-webhooks/src/worker.rs:78-89`) ticks through all 1M before getting to anyone else's event | Webhook delivery for **all** projects delayed; disk fills | Per-project queue partition; per-project backlog cap; multi-worker pool |
| E | Project creates 10k cron jobs | `register_project` → tick walks all 10k each minute (`crates/basin-cron/src/runner.rs:170-186, 213-260`); per-tick lock held on `last_tick` map (`:189-195`) | All projects — tick-loop saturated; due jobs for other projects run late or skipped | Add per-project schedule count cap; parallelise tick across projects |
| F | Project pushes catalog-heavy DDL | `Mutex<Client>` (`crates/basin-catalog/src/postgres.rs:58`) serialises every DDL; cost-check `estimate_query_rows` does one PG round-trip per read query (`executor.rs:1142-1153`) | All projects — every read-path catalog hit waits | Pool the PG client (`deadpool-postgres`); split read-only path onto separate replicas |
| G | Project invokes 100k WASM fns/s | Per-project semaphore caps at 16 in-flight (`crates/basin-fn/src/governance.rs:343-353`); excess wait on `acquire_owned` | Project's queue grows unbounded (no admission cap); tokio blocking-pool starvation across all projects (wasm runs on `spawn_blocking`, `:359`) | Add per-project pending-invocation cap; size blocking-pool with budget per project |
| H | Project uploads 10 GiB blobs in a loop | REST `DefaultBodyLimit` = 1 MiB rejects (`crates/basin-rest/src/server.rs:90`); ✅ blocked. **But** if operator raises body cap to support large blobs there's no per-project upload-rate or upload-byte cap | Project fills object store; bill explodes; no upload-rate throttle exists for blob path | Add per-project `bytes_written_per_sec` token bucket parallel to basin-net's |
| I | Project generates query shapes with literals not parameterised | `QueryStatRegistry` (`crates/basin-engine/src/query_stats_export.rs`) shape_hash diverges per literal; registry grows unbounded; OTLP export tick takes longer per cycle | All projects — exporter thread busy, log volume explodes | Cap distinct shapes per project; LRU eviction in registry |
| J | Project flips allowlist 1000×/sec | `AllowList::allow` takes `RwLock::write` per call (`crates/basin-net/src/guards.rs:88-94`); writers block readers (other projects' HTTP calls stall waiting to look up *their* allowlist) | All outbound HTTP requests stall briefly | Shard the allowlist map per project (DashMap), or copy-on-write |
| K | Project opens many WS subscriptions (10k tables × 10k clients) | `ChannelRegistry.channels` DashMap entries persist forever; `prune()` not auto-called (`crates/basin-realtime/src/lib.rs:159-167`); presence map has no per-project cap (`crates/basin-realtime/src/presence.rs`) | Memory growth; iteration cost in subscribe path | Per-project channel count cap; presence per-project cap; auto-prune timer |

---

## 4. Multi-instance hot-project pinning analysis

### What pins (one project lives on one replica)
- **Pgwire connections / sessions**: `ShardMap::shard_for` is
  `hash(project) % N` (`crates/basin-router/src/sharding.rs:88-94`).
  *Every* pgwire connection for a project routes to the same shard owner.
  A whale on shard #3 pins shard #3.
- **HTAP memtable** for `(project, table)`:
  `crates/basin-hottier/src/registry.rs:149` — keyed `(ProjectId, TableName)`.
  Lives only on the shard owner that received writes; not replicated. Reads
  from a follower would need WAL replay (per `docs/scaling/read-replicas.md`
  §2.2) — follower path is design-only as of today.
- **Per-shard WAL `PartitionState`** mutex
  (`crates/basin-wal/src/file_wal.rs:57, 111-129`) lives on the writing
  shard.
- **Webhook retry queue position** — `queue.jsonl` lives on one node (the
  worker process); a project pinned to that node has retries serialised in
  that file.
- **Per-project rate-limit `governor` buckets** — `DashMap` per replica;
  state is local and does not migrate when a connection rebalances.

### What is replicated (all replicas hold the same)
- **Catalog rows** — every replica points at the same Postgres (single
  truth). ⚠️ This is the shared substrate; not really "replicated".
- **Object store contents** — single bucket; all replicas read/write the
  same keys.
- **Allowlist edits** — *not* replicated; per-replica `AllowList`
  (`crates/basin-net/src/guards.rs:78-82`) is in-memory only. The
  `_net_allowed_hosts` table is "Optional in v0.1" (`crates/basin-net/src/store.rs:32`). Cross-replica
  drift today.

### What is shared (one backing service everyone hits)
- Catalog Postgres (single connection per replica → shared DB).
- WAL object_store (S3 / LocalFS bucket).
- Storage object_store.
- OTLP collector.

### Coordinator hazards
- **Per-project byte cap arbitration** across replicas: **none**. The
  `GlobalPressureScheduler` (`crates/basin-hottier/src/budget.rs:228-312`) is
  per-process; replicas see only their own totals.
- **Cross-replica cap evasion**: a project whose REST cap is 100 qps can
  round-robin across the load-balancer to N replicas and sustain N × 100.
  Same for pgwire QPS, wasm concurrency, realtime budget, webhook rate.
- **Sticky-routing partial coverage**: `ShardMap` pins pgwire by project
  hash (good for state colocation), but REST/HTTP requests in
  `basin-rest` aren't shard-aware — they hit whichever replica the
  external load-balancer picks. Per-project counters in basin-rest are
  therefore strictly per-replica.

### Net result
- **Sticky-routed state (pgwire / hot tier / WAL)** correctly localises a
  whale's blast radius to one shard owner — but means the whale pins that
  one shard at 100% while others sit idle.
- **Stateless-routed paths (REST / webhooks / wasm via REST / blob)** let a
  project multiply any per-project budget by the replica count.
- The wedge invariant "one project can't take down another" is **partially
  satisfied**: noisy-project memory and queue bytes are *bounded* per
  project, but **wall-clock CPU and IO** on shared resources (catalog PG,
  WAL flush task, compactor, webhook worker, blocking pool) are not.

---

## 5. Concrete TODO list — ranked by blast-radius × likelihood

| Rank | Fix | Blast radius if unfixed | Likelihood | Effort |
|------|-----|-------------------------|------------|--------|
| **P0-1** | Add statement-level wall-clock timeout in `executor.rs:4541-4565` (both shard and non-shard paths) | All projects on same process; runaway query holds DataFusion thread + memory | high — easy to trigger accidentally with `JOIN` shapes that bypass `cost_check` | S |
| **P0-2** | Wire a default `ConnectionLimitProvider` (env-var or static config) so OSS deployments get a per-project cap (default 200) instead of `NoConnectionLimit` | All projects on the process; FD exhaustion, accept-loop saturation | high — trivial DOS | S |
| **P0-3** | Pool the catalog Postgres client (`deadpool-postgres` or `bb8`); split read-only path from txn path | All catalog reads stall behind any DDL on the single `Mutex<Client>` | high — every cost-check is one round-trip; high-DDL project blocks all reads | M |
| **P0-4** | Default-enable `BASIN_PGWIRE_RATE_LIMIT_QPS` (e.g. 100); same for cost-limit-rows | Unlimited QPS today; runaway query today | high | S |
| **P1-5** | Multi-instance budget aggregation for billing-impacting caps (memtable bytes, REST QPS): periodic snapshot to catalog; admission compares against fleet total | A project on a paid tier can exceed their cap N× by spraying across replicas | medium — observable in metrics; less likely to crash | L |
| **P1-6** | Per-project caps for webhooks: shard the `queue.jsonl` per project; cap backlog per project; multi-worker pool | Webhook delivery globally stalls behind one noisy project | medium | M |
| **P1-7** | Cron: per-project schedule-count cap + tick-loop parallelism across projects | Tick loop O(total schedules); slow job blocks others | medium | M |
| **P1-8** | Realtime: per-project channel-count cap; presence per-project entry cap; auto-prune timer | Memory leak; per-project unbounded | medium | S |
| **P1-9** | WAL flush task: parallelise across partitions; bound dirty partitions per project | One project with 1000 partitions stalls others' WAL durability | medium | M |
| **P1-10** | OTLP shape registry: per-project shape-cardinality cap + LRU eviction | Unbounded growth; export tick CPU explodes | medium | S |
| **P2-11** | basin-net allowlist: shard per project (DashMap) or copy-on-write to drop the global writer lock | Allowlist edit stalls other projects' HTTP calls briefly | low — narrow window | S |
| **P2-12** | Per-project blob upload-byte-rate token bucket (parallel to basin-net's) | Project bill explosion; storage IO saturation | medium | M |
| **P2-13** | Per-IP throttle at pgwire accept (before resolver runs) — protects against unauth'd scans | Unauth'd traffic can exhaust connection-startup work | low — auth still catches | S |
| **P2-14** | Compactor: per-project concurrency lane(s); cap on small-table compactions per tick | One project with 10k small tables monopolises | low — workload-specific | M |
| **P3-15** | Cross-replica allowlist consistency (catalog-backed) — per ADR's deferred work | Allowlist drift across replicas | low — operationally surprising | M |

---

## 6. Cross-reference with the wasm-functions perf audit

The wasm-functions audit covers per-invocation caps (CPU / memory / wall) and
the per-project semaphore (`crates/basin-fn/src/governance.rs:14, 343-379`).
The noisy-neighbor concerns specific to wasm that overlap:

1. **Per-project semaphore is per-replica** (axis #6 above) — multi-instance
   cap evasion: a project's 16-concurrent cap becomes N × 16 across N
   replicas.
2. **Wasm runs on `tokio::task::spawn_blocking`**
   (`crates/basin-fn/src/governance.rs:359`). The blocking pool is **shared
   across all projects** in the process. With N projects × 16 permits each
   = potentially 100s of concurrent blocking tasks; the tokio default
   blocking-pool size (512) can be exhausted by a few aggressive projects,
   starving wasm invocations for everyone else *and* starving any other
   `spawn_blocking` user (notably the executor's
   `executor.rs:4547-4559` shard-mode path).
3. **No per-project pending-invocation cap** — a project can queue
   unbounded acquires on the semaphore (`Semaphore::acquire_owned` waits
   indefinitely, `governance.rs:349-353`). Memory grows linearly with
   pending invocations; no `try_acquire` path that rejects when the queue
   is too deep.
4. **Engine-level memory cap** is per-Engine, not per-invocation
   (`governance.rs:202-211`, admitted in source). All invocations across
   all projects share one Engine; a single misbehaving wasm reserving the
   full 64 MiB will reduce headroom for everyone until the Store drops.
5. **Wall-clock timeout interrupts via epoch bump** (`governance.rs:364-378`)
   — works only if the wasm trap fires promptly. A guest looping in *host*
   code (any host call that itself awaits — net glue, engine queries) is
   not interrupted by the epoch bump. The wasm-fn audit should verify
   every host-side await has its own timeout.

See the wasm-functions audit for the per-invocation cap details; this
audit's contribution is the multi-instance dimension (#1-#3) and the shared
blocking-pool concern (#2).

---

## Total findings

- **14 per-axis evaluations** in §2; **11 adversarial scenarios** in §3;
  **15 ranked TODOs** in §5.
- **4 P0 single-instance gaps**: no statement timeout, no default
  connection cap, catalog-PG single mutex, default-disabled pgwire QPS.
- **3 P0 multi-instance gaps**: hot-tier memtable pins the shard,
  per-project caps multiply by N replicas, catalog PG shared without
  fleet-level pool sizing.
- **1 wedge-violating composite**: a project with high DDL + high JOIN
  workload combines (P0-1 + P0-3): catalog mutex held by DDL while a
  bypass-cost-check JOIN burns DataFusion thread — every other project's
  reads stall on both fronts simultaneously.
