---
title: "Phase 5.14 remaining + 5.16 — wave decomposition (2026-05-19)"
nav_section: meta
sidebar_position: 10
summary: "Five-wave execution plan for Phase 5.14 HTAP + Phase 5.16 Query Insights, decomposed into file-disjoint sonnet-agent-ready specs by an opus decomposer on 2026-05-19."
---

# Phase 5.14 remaining + 5.16 — wave decomposition (2026-05-19)

This document is the durable home of the opus-decomposer output dated
2026-05-19.  Every wave-item below has a TASK.md sub-item it pairs
with; this file gives the parallel-dispatch grouping and the rationale
for the inter-item dependencies.

## Status snapshot at 2026-05-19

**Shipped this cycle (12 commits on `main`):**

- `50b9306` 5.14.A2+A3 catalog blooms (writer + probe)
- `c5739a6` 5.14.A4 differential test + **CRITICAL fixed-seed fix** (closed 982/1000 false-negative bug)
- `fc00a41` 5.14.B3 APPROX_COUNT_DISTINCT UDF (inline HLL)
- `4935115` 5.14.B4 APPROX_PERCENTILE UDF (inline t-digest)
- `ee80b36` + `1b86751` 5.14.D3 catalog-aware WindowExec sort elision
- `caa43e6` 5.14.D1 query-history collector (executor records ORDER BY / GROUP BY column tuples)
- `cd5b626` FAST-AGG-GROUPBY (low-card GROUP BY fast path)
- `dc8cd96` per-table `basin.row_block_size` DDL
- `218a5c6` NULLIF analyzer rewrite
- `3c79680` IS DISTINCT FROM analyzer rewrite

**Locked decisions (ADR 0016 addendum + ADR 0017 + TASK.md updates 2026-05-19):**

1. HLL / t-digest live in a new `basin-sketch` crate (shared by `basin-storage` writer-side and `basin-engine` query-side).
2. Plan-shape hash uses `xxhash3` 64-bit, fixed seed `0xBA51_4145_7E11_5A95`.
3. Memtable schema evolution: DEFAULT-at-read for constant defaults; force-flush-required for non-constant.
4. Compaction multi-sort: user-declared `CLUSTER BY` always wins; `basin.adaptive_sort_override = true` per-table opt-in for override.
5. Cross-project aggregates: anonymised template (`t1.c0`); k-anonymity ≥ 5.

## Wave structure

| Wave | Items | Inter-wave dependency |
|---|---|---|
| **0** | basin-sketch crate stub; xxhash-rust workspace dep | Lock-in artefacts so Wave 1 inherits structure |
| **1** | 5.14.B1, 5.14.D2, 5.16.A, 5.14.C1 | None — parallel-safe, file-disjoint |
| **2** | 5.14.B2, 5.16.B, 5.14.C2, 5.14.C5 | Each depends on a Wave-1 sibling |
| **3** | 5.14.B5, 5.14.C3, 5.16.C, 5.14.D4 | Each depends on a Wave-2 sibling |
| **4** | 5.14.C4, 5.16.D | C4 depends on C3; 5.16.D depends on B + C |
| **5** | 5.14.C6 | Final differential gate |

## Wave 0 — design lock-in (Basin engineer, ~30 min, one commit)

The five locked decisions above need three small code artefacts in
place so the Wave 1 agents can `cargo build` cleanly on first try
and inherit the locked design without re-litigating it.

1. **`crates/basin-sketch/`** new crate.  Cargo.toml + `src/lib.rs`
   with `pub mod hll;` `pub mod tdigest;` stubs (just `pub struct Hll;`
   `pub struct TDigest;` placeholders).  Workspace `Cargo.toml`
   `[workspace.members]` adds the new crate.  Wave 1 5.14.B1 fills
   in the actual hoist.
2. **`xxhash-rust = "0.8"`** added to workspace `[dependencies]`
   table (or `[workspace.dependencies]` if the workspace uses
   inherited deps).  Wave 1 5.16.A consumes it.
3. **TASK.md** mark 5.14.A4, 5.14.D1 as `[x]` (both shipped).

Acceptance gate: `cargo build --workspace` clean, no behaviour
change in any test.

## Wave 1 — parallel, file-disjoint (4 sonnet agents)

Ordered by **critical-path-first** — start the longest item earliest,
the shortest items can finish while the longest is still running.

### Priority 1 — 5.14.C1 — basin-hottier crate skeleton (1 week)

Longest item; unlocks the entire C-series (C2 → C3 → C4 → C5 → C6
= ~5 weeks of dependent work).  **Start this agent first.**

- New crate `crates/basin-hottier/`.
- `MemTable` (parking_lot RwLock'd BTreeMap) + `MemTableRegistry`
  (DashMap + per-project `AtomicU64` + `Semaphore`).
- `RowKey` newtype with big-endian PK encoding (mirror
  `sort_batch_by_cluster_cols` byte semantics).
- Bench suite: insert ≥ 100 k/s debug / ≥ 1 M/s release; point-lookup
  p99 ≤ 500 µs at 1 M rows; range-scan 1 k-row ≤ 2 ms.
- If p99 lookup fails the gate, switch to `crossbeam-skiplist::SkipMap`
  before C2 starts (documented fallback per ADR 0016).
- **File scope (zero overlap with siblings):** entirely new crate.
  Plus one line in workspace `Cargo.toml` `[workspace.members]`.

### Priority 2 — 5.16.A — Plan-shape canonical hash (4 days)

Second-longest; unlocks 5.16.B / C / D (~3 weeks of dependent work).
**Start this agent second.**

- Per locked decision: `xxhash-rust` `xxh3_64` with fixed seed.
- New module `crates/basin-engine/src/query_shape.rs`.
- Strip literals at LogicalPlan layer (`Expr::Literal(_) → LiteralSlot(DataType)`); canonical post-order walk.
- Tests: same-shape-different-literals → equal hash; commutative
  conjunct order → equal hash; different operator → different
  hash; cross-process stability (run twice in separate processes,
  hash matches).
- Bench: ≤ 5 µs/query on representative LogicalPlan.
- **File scope:** new `query_shape.rs`; one-line `mod` add to
  `lib.rs`; one-line hash compute in `executor.rs` and
  `fast_select.rs` (record-only; no storage yet).

### Priority 3 — 5.14.B1 — basin-sketch hoist + DataFileRef fields (1 day)

Smallest item; can finish before either C1 or 5.16.A.  Unlocks
5.14.B2 (writer-side sketches) and 5.14.B5 (query-time fast path).

- Wave-0 stub of basin-sketch becomes a real impl: hoist the existing
  inline `Hll` (from `approx_count_distinct.rs`) and `TDigest`
  (from `approx_percentile.rs`) — keep the bytes-format identical
  so existing UDF tests pass without regeneration.
- Add `hll_sketches: BTreeMap<String, Vec<u8>>` and
  `tdigest_sketches: BTreeMap<String, Vec<u8>>` (both
  `#[serde(default)]`) to `DataFileRef` (catalog) and `DataFile`
  (storage).
- Both `basin-storage` and `basin-engine` add `basin-sketch` as
  a workspace dep.
- **File scope:** `crates/basin-sketch/src/*.rs`,
  `crates/basin-catalog/src/metadata.rs`,
  `crates/basin-storage/src/data_file.rs`,
  `crates/basin-engine/src/approx_count_distinct.rs` (re-export
  from basin-sketch), `crates/basin-engine/src/approx_percentile.rs`
  (same).

### Priority 4 — 5.14.D2 — Compaction-time multi-sort (3 days)

Independent of C1, B1, 5.16.A.  Measurable smoke-win
(`order_by_multi` from 0.83× → ≥ 1.6× per locked acceptance gate).

- Compactor reads `QueryHistory::top_pattern(project, table)` and
  sets `WriteOptions::cluster_columns` from it when (a)
  `TableMetadata::cluster_columns` is empty OR
  `TableMetadata::adaptive_sort_override = true`, AND (b) threshold
  met (≥ 30 % share + ≥ 100 queries).
- Add `basin.adaptive_sort_override` DDL option (parse + persist;
  mirror `basin.row_block_size` pattern).
- Add `TableMetadata::adaptive_sort_override: bool` with
  `#[serde(default)]`.
- Surface `(declared_cluster, observed_top)` delta into
  `query_history` for future 5.16.G consumption (lay the API now;
  consumer ships in 5.16.G).
- **File scope:** `crates/basin-shard/src/in_process.rs`,
  `crates/basin-engine/src/ddl.rs`,
  `crates/basin-catalog/src/metadata.rs` (+ 1 field — minor conflict
  with B1; sequence B1 first if possible).

## Wave 2 — parallel after Wave 1 lands (4 sonnet agents)

Each item below depends on a single Wave-1 sibling.

### 5.14.B2 — Writer-side sketch computation (3 days)

Depends on **B1.**  Files: `crates/basin-storage/src/writer.rs`
(add `compute_sketches`, mirroring `compute_bloom_filters`),
`crates/basin-engine/src/{executor,dml_mutate}.rs` (thread
`sketch_columns` through, same way `bloom_columns` was threaded).
Decision (locked): default `sketch_columns = global_sort_order`,
just like blooms.  Acceptance gate: writer overhead ≤ 5 % of total
write time; existing UDF tests still pass.

### 5.16.B — Per-shape rolling histograms (1 week)

Depends on **5.16.A.**  Files: `crates/basin-engine/src/query_stats.rs`
(new), `crates/basin-engine/src/lib.rs` (mod + Engine accessor),
`crates/basin-engine/src/session.rs` (record at end of each
statement).  Add `hdrhistogram` to workspace.  Multi-project
pattern: DashMap shared registry + per-project bounded LRU
(max 500 shapes/project).  Acceptance gate: registry overhead ≤ 1 %
p99 at 10 k QPS.

### 5.14.C2 — Memtable write path + WAL transaction markers (1.5 weeks)

Depends on **C1.**  Files: `crates/basin-engine/src/executor.rs`
(INSERT), `crates/basin-engine/src/dml_mutate.rs` (UPDATE/DELETE
→ memtable upsert/tombstone), `crates/basin-engine/src/session.rs`
(TxState memtable watermarks), `crates/basin-shard/src/in_process.rs`
(routing), `crates/basin-wal/src/file_wal.rs` (BEGIN/ROLLBACK
markers, replay-time suppression).  Acceptance gate per ADR 0016:
single-row UPDATE p99 ≤ 5 ms (10× current), INSERT p99 ≤ 1 ms,
rolled-back-tx WAL entries NOT re-applied on restart.

### 5.14.C5 — Multi-project memory budget (1 week)

Depends on **C1.**  Files: `crates/basin-hottier/src/budget.rs`
(new), `crates/basin-hottier/src/registry.rs` (extend),
`crates/basin-engine/src/{lib,alter,ddl}.rs` (config plumbing +
`ALTER PROJECT SET basin.memtable_hard_cap = N` DDL).  Hard cap
256 MB / soft cap 192 MB / per-table 16 MB defaults per ADR 0016.
Largest-first global flush scheduler.  Acceptance gate: 10 k-project
fuzz fits in 4 GB heap; per-project byte usage scales O(bytes)
not O(active_projects).

## Wave 3 — parallel after Wave 2 lands (4 sonnet agents)

### 5.14.B5 — Query-time sketch fast path (4 days)

Depends on **B2.**  When `APPROX_*` UDF's input is a bare column on
a single table with no predicate (or a fully prunable predicate),
short-circuit by merging the catalog-resident sketches.  Heterogeneous
fallback: if ANY file lacks the sketch, full-scan path.  Files:
`crates/basin-engine/src/approx_count_distinct.rs`,
`crates/basin-engine/src/approx_percentile.rs`,
`crates/basin-engine/src/approx_fast_path.rs` (new).  Acceptance
gate: fast path matches full-scan within accuracy tolerance;
performance ≤ 5 ms on 100-file table where full scan takes ≥ 200 ms.

### 5.14.C3 — Read-merge path (1.5 weeks)

Depends on **C1 + C2.**  Files:
`crates/basin-hottier/src/merge.rs` (new) — PK-ordered merge with
dedup-keep-memtable and tombstone suppression.  Hook into
`crates/basin-engine/src/fast_select.rs` (point lookup probes
memtable first) and `crates/basin-engine/src/executor.rs`
(full-scan merge).  Schema-evolution per ADR 0016 addendum: apply
`schema_version` delta chain on read.  Acceptance gate: 88-shape
smoke runs identical results with and without memtable populated.

### 5.16.C — Scale-dependent regression tracking (4 days)

Depends on **5.16.B.**  Bucket each query by `log2(table_row_count)`
(8 buckets covering 1 k → 16 M rows).  Per-(shape, bucket)
histograms.  Expose `top_regressions(project, table)`.  Files:
`crates/basin-engine/src/query_stats.rs` (extend).  Memory
adjustment: 500 shapes × 8 buckets × ~2 KiB = ~8 MiB/project
(decision required at PR — relax bound or reduce to 250 shapes).
Acceptance gate: synthetic monotone-growing-latency shape flagged;
flat-latency shape not flagged.

### 5.14.D4 — Multi-sort + WindowExec differential (2 days)

Depends on **D2.**  Reuses 88-shape smoke battery.  Asserts 0
differential rows across the (D2 adaptive sort enabled / disabled)
× (D3 WindowExec enabled / disabled) cells for the relevant shapes
(`order_by_multi`, `window_partition_sum`, `lag_lead_window`).

## Wave 4 — parallel after Wave 3 (2 sonnet agents)

### 5.14.C4 — Flush task (2 weeks)

Depends on **C1 + C2 + C3.**  Background Tokio task per
`MemTableRegistry`.  Triggers: size / age / scan pressure.
Algorithm per ADR 0016 §Flush.  Non-blocking writes (write lock
held only across snapshot clone + final GC; I/O lock-free).
Files: `crates/basin-hottier/src/flush.rs` (new),
`crates/basin-shard/src/in_process.rs` (integrate alongside
existing compactor).  Acceptance gate: 1 M-row mixed workload
flushes without read stall; 0 differential rows.

### 5.16.D — OTLP export + basin_stat_statements SQL view (1 week)

Depends on **5.16.B + 5.16.C.**  OTLP metrics for cross-process
shape stats export; Postgres-style `basin_stat_statements` view
for in-process consumption.  Privacy guards per ADR 0017
(per-customer view shows real names; cross-project export later
anonymises at cloud-side).  Files:
`crates/basin-engine/src/query_stats_export.rs` (new),
`crates/basin-engine/src/info_schema_provider.rs` (extend).

## Wave 5 — final differential gate

### 5.14.C6 — Hot-tier full differential harness (1 week)

Depends on **C1 + C2 + C3** (C4 strongly recommended for stability
under load).  Every shape in `vortex_vs_parquet_smoke` runs in
three modes: empty-memtable / all-memtable / split.  0 differential
rows.  Files: `tests/integration/tests/hottier_differential.rs`
(new).

## Critical path

`C1 → C2 → C3 → C4 → C6` is the longest chain at ~6.5 weeks
sequential.  Everything else fits inside C's timeline:

- B1 (1 day) + B2 (3 days) + B5 (4 days) = 1.5 weeks parallel to C.
- 5.16.A (4 days) + B (1 wk) + C (4 days) + D (1 wk) ≈ 3 weeks parallel.
- D2 (3 days) + D4 (2 days) ≈ 1 week parallel.
- C5 (1 wk) parallel to C2 / C3.

With 4 parallel agent slots, wall-clock for the full plan is
~6.5 weeks bottlenecked on the C-series.

## Out of scope

- ADR 0017 amendments for new privacy regulations
- 5.16.E – 5.16.H (basin-cloud repo; spec'd in
  `docs/basin-cloud-roadmap.md`)
- Phase 5.7.B1 per-project secondary indexes — deferred until
  Phase 5.14.A / C land and the residual point-query gap is
  measured (already noted in WEDGE.md)
