---
title: "DF removal — physical operator inventory"
nav_section: architecture
sidebar_position: 3
summary: "Which physical operators Basin's queries actually reach, what each must support, and an LOC estimate for rebuilding the physical layer on arrow-rs kernels."
tags: [datafusion, migration, execution, performance]
---

# 03 — Physical operators we must build

**Status:** draft, evidence-gathering complete for the items marked verified.
Items marked **UNVERIFIED** were not confirmed against source before this
document was landed and must be checked before any LOC number here is used
for planning.

**Scope boundary.** arrow-rs 58 stays. `filter`, `take`, `sort_to_indices`,
`lexsort_to_indices`, `concat_batches`, `interleave`, comparison kernels, hash
kernels, cast, and the aggregate primitives on `arrow-arith` are all kernels we
keep. Every LOC estimate below is **orchestration only** — batch plumbing,
state machines, spill/limit logic, schema bookkeeping — never kernel
re-implementation.

---

## 1. The fact that shrinks the problem: `target_partitions = 1`

Confirmed. Two independent mechanisms:

- `crates/basin-engine/src/session.rs:2705` — every session is opened with
  `SessionConfig::new().…​.with_target_partitions(1)`. The comment above it
  (session.rs:2687–2701) states the reason explicitly: bounded per-query
  Parquet fan-out for multi-project fairness on bounded-concurrency object
  stores.
- `crates/basin-engine/src/executor.rs:11302–11305` — "because `open_session`
  pins `datafusion.execution.target_partitions = 1` (session.rs), every
  aggregate plans as `AggregateExec: mode=Single` with ZERO `RepartitionExec`
  and all files in a single file_group".

### When is parallelism ever enabled?

Exactly one path, and it is **scan-only**:

- `crates/basin-engine/src/session.rs:288–320`,
  `target_partitions_for_bulk_scan(file_count)`:
  1. `file_count < MIN_FILES_FOR_PARALLEL_SCAN` (= 2, session.rs:286) → return `1`.
  2. Cap = `BASIN_ENGINE_TARGET_PARTITIONS_MAX` env var, else
     `available_parallelism()`.
  3. Return `min(cap, file_count)`.
  The doc comment states: "This function is called only from the full-DataFusion
  SELECT path (`exec_select`). The simple-SELECT fast-path and all DML paths
  keep `target_partitions = 1` permanently." (session.rs:300–302)

- Its only non-test caller is `crates/basin-engine/src/executor.rs:10965`,
  inside the `TargetPartitionsGuard` block (executor.rs:10930–11000). That
  block **also disables every repartition rule** when it raises the value
  (executor.rs:10984–10993):

  ```rust
  opts.execution.target_partitions = new_tp;
  opts.optimizer.repartition_aggregations = false;
  opts.optimizer.repartition_joins      = false;
  opts.optimizer.repartition_windows    = false;
  ```

  with the comment: "the exchange overhead exceeds the benefit at Basin's
  typical table sizes, and `repartition_windows=true` (DataFusion 53.1 default)
  inserts exchange nodes around `WindowAggExec` breaking LAG/LEAD/RANK queries
  when `target_partitions > 1`."

- `TABLESAMPLE … REPEATABLE(seed)` forces `new_tp = 1` unconditionally
  (executor.rs:10963–10965) for determinism.

- The guard restores `target_partitions` to 1 on drop, on both the success and
  the `?` early-return path (executor.rs:10509–10518).

**Consequence for the rebuild.** The only operator that ever sees more than one
partition is the *file scan*. Everything above it consumes a single stream. We
need:

- a scan that can fan out across file groups, plus
- **one** merge point (`CoalescePartitions` / `SortPreservingMerge`),

and **no** exchange operator, no hash repartitioner, no partition-aware
aggregate/join/window variants, no `Partial`/`FinalPartitioned` aggregate split,
no `InterleaveExec`. That is the single largest saving in the whole migration.

Corroborating test: `tests/integration/tests/aggregate_tuning.rs:327–349`
(`small_input_aggregate_is_single_partition`) asserts a `GROUP BY` plan contains
**no** `RepartitionExec`, and :353+ asserts the same for `UNION ALL`.

---

## 2. Where DataFusion is bypassed entirely today

Basin already hand-writes a large fraction of its hot paths. These do not need
new operators — they need re-pointing at the new scan/stream types.

| Module | LOC | Role |
|---|---:|---|
| `fast_select.rs` | 6523 | Simple-SELECT fast path (no DF plan at all) |
| `index_probe.rs` | 6411 | PK / secondary-index / GIN / trigram point + range probes |
| `fast_aggregate.rs` | 2205 | Hand-written aggregate shortcut |
| `values_fast.rs` | 2177 | `VALUES` / literal-row INSERT path |
| `gapfill.rs` | 1557 | Time-bucket gap-fill (Basin-specific operator) |
| `hot_tombstone.rs` | 1512 | `TombstoneFilterExec` + `UpdateOverlayExec` (custom `ExecutionPlan`) |
| `vortex_listing_format.rs` | 1184 | `UuidDecimal256RestoreExec`, `PointFsbRestoreExec` |
| `tombstone_cold_scan.rs` | 422 | `TombstoneColdScanExec` |
| `gin_rowgroup_scan.rs` | 396 | `GinRowGroupScanExec` (row-group allowlist bridge) |
| `catalog_window_exec.rs` | 395 | Physical rule: elide `SortExec` above window when file order covers it |
| `sort_streaming_limit.rs` | 368 | Physical rule: coalesce before TopK when `OFFSET > 0` on natural order |
| `rtree_rowgroup_scan.rs` | 335 | `RTreeScanExec` |
| `jsonb_posting_scan.rs` | 272 | `JsonbPostingScanExec` |

**Total already Basin-owned: ~23.7 kLOC.** Eight `impl ExecutionPlan for …`
sites exist in-tree (grep `impl ExecutionPlan for` across `crates/`), all of
them scan-shaped or overlay-shaped. Porting these to a Basin-native
`ExecutionPlan`-equivalent trait is *mechanical* — the trait surface we define
should be shaped to keep these edits small.

---

## 3. Operator-by-operator requirements

### 3.1 Joins — which types are reachable?

Evidence from SQL appearing in the test corpus (`grep -rio` over
`tests/` + `crates/`, `*.rs`):

| Syntax | Occurrences | Verdict |
|---|---:|---|
| `LEFT JOIN` | 150 | reachable |
| `INNER JOIN` | 73 | reachable |
| `CROSS JOIN` | 67 | reachable |
| `LATERAL` (word-boundary) | 461 | reachable — dedicated suite `tests/integration/tests/lateral_joins.rs` |
| `NOT EXISTS` | 304 | reachable → anti-join |
| `EXISTS (` | 85 | reachable → semi-join |
| `USING (` | 60 | reachable |
| `NOT IN (` | 32 | reachable → anti-join (null-aware) |
| `FULL JOIN` / `FULL OUTER JOIN` | 6 / 4 | reachable |
| `RIGHT JOIN` | 3 | reachable |
| `NATURAL JOIN` | 2 | reachable |

Direct physical-plan evidence:

- `tests/integration/tests/plan_quality.rs:280–306` —
  `exists_lowers_to_semijoin` asserts the physical plan **must** contain
  `HashJoinExec` **and** `join_type=LeftSemi`, with the comment "physical plan
  must be a LeftSemi HashJoinExec, not a nested-loop correlated subquery".
  So `LeftSemi` hash join is a *contract*, not an accident.
- `crates/basin-engine/src/any_all_rewrite.rs:10` — Basin adds a rewrite rule
  specifically to *avoid* DataFusion lowering uncorrelated `ANY`/`ALL` to a
  "`LeftMark NestedLoopJoinExec` — an O(n²) algorithm". This is the only
  in-tree mention of `NestedLoopJoinExec`, and it exists to *suppress* it.
- `crates/basin-engine/src/query_shape.rs:313–322` enumerates all ten
  `JoinType` variants (`Inner, Left, Right, Full, LeftSemi, RightSemi,
  LeftAnti, RightAnti, LeftMark, RightMark`). This is a query-shape hashing
  table for ADR 0017 privacy, so it is **weak** evidence of reachability — but
  it is the complete set DataFusion can emit, and the rewrite rules above
  confirm `LeftMark` at minimum is emitted by the stock planner.

**Required join set:**

- **Hash join**, equi-predicate, with build/probe sides on one partition each:
  `Inner`, `Left`, `Right`, `Full`, `LeftSemi`, `LeftAnti`, `LeftMark`.
  `RightSemi` / `RightAnti` / `RightMark` are producible by the planner
  swapping sides; we can either implement them or normalise them away at
  planning time. **Recommend: normalise, implement left-side variants only.**
- **Cross join / nested-loop join** with an arbitrary filter — needed for
  `CROSS JOIN` (67 sites) and for any non-equi `ON` predicate. A nested-loop
  join is the fallback that *must* exist for correctness even though
  `any_all_rewrite` works hard to keep queries off it.
- **LATERAL** — 461 mentions and a dedicated suite. Its header notes Basin
  "routes every multi-table / LATERAL SELECT straight to" the full DF path.
  **UNVERIFIED**: whether DF 53 decorrelates every reachable LATERAL shape into
  a plain join, or whether some shapes require a genuine
  correlated/dependent-join operator. *This is the single biggest open risk in
  the join section* — if a real dependent join is needed, add ~600 LOC.
- **Sort-merge join**: 0 occurrences in-tree. **Not needed.**
- **Symmetric hash join** (streaming): 0 occurrences. **Not needed** — Basin
  has no unbounded sources on this path.

Null-aware anti-join semantics for `NOT IN` with nullable columns is the
classic correctness trap; budget for it explicitly.

### 3.2 Aggregates

Reachable features (occurrence counts over `tests/`, `crates/`, `docs/`):

| Feature | Count | Notes |
|---|---:|---|
| `GROUP BY` (grouped) | — | universal; `aggregate_tuning.rs` |
| Ungrouped / scalar aggregate | — | universal |
| `COUNT(DISTINCT …)` | ~50 | distinct accumulator required |
| `FILTER (WHERE …)` | 53 | per-aggregate filter mask |
| `ROLLUP` | 53 | grouping-sets machinery |
| `GROUPING SETS` | 17 | grouping-sets machinery |
| `CUBE(` | 4 | grouping-sets machinery |
| `GROUPING(` | 4 | grouping-id column |
| `WITHIN GROUP` | 59 | ordered-set aggregates |
| `percentile_disc` | 56 | ordered-set |
| `percentile_cont` | 46 | ordered-set |
| `array_agg` | 152 | list-building accumulator |
| `string_agg` | 49 | with `ORDER BY` inside the aggregate |
| `DISTINCT ON` | 60 | Postgres-specific; distinct-by-prefix |

Aggregate **modes** required: **`Single` only.** No `Partial`/`Final` split, no
`FinalPartitioned`, because repartition is disabled unconditionally
(executor.rs:10984–10993, aggregate_tuning.rs:327–349).

- `tests/integration/tests/array_agg_perf_shape.rs:358–401` asserts grouped
  `array_agg` plans as a single `AggregateExec` and references DataFusion's
  `GroupsAccumulator` selection — so we need a two-tier accumulator design
  (row-wise accumulator + a vectorised group-wise fast path) to avoid a
  regression on the shapes that test guards.

**Spilling for aggregates: see §4 — recommend NOT building it in v1.**

### 3.3 Window functions

Reachable functions (all documented in `CAPABILITIES.md`, all exercised by
`tests/integration/tests/window_fns.rs`, which has **zero** `#[ignore]`s —
verified by `grep -c ignore` returning 0):

- Ranking: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`,
  `ntile`
- Offset: `lag`, `lead`, `first_value`, `last_value`, `nth_value`
- Aggregates-as-window: `sum`, `avg`, `count`, `min`, `max`
- Named `WINDOW w AS (…)` clauses (window_fns.rs:6)

**Frame units — all three are reachable:**

| Unit | Evidence |
|---|---|
| `ROWS` | window_fns.rs:438, :508 — `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`, `UNBOUNDED PRECEDING AND CURRENT ROW` |
| `RANGE` | window_fns.rs:541 — `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`; **window_fns.rs:893–960** — `RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND CURRENT ROW`, a full value-offset RANGE frame over a `TIMESTAMPTZ` order column, with exact expected values asserted |
| `GROUPS` | window_fns.rs:798–888 — `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW` and `GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| offset RANGE (numeric) | `tests/integration/tests/hottier_differential.rs:712` — `RANGE BETWEEN 100 PRECEDING AND CURRENT ROW` |

**Bounds required:** `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`,
`n FOLLOWING`, `UNBOUNDED FOLLOWING` — in all three units. The `RANGE …
INTERVAL` case means the frame-bound comparison must be a *typed value
comparison* against the ORDER BY column (including interval arithmetic on
timestamps), not just an integer row offset.

**`EXCLUDE` (CURRENT ROW / GROUP / TIES / NO OTHERS): NOT required.** The only
occurrences are in `tests/integration/tests/sql_support_matrix.rs`, which is
the *generator* of the support matrix (it asserts rejection), not a passing
feature test. **UNVERIFIED at the level of reading the matrix assertions
directly** — confirm before dropping `EXCLUDE` from scope, it is ~150 LOC if
needed.

Window execution mode: **`Sorted` only.** `plan_quality.rs` (comment at
:325–330) describes the target shape as "exactly ONE `SortExec` on
[partition_cols, order_cols] feeding a `BoundedWindowAggExec mode=[Sorted]`".
The `Linear` / `PartiallySorted` modes DataFusion also has are not required
because we always sort into the window's required order (or elide the sort when
the file order already satisfies it, per `catalog_window_exec.rs`).

`catalog_window_exec.rs:1–50` is a physical rule that removes the `SortExec`
above a window when `basin.sort_by` already covers `PARTITION BY` + `ORDER BY`.
That optimisation must be preserved — it is load-bearing for the time-series
shapes.

### 3.4 Sort

- **Top-K is required and is load-bearing.** `sort_streaming_limit.rs:18–52`
  documents the exact plan shape it optimises —
  `GlobalLimitExec(skip, fetch) → SortExec(fetch) → …` — and relies on "a TopK
  sort [that] terminates after collecting `fetch` rows". `plan_quality.rs:260+`
  asserts `LIMIT 10` with `NULLS LAST` returns exactly the true top 10.
  So: a bounded heap sort with correct `NULLS FIRST/LAST` and multi-key
  `SortOptions`.
- **`SortPreservingMerge` is required** — one mention in-tree, and it is the
  natural merge point above a fanned-out ordered file scan.
- **Sort spilling: see §4 — recommend NOT building it in v1.**

### 3.5 Set operations, CTEs, misc

| Feature | Count | Operator |
|---|---:|---|
| `INTERSECT` | 325 | lowered to `LeftSemi` join + distinct |
| `EXCEPT` | 156 | lowered to `LeftAnti` join + distinct |
| `UNION` / `UNION ALL` | — | `UnionExec` (6 in-tree mentions) — a trivial concat operator |
| `WITH RECURSIVE` | 91 | **`RecursiveQueryExec` required.** Tested at `tests/integration/tests/3way_pg_compat.rs:355`, `compare_postgres_common.rs:1313`, `differential_pg.rs:1614–1629`. There is **no** Basin-side implementation — grep for `recursive` in `crates/basin-engine/src` returns only JSONB-path hits. This is 100% DataFusion today and must be rebuilt. |
| `TABLESAMPLE` | 159 | scan-level; seeded variant forces single partition (executor.rs:10963) |
| `generate_series` | 132 | table function |
| `UNNEST(` | 65 | `UnnestExec` — 0 in-tree mentions of the exec type, but 65 SQL occurrences, so it is reached via DF's planner |
| `MERGE INTO` | 50 | DML path, not a SELECT operator |
| `DISTINCT ON` | 60 | distinct-by-prefix over sorted input |

`UnionScanCollapse` (a Basin optimizer rule, referenced at
executor.rs:11305–11306) collapses two `UNION ALL` scan branches into one scan
with an `OR` predicate — preserve it.

---

## 4. Is spilling to disk reachable?

**Mechanically yes; in practice it is an unexercised safety net. Recommend
building the memory *accounting* and the clean-failure path, and deferring the
*spill* path.**

Evidence:

- `crates/basin-engine/src/lib.rs:519–526` — one process-wide
  `FairSpillPool::new(query_memory_bytes)`, installed into every per-session
  `RuntimeEnv` at `session.rs:2748–2755` via `.with_memory_pool(…)`.
- `crates/basin-engine/src/lib.rs:392–420`, `derive_query_memory_bytes`:
  - `BASIN_QUERY_MEMORY_BYTES` — explicit byte count, wins outright.
  - `BASIN_QUERY_MEMORY_FRACTION` — percent of detected container RAM,
    clamped to `10..=90`, **default 50**.
  - Floor: **256 MiB** (`const FLOOR`, lib.rs:409).
- `lib.rs:185–195` states the intended split: "spillable operators (external
  sort, grouped aggregate) spill to disk and complete; non-spillable ones
  (distinct accumulators) fail cleanly with a retryable `ResourcesExhausted`
  error. Either way the node never dies."

Why it is effectively unreachable in practice:

1. **No test anywhere asserts a spill or a `ResourcesExhausted`.** A grep for
   `spill` / `ResourcesExhausted` across `tests/` and
   `crates/basin-engine/src` returns only the `FairSpillPool` construction and
   its doc comments. The path has never been validated.
2. **No `DiskManager` is configured.** `RuntimeEnvBuilder` at session.rs:2748
   sets only `.with_cache_manager(…)` and `.with_memory_pool(…)`. Spill files
   therefore land in the OS temp dir by DataFusion default — which for a
   container deployment is not a deliberate choice anyone made.
3. **The real bound is the statement timeout, not memory.**
   `session.rs:70–75`: `DEFAULT_STATEMENT_TIMEOUT_MS = 30_000`, described as
   "cheap insurance against a hostile cartesian self-join or an ill-bounded
   recursive CTE". A query large enough to spill a ≥256 MiB working set will
   almost always hit 30 s first.
4. **Results are fully materialised anyway.** The executor collects into
   `Vec<RecordBatch>` (`df_batches`, executor.rs:11315+) before returning. The
   architecture already assumes the *result set* fits in memory, so an
   operator-level spill only helps when an intermediate is much larger than the
   output — grouped aggregate and sort being the two candidates.

**Recommendation.** Build a `MemoryPool` equivalent (reservation + fair-share
accounting + clean `ResourcesExhausted`) — that is cheap and preserves the
"one project cannot OOM the node" invariant, which is the *actual* stated goal
at lib.rs:185–195. Do **not** build external merge sort or spilling hash
aggregate in v1. Revisit when a real workload trips `ResourcesExhausted`.
This removes roughly 1200–1500 LOC of the hardest, least-testable code in the
whole physical layer.

---

## 5. The table

LOC = orchestration on top of arrow-rs kernels, excluding tests. "Needed?"
answers *for the v1 cut* under the §4 no-spill recommendation.

| Operator | Needed? | Evidence | Est. LOC | Notes |
|---|---|---|---:|---|
| **Scan / DataSource** (Vortex + Parquet, file-group fan-out) | **Yes** | executor.rs:10930–11000; `target_partitions_for_bulk_scan` session.rs:303 | 900 | Only operator that is ever multi-partition. Projection + predicate pushdown + row-group selection must survive. |
| Row-group-pruned scans (GIN / R-tree / JSONB posting / tombstone-cold) | **Yes — port, not build** | `gin_rowgroup_scan.rs`, `rtree_rowgroup_scan.rs`, `jsonb_posting_scan.rs`, `tombstone_cold_scan.rs` (1425 LOC today) | 250 | Mechanical re-target onto the new trait. Shape the trait to keep this small. |
| Overlay execs (`TombstoneFilterExec`, `UpdateOverlayExec`, UUID/Point restore) | **Yes — port** | `hot_tombstone.rs:403,544`; `vortex_listing_format.rs:863,991` | 200 | Same: re-target existing 2.7 kLOC. |
| **Filter** | Yes | universal | 150 | `arrow::compute::filter_record_batch` + selectivity-adaptive batch coalescing. |
| **Projection** | Yes | universal | 120 | Expression eval + schema rewrite. |
| **CoalesceBatches** | Yes | needed after any selective filter | 100 | Target-batch-size accumulator over `concat_batches`. |
| **CoalescePartitions** | Yes | `sort_streaming_limit.rs:30–35` injects one | 120 | The single merge point above the scan. |
| **SortPreservingMerge** | Yes | ordered multi-file scan; 1 in-tree mention | 250 | K-way loser-tree merge over pre-sorted streams. |
| **Sort (in-memory)** | Yes | universal | 300 | `lexsort_to_indices` + `take`; multi-key, NULLS FIRST/LAST. |
| **Sort — TopK** | **Yes, required** | `sort_streaming_limit.rs:18–52`; `plan_quality.rs:260+` | 300 | Bounded heap with early termination. Load-bearing for `ORDER BY … LIMIT`. |
| **Sort — external/spilling** | **No (v1)** | §4: no test, no DiskManager, 30 s timeout dominates | 0 *(deferred ~700)* | Deferred. |
| **Limit / Offset** | Yes | `GlobalLimitExec` 19 mentions | 80 | Skip + fetch, fused into TopK where possible. |
| **Aggregate — ungrouped** | Yes | universal | 250 | Single-pass accumulators. |
| **Aggregate — grouped, `mode=Single`** | Yes | executor.rs:11303; aggregate_tuning.rs:327 | 900 | Row-format group keys + hash table. Needs both a generic accumulator path and a vectorised group-wise path (`array_agg_perf_shape.rs:358`). |
| Aggregate — `Partial`/`Final`/`FinalPartitioned` | **No** | repartition disabled: executor.rs:10984–10993 | 0 | Direct saving from `target_partitions=1`. |
| **Aggregate — DISTINCT** | Yes | `COUNT(DISTINCT …)` ×50 | 200 | Per-group hash set. Non-spillable by design (lib.rs:191). |
| **Aggregate — FILTER clause** | Yes | ×53 | 80 | Per-aggregate boolean mask before accumulation. |
| **Aggregate — GROUPING SETS / ROLLUP / CUBE** | Yes | ×17 / ×53 / ×4, `GROUPING()` ×4 | 350 | Expand to grouping-set list + `__grouping_id` column. |
| **Aggregate — ordered-set (`WITHIN GROUP`)** | Yes | ×59; `percentile_cont` ×46, `percentile_disc` ×56 | 250 | Buffer-per-group then sort; `string_agg`/`array_agg` ORDER BY share this. |
| **Aggregate — spilling** | **No (v1)** | §4 | 0 *(deferred ~500)* | Deferred. |
| **Hash join (Inner/Left/Right/Full)** | Yes | `LEFT JOIN` ×150, `INNER` ×73, `FULL` ×10, `RIGHT` ×3 | 900 | Single-partition build+probe. No exchange. Null-key handling per join type. |
| **Hash join (LeftSemi / LeftAnti / LeftMark)** | Yes | `plan_quality.rs:304` asserts `join_type=LeftSemi` as a contract; `EXISTS (` ×85, `NOT EXISTS` ×304 | 300 | Incremental on the above. Null-aware `NOT IN` anti-semantics is the trap. |
| Hash join Right{Semi,Anti,Mark} | **No** | `query_shape.rs:317–322` lists them but no SQL reaches them directly | 0 | Normalise to left-side variants at plan time. |
| **Nested-loop / cross join** | Yes | `CROSS JOIN` ×67; `any_all_rewrite.rs:10` exists to avoid it but it remains the correctness fallback | 250 | Needed for non-equi `ON` predicates. |
| **Sort-merge join** | **No** | 0 in-tree occurrences | 0 | |
| **Symmetric hash join** | **No** | 0 occurrences; no unbounded sources | 0 | |
| **LATERAL / dependent join** | **Yes, sizing UNVERIFIED** | `lateral_joins.rs`; `LATERAL` ×461; `compare_postgres_common.rs` "#61 LATERAL JOIN" | 400 *(risk: +600)* | **Open risk.** If DF 53's decorrelation handles every reachable shape, this collapses into the join types above and costs ~0. If a genuine correlated re-execution operator is needed, it is the most expensive single item on this list. **Resolve this first.** |
| **Window — `mode=Sorted`, ROWS frames** | Yes | window_fns.rs:438,508 | 450 | Sorted input + sliding accumulator. |
| **Window — RANGE frames (incl. value/INTERVAL offsets)** | Yes | window_fns.rs:893–960 (INTERVAL, exact values asserted); hottier_differential.rs:712 (numeric) | 350 | Typed value comparison against the ORDER BY column, incl. timestamp+interval arithmetic. |
| **Window — GROUPS frames** | Yes | window_fns.rs:798–888 | 200 | Peer-group boundary tracking. |
| **Window — EXCLUDE** | **Probably no — UNVERIFIED** | only in `sql_support_matrix.rs` (the rejection generator) | 0 *(≈150 if needed)* | Confirm the matrix asserts rejection before dropping. |
| Window — `Linear` / `PartiallySorted` modes | **No** | we always sort into required order; `catalog_window_exec.rs` elides when file order suffices | 0 | |
| Window sort-elision rule | Yes — port | `catalog_window_exec.rs` (395 LOC) | 100 | Preserve; load-bearing for time-series. |
| **Union / Union All** | Yes | 6 mentions; `aggregate_tuning.rs:353+` | 80 | Concat of single-partition streams. `UnionScanCollapse` rule preserved separately. |
| **Distinct / DISTINCT ON** | Yes | `DISTINCT ON` ×60 | 180 | Hash-distinct + sorted distinct-by-prefix. |
| **Intersect / Except** | Yes | ×325 / ×156 | 0 | Lower to `LeftSemi` / `LeftAnti` join + distinct — no new operator. |
| **RecursiveQuery** | **Yes** | `3way_pg_compat.rs:355`, `differential_pg.rs:1614–1629`, `compare_postgres_common.rs:1313`; **no Basin-side impl exists** | 300 | Fixpoint loop with a working table. Needs an iteration cap / timeout hook (session.rs:72). |
| **Unnest** | Yes | `UNNEST(` ×65 | 200 | List/struct expansion. |
| **Values / PlaceholderRow** | Yes | `values_fast.rs` covers the DML side | 60 | Literal row source. |
| **Empty / no-op** | Yes | `EmptyExec` ×3 | 30 | |
| **Table functions** (`generate_series`, etc.) | Yes | ×132 | 150 | Streaming generator source. |
| **Gapfill** | Yes — port | `gapfill.rs` (1557 LOC, Basin-specific) | 150 | Re-target only. |
| **Memory accounting / reservation** | Yes | lib.rs:519–526, 185–195; session.rs:2753 | 300 | Fair-share pool + clean `ResourcesExhausted`. **Keep this even though spill is deferred** — it is the actual noisy-neighbour invariant. |
| **Disk manager / spill files** | **No (v1)** | §4 | 0 *(deferred ~200)* | |
| **RepartitionExec / exchange** | **No** | executor.rs:10984–10993 disables all three repartition flags | 0 | Largest single saving. |
| **Metrics / EXPLAIN rendering** | Yes | `explain.rs`, `explain_pg_stat_harness.rs`, plan-shape tests assert on `EXPLAIN` text | 400 | Many tests match on plan strings; format compatibility is a real constraint. |
| **Stream/plan plumbing** (trait, `RecordBatchStream`, cancellation, statement deadline) | Yes | executor.rs:34–44 (deadline published per collect thread) | 500 | Cooperative cancellation for the 30 s timeout must be threaded through every operator. |

### Totals

| Bucket | LOC |
|---|---:|
| Scans + ports of existing Basin execs | 1,500 |
| Core streaming operators (filter, project, coalesce, limit, union, empty, values) | 620 |
| Sort family (in-memory, TopK, merge) | 850 |
| Aggregate family (incl. distinct, filter, grouping sets, ordered-set) | 2,030 |
| Join family (hash + variants + nested loop, incl. LATERAL at base estimate) | 1,850 |
| Window family | 1,100 |
| Distinct / recursive / unnest / table functions / gapfill | 830 |
| Memory accounting | 300 |
| Metrics, EXPLAIN, stream plumbing | 900 |
| **Total (v1, no spill)** | **≈ 9,980** |

**Call it ~10 kLOC ± 25%.** Risk band **8.5 k – 13 k**, driven almost entirely
by three items: LATERAL/dependent join (+600 worst case), `EXCLUDE` frames
(+150), and EXPLAIN-format compatibility with the existing plan-shape test
corpus (open-ended). Adding spill support later is a further ~1,400 LOC.

For scale: Basin already owns ~23.7 kLOC of scan/fast-path execution code, so
the physical layer roughly grows the hand-written execution surface by 40%,
not by a multiple.

---

## 6. What to resolve before committing to this estimate

1. **LATERAL.** Read `tests/integration/tests/lateral_joins.rs` end to end and
   determine whether every passing shape decorrelates to a plain join in DF 53.
   This is the only item that can move the total by >5%.
2. **Window `EXCLUDE`.** Confirm `sql_support_matrix.rs` asserts rejection.
3. **EXPLAIN string compatibility.** Enumerate every test that matches on plan
   text (`SortExec` ×98, `DataSourceExec` ×88, `FilterExec` ×80,
   `WindowAggExec` ×33, `RepartitionExec` ×25 …) and decide whether the new
   layer mimics DataFusion's `EXPLAIN` format or those tests get rewritten.
   ~25 of those mentions are `RepartitionExec` assertions that assert *absence*
   — those become trivially true and can be deleted.
4. **UNVERIFIED — benchmark shapes.** `benchmark/RESULTS_localfs.md` was not
   read before this document landed. It should confirm (or contradict) that the
   benchmarked query set stays inside the operator set above, and give the
   table sizes that determine whether the no-spill decision is safe.
5. **UNVERIFIED — `docs/sql-support.md`.** The support matrix was not read
   directly; the feature reachability in §3 is inferred from the test corpus
   and from plan-shape assertions. Cross-check the two before freezing scope.
