---
title: "DF removal — beating Postgres on every shape: what is winnable and what is a chosen trade"
nav_section: migration
sidebar_position: 14
summary: "Splits the published OLTP losses into scaling regressions that are bugs (winnable, and mostly independent of the engine swap) and µs-class point-read floors that cannot be beaten without giving up Basin's idle-cost wedge."
tags: [migration, performance, benchmarks, oltp]
---

# 14 — Beating Postgres everywhere: winnable vs. chosen trade

Goal on record: **beat Postgres on every shape at every size.** This document
sorts the published losses by whether that is achievable, because they are not
one problem. Two of the three categories are winnable. One is a trade Basin has
already made on purpose, and winning it means un-making that trade.

## Category A — scaling regressions. These are bugs, and they are winnable.

The signature: **Basin is competitive or winning at 10k rows and collapses at
1M, while Postgres stays flat.** A cost that grows with table size on a query
whose answer does not is pruning or early-exit failing to engage — not a floor.

| Shape | Basin 10k | Basin 1M | PG 1M | Degradation |
|---|---|---|---|---|
| Keyset pagination (`WHERE id > … LIMIT`) | 0.07 ms | 23.5 ms | 0.01 ms | **335×** |
| `LIMIT` without `ORDER BY` (early exit) | 0.06 ms | 52 ms | 0.03 ms | **870×** |
| `ARRAY_AGG` + `ORDER BY` in aggregate | 3.2 ms | 516 ms | 90 ms | 161× |
| `DISTINCT ON` first row per group | 1.9 ms (**win**) | 155 ms | 92 ms | 82× |
| JSONB GIN effectiveness | 4.3× | 1.12× | — | index decaying |
| Deep top-K (`ORDER BY … LIMIT 1000`) | — | 161 ms | 53 ms | wide-decode floor |
| `COUNT(*)` full table | — | 95 ms | 29 ms | should be metadata |

Diagnoses, each testable:

- **Keyset and bare `LIMIT`** should be O(limit), not O(table). Basin has the
  machinery — `keyset_fast_select_count` and
  `unordered_limit_fast_select_count` counters exist in `lib.rs:134-143`. The
  question is whether those branches *engage* at 1M or silently fall through to
  a full scan. Instrument first: if the counters are zero on the 1M card, this is
  a routing bug worth ~0 LOC of new algorithm.

### Found: the keyset zone-map prune is all-or-nothing across files

`fast_select.rs:3395-3423` builds `keyset_zone_maps`, the per-file `[min, max]`
map that lets the keyset branch open candidate files in ascending-min order and
**short-circuit once the page is provably complete**. The loop is:

```rust
for f in &live_files {
    match (mn, mx) {
        (Some(mn), Some(mx)) => { m.insert(...); }
        _ => { complete = false; break; }      // ← one bad file kills it
    }
}
complete.then_some(m)
```

A **single** live file lacking a decodable Int64 min/max sets `complete = false`
and discards the entire map. The code's own comment states the consequence: the
branch "then keeps the existing open-all-candidates behaviour, which is always
correct." Correct, and O(table).

**Why this shows up only at scale.** `insert_batch_for` issues one batch at 10k
but ~100 INSERT statements at 1M, so the 10k card has essentially one data file
and the 1M card has many. Every additional file is an independent chance to miss
a statistic — and missing one forfeits the optimization for *all* of them. A
per-file failure probability that is negligible at one file approaches certainty
across dozens.

**The fix is local and strictly better: degrade per-file, not globally.** Files
that have stats keep their `[min, max]` and stay in the ascending-min
short-circuit; files that lack stats are simply treated as always-candidate and
opened unconditionally. That is never worse than today's behaviour on any input,
and on the 1M card it should recover most of the 335×. Estimated at tens of
lines, not hundreds.

Two things to verify before treating this as *the* cause rather than *a* cause:

1. Confirm empirically that some 1M-card file genuinely lacks Int64 min/max for
   the `id` column — the mechanism is proven, its occurrence is **UNVERIFIED**.
2. Check whether the bare-`LIMIT` path (`unordered_limit_target`,
   `fast_select.rs:2711`) has an analogous all-or-nothing guard. Its 870×
   degradation has the same signature and may share the root cause.

Neither depends on the DataFusion migration, and both are worth fixing on `main`
independently of it.
- **Deep top-K** is described as a "whole-table wide-decode floor", but
  `topk_late_fast_select_count` (`lib.rs:146-150`) exists precisely to decode
  only the sort key first and materialize the wide columns for the surviving
  1,000 rows. Same question: is it firing at 1M?
- **`COUNT(*)`** should never scan. Both Vortex and Parquet footers carry row
  counts, and Basin already reads footers for stats
  ([06](./06-scan-and-storage.md)). 95 ms says it is counting rows.
- **JSONB GIN decaying from 4.3× to 1.12×** is the posting list losing
  selectivity at scale — likely per-file postings with no cross-file pruning, so
  every file gets opened.

None of this depends on removing DataFusion. **These are winnable now, and
several are probably routing bugs rather than missing algorithms.** They are
also the losses that look worst, because a 335× regression between two scales of
the same benchmark is a much weaker position than a constant-factor gap.

## Category B — µs-class point reads. Not winnable without un-making a trade.

| Shape | Basin 1M | PG 1M | Ratio |
|---|---|---|---|
| Point query p50 (unindexed PK) | 0.50 ms | 0.002 ms | 250× |
| Single-row `UPDATE` p50 | 1.24 ms | 0.012 ms | 103× |
| UPSERT (`ON CONFLICT DO UPDATE`) | 0.26 ms | 0.02 ms | 13× |
| JSONB `->>` (promoted column) | 1.97 ms | 0.11 ms | 18× |

Postgres's 0.002 ms is **2 microseconds** — a warm buffer-cache B-tree descent,
pointer-chasing through resident pages. Nothing involving an object-store round
trip, a footer parse, or a columnar decode competes with that. Matching it
requires the index *and* the row to be resident in memory.

**This collides directly with Basin's headline claim.** The README leads on
~2 KiB of RAM per idle project and cost that is O(bytes active), not
O(projects provisioned). A resident PK→(file, row-offset) map over every row of
every project is precisely O(rows provisioned). You cannot have microsecond
point reads on cold data *and* 2 KiB idle projects. That is not a bug; it is the
trade Basin chose, and it is the trade the wedge is built on.

### The honest middle path

Extend the hot tier into a **read-through PK index cache, resident only for
active projects, LRU-evicted**:

- Active project → PK map resident → point read becomes a memory probe plus at
  most one cached-page read. Plausibly low-µs to tens of µs.
- Idle project → nothing resident → stays at ~2 KiB, wedge intact.
- Cold project's first point read after eviction → still an object-store GET.
  **Postgres wins that one, always.** Object storage cold latency is 10–50 ms;
  no engine work changes it.

So the achievable claim is *"beats Postgres on point reads for active
projects, loses on the first touch of a cold one"* — which is defensible and
true, rather than "superior at every shape and size", which is not.

**This is a product decision, not an engineering one, and it needs an explicit
answer before the work is scheduled:** how much resident memory per active
project is Basin willing to spend to buy µs point reads, and does the idle-cost
claim get restated as an *idle* claim rather than a general one?

## Category C — capability gaps the owned engine closes

- **`INSERT … SELECT` unsupported** (honest SQLSTATE reject). This is exactly
  what `LogicalPlan::Insert { input }` in [08](./08-ir-design.md) makes
  expressible — DML as a relation. Fixed by the migration itself.
- **Bulk `UPDATE` 9.4 s vs 3.4 s (2.8×)** — copy-on-write file rewrite versus
  in-place tuple update. The delta/overlay path from ADR 0016 is the lever;
  this is a constant factor, not a scaling collapse.
- **Concurrency shapes** (mixed 8R+4W 202 ms vs 12 ms; 16-session SELECT 19 ms
  vs 2.3 ms; read-modify-write 8.4 ms vs 4.7 ms) — these need their own
  investigation. Note Basin's structural counter-win: it holds 1,000
  connections where Postgres holds 100 and refuses 900. Per-op latency under
  concurrency and connection scalability are different axes, and the benchmark
  currently publishes one without the other.

## Recommended order

1. **Instrument Category A before writing any code.** Dump the four fast-path
   counters on the 1M card. If they are zero, these are routing bugs and the
   cheapest wins available.
2. Make `COUNT(*)` metadata-only.
3. Fix cross-file JSONB posting pruning.
4. Get a product answer on Category B's memory-for-latency trade.
5. `INSERT … SELECT` falls out of the engine migration.

## Note on the harness

The goal of "more robust benchmarking than Postgres's" runs into what
[07](./07-conformance-tests.md) found: `compare_postgres_common.rs` is 6,821
lines carrying **two assertions**. It times shapes; it does not verify them.
Two of its shapes are labelled for NULL handling and 3VL semantics and are
merely timed. A harness that measures without asserting can report a win on a
wrong answer — so the assertion work in Phase 0 and the benchmark-credibility
work are the same work, not competing priorities.
