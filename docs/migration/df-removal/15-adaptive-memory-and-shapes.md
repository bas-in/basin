---
title: "Adaptive memory and shape-triggered structures"
nav_section: migration
sidebar_position: 15
summary: "How Basin can beat Postgres on shapes it currently loses without giving up the idle-cost wedge: per-query memory arenas, a shape-keyed adaptive index that builds itself on the third occurrence and evaporates when the shape stops, and cracking instead of upfront index builds."
tags: [migration, performance, memory, adaptive-indexing]
---

# 15 — Adaptive memory and shape-triggered structures

Answers the question [14](./14-performance-parity.md) left open: how much
resident memory per active project, and is there a way to buy Postgres-class
point reads without giving up the ~2 KiB idle project.

**The answer is that the trade should not be made statically at all.** Basin
already computes the one thing needed to make it dynamic, and is not using it
for this.

## The insight: `QueryShapeHash` is already the right key

Basin computes a stable, seeded 64-bit hash over the *shape* of every query with
literals stripped (`query_shape.rs`, ADR 0017). It is cross-process stable,
cross-Rust-version stable, and already persisted.

That is precisely the key an adaptive system needs. It answers "have I seen this
exact query pattern before, and how often?" — which is the trigger for building
a structure, and the signal for dropping it. Basin built this for **stats
privacy** and it turns out to be the foundation for **adaptive optimization**.

Nothing else needs inventing to get the trigger right.

## Layer 1 — memory as per-query arenas, not a shared pool

Postgres's model is `shared_buffers` plus per-backend `work_mem`, which means a
1,000-connection flood needs 1,000 × `work_mem` worst case. That is the
structural reason Basin holds 1,000 connections where Postgres holds 100.

Basin should not copy it. The model that fits:

- **One arena per running query.** All intermediate state — hash tables, sort
  runs, decoded batches — allocates from a single contiguous region with a hard
  cap. Query ends, the whole region is released in one operation. No
  fragmentation, no per-allocation bookkeeping, no GC pressure.
- **Memory belongs to *running* queries, not to open connections.** An idle
  connection holds its ~310 KiB of protocol state and nothing else. This is
  already Basin's advantage; arenas make it explicit and enforceable.
- **The bounded pool becomes an admission controller.** `FairSpillPool` already
  exists (`lib.rs:519-526`). With arenas it can do something better than
  fail-clean: refuse to *start* a query whose estimated arena exceeds the
  remaining budget, and queue it. A queued query is a latency cost; an OOM is an
  availability cost.

**Why this beats Postgres rather than matching it:** per-query arenas mean the
memory ceiling is a function of *concurrent running queries*, not *open
connections*. That is a strictly better scaling constant, and it is the same
property that already produces the 27× connection-RAM win — extended from
connection state to working state.

## Layer 2 — shape-triggered structures that build and evaporate

The user-facing ask: *configure for a shape on the fly, deactivate it on the
fly.* Concretely:

```
observed shape hash → frequency counter → threshold → build structure
                                        → idle timeout → drop structure
```

| Shape observed | Structure built | Kills which published loss |
|---|---|---|
| Repeated PK point lookup | Resident PK → (file, row-offset) map for that table | Point query 0.50 ms vs 0.002 ms |
| Repeated single-row UPDATE by PK | Same map, plus hot-tier pin | Single-row UPDATE 1.24 ms vs 0.012 ms |
| Repeated keyset page over a column | Sorted run / cracked column in hot tier | Keyset 23.5 ms vs 0.01 ms |
| Repeated `COUNT(*)` | Cached count invalidated by write epoch | COUNT(*) 95 ms vs 29 ms |
| Repeated JSONB path filter | Promoted shadow column (already exists — trigger it by shape rather than by warmup) | JSONB 1.97 ms vs 0.11 ms |
| Repeated deep top-K on a column | Partial sorted index over that column | Top-K 161 ms vs 53 ms |

Crucially, **each structure is per (project, table, shape) and evictable**. An
idle project holds none of them, so the ~2 KiB idle figure survives intact. The
claim becomes precise rather than weakened:

> Idle projects cost ~2 KiB. Active projects spend memory proportional to the
> shapes they actually run, and give up that memory when they stop running them.

That is a *better* claim than a flat one, because it is the thing that is
actually true, and it is defensible under measurement.

## Layer 3 — cracking instead of upfront index builds

The classic objection to adaptive indexes is the build cost: the query that
triggers the build pays for it. **Database cracking** (Idreos et al.) avoids
this — the first range query over a column physically partitions it as a
side-effect of answering, at roughly the cost of the scan it was doing anyway.
The second query on an overlapping range gets a partially-sorted column for
free. After a handful of queries the column is effectively indexed, and no
single query ever paid an index-build cost.

This fits Basin unusually well:

- Columnar storage means cracking operates on one column, not a heap tuple.
- The hot tier (ADR 0016) is already the mutable overlay where a cracked,
  progressively-sorted copy naturally lives.
- Cold files stay immutable and untouched — cracking only reorganises the hot
  copy, so nothing about the bucket layout or Iceberg snapshots changes.

## Layer 4 — the bucket→RAM path

The floor Basin cannot beat is a cold object-store GET at 10–50 ms. Everything
here is about making it rare and making everything after it fast:

- **A local NVMe cache tier between bucket and RAM**, addressed by content hash,
  so a re-read is a local read. Postgres has no equivalent because it has no
  cold tier — but it also never pays 10–50 ms.
- **`io_uring` on Linux** for the local tier: batched, queue-depth-aware reads,
  no thread-per-read.
- **Prefetch driven by shape.** A known keyset-pagination shape predicts the
  next page's files before the client asks. Postgres cannot prefetch what it
  cannot predict; a shape-keyed system can.

## What this does and does not promise

**Does:** removes the *architectural* argument that Basin must lose OLTP point
shapes. With a resident PK map for an active project, a point read is a memory
probe plus a cached-page read — the same order as Postgres's B-tree descent.

**Does not:** beat Postgres on the *first* touch of a cold project. That read
crosses a network to object storage. No technique here changes it; the honest
framing is first-touch latency, and it is the price of the storage model that
produces the 102× disk win and the 2 KiB idle project.

Claiming victory on every shape at every size **including cold first-touch**
would be a claim the architecture cannot support, and this repo's benchmark
culture is built on not making those.

## Suggested order

1. **Shape-frequency counters on the existing `QueryShapeHash`.** Pure
   instrumentation, no behaviour change, and it tells you which structures would
   actually have fired. Cheap and decides everything downstream.
2. **Per-query arenas** — independent of the DataFusion migration and improves
   the multi-tenant story immediately.
3. **Resident PK map**, shape-triggered, LRU-evicted. Kills the two worst
   published losses.
4. **Cached `COUNT(*)`** with write-epoch invalidation. Nearly free.
5. **Cracking in the hot tier** for keyset and top-K shapes.
6. **NVMe cache tier + prefetch.** Largest effort, most infrastructure.

Steps 1, 2 and 4 are worth doing regardless of whether the engine migration
proceeds.
