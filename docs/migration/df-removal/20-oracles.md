---
title: "DF removal — what we test against, and in what order"
nav_section: migration
sidebar_position: 20
summary: "Three oracles, used in sequence: Postgres for what is correct, the incumbent DataFusion path for what changed, and recorded answers for after it is gone. The middle one is free today and impossible later, which decides the order."
tags: [migration, testing, oracles, datafusion]
---

# 20 — What we test against, and in what order

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [19](./19-expires-at-removal.md) lists what stops being safe when
DataFusion goes. This document answers the question that follows from it: **how
do we know the owned engine agrees with what Basin does today, and how do we
keep knowing after the thing it agreed with is deleted?**

## The problem with one oracle

PostgreSQL is the authority on what is *correct*. It is not the authority on
what Basin currently *does*.

Those differ, and the difference matters. Basin already diverges from Postgres in
20 known places, tracked in
[`differential-baseline.txt`](./16-differential-baseline.md). It also has
behaviour Postgres has no opinion about — Vortex file pruning, the hot-tier and
tombstone overlay, RLS predicate injection, promoted-JSONB shadow columns.

So a change that makes the owned engine disagree with the incumbent falls into
one of three buckets, and only a Postgres oracle cannot tell them apart:

1. The owned engine is wrong. Fix it.
2. The incumbent was wrong, and the owned engine is right. Prune a baseline entry.
3. Both are defensible and the behaviour is genuinely changing. Decide deliberately.

Distinguishing these needs **both** oracles, and one of them has an expiry date.

## Three oracles, in the order they become available

### 1. The incumbent, in-process — available NOW, impossible later

`owned_engine.rs` is a bridge: it runs the owned engine and falls back to
DataFusion when it cannot serve a query. **Both engines already exist in the
same process, over the same data, reachable from the same call site.**

That makes a shadow-compare mode nearly free: run the statement through both,
diff schema and rows, return the owned result, record the disagreement. No
second oracle to install, no fixtures to maintain, and it applies to *every*
query any existing test happens to issue — this crate alone runs ~1,876 tests
that issue real SQL.

This is the highest-value instrument in the whole program and it is available
for exactly as long as DataFusion is still linked. **It should be built first
and run hardest, because it is the only one that gets harder later rather than
easier.**

Cost: everything runs twice. That is fine for a diagnostic mode and unacceptable
as a default, so it is env-gated off like `BASIN_OWNED_ENGINE`.

### 2. Postgres, differential — available always

The [differential suite](./16-differential-baseline.md) against a live
PostgreSQL 18.2. This is the authority on correctness and the only oracle that
survives every other change. It is also the one that catches the failure mode
the incumbent cannot: **where Basin and DataFusion agree with each other and
both differ from Postgres.** Two engines sharing an assumption is not evidence.

Its weakness is size. 79 tests is small for a database, which is why the corpus
is being expanded by feature area rather than by whatever comes to mind.

### 3. Recorded answers — the one that survives removal

The instinct is to keep a released DataFusion-based binary around to diff
against after removal. That works, but it keeps a whole dependency alive as a
test fixture, with its own build, its own version drift, and its own reasons to
break.

**Keep the answers, not the engine.** Run a corpus through the incumbent once,
while it still exists, and record the results as golden files. After that the
comparison needs no DataFusion at all — just the recorded output and a diff.

This is strictly better than a pinned binary for the same reason a photograph
beats keeping the subject: the thing being preserved is the behaviour, not the
machinery. It also makes the recording step an explicit, dated artifact rather
than an implicit dependency on whatever a rebuild happens to produce.

The corpus worth recording is the union of the differential suite, the coverage
probe, and whatever the shadow mode found interesting — recorded **before** step
5, because after it the opportunity is gone permanently.

## What this means for sequencing

The order is forced by availability, not by preference:

| | Oracle | Available | Build it |
|---|---|---|---|
| 1 | Incumbent, in-process shadow | until step 5 | **now** |
| 2 | Postgres differential | always | continuously |
| 3 | Recorded golden answers | forever, once recorded | **before step 5** |

The trap is doing them in the intuitive order — expand the Postgres suite first,
because it is the one that obviously matters — and reaching step 5 having never
run the in-process comparison. At that point the question "did the owned engine
change any answer Basin used to give?" becomes unanswerable, and it is the
question a user of the existing product actually cares about.

## The removal itself, in two moves rather than one

"Delete DataFusion" is usually imagined as one commit. It is better as two:

**Move it out of production, keep it in dev.** `basin-engine`'s shipped code
stops naming DataFusion; the dependency survives under `[dev-dependencies]` for
the shadow comparison alone. Users get a build with no DataFusion in it; the
project keeps its oracle. This is the point at which the removal is real for
anyone downstream.

**Then drop the dev-dependency**, once the golden answers are recorded and the
shadow mode has nothing left to say.

The [removal surface](./18-removal-surface.md) census makes the first move
measurable: DataFusion is confined to one crate, and 380 of its 566 import lines
are genuine API use rather than arrow re-exports. Those 380 are what has to
leave production code; the arrow re-export lines can be rewritten mechanically
at any time and decouple nothing on their own.
