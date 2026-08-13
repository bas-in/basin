---
title: "DF removal — the removal surface, measured"
nav_section: migration
sidebar_position: 18
summary: "DataFusion is confined to exactly one crate, but 380 of its 566 import lines are genuine API use rather than arrow re-exports. The comforting version of this measurement is wrong, and this document records why."
tags: [migration, datafusion, metrics]
---

# 18 — The removal surface, measured

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. Every other document here sizes a *capability* to be rebuilt.
This one sizes the thing to be *deleted*, and it is deliberately a measurement
rather than an estimate: step 5 of the plan is "delete DataFusion from
`Cargo.toml` last", and the only honest way to know how far away that is, is to
count.

Everything below is reproducible from the commands in the last section.

## The structural result: one crate

```
crates declaring a datafusion dependency: 1   (basin-engine)
```

This is the genuinely good news, and it was not guaranteed. Eight other
crates — `basin-plan`, `basin-exec`, `basin-pgtype`, `basin-pgcatalog` and the
rest — have **no DataFusion dependency at all**, so the owned engine is not
growing inside a DataFusion-shaped hole. `basin-pgcatalog`'s production
dependency graph is three edges wide (`arrow-array`, `arrow-schema`,
`basin-pgtype`), verified separately.

The removal is therefore a single-crate problem. It is a large one:

| | Files | Lines |
|---|---:|---:|
| `basin-engine/src` total | — | 196,599 |
| …importing DataFusion | 63 | 117,614 |
| …of those, UDF/glue files | 23 | 43,505 |
| …of those, core engine files | 40 | 74,109 |

**60% of the crate's source sits in a file that imports DataFusion.** That is
the number that matters for sequencing, and it is why step 5 is last rather
than merely late.

## The hypothesis that measurement killed

DataFusion re-exports arrow. A file writing `use datafusion::arrow::array::…`
is not using DataFusion at all — it is using arrow-rs through an alias, and the
workspace already depends on arrow-rs 58 directly. Those imports can be
rewritten to `arrow_array::` mechanically with zero semantic change.

186 of the 566 `use datafusion::…` lines are exactly that. A third. It is
tempting to conclude the coupling is substantially cosmetic.

It is not:

```
files whose ONLY datafusion imports are arrow re-exports:  1 of 63
```

Just `convert.rs`. Every other file reaches for something that is really
DataFusion — `logical_expr` (64 lines), `SessionContext` (39), `common` (26),
`ScalarValue` (18), `ExecutionPlan` (10), `TableProvider`, `MemTable`,
`TaskContext`. Rewriting all 186 arrow lines would shrink the eventual diff and
make the real coupling legible, but it would **not** decouple a single file
beyond `convert.rs`.

The distinction matters because the two figures support opposite plans. "A third
of the coupling is re-exports" invites treating this as a mechanical sweep. "One
file of sixty-three is decoupled by that sweep" says the remaining 380 lines are
each a design decision. The second is the true one.

## Where the genuine coupling concentrates

Genuine (non-arrow) DataFusion import lines, by file:

| File | Lines |
|---|---:|
| `session.rs` | 19 |
| `catalog_window_exec.rs` | 16 |
| `sort_streaming_limit.rs` | 15 |
| `udf.rs` | 14 |
| `vortex_listing_format.rs` | 13 |
| `jsonb_udf.rs` | 11 |
| `tombstone_cold_scan.rs` | 10 |
| `pg_scalar_aliases.rs` | 10 |
| `pg_agg_udf.rs` | 10 |
| `hot_tombstone.rs` | 10 |
| `citext_analyzer.rs` | 10 |
| `nullif_rewrite.rs` | 9 |

Three shapes, and they do not cost the same:

**Custom physical operators** — `catalog_window_exec.rs`,
`sort_streaming_limit.rs`, `tombstone_cold_scan.rs`, `hot_tombstone.rs`. These
implement DataFusion's `ExecutionPlan` trait. They are the deepest coupling by
kind, but they map onto `basin-exec`'s own `Operator` trait, which exists and is
pull-based, suspendable and cancellable. Cost is proportional to the algorithm
inside, not to the trait.

**Analyzer/rewrite passes** — `citext_analyzer.rs`, `nullif_rewrite.rs`, and
the LATERAL decorrelation in `pg_operators.rs`. These are `AnalyzerRule` /
`OptimizerRule` implementations over DataFusion's `LogicalPlan`. `basin-plan`
has its own `OptimizerRule` and its own IR, so these are ports, not rewrites —
**but they must actually be ported.** The 462× LATERAL win is Basin's own
rewrite, not DataFusion's, and deleting it with its host would be a silent
regression of that magnitude.

**UDF shells** — the 23 udf/glue files, 43,505 lines. [Document 17](./17-udf-rehosting.md)
covers these: the logic is already DataFusion-independent, and what they need is
a session-context abstraction the owned engine does not yet have.

`session.rs` leads the table and belongs to none of these categories. It is the
`SessionContext` itself — the thing every other file reaches through. It cannot
be ported; it has to be replaced by whatever the owned engine's session becomes,
and that decision gates the ENTANGLED half of document 17.

## What this does not measure

An import count is a coupling *census*, not a difficulty ranking. A file with
one import can be harder than a file with nineteen — `array_agg`'s hand-rolled
vectorised `GroupsAccumulator` is one trait import and the single most expensive
item in the entire migration, because `basin-exec`'s `aggregate.rs` has no
vectorised group-wise tier to receive it.

Nor does it count files that depend on DataFusion *behaviour* without importing
it: anything relying on DataFusion's type coercion, its null ordering defaults,
or its expression simplification is coupled and invisible here. The
[differential baseline](./16-differential-baseline.md) is the instrument for
that class, not this document.

## Reproducing these numbers

```sh
# crates with a datafusion dependency
grep -rl '^datafusion' --include=Cargo.toml crates/

# files importing it, and their weight
grep -rl 'use datafusion' --include='*.rs' crates/basin-engine/src | xargs wc -l | tail -1

# genuine API use vs arrow re-exports
grep -rho 'use datafusion::[a-z_]*' --include='*.rs' crates/basin-engine/src | wc -l
grep -rho 'use datafusion::arrow'   --include='*.rs' crates/basin-engine/src | wc -l

# files decoupled by an arrow-import sweep alone
for f in $(grep -rl 'use datafusion' --include='*.rs' crates/basin-engine/src); do
  n=$(grep -o 'use datafusion::[a-z_]*' "$f" | grep -vc 'use datafusion::arrow')
  [ "$n" = 0 ] && echo "$f"
done
```

Re-run them when the figure is claimed to have moved. The point of writing them
down is that "DataFusion is nearly gone" should be a measurement anyone can
repeat, not a status anyone can assert.
