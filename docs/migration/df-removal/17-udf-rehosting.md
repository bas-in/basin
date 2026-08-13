---
title: "DF removal — UDF re-hosting inventory"
nav_section: migration
sidebar_position: 17
summary: "The 249 UDFs are mostly not a porting problem. Their logic is already DataFusion-independent; what they need is a session-context abstraction the owned engine does not have, and several are fake stubs returning wrong answers today."
tags: [migration, udf, functions, pg-compat]
---

# 17 — UDF re-hosting inventory

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [04](./04-function-gap.md) measured the *size* of this block —
238 `ScalarUDFImpl` plus 11 aggregate/window across ~37k LOC, the largest
untouched piece of the migration. This document asks the different question:
**what kind of work is it?**

**Status: partial.** Four families are inventoried below. String, math, JSONB,
geo and range are not yet surveyed.

## The headline: it is not mostly a porting problem

Across the families surveyed, the logic is **already
DataFusion-independent pure Rust**. The advisory-lock module's own docs note it
has zero external dependencies; the sketches live in `basin-sketch`; the
wasmtime sandbox is standalone. What couples them to DataFusion is, in most
cases, only the `ScalarUDFImpl` trait shell.

What they actually need is something the owned engine does not have:

> **A session-context abstraction reachable from function resolution.**

`basin-exec`'s `eval.rs` is scalar-expression-only and has no notion of a
session, a lock table, a cancellation channel, or an HTTP client — its own
module docs say so. Advisory locks, `pg_cancel_backend`, the cron and net glue,
and the sequence rewrite path all need a session context threaded into wherever
function calls resolve. **That plumbing is a bigger lift than porting any
individual function's logic**, and it is shared by every ENTANGLED entry below.

## Surveyed families

| Family | Names | LOC | TRIVIAL | MECHANICAL | ENTANGLED |
|---|---:|---:|---:|---:|---:|
| Aggregate / window | 10 | ~4,265 | 0 | 9 | 1 |
| System / misc | ~25 | ~7,482 | 13 | 2 | 15 |
| Date / time / interval | 26 | ~3,116 | 10 | 14 | 2 |
| Regex / FTS / trigram | 25 | ~4,454 | 8 | 17 | **0** |
| Geo / PostGIS | 43 live | ~3,820 | 12 | 45 | 3 |
| **Surveyed total** | **~129** | **~23,137** | **43** | **87** | **21** |

Not yet surveyed: string, math, JSONB, range.

The regex/FTS/trigram family is worth singling out: **zero entangled**, across
4,454 lines including the whole full-text search stack. `tsvector` and `tsquery`
are not Arrow extension types — they are plain `Utf8` holding a canonical text
form, and every parser, ranker and evaluator over them is self-contained Rust
touching DataFusion only at the `invoke_with_args` boundary. Since `eval.rs`
already speaks raw `ArrayRef`/`RecordBatch` rather than DataFusion's
`ColumnarValue`, the shell swap is mechanical. `ts_rank_cd`'s sliding-window
minimal-cover algorithm is the most involved thing in the family and is still
pure arithmetic.

### Geo: 59 structs behind 43 names, and 16 of them are dead

The geo family was assumed to be ~34 structs. It is **59**, in two
generations. Thirty-three "wave-α" structs handle POINT only, stored as
`FixedSizeBinary(21)`, with many bodies degenerate for non-POINT semantics —
`st_area` returns a constant `0.0`, `st_centroid` is identity, `st_numpoints`
returns `1`. Twenty-six later general-geometry structs handle real
LineString/Polygon/Multi* over variable-length WKB.

**Sixteen SQL names are registered twice**, and because `register_udf`
overwrites by name, the general implementation wins and the wave-α struct is
unreachable from SQL — while still compiling, and still being unit-tested
directly by tests that instantiate it by hand. So those tests pass against code
no query can reach.

The re-hosting target is therefore **43 live names, not 59 structs**, always
taking the general implementation where a name is shadowed. Porting all 59
would either duplicate OIDs or silently pick the degenerate POINT-only version
of a function that currently works properly.

The geometry itself is in excellent shape for the migration: `basin-geo` is a
pure-Rust crate with zero Arrow and zero DataFusion dependency, and `geo` and
`proj4rs` are likewise independent. The coupling is only the trait shell — with
one real exception. `st_srid` and `st_transform` read `BASIN_SRID` out of
DataFusion's per-call `ScalarFunctionArgs.arg_fields[].metadata()`, and
`basin-exec`'s evaluator has no equivalent: it sees arrays, not the source
columns' Field metadata. That is a design question, not a translation.

## The cheapest win, and the most expensive one

**Cheapest:** the ~17 `pg_catalog` stub functions in `pg_catalog_udf.rs` are
hardcoded constants or trivial format strings with no session or catalog
dependency — `pg_table_is_visible` returns `true`, `current_schema` returns
`'public'`, `pg_relation_size` returns `0`. They port as plain Rust functions
in an afternoon.

**Most expensive:** `array_agg` is the one genuine *algorithmic* entanglement.
`PgArrayAggUdaf` hand-rolls a vectorised `GroupsAccumulator` — global
`lexsort_to_indices` plus `interleave` plus a partial-state struct layout —
specifically because DataFusion's generic accumulator was 6.5× slower than
Postgres at 1M rows. `basin-exec`'s `aggregate.rs` has **no vectorised
group-wise tier**; its own header says that is future work. So re-hosting
`array_agg` means building infrastructure Basin does not have, or accepting a
documented performance regression.

## Traps worth knowing before anyone starts

These are the findings that change how the work should be approached, rather
than its size.

**Several functions are fake stubs that return wrong answers today.** Not
simplified — wrong.
- `array_contains` (`@>`) checks `rhs.len() <= lhs.len()` instead of element
  containment.
- `arrays_overlap` (`&&`) checks "both non-empty" instead of intersection.
- `timezone` / `at_time_zone` pass non-UTC zones through **unchanged**, so any
  zone other than UTC silently produces a wrong value with a UTC annotation
  attached.
Re-hosting these mechanically would carry the wrongness forward under a new
implementation that looks more trustworthy. Each needs a decision: fix, or
carry the stub-ness forward with equal prominence.

**The sequence UDFs are a red herring.** `nextval` / `currval` / `setval` /
`lastval` in `seq_udf.rs` look like ordinary `ScalarUDFImpl`s but are dead-code
tombstones that always error. The real logic is `rewrite_sequence_calls`, a
**pre-parse SQL string rewriter** that resolves literal-argument sequence calls
via an async catalog call before the parser runs — written that way precisely to
dodge DataFusion's synchronous `invoke` being unable to call async catalog
methods. Whoever re-hosts these must know the struct is not the target.

**Some "UDFs" in `hypertable.rs` are not UDFs at all.** `create_hypertable`,
`add_retention_policy`, `drop_chunks` and friends are `match_*` SQL-text pattern
matchers that intercept whole statements and mutate a registry directly,
bypassing the function-call machinery entirely. Only `time_bucket` is a real
`ScalarUDFImpl`. The statement-interception pattern still needs a home in the
owned engine, but it is not UDF work.

**`to_char` is registered three times.** `ToCharPgUdf` in `udf.rs`,
`ToCharMoreUdf` in `datetime_more_udf.rs`, and `ToCharIntervalUdf` under the
different name `to_char_interval`. DataFusion's `register_udf` overwrites by
name, so **registration order decides which one wins** — and re-hosting must
pick a canonical implementation rather than silently dropping the loser's logic.
Worse, nothing visibly routes `to_char(interval, fmt)` to `to_char_interval`,
and the winning `to_char` does not handle interval input at all.

**`statement_timestamp` / `transaction_timestamp` do not work today.** Their
session state exists but no executor hook ticks it, so both fall back to
`Utc::now()` per call and do not implement Postgres's stable-within-statement
semantics. They are ENTANGLED because fixing them needs statement and
transaction lifecycle events, not because the arithmetic is hard.

**Interval maths uses a 30-day month in two places** — `date_bin` and
`extract_epoch_from_interval` — with no leap-year awareness. Both self-flag it.
A silent wrong answer if re-hosted without preserving or fixing the caveat.

**`net_http_get` / `net_http_post` call `block_on` inside a scalar function.**
That is a blocking-in-async hazard to fix deliberately during re-hosting, not
to copy forward.

**Wiring debt travels with some UDFs.** `substring_regex` is unreachable
without a SQL-text rewrite turning `SUBSTRING(x FROM 'regex')` into a call,
because DataFusion's native `SUBSTRING` will not take a bare regex second
argument — and that rewrite is still a TODO. `trgm_glue.rs` carries ~250 lines
of similar rewriters lowering `%`, `<%` and `<->`, with quote-aware scanning to
disambiguate `%` from modulo and `<->` from pgvector's distance operator. None
of that is UDF-body work, and all of it has to move with them.

**More honest stubs, in the same family.** `regexp_split_to_table` returns its
input unchanged — real SETOF row-splitting is deferred. `regexp_matches` reuses
single-match logic where Postgres returns every match. `websearch_to_tsquery` is
a bare alias to `plainto_tsquery` with no websearch syntax at all: no quoted
phrases, no `-exclude`, no `OR`. Each is documented in place, and each will look
like a working function to anyone porting it.

## What this changes about sequencing

The ENTANGLED count is not a porting backlog; it is one shared prerequisite
wearing eighteen hats. Building the session-context abstraction first collapses
most of it. Doing the TRIVIAL families first — the `pg_catalog` stubs, the
date arithmetic — buys coverage cheaply while that design is settled.

Fixing the fake stubs is separable from the migration entirely: they are wrong
on `main` today.
