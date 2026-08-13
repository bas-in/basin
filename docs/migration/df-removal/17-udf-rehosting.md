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

**Status: complete.** All eight families are inventoried below, plus the
fourth ABSENT category (math) found along the way.

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
| String | 19 | ~2,502 | 12 | 7 | 0 |
| JSONB | 51 live | ~11,628 | 16 | 31 | 4 |
| Range | 30 live | ~3,155 | 2 | 13 | 0 |
| Math | **see below** | — | — | — | — |
| **Surveyed total** | **~229** | **~40,422** | **73** | **138** | **25** |

### Math: the taxonomy has no slot for it, and that is the finding

The string family behaves as expected — 19 structs across `string_dt_udf.rs`
(14, 1,285 lines) and `string_more_udf.rs` (5, 1,217 lines), zero entangled,
mostly pure text manipulation that ports as plain Rust.

Math does not, because **Basin does not implement it**. Searching the whole of
`basin-engine` for `sqrt`, `cbrt`, `ln`, `log`, `exp`, `power`, `trunc`,
`degrees`, `radians`, `atan2` and the trigonometric family returns **nothing**.
The only math-ish names Basin registers of its own are `div`, `width_bucket`,
`to_number` and `sign`, in `pg_scalar_aliases.rs`.

`SELECT sqrt(2)` works today entirely because DataFusion's built-in
`datafusion-functions` math module is registered on the `SessionContext`.

That makes the math family a **fourth category** this document did not have:

> **ABSENT** — no Basin code exists to re-host. Deleting DataFusion deletes the
> function outright, and it must be *written*, not moved.

The other three categories all describe code that exists and needs a new home.
This one describes a capability that silently belongs to the dependency being
removed. It is invisible to any inventory that counts `ScalarUDFImpl`
definitions, because there is nothing to count — which is exactly why it went
unrecorded through four earlier surveys.

The current state across the owned crates:

| | Math names known |
|---|---|
| `basin-pgtype`'s `pg_proc` table | 7 — `abs`, `ceil`, `floor`, `mod`, `power`, `round`, `sqrt` |
| `basin-exec`'s evaluator | 4 — `abs`, `ceil`, `floor`, `round` |

So even the names Basin has OIDs for are only half executable, and `sqrt` is
already a table entry with no implementation behind it.

**This is not a large amount of work** — the functions are individually trivial
and mostly one `f64` method call each, with the real care going into Postgres's
rounding and error semantics (`round` on `double precision` is half-to-even, on
`numeric` half-away-from-zero; `ln(0)` and `sqrt(-1)` must error rather than
return infinity or NaN). It is, however, work that no one had counted, and it is
a hard blocker on step 5: the `Cargo.toml` line cannot be deleted while `sqrt`
has no implementation.

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

### JSONB: 59 structs, two of them shadowed the same way Geo's are

`jsonb_udf.rs` (7,524 lines), `jsonb_path_udf.rs` (1,952), `jsonb_modify_udf.rs`
(1,395) and `json_build_udf.rs` (757) together register 59 `ScalarUDFImpl`
structs. Eight are dead on arrival, shadowed the same way the geo family's
wave-α structs are: `session.rs` calls `register_jsonb_udfs` first, then
`register_jsonb_path_udfs`, then `register_jsonb_modify_udfs` — and DataFusion's
`register_udf` replaces by name, so whichever registers last wins.

- `jsonb_modify_udf.rs` re-registers `jsonb_typeof`, `jsonb_pretty`,
  `jsonb_strip_nulls`, `jsonb_set` and `jsonb_insert` (`session.rs:380`, after
  `jsonb_udf.rs`'s copies at `session.rs:371`). This one is **intentional and
  self-documented** — `jsonb_modify_udf.rs:23-25` says so in a doc comment,
  and its versions genuinely handle the `text[]` path argument better. The
  five structs left behind in `jsonb_udf.rs` (`JsonbTypeofUdf` at line 1883,
  `JsonbPrettyUdf` at 1944, `JsonbStripNullsUdf` at 2054, `JsonbSetUdf` at
  2100, `JsonbInsertUdf` at 2211) are unreachable from SQL and should not be
  ported.
- `jsonb_path_udf.rs` re-registers `jsonb_path_query_first`,
  `jsonb_path_query_array` and `jsonb_path_match` (`session.rs:379`, also
  after `jsonb_udf.rs`'s copies). This one is **not** documented as
  intentional shadowing anywhere, and it is the more consequential of the
  two — see below.

Re-hosting target: **51 live names across the 59 structs**, same rule as
geo — always take the version that wins the registration race, never the
one left behind.

### JSONB path functions: the real JSONPath engine only covers three of five entry points

`jsonb_path_udf.rs`'s module doc advertises a real JSONPath subset — `$`,
`.key`, `[0]`, `[*]`, `.*`, `..key`, and `[?(@.x > 1)]` filters — implemented
by a genuine recursive-descent parser and evaluator (`parse_jsonpath` /
`jsonpath_eval`, `jsonb_path_udf.rs:526-928`). But that parser only backs
three of the five JSONPath entry points PostgreSQL exposes:
`jsonb_path_query_first`, `jsonb_path_query_array`, and `jsonb_path_match`
(the `@@` operator). It does **not** back plain `jsonb_path_query` in scalar
position, and it does not back `jsonb_path_exists` (the `@?` operator) at
all — `register_jsonb_path_udfs` (`jsonb_path_udf.rs:72-106`) never
registers either name.

Both of those names fall through to `jsonb_udf.rs`'s original, much weaker
implementation: `JsonbPathQueryUdf` (line 2619) and `JsonbPathExistsUdf`
(line 2719) strip the leading `$.` and split what's left on `.` — no `[*]`,
no filters, no recursive descent, no array-index brackets as anything but
literal path segments. `jsonb_path_match` on the *jsonb_udf.rs* copy
(line 2814, dead — shadowed as above) even says so in its own comment:
`// jsonb_path_match  (alias of jsonb_path_exists for simple paths)`.

`jsonb_path_query` in scalar/SELECT-list position is also missing PostgreSQL's
row-expansion behavior entirely — it's a genuine SRF there (one output row
per match), but the scalar stub caps out at one match per input row, same as
`jsonb_path_query_first`. Confirmed against Postgres 18.2:

```sql
-- jsonb_path_exists: filter predicate
SELECT jsonb_path_exists('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)');
-- Postgres: t.  Basin's live jsonb_path_exists: parse_path("a[*] ? (@ > 2)")
-- treats the whole filter text as one literal key segment and never finds
-- it → f.

-- jsonb_path_query: row expansion in scalar position
SELECT jsonb_path_query('{"a":[1,2,3]}'::jsonb, '$.a[*]');
-- Postgres: three rows (1, 2, 3).  Basin's live jsonb_path_query strips
-- '$.' and splits on '.', leaving the single segment "a[*]" which matches no
-- object key → one row, NULL.
```

Whoever re-hosts this family should retire `JsonbPathQueryUdf` and
`JsonbPathExistsUdf` and route both names through `jsonpath_eval` instead —
otherwise the fuller parser's own module doc becomes misleading: it describes
capabilities two of the five call sites into it don't have.

### Range: only `range_eq` knows its own subtype, and every other predicate silently mishandles date/timestamp bounds

Range values are stored as JSON text: `{"l":<lower>,"u":<upper>,"li":<bool>,
"ui":<bool>}` (`range_udf.rs:1-6`). For `int4range`/`int8range`/`numrange`
the bounds are JSON numbers. For `daterange`/`tsrange`/`tstzrange` they are
JSON **strings** — `"2024-01-01"`, `"2024-01-01 00:00:00"` — because dates and
timestamps don't fit in a JSON number.

Every predicate except `range_eq` reads bounds through `range_bound_f64`
(`range_udf.rs:849-856`), which does
`fv.as_f64().or_else(|| fv.as_str().and_then(|s| s.parse().ok()))` — a plain
`f64::from_str` on the bound text. `"2024-01-01".parse::<f64>()` fails
(multiple hyphens are not valid float syntax), so **every finite date or
timestamp bound silently becomes
`None`**, and every one of these functions treats `None` the same as an
*infinite* bound rather than "couldn't parse":

- `range_overlaps` (`range_udf.rs:778-847`): with both bounds unparseable,
  `a_ends_before_b` and `b_ends_before_a` both default to `false` (the `_ =>
  false` arms at lines 826, 837) → the function reports **every pair of
  date/timestamp ranges as overlapping**, even ones nowhere near each other.
- `range_contains_range` (`range_udf.rs:873-958`): `i_hi` parses to `None`,
  which the `(None, _) => false` arm at line 932 treats as "inner extends to
  +infinity, outer can't contain it" → **always `false`** for date/timestamp
  containment, even when the inner range is fully inside the outer one.
- `range_contains_elem` (`range_udf.rs:686-761`): the *element* is also text
  (`"2024-03-01"`), so `elem_s.parse::<f64>()` fails too and the whole
  closure short-circuits via `?` → returns **SQL `NULL`** instead of a
  boolean, for every date/timestamp element test.
- `range_strictly_left` / `range_strictly_right` / `range_adjacent`
  (`RangeRelationalUdf`, `range_udf.rs:977-1118`): every `_ => Some(false)`
  fallback arm fires on the unparseable bound → **always `false`**, so `<<`,
  `>>` and `-|-` never fire for date/timestamp ranges no matter how the
  ranges actually relate.
- `range_merge` / `range_union` / `range_intersection` / `range_diff`
  (`RangeParts`, `range_udf.rs:1322-1384`): `RangeParts.lo`/`.hi` are
  `Option<f64>`; a date bound parses to `None`, and `format_range_parts`
  writes `None` back out as JSON `null` — the storage encoding for
  **infinity**. A `range_merge` of two ordinary, fully-bounded date ranges
  silently produces `(-infinity, +infinity)`, discarding the actual dates
  entirely rather than erroring.
- `isempty` also reads bounds through `range_bound_f64`
  (`range_is_empty`, `range_udf.rs:239-257`), so it inherits the same
  failure — see below.

`range_eq` is the one exception, and the reason is structural, not
accidental: it's the only predicate the pre-parse rewriter hands a third
argument, the subtype name (`range_udf.rs:1160-1197`), and it delegates to
`basin_common::types::range::RangeValue::semantic_eq`, whose `bounds_eq`
helper (`basin-common/src/types/range.rs:473-485`) falls back to plain
string comparison when the numeric parse fails. Every other range function
in `range_udf.rs` has no subtype argument in its signature at all — `isempty`
is `Signature::exact(vec![DataType::Utf8], ...)`, one argument, no way to
know discreteness even in principle — and none of them has the string
fallback either.

Confirmed against Postgres 18.2 (all five diverge from Basin's current
behavior):

```sql
SELECT
  daterange('2024-01-01','2024-06-01') @> '2024-03-01'::date,        -- t (Basin: NULL)
  daterange('2024-01-01','2024-03-01')
    && daterange('2024-06-01','2024-08-01'),                          -- f (Basin: t)
  daterange('2024-01-01','2024-06-01')
    @> daterange('2024-02-01','2024-03-01'),                          -- t (Basin: f)
  daterange('2024-01-01','2024-03-01')
    << daterange('2024-06-01','2024-08-01'),                          -- t (Basin: f)
  daterange('2024-01-01','2024-03-01')
    -|- daterange('2024-03-01','2024-06-01');                         -- t (Basin: f)
```

A second, smaller bug shares the same root cause of "predicates don't know
their own subtype": `range_is_empty` treats an open range between adjacent
integers as non-empty, because nothing tells it the type is discrete.
PostgreSQL canonicalizes `(5,6)::int4range` to `empty` (no integer lies
strictly between 5 and 6); Basin's `range_is_empty` only checks `lo > hi` or
`lo == hi` (`range_udf.rs:239-257`), sees `5 < 6`, and reports non-empty.
Confirmed: `SELECT isempty('(5,6)'::int4range)` is `t` in Postgres 18.2.
`int4range`/`int8range`/`daterange` are exactly the three subtypes this
affects, and they're also three of the six range constructors.

`range_udf.rs` has **zero ENTANGLED** functions — no session, no catalog, no
async, same as regex/FTS — but re-hosting it correctly requires first
deciding how every predicate learns its subtype, not just `range_eq`. That's
a smaller version of the session-context problem this document keeps
surfacing: the missing input isn't DataFusion-shaped, it's "which of six
range types is this text really".

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

> **Measured 2026-08-13, and it changes this entry.** A first vectorised tier
> was built and benchmarked in release against a faithful stand-in for the
> existing row-wise algorithm:
>
> | Shape | Row-wise | Vectorised | |
> |---|---:|---:|---:|
> | 1M rows / 100k groups | 99 ms | 165 ms | **0.60×** |
> | 1M rows / 10 groups | 77 ms | 102 ms | **0.76×** |
>
> Correct on all 334 tests, and slower on both shapes. It is preserved on
> `spike/vectorised-aggregate` and deliberately not merged.
>
> This entry assumed the expensive part of re-hosting `array_agg` is that the
> tier does not exist. That was wrong. The expensive part is building one that
> **beats the scalar loop** — DataFusion's own generic accumulator was 6.5×
> slower than Postgres, which is why `PgArrayAggUdaf` hand-rolls
> `lexsort_to_indices` + `interleave` rather than looping. Reproducing that
> specific algorithm is the work; "add a vectorised tier" is not a plan.
>
> A caution about the measurement itself: the benchmark was initially broken in
> a way that hid this. It pulled a single batch from the operator and compared
> its row count to the full group count — asserting `8192 == 100000` — because
> the operator emits groups in output-sized batches like every other one in the
> crate. A benchmark that measures the first 8192 groups and calls it the whole
> aggregation would have reported a flattering number just as confidently.

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

**Range predicates other than `range_eq` silently mishandle date/timestamp
bounds.** `range_bound_f64` (`range_udf.rs:849`) parses a bound as `f64`;
date and timestamp bounds are stored as JSON strings like `"2024-01-01"`,
which fail that parse and become `None` — indistinguishable from a genuine
infinite bound to every caller. Confirmed against Postgres 18.2:
`range_overlaps` (`range_udf.rs:778`) reports **every** pair of
daterange/tsrange/tstzrange values as overlapping regardless of their actual
dates; `range_contains_range` (`range_udf.rs:873`) reports **containment
always false** for the same three subtypes; `range_contains_elem`
(`range_udf.rs:686`) returns `NULL` instead of a boolean; `range_adjacent` /
`range_strictly_left` / `range_strictly_right` (`RangeRelationalUdf`,
`range_udf.rs:977`) are **always false**; `range_merge` / `range_union` /
`range_intersection` / `range_diff` (`RangeParts`, `range_udf.rs:1322`)
silently rewrite a finite date bound to `(-infinity, +infinity)` rather than
erroring. `range_eq` alone is unaffected — it's the only function the
pre-parse rewriter hands a subtype argument, and its equality helper
(`basin-common/src/types/range.rs:473`) has a string-comparison fallback
none of the others do. See "Range" above for the full mechanism and psql
evidence.

**`jsonb_path_exists` and scalar-position `jsonb_path_query` implement a
different, much weaker JSONPath than the other three JSONPath functions.**
`jsonb_path_query_first`, `jsonb_path_query_array` and `jsonb_path_match`
resolve to `jsonb_path_udf.rs`'s real recursive-descent parser (wildcards,
filters, recursive descent). `jsonb_path_exists` and plain `jsonb_path_query`
(`jsonb_udf.rs:2719` and `2619`) are never re-registered by that module, so
they keep `jsonb_udf.rs`'s original dot-split-only parser: no `[*]`, no
`[?(...)]` filters, no `..key`, and no SRF row-expansion for `jsonb_path_query`
in scalar position. Confirmed: `jsonb_path_exists('{"a":[1,2,3]}'::jsonb,
'$.a[*] ? (@ > 2)')` is `t` in Postgres 18.2, `f` on Basin's live
implementation. See "JSONB path functions" above.

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
