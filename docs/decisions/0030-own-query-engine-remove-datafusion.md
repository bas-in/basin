---
title: "ADR 0030 — Remove DataFusion; build Basin's own query engine"
nav_section: decisions
sidebar_position: 30
summary: "DataFusion leaves the dependency tree entirely. Basin builds its own logical IR, optimizer, and physical executor on top of arrow-rs, in order to reach Postgres fidelity that DataFusion structurally prevents. The delete lands last; the branch stays green throughout."
tags: [architecture, query-engine, pg-compat, technical-debt]
---

# 0030 — Remove DataFusion; build Basin's own query engine

- **Status:** Accepted (2026-08-12), in progress
- **Tags:** architecture, query-engine, pg-compat, technical-debt
- **Cross-references:**
  [ADR 0014 — pg_query as canonical parser](./0014-pg-query-as-canonical-parser.md),
  [ADR 0015 — Vortex storage format](./0015-vortex-storage-format.md),
  [ADR 0002 — no Postgres extensions](./0002-no-postgres-extensions.md),
  [ADR 0022 — system schema namespacing](./0022-system-schema-namespacing.md),
  [ADR 0024 — UUID/Decimal128 storage](./0024-uuid-decimal128-storage.md),
  [ADR 0027 — binary JSONB encoding](./0027-binary-jsonb-encoding.md)

## Context

Basin has been built on DataFusion 53 since inception. DataFusion earned its
place: the analytics wins Basin publishes against Postgres 18 — LATERAL join
462×, star join 261×, correlated subquery 113×, `PERCENTILE_CONT` 19× — are
produced by DataFusion's logical optimizer and vectorised executor, not by
anything Basin wrote. That is a real debt owed to a good library, and this ADR
does not pretend otherwise.

The problem is that DataFusion's ceiling is no longer a performance ceiling. It
is a **Postgres-fidelity** ceiling, and Basin's product is Postgres
compatibility.

Measured state of the coupling as of this ADR (workspace-wide grep, 2026-08-12):

- **1,016 `datafusion` references across 69 files** — 67 in `basin-engine`, one
  in `basin-storage`, one in `basin-bench-harness`. The coupling is
  well-contained by crate, which is what makes removal tractable at all.
- **279 of those references are `datafusion::arrow::*` re-exports.** arrow-rs
  is a separate dependency that Basin keeps. Those references are a rename, not
  work.

The fidelity ceiling shows up as concrete, load-bearing damage:

- **`crates/basin-engine/src/pg_operators.rs` is 9,546 lines of string
  rewriting** that exists only because DataFusion's SQL surface is not
  Postgres's. Its own header concedes the approach: "pure string-manipulation
  functions — no AST", and "best-effort … do NOT handle dollar-quoted strings,
  comments, or identifier quoting." A correctness hazard sits in front of the
  planner, and it is structural, not a bug to be fixed.
- **`pg_catalog` is a simulation.** `info_schema_provider.rs` is 2,568 lines of
  `TableProvider` shims, and notes at line 80 that DataFusion's `TableProvider`
  "can't see through the `Arc<dyn Catalog>`". Real clients — `psql \d`,
  `pg_dump`, ORM introspection, migration tools — query the catalog as ordinary
  joinable relations with stable OIDs. A per-query shim cannot serve them.
- **Set-returning functions cannot exist.** `jsonb_udf.rs:16`:
  `jsonb_array_elements` and its family "cannot be true SRFs inside DataFusion".
  `generate_series` and `unnest` in the target list are ordinary Postgres, and
  are unavailable.
- **DML is not a relation.** `executor.rs:3128` and `:13038` — DataFusion cannot
  plan DML as a relation, so data-modifying CTEs
  (`WITH x AS (INSERT … RETURNING …) SELECT …`) are unexpressible.
- **RLS is bypassable by construction.** `executor.rs:3182` documents a separate
  RLS gate needed because the fast paths skip DataFusion's logical planner. A
  security rewrite that some execution paths can route around is the wrong
  shape for a security rewrite.
- **Upstream limitations Basin cannot fix**: correlated LATERAL left unrewritten
  (`pg_operators.rs:2246`), a DataFusion 53 optimizer bug worked around for
  multi-column recursive CTEs (`:5069`), `= ANY(ARRAY[…])` coercion
  (`:1593`, `:2409`), `WITH TIES` silently degraded to `ONLY`
  (`select_advanced.rs:455`), and synchronous UDFs that cannot be preempted,
  capping `statement_timeout` (`executor.rs:12`).

Two decisions already made point the same direction. ADR 0014 retired
DataFusion's `sqlparser` frontend for libpg_query, because the dialect was
"materially incomplete". And five modules — `fast_select`, `fast_aggregate`,
`values_fast`, `index_probe`, `point_join` — already execute queries without
DataFusion, because its per-query planning cost dominates OLTP latency.
`fast_select.rs:5` states it plainly. Basin's best OLTP numbers come from code
paths that do not touch DataFusion at all.

Basin has, in other words, already left DataFusion at the frontend and at the
OLTP hot path. What remains is an analytical execution engine whose SQL
semantics Basin must continuously translate into and out of.

## Decision

**Remove DataFusion from Basin entirely. No fallback path, no trait-boundary
retention, no vendored fork. DataFusion leaves `Cargo.toml`.**

Basin builds its own logical IR, optimizer, and physical executor. Concretely:

1. **arrow-rs 58 stays.** All compute kernels — filter, take, sort, compare,
   hash, cast — remain arrow's. Basin builds plan orchestration on top of them,
   not vectorised kernels from scratch. This is the single largest reason the
   effort is bounded.
2. **The logical type system becomes Postgres's, not Arrow's.** Arrow remains
   the physical representation. `typmod`, the `unknown` type for untyped
   literals, domains, composites, enums, ranges, and genuine multidimensional
   arrays are modelled directly, consistent with ADR 0024 and ADR 0027.
3. **`pg_catalog` becomes real relations** backed by Basin's actual metadata,
   with OIDs matching Postgres's well-known values for builtin types. Faithful
   catalog behaviour is a first-class goal, not an emulation layer.
4. **Extensions remain native Rust crates — no `.so`, no dlopen.** This extends
   rather than contradicts ADR 0002. `CREATE EXTENSION` becomes a catalog
   operation that registers an already-compiled crate's functions into
   `pg_proc`, so clients issue the statement and it works.
5. **The parser stays libpg_query** (ADR 0014). Lowering goes
   pg_query parse tree → Basin IR directly, which deletes the string-rewriting
   layer and replaces it with operator resolution against a real `pg_operator`
   catalog.
6. **Single-partition streaming execution first.** Basin already pins
   `target_partitions = 1` by default (`session.rs:2705`); parallelism gets a
   clean extension point rather than being built now.
7. **Security rewrites become structurally unbypassable.** RLS, CHECK, and FK
   enforcement are mandatory plan rewrites in one planner, not gates bolted onto
   each execution path.

### Sequencing

**DataFusion is deleted last, not first.** The replacement must exist before the
delete can compile. The branch (`feat/own-engine-remove-datafusion`) builds and
passes tests at every commit; the final commit in the sequence is the
`Cargo.toml` line removal. Migration analysis lives in
`docs/migration/df-removal/`.

The distance to that final commit is measured, not asserted, in
[`18-removal-surface.md`](../migration/df-removal/18-removal-surface.md).
DataFusion is confined to exactly one crate — `basin-engine` — but 63 files
holding 117,614 lines, 60% of that crate's source, import it. A third of the
`use datafusion::…` lines are arrow re-exports that could be rewritten
mechanically; doing so would decouple **one** file of the sixty-three. The
remaining 380 lines are each a design decision.

> **Amended 2026-08-13 — the surface re-counted by category, and the removal
> re-planned as two moves.** From
> [`17-udf-rehosting.md`](../migration/df-removal/17-udf-rehosting.md) §1 and §8
> and [`20-oracles.md`](../migration/df-removal/20-oracles.md).
>
> **The dependency, exactly.** `datafusion = "53"` at `Cargo.toml:149`, consumed
> by **`basin-engine` alone**. `basin-plan/Cargo.toml:19` mentions DataFusion in
> a comment — it is the crate that *replaces* `pg_operators.rs`, so it names the
> thing it replaces — and has no dependency on it. `grep -rln datafusion
> --include=Cargo.toml` matches three files (workspace root, `basin-engine`,
> `basin-plan`) and this has been misread as three consumers, including in
> commit `d0e14e87`'s message. There is one consumer. That materially simplifies
> the endgame: the two-move removal below touches exactly one crate's manifest.
>
> **Three file counts are in circulation and all three are correct at different
> scopes.** They must not be quoted interchangeably:
>
> | Count | Scope | Source |
> |---:|---|---|
> | **69** | files mentioning `datafusion` **workspace-wide** — 67 engine, one `basin-storage`, one `basin-bench-harness` | the Context bullet above |
> | **67** | of `basin-engine/src`'s 133 files mention `datafusion` | [17](../migration/df-removal/17-udf-rehosting.md) §1, `grep -rl` |
> | **63** | of those actually carry a `use datafusion::…` import | [18](../migration/df-removal/18-removal-surface.md) |
>
> **69 is not the engine figure**, and quoting it as one overstates the engine's
> coupling by two files.
>
> **1,017 references** across those 67 files (`grep -rc`, summed). They are five
> removal problems with different owners, not one:
>
> | Category | Refs | Files |
> |---|---:|---:|
> | Function hosting (`ScalarUDFImpl`/`AggregateUDFImpl` bodies + registration) | **521** | 29 |
> | Physical plan nodes + physical optimizer | 164 | 8 |
> | Session / context / driver | 118 | 6 |
> | Table providers / scan | 74 | 7 |
> | Logical optimizer / analyzer rules | 59 | 5 |
> | Type conversion | 44 | 2 |
> | Remainder | 37 | 10 |
>
> Function hosting is **51%** of the reference count and shares no owner with the
> other four. A plan quoting "1,017 references" should not imply that finishing
> the functions finishes the migration; one quoting "372 UDF sites" should not
> imply the functions are the whole 1,017 — 372 was a grep line count, not a
> function count.
>
> **The delete is two moves, not one.** The paragraph above makes the final
> commit the `Cargo.toml` line removal.
> [20](../migration/df-removal/20-oracles.md) supersedes that:
>
> 1. **DataFusion moves to `[dev-dependencies]`.** Shipped `basin-engine` code
>    stops naming it; it survives for the shadow comparison alone. **This is the
>    point at which the removal becomes real for anyone downstream** — users get
>    a build with no DataFusion in it while the project keeps its oracle.
> 2. **The dev-dependency is dropped**, once golden answers are recorded and the
>    shadow mode has nothing left to say.
>
> What must leave production code in move 1 is the ~380 genuine API import lines
> of the 566 total; the 186 `datafusion::arrow::*` re-export lines are a
> mechanical rename that decouples exactly one file (`convert.rs`) and can happen
> at any time.

## Consequences

### Positive

- Postgres fidelity stops being bounded by a third party's dialect. The
  blocked-feature list above becomes ordinary backlog.
- The 9,546-line string rewriter is deleted rather than extended.
- `pg_catalog` fidelity unlocks the tooling ecosystem — psql, pg_dump, ORM
  introspection, migration tools — which is worth more to Basin's users than
  any additional benchmark multiplier.
- The arrow 58 / DataFusion 53 / vortex 0.71 version lockstep documented in
  ADR 0015 and the root `Cargo.toml` loses one of its three legs.
- Cost modelling can use Basin's real unit — an S3 GET — instead of
  DataFusion's local-page assumption.

### The function surface, and the 49 nobody was counting

> **Added 2026-08-13.** Recorded here because it is the consequence that was
> discovered rather than predicted, and the only one where deleting the
> dependency removes a *working* feature with no code to port. Source:
> [`17-udf-rehosting.md`](../migration/df-removal/17-udf-rehosting.md), measured
> against `6f0d9630`.

`basin-engine` registers **308 distinct SQL function names** on DataFusion
(~362 registration calls; 23 names registered twice under a `pg_catalog.`-
qualified alias, 18 more from two modules). **`basin-exec` implements 12 of
them.** 296 remain.

Separately, and not previously counted: **49 `pg_catalog` function names are
served today only by DataFusion's own builtin registry.** No Basin code
registers them and none exists to port. They include `date_trunc`, `date_part`,
`now`, `md5`, `lpad`, `rpad`, `initcap`, `repeat`, `concat_ws`, `overlay`,
`starts_with`, `stddev`, `bool_and`, `bool_or`, `array_length`, `cardinality`,
`string_to_array`, `random`, `ntile`, `cume_dist` and `percent_rank`. These are
ordinary SQL that Basin answers correctly today and stops answering the moment
`Cargo.toml:149` is deleted. They are invisible to every inventory that greps
for what Basin registers, **because Basin registers nothing**.

**The real remaining function surface is 296 + 49 = 345 names.** The 49 are
strictly harder to notice and strictly easier to write.

> **Ambiguity, recorded rather than resolved: 49 is a floor, not a count.** It
> is 48 names found by applying a `fn name()`/`aliases()` heuristic to the
> `datafusion-functions{,-aggregate,-nested,-window}-53.1.0` crates, plus
> `percent_rank`, which that heuristic missed. `percent_rank` was missed because
> `datafusion-functions-window` generates it by macro rather than declaring a
> literal name — **and `rank` and `dense_rank` are generated the same way**, so
> the heuristic is known to under-count and the true figure is higher than 49 by
> an unmeasured amount. Document 17's YAML frontmatter summary still says 48
> while its body §3 says 49; **the body is the one to trust**, and the
> discrepancy is left visible here rather than smoothed into false precision.

### Oracles, and the one with an expiry date

> **Added 2026-08-13**, from
> [`20-oracles.md`](../migration/df-removal/20-oracles.md). This supersedes
> nothing above; it sequences the Phase 0 work the 2026-08-12 amendment under
> *Mitigations* made a prerequisite.

PostgreSQL is the authority on what is **correct**. The incumbent DataFusion
path is the authority on what Basin currently **does**. Those differ — Basin has
20 known deliberate divergences from Postgres, plus behaviour Postgres has no
opinion about (Vortex file pruning, the hot-tier and tombstone overlay, RLS
predicate injection, promoted-JSONB shadow columns) — so a disagreement between
the owned engine and the incumbent is one of three things (owned engine wrong;
incumbent wrong; behaviour deliberately changing), and a Postgres oracle alone
cannot tell them apart.

Three oracles, ordered by availability rather than by preference:

| | Oracle | Available | Build it |
|---|---|---|---|
| 1 | Incumbent, in-process shadow compare | **only until removal** | **now** |
| 2 | PostgreSQL differential | always | continuously |
| 3 | Recorded golden answers | forever, once recorded | **before the delete** |

Oracle 1 is nearly free — `owned_engine.rs` already runs both engines in one
process, over the same data, from the same call site — and it is the only
instrument that gets *harder* later rather than easier. Oracle 2 catches the
failure mode oracle 1 structurally cannot: where Basin and DataFusion agree with
each other and both differ from Postgres. Oracle 3 keeps the answers, not the
engine: run the corpus through the incumbent once while it exists and record the
results, rather than preserving a DataFusion-linked binary as a test fixture.

The trap is the intuitive order — expand the Postgres suite first, because it is
obviously the one that matters — and arriving at the delete having never run the
in-process comparison. At that point "did the owned engine change any answer
Basin used to give?" becomes permanently unanswerable, and it is the question a
user of the existing product actually cares about.

### Negative — stated plainly

- **Cost.** Measured estimate: ~62–98k LOC of new engine code (midpoint ~75k),
  ~115k including tests at Basin's own observed code:test ratio, plus a further
  ~15–25k for catalog and type fidelity. Roughly 18–30 engineer-months. This is
  the dominant consequence and it is not hedged.
- **The optimizer is the risk concentration.** DataFusion's
  `decorrelate_predicate_subquery` (1,780 lines), `push_down_filter` (3,469),
  `eliminate_cross_join` (1,133), and `extract_equijoin_predicate` (400) are
  what produce the published win table. Underscoping the optimizer keeps the
  engine and loses the benchmarks.
- **Semantic drift is the highest-severity risk** — a wrong answer is worse
  than a slow one. DataFusion's 475k LOC encode years of edge-case fixes in
  NULL semantics, decimal overflow, timestamp coercion, and NaN ordering. Basin
  will rediscover a long tail of these.
- **Opportunity cost.** Basin is pre-alpha with a v0.1 cut-off
  (`docs/V0_1_SCOPE.md`). Engine work competes directly with that roadmap.
- **Vortex coupling.** `vortex-datafusion` 0.71 implements DataFusion's
  `FileFormat` trait. Removing DataFusion means reading Vortex through the base
  `vortex` crate directly. This is being assessed as a potential blocker.

  > **Resolved 2026-08-12: not a blocker.** `basin-storage` carries no
  > DataFusion dependency at all and already reads both formats directly —
  > Vortex through `open_buffer().scan()` with projection and filter, Parquet
  > through arrow-rs with row-group, page-index and bloom pruning. The coupling
  > is confined to `crates/basin-engine/src/vortex_listing_format.rs`, a
  > 1,184-line adapter that exists to present files to DataFusion's
  > `ListingTable`. It is **deleted, not ported**, and takes four direct
  > dependencies with it. Verified three ways: `datafusion-pruning` has zero
  > call sites, Basin implements `PruningStatistics` nowhere, and the crate
  > reaches `Cargo.lock` only via DataFusion-side dependents. See
  > [`06-scan-and-storage.md`](../migration/df-removal/06-scan-and-storage.md).

- **The instruments work, and that is the bad news as well as the good.**
  *Added 2026-08-13.* The differential harnesses built for this migration have
  already found, every item traceable to a commit:

  - **A panic reachable from ordinary SQL.** `round(n::numeric, -2147483648)`
    computed `scale - ndigits` on `i32` with caller-supplied `ndigits`;
    24 call sites reported `attempt to subtract with overflow`. It killed the
    query. (`6f0d9630`)
  - **Silent wrong answers.** `lower('{"l":1,"u":5,…}')` returned `1` instead of
    the string — `range_udf.rs:520` registers the range bound accessors under
    the bare names `lower`/`upper`, shadowing the string builtins, and
    disambiguates on a *content* heuristic where PostgreSQL dispatches on
    argument **type** (`d0e14e87`). `replace(s, '', to)` returned
    `"0h0e0l0l0o0 0w0o0r0l0d0"` for `"hello world"`, because Rust's
    `str::replace` matches an empty pattern at every character boundary
    (`6f0d9630`). `OVER w` lowered as an **empty window** — the code checked
    `WindowDef.refname`, which gram.y sets only for the copy-and-extend form
    `OVER (w …)`, so it refused the right construct and mis-lowered the wrong
    one (`7285398d`).
  - **127 real bugs across five `pg_catalog` relations that had passed every
    shape check** (`c09b783b`), found when the oracle stopped checking shape and
    started diffing all 10,443 cells against the live server. The load-bearing
    class is **polymorphic functions monomorphized**: `pg_proc` oid 3106 read
    `lag(integer) -> integer` where the real row is
    `lag(anyelement) -> anyelement`, and the same for `lead`, `first_value`,
    `last_value`, `nth_value`, `unnest` and `array_agg`; in `pg_operator`,
    `@>`, `<@` and `&&` claimed `integer[]` where the real rows are `anyarray`.
    A driver resolving parameter types from `pg_proc` would have refused every
    non-`int4` `lag()` in existence.

  **This is evidence for the approach and against the premise.** Every one of
  those defects predates the owned engine and was live in shipped behaviour.
  They were found because someone pointed a real oracle at a surface previously
  checked only for shape. **The pre-existing surface was less correct than
  believed** — which raises the cost of this migration in one direction, since
  there is more to fix than the port itself, and lowers the risk in another,
  since "matching the incumbent" was never the bar it was assumed to be.

- **The oracle work is itself a source of hazards, not only a detector of them.**
  *Added 2026-08-13.* The shadow-compare design doc specified its double-write
  guard as a `match` on `NodeEnum::SelectStmt`. That is unsafe:
  `WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM x` roots at a
  `SelectStmt` whose `with_clause` holds the `InsertStmt`, **so the documented
  guard would have double-written on every execution.** It was replaced with
  `is_side_effect_free()`, which walks the WITH list and each set-op arm's own
  WITH and refuses anything unrecognised rather than assuming it is safe
  (`02f8008f`). A differential harness runs every statement twice by
  construction; that is harmless only for as long as the side-effect analysis is
  exactly right, and this ADR should not treat oracle-building as risk-free
  overhead.

  The same first run is direct evidence for this ADR's own claim that the
  incumbent records what Basin *does* and never what is *correct*: of 13
  divergences over 146 comparisons, the owned engine was wrong in 4 — and
  **DataFusion was wrong or unable in 5**. `0.0 = -0.0` is false on DataFusion
  and true on both the owned engine and PostgreSQL; `count(DISTINCT) FILTER`, a
  correlated scalar subquery and `FETCH FIRST` all error on DataFusion and are
  served correctly by the owned engine. The remaining 4 were oracle false
  positives (unordered `array_agg`/`string_agg` element order, one genuine
  3-way `ORDER BY` tie).

- **Fallback rate cannot be the sole governing metric.** *Added 2026-08-13.* A
  query that falls back is invisible to a correctness harness *and* correct for
  the user; both of those stop being true at the delete. What "falls back" is
  currently doing, "correct" will have to do afterwards, and the running list of
  those places is
  [`19-expires-at-removal.md`](../migration/df-removal/19-expires-at-removal.md).

### Estimates as measured

The sizing above was written before the subsystem surveys. Recording where it
moved, in both directions, so the number is not quietly re-anchored:

| Component | Original | Measured | Source |
|---|---|---|---|
| Optimizer | 8–12k | **4.4–6.7k** | Rule-by-rule ablation against real DataFusion 53 plans ([05](../migration/df-removal/05-optimizer-rules.md)) |
| Physical layer | 19–30k | **8.5–13k** | Parallelism is never constructed — the guard that raises `target_partitions` also disables repartitioning ([03](../migration/df-removal/03-physical-operators.md)) |
| Function library | 9–17k | **22–30k** | 215 missing names, 127 required; window functions are entirely greenfield ([04](../migration/df-removal/04-function-gap.md)) |
| Catalog fidelity | 15–25k | **9–13.5k** | ~65 relations already exist and are real queries, not stubs ([11](../migration/df-removal/11-pg-catalog-fidelity.md)) |

Two corrections worth naming, because both were assumptions rather than
measurements. The claim that six of thirteen published wins are storage results
that "survive whatever the optimizer does" is **false**: ablation showed they
collapse without `optimize_projections` and `push_down_filter`. And the 462×
LATERAL win is **not** DataFusion's `DecorrelateLateralJoin` — DataFusion never
decorrelates that query; Basin's own textual rewrite in `pg_operators.rs:3461`
does, which means it must be ported rather than discarded with the rest of that
file.

### Mitigations

- Basin's ~199,530 LOC integration suite plus ~66,583 LOC of in-file unit tests
  form an executable conformance spec for current behaviour. Because DataFusion
  is deleted last, a differential harness can run both engines and diff results
  during the entire migration.

  > **Amended 2026-08-12, same day, before any engine work.** The claim above is
  > materially overstated and is corrected here rather than left standing.
  > `.github/workflows/ci.yml:136` runs
  > `cargo test --workspace --exclude basin-integration-tests` — **every one of
  > the 383 integration test files is excluded from the PR gate**, on the
  > recorded grounds that their binaries OOM the runner at link time
  > (`ci.yml:79`). The differential-vs-Postgres oracle covers **23 shapes**, and
  > there is exactly **one** `.slt` file
  > (`tests/integration/sqllogictest-suites/basic.slt`).
  >
  > So the conformance spec exists as code but **does not run automatically**,
  > and the part that directly compares Basin against Postgres is a fraction of
  > the LOC figure. Semantic drift — this ADR's highest-severity risk — is
  > currently guarded by an instrument that no PR trips.
  >
  > **This makes the oracle a prerequisite, not a mitigation.** Phase 0 of the
  > migration is building it: get the integration suite running in CI (or a
  > nightly gate if the OOM constraint is real), and widen the differential
  > oracle well beyond 23 shapes. No engine code should be written before that
  > lands. See
  > [`docs/migration/df-removal/10-risk-and-phases.md`](../migration/df-removal/10-risk-and-phases.md).

- Fallback rate — the share of queries the owned engine cannot yet execute — is
  the governing metric. DataFusion is removed when it reaches zero, not on a
  date.

## Architectural compatibility

Nothing in this ADR changes the storage format, the wire protocol, the catalog
schema on disk, or the project-per-S3-prefix model. If the migration is halted,
Basin remains on DataFusion with the migration documents as sunk analysis; the
owned IR crates can sit unused without affecting the shipped engine, because
the delete is sequenced last and never lands early.

## Trigger to reconsider

This ADR is a "yes" to a large build, so its trigger is a kill criterion rather
than a customer contract. Pause or abandon if any of the following hold at a
phase boundary:

- The owned optimizer cannot reproduce the published benchmark table within a
  2× regression on any headline shape, after a genuine attempt at the
  must-have rules.
- The differential harness shows semantic divergences that are not converging
  cycle over cycle.
- Vortex cannot be read without `vortex-datafusion`, and the alternative is
  rewriting the Vortex reader as well.
- v0.1 slips materially because engine work has displaced roadmap work.

## Alternatives considered and why we didn't pick them

**Keep DataFusion behind a trait boundary, owning only the OLTP path.** The
lowest-cost option, and the one this ADR was originally going to recommend. It
fails the actual goal: `pg_catalog` fidelity, true SRFs, DML-as-relation, and
unbypassable RLS all require owning the planner. A trait boundary relocates the
ceiling without raising it.

**Fork DataFusion and modify it in place.** Licensing permits it — both projects
are Apache-2.0. Rejected because a fork inherits 475k LOC of design decisions
aimed at a different target (a general-purpose, extensible, distributed query
framework) and Basin would carry maintenance of code it did not write toward
objectives DataFusion does not share. The divergence cost compounds with every
upstream release.

**Stay as-is and accept the fidelity ceiling.** Viable if Basin's product were
analytics. It is not: Basin's value proposition is Postgres compatibility on
bucket-native storage, and the ceiling caps precisely that.
