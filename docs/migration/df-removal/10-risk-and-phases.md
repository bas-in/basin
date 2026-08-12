---
title: "DataFusion removal — risk register and phase plan"
nav_section: architecture
sidebar_position: 50
summary: "Risk register, ordered phase plan, and kill criteria for replacing DataFusion with a Basin-owned query engine, with DataFusion deleted last."
tags: [engine, migration, risk, planning]
---

# DataFusion removal — risk register and phase plan

- **Status:** Proposed. Branch `feat/own-engine-remove-datafusion` (0 commits
  ahead of `main` at the time of writing — this plan is written on day zero,
  before any divergence exists).
- **Decision assumed, not argued:** DataFusion is removed completely, per
  [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md). No
  fallback engine ships. arrow-rs 58 stays. libpg_query stays the parser
  ([ADR 0014](../../decisions/0014-pg-query-as-canonical-parser.md)).
- **Companion surveys:** [02 — logical IR surface](./02-logical-ir-surface.md)
  sizes the plan-IR work this document's Phase 1 schedules;
  [06 — scan and storage integration](./06-scan-and-storage.md) is the
  detailed survey behind R1 and Phase 3, and reaches the same verdict
  independently; [09 — docs edit map](./09-docs-edit-map.md) enumerates the
  public claims Phase 6 has to rewrite.
- **Scope of this document:** what can go wrong, in what order the work
  happens, and the conditions under which it stops.

## Sizing, taken as given

| Quantity | Value |
|---|---|
| New engine code | ~75k LOC (~115k with tests) |
| Effort | 18–30 engineer-months |
| DataFusion non-test LOC replaced | ~217k across 12 crates |
| Discount: arrow-rs stays | 279 of ~1,016 DF references are arrow re-exports |
| Discount: `target_partitions = 1` | pinned by default (`session.rs` ~2705) — most parallel/distributed machinery is dead code for us |
| Discount: `datafusion-sql` (22k) | already replaced by pg_query |
| Head start: Basin's own UDFs | 40,871 LOC |
| Head start: modules already bypassing DF | 18,385 LOC (`fast_select` 6,523 · `index_probe` 6,411 · `fast_aggregate` 2,205 · `values_fast` 2,177 · `point_join` 1,069) |

## Measured coupling surface

These numbers were taken from the tree at the head of this branch and are the
factual basis for the risk assessments below.

| Fact | Value | Why it matters |
|---|---|---|
| Crates depending on `datafusion` | **1** — `basin-engine` | The blast radius is a single crate. `basin-storage`, `basin-catalog`, `basin-shard`, `basin-wal`, `basin-router` and the batteries are already clean. |
| `datafusion` references in `crates/basin-engine/src` | 1,009, across 64 of 120 source files | 56 engine files are already DF-free. |
| `datafusion` references in `crates/basin-storage/src` | 1 — and it is a *comment* in `predicate.rs` explaining why Basin defined its own predicate enum rather than reusing DF's `Expr` | The storage layer was already built to survive this. |
| `vortex_datafusion` references, whole workspace | **8**, in 2 files | See R1. |
| `impl ScalarUDFImpl for` | 238 | The largest single mechanical coupling. |
| `impl AggregateUDFImpl` / `WindowUDFImpl` | 11 | |
| `impl TableProvider for` | 33 | Catalog/system-table surface. |
| `impl ExecutionPlan for` | 8 (`TombstoneFilterExec`, `UpdateOverlayExec`, `GinRowGroupScanExec`, `JsonbPostingScanExec`, `RTreeScanExec`, `TombstoneColdScanExec`, `UuidDecimal256RestoreExec`, `PointFsbRestoreExec`) | Basin already writes physical operators; it is not starting from zero. |
| `OptimizerRule` / `AnalyzerRule` / `PhysicalOptimizerRule` impls | 7 | Basin already owns rewrite passes. |
| `basin-engine` total LOC | 194,345 | The ~75k of new engine code lands inside a crate that is already this large. |

---

## 1. Risk register

Likelihood and impact are scored **Low / Medium / High**. "Early warning
sign" is deliberately written as something observable in a normal week of
work, not as a retrospective.

### R1 — vortex-datafusion 0.71 coupling

**Description.** [ADR 0015](../../decisions/0015-vortex-storage-format.md)
states that Vortex reads go through `vortex-datafusion`'s `VortexFormat`,
which implements DataFusion 53's `FileFormat` trait. Vortex is the *default*
storage format. On its face, deleting DataFusion deletes the read path for
the default file format.

**Assessment: this is not a hard blocker.** The ADR describes the situation as
of 2026-05-18 and the tree has moved. Three findings:

1. `vortex_datafusion` appears **8 times in the entire workspace**, in two
   files: `crates/basin-engine/src/session.rs:3110` (one construction site)
   and `crates/basin-engine/src/vortex_listing_format.rs` (a 1,184-LOC
   Basin-owned wrapper, `BasinVortexFormat`, that already patches
   `total_byte_size` for the join planner and applies the ADR-0024 UUID
   `Decimal256(39,0)` → `FixedSizeBinary(16)` inverse). Basin already owns
   the majority of the code on this seam; `vortex-datafusion` supplies the
   inner `VortexSource` / `VortexOpener`.
2. **`basin-storage` has no DataFusion dependency at all.** Its only mention
   of the word is a comment. It nonetheless exposes a complete read path —
   `Storage::read`, `read_paths`, `read_paths_with_schema`, `read_file`,
   `read_file_with_options` in `crates/basin-storage/src/lib.rs`, backed by
   `reader.rs` — which dispatches on `.parquet` vs `.vortex`, reads Vortex
   footers and column stats via tail range GETs
   (`vortex_format::footer_meta_from_store`), caches them
   (`VortexFooterCache`), and applies filters through Basin's own
   `predicate` enum. `reader.rs:1070` explicitly notes that "Vortex
   filter-pushdown optimisation can fire even on the paths-only path."
3. That DF-free path is not theoretical — it is what `fast_select`,
   `fast_aggregate`, `index_probe`, `point_join` and `values_fast` (18,385
   LOC, default-on) already use in production today.

So the question is not "can Basin read Vortex without DataFusion" — it
demonstrably already does, on the hot path. The question is what is lost by
dropping `vortex-datafusion`: layout-aware scan integration inside a
DataFusion plan (row-selection pushdown, projection through the opener,
statistics feeding DF's `join_selection`). That capability has to be
reproduced against Basin's own scan operator, and the DF-free reader is
today's *simple* path, not today's *optimised* path.

Residual risk is therefore real but bounded: **~1,200–2,500 LOC of scan
operator work in Phase 3, not a storage-format re-decision.** The genuine
danger is the second-order one — ADR 0015 records that Vortex already trails
Parquet on point-lookup (≈0.65×) and `ORDER BY … LIMIT` (≈0.38×) because
"native vortex-datafusion execution is still maturing." Removing the
maturing-upstream path means Basin inherits that maturation work rather than
receiving it from upstream.

- **Likelihood:** Medium (that the scan rewrite costs materially more than
  budgeted) / Low (that it blocks the migration).
- **Impact:** Medium.
- **Early warning sign:** during Phase 3, the Vortex scan operator's p50 on
  the `vortex_vs_parquet_smoke` battery is worse than the current
  `BasinVortexFormat` path on more than a quarter of the 88 shapes after two
  weeks of tuning.
- **Mitigation:** (a) Phase 3 ports Vortex *first* and Parquet second, so the
  harder format is de-risked while the DF path is still available for
  A/B comparison. (b) Keep `vortex_parquet_differential` as a
  byte-identity gate on every Phase 3 commit. (c) Treat the `vortex` /
  `vortex-array` / `vortex-file` / `vortex-layout` crates as retained
  dependencies — only `vortex-datafusion` is dropped; the layout-scan
  primitives it wraps remain available directly. (d) Parquet stays
  first-class per-table, so a catastrophic Vortex-scan outcome has a
  documented per-table escape hatch that already ships.

### R2 — Semantic drift: the engine silently returns different answers

**Description.** A from-scratch engine reimplements NULL propagation, three-
valued logic, numeric coercion and overflow, collation and case folding,
`ORDER BY` tie-breaking and NULL ordering, timestamp/timezone arithmetic,
`GROUP BY` on floats and NULLs, string/`NUMERIC` casts, JSONB comparison
semantics, empty-input aggregate results, and window-frame boundary
semantics. Every one of those is a place where a plausible implementation
returns a plausible-but-wrong answer with no error. **A wrong answer is worse
than a slow one**, and it is worse than a crash, because it is silent and it
propagates into customer data.

**Why this is the highest-severity risk here specifically:** Basin's
verification assets are strong on paper and weakly enforced in practice.

- `tests/integration/tests/differential_pg.rs` is 3,004 LOC and runs
  identical SQL against Basin and a real Postgres, failing on divergence.
  Its own README records **26 tests, 3 ignored**. Twenty-three enforced
  differential shapes is a thin oracle against a 75k-LOC engine rewrite.
- `tests/integration/tests/sqllogictest_pg.rs` is 470 LOC and there is
  exactly **one `.slt` file** in the repository. `docs/V0_1_SCOPE.md` marks
  the wider PostgreSQL-port slice as unconfirmed.
- `docs/sql-support.md` covers **975 fragments across 3 configurations** —
  this is the strongest asset, and it is generated by a test that must be
  run manually.
- **`.github/workflows/ci.yml` runs `cargo test --workspace --exclude
  basin-integration-tests`.** That exclusion removes all **383** integration
  test files — `differential_pg`, `sqllogictest_pg`,
  `vortex_parquet_differential`, `compare_postgres*`, `orm_compat`,
  `sql_support_matrix` — from every PR gate. The reason recorded in the
  workflow is honest and legitimate (link-time OOM on a 7 GB runner), but the
  consequence stands: **the oracle that would catch a wrong answer does not
  run automatically.**

This is why Phase 0 below is oracle work and contains no engine code.

- **Likelihood:** High. Not "might happen" — *some* semantic divergence is
  certain across 75k LOC. The variable is whether it is caught before it
  reaches a user.
- **Impact:** Critical. Silent data corruption is the one failure mode with
  no honest recovery story for a database.
- **Early warning sign:** the first divergence found by a *customer* or by
  manual inspection rather than by the harness. Also: any phase whose exit
  review contains the phrase "the differential suite doesn't cover that
  shape yet."
- **Mitigation:**
  1. **Phase 0 is mandatory and blocks Phase 1.** Get the differential
     suites into CI on every PR — self-hosted runner, larger runner, split
     binaries, or `cargo nextest` with per-test link, whichever works — and
     expand `differential_pg` from 23 enforced shapes toward the full 975
     `sql-support` fragment corpus, executed against real Postgres.
  2. **Golden corpus captured before any engine code lands.** Freeze a
     DataFusion-era SHA and record the *results*, not just pass/fail, for
     every fragment in the corpus. This is the only artifact that can prove
     the new engine matches the old one, and it is unrecoverable once
     DataFusion is deleted.
  3. **Shadow execution as a first-class mode.** From Phase 4 onward, an
     env-gated mode runs every query on both engines and diffs the resulting
     `RecordBatch`es, logging divergences without failing the request. Run
     it over the benchmark harness, the ORM live corpora
     (`testing/orm-live/`: drizzle, prisma, django, sqlalchemy, gorm), and
     the sqllogictest slice. Divergence count is the primary progress
     metric for Phases 4–6 — more meaningful than LOC.
  4. **Property/fuzz differential.** Generate random expressions over random
     typed data and diff Basin against Postgres. The 3-valued-logic and
     numeric-coercion classes are exactly what hand-written tests miss and
     generators find.
  5. **Fail loudly by default.** In the new engine, an unimplemented
     semantic corner raises SQLSTATE `0A000` rather than falling through to
     a default. Basin already has this discipline via `reject_unsupported`
     (ADR 0014) and the documented-exclusions culture in `CAPABILITIES.md`.

### R3 — Scope creep: engine work starves the product roadmap

**Description.** 18–30 engineer-months of engine work, absorbed by a
pre-alpha project whose own documents say the binding constraint is
elsewhere.

**The conflict, stated honestly.** `ROADMAP.md` opens with "**Basin is
pre-alpha.** There has been no v0.1 release… do not put a business on it
yet." It lists six items required for the v0.1 cut:

| v0.1 gate | Does the engine rewrite advance it? |
|---|---|
| WAL records for hot-tier DELETE/UPDATE + replay | No — `basin-wal` / `basin-hottier`, no DF involvement |
| Real-cloud (S3) read-shape parity sweep | No — storage/network, and the rewrite *destabilises* the numbers it must sweep |
| ORM corpus ≥ 95% per ORM | Marginally, eventually. Near-term it puts the corpus at risk |
| sqllogictest PG-port slice | Same — a rewrite makes this harder before easier |
| Dogfooding the control-plane catalog onto Basin | No. `ROADMAP.md` calls this "the highest-visibility credibility gap in the project" |
| Wasm UDF jsonb + vectorised invocation | No |

**Zero of the six v0.1 gates are advanced by removing DataFusion.** Two of
them (real-cloud parity, ORM corpus) are actively made harder, because both
are measurements taken against an engine that would be under reconstruction.

`WEDGE.md`'s 2026-05-25 priority update is more direct still: "The technical
critical path is complete… The binding constraint is now **distribution /
first reference customer**, not engine work." `ROADMAP.md` echoes it: "Phase
0 customer interviews are the real gate on everything (the architecture is
far ahead of the demand signal)."

And `docs/V0_1_SCOPE.md` sets a bar that this project, applied to itself,
does not clear: "Anyone proposing new work should be able to map the work to
one bullet under 'Required for v0.1' — otherwise the work is post-v0.1 and
needs a wedge-customer trigger." There is no wedge-customer trigger for
removing DataFusion. The unparking criteria — "a named wedge customer
requests it AND has a paid contract pending," or "a production-blocking
dependency forces it" — are the project's own stated bar, and the second
clause is the only one that could plausibly apply.

Note also that `V0_1_SCOPE.md` names DataFusion as a *moat* in the wedge
statement: "One engine for OLTP point reads and OLAP scans (DataFusion +
HTAP hot tier + per-file catalog blooms / stats)." The rewrite temporarily
weakens a claim the positioning currently rests on.

This is not an argument that the decision is wrong — the decision is made
and this document does not relitigate it. It is a statement that **the
opportunity cost is the largest risk in this register by likelihood**, and
that pretending otherwise would violate the assessment culture that
`WEDGE.md` and `benchmark/BENCHMARKS.md` establish.

- **Likelihood:** **High.** This is the default outcome, not the tail case.
  18–30 engineer-months on a team small enough that `docs/V0_1_SCOPE.md`
  exists to enforce scope discipline means the v0.1 cut slips by at least
  the duration of the migration.
- **Impact:** High — existential rather than technical. A database with a
  beautiful owned engine and no users is a worse outcome than a database
  with a vendored engine and three design partners.
- **Early warning sign:** the v0.1 gate list in `docs/V0_1_SCOPE.md` goes
  two consecutive months with no box flipped and no commit SHA cited, while
  the engine branch accumulates commits. The cite-on-close contract in that
  file makes this trivially measurable — it is the cheapest instrument in
  this register, and it should be checked monthly.
- **Mitigation:**
  1. **Ring-fence capacity explicitly.** Name the fraction of team capacity
     the engine may consume, write it here, and hold it. If that fraction is
     100%, say so and accept that v0.1 is deferred by 18–30 engineer-months.
  2. **Close the six v0.1 gates *first*, or formally re-cut v0.1.** They are
     mostly small and none is engine-shaped. Shipping v0.1 before the
     rewrite gives the migration a stable, released baseline to diff against
     — which also strengthens R2's oracle.
  3. **Keep Phase 0 (customer interviews) running in parallel.** It costs no
     engineering capacity and it is the only activity that can produce the
     signal that either justifies or kills this migration.
  4. **Phase boundaries are re-decision points**, not milestones. Each exit
     review asks "is the remaining work still the best use of the next
     quarter?" — with the answer written down.

### R4 — Optimizer quality regresses the published benchmark table

**Description.** `README.md` publishes roughly twenty specific multipliers
against Postgres 18 — LATERAL JOIN **462×**, star join **261×**, correlated
subquery **113×**, range scan **81×**, `= ANY(int[])` **63×**, selective
COUNT **51×**, `COUNT(DISTINCT)` **30×**, 2-table JOIN GROUP BY **28×**,
100k-row drain **25×**, `PERCENTILE_CONT` **19×**, UNION **11×**, GROUP BY
**10×**, window frame **9.3×**, `ILIKE` **5.3×**. Several of these are
*optimizer* results, not storage results: LATERAL, star join and correlated
subquery are decorrelation, join ordering and join-strategy selection.
DataFusion's optimizer (16.5k LOC) plus physical-optimizer (7.9k) plus
pruning (2.1k) — ~26.5k LOC — produces those plans today.

A first-cut owned optimizer will not match that. The 462× LATERAL number in
particular is a decorrelation result; without decorrelation it becomes a
nested-loop and the multiplier collapses by orders of magnitude. `README.md`
already describes these as measured cards regenerated by
`benchmark/run/run_all.sh`, and `ROADMAP.md`'s maintenance contract is that
these documents hold no unsourced facts — so a regression cannot be quietly
absorbed; it has to be published.

A mitigating fact: `target_partitions = 1` means Basin never used DF's
partitioning/repartition planning, which removes a large slice of the
optimizer surface that would otherwise need reproducing. And Basin already
owns 7 rewrite rules including `AnyAllToScalarSubquery`,
`UnionScanCollapse`, `SortStreamingLimit` and
`CatalogWindowExecSortElision` — several of the published multipliers are
already Basin's own work, not DataFusion's.

- **Likelihood:** High that *some* shapes regress; Medium that a headline
  multiplier regresses by more than 2×.
- **Impact:** High for credibility. `README.md`, `benchmark/BENCHMARKS.md`
  and `RESULTS_localfs.md` are load-bearing positioning artifacts, and
  Basin's differentiator is documented honesty about numbers.
- **Early warning sign:** at the Phase 5 exit sweep, any published shape more
  than 2× off its recorded value — or, earlier and more usefully, the first
  Phase 4 shadow-execution run showing the new engine's plan for the LATERAL
  or correlated-subquery shape is a nested loop.
- **Mitigation:**
  1. **Record the full benchmark matrix at the pre-migration SHA** as a
     committed baseline artifact, on the same box, before Phase 1 — same
     discipline as the golden result corpus.
  2. **Rank optimizer work by published-shape coverage**, not by textbook
     completeness. Decorrelation, join ordering by catalog stats,
     predicate/projection pushdown and TopK come first because they are what
     the published table is made of.
  3. **Benchmark regression becomes a phase-exit gate** with a stated
     tolerance (proposal: no published shape may regress more than 20%, and
     no shape may regress more than 2× at any point).
  4. **Do not republish the table mid-migration.** Freeze the published
     numbers at the DataFusion baseline and mark them as such until the
     Phase 6 exit sweep, rather than shipping a slowly-degrading table.
  5. **Accept and publish targeted losses.** `CAPABILITIES.md` already has a
     "Performance residuals (won't chase further)" section; a shape that
     lands at 0.8× and is honestly documented is fine. A shape that silently
     drops from 462× to 3× is not.

### R5 — Long-lived feature branch divergence

**Description.** `feat/own-engine-remove-datafusion` is currently 0 commits
ahead of `main`. Over 18–30 engineer-months it would touch 64 of 120 files
in a 194k-LOC crate — the same crate where all other engine work lands. The
`CHANGELOG.md` for 0.1.10 alone lists ~70 waves of work, most of them inside
`basin-engine`. A branch of this shape either becomes unmergeable or freezes
`main`.

- **Likelihood:** High if the work is actually done on a long-lived branch.
  Low if the phase plan below is followed, because every phase is designed
  to merge to `main` behind a flag.
- **Impact:** High — the classic failure mode is a rewrite that is 90% done
  forever.
- **Early warning sign:** the branch goes more than two weeks without
  merging to `main`; or a rebase produces conflicts in more than ~10 files;
  or someone proposes a feature freeze on `main` "just until the engine
  lands."
- **Mitigation:**
  1. **There is no long-lived branch.** Every phase merges to `main`,
     off by default behind an env gate. Basin has done exactly this before —
     `BASIN_PG_QUERY`, `BASIN_PG_QUERY_PLAN`, `BASIN_HOTTIER_FASTPATH_DISABLE`
     — and `docs/sql-support.md` already renders three parser/planner
     configurations side by side. Add a fourth column.
  2. **The hard constraint applies at every merge:** the workspace compiles
     and the tests pass. DataFusion is removed in the final phase.
  3. **Delete the branch after Phase 0.** Rename the effort to a flag, not a
     branch. The branch name is a smell worth correcting early.
  4. Cap the flag matrix: at most one new engine flag at a time, and each
     phase's flag is folded into the previous one at its exit.

### R6 — The 249-UDF re-hosting tax

**Description.** 238 `ScalarUDFImpl` and 11 aggregate/window UDF impls,
across ~37,371 LOC of `*udf*.rs` files, are written against DataFusion's
trait signatures (`ColumnarValue`, `Signature`, `ReturnTypeArgs`,
`ScalarFunctionArgs`, `Accumulator`, `GroupsAccumulator`). This is the
largest mechanical coupling in the tree and it is spread across the files
with the highest DF reference counts (`udf.rs` 57, `pg_agg_udf.rs` 55,
`geo_glue.rs` 52).

- **Likelihood:** Certain (it is work, not a risk event). The risk is that it
  is estimated as mechanical and turns out to be semantic — DF's coercion
  and signature-resolution rules are embedded in the *absence* of code in
  each impl, and reproducing them incorrectly lands directly in R2.
- **Impact:** Medium-High.
- **Early warning sign:** the port of the first 20 UDFs takes materially
  longer per-UDF than budgeted, or requires per-UDF semantic decisions rather
  than a `sed`-shaped rename.
- **Mitigation:** Design Basin's native UDF trait to be *deliberately
  signature-compatible* with DF's, so the port is a rename plus an import
  change, and the type-coercion/signature-resolution engine is ported as a
  unit and tested against the 975-fragment corpus before any UDF moves.
  Port the 20 highest-traffic UDFs first as a calibration sample and re-
  estimate the remaining 229 from the measured rate.

### R7 — arrow-rs / DataFusion version-pin whiplash during the transition

**Description.** For the duration of Phases 1–6 the tree carries both
DataFusion 53 and a growing Basin engine on arrow-rs 58, plus vortex 0.71.
ADR 0015 already flags this: "`vortex` 0.70 and `vortex-datafusion` 0.70
must track the workspace arrow 58 / DataFusion 53 toolchain on every
upgrade." An arrow-rs security or correctness fix that requires a bump can
force a three-way alignment across DF, Vortex, and Basin's own code.

- **Likelihood:** Medium over an 18–30 month window.
- **Impact:** Medium — schedule, not correctness.
- **Early warning sign:** an arrow-rs release Basin wants that DataFusion 53
  has not adopted.
- **Mitigation:** Freeze DataFusion at 53 for the whole migration and accept
  no DF upgrades. Basin's own engine code targets arrow-rs directly, so the
  DF pin only constrains the shrinking legacy path. Dropping
  `vortex-datafusion` early (Phase 3) removes one leg of the triangle sooner
  than dropping DataFusion itself.

### R8 — Test-suite mass is mistaken for test-suite coverage

**Description.** The workspace holds 3,212 `#[test]` and 2,967
`#[tokio::test]` functions — 6,179 in total — and 383 integration test files.
That is a genuinely large suite, and it creates a false sense of safety:
most of those tests assert against hand-coded expected values, which is
precisely the failure mode `DIFFERENTIAL_README.md` calls out — "Basin **and**
its expected outputs could be wrong in the same direction." A rewrite that
reproduces the *old engine's* behaviour, including its bugs, will pass them.

- **Likelihood:** High.
- **Impact:** Medium-High (it is R2's amplifier rather than an independent
  failure).
- **Early warning sign:** a phase completes with 6,179/6,179 green and fewer
  than 50 differential-vs-Postgres shapes enforced.
- **Mitigation:** Treat the oracle count — differential shapes actually
  enforced against real Postgres in CI — as the headline coverage metric for
  this project, and publish it per phase. "6,179 tests pass" is not the
  number that matters here.

### R9 — Loss of upstream maintenance leverage

**Description.** DataFusion 53's ~217k non-test LOC are maintained,
fuzzed and bug-fixed by a large upstream community for free. After removal,
every optimizer bug, every arrow-compat break, every SQL semantic corner is
Basin's to fix, forever. The 18–30 engineer-months is the *build* cost; the
maintenance cost is permanent and is not in the estimate.

- **Likelihood:** Certain.
- **Impact:** Medium, compounding.
- **Early warning sign:** post-cutover, the rate of engine-correctness bug
  fixes does not decline over two consecutive quarters.
- **Mitigation:** Deliberately keep the engine small. `target_partitions = 1`,
  no distributed execution, no multi-format catalog, and the documented-
  exclusion culture (`CAPABILITIES.md`, SQLSTATE `0A000`) are what make a
  75k-LOC engine defensible where a 217k-LOC one would not be. Every feature
  the owned engine does *not* implement is a permanent maintenance dividend —
  and this argues for tightening `docs/V0_1_SCOPE.md`'s exclusions during
  the migration, not loosening them.

### R10 — Key-person concentration

**Description.** A 75k-LOC query engine designed and largely written by one
or two people is a bus-factor problem that outlives the migration. Basin is
a small team; the coupling map above (1,009 DF references in one crate) shows
the engine is already a single-owner-shaped artifact.

- **Likelihood:** Medium.
- **Impact:** High if it fires.
- **Early warning sign:** phase design documents exist only as commit
  messages; no second person can explain the expression evaluator's
  null-handling contract without reading the code.
- **Mitigation:** Each phase lands a short design note under
  `docs/migration/df-removal/` before its code. The `docs/decisions/` ADR
  culture already exists — extend it to engine internals rather than only to
  load-bearing "no"s.

### Risk summary

| # | Risk | Likelihood | Impact | Net |
|---|---|---|---|---|
| **R2** | Semantic drift — silent wrong answers | High | **Critical** | **Top** |
| **R3** | Scope creep starving the v0.1 roadmap | **High** | High | **Top** |
| **R4** | Optimizer quality regressing published benchmarks | High (some) / Medium (headline) | High | High |
| **R5** | Long-lived branch divergence | High if branched; Low under this plan | High | High |
| **R6** | 249-UDF re-hosting tax | Certain (work) | Medium-High | Medium-High |
| R8 | Test mass mistaken for coverage | High | Medium-High | Medium-High |
| R1 | vortex-datafusion coupling | Medium (cost) / Low (blocker) | Medium | Medium |
| R9 | Loss of upstream maintenance leverage | Certain | Medium, compounding | Medium |
| R7 | arrow-rs / DF pin whiplash | Medium | Medium | Medium |
| R10 | Key-person concentration | Medium | High | Medium |

---

## 2. Phase plan

**Hard constraint, restated:** the tree compiles and the tests pass at every
phase boundary. DataFusion stays in `Cargo.toml` until Phase 7. Every phase
merges to `main` behind an env gate, default OFF, so R5 never materialises.

Flag naming follows the existing convention (`BASIN_PG_QUERY`,
`BASIN_PG_QUERY_PLAN`): `BASIN_ENGINE=native` with per-phase sub-gates that
fold in at each exit.

LOC figures are new non-test code; roughly +50% for tests, consistent with
the 75k → 115k ratio given.

### Phase 0 — Oracle before engine

**Goal.** Make it possible to detect a wrong answer *before* writing code
that could produce one. No engine code in this phase.

**Work.**
- Get the integration suites into CI on every PR. The current
  `--exclude basin-integration-tests` exists for a real reason (link-time
  OOM on a 7 GB runner); the fix is larger/self-hosted runners, `cargo
  nextest` with per-test binaries, or splitting the benchmark-flavoured
  `viability_*` / `s3_scaling_*` cards from the correctness suites so the
  latter can be gated cheaply. The split is likely the cheapest.
- Expand `differential_pg.rs` from 23 enforced shapes toward the
  975-fragment `sql-support` corpus, executed against a real Postgres 18
  service container. Target: ≥ 500 enforced differential shapes.
- Capture the **golden result corpus** at a frozen DataFusion-era SHA:
  actual result sets, types, null placement and ordering for every fragment.
  Commit it. This artifact is unrecoverable after Phase 7.
- Capture the **benchmark baseline matrix** (`benchmark/run/run_all.sh`,
  all three storage configs) at the same SHA, on the same box.
- Stand up a property/fuzz differential generator for expressions and
  aggregates.

**Exit criteria.**
- `differential_pg`, `sqllogictest_pg`, `vortex_parquet_differential`,
  `sql_support_matrix` and `orm_compat` all run on every PR and are green.
- ≥ 500 enforced differential shapes against real Postgres.
- Golden corpus and benchmark baseline committed and reproducible.
- A deliberately-planted wrong answer is caught by CI (self-test
  discipline, per `scripts/check-test-executes.sh --selftest`).

**LOC.** ~4–6k (harness + corpus tooling; the corpus data itself is larger).

**Unblocks.** Everything. Without this, R2 is unmanaged and the migration
should not start.

### Phase 1 — Basin-native plan IR, lowered to DataFusion

**Goal.** Own the plan representation. Keep DataFusion as the executor.

**Work.**
- New crate `basin-plan`: Basin's own `LogicalPlan`, `PhysicalPlan`, `Expr`,
  `Schema`-binding and type-coercion rules, modelled on the semantics the
  golden corpus records — not on DataFusion's internals.
- Extend the existing ADR-0014 Phase 2 translator so `PgNode` lowers to
  Basin IR (today `BASIN_PG_QUERY_PLAN=1` lowers single-table SELECT
  straight to a DF `LogicalPlan`).
- Write `basin-plan → datafusion::LogicalPlan` lowering so DataFusion still
  executes everything.

**Exit criteria.**
- A fourth configuration column in `docs/sql-support.md` (Basin IR →
  DF) at parity with configuration 3 across all 975 fragments.
- Zero divergences vs the golden corpus.
- Flag default OFF; workspace green with it ON and OFF.

**LOC.** ~9–11k.

**Unblocks.** Every later phase, because the IR is the seam that lets
operators be replaced one at a time.

### Phase 2 — Re-host the function catalogue

**Goal.** Move 238 scalar + 11 aggregate/window UDFs off DataFusion traits.

**Work.**
- Basin-native `ScalarFn` / `AggregateFn` / `WindowFn` traits, deliberately
  signature-shaped like DF's so the port is mechanical (R6).
- Port Basin's own type-coercion and signature-resolution rules first, gated
  on the fragment corpus, before any UDF moves.
- Port the 20 highest-traffic UDFs, measure the rate, re-estimate, then port
  the rest.
- A thin `basin-fn → DF UDF` adapter keeps DataFusion able to call them, so
  the legacy path keeps working.

**Exit criteria.**
- No `*udf*.rs` file imports `datafusion::`; the adapter is the single
  DF-aware file on this surface.
- Full fragment corpus and `differential_pg` green.
- Wasm, geo, JSONB, regex, datetime, range and approx-aggregate families all
  ported.

**LOC.** ~7–9k net new (≈37k touched).

**Unblocks.** Phase 3 and 4 — no operator can run without functions.

### Phase 3 — Scan and expression evaluation

**Goal.** Own the vectorized core: file scans and expression evaluation over
arrow-rs. This is where `vortex-datafusion` leaves the tree.

**Work.**
- Basin-native scan operators over `basin-storage`'s existing DF-free read
  path (`Storage::read_paths_with_schema`, `read_file_with_options`,
  `predicate::*`, `VortexFooterCache`) — generalising what `fast_select` and
  `index_probe` already do into a first-class operator.
- Vortex first, Parquet second (R1): layout-aware pruning, row selection,
  projection pushdown against `vortex-array` / `vortex-file` /
  `vortex-layout` directly.
- Vectorized expression evaluator over arrow-rs, including the ADR-0024
  UUID `Decimal256` inverse and view-type normalisation that
  `vortex_listing_format.rs` currently performs.
- Port the 8 existing `ExecutionPlan` impls (`TombstoneFilterExec`,
  `UpdateOverlayExec`, `GinRowGroupScanExec`, `JsonbPostingScanExec`,
  `RTreeScanExec`, `TombstoneColdScanExec`, and the two type-restore execs)
  onto the native operator trait, with a DF adapter so DF can still drive
  them.
- Delete `vortex_listing_format.rs` and the `vortex-datafusion` dependency.

**Exit criteria.**
- `vortex-datafusion` removed from `crates/basin-engine/Cargo.toml`; every
  Vortex read goes through Basin's scan.
- `vortex_parquet_differential` byte-identical across the full battery.
- The 88-shape `vortex_vs_parquet_smoke` battery shows no shape more than
  20% worse than the recorded baseline.
- HTAP hot-tier merge-on-read semantics preserved (the 16-test gate matrix
  from Phase 5.14.C stays green).

**LOC.** ~16–19k.

**Unblocks.** Phase 4 — stateful operators need a scan and an evaluator
underneath them. Also closes R1 and one leg of R7.

### Phase 4 — Stateful operators + shadow execution

**Goal.** Own join, aggregate, sort, window, limit, union, set ops, and
subquery execution.

**Work.**
- Hash join (build/probe, all join types), nested-loop and index-nested-loop
  join, hash aggregate with spill, sort with spill, TopK, window operators,
  `UNION`/`INTERSECT`/`EXCEPT`, CTE materialisation, correlated-subquery
  execution.
- `target_partitions = 1` is assumed throughout — no repartition, no
  exchange operators. This is the single largest discount available and it
  should be architecturally baked in, not a configuration.
- **Shadow execution mode**: run every query on both engines, diff the
  `RecordBatch` output, log divergences without failing the request.

**Exit criteria.**
- Shadow mode over the full fragment corpus, ORM live corpora
  (`testing/orm-live/`), sqllogictest slice and benchmark harness reports
  **zero** divergences for two consecutive weeks.
- Native engine passes the full workspace test suite with the flag ON.
- Divergence count published per week as the phase's progress metric (R8).

**LOC.** ~22–26k.

**Unblocks.** Phase 5 — you cannot tune a planner without operators to plan
over.

### Phase 5 — Planner and optimizer

**Goal.** Recover plan quality. This phase owns R4.

**Work, ordered by published-benchmark coverage:**
1. Subquery decorrelation (LATERAL 462×, correlated subquery 113×).
2. Join ordering + join-strategy selection from catalog stats and per-file
   blooms (star join 261×, 2-table JOIN GROUP BY 28×).
3. Predicate and projection pushdown into the scan, catalog file pruning
   (range scan 81×, `= ANY` 63×, selective COUNT 51×).
4. TopK / sort-limit fusion (`ORDER BY … LIMIT` — already a known Vortex
   weak shape at ≈0.38×, and the pagination gap `ROADMAP.md` documents).
5. Metadata-only aggregate routing (the existing catalog-stats shortcut,
   30–40× on metadata aggregates).
6. Port the 7 existing rewrite rules (`AnyAllToScalarSubquery`,
   `IsDistinctRewrite`, `NullifRewrite`, `UnionScanCollapse`,
   `SortStreamingLimit`, `CatalogWindowExecSortElision`,
   `CitextAnalyzerRule`).

**Exit criteria.**
- Full `benchmark/run/run_all.sh` sweep across all three storage configs.
- No published README/`BENCHMARKS.md` shape regressed more than 20% vs the
  Phase 0 baseline; no shape regressed more than 2× at any point.
- Any accepted regression documented in `CAPABILITIES.md` under
  "Performance residuals," with the trigger that would reopen it.
- Shadow divergence still zero.

**LOC.** ~13–16k.

**Unblocks.** Cutover.

### Phase 6 — Cutover

**Goal.** Native engine becomes the default; DataFusion becomes the
kill-switch.

**Work.**
- Flip `BASIN_ENGINE` default to `native`; DataFusion path reachable only
  via an explicit kill-switch, the same shape as
  `BASIN_HOTTIER_FASTPATH_DISABLE`.
- Republish the benchmark table from the native engine.
- Execute the rewrite map in
  [09 — docs edit map](./09-docs-edit-map.md): `ROADMAP.md`,
  `docs/V0_1_SCOPE.md`, `CAPABILITIES.md` and ADR 0015 stop describing
  DataFusion as the engine, and ADR 0030 gets its status line updated.

**Exit criteria.**
- Default-native for a stated soak period (proposal: 6 weeks) with zero
  correctness escapes and no use of the kill-switch in any environment.
- Published benchmark table regenerated and honest.

**LOC.** ~2–3k plus docs.

**Unblocks.** Deletion.

### Phase 7 — Delete DataFusion

**Goal.** `datafusion = "53"` leaves `Cargo.toml`.

**Work.** Delete the DF adapters, the DF lowering in `basin-plan`, the
`ScalarUDFImpl` adapter shim, the DF-side `TableProvider` impls, the
kill-switch, and the dependency. Reclaim the compile time.

**Exit criteria.**
- `grep -rn datafusion crates/` returns zero hits outside comments.
- `Cargo.toml` line 128 removed; `Cargo.lock` regenerated.
- Full workspace green, full integration suite green in CI, benchmark sweep
  unchanged from Phase 6.
- Clean-build wall time recorded before and after.

**LOC.** Net negative.

### Phase summary

| Phase | Goal | New LOC | Exit gate |
|---|---|---|---|
| **0** | Oracle before engine | ~4–6k | Differential suites in CI; ≥500 enforced shapes; golden corpus + benchmark baseline committed |
| **1** | Basin plan IR, lowered to DF | ~9–11k | 4th `sql-support` config at parity, 975/975 |
| **2** | Re-host 249 UDFs | ~7–9k | No `*udf*.rs` imports `datafusion::` |
| **3** | Scan + expression evaluation | ~16–19k | `vortex-datafusion` deleted; Vortex⇆Parquet byte-identical; ≤20% perf delta on 88 shapes |
| **4** | Stateful operators + shadow exec | ~22–26k | Zero shadow divergences for two weeks across every corpus |
| **5** | Planner + optimizer | ~13–16k | No published shape regressed >20% vs Phase 0 baseline |
| **6** | Cutover to native default | ~2–3k | 6-week soak, kill-switch unused |
| **7** | Delete DataFusion | negative | Zero `datafusion` references; `Cargo.toml` clean |
| | **Total** | **~74–90k** | |

---

## 3. Kill criteria

These are conditions under which the migration is **paused** (work stops,
flags stay OFF, `main` keeps the DataFusion path) or **abandoned** (the
branch and flags are deleted and an ADR records why). They are written to be
checkable by someone who did not do the work, in the spirit of
`benchmark/BENCHMARKS.md` and `WEDGE.md`.

### Abandon

**K1 — Phase 0 cannot be completed within 8 weeks.**
If the differential suites cannot be made to run in CI on every PR, and
`differential_pg` cannot be expanded past 500 enforced shapes against real
Postgres, then the oracle does not exist. Writing 75k LOC of query engine
without an oracle is not a risk to be managed — it is a decision to ship
silent wrong answers. Phase 0 is also independently valuable: if the
migration is abandoned here, Basin still gains a real CI correctness gate,
which closes part of R8 and helps three of the six open v0.1 items.

**K2 — Shadow divergences do not converge to zero.**
If, at the Phase 4 exit gate, the divergence rate against the full corpus has
not reached zero within **4 consecutive weeks** of dedicated fixing — or if
it reaches zero and then regresses above zero twice — the semantic surface is
larger than the team can reproduce. Abandon.

**K3 — Measured velocity implies more than 36 engineer-months.**
Phases 0–3 are ~36–45k LOC, roughly half the total. If they are not complete
by **month 9 at the ring-fenced capacity**, extrapolate: if the projected
total exceeds 36 engineer-months, abandon. The 18–30 estimate is what makes
this defensible; 36+ is a different decision that was never made.

**K4 — The v0.1 cut has not happened 12 months from Phase 1 start, and the
engine is the reason.**
Measured directly from `docs/V0_1_SCOPE.md`'s cite-on-close contract: if the
six required-for-v0.1 boxes have not all been flipped with commit SHAs
within 12 months, and the commit history shows engine-migration work
consuming the capacity, then R3 has fired. Abandon the migration, ship v0.1,
and reopen the question with a released baseline and — ideally — users.

**K5 — A wrong answer reaches a user.**
If any native-engine path produces a silently incorrect result observed
outside the test harness, at any phase, in any environment: stop
immediately, flag OFF, root-cause published. Resume only if the root cause
is a class the oracle can now catch, and the oracle is extended to catch it.
Two such events: abandon.

### Pause and re-decide

**K6 — A design partner appears with blocking asks that are not engine-internal.**
`ROADMAP.md` and `WEDGE.md` both name Phase 0 customer interviews as the real
gate. If a named wedge customer with a contract pending needs work that this
migration does not advance, the migration pauses by the project's own
unparking logic (`docs/V0_1_SCOPE.md` § "Acceptance criteria for
unparking") — inverted: the same bar that keeps frozen crates frozen should
keep an unrequested 18–30 engineer-month rewrite from outranking a paying
customer.

**K7 — Two or more published benchmark multipliers regress by more than 2×
and are not recovered within one phase.**
`README.md`'s table is a positioning asset. A 462× LATERAL that becomes 200×
is a footnote; two headline shapes collapsing by more than 2× means the
optimizer gap is structural, not a tuning backlog. Pause at the Phase 5
gate, re-scope, and only resume with a written plan for each regressed shape.

**K8 — The team drops below two engineers who can work in the engine.**
18–30 engineer-months on one person is 2–2.5 calendar years, and R10 becomes
a certainty rather than a risk. Pause until it does not.

**K9 — R1 proves worse than assessed.**
If, four weeks into Phase 3, the Basin-native Vortex scan is more than 20%
slower than `BasinVortexFormat` on more than a quarter of the 88
`vortex_vs_parquet_smoke` shapes and no path to parity is identified: pause
Phase 3, keep `vortex-datafusion` for the moment, and re-plan. This is the
one risk in the register whose assessment ("not a hard blocker") could be
wrong in a way that invalidates the phase ordering.

### What is explicitly *not* a kill criterion

- **The engine being slower than DataFusion on unpublished shapes.** Basin
  already publishes losses honestly (`CAPABILITIES.md` § "Performance
  residuals"). A documented, bounded loss on a shape nobody measured is
  acceptable.
- **Missing SQL features at cutover.** SQLSTATE `0A000` with a clear message
  is a shipped behaviour in Basin, not a failure. Fewer features in the owned
  engine is the point (R9).
- **LOC overruns without schedule overruns.** LOC is an estimate input, not
  a gate. K3 measures time, which is what actually costs.

---

## References

- [`ROADMAP.md`](../../../ROADMAP.md) — pre-alpha status, the six v0.1 gates,
  Phase 0 as the real constraint
- [`docs/V0_1_SCOPE.md`](../../V0_1_SCOPE.md) — the cut-off, the
  map-to-a-bullet rule, the unparking bar
- [`WEDGE.md`](../../../WEDGE.md) — 2026-05-25 priority update: the binding
  constraint is distribution, not engine work
- [`ADR 0030`](../../decisions/0030-own-query-engine-remove-datafusion.md) —
  the decision this document plans and risk-assesses
- [`02 — logical IR surface`](./02-logical-ir-surface.md),
  [`06 — scan and storage integration`](./06-scan-and-storage.md),
  [`09 — docs edit map`](./09-docs-edit-map.md) — companion surveys
- [`ADR 0014`](../../decisions/0014-pg-query-as-canonical-parser.md) —
  libpg_query as canonical parser; the Phase 2 translator this plan extends
- [`ADR 0015`](../../decisions/0015-vortex-storage-format.md) — Vortex as
  default; the `FileFormat` coupling assessed in R1
- [`benchmark/BENCHMARKS.md`](../../../benchmark/BENCHMARKS.md) and
  [`README.md`](../../../README.md) — the published multipliers R4 protects
- [`tests/integration/tests/DIFFERENTIAL_README.md`](../../../tests/integration/tests/DIFFERENTIAL_README.md)
  — the PG oracle, and the statement of why hand-coded expectations are not
  enough
- [`docs/sql-support.md`](../../sql-support.md) — the 975-fragment corpus and
  the three-configuration pattern Phase 1 extends
- [`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml) — the
  `--exclude basin-integration-tests` line that Phase 0 exists to address
