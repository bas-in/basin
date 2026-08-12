---
title: "DF removal 07 — the conformance suite is not a safety net yet"
nav_section: migration
sidebar_position: 7
summary: "Why Basin's 197k-LOC test suite is not yet a safety net for the engine swap: it is excluded from CI, its big oracles assert nothing, and 18 semantic gaps need closing before engine code is written."
tags: [engine, migration, testing, conformance, datafusion-removal]
---

# 07 — Conformance testing for the DataFusion removal

**Status:** draft, Phase 0 spec.
**Depends on:** [02 — logical IR surface](02-logical-ir-surface.md), [06 — scan and storage](06-scan-and-storage.md), [10 — risk and phases](10-risk-and-phases.md).

---

## The headline, before anything else

The premise going in was that Basin's test suite — ~197,550 LOC across 383
integration test files, plus 1,843 unit tests inside `basin-engine` — is an
executable conformance spec for current behaviour, and therefore a ready-made
safety net for replacing DataFusion.

It is not. Four findings, in descending order of severity.

### 1. The integration suite does not run in CI. At all.

`.github/workflows/ci.yml:136`:

```yaml
- run: cargo test --workspace --exclude basin-integration-tests --no-fail-fast --locked
```

The recorded reason (`ci.yml:79`) is a link-time OOM:

> `--exclude basin-integration-tests`: those are benchmark-flavoured
> viability cards (`viability_*`, `s3_scaling_*`) that link a near-full
> workspace per binary and OOM a 7GB runner at link time with
> `ld terminated with signal 7`. They are built for a workstation, not
> free CI — and clippy above still compiles them.

383 test binaries / 2,524 `#[test]` functions are compiled by clippy
(`--all-targets`) but **never executed** on any pull request. Only
`basin-engine`'s in-file unit tests gate merges today.

### 2. The four "oracle" harnesses mostly assert nothing

This is the finding that most changes the plan. Each of the four largest files
that *look* like conformance infrastructure turns out not to be:

| File | LOC | What it actually does |
|---|---:|---|
| `sql_support_matrix.rs` | 5,955 | Classifies each fragment Ok / ExecFailed / PlannerRejected / ParserRejected / OutOfScope and writes `docs/sql-support.md`. **Asserts no values.** |
| `compare_postgres_common.rs` | 6,821 | A **benchmark**. `pair()` (line 1250) returns `(basin_p50_ms, pg_p50_ms)` — it never compares result sets. The entire file contains **2 assertions** (lines 585, 5023). |
| `3way_pg_compat.rs` | 1,050 | **Explicitly non-failing** on divergence — emits JSON. Needs three remote DSNs. |
| `sqllogictest_pg.rs` + `basic.slt` | 470 + 312 | Runs exactly **one** `.slt` file (34 queries) behind `PASS_THRESHOLD_PCT = 78.0`. **22% of records may fail and CI stays green.** `basic.slt` contains no NULL/NaN/overflow cases. |

`compare_postgres_common.rs` is the sharpest trap: it contains shapes labelled
`#47 COUNT(col) vs COUNT(*) NULL handling` and `#48 NOT IN with NULL in
subquery (3VL)`. Those comments describe exactly the semantics this migration
most endangers — and the file only times them. Reading the shape list would
lead you to believe the coverage exists.

### 3. The one real oracle is switched off

`differential_pg.rs` (3,004 LOC, 79 tests, zero `#[ignore]`) *is* a true
cell-by-cell comparison against a live Postgres, including SQLSTATEs. It is
the best asset in the repo. It skips silently unless `PG_DIFF_TEST_DSN` is set
(line 552), and **nothing in CI sets it**.

Turning it on is the single highest-value action available, and it is a
workflow edit plus a service container. It converts a large fraction of §4's
gaps from "untested" to "tested" without writing a test.

### 4. The suite's own oracle is the thing being deleted

Several of the best-looking tests derive ground truth from DataFusion itself.
`topk_late_materialize.rs:325 topk_nulls_ordering` differentially compares the
fast path against the fast-path-disabled path — both DataFusion. `hottier_
differential.rs` compares four storage tiers — all DataFusion.
`interval_tz.rs`'s header says EXTRACT support "depends on DF's native support
— verified inline". These tests pin *internal consistency*, not PG conformance.
They will pass a wrong new engine as readily as a right one, and some lose
their oracle entirely at the swap.

### What this document is for

Because of the above, this is not "here is your safety net". It is the
**Phase 0 spec**: the harness and coverage work that must land *before the
first line of new engine code is written*. §5 is the buildable part; §1–§4 are
the evidence for why it is scoped the way it is.

---

## 1. Inventory

| Metric | Value |
|---|---|
| Test files (`tests/integration/tests/*.rs`) | 383 |
| Lines of test code | 197,550 |
| `#[test]` / `#[tokio::test]` functions | 2,524 |
| `basin-engine` in-file unit tests | 1,843 |
| `basin-engine` source LOC (incl. tests) | 194,345 |

### 1.1 Bucket summary

| Bucket | Files | LOC | Test cases | Relationship to the swap |
|---|---:|---:|---:|---|
| ENGINE-CRITICAL | 95 | 66,619 | 1,443 | The conformance gate — must pass identically |
| STORAGE / EXEC-ADJACENT | 74 | 35,985 | 463 | Indirectly critical: pruning, overlays, MVCC visibility decide *which rows* the operators see |
| PERIPHERAL | 73 | 38,987 | 392 | Auth, REST, realtime, CDC, cron, blob, pool, ORM, wire protocol — unaffected |
| BENCHMARK | 141 | 55,959 | 226 | Timing cards; not correctness |

The BENCHMARK bucket is the largest by file count and is the direct cause of
the CI exclusion (§5.1): `viability_*`, `s3_*`, `scale_*`, `scaling_*`,
`ext_bench_*`, `compare_postgres_*` and `*_probe` each link a near-full
workspace to measure latency.

### 1.2 Core relational algebra is 4% of the suite

Within ENGINE-CRITICAL, the surface splits sharply between **extension/UDF
conformance** (large, well covered) and **core relational algebra** (small).

Extension and type surface — genuinely strong:

| File | LOC | Tests |
|---|---:|---:|
| `postgis_conformance.rs` | 2,125 | 58 |
| `fts_conformance.rs` | 1,888 | 52 |
| `vector_conformance.rs` | 1,851 | 37 |
| `range_conformance.rs` | 1,614 | 58 |
| `interval_tz.rs` | 825 | 37 |
| `trgm_conformance.rs` | 749 | 43 |
| `jsonb_udfs.rs` | 717 | 22 |
| `array_fns.rs` | 657 | 31 |
| `pg_scalar_fn_inventory.rs` | 574 | 41 |
| `pg_operators.rs` | 493 | 23 |

Core relational algebra — the actual engine-swap gate:

| File | LOC | Tests | Surface |
|---|---:|---:|---|
| `shape_sweeps.rs` | 1,008 | 3 | Query shape sweeps |
| `window_fns.rs` | 960 | 24 | Window functions, frames |
| `is_operators.rs` | 894 | 49 | IS TRUE/FALSE/UNKNOWN/DISTINCT FROM, COALESCE, NULLIF, CASE |
| `pg_aggregates.rs` | 754 | 17 | count/sum/avg/min/max, stat aggs, json aggs |
| `pagination_residuals.rs` | 730 | 13 | LIMIT/OFFSET residuals |
| `catalog_window.rs` | 710 | 1 | Window over catalog |
| `lateral_joins.rs` | 623 | 11 | LATERAL, table functions |
| `subquery_patterns.rs` | 559 | 13 | EXISTS, IN, ANY/ALL, scalar, derived |
| `set_ops_ctes.rs` | 558 | 13 | UNION/INTERSECT/EXCEPT (+ALL), recursive CTEs |
| `views_and_join_dml.rs` | 455 | 11 | Views, join-driven DML |
| `select_advanced.rs` | 388 | 17 | DISTINCT ON, FOR UPDATE, FETCH/OFFSET, ORDER BY NULLS |
| `aggregate_tuning.rs` | 375 | 7 | Aggregate execution tuning |
| `groupby_low_card_aggs.rs` | 247 | 3 | Low-cardinality GROUP BY + NULL values |
| `limit_pushdown.rs` | 237 | 5 | LIMIT pushdown |
| `where_null_3vl_fold.rs` | 231 | 6 | `col = NULL` folding, empty-relation aggregate |

**8,729 LOC and 193 test cases — 4.4% of the suite's lines, 7.6% of its test
cases.** Add `differential_pg.rs` and it is still under 6%.

**There is no dedicated join test file.** `LEFT JOIN` appears 71 times across
11 mostly-ORM/perf/differential files; `FULL OUTER JOIN` 6 times across 5
files; `RIGHT JOIN` in 2 files, both acceptance-shaped. The highest-risk
operator in the rewrite has the thinnest coverage in the suite.

### 1.3 Caveats

- Bucketing is heuristic (filename patterns + sampling); ENGINE-CRITICAL is a
  lower bound. **UNVERIFIED** at per-file level for ~150 files not opened.
- Test-case counts are attribute counts. Files with 1 test often loop over
  dozens of shapes (`sql_support_matrix.rs` = 1 test, ~1,500 fragments;
  `hottier_differential.rs` = 3 tests × 88 shapes × 4 modes), so the number
  understates breadth and overstates isolation — one failure aborts the file.

---

## 2. Classification rules

**ENGINE-CRITICAL** — SQL execution semantics: relational algebra, expression
evaluation, type coercion, NULL semantics, ordering, aggregation, windows,
subqueries, set operations, scalar/aggregate function surface. Must produce
identical results after the swap. These *are* the gate.

**STORAGE / EXEC-ADJACENT** — compaction, overlays, row-/hot-tier residency,
MVCC visibility, index probes (GIN/BTree/RTree/trgm), bloom and min/max
pruning, COPY ingest, fastpath routing, prepared-statement caching. Determines
*which rows reach the operators*; a pruning bug and a join bug are
indistinguishable from the result set. Second-tier gate.

**PERIPHERAL** — auth/OAuth/MFA/JWT, REST, webhooks, realtime, CDC, cron,
blob, pooling, ORM and migration-tool compat, pgwire framing, multi-region,
project lifecycle. Unaffected — *except* where they assert on result content.
RLS predicate enforcement is executed by the engine and should be re-read as
engine-critical.

**BENCHMARK** — `viability_*`, `s3_*`, `scale_*`, `scaling_*`, `ext_bench_*`,
`compare_postgres_*`, `perf_*`, `*_probe`, `*_latency_*`, `noisy_*`, `*_soak`.
Some carry hard correctness assertions inside a perf card
(`compare_postgres.rs::perf_smoke_pg_10k`, `scale_invariants.rs`); extract
those into the gate.

---

## 3. What each harness actually proves

| Harness | Proves | Does not prove |
|---|---|---|
| `differential_pg.rs` (79 tests) | Cell-by-cell match vs live PG incl. SQLSTATE | Anything, when `PG_DIFF_TEST_DSN` is unset (line 552) — which is CI's state |
| `sql_support_matrix.rs` | ~1,500 fragments parse/plan/execute without error, across 3 parser configs | **Any value whatsoever** |
| `compare_postgres_common.rs` | Relative p50 latency Basin vs PG | Result equality — 2 assertions in 6,821 lines |
| `sqllogictest_pg.rs` | ≥78% of 34 queries in one `.slt` | The other 22%, and every edge case |
| `3way_pg_compat.rs` | Emits a divergence report | Nothing — non-failing by design |
| `feature_coverage.rs` | One assertion per ✅ row in `CAPABILITIES.md` | Depth |
| `perf_suite.rs` | Per-shape timing vs thresholds | Correctness |
| `services/basin-e2e-runner` | Network perf, noisy-neighbour fairness, replica handoff | Not a conformance runner |

### 3.1 The differential oracle, in detail

`differential_pg.rs` starts an in-process Basin pgwire server, connects to real
Postgres via `PG_DIFF_TEST_DSN`, and diffs. The README's rationale is exactly
right for this migration:

> Every existing Basin test compares against hand-coded expected outputs. This
> harness closes the verification gap: Basin **and** its expected outputs could
> be wrong in the same direction.

The helper API is already the right shape:

```rust
runner.run_setup(&[ /* DDL */ ]).await?;
runner.run_assert_match("SELECT n FROM t WHERE n NOT IN (1, NULL) ORDER BY n").await?;
runner.run_assert_both_error("SELECT 1 / 0", Some("22012")).await?;
```

Note the honesty already embedded in it — `diff_cast_int8_to_int4_overflow`
(line 2316) carries a doc comment flagging itself as a *"KNOWN POTENTIAL
DIVERGENCE… Basin may wrap silently."* That is a defect the new engine
inherits or fixes, and either way must be pinned.

**UNVERIFIED, and load-bearing for §5.2:** the normalisation applied before
diffing — row-order sensitivity, float tolerance, NULL rendering, and whether
values compare as text (in which case `1` vs `1.0`, or `Decimal128` vs
`Float64`, would pass silently). Text-blind comparison is tolerable for a
Basin-vs-PG oracle but is a real weakness for Basin-old-vs-Basin-new, where
output *type* drift is a likely failure mode.

**UNVERIFIED:** whether the PG side sits behind a trait or is hardcoded
`tokio-postgres`. Determines the cost of §5.3's three-way mode.

### 3.2 The in-repo differential pattern

`hottier_differential.rs` (1,342 LOC; 3 tests × 88 shapes × 4 storage modes),
`gin_row_tier_differential.rs`, `realtime_differential.rs`, and
`wasm_functions_differential.rs` already run the same query through two
internal paths and diff. **This is the template for old-engine-vs-new-engine**
— it proves the in-process two-path diff works against Basin's own session API.
`hottier_differential.rs` also has the highest shape-per-test density in the
repo. Read it before designing §5.3.

---

## 4. Gap list

Prioritised by (likelihood a hand-written engine gets it wrong) × (silence of
the failure). "DSN-gated" means the coverage exists in `differential_pg.rs`
but does not currently execute — §5.2 fixes all of those at once.

### P0 — untested, silent, near-certain to break

**G1. Outer-join NULL semantics.** The largest gap in the suite. No dedicated
join test file exists.

| Sub-case | State |
|---|---|
| LEFT JOIN NULL-extension | Tested **only via LATERAL** — `lateral_joins.rs::left_join_lateral_*` (lines 141, 246, 397, 532) genuinely asserts NULL for the unmatched outer row (420-424, 553-556). Plain `LEFT JOIN … ON` extension is asserted nowhere except DSN-gated `differential_pg.rs:1947`. |
| RIGHT JOIN | **Not tested.** `sql_support_matrix.rs:909` (runs-or-not), `3way_pg_compat.rs:395` (non-failing). |
| FULL OUTER JOIN | **Not tested against PG.** `sql_support_matrix.rs:920/4020`, `3way_pg_compat.rs:403`, and `hottier_differential.rs:572` — the last is a *self*-differential across storage tiers, so it ratifies whatever the engine answers. |
| NULL join key never matching | **Not tested.** No test inserts NULL into a join key. |
| `COUNT(*)` vs `COUNT(col)` over a null-extended side | **Not tested.** The COUNT divergence is asserted only for a *base* table (`groupby_low_card_aggs.rs:120`), never across an outer join. |
| `ON` vs `WHERE` placement | **Not tested.** `sql_support_matrix.rs:4056` runs `LEFT JOIN u b ON a.id=b.id AND b.id<>5` and asserts nothing. |

A homegrown hash join that gets null-extension or ON/WHERE pushdown wrong
passes the entire suite today.

```sql
SELECT l.id, r.v FROM l LEFT JOIN r ON l.id = r.id ORDER BY l.id;
SELECT l.id, COUNT(*), COUNT(r.v) FROM l LEFT JOIN r ON l.id = r.id GROUP BY l.id;
SELECT * FROM l LEFT JOIN r ON l.k = r.k;          -- NULL l.k is null-extended, never matched
SELECT * FROM l LEFT JOIN r ON l.id = r.id WHERE r.v > 5;      -- degrades to INNER
SELECT * FROM l LEFT JOIN r ON l.id = r.id AND r.v > 5;        -- stays OUTER
SELECT l.id, r.id FROM l FULL OUTER JOIN r ON l.id = r.id ORDER BY 1, 2;
SELECT l.id, r.id FROM l RIGHT JOIN r ON l.id = r.id ORDER BY 2;
```

**G2. Three-valued logic — the classic traps.** `is_operators.rs` (50 tests)
exhaustively covers `IS TRUE/NOT TRUE/FALSE/NOT FALSE/UNKNOWN/NOT UNKNOWN/
NULL/NOT NULL` on true/false/null inputs (lines 147-357) plus `IS [NOT]
DISTINCT FROM NULL` (846). `where_null_3vl_fold.rs` (6 tests) covers `col =
NULL`, `NULL = col`, `col <> NULL`, and the AND-combined form folding to zero
rows. That is a genuinely strong base. What is missing:

- **`NOT IN (subquery containing NULL)`** — `subquery_patterns.rs:200
  subq_not_in_subquery` exists, but its seed (line 93) declares **every column
  `NOT NULL`**, so no NULL ever enters the subquery. DSN-gated
  `differential_pg.rs:680` covers only the *literal list* form
  `NOT IN (1, NULL)`. `compare_postgres_common.rs:2617` has a shape labelled
  for exactly this and only times it.
- **`x IN (1, NULL)`** returning NULL rather than FALSE for non-members — not
  tested.
- **`NULL = NULL`** as a scalar — not tested (only the folded table form).
- **`NULL AND FALSE` = FALSE vs `NULL AND TRUE` = NULL** — not tested. The
  logic exists at `crates/basin-engine/src/executor.rs:9417-9432`
  (`fold_const_expr`) with correct comments and **no unit test on those
  branches**.
- **EXISTS vs IN divergence under NULL** — `subquery_patterns.rs` covers
  EXISTS / NOT EXISTS / IN / NOT IN / ANY / ALL across 13 tests (125-549), all
  against a 100%-`NOT NULL` fixture, so the NULL-sensitive distinction never
  fires.

```sql
-- items.order_id contains at least one NULL
SELECT id FROM orders WHERE id NOT IN (SELECT order_id FROM items);  -- PG: 0 rows
SELECT id FROM orders o WHERE NOT EXISTS
  (SELECT 1 FROM items i WHERE i.order_id = o.id);                   -- PG: rows
SELECT 5 IN (1, NULL), 1 IN (1, NULL);            -- PG: NULL, true
SELECT (NULL AND FALSE), (NULL AND TRUE), (NULL OR TRUE), (NULL OR FALSE);
-- PG: false, NULL, true, NULL
```

**G3. Integer and numeric overflow must ERROR, not wrap.**

- INT4 addition overflow — **not tested**. The near-miss,
  `differential_pg.rs:750 diff_type_int4_plus_int8`, uses
  `2147483647::int4 + 1::int8`, an *int8* add that cannot overflow.
- int8→int4 cast overflow (22003) — only `differential_pg.rs:2316`, DSN-gated,
  self-flagged as a likely divergence where "Basin may wrap silently".
- `SUM(int4)` promotion to int8/numeric — not tested.
  `groupby_low_card_aggs.rs:141` asserts `sum(t.v)` is `Int64`, but the input
  is already BIGINT.
- `abs(INT_MIN)`, unary-minus overflow — not tested.
- Engine-side coverage is narrow: only
  `dml_mutate.rs:9824 pk_scalar_to_row_key_int32_overflow_returns_none` (PK
  narrowing). The INT4/INT2 range checks at `dml_mutate.rs:7702/7725` and
  `dml.rs:93/115` have no test.

```sql
SELECT 2147483647::int4 + 1;            -- PG: ERROR 22003
SELECT (-2147483648)::int4 * -1;        -- PG: ERROR 22003
SELECT abs((-2147483648)::int4);        -- PG: ERROR 22003
SELECT 9223372036854775807::int8 + 1;   -- PG: ERROR 22003
SELECT 40000::int2;                     -- PG: ERROR 22003
SELECT pg_typeof(SUM(c_int4)), pg_typeof(SUM(c_int8)) FROM t;  -- bigint, numeric
```

**G4. Float NaN / -0.0 / Infinity — zero coverage.** No SQL-level `'NaN'`
literal exists anywhere in the suite; every `NaN` grep hit in `tests/` is an
`f64::NAN` sentinel in a benchmark harness. Postgres sorts `NaN` as **larger
than everything including Infinity**, and treats `NaN = NaN` as **true**.
`gapfill.rs:856` and `fast_select.rs:4546` carry NaN-ordering assumptions in
comments with no test. Timestamp infinity *is* covered
(`datetime_extras.rs:133/152/170`); **float** infinity is not.

```sql
-- v in {1.0, 'NaN', 'Infinity', '-Infinity', NULL}
SELECT v FROM f ORDER BY v;     -- PG ASC: -Infinity, 1.0, Infinity, NaN, NULL
SELECT 'NaN'::float8 = 'NaN'::float8;       -- PG: true
SELECT 'NaN'::float8 > 'Infinity'::float8;  -- PG: true
SELECT DISTINCT v FROM (VALUES (0.0::float8), (-0.0::float8)) s(v);  -- PG: 1 row
SELECT 1.0::float8 / 0.0;       -- PG: ERROR 22012 — not Infinity
```

**G5. `pg_type_casts.rs:340` appears to pin PG-*incorrect* behaviour.** A
landmine, not a gap. `cast_float_to_integer_truncates` asserts `3.9 → 3` and
`-2.7 → -2` with the comment "PG truncates toward zero". **Postgres rounds**
float8→int (half to even): `SELECT 3.9::float8::bigint` returns **4**,
`(-2.7)::float8::bigint` returns **-3**.

**Correction (verified on PG 18.2):** an earlier revision of this section ended
"Only `numeric`→int truncates (correctly pinned at `differential_pg.rs:2301`)."
That is also wrong, and `differential_pg.rs:2301` was *not* pinning it
correctly — its comment claimed "3.7 truncates to 3 in PG" when
`SELECT 3.7::numeric::int4` returns **4**. **No integer cast in Postgres
truncates.** Both source types round; they differ only in the tie rule:
`float` rounds half to **even** (`0.5 → 0`, `2.5 → 2`), `numeric` rounds half
**away from zero** (`0.5 → 1`, `2.5 → 3`). Because the two agree on every
non-tie input, the error survived review in both places. Both sites are now
fixed; see [12 — PG type fidelity §8](./12-pg-type-fidelity.md).

If the new engine implements PG-correct rounding, this test **fails on a
correct implementation**. Verify against a live PG and fix the test *before*
the migration, or it will be read as a regression.

This also implies a broader action: any test whose expected value was recorded
from DataFusion rather than from PG is a potential false-failure at swap time.
`viability_pg_compat_funcs.rs` explicitly documents this practice — "where
DataFusion's semantics diverge from PG in a non-trivial way we document the
divergence rather than coerce the result here".

### P1 — untested, surfaces as wrong values rather than errors

**G6. Explicit `NULLS FIRST` / `NULLS LAST` is not tested.** The *defaults*
are, which is the happier half of this finding:

- `array_agg_perf_shape.rs:216 grouped_nulls_kept_and_sorted` — real
  assertion: `ARRAY_AGG(v ORDER BY k DESC)` yields `["c", NULL, "a"]`, pinning
  NULLS-FIRST-on-DESC.
- `topk_late_materialize.rs:325 topk_nulls_ordering` — ASC and DESC, with a
  LIMIT straddling the NULL block. **Caveat: its oracle is the DataFusion
  fast-path-disabled path**, so it loses its ground truth at the swap.

The explicit clause is token coverage only. `select_advanced.rs:311
order_by_nulls_first_accepted` and `:325 order_by_nulls_last_accepted` query a
table declared `val BIGINT NOT NULL` with no NULL ever inserted — the doc
comment says so outright: *"We can't test NULL ordering without NULLs in the
data"*. The only real assertion is DSN-gated
`differential_pg.rs:2175 diff_order_nulls_first_last`.

```sql
INSERT INTO t2(id, val) VALUES (1, 10), (2, NULL), (3, 30);
SELECT id FROM t2 ORDER BY val ASC;              -- PG: 1,3,2
SELECT id FROM t2 ORDER BY val DESC;             -- PG: 2,3,1
SELECT id FROM t2 ORDER BY val ASC NULLS FIRST;  -- PG: 2,1,3
SELECT id FROM t2 ORDER BY val DESC NULLS LAST;  -- PG: 3,1,2
```

**G7. GROUP BY with NULL keys, and DISTINCT with NULLs.** NULL *values* under
aggregation are covered well — `groupby_low_card_aggs.rs:120` and `:176`
assert SUM/AVG/COUNT(col) ignore NULLs, an all-NULL group gives SUM=NULL /
AVG=NULL / COUNT(col)=0 / COUNT(*)=n, and check output-schema nullability.
NULL *keys* are barely covered:

- Multiple NULL rows collapsing to one group: the only assertion is
  `timescale_conformance.rs:484 tb_nullable_ts`, which reaches GROUP BY through
  `time_bucket()` rather than a plain nullable column.
  `groupby_low_card_aggs.rs` seeds `k BIGINT NOT NULL`;
  `array_agg_perf_shape.rs` seeds `g BIGINT NOT NULL`.
- GROUP BY on an all-NULL column — not tested.
- **DISTINCT with NULLs — not tested.** `SELECT DISTINCT` appears 21 times in
  197k LOC; none of the occurrences (`select_advanced.rs:122`,
  `orm_compat.rs:472`, `hottier_differential.rs:480`) involve NULL values.

```sql
SELECT k, COUNT(*) FROM t GROUP BY k;      -- all NULL k → ONE group
SELECT DISTINCT k FROM t;                  -- NULL appears exactly once
SELECT COUNT(DISTINCT k) FROM t;           -- PG does NOT count NULL
SELECT DISTINCT a, b FROM t;               -- (NULL,1) and (NULL,1) are one row
SELECT DISTINCT ON (k) k, v FROM t ORDER BY k, v;
```

**G8. Empty-input aggregates — half covered.** Better than expected:
`where_null_3vl_fold.rs:169 aggregate_under_null_eq_returns_empty_relation_answer`
asserts exactly one batch, one row, `COUNT`=0, and MIN/MAX/SUM all
`is_none()`. Missing:

- **AVG over zero rows** — absent from that projection.
- **Aggregate over empty input WITH `GROUP BY` → zero rows** — not tested
  anywhere.
- The covered case passes partly because `fast_select.rs:2443` short-circuits;
  the same answer through the **general** aggregate path (e.g.
  `WHERE id > 10^9`) is not asserted.

```sql
SELECT SUM(v), AVG(v), MIN(v), MAX(v), COUNT(v), COUNT(*) FROM t WHERE 1=0;
-- PG: one row: NULL, NULL, NULL, NULL, 0, 0
SELECT k, SUM(v) FROM t WHERE 1=0 GROUP BY k;    -- PG: zero rows
SELECT SUM(v), AVG(v) FROM t WHERE id > 1000000000;   -- general path, not fastpath
```

**G9. Window frame edges.** `window_fns.rs` (24 tests, all exact-value) is a
strong asset: rank family (137-293), LAG with offset (320) *and* offset+default
(344), LEAD (375), first_value (399), last_value (430), nth_value (463),
`ROWS UNBOUNDED PRECEDING…CURRENT ROW` (500), `RANGE UNBOUNDED…UNBOUNDED`
(533), named WINDOW (570), SUM/AVG/COUNT/MIN/MAX OVER (605-722), **GROUPS**
(816, 859), **`RANGE BETWEEN INTERVAL '5 minutes' PRECEDING`** (917). The
untested parts are exactly the ones hand-rolled window operators get wrong:

- **Default frame vs explicit ROWS.** `last_value_full_frame` (430) *always*
  supplies the explicit `UNBOUNDED FOLLOWING` frame. The notorious behaviour —
  `last_value` under the *default* frame returns the current row — is never
  asserted. Same for `first_value` (399) and `nth_value` (463).
- **RANGE vs ROWS with peers/ties.** `setup_sales` uses distinct amounts, so no
  test has duplicate ORDER BY values under a RANGE frame. RANGE-collapses-peers
  vs ROWS-does-not is unexercised.
- **`EXCLUDE`** — not tested. All four variants appear in
  `sql_support_matrix.rs:2716/2746/2752/2758/4012` as runs-or-not only.
- **Empty frame** → NULL for SUM, 0 for COUNT — not tested.
- **Window over an empty partition** — not tested.
- **`RANGE BETWEEN <n> PRECEDING`** on a numeric (non-interval) key — not
  tested.

```sql
SELECT id, last_value(v) OVER (ORDER BY k) FROM t;   -- default frame: peers, not partition end
SELECT id, last_value(v) OVER (ORDER BY k
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;
SELECT k, SUM(v) OVER (ORDER BY k) FROM t;                       -- ties share a frame
SELECT k, SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING
                                       AND CURRENT ROW) FROM t;  -- ties differ
SELECT SUM(v)  OVER (ORDER BY k ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM t; -- NULL
SELECT COUNT(*) OVER (ORDER BY k ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM t; -- 0
SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) FROM t;
```

**G10. Timestamp / timezone.** `interval_tz.rs` (37 tests) is strong on
interval arithmetic, `date_trunc` (quarter/week/decade), `EXTRACT(epoch)`,
`age()`, `justify_*`, `to_char(interval)`. The gaps are all in the *timezone*
half:

- **DST boundaries — zero hits** for "DST"/"daylight" in either corpus. Every
  `AT TIME ZONE` test uses `'UTC'`, the one zone where the hard cases vanish.
  `AT TIME ZONE` appears in only two files total.
- **TIMESTAMP vs TIMESTAMPTZ coercion — zero hits.** Comparing the two (PG
  casts via the session `TimeZone` GUC) is unexercised.
- **Interval across month boundaries** (`'2024-01-31' + INTERVAL '1 month'`)
  — `interval_chained_addition` (272) does not hit the month-clamp case.
- **`date_trunc` with a tz argument** (3-arg form) — not tested.

```sql
SET TIME ZONE 'America/New_York';
SELECT '2024-01-15 12:00:00'::timestamp = '2024-01-15 12:00:00'::timestamptz;
SELECT '2024-03-10 02:30:00'::timestamptz;             -- spring-forward: does not exist
SELECT '2024-03-09 12:00'::timestamptz + interval '1 day';  -- 24h vs 1 calendar day
SELECT '2024-01-31'::date + interval '1 month';        -- PG: 2024-02-29
SELECT date_trunc('day', ts, 'America/New_York') FROM t;
```

**G11. NUMERIC/DECIMAL precision.** Coverage is storage- and wire-shaped only:
`extra_types.rs:71` (NUMERIC(10,2) ↔ Decimal128(10,2)),
`numeric_type_pgwire.rs:93`, `pgwire_wire_compat.rs:1471`. Untested: scale
propagation through `+`/`-`/`*`; PG's division-scale rule; NUMERIC(p,s)
rounding on insert/cast; Decimal128 overflow; 22003 numeric field overflow;
float→numeric conversion. `differential_pg.rs:757 diff_type_numeric_precision`
exists but is DSN-gated. `viability_pg_compat_funcs.rs:243-244` documents a
banker's-rounding ambiguity and asserts only the integer side — an acknowledged
hole. Decimal128 is Basin's declared NUMERIC storage (ADR 0024), so overflow
here is a storage error too.

```sql
SELECT 1.005::numeric(10,3) * 3;
SELECT 10::numeric / 3;                     -- PG: 3.3333333333333333333
SELECT 1.5::numeric(2,0), 2.5::numeric(2,0);  -- PG: 2, 3 (half-up, not banker's)
SELECT 999.99::numeric(4,2) + 0.01;         -- PG: ERROR 22003
SELECT 0.1::float8::numeric;
```

**G12. Division by zero is not pinned unconditionally.**
`coverage_errorpaths.rs:221 error_division_by_zero_in_select` accepts *either*
outcome (lines 244-247):

```rust
Ok(ExecResult::Rows { .. }) => {
    // DataFusion may return a batch with a NULL for integer div-by-zero
    // (some builds propagate NULL instead of erroring). Accept either
    // outcome but document it.
```

`pgwire_wire_compat.rs:950` checks only that the wire envelope is well-formed,
explicitly not the SQLSTATE. `differential_pg.rs:1166` does pin 22012 — DSN-
gated. A lenient assertion is worse than none, because it reads as coverage:
audit `coverage_errorpaths.rs` for other match-arms of this shape.

### P2 — real, lower blast radius

**G13. String comparison, collation, CHAR(n) padding.** Mixed-case TEXT
ordering is not tested; the only ordering assertion is the *inverse* case
(`citext_harness.rs:186/226` asserts **citext** sorts case-insensitively).
`CHAR(n)` blank-padding lives at `crates/basin-engine/src/types.rs:174-190`
with `enforce_charlen` at line 256 and **has no unit test at all**. `LIKE …
ESCAPE` is untested (the only `ESCAPE` hit is COPY's option,
`copy_extras.rs:218`). Empty-string-vs-NULL is not tested as a distinction.
Multibyte strings appear as payloads (`values_fast_ingest.rs:260`) but never in
an ORDER BY assertion. `upper()`/`lower()` on non-ASCII: untested.

```sql
SELECT 'abc'::char(5) = 'abc';   -- PG: true (blank-padded compare)
SELECT length('abc'::char(5));   -- PG: 3
SELECT 'abc '::text = 'abc';     -- PG: false
SELECT '' IS NULL;               -- PG: false
SELECT 'A' < 'a';                -- collation-dependent
SELECT 'a_b' LIKE 'a\_b', 'axb' LIKE 'a\_b';
```

**G14. Scalar subquery cardinality.** A correlated scalar subquery returning
>1 row must raise 21000. The error string exists at
`crates/basin-engine/src/dml_mutate.rs:186` ("more than one row returned by a
subquery used as an expression") for the UPDATE-SET path, and **no test
exercises it** in either corpus. The only 21000 test is a different rule
(`merge_into.rs:269`). Zero-row scalar subquery → NULL is also untested.
`subquery_patterns.rs` covers the happy paths (405, 432, 492) with no NULL or
empty cases.

```sql
SELECT (SELECT v FROM r WHERE r.k = l.k) FROM l;  -- ERROR 21000 if >1 match
SELECT (SELECT v FROM r WHERE false);             -- PG: NULL
SELECT (SELECT COUNT(*) FROM r WHERE false);      -- PG: 0, not NULL
```

**G15. Set-op NULL dedup and type unification.** `set_ops_ctes.rs` covers
UNION / UNION ALL / INTERSECT / INTERSECT ALL / EXCEPT / EXCEPT ALL with exact
assertions (79-240) — but every fixture is `CREATE TABLE a (v BIGINT NOT NULL)`
(line 84). So UNION dedup treating NULL=NULL, INTERSECT/EXCEPT with NULLs, and
cross-branch type unification are all untested.

```sql
SELECT NULL UNION SELECT NULL;        -- PG: 1 row
SELECT NULL UNION ALL SELECT NULL;    -- PG: 2 rows
SELECT NULL EXCEPT SELECT NULL;       -- PG: 0 rows
SELECT 1 UNION SELECT 2.5;            -- unify to numeric
SELECT 1::int4 UNION SELECT 'x';      -- ERROR 42804
SELECT a FROM t INTERSECT ALL SELECT a FROM u;   -- multiplicity = min(m,n)
```

**G16. CASE/COALESCE type unification and short-circuit.** Value semantics are
solid (`is_operators.rs` 451-591). Untested: type unification across CASE
branches / COALESCE args (zero grep hits for "common type"/"coerce branch"),
and short-circuit evaluation of an erroring branch — zero hits for
"short-circuit" outside GIN posting lists.

```sql
SELECT CASE WHEN v <> 0 THEN 1/v ELSE NULL END FROM t;  -- must NOT error on v=0
SELECT COALESCE(a, 1/0) FROM t WHERE a IS NOT NULL;     -- PG: does not error
SELECT COALESCE(1, 'x');
SELECT NULLIF(1, 1.0);
```

**G17. CAST and coercion.** `pg_type_casts.rs` (13 tests) covers the common
paths. Untested: implicit int→float in comparison; boolean coercion
(`'true'::BOOLEAN`, `1::BOOLEAN` are runs-or-not entries in
`sql_support_matrix.rs:1548/5124`); out-of-range casts (see G3). text→int
error (22P02) is only DSN-gated (`differential_pg.rs:2298`, `:775`); the
unconditional near-miss, `sec_sql_adversarial.rs:438`, asserts only that
`WHERE int_col = 'x'` does not *panic*. `type_ddl.rs:95/206` asserts 22P02 for
**enum labels**, not numeric parsing.

**G18. Order-dependent assertions without ORDER BY.** A migration fragility,
not a semantic gap — and the audit *has* been run. The suite is mostly
disciplined (most assertions include ORDER BY or `.sort()` first, e.g.
`set_ops_ctes.rs::collect_i64_sorted` at line 54). Real hits:

- `groupby_low_card_aggs.rs:127` — `GROUP BY k` with **no ORDER BY**, then
  indexes `rows[0]/rows[1]/rows[2]` positionally (155, 161, 168). Breaks on any
  hash-aggregate with different emission order. **Highest-risk instance.**
- `timescale_caggs.rs:402` — same pattern, single row today, recurs in-file.
- `array_agg_perf_shape.rs:230` — `GROUP BY g`, no ORDER BY (single group, so
  latent).
- `dml_extras.rs:1107` and `:1277`, `postgrest_pgadmin_compat.rs:213` —
  positional assertions over unordered scans.

Expect a handful of breakages concentrated in GROUP BY tests, not a systemic
problem. Fix them *before* the swap so they do not read as engine defects.

### Gap summary

| # | Gap | Priority | Current state |
|---|---|---|---|
| G1 | Outer-join NULL extension, RIGHT/FULL, ON vs WHERE | P0 | No join test file; only LATERAL covers extension |
| G2 | 3VL: NOT IN (subquery w/ NULL), IN w/ NULL, NULL AND FALSE | P0 | All subquery fixtures are NOT NULL |
| G3 | Integer/numeric overflow must error | P0 | Only DSN-gated, self-flagged as likely divergence |
| G4 | NaN / -0.0 / Infinity ordering | P0 | Zero coverage; no SQL `'NaN'` literal in the suite |
| G5 | `pg_type_casts.rs:340` pins PG-incorrect float→int | P0 | **Will fail a correct engine.** Fix before migrating |
| G6 | Explicit NULLS FIRST/LAST | P1 | Fixture has no NULLs; defaults *are* covered |
| G7 | GROUP BY NULL keys, DISTINCT with NULLs | P1 | NULL *values* covered; NULL *keys* barely |
| G8 | Empty-input aggregates | P1 | Ungrouped covered; AVG and GROUP BY forms not |
| G9 | Window default frame, RANGE peers, EXCLUDE | P1 | Main frames strong; edges absent |
| G10 | DST, TIMESTAMP↔TIMESTAMPTZ, month-clamp | P1 | Every tz test uses UTC |
| G11 | NUMERIC scale propagation / overflow | P1 | Storage + wire round-trip only |
| G12 | Division by zero | P1 | Assertion accepts either outcome |
| G13 | CHAR padding, collation, LIKE ESCAPE | P2 | `enforce_charlen` has no test |
| G14 | Scalar subquery 21000, zero-row → NULL | P2 | Error string exists, untested |
| G15 | Set-op NULL dedup, type unification | P2 | Fixtures are all NOT NULL |
| G16 | CASE/COALESCE unification, short-circuit | P2 | Not tested |
| G17 | CAST errors, boolean coercion, range | P2 | Partial, mostly DSN-gated |
| G18 | Positional assertions without ORDER BY | P2 | Audited — ~6 sites, GROUP BY-concentrated |

---

## 5. Recommended harness — the Phase 0 build

Ordered by dependency. Nothing in Phase 1+ should start until 5.1 and 5.2 are
green.

### 5.1 Get the engine-critical tests into CI (blocking)

The exclusion at `ci.yml:136` is caused by the BENCHMARK bucket, not by the
tests we need. 141 benchmark files link a near-full workspace each; the 95
ENGINE-CRITICAL files are a smaller problem. Stop treating
`basin-integration-tests` as one indivisible unit.

In order of preference:

1. **One test binary instead of many.** Cargo links one binary per file in
   `tests/`. Consolidating the conformance targets behind a single
   `tests/conformance/main.rs` with `mod` declarations collapses ~95 links into
   one. Largest single lever; compatible with (2).
2. **Split the package.** Extract ENGINE-CRITICAL + STORAGE targets into
   `basin-conformance-tests` with a minimal dependency set (`basin-engine`,
   `basin-catalog`, `basin-storage`, `basin-common` — not `basin-rest`,
   `basin-realtime`, `basin-auth`, `basin-blob`, `axum`). Run that in CI; leave
   `basin-integration-tests` excluded. Attacks the root cause.
3. **Shrink the link.** The repo already reaches for per-package profile
   tricks; apply the same shape to the test package:
   ```toml
   [profile.test.package.basin-conformance-tests]
   debug = 0
   strip = true
   ```
   plus `[profile.test] debug = 0` and, on Linux CI, `-C link-arg=-fuse-ld=lld`
   or `mold`, which cut peak linker RSS substantially. Often enough on its own
   to clear 7GB.
4. **Bigger runner** for a nightly full-suite job — the BENCHMARK bucket only.
   A cost decision, not an engineering one, and not the PR gate.

**Do not skip this.** Every other recommendation is worthless if the result is
a suite that still does not run.

### 5.2 Switch on the PG oracle and extend it (blocking)

Two steps, in order.

**(a) Turn it on.** Add a Postgres 16 service container to `ci.yml` and export
`PG_DIFF_TEST_DSN`. This alone activates 79 existing cell-by-cell tests and
converts a large fraction of §4 — G1's LEFT JOIN case, G2's NOT IN list form,
G3's int8→int4 overflow, G6's explicit NULLS FIRST/LAST, G11's numeric
precision, G12's 22012, G17's 22P02 — from "untested" to "tested". It is a
workflow edit. It is the highest value-per-line change available in this
document.

Make it **fail hard** when the DSN is absent under `CI=true`. A gate that
no-ops on misconfiguration is not a gate.

**(b) Extend it.** Add one test per gap G1–G17 using the SQL in §4. Before
that, verify the normalisation (§3.1) — if the comparison is text-based and
type-blind, add a type-aware mode, because a `Float64` where PG returns
`Numeric` is a real defect that a text compare hides.

**Fixture discipline is the lesson of §4.** Nearly every gap above exists
because a fixture was declared `NOT NULL`: `subquery_patterns.rs:93`,
`set_ops_ctes.rs:84`, `select_advanced.rs`'s `t`, `groupby_low_card_aggs.rs`'s
`k`, `array_agg_perf_shape.rs`'s `g`. The tests were written; the data made
them unfalsifiable. Every new fixture must contain NULLs, and where relevant
NaN, -0.0, empty strings, duplicate ORDER BY keys, and boundary integers.

### 5.3 Build the two-engine differential runner

DataFusion is deleted last, so both engines coexist for the whole migration.
This is the highest-leverage asset available.

**Shape:** a runner that takes a SQL string, executes it against a
DataFusion-backed session and a new-engine session, and asserts identical
result sets — same values, same **types**, same row order under `ORDER BY`,
same error SQLSTATE.

**Precedent to copy, not invent:** `hottier_differential.rs` (1,342 LOC; 3
tests × 88 shapes × 4 modes), `gin_row_tier_differential.rs`,
`wasm_functions_differential.rs` already do in-process two-path diffing against
Basin's session API. Read `hottier_differential.rs` first.

**Selector:** a runtime switch, not a cargo feature, so one binary holds both.
`sql_support_matrix.rs` already establishes the convention — it toggles
`BASIN_PG_QUERY` / `BASIN_PG_QUERY_PLAN` across three passes in a single test.
Follow it (`BASIN_ENGINE=df|native`) and inherit its documented constraint: env
vars are process-wide, so passes run serially.

**Corpus, in order:**

1. **The ~1,500 SQL fragments already in `sql_support_matrix.rs`.** They are
   written, curated, and currently prove only that SQL *runs*. Pointing the
   two-engine differ at the same list upgrades all of them from acceptance
   checks to conformance checks at near-zero authoring cost. **The cheapest
   large win in this document.**
2. The 88 shapes in `hottier_differential.rs` — already differential-shaped.
3. The §4 gap SQL.
4. Extracted SQL from the ENGINE-CRITICAL bucket.
5. A randomised shape generator (see 5.4).

**Three-way mode:** where PG is available, diff all three. Where the two Basin
engines agree but PG differs, that is a **pre-existing** Basin divergence —
record it as an accepted-divergence test, do not fix it mid-migration, or the
migration absorbs unrelated compat work. `differential_pg.rs:2316` and
`viability_pg_compat_funcs.rs` show the repo already has this discipline.

### 5.4 Revive sqllogictest, and raise its threshold

One `.slt` file behind a 78% pass threshold is not infrastructure — it is a
green light that means nothing. But the runner exists, and `.slt` files are
**data, not code**, so they add zero link cost to the §5.1 problem. That makes
`.slt` the right authoring format for new conformance cases.

Actions: raise `PASS_THRESHOLD_PCT` to 100 with explicit per-record skips
rather than a blanket tolerance; express every §4 gap as an `.slt` file; drive
the files from the two-engine runner in 5.3 so each is checked against both
engines.

A randomised query generator (SQLancer-style, or an in-house shape generator
over a NULL-heavy fixed schema) is the only realistic way to cover the
combinatorial space of join × NULL × type × frame. Phase 1, not Phase 0 — but
5.3 is its prerequisite, so give 5.3 a programmatic entry point, not a
macro-only API.

### 5.5 What not to do

- Do not treat `sql_support_matrix.rs`, `compare_postgres_common.rs`, or
  `3way_pg_compat.rs` passing as evidence of conformance. They are,
  respectively, an acceptance classifier, a benchmark, and a report generator.
- Do not port the BENCHMARK bucket into the gate. Timing cards on shared CI
  runners produce flakes and erode trust in the gate.
- Do not delete `regression_engine_bugs.rs` bug #40 when the DataFusion arrow
  conversion goes away. Retarget it.
- Do not fix Basin-vs-PG divergences discovered by the newly-enabled oracle
  during the migration. Record and pin them; fix after.
- Do not trust expected values that were recorded from DataFusion rather than
  PG (G5). Audit them first, or a correct new engine will look like a
  regression.

---

## 6. Open questions

1. **UNVERIFIED** — the normalisation in `differential_pg.rs` (row order, float
   tolerance, type blindness). Blocks sizing 5.2(b).
2. **UNVERIFIED** — whether the PG side of the oracle is behind a trait or
   hardcoded `tokio-postgres`. Blocks sizing 5.3's three-way mode.
3. **UNVERIFIED** — `pg_type_casts.rs:340`. Confirm against a live PG that
   float8→int rounds rather than truncates, then fix the test. This is a
   pre-migration blocker (G5).
4. **UNVERIFIED** — per-file classification for ~150 files not individually
   inspected; ENGINE-CRITICAL is a lower bound.
5. How much of `basin-engine`'s 1,843 unit tests are DataFusion-API-shaped
   (constructing `LogicalPlan`/`Expr`) versus behaviour-shaped? The former die
   with DataFusion and are not migration assets. Triage `pg_operators.rs` (153
   tests), `executor.rs` (114), `dml_mutate.rs` (82) first. See
   [02 — logical IR surface](02-logical-ir-surface.md).
6. How many other assertions share `coverage_errorpaths.rs:244-247`'s
   accept-either shape? Those read as coverage and are not.
