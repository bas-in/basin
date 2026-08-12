---
title: "DF removal 07 — the conformance suite is not a safety net yet"
nav_section: migration
sidebar_position: 7
summary: "Inventory and classification of Basin's 197k-LOC integration suite against the engine swap, the semantic gaps a homegrown engine would plausibly get wrong, and the Phase 0 work — CI re-enablement and a differential oracle — that must land before any engine code is written."
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

It is not. Three findings, in descending order of severity:

**1. The integration suite does not run in CI. At all.**

`.github/workflows/ci.yml:136`:

```yaml
- run: cargo test --workspace --exclude basin-integration-tests --no-fail-fast --locked
```

The recorded reason (`ci.yml:79`, comment above the step) is a link-time OOM:

> `--exclude basin-integration-tests`: those are benchmark-flavoured
> viability cards (`viability_*`, `s3_scaling_*`) that link a near-full
> workspace per binary and OOM a 7GB runner at link time with
> `ld terminated with signal 7`. They are built for a workstation, not
> free CI — and clippy above still compiles them.

So 383 test binaries / 2,524 `#[test]` functions are compiled by clippy
(`--all-targets`) but **never executed** on any pull request. Whatever those
tests assert about current DataFusion behaviour, nothing is enforcing it today,
and nothing would catch a regression introduced by the engine swap.

**2. The "compliance suite" runs 4 targets out of 383.**

`scripts/run-suite.sh` — the documented entry point for "run the full Basin
compliance suite" (`tests/integration/suite/README.md`) — has exactly four fast
categories:

```
sql-compat        | sql_support_matrix
feature-coverage  | feature_coverage
differential-pg   | differential_pg
performance       | perf_suite
```

Of these, only `differential_pg` checks *results*. `sql_support_matrix` checks
that SQL **parses, plans, and executes without erroring** — it classifies each
fragment as Ok / ExecFailed / PlannerRejected / ParserRejected / OutOfScope and
writes `docs/sql-support.md`. It never compares a value. A new engine that
returns wrong answers for every one of its ~1,500 fragments would pass it green.

**3. The differential oracle is thin, and there is one `.slt` file.**

`tests/integration/tests/differential_pg.rs` is the only thing in the repo that
compares Basin against ground truth. It holds 79 `#[tokio::test]` functions, but
the README reports a live run as "23 passed; 3 ignored" — i.e. the effective
coverage is roughly two dozen shapes, and it skips silently and exits 0 when
`PG_DIFF_TEST_DSN` is unset, which is its state in CI.
`tests/integration/sqllogictest-suites/` contains exactly one file, `basic.slt`.

> **UNVERIFIED:** the 23-vs-79 discrepancy is taken from the sample output
> quoted in `DIFFERENTIAL_README.md`, which may predate later test additions.
> A direct count found 79 `#[tokio::test]` attributes and **zero** `#[ignore]`
> attributes in the current file, so the live number today may be closer to 79.
> Resolve this by running the suite before sizing the gap work.

### What this document is for

Because of the above, this document is not "here is your safety net". It is the
**Phase 0 spec**: the harness and coverage work that must land *before the first
line of new engine code is written*, because the migration's entire risk model
assumes a conformance gate exists. Section 5 is the buildable part; sections 1–4
are the evidence for why it is scoped the way it is.

---

## 1. Inventory

Totals, measured across `tests/integration/tests/*.rs`:

| Metric | Value |
|---|---|
| Test files | 383 |
| Lines of test code | 197,550 |
| `#[test]` / `#[tokio::test]` functions | 2,524 |
| `basin-engine` in-file unit tests | 1,843 |
| `basin-engine` source LOC (incl. tests) | 194,345 |

### 1.1 Bucket summary

Classification is by filename convention plus content sampling; see §2 for the
definitions and §1.4 for the caveats.

| Bucket | Files | LOC | Test cases | Relationship to the swap |
|---|---:|---:|---:|---|
| ENGINE-CRITICAL | 95 | 66,619 | 1,443 | The conformance gate — must pass identically |
| STORAGE / EXEC-ADJACENT | 74 | 35,985 | 463 | Indirectly critical: pruning, overlays, MVCC visibility all change *which rows* an engine sees |
| PERIPHERAL | 73 | 38,987 | 392 | Auth, REST, realtime, CDC, cron, blob, pool, ORM drivers, wire protocol — unaffected |
| BENCHMARK | 141 | 55,959 | 226 | Timing cards; not correctness |

The BENCHMARK bucket is the largest by file count and is also the direct cause
of the CI exclusion (§5.1): `viability_*`, `s3_*`, `scale_*`, `scaling_*`,
`ext_bench_*`, `compare_postgres_*`, and `*_probe` files each link a near-full
workspace to measure latency.

### 1.2 The engine-critical files that matter most

Within the 95 ENGINE-CRITICAL files, the surface splits sharply between
**extension/UDF conformance** (large, well covered) and **core relational
algebra** (small).

Extension and type-system surface — genuinely strong assets:

| File | LOC | Tests | Surface |
|---|---:|---:|---|
| `postgis_conformance.rs` | 2,125 | 58 | Spatial types, predicates, ST_* functions |
| `fts_conformance.rs` | 1,888 | 52 | `tsvector`/`tsquery`, ranking, `@@` |
| `vector_conformance.rs` | 1,851 | 37 | pgvector operators, distance functions |
| `range_conformance.rs` | 1,614 | 58 | Range types, containment, overlap operators |
| `trgm_conformance.rs` | 749 | 43 | `pg_trgm` similarity |
| `trgm_sql_conformance.rs` | 711 | 25 | Trigram SQL surface |
| `jsonb_udfs.rs` | 717 | 22 | JSONB function surface |
| `json_path_extras.rs` | 584 | 28 | JSONPath |
| `timescale_conformance.rs` | 866 | 18 | Hypertables, time_bucket |
| `pg_scalar_fn_inventory.rs` | 574 | 41 | Scalar function catalogue |
| `pg_operators.rs` | 493 | 23 | Operator surface |
| `array_fns.rs` | 657 | 31 | Array functions |
| `interval_tz.rs` | 825 | 37 | Interval and timezone arithmetic |

Core relational algebra — the actual engine-swap conformance gate:

| File | LOC | Tests | Surface |
|---|---:|---:|---|
| `shape_sweeps.rs` | 1,008 | 3 | Query shape sweeps |
| `window_fns.rs` | 960 | 24 | Window functions, frames |
| `is_operators.rs` | 894 | 49 | `IS TRUE/FALSE/UNKNOWN/DISTINCT FROM`, COALESCE, NULLIF, CASE, GREATEST/LEAST |
| `pg_aggregates.rs` | 754 | 17 | COUNT/SUM/AVG/MIN/MAX, statistical aggs, json aggs |
| `pagination_residuals.rs` | 730 | 13 | LIMIT/OFFSET residuals |
| `catalog_window.rs` | 710 | 1 | Window over catalog |
| `lateral_joins.rs` | 623 | 11 | LATERAL, table functions |
| `subquery_patterns.rs` | 559 | 13 | EXISTS, IN, ANY/ALL, scalar and derived |
| `set_ops_ctes.rs` | 558 | 13 | UNION/INTERSECT/EXCEPT (+ALL), CTEs, recursive CTEs |
| `views_and_join_dml.rs` | 455 | 11 | Views, join-driven DML |
| `select_advanced.rs` | 388 | 17 | DISTINCT ON, FOR UPDATE, FETCH/OFFSET, ORDER BY NULLS |
| `aggregate_tuning.rs` | 375 | 7 | Aggregate execution tuning |
| `groupby_low_card_aggs.rs` | 247 | 3 | Low-cardinality GROUP BY |
| `limit_pushdown.rs` | 237 | 5 | LIMIT pushdown |
| `where_null_3vl_fold.rs` | 231 | 6 | `col = NULL` folding to empty |

**That is 8,729 LOC and 193 test cases — 4.4% of the integration suite's lines
and 7.6% of its test cases.** Everything the engine swap most endangers lives in
that table. Add `differential_pg.rs` (3,004 LOC, 79 tests) and it is still under
6% of the suite.

**There is no dedicated join test file.** `LEFT JOIN` appears 71 times across 11
files, most of them ORM-compat, perf, or differential files. `FULL OUTER JOIN`
appears 6 times across 5 files. `RIGHT JOIN` appears in 2 files
(`3way_pg_compat.rs`, `sql_support_matrix.rs`), both of which are acceptance-
shaped rather than semantics-shaped. For a query-engine rewrite, joins are the
single highest-risk area and they are the least covered.

### 1.3 Other notable assets

- `regression_engine_bugs.rs` (1,457 LOC, 30 tests) — pinned regressions for
  engine bugs #40 (`Utf8View` conversion), #41 (rollback over-restores rows),
  #42. Bug #40 is explicitly a DataFusion-arrow-to-workspace-arrow conversion
  bug; it will become moot, but the *test* should be retargeted, not deleted.
- `coverage_errorpaths.rs` (957 LOC, 27 tests) — SQLSTATE error paths. See §4
  for why several of these assertions are too lenient to serve as a gate.
- `sql_syntax_fuzz.rs`, `sec_sql_adversarial.rs`, `sec_parser_dos.rs` — parser
  robustness; useful as crash gates, silent on semantics.
- `3way_pg_compat.rs` (1,050 LOC, 2 tests) — three-way comparison, one of the
  few places `RIGHT`/`FULL` joins appear at all.

### 1.4 Caveats on the inventory

- Bucketing is heuristic (filename patterns + sampling). Files such as
  `hottier_differential.rs` and `gin_row_tier_differential.rs` land in STORAGE
  but contain real SQL-semantics assertions; the ENGINE-CRITICAL count is
  therefore a lower bound. **UNVERIFIED** at the per-file level for the ~150
  files not individually opened.
- Test-case counts are attribute counts. Files with 1 test frequently contain
  dozens of assertions in a loop (`sql_support_matrix.rs` = 1 test,
  ~1,500 fragments), so "test cases" understates breadth and overstates
  isolation — a single failure aborts the rest of the file.

---

## 2. Classification rules

**ENGINE-CRITICAL** — exercises SQL execution semantics: relational algebra,
expression evaluation, type coercion, NULL semantics, ordering, aggregation,
windows, subqueries, set operations, and the scalar/aggregate function surface.
These must produce byte-identical results after the swap. They *are* the gate.

**STORAGE / EXEC-ADJACENT** — compaction, overlays, row-tier/hot-tier residency,
MVCC visibility, index probes (GIN/BTree/RTree/trgm), bloom and min/max pruning,
COPY ingest, fastpath routing, prepared-statement caching. Not "SQL semantics",
but they determine *which rows reach the operators*. A pruning bug and a join
bug are indistinguishable from the result set. Treat as second-tier gate.

**PERIPHERAL** — auth/OAuth/MFA/JWT/RLS-plumbing, REST, webhooks, realtime
(WS/SSE/presence), CDC, cron, blob/object storage, connection pooling, ORM and
migration-tool compat, pgwire protocol framing, multi-region/lease/placement,
project lifecycle. Unaffected by the engine swap *except* where they assert on
result content — RLS predicate enforcement in particular is executed by the
engine and should be re-read as engine-critical.

**BENCHMARK** — `viability_*`, `s3_*`, `scale_*`, `scaling_*`, `ext_bench_*`,
`compare_postgres_*`, `perf_*`, `*_probe`, `*_latency_*`, `noisy_*`, `*_soak`.
Measurement, not correctness. Some (`compare_postgres.rs::perf_smoke_pg_10k`,
`scale_invariants.rs`) carry hard correctness assertions inside a perf card;
those assertions are worth extracting into the gate.

---

## 3. What the existing harnesses actually prove

| Harness | File | Proves | Does not prove |
|---|---|---|---|
| Differential PG oracle | `tests/integration/tests/differential_pg.rs` | Basin's answers match a real PG 16/18 for ~23–79 shapes | Anything outside those shapes; skips silently without `PG_DIFF_TEST_DSN` |
| SQL support matrix | `tests/integration/tests/sql_support_matrix.rs` | ~1,500 fragments parse/plan/execute without error, across 3 parser configs (`BASIN_PG_QUERY`, `BASIN_PG_QUERY_PLAN`) | **Any result value whatsoever.** Pure acceptance classification |
| Feature coverage | `tests/integration/tests/feature_coverage.rs` | One assertion per ✅ row in `CAPABILITIES.md` | Depth |
| Perf suite | `tests/integration/tests/perf_suite.rs` | Per-shape timing vs documented thresholds | Correctness |
| sqllogictest | `sqllogictest-suites/basic.slt` (1 file), `tests/sqllogictest_pg.rs` | Effectively nothing — one file | Everything. Vestigial |
| e2e runner | `services/basin-e2e-runner` | Over-the-network perf, noisy-neighbour fairness, replica handoff | Not a conformance runner; workloads are `perf`, `noisy-neighbor`, `handoff` |

### 3.1 The differential oracle in detail

`differential_pg.rs` starts an in-process Basin pgwire server and connects to a
real Postgres via `PG_DIFF_TEST_DSN`, running identical SQL against both and
failing on divergence. The README states its rationale precisely, and it is the
right rationale for this migration:

> Every existing Basin test compares against hand-coded expected outputs. This
> harness closes the verification gap: Basin **and** its expected outputs could
> be wrong in the same direction.

The helper shape (`run_setup`, `run_assert_match`, `run_assert_both_error`) is
already the right API. Example of it doing exactly the job we need:

```rust
runner.run_assert_both_error("SELECT 1 / 0", Some("22012")).await
```

```rust
// NOT IN with NULL in list → should return 0 rows.
runner.run_assert_match(&format!(
    "SELECT n FROM {t} WHERE n NOT IN (1, NULL) ORDER BY n"
)).await.unwrap();
```

**UNVERIFIED, and load-bearing for §5:** the normalisation the comparison
applies before diffing — row-order sensitivity, float tolerance, NULL rendering,
and whether values are compared as text (in which case `1` vs `1.0`, or
`Decimal128` vs `Float64`, would pass silently). A text-blind comparison is
adequate for a Basin-vs-PG oracle but is a real weakness for a
Basin-old-vs-Basin-new oracle, where output *type* drift is a likely failure
mode. Confirm before extending.

Also **UNVERIFIED**: whether the PG side sits behind a trait or is hardcoded
`tokio-postgres`. This determines how cheap §5.2 is.

### 3.2 The in-repo differential pattern

The repo already has files named `*_differential.rs` — `hottier_differential.rs`
(1,342 LOC), `realtime_differential.rs`, `gin_row_tier_differential.rs`,
`wasm_functions_differential.rs`. These run the same query through two internal
paths (hot tier vs cold, GIN-indexed vs scan, fastpath on vs off) and diff.
**This is already the template for old-engine-vs-new-engine.** It is a stronger
starting point than building something new, because it proves the in-process
two-path-diff pattern works against Basin's own session API rather than over the
wire.

---

## 4. Gap list — semantics DataFusion gets right that a new engine will get wrong

Prioritised by (likelihood a hand-written engine gets it wrong) × (silence of
the failure). Each entry names the evidence and gives SQL that must be added.

Where an entry says a test "exists", read the note — several existing tests are
shaped so they cannot fail.

### P0 — untested, silent, and near-certain to break

**G1. ORDER BY default NULL placement.** Postgres: `ASC` ⇒ NULLS LAST, `DESC` ⇒
NULLS FIRST. Arrow/DataFusion's native default is the opposite unless configured.

Evidence — this is the clearest finding in the audit.
`select_advanced.rs::order_by_nulls_first_accepted` and
`order_by_nulls_last_accepted` both query table `t`, declared as:

```sql
CREATE TABLE t (id BIGINT NOT NULL, cat TEXT NOT NULL, val BIGINT NOT NULL)
INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)
```

`val` is `NOT NULL` and no NULL is ever inserted. The tests assert
`[1,2,3,4]` and `[4,3,2,1]`. **They exercise NULLS FIRST/LAST against data
containing no NULLs.** The function names say "accepted", which is honest — they
test acceptance of the syntax, not the ordering. A new engine could invert NULL
placement entirely and both tests stay green.

```sql
INSERT INTO t2(id, val) VALUES (1, 10), (2, NULL), (3, 30);
SELECT id FROM t2 ORDER BY val ASC;              -- PG: 1,3,2   (NULLS LAST)
SELECT id FROM t2 ORDER BY val DESC;             -- PG: 2,3,1   (NULLS FIRST)
SELECT id FROM t2 ORDER BY val ASC NULLS FIRST;  -- PG: 2,1,3
SELECT id FROM t2 ORDER BY val DESC NULLS LAST;  -- PG: 3,1,2
```

**G2. Outer-join NULL extension.** No dedicated join test file exists. `LEFT
JOIN` = 71 occurrences across 11 mostly-ORM/perf files; `FULL OUTER JOIN` = 6
occurrences; `RIGHT JOIN` = 2 files. The highest-risk operator in the rewrite
has the thinnest coverage in the suite.

```sql
-- NULL-extended right side
SELECT l.id, r.v FROM l LEFT JOIN r ON l.id = r.id ORDER BY l.id;
-- COUNT(*) vs COUNT(col) over a null-extended side
SELECT l.id, COUNT(*), COUNT(r.v) FROM l LEFT JOIN r ON l.id = r.id GROUP BY l.id;
-- join key that is NULL never matches, even against another NULL
SELECT * FROM l LEFT JOIN r ON l.k = r.k;   -- rows with l.k IS NULL are null-extended
-- ON vs WHERE: this degrades LEFT JOIN to INNER
SELECT * FROM l LEFT JOIN r ON l.id = r.id WHERE r.v > 5;
SELECT * FROM l LEFT JOIN r ON l.id = r.id AND r.v > 5;   -- different result
-- FULL OUTER with no match on either side
SELECT l.id, r.id FROM l FULL OUTER JOIN r ON l.id = r.id ORDER BY 1 NULLS LAST, 2 NULLS LAST;
```

**G3. Empty-input aggregates.** `SUM`/`AVG`/`MIN`/`MAX` over zero rows return
NULL, not 0; `COUNT` returns 0. With no `GROUP BY` the query returns exactly one
row; with `GROUP BY` it returns zero rows. Searching for `WHERE 1=0` / `WHERE
false` across the suite finds only pgwire RowDescription tests
(`pgwire_wire_compat.rs`) and a DML `RETURNING` synthesis comment — **no
aggregate-over-empty-input assertion anywhere**.

```sql
SELECT SUM(v), AVG(v), MIN(v), MAX(v), COUNT(v), COUNT(*) FROM t WHERE 1=0;
-- PG: one row: NULL, NULL, NULL, NULL, 0, 0
SELECT k, SUM(v) FROM t WHERE 1=0 GROUP BY k;   -- PG: zero rows
SELECT SUM(v) FROM t WHERE 1=0 HAVING SUM(v) IS NULL;  -- PG: one row
SELECT COUNT(*) FROM (SELECT 1) x WHERE false;  -- PG: 0
```

**G4. Three-valued logic in `NOT IN` against a subquery containing NULL.** The
one existing test, `subquery_patterns.rs::subq_not_in_subquery`, asserts
`ids == vec![4, 5]` — a non-empty result, which means the subquery
(`SELECT order_id FROM items`) contains no NULLs. The NULL trap is untested on
the subquery form. `differential_pg.rs::diff_null_not_in_with_null` covers the
*list* form `NOT IN (1, NULL)` only.

```sql
-- items.order_id contains at least one NULL
SELECT id FROM orders WHERE id NOT IN (SELECT order_id FROM items);  -- PG: 0 rows
SELECT id FROM orders WHERE id IN (SELECT order_id FROM items);      -- unaffected by NULL
-- NOT EXISTS is NOT equivalent and must still return rows
SELECT id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id);
SELECT 1 WHERE (NULL = NULL) IS NULL;
SELECT (NULL AND FALSE), (NULL AND TRUE), (NULL OR TRUE), (NULL OR FALSE);
-- PG: false, NULL, true, NULL
```

**G5. Integer overflow must error, not wrap.** `grep -ril overflow` over the
integration tests hits 5 files, all of them adversarial-parser, noisy-neighbour,
or the perf-common harness. No arithmetic overflow assertion exists.

```sql
SELECT 2147483647::int4 + 1;          -- PG: ERROR 22003 integer out of range
SELECT (-2147483648)::int4 * -1;      -- PG: ERROR 22003
SELECT abs((-2147483648)::int4);      -- PG: ERROR 22003
SELECT 9223372036854775807::int8 + 1; -- PG: ERROR 22003
SELECT SUM(v) FROM t;                 -- SUM(int4) → bigint; SUM(int8) → numeric
```

The `SUM` return-type promotion is a separate and equally silent hazard: a new
engine that returns `int8` where PG returns `numeric` changes the wire type.

### P1 — untested, and would surface as wrong values rather than errors

**G6. NUMERIC/DECIMAL scale propagation and overflow.** Coverage of `NUMERIC` is
storage-shaped only: `extra_types.rs` asserts `NUMERIC(10,2)` maps to
`Decimal128(10,2)` and round-trips; `numeric_type_pgwire.rs` asserts wire
encoding. Nothing tests arithmetic.

```sql
SELECT 1.005::numeric(10,3) * 3;            -- scale propagation on *
SELECT 10::numeric / 3;                     -- PG: 3.3333333333333333333 (div scale rule)
SELECT (1e30::numeric) * (1e30::numeric);   -- precision growth
SELECT 1.5::numeric(2,0);                   -- PG: 2  (half-up, not banker's)
SELECT 2.5::numeric(2,0);                   -- PG: 3
SELECT 999.99::numeric(4,2) + 0.01;         -- PG: ERROR 22003 numeric field overflow
SELECT 0.1::float8::numeric;                -- float→numeric conversion
```

Decimal128 is Basin's declared storage for NUMERIC (ADR 0024), so an overflow
here is a storage-layer error too.

**G7. Float semantics: NaN ordering, -0.0, Infinity.** Postgres sorts `NaN` as
**larger than everything, including Infinity**, and treats `NaN = NaN` as true
for sorting/grouping purposes while IEEE says otherwise. `-0.0 = 0.0` is true but
they are distinct bit patterns for `DISTINCT`/`GROUP BY`. Grep for `'NaN'` /
`f64::NAN` / `is_nan` hits only perf harnesses (median computation) and JSONB
paths — no SQL-level float ordering test.

```sql
SELECT v FROM f ORDER BY v;
-- with v in {1.0, 'NaN', 'Infinity', '-Infinity', NULL}
-- PG ASC: -Infinity, 1.0, Infinity, NaN, NULL
SELECT 'NaN'::float8 = 'NaN'::float8;        -- PG: true
SELECT 'NaN'::float8 > 'Infinity'::float8;   -- PG: true
SELECT DISTINCT v FROM (VALUES (0.0::float8), (-0.0::float8)) s(v);  -- PG: 1 row
SELECT 1.0::float8 / 0.0;                    -- PG: ERROR 22012 division by zero
```

Note the last one: PG errors on **float** division by zero too, it does not
return Infinity.

**G8. Division-by-zero is not actually pinned.**
`coverage_errorpaths.rs::error_division_by_zero_in_select` accepts *either*
outcome:

```rust
Ok(ExecResult::Rows { .. }) => {
    // DataFusion may return a batch with a NULL for integer div-by-zero
    // (some builds propagate NULL instead of erroring). Accept either
    // outcome but document it.
    println!("[error_paths] division by zero returned rows (NULL propagation path)");
}
```

This is a documented non-assertion. `differential_pg.rs` does pin it
(`run_assert_both_error("SELECT 1 / 0", Some("22012"))`), but only for the
integer literal form, and only when the oracle is running. Audit
`coverage_errorpaths.rs` for other match-arms shaped like this; a lenient
assertion is worse than no assertion, because it reads as coverage.

**G9. GROUP BY / DISTINCT NULL grouping.** NULLs are *equal* for grouping and
DISTINCT but *not* for `=`. `SELECT DISTINCT` appears only 21 times in the whole
suite, mostly in ORM-compat and perf files.

```sql
SELECT k, COUNT(*) FROM t GROUP BY k;      -- all NULL k collapse to ONE group
SELECT DISTINCT k FROM t;                  -- NULL appears exactly once
SELECT COUNT(DISTINCT k) FROM t;           -- PG: does NOT count NULL
SELECT k FROM t GROUP BY k HAVING k IS NULL;
SELECT DISTINCT a, b FROM t;               -- (NULL,1) and (NULL,1) are one row
SELECT DISTINCT ON (k) k, v FROM t ORDER BY k, v;  -- NULL key group
```

**G10. Timestamp / timezone coercion.** `AT TIME ZONE` appears in exactly two
files (`interval_tz.rs`, `sql_support_matrix.rs`). Comparison between
`timestamp` and `timestamptz` — which in PG is resolved via the session
`TimeZone` GUC — is the specific hazard: a new engine that treats both as UTC
instants gives different answers in a non-UTC session.

```sql
SET TIME ZONE 'America/New_York';
SELECT '2024-01-15 12:00:00'::timestamp AT TIME ZONE 'UTC';
SELECT '2024-01-15 12:00:00'::timestamptz AT TIME ZONE 'UTC';
SELECT '2024-01-15 12:00:00'::timestamp = '2024-01-15 12:00:00'::timestamptz;
-- DST boundary (spring forward, 2024-03-10 02:00 does not exist in America/New_York)
SELECT '2024-03-10 02:30:00'::timestamptz;
SELECT '2024-03-09 12:00'::timestamptz + interval '1 day';   -- 24h vs 1 calendar day
SELECT date_trunc('day', ts AT TIME ZONE 'America/New_York') FROM t;
SELECT '2024-01-31'::date + interval '1 month';   -- PG: 2024-02-29, not 03-02
SELECT EXTRACT(epoch FROM '2024-01-15 12:00:00'::timestamptz);
```

**G11. Window frame edge bounds.** `window_fns.rs` (24 tests) covers the main
frames well — `ROWS UNBOUNDED PRECEDING`, `RANGE BETWEEN UNBOUNDED PRECEDING AND
UNBOUNDED FOLLOWING`, `GROUPS`, `RANGE INTERVAL '5 min' PRECEDING`, LAG/LEAD with
offset+default, NTILE, first/last/nth_value. What is missing is the edge
behaviour:

```sql
-- the classic: default frame is RANGE ... CURRENT ROW, so last_value != max
SELECT id, last_value(v) OVER (ORDER BY k) FROM t;        -- peers, not partition end
SELECT id, last_value(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING
                                              AND UNBOUNDED FOLLOWING) FROM t;
-- RANGE with peers/ties: rows with equal ORDER BY key share a frame
SELECT k, SUM(v) OVER (ORDER BY k) FROM t;   -- ties get identical sums
SELECT k, SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING
                                       AND CURRENT ROW) FROM t;  -- ties differ
-- empty frame → NULL for SUM, 0 for COUNT
SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM t;
SELECT COUNT(*) OVER (ORDER BY k ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM t;
-- EXCLUDE
SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
                    FOLLOWING EXCLUDE CURRENT ROW) FROM t;
-- window over a partition with zero rows / all-NULL ordering key
SELECT row_number() OVER (PARTITION BY k ORDER BY v) FROM t;  -- k all NULL
```

The `last_value` default-frame case is the highest-value single addition: it is
the most commonly mis-implemented window semantic in the industry, and
`window_fns.rs::last_value_full_frame` tests the *explicit* full frame, not the
default.

### P2 — real, lower blast radius, or partially covered

**G12. Scalar subquery cardinality.** A correlated scalar subquery returning >1
row must raise `21000`; returning 0 rows must yield NULL, not zero rows.
`subquery_patterns.rs` has `subq_scalar_in_select` and
`subq_correlated_scalar_in_select` but **UNVERIFIED** whether either asserts the
error path.

```sql
SELECT (SELECT v FROM r WHERE r.k = l.k) FROM l;  -- ERROR 21000 if >1 match
SELECT (SELECT v FROM r WHERE false);             -- PG: NULL
SELECT (SELECT COUNT(*) FROM r WHERE false);      -- PG: 0, not NULL
```

**G13. Set operation NULL and type unification.** `set_ops_ctes.rs` covers
UNION/UNION ALL/INTERSECT/INTERSECT ALL/EXCEPT/EXCEPT ALL, but **UNVERIFIED**
whether NULLs or cross-branch type coercion are in the fixtures.

```sql
SELECT NULL UNION SELECT NULL;                -- PG: 1 row
SELECT NULL UNION ALL SELECT NULL;            -- PG: 2 rows
SELECT 1 UNION SELECT 1.5;                    -- branch types unify to numeric
SELECT 1::int4 UNION SELECT 'x';              -- ERROR 42804
SELECT NULL EXCEPT SELECT NULL;               -- PG: 0 rows (NULL matches NULL here)
SELECT a FROM t INTERSECT ALL SELECT a FROM u; -- duplicate multiplicity = min(m,n)
```

**G14. String comparison, collation, CHAR padding.** `CHAR(n)` appears only in
`sql_support_matrix.rs` (acceptance). PG blank-pads `CHAR(n)` and ignores
trailing blanks in comparison; `VARCHAR`/`TEXT` do not.

```sql
SELECT 'abc'::char(5) = 'abc';                 -- PG: true (blank-padded compare)
SELECT length('abc'::char(5));                 -- PG: 3
SELECT 'abc '::text = 'abc';                   -- PG: false
SELECT '' IS NULL;                             -- PG: false — empty string is not NULL
SELECT 'A' < 'a';                              -- collation-dependent; C locale: true
SELECT upper('ß'), lower('İ');                 -- non-ASCII case mapping
SELECT 'a_b' LIKE 'a\_b', 'axb' LIKE 'a\_b';   -- escape handling
SELECT v FROM t ORDER BY v;                    -- mixed case + multibyte UTF-8 ordering
```

**G15. CAST and implicit coercion.**

```sql
SELECT 'abc'::int;                   -- ERROR 22P02
SELECT '1e10'::int4;                 -- ERROR 22003 / 22P02
SELECT 1 = 1.0;                      -- implicit int→numeric
SELECT 't'::boolean, 'yes'::boolean, '1'::boolean;
SELECT 300::int2;                    -- fits
SELECT 40000::int2;                  -- ERROR 22003
SELECT '2024-13-01'::date;           -- ERROR 22008
```

**G16. CASE / COALESCE short-circuit and type unification.** `is_operators.rs`
covers 49 cases of IS/COALESCE/NULLIF/CASE, which is genuinely good. The
untested part is short-circuit evaluation of an erroring branch:

```sql
SELECT CASE WHEN v <> 0 THEN 1/v ELSE NULL END FROM t;  -- must NOT error on v=0
SELECT COALESCE(a, 1/0) FROM t WHERE a IS NOT NULL;     -- PG: does not error
SELECT COALESCE(1, 'x');                                -- type unification
SELECT NULLIF(1, 1.0);                                  -- cross-type NULLIF
```

Constant folding in the planner is exactly where a new engine breaks this.

**G17. Order-dependent assertions without ORDER BY.** A migration-specific
fragility, not a semantic gap. Any test asserting an exact row vector from a
query with no `ORDER BY` will flake or fail spuriously when the new engine
changes its scan or hash-aggregate output order. This is a mechanical audit that
should be run over the whole ENGINE-CRITICAL bucket *before* the swap, and it is
cheap: flag every `assert_eq!(rows, vec![...])` whose SQL lacks `ORDER BY`.
**UNVERIFIED** — not yet run; expected to produce a large hit list.

### Gap summary table

| # | Gap | Priority | Current state |
|---|---|---|---|
| G1 | ORDER BY default NULL placement | P0 | Tests exist but run against NOT NULL data |
| G2 | Outer-join NULL extension, ON vs WHERE | P0 | No dedicated join test file |
| G3 | Empty-input aggregates (SUM ⇒ NULL) | P0 | Not tested |
| G4 | `NOT IN (subquery with NULL)` | P0 | Only the list form is covered |
| G5 | Integer overflow must error | P0 | Not tested |
| G6 | NUMERIC scale propagation / overflow | P1 | Storage round-trip only |
| G7 | NaN / -0.0 / Infinity ordering | P1 | Not tested |
| G8 | Division by zero | P1 | Existing assertion accepts either outcome |
| G9 | GROUP BY / DISTINCT with NULLs | P1 | 21 `SELECT DISTINCT` in 197k LOC |
| G10 | Timestamp/timestamptz coercion, DST | P1 | `AT TIME ZONE` in 2 files |
| G11 | Window frame edges, default-frame `last_value` | P1 | Main frames covered; edges not |
| G12 | Scalar subquery cardinality error | P2 | UNVERIFIED |
| G13 | Set-op NULL dedup and type unification | P2 | UNVERIFIED |
| G14 | CHAR padding, collation, LIKE escapes | P2 | Acceptance only |
| G15 | CAST error and range behaviour | P2 | Partial |
| G16 | CASE/COALESCE short-circuit | P2 | Not tested |
| G17 | Order-dependent assertions without ORDER BY | P2 | Audit not run |

---

## 5. Recommended harness — the Phase 0 build

Four pieces, in dependency order. Nothing in Phase 1+ of the migration should
start until 5.1 and 5.2 are green.

### 5.1 Get the engine-critical tests into CI (blocking)

The exclusion at `ci.yml:136` is caused by the BENCHMARK bucket, not by the
tests we need. 141 benchmark files link a near-full workspace each; the 95
ENGINE-CRITICAL files are a different and much smaller link problem. The fix is
to stop treating `basin-integration-tests` as one indivisible unit.

Recommended, in order of preference:

1. **Split the package.** Extract the ENGINE-CRITICAL + STORAGE targets into a
   new `basin-conformance-tests` package with a minimal dependency set
   (`basin-engine`, `basin-catalog`, `basin-storage`, `basin-common` — not
   `basin-rest`, `basin-realtime`, `basin-auth`, `basin-blob`, `axum`, …). Run
   that package in CI; leave `basin-integration-tests` excluded. This attacks the
   root cause — per-binary link size — rather than the symptom.
2. **Shrink the link.** The repo already reaches for per-package profile tricks;
   apply the same to the test package:
   ```toml
   [profile.test.package.basin-conformance-tests]
   debug = 0
   strip = true
   ```
   plus `[profile.test] debug = 0` and, on Linux CI, `-C link-arg=-fuse-ld=lld`
   or `mold`, which cut peak linker RSS substantially. Combined with `codegen-units`
   defaults this is usually enough to clear a 7GB runner.
3. **One test binary instead of 383.** Cargo links one binary per file in
   `tests/`. Consolidating the conformance targets behind a single
   `tests/conformance/main.rs` with `mod` declarations collapses 95 links into 1.
   This is the single largest lever and is compatible with (1).
4. **Bigger runner** for a nightly full-suite job. Use this for the BENCHMARK
   bucket only; it is a cost decision, not an engineering one, and it should not
   be the PR gate.

**Do not skip this step.** Every other recommendation in this document is
worthless if the result is a test suite that still does not run.

### 5.2 Extend the differential oracle to cover §4 (blocking)

`differential_pg.rs` already has the right shape. The work is:

- Add one test per gap G1–G16, using the SQL in §4, via `run_assert_match` and
  `run_assert_both_error`.
- **First**, verify the normalisation (§3.1). If the comparison is text-based
  and type-blind, add a type-aware mode — for the engine swap, a `Float64` where
  PG returns `Numeric` is a real defect that a text compare will hide.
- Make the oracle **mandatory in CI**, not silently skipped. A PG 16 service
  container in the workflow, with `PG_DIFF_TEST_DSN` set, and a hard failure if
  the DSN is absent when a `CI=true` env var is present. A conformance gate that
  no-ops when misconfigured is not a gate.
- Fixture discipline: every gap fixture must contain NULLs, and where relevant
  NaN, -0.0, empty strings, and boundary integers. G1 exists as a test today and
  fails to catch anything purely because its fixture has no NULLs.

### 5.3 Build the two-engine differential runner (the actual safety net)

Because DataFusion is deleted last, both engines can coexist for the whole
migration. This is the highest-leverage asset available and should be built
early.

**Shape:** a runner that takes a SQL string, executes it against a session built
on the DataFusion path and a session built on the new path, and asserts the
result sets are identical — same values, same types, same row order under
`ORDER BY`, same error SQLSTATE.

**Precedent to copy, not invent:** `hottier_differential.rs`,
`gin_row_tier_differential.rs`, and `wasm_functions_differential.rs` already do
in-process two-path diffing against Basin's own session API. Read
`hottier_differential.rs` (1,342 LOC) first and follow its structure.

**Selector:** the engine choice must be a runtime switch, not a cargo feature,
so one test binary can hold both. The repo already uses process-global env
selectors for exactly this pattern — `sql_support_matrix.rs` toggles
`BASIN_PG_QUERY` / `BASIN_PG_QUERY_PLAN` across three passes in a single test.
Follow that convention (e.g. `BASIN_ENGINE=df|native`), and note the same
constraint it documents: env vars are process-wide, so the passes run serially.

**Corpus:** feed it, in this order —
1. The ~1,500 SQL fragments already enumerated in `sql_support_matrix.rs`. They
   are written, curated, and currently prove only that SQL *runs*. Pointing the
   two-engine differ at the same list upgrades all of them from acceptance
   checks to conformance checks at near-zero authoring cost. **This is the
   single cheapest large win in this document.**
2. The §4 gap SQL.
3. The extracted SQL from the ENGINE-CRITICAL bucket.
4. A generator for randomised shapes (see 5.4).

**Three-way mode:** where PG is available, diff all three (DataFusion, new
engine, PG). Where the two Basin engines agree but PG differs, that is a
pre-existing Basin divergence and must be recorded as accepted, not fixed
mid-migration — otherwise the migration absorbs unrelated compat work.

### 5.4 Revive sqllogictest, and add a shape generator

One `.slt` file is not infrastructure. `sqllogictest` is the standard format for
exactly this problem, has a mature Rust runner, and — critically — files are
data, not code, so they add no link cost to the CI problem in 5.1. Every gap in
§4 can be expressed as an `.slt` file. Recommend: adopt `.slt` as the authoring
format for new conformance cases, and drive it from the two-engine runner in 5.3
so each file is checked against both engines.

A randomised query generator (SQLancer-style, or a small in-house shape
generator over a fixed schema with NULL-heavy fixtures) is the only realistic way
to cover the combinatorial space of join × NULL × type × frame. It is a Phase 1
item, not Phase 0, but the two-engine runner in 5.3 is its prerequisite, so build
5.3 with a programmatic entry point rather than a macro-only API.

### 5.5 What not to do

- Do not treat `sql_support_matrix.rs` passing as evidence of conformance. It is
  an acceptance classifier.
- Do not port the BENCHMARK bucket into the gate. Perf regressions are a
  separate, later conversation; blocking the migration on timing cards on shared
  CI runners will produce flakes and erode trust in the gate.
- Do not delete `regression_engine_bugs.rs` bug #40 when the DataFusion arrow
  conversion goes away. Retarget it.
- Do not fix Basin-vs-PG divergences discovered by the oracle during the
  migration. Record them, pin them as known-divergence tests, and fix them after.

---

## 6. Open questions

1. **UNVERIFIED** — the exact normalisation in `differential_pg.rs` (row order,
   float tolerance, type blindness). Blocks sizing 5.2.
2. **UNVERIFIED** — whether the PG side of the oracle is behind a trait. Blocks
   sizing 5.3's three-way mode.
3. **UNVERIFIED** — the true live pass count of `differential_pg.rs` (23 per the
   README sample vs 79 attributes present, 0 of them `#[ignore]`d).
4. **UNVERIFIED** — per-file classification for the ~150 files not individually
   inspected; ENGINE-CRITICAL is a lower bound.
5. **UNVERIFIED** — G17's blast radius. Run the no-`ORDER BY` assertion audit.
6. How much of `basin-engine`'s 1,843 unit tests are DataFusion-API-shaped (they
   test `LogicalPlan`/`Expr` construction) versus behaviour-shaped? The former
   die with DataFusion and are not migration assets. `pg_operators.rs` (153
   tests), `executor.rs` (114), `dml_mutate.rs` (82) are the files to triage
   first. See [02 — logical IR surface](02-logical-ir-surface.md).
