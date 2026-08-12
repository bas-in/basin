---
title: "DF removal — the 28-entry differential baseline, root-caused"
nav_section: migration
sidebar_position: 16
summary: "Root-causes all 28 remaining differential-test divergences against live PostgreSQL 18.2 into ten clusters. The honest split: about half trace to DataFusion's own behaviour, but most of that half is a Basin-side naming shim that is already proven cheap to extend (7131b8b5 fixed thirteen near-identical cases in one function) — only two or three clusters are a genuine DataFusion ceiling. Roughly 40% are Basin bugs with nothing to do with DataFusion at all: a wire-encoding fallback, a flat multi-schema catalog, a wrong SQLSTATE constant, a textual-rewrite heuristic that mangles its own input."
tags: [migration, query-engine, conformance, postgres-fidelity, differential-testing]
---

# 16 — The 28-entry differential baseline, root-caused

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map, and the document `tests/integration/differential-baseline.txt`
points at directly. That file's own header states the goal: "EVERY LINE HERE
IS A BUG... the gate is not to make them permanent." This document is the
per-line diagnosis it promises.

## Method

`differential_pg` (`tests/integration/tests/differential_pg.rs`) runs 79
identical SQL statements against an in-process Basin server and a real
PostgreSQL instance and compares results cell-by-cell. The harness's
comparison rules (from the file's own doc comment) determine the
`DivergenceKind` reported on failure: `OneErrored` (one side errors, the
other succeeds), `DifferentSqlstate` (both error, codes differ),
`RowCountMismatch`, `SchemaMismatch` (column names differ, case-insensitive),
`CellMismatch` (a value differs beyond float/timestamp/JSONB tolerance), or
`TagMismatch` (CommandComplete tag differs).

**Reproduction:**

```sh
createdb -U postgres basin_diff   # if not already present
PG_DIFF_TEST_DSN="postgres://postgres@127.0.0.1:5432/basin_diff" \
  cargo test -p basin-integration-tests --test differential_pg --release
```

Run once, on branch `feat/own-engine-remove-datafusion`, against **PostgreSQL
18.2** (Homebrew, local, already running), captured **2026-08-12**. Result:
`51 passed; 28 failed` — the exact 28 names in
`tests/integration/differential-baseline.txt`, no more, no fewer. Full output
saved and inspected; every panic message quoted below is copied verbatim from
that run, not reconstructed from memory or source reading.

A previous fix, commit `7131b8b5` ("engine: report Postgres column names, not
DataFusion's — 13 divergences fixed"), took the count from 41 to 28 by adding
`crates/basin-engine/src/pg_colnames.rs`: a renaming shim that intercepts
`DataFusion`'s `Expr::display_name`-derived schema at the single choke point
(`convert.rs`) where every DataFusion schema becomes Basin's wire schema, and
overrides it with Postgres's actual naming rule. That fix is important
context for several clusters below: it proves this entire *class* of bug is
fixable **without removing DataFusion**, at the cost of covering more plan
shapes in one function — which matters for how honestly clusters here can be
attributed to "DataFusion's fault" versus "Basin hasn't finished a shim it
already wrote."

## Per-entry results

| # | Test | SQL shape | Divergence | Cluster |
|---|---|---|---|---|
| 1 | `diff_advisory_blocking_lock_uncontended` | `SELECT pg_advisory_lock(31337)` | `CellMismatch`: basin `"Null@0"`, pg `""` | [C2](#c2-null0--void-values-serialized-as-literal-text-3-tests) |
| 2 | `diff_advisory_xact_lock_releases_on_commit` | `SELECT pg_advisory_xact_lock(246810)` | `CellMismatch`: basin `"Null@0"`, pg `""` | [C2](#c2-null0--void-values-serialized-as-literal-text-3-tests) |
| 3 | `diff_agg_filter_plain` | `SELECT SUM(x) FILTER (WHERE x > 0) FROM t` | `SchemaMismatch`: basin `sum(CASE WHEN t.x > Int64(0) THEN t.x END)`, pg `sum` | [C1a](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 4 | `diff_array_containment_operators` | `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]` | `SchemaMismatch`: basin `list_has_all`, pg `?column?` | [C1c](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 5 | `diff_cast_int8_to_int4_overflow` | `SELECT 2147483648::int8::int4` | `DifferentSqlstate`: basin `XX000`, pg `22003` | [C5](#c5-arrow-errors-surface-as-generic-xx000-2-tests) |
| 6 | `diff_cast_matrix_basic` | `SELECT 42::int4::text` (first of 6 sub-cases) | `OneErrored`: basin errors `"Unsupported SQL type vector_to_text(int4)"` | [C9](#c9-vector_to_text-rewrite-mangles-typetext-double-casts-1-test) |
| 7 | `diff_cross_schema_qualified_insert` | Setup: `CREATE TABLE {schema_b}.t (id INT)` after `{schema_a}.t` already created | Setup failure: `catalog error: table .../public.t already exists` | [C3](#c3-multi-schema-catalog-cascade-117-125-4-tests) |
| 8 | `diff_cross_schema_update_only_target_schema` | Same setup shape as #7 | Setup failure: same `public.t already exists` | [C3](#c3-multi-schema-catalog-cascade-117-125-4-tests) |
| 9 | `diff_drop_schema_with_tables_restrict_errors` | `DROP SCHEMA {schema}` with a table still inside | Basin succeeded; PG errors `2BP01` | [C3](#c3-multi-schema-catalog-cascade-117-125-4-tests) |
| 10 | `diff_error_division_by_zero` | `SELECT 1 / 0` | `DifferentSqlstate`: basin `XX000` ("Arrow error: Divide by zero error"), pg `22012` | [C5](#c5-arrow-errors-surface-as-generic-xx000-2-tests) |
| 11 | `diff_error_not_null_violation` | `INSERT INTO t VALUES (NULL)` (NOT NULL column) | `DifferentSqlstate`: basin `42601` (syntax_error), pg `23502` (not_null_violation) | [C7](#c7-not-null-violation-tagged-with-the-wrong-sqlstate-1-test) |
| 12 | `diff_jsonb_array_elements_literal_srf` | `SELECT jsonb_array_elements('[10,20,30]'::jsonb)` | `SchemaMismatch`: basin `value`, pg `jsonb_array_elements` | [C10](#c10-139-jsonb-set-returning-functions-2-tests-two-different-maturity-levels) |
| 13 | `diff_jsonb_arrow_op` | `SELECT data -> 'a' FROM t ORDER BY id` | `SchemaMismatch`: basin `json_get(t.data,Utf8("a"))`, pg `?column?` | [C1b](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 14 | `diff_jsonb_double_arrow_op` | `SELECT data ->> 'name' FROM t ORDER BY id` | `SchemaMismatch`: basin `json_get_text(t.data,Utf8("name"))`, pg `?column?` | [C1b](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 15 | `diff_jsonb_each_srf` | `SELECT key, value FROM t, jsonb_each(data) ORDER BY key` | `OneErrored`: basin errors `"jsonb_each: argument must be a JSON literal"` | [C10](#c10-139-jsonb-set-returning-functions-2-tests-two-different-maturity-levels) |
| 16 | `diff_jsonb_path_op` | `SELECT data #> '{a,b}' FROM t ORDER BY id` | `SchemaMismatch`: basin `json_path_extract(t.data,Utf8("{a,b}"))`, pg `?column?` | [C1b](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 17 | `diff_lateral_basic` | `SELECT t.id, sub.uid FROM t1 t, LATERAL (SELECT id AS uid FROM t2 WHERE t_id = t.id ORDER BY id LIMIT 1) sub` | `OneErrored`: basin errors `"Physical plan does not support logical expression OuterReferenceColumn(...)"` | [C4a](#c4-lateral-correlated-references-113-3-tests) |
| 18 | `diff_lateral_generate_series` | `SELECT t.id, gs.i FROM t, LATERAL generate_series(1, t.n) AS gs(i)` | `OneErrored`: basin errors `"relation \"t\" does not exist"` | [C4b](#c4-lateral-correlated-references-113-3-tests) |
| 19 | `diff_lateral_left_join` | `SELECT t.id, sub.val FROM t LEFT JOIN LATERAL (SELECT val FROM t2 WHERE t_id = t.id ORDER BY id LIMIT 1) sub ON true` | `OneErrored`: same `OuterReferenceColumn` error as #17 | [C4a](#c4-lateral-correlated-references-113-3-tests) |
| 20 | `diff_sanity_select_null` | `SELECT NULL` | `CellMismatch`: basin `"Null@0"`, pg `NULL` (true SQL NULL) | [C2](#c2-null0--void-values-serialized-as-literal-text-3-tests) |
| 21 | `diff_schema_isolation_multi_schema` | Setup: two schemas, both with a table `t` | Setup failure: `public.t already exists` | [C3](#c3-multi-schema-catalog-cascade-117-125-4-tests) |
| 22 | `diff_serial_nextval_currval` | `SELECT nextval('seq')` | `SchemaMismatch`: basin `?column?`, pg `nextval` | [C1d](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 23 | `diff_serial_setval` | `SELECT nextval('seq')` (after `setval`) | `SchemaMismatch`: basin `?column?`, pg `nextval` | [C1d](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 24 | `diff_str_format_and_position` | `SELECT position('ll' IN 'hello')` | `SchemaMismatch`: basin `strpos`, pg `position` | [C1c](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 25 | `diff_txn_rollback` | `SELECT COUNT(*) FROM t` | `SchemaMismatch`: basin `count(*)`, pg `count` | [C1a](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |
| 26 | `diff_type_date_interval_arithmetic` | `SELECT '2024-01-15'::date + INTERVAL '10 days'` | `CellMismatch`: basin `"2024-01-25"`, pg `"2024-01-25 00:00:00"` | [C8](#c8-date--interval-does-not-type-promote-to-timestamp-1-test) |
| 27 | `diff_type_text_to_int_implicit_cast_errors` | `SELECT 'abc' + 1` | `DifferentSqlstate`: basin `XX000` ("Cannot coerce arithmetic expression Utf8 + Int64"), pg `22P02` | [C6](#c6-no-implicit-text-to-int-coercion-1-test) |
| 28 | `diff_window_percentile_disc` | `SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM t` | `SchemaMismatch`: basin `percentile_disc(Float64(0.5)) WITHIN GROUP [t.x ASC NULLS LAST]`, pg `percentile_disc` | [C1a](#c1-column-naming-still-leaking-through-despite-7131b8b5-10-tests) |

## Clusters

### C1: Column naming still leaking through, despite 7131b8b5 (10 tests)

The single biggest cluster, and the same *class* of bug 7131b8b5 already
fixed 13 instances of. `pg_colnames.rs` intercepts DataFusion's schema at one
choke point and renames columns per Postgres's rule — but its coverage is
incomplete in three distinct, separately-diagnosable ways. This is worth
stating plainly: **the mechanism is DataFusion's naming convention, but the
gap is Basin's, and it is demonstrably closeable without touching
DataFusion** — that is exactly what the earlier 13-test fix proved.

**C1a — the renamer never runs at all when `Aggregate` is the top plan node
(3 tests: `agg_filter_plain`, `txn_rollback`, `window_percentile_disc`).**
`pg_colnames::top_projection_exprs` walks down through pass-through plan
nodes to find the `Projection` that defines column names:

```rust
fn top_projection_exprs(plan: &LogicalPlan) -> Option<Vec<Expr>> {
    match plan {
        LogicalPlan::Projection(p) => Some(p.expr.clone()),
        LogicalPlan::Sort(s) => top_projection_exprs(&s.input),
        LogicalPlan::Limit(l) => top_projection_exprs(&l.input),
        LogicalPlan::Distinct(d) => top_projection_exprs(d.input()),
        LogicalPlan::SubqueryAlias(s) => top_projection_exprs(&s.input),
        _ => None,
    }
}
```
(`crates/basin-engine/src/pg_colnames.rs:101-114`)

There is no `LogicalPlan::Aggregate` arm. For a bare `SELECT agg(...) FROM t`
with no `GROUP BY` and nothing else, DataFusion can plan straight to
`Aggregate` with no separate `Projection` wrapping it — the match falls to
`_ => None`, `pg_style_column_names` returns the schema **unchanged**
(`crates/basin-engine/src/pg_colnames.rs:69-73`), and DataFusion's raw,
argument-inclusive `Expr::schema_name()` reaches the wire verbatim. This
predicts exactly the observed shape for all three tests: the basin column
name is the *entire* aggregate expression text (`sum(CASE WHEN ... END)`,
`count(*)`, `percentile_disc(Float64(0.5)) WITHIN GROUP [...]`), not a
partially-wrong name — consistent with the renamer never touching the
schema at all, rather than mis-renaming it.

**C1b — `ORDER BY` forces an extra `Projection`, and the outer projection's
`Expr::Column` pass-through trusts a synthetic inner name literally (3 tests:
`jsonb_arrow_op`, `jsonb_double_arrow_op`, `jsonb_path_op`).** All three of
these — and only these, among the JSONB operator tests — have `ORDER BY id`
in their SQL. `pg_expr_display_name` has:

```rust
Expr::Column(c) => Some(c.name.clone()),
```
(`crates/basin-engine/src/pg_colnames.rs:191`)

correctly trusts a `Column` reference's name for genuine table columns. The
hypothesis — **UNVERIFIED** by direct plan inspection, but consistent with
every piece of evidence gathered (the ORDER BY correlation is exact across
all six JSONB-operator/matrix tests, and the leaked names are DataFusion's
full argument-inclusive `Expr::schema_name()`, matching the sort-forces-a-
second-Projection pattern DataFusion commonly uses so the sort key survives
past the user-visible projection) — is that the plan is
`Projection[json_get(...)] -> Sort[id] -> Projection[json_get(...), id] ->
TableScan`, and the *outer* projection's item is a `Column` reference into
the *inner* projection's already-materialized field, whose name is
DataFusion's own synthesized full-text name. `pg_expr_display_name` has no
way to tell that column name is synthetic rather than a real user column,
so it passes it through unchanged. Worth confirming by dumping the actual
`LogicalPlan` before treating this as settled, but it is the only hypothesis
consistent with both the ORDER BY correlation and the exact shape of the
leaked text.

**C1c — an operator is textually pre-rewritten into a function call before
DataFusion ever parses it, and the renamer correctly extracts a function
name — just the wrong one (2 tests: `array_containment_operators`,
`str_format_and_position`).** Confirmed at the source level for the `<@`
case:

```rust
// pg_operators.rs:978-981
"@>" => format!("list_has_all({lhs}, {rhs})"),
"<@" => format!("list_has_all({rhs}, {lhs})"),
```

`ARRAY[1,2] <@ ARRAY[1,2,3]` is rewritten to `list_has_all(...)` as SQL
**text**, before DataFusion's parser sees it — by the time `pg_colnames`
looks at the `Expr` tree, it is indistinguishable from a user typing
`list_has_all(...)` directly. `pg_expr_display_name`'s
`Expr::ScalarFunction(f) => Some(f.func.name().to_string())` arm fires
correctly and returns `"list_has_all"` — a real, reduced function name, not
a leaked full expression — but Postgres's actual rule for the `<@`
*operator* (as opposed to an explicit function call) is `?column?`.
`position('ll' IN 'hello')` is the same mechanism with a different
rewrite target: Basin implements `POSITION(a IN b)` by calling its own
`strpos`-named function, while Postgres's parser resolves that syntax to a
function that Postgres itself calls `position` — the two engines chose
different **internal names** for the same syntax form, and the renamer,
working correctly, reports Basin's.

**C1d — `nextval`/`setval` fall to the generic `?column?` default, exact
mechanism UNVERIFIED (2 tests: `serial_nextval_currval`, `serial_setval`).**
This is the mirror image of the other three: Postgres expects `nextval` as
the column name, and Basin produces the *generic fallback* `?column?`
instead — meaning `pg_expr_display_name` hit its `_ => None` arm for
whatever expression shape `nextval('seq')` plans to. Plausible explanation:
`nextval` has session-scoped side effects that a plain stateless
`ScalarFunction` can't cleanly express, so Basin likely routes it through a
different `Expr` variant (a scalar subquery or similar) that isn't in
`pg_expr_display_name`'s match arms. **Not traced to the exact plan node —
mark UNVERIFIED** — but it is the same class of bug as C1a/C1b (a plan
shape the renamer doesn't cover), not a new mechanism.

### C2: `Null@0` — void values serialized as literal text (3 tests)

`diff_sanity_select_null`, `diff_advisory_blocking_lock_uncontended`,
`diff_advisory_xact_lock_releases_on_commit`. All three involve a value
that is conceptually SQL `NULL` (a bare `NULL` literal, or a `void`-returning
function — the advisory-lock UDFs' own doc comment says "PG
`pg_advisory_lock` returns void; Basin surfaces void as NULL",
`crates/basin-engine/src/advisory_lock.rs:1008`) landing on the wire as the
literal text string `"Null@0"` instead of a true protocol NULL.

Root cause, confirmed at the source: `basin-router`'s text-cell renderer has
a Debug-format catch-all fallback for any `DataType` it does not explicitly
handle:

```rust
// crates/basin-router/src/types.rs:1204-1279 (render_cell)
DataType::FixedSizeList(_, _) => { ... }
// Fallback: best-effort Debug rendering.
other => format!("{other:?}@{idx}"),
```

Arrow's `DataType::Null` is not one of the explicitly-matched arms
(`Boolean`, `Int*`, `Float*`, `Utf8`, `Binary`, `Timestamp`, `Date32`,
`Interval`, `Decimal128`, `List`/`LargeList`/`FixedSizeList`) — it falls to
this catch-all, and `{other:?}` on the unit-like `DataType::Null` variant
Debug-prints as the literal string `"Null"`, giving `"Null@0"` for row index
0. This matches the observed value byte-for-byte. The general encode path
(`encode_value`, `types.rs:1010-1160`) *does* check `col.is_null(idx)` first
and would correctly emit a wire NULL — so the open question, **UNVERIFIED**,
is why that guard does not catch these three cases (a `NullArray`'s
`is_null` should report `true` for every index in-bounds). The most likely
explanation is that these values are constructed by a different path that
does not set up the validity bitmap the same way a genuine `NullArray` scan
result would, but this was not traced to the exact construction site.
Regardless of that remaining gap, the fallback line itself
(`crates/basin-router/src/types.rs:1278`) is real, reachable, and squarely
Basin's own pgwire encoding code — nothing here is DataFusion's behaviour.

### C3: Multi-schema catalog cascade (#117-125) (4 tests)

`diff_cross_schema_qualified_insert`, `diff_cross_schema_update_only_target_schema`,
`diff_schema_isolation_multi_schema`, `diff_drop_schema_with_tables_restrict_errors`.
Already self-diagnosed in the test file's own comments, which this run
confirms accurately:

> "Basin's flat catalog stores a.t and b.t under the same unqualified key
> 't', so inserts may go to the wrong table." (`differential_pg.rs:2637-2639`)

The observed failures are consistent with exactly that: `CREATE TABLE
{schema_b}.t (id INT)` fails during test **setup** (before any assertion
even runs) with `catalog error: table {project}/public.t already exists` —
the schema qualifier `{schema_b}` is dropped somewhere in table creation and
both schemas' `t` collide under a single `public.t` catalog key. The fourth
test (`DROP SCHEMA ... RESTRICT` with a dependent table) is a related but
distinct symptom of the same underlying gap: Postgres tracks
schema-to-object dependency and refuses the drop (`2BP01`); Basin has no
such dependency tracking and the drop silently succeeds. This is a real
catalog data-model gap — a flat namespace where Postgres's is
`(schema, table)`-keyed — and has nothing to do with DataFusion; it is
Basin's own `basin-catalog` crate. Already tracked as issue #117-125 (one
comment says #116-125), i.e. this is known, scoped work, not a fresh
discovery.

### C4: LATERAL correlated references (#113) (3 tests)

All three carry the same tracked issue number in their doc comments, but
split into two distinct failure mechanisms:

**C4a — DataFusion's own physical planner has no implementation for this
shape (2 tests: `diff_lateral_basic`, `diff_lateral_left_join`).** Both hit
the identical error:

```
This feature is not implemented: Physical plan does not support logical
expression OuterReferenceColumn(Field { name: "id", ... }, Column { ... })
```

This message is raised from inside DataFusion's own physical-plan code, not
from any Basin code path — DataFusion's decorrelation genuinely has no
physical operator for an outer-referenced column surviving into this plan
shape. [05](./05-optimizer-rules.md) already documents the adjacent finding
that DataFusion's `decorrelate_lateral_join` "bails" when a correlated
predicate can't be pulled through a `Sort`/`Limit` — both failing queries
here have exactly that shape (`... ORDER BY id LIMIT 1) sub`). Basin's own
`rewrite_lateral_order_limit` textual rewrite (documented in
[05](./05-optimizer-rules.md)) routes *around* this exact DataFusion gap for
the published 462× LATERAL benchmark, but only for the scalar-subquery
shape that benchmark uses — it does not cover `, LATERAL (...) sub` or
`LEFT JOIN LATERAL ... ON true` in `FROM`-clause position, which is what
these two tests exercise. So: the underlying limitation is genuinely
DataFusion's; the fact that it is *unfixed for this specific SQL shape* is
that Basin has not yet extended a rewrite pattern it already owns and has
already used successfully elsewhere.

**C4b — a Basin-owned textual rewrite fails to resolve its own output (1
test: `diff_lateral_generate_series`).** Different error entirely:
`relation "t" does not exist` — a catalog-resolution failure, not a
DataFusion "not implemented" message. This points at Basin's own
`rewrite_lateral_generate_series` (`pg_operators.rs`, per
[05](./05-optimizer-rules.md)'s inventory of the six LATERAL textual
rewrites) mishandling the `AS gs(i)` column-aliasing form and producing SQL
that no longer resolves the `t` alias correctly. **Not traced line-by-line
— mark UNVERIFIED** on the exact rewrite defect, but the error class (table/
alias resolution, not "feature not implemented") makes DataFusion an
unlikely culprit here: this looks like Basin's own string-surgery rewrite,
the same fragility [05](./05-optimizer-rules.md) already flagged generally
("currently string surgery guarded by `find_keyword_at_depth0` /
`find_matching_close_paren` heuristics").

### C5: Arrow errors surface as generic `XX000` (2 tests)

`diff_cast_int8_to_int4_overflow`, `diff_error_division_by_zero`. Both are
runtime errors that Arrow's kernels genuinely raise — Basin is not silently
wrong, it detects the problem — but the *SQLSTATE* Basin returns is the
generic internal-error code `XX000` in both cases, where Postgres has a
specific one:

```
error=internal: execute: Optimizer rule 'simplify_expressions' failed
caused by
Arrow error: Cast error: Can't cast value 2147483648 to type Int32
```
→ basin `XX000`, pg `22003` (`numeric_value_out_of_range`)

```
error=internal: execute: Arrow error: Divide by zero error
```
→ basin `XX000`, pg `22012` (`division_by_zero`)

Both are Arrow's own error type propagating through Basin's generic
error-mapping path with no per-kind translation. This is a real DataFusion/
Arrow-semantics artifact (Arrow's cast and arithmetic kernels raise their
own error taxonomy, not Postgres's SQLSTATE space), but it is not a ceiling
— it is missing plumbing. A translation layer that pattern-matches specific
`ArrowError` variants at the query-execution boundary and remaps them to the
matching SQLSTATE would close both without touching DataFusion itself.

### C6: No implicit text-to-int coercion (1 test)

`diff_type_text_to_int_implicit_cast_errors`: `SELECT 'abc' + 1`. Basin
errors at **plan time** with `XX000` ("Cannot coerce arithmetic expression
Utf8 + Int64 to valid types") — DataFusion's analyzer refuses to plan
`text + int` at all. Postgres instead implicitly casts the text operand to
`int4` and fails at **runtime**, when the cast itself can't parse `"abc"`,
with `22P02` (`invalid_text_representation`). This is a genuine semantic
difference in the two engines' type-coercion rules, not a missing
translation: DataFusion's analyzer requires explicit casts for
cross-type arithmetic where Postgres attempts an implicit one and defers the
error to the value. Closing this gap on top of DataFusion means either
building a pre-pass that duplicates Postgres's implicit-cast table for
arithmetic generally (real, ongoing duplication-of-effort risk — the more
of Postgres's coercion table gets re-implemented as a Basin pre-pass on top
of DataFusion's own coercion analyzer, the closer that pre-pass gets to
being a second type system) or patching DataFusion's analyzer directly. Of
everything in this document, this is one of the clearer examples supporting
ADR 0030's argument.

### C7: NOT NULL violation tagged with the wrong SQLSTATE (1 test)

`diff_error_not_null_violation`: `INSERT INTO t VALUES (NULL)` into a NOT
NULL column. Basin's own constraint check **correctly detects** the
violation — the log line reads `invalid schema: NULL inserted into NOT
NULL column id`, proving the check fires — but tags it with SQLSTATE
`42601` (`syntax_error`) where Postgres uses `23502`
(`not_null_violation`). This is Basin's own DML-validation code using the
wrong SQLSTATE constant for a condition it already catches correctly.
Nothing to do with DataFusion.

### C8: `DATE + INTERVAL` does not type-promote to `TIMESTAMP` (1 test)

`diff_type_date_interval_arithmetic`: `'2024-01-15'::date + INTERVAL '10
days'`. Postgres returns a `timestamp` (`"2024-01-25 00:00:00"`) because an
`INTERVAL` may carry a time-of-day component in general, so `DATE +
INTERVAL` must be able to represent one even when this particular interval
doesn't; Basin returns a bare date-formatted string (`"2024-01-25"`),
implying its `Date32 + Interval` arithmetic keeps the `Date32` type rather
than promoting to `Timestamp`. This is a DataFusion/Arrow type-arithmetic
semantics difference from Postgres's promotion rule — real, and the kind of
thing that's cheap to work around with a Basin-side rewrite (cast the
`DATE` operand to `TIMESTAMP` before the addition) but is a difference that
originates in DataFusion's own arithmetic kernel, not Basin's code.

### C9: `vector_to_text` rewrite mangles `TYPE::text` double-casts (1 test)

`diff_cast_matrix_basic` fails on its very **first** assertion,
`SELECT 42::int4::text`, with:

```
plan: This feature is not implemented: Unsupported SQL type vector_to_text(int4)
```

Traced to source. `crates/basin-engine/src/pg_operators.rs:6663-6742`
(`rewrite_vector_col_text_cast`) exists to support pgvector-style
`embedding::text` by rewriting `IDENT::text` to `vector_to_text(IDENT)`
before DataFusion parses the SQL — because Arrow's cast kernel can't cast
`FixedSizeList<Float32>` (a vector column) to `Utf8` directly. Its
identifier-detection logic scans **backward** from `::text` for a
contiguous `[a-zA-Z0-9_]` run:

```rust
// pg_operators.rs:6694-6708
// Look back to find the identifier that precedes `::text`.
let out_bytes = out.as_bytes();
let ident_end = out_bytes.len();
let mut id_start = ident_end;
while id_start > 0 {
    let b = out_bytes[id_start - 1];
    if b.is_ascii_alphanumeric() || b == b'_' { id_start -= 1; } else { break; }
}
```

For `42::int4::text`, the text immediately preceding `::text` is
`int4` — the **type name** from the prior `::int4` cast, not a column
identifier — but the scan can't tell the difference: `int4` is a
contiguous alphanumeric run starting with a letter, so it passes the
`is_ident` check and is not one of the excluded keyword literals
(`NULL`/`TRUE`/`FALSE`/`UNKNOWN`). The rewriter wraps it, producing
`42::vector_to_text(int4)` — a syntactically nonsensical cast target that
sqlparser rejects as an unsupported type. This is a scope bug in a
Basin-authored textual pre-rewrite: it has no way to know that `int4` here
is itself the target of a preceding `::` cast rather than a bare column
reference. Entirely independent of DataFusion — the bug is in the SQL text
Basin hands to DataFusion, not in anything DataFusion does with it.

### C10: #139 — JSONB set-returning functions (2 tests, two different maturity levels)

Both tests carry the same tracked issue (#139) in their comments, but are
at very different points of completion:

- `diff_jsonb_array_elements_literal_srf` (`SELECT jsonb_array_elements(...)`
  over a JSON **literal**): row expansion already works correctly — the
  only divergence is `SchemaMismatch` (basin names the output column
  `value`, Postgres names it `jsonb_array_elements`). This is nearly done;
  it needs the same kind of naming fix as C1, not new functionality.
- `diff_jsonb_each_srf` (`SELECT key, value FROM t, jsonb_each(data)`, a
  JSON **column reference**): genuinely unimplemented. The test file's own
  comment is explicit: *"basin's jsonb_each is a scalar stub returning a
  single text value, while PG returns N rows (one per JSON object key)"*
  (`differential_pg.rs:920-923`), and the observed error confirms it:
  `"jsonb_each: argument must be a JSON literal"` — the implementation
  only accepts a literal, not a table column. This is Basin's own
  incomplete implementation choice; it has nothing to do with DataFusion's
  architecture (real N-row set-returning-function support over a column
  argument is buildable on top of DataFusion, it just hasn't been built).

## The honest split

Classifying each of the 28 individually (not by cluster, since C1, C4, and
C10 mix causes within themselves):

| Class | Count | Tests |
|---|---|---|
| **DataFusion's own semantics leaking through** | **14** | agg_filter_plain, array_containment_operators, jsonb_arrow_op, jsonb_double_arrow_op, jsonb_path_op, str_format_and_position, txn_rollback, window_percentile_disc, serial_nextval_currval, serial_setval (naming, 10) + cast_int8_to_int4_overflow, error_division_by_zero (unmapped SQLSTATE, 2) + type_date_interval_arithmetic, type_text_to_int_implicit_cast_errors (type/coercion semantics, 2) |
| **Basin bug, independent of DataFusion** | **11** | advisory_blocking_lock_uncontended, advisory_xact_lock_releases_on_commit, sanity_select_null (Null@0, 3) + cross_schema_qualified_insert, cross_schema_update_only_target_schema, drop_schema_with_tables_restrict_errors, schema_isolation_multi_schema (catalog, 4) + error_not_null_violation (wrong SQLSTATE, 1) + cast_matrix_basic (rewrite bug, 1) + lateral_generate_series (own rewrite gap, 1) + jsonb_array_elements_literal_srf (naming on an otherwise-working feature, 1) |
| **Genuinely unimplemented feature** | **3** | jsonb_each_srf (Basin's own scalar stub) + lateral_basic, lateral_left_join (root-caused to DataFusion's physical-planner gap) |

**14 / 28 (50%) trace in some way to DataFusion's own behaviour. 11 / 28
(39%) are Basin's own bugs with nothing to do with DataFusion. 3 / 28 (11%)
are unimplemented features, two of which trace back to a DataFusion gap and
one of which is Basin's own stub.**

The number worth sitting with, though, is not that 50/39 split — it's how
much of the "DataFusion" half is actually load-bearing for ADR 0030's
argument. Of the 14 DataFusion-attributed tests, **10 are naming (C1)** and
**2 are unmapped SQLSTATEs (C5)** — both classes 7131b8b5 already proved
fixable with a Basin-side shim at a single choke point, no DataFusion code
touched, no engine removed. Only **4 of the 28** — the two `LATERAL`
physical-planner gaps (C4a) and the two type/coercion-semantics differences
(C8, C6) — are genuinely hard to close *without* either patching
DataFusion's own analyzer/physical-planner or building the equivalent of
that logic as a duplicate layer on top of it. **Those 4 are the honest
evidence for ADR 0030's fidelity-ceiling argument in this baseline; the
other 24 are Basin engineering debt, most of it (11 of 24) not about
DataFusion at all.** If the question is "does this baseline justify
removing DataFusion," the answer from this data is: not on its own — most
of what's here is fixable on top of DataFusion, cheaply, in the same way
the last 13 were. The 4 harder cases are real, but they are a much smaller
slice than "28 divergences" suggests at a glance.

## Ranked by (tests fixed / effort)

Effort tiers are qualitative (LOW / MEDIUM / HIGH), not LOC estimates —
none of this was scoped to the level of [05](./05-optimizer-rules.md)'s
ablation. Ranked descending by fix-location confidence and yield.

| Rank | Cluster | Tests fixed | Effort | Confidence in fix location | Notes |
|---|---|---|---|---|---|
| 1 | C2 — `Null@0` wire encoding | 3 | LOW | High (exact line identified: `types.rs:1278`) | Mechanism for the fallback text is confirmed; why `is_null()` doesn't short-circuit first is UNVERIFIED but the fix is local either way |
| 2 | C1a — Aggregate-bypasses-Projection naming | 3 | LOW–MEDIUM | High (`pg_colnames.rs:101-114`, add an `Aggregate` arm) | Same shim, same file as the already-successful 7131b8b5 fix |
| 3 | C7 — wrong SQLSTATE for NOT NULL | 1 | TRIVIAL | High | Best per-test ratio in the set; only fixes one test but costs almost nothing |
| 4 | C9 — `vector_to_text` rewrite scope bug | 1 | LOW | High (`pg_operators.rs:6694-6708`) | Needs the backward-scan to recognise a preceding `::TYPE` and exclude it |
| 5 | C1c — operator→internal-UDF-name naming | 2 | MEDIUM | High | Needs a per-rewrite-target override table (`list_has_all`→`?column?` when it came from `<@`/`@>`; `strpos`→`position` when it came from `POSITION(...IN...)`), not a generic rule |
| 6 | C1b — ORDER BY double-projection naming | 3 | MEDIUM | Medium (mechanism inferred, not plan-traced) | Needs confirming the plan shape before scoping the fix |
| 7 | C5 — Arrow error → SQLSTATE mapping | 2 | MEDIUM | Medium | Needs an `ArrowError`-variant → SQLSTATE table at the execution-error boundary; two known variants today, likely more exist |
| 8 | C1d — nextval/setval naming | 2 | UNVERIFIED (likely LOW–MEDIUM) | Low (exact plan shape not traced) | Probably the same class of fix as C1a/C1b once traced |
| 9 | C8 — DATE+INTERVAL promotion | 1 | LOW–MEDIUM | Medium | Rewrite DATE operand to TIMESTAMP before arithmetic, or post-process the result type |
| 10 | C3 — multi-schema catalog cascade | 4 | HIGH | Medium (root cause is clear; fix is a data-model change) | `basin-catalog` needs `(schema, table)` keys, not flat `table`; also needs DROP SCHEMA dependency tracking. Already tracked as #117-125 — a real multi-issue cascade, not a quick patch |
| 11 | C4 — LATERAL correlated references | 3 | HIGH (2 of 3), MEDIUM (1 of 3) | Medium | `lateral_generate_series` (1 test) may be a contained rewrite-heuristic fix; `lateral_basic`/`lateral_left_join` (2 tests) need either a DataFusion physical-planner patch or a new Basin-owned rewrite pattern extending what [05](./05-optimizer-rules.md) already documents for the benchmark shape |
| 12 | C10 (jsonb_each half) — real SRF over a column | 1 | HIGH | Medium | Needs genuine table-function/lateral-unnest support, not a naming fix; the literal-argument half of C10 is already covered under C1's naming fixes |
| 13 | C6 — implicit text→int coercion | 1 | MEDIUM–HIGH | Low | Risks scope creep if generalized past this one shape; the honest fix duplicates part of Postgres's coercion table |

**Recommended order given this ranking:** C2 and C1a first — six tests,
both LOW/LOW-MEDIUM effort, both in files already proven to take this kind
of patch cleanly. Then C7 and C9 (one test each, but both trivial to
contained). C1c next (two tests, one override table). That is 10 of the 28
tests closed before touching anything that requires a real design decision
(catalog schema-keying, LATERAL physical execution, or a coercion-semantics
duplication of Postgres's own rules).

## What could not be determined

Marked inline above with **UNVERIFIED**, collected here:

- **C2**: the exact reason `encode_value`'s `col.is_null(idx)` guard
  (`types.rs:1018`) does not short-circuit before reaching the
  `render_cell` Debug fallback for these three values. The fallback
  mechanism itself is confirmed; the upstream cause (how a `NULL`/`void`
  scalar's Arrow array ends up not reporting `is_null() == true`) is not
  traced to its construction site.
- **C1b**: the exact `LogicalPlan` shape for the three `ORDER BY`-plus-
  JSONB-operator queries was not dumped and inspected directly — the
  double-`Projection` hypothesis is inferred from the code and from an
  exact correlation with which tests have `ORDER BY`, not measured.
- **C1d**: `nextval`/`setval`'s exact `Expr` shape (why it hits
  `pg_expr_display_name`'s `_ => None` arm) was not traced.
- **C4b**: the specific defect in `rewrite_lateral_generate_series` that
  produces `relation "t" does not exist` was not traced line-by-line; only
  the error class (resolution failure, not "not implemented") was used to
  attribute it to Basin's own rewrite rather than DataFusion.
- **General**: no fix proposed in this document has been implemented or
  tested. All effort tiers are qualitative judgment calls based on how
  precisely the cause was traced, not on writing and measuring a patch.
  Per the task instructions, no Rust source was edited to produce this
  document — it is diagnosis only.
