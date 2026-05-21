---
title: "3-way pg-compatibility differential — Basin vs Neon vs Supabase"
nav_section: operations
sidebar_position: 60
summary: "How to run the OSS 3-way SQL-compatibility differential test and read its JSON output."
tags: [operations, testing, compat, pgwire]
---

# 3-way pg-compatibility differential

A standing OSS regression: anyone with a Basin endpoint, a Neon endpoint, and
a Supabase endpoint can fire the same corpus of ~60 representative SQL
shapes at all three and capture a JSON report of where Basin's
pg-compatibility lines up with the two leading Postgres-as-a-service
incumbents — and where it diverges.

The test lives at:

```
tests/integration/tests/3way_pg_compat.rs
```

The output lives at:

```
benchmark/data/3way_pg_compat.json
```

---

## What it tests

The corpus covers, per category:

| Category | Examples |
|---|---|
| `basic_select` | `SELECT 1::int`, literal casts, `pg_typeof(now())`, array/jsonb/uuid/timestamp literals |
| `dml_types` | CREATE + INSERT + SELECT round-trips for `int`, `text`, `jsonb`, `timestamptz`, `uuid`, `numeric`, `int[]` |
| `update_delete` | `UPDATE … SET`, `DELETE … WHERE`, `UPDATE … RETURNING` |
| `window_fn` | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, running `SUM` |
| `cte` | Simple CTE, multi-CTE, recursive CTE (`WITH RECURSIVE`) |
| `join` | INNER, LEFT, RIGHT, FULL, CROSS |
| `subquery` | EXISTS, IN, NOT IN, correlated subquery |
| `aggregate` | GROUP BY, COUNT DISTINCT, FILTER, HAVING, AVG/NUMERIC |
| `ddl` | CREATE/DROP, ALTER ADD/DROP COLUMN, CREATE INDEX, TRUNCATE |
| `rls` | ENABLE ROW LEVEL SECURITY, CREATE POLICY |
| `pg_catalog` | `version()`, `current_schema()`, `current_user`, `SHOW search_path`, `pg_class`/`pg_namespace`, the `psql \d` shape, `information_schema.tables` |
| `expressions` | Concat, `LENGTH`, `SUBSTRING`, `UPPER`/`LOWER`, math, jsonb `->`/`->>`/`#>`, `ARRAY_LENGTH`, `@>` |
| `datetime` | `EXTRACT`, `INTERVAL` arithmetic, `AGE` |

Total: ~60 shapes. The list is in `build_corpus(ns)` in the test file —
add to it freely; the JSON output schema is stable so cross-run diffs
will only show new rows.

---

## How to run

### Skipped by default

Without the three env vars set, the test prints a skip message and exits
clean. CI never accidentally fires real traffic at Neon or Supabase.

```sh
cargo test -p basin-integration-tests --test 3way_pg_compat -- --nocapture
# → skip: set BASIN_3WAY_*_URL for 3-way compat run
```

### Full 3-way run

You need three reachable libpq DSNs. Use any region; the test does not
assume Frankfurt or any specific provisioning.

```sh
BASIN_3WAY_BASIN_URL='postgres://USER:PASS@HOST:5432/db?sslmode=disable' \
BASIN_3WAY_NEON_URL='postgres://USER:PASS@ep-xxx.NEON.tech/neondb?sslmode=require' \
BASIN_3WAY_SUPABASE_URL='postgres://postgres.[ref]:PASS@aws-0-REGION.pooler.supabase.com:6543/postgres?sslmode=require' \
  cargo test -p basin-integration-tests --test 3way_pg_compat -- --nocapture
```

Per-shape console line shape:

```
[MATCH] select_one_int                  basin=ok  neon=ok  supa=ok
[PART ] rls_enable_no_policy            basin=ok  neon=ok  supa=ok
[DIFF ] catalog_psql_d_shape            basin=err neon=ok  supa=ok
```

| Tag | Meaning |
|---|---|
| `MATCH` | Basin's rendered value matches BOTH Neon and Supabase (cell-for-cell, including both-errored) |
| `PART`  | Basin matches one of the two incumbents but not the other |
| `DIFF`  | Basin matches neither incumbent (Neon and Supabase may still agree) |

### What the test asserts

**Only that no panic occurred.** The test does NOT fail on divergence —
divergence is the *data*, captured in JSON. If you want a CI gate, write
a separate script that reads `3way_pg_compat.json` and asserts e.g.
`summary.basin_diverges_from_neon <= K`.

---

## JSON output schema

```json
{
  "run_id": "<uuid>",
  "timestamp": "2026-05-21T10:00:00+00:00",
  "namespace": "basin_3way_<uuid_simple>",
  "shapes": [
    {
      "name": "select_one_int",
      "category": "basic_select",
      "sql": "SELECT 1::int",
      "basin_ok": true,
      "neon_ok": true,
      "supabase_ok": true,
      "basin_value": "[[\"1\"]]",
      "neon_value":  "[[\"1\"]]",
      "supabase_value": "[[\"1\"]]",
      "basin_error": null,
      "neon_error":  null,
      "supabase_error": null,
      "matches_neon": true,
      "matches_supabase": true,
      "neon_matches_supabase": true,
      "all_three_match": true
    }
  ],
  "summary": {
    "total_shapes": 60,
    "basin_ok": 58,
    "neon_ok": 60,
    "supabase_ok": 60,
    "all_three_match": 54,
    "basin_diverges_from_neon": 4,
    "basin_diverges_from_supabase": 4,
    "neon_diverges_from_supabase": 2
  }
}
```

### Value rendering

Every cell is rendered as the string returned by the pgwire **text protocol**
(via `tokio_postgres::SimpleQueryMessage::Row::get`). The grid is then
JSON-encoded as a 2-D string array. This makes the value byte-comparable
across runs and across vendors with one caveat: vendors that render the same
logical value with different *text* (e.g. `"1.5"` vs `"1.50"` for NUMERIC)
will diverge. We accept that — it IS a compatibility difference.

NULL cells render as the bare string `"NULL"`. Empty result sets render as
`"[]"`.

### Match rule

Two probe results "match" when:
- Both succeeded AND their rendered grids are byte-equal, OR
- Both failed (error message text does NOT need to match — different
  vendors emit different prose for the same SQLSTATE).

---

## Reading the report

### "I just want to know how Basin compares"

```sh
jq '.summary' benchmark/data/3way_pg_compat.json
```

### "Show me only the shapes where Basin diverges from BOTH incumbents"

```sh
jq '.shapes[] | select(.matches_neon == false and .matches_supabase == false) | {name, basin_value, neon_value, supabase_value}' \
  benchmark/data/3way_pg_compat.json
```

### "Show me shapes where Basin matched one but not the other"

```sh
jq '.shapes[] | select(.matches_neon != .matches_supabase) | {name, matches_neon, matches_supabase}' \
  benchmark/data/3way_pg_compat.json
```

### Cross-run diff

The output schema is stable. Save the report from a baseline run, then
diff against a later run with `jq` or any structural-diff tool:

```sh
diff <(jq '.shapes[] | {name, all_three_match}' baseline.json) \
     <(jq '.shapes[] | {name, all_three_match}' new.json)
```

---

## Getting the three endpoints

### Basin Cloud
Either spin up a local Basin engine (see `docs/operators/dev-stack.md`) and
use its pgwire endpoint, or use a provisioned Basin Cloud project's DSN.

### Neon
[neon.tech](https://neon.tech) — the free tier is enough. Copy the
connection string from the dashboard; it already includes
`sslmode=require&channel_binding=require`.

### Supabase
[supabase.com](https://supabase.com) — the free tier is enough. Use the
**Session-pooler** URI (`aws-0-REGION.pooler.supabase.com:6543`), not the
transaction-pooler, because the test relies on multi-statement scripts
(DDL + DML + SELECT) within a single simple-query round-trip.

---

## Safety

- The test **does not write to any shared schema**. Every shape is
  namespaced under a fresh per-run schema `basin_3way_<uuid>`, created at
  the start and `DROP SCHEMA … CASCADE`'d on the way out (best-effort).
- The test **does not connect** unless all three env vars are non-empty.
- Connection attempts have a **15-second timeout**, so a misconfigured
  DSN cannot hang CI.
- The test **never panics on a divergence** — only on a harness error
  (e.g. failure to write the JSON output, which means the local
  filesystem is broken).

---

## Adding a new shape

Edit `build_corpus(ns)` in `tests/integration/tests/3way_pg_compat.rs`.
Each `Shape` is `(name, category, sql)`. The `ns` placeholder gives you
the per-run schema name so multiple parallel runs can never collide:

```rust
s.push(Shape::new(
    "my_new_shape",
    "my_category",
    format!(
        "DROP TABLE IF EXISTS {ns}.t_new;
         CREATE TABLE {ns}.t_new (id INT);
         INSERT INTO {ns}.t_new VALUES (1), (2);
         SELECT id FROM {ns}.t_new ORDER BY id"
    ),
));
```

The `corpus_is_non_empty` compile-time check guards the floor at 50 shapes.

---

## Related

- `tests/integration/tests/differential_pg.rs` — 2-way (Basin vs PG) with
  strict assertion. The 3-way is the *informational* sibling.
- `benchmark/three_way/run_three_way.sh` — performance-only 3-way (no
  correctness, just timings).
