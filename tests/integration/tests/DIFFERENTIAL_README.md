# Differential PG Oracle Test Harness

`differential_pg.rs` runs identical SQL against both an in-process Basin
pgwire server **and** a real PostgreSQL instance, failing on any divergence.

Every existing Basin test compares against hand-coded expected outputs.  This
harness closes the verification gap: Basin **and** its expected outputs could
be wrong in the same direction.  The real Postgres instance is the
ground-truth oracle.

---

## Quick start (Docker PG)

```sh
# Start a local Postgres 16 instance.
docker run -d \
  --name pg-diff \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16

# Wait for it to be ready (~2 s).
until docker exec pg-diff pg_isready -U postgres 2>/dev/null; do sleep 1; done

# Run the differential suite.
PG_DIFF_TEST_DSN=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p basin-integration-tests --test differential_pg --release
```

Expected output when all non-ignored tests pass:

```
running 26 tests
test diff_agg_filter_window ... ignored, ...
test diff_jsonb_each_srf ... ignored, ...
test diff_schema_isolation_multi_schema ... ignored, ...
test diff_sanity_select_1 ... ok
...
test result: ok. 23 passed; 0 failed; 3 ignored; 0 measured; ...
```

---

## Skip behaviour

If `PG_DIFF_TEST_DSN` is **not set**, every test exits cleanly (exit 0) and
prints:

```
[differential] PG_DIFF_TEST_DSN not set — skipping differential tests
```

This means the suite is safe to include in CI without a PG sidecar; it simply
skips everything rather than failing or requiring conditional invocation.

---

## Architecture

### `DifferentialRunner`

Defined at the top of `differential_pg.rs`, wraps two `tokio-postgres::Client`
handles (one Basin, one PG) and exposes four methods:

| Method | Behaviour |
|---|---|
| `run_setup(sqls)` | Runs DDL/DML on both sides; panics on harness failure |
| `run_assert_match(sql)` | Runs SQL on both sides, compares cell-by-cell |
| `run_assert_both_error(sql, sqlstate)` | Asserts both sides error with the same (optional) SQLSTATE |
| `run_assert_both_ok(sql)` | Asserts both sides succeed (used for DDL) |

### `run_assert_match` comparison rules

1. **One side errors, other succeeds** → `DivergenceKind::OneErrored` (fail)
2. **Both error** → compare SQLSTATE codes; diverge if different
3. **Both succeed**:
   - Row counts must match
   - Column names must match (case-insensitive)
   - Each `(row, col)` cell:
     1. Exact string equality (text-protocol wire format covers most types)
     2. **JSONB**: parse both sides with `serde_json`, normalize by sorting
        object keys recursively, then compare — PG and Basin may serialize
        the same logical value with different key ordering
     3. **Float**: epsilon comparison: `|a − b| ≤ ε × max(|a|, |b|, 1.0)`
        with ε = 1e-9
     4. **Timestamp**: parse to microseconds via `chrono`, tolerance ≤ 1 µs
   - CommandComplete tag: prefix (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) +
     numeric count must match; `INSERT 0 N` and `INSERT N` are normalized

### Table isolation

Each test generates a UUID-prefixed table name (`diff_<12hex>_<purpose>`) via
`table_prefix()`.  Tests clean up with `DROP TABLE IF EXISTS` at the end (best
effort).  The prefixed naming ensures concurrent test runs don't interfere with
each other.

---

## Ignored tests (guards-in-waiting)

| Test | Gate | Unblocking fix |
|---|---|---|
| `diff_jsonb_each_srf` | `#[ignore = "gated on #139 jsonb SRF fix"]` | `jsonb_each` must return one row per key, not a scalar |
| `diff_agg_filter_window` | `#[ignore = "gated on #110"]` | `SUM(x) FILTER (WHERE …) OVER (PARTITION BY g)` diverges today |
| `diff_schema_isolation_multi_schema` | `#[ignore = "gated on #116"]` | Multi-schema `CREATE SCHEMA` / qualified table names |

When the gating fix lands, remove or adjust the `#[ignore]` attribute.  The
test will then run against real PG and must pass, confirming the fix is correct
end-to-end.

---

## 26 test cases

### Sanity (tests 1–5)
Tests 1–5 cover `SELECT 1`, multi-column projections, `NULL` literal,
`int4` arithmetic, and text concatenation — the pipeline floor.

### NULL semantics (tests 6–8)
- `IS NULL` vs `= NULL` three-valued logic
- `NOT IN (…, NULL)` returns 0 rows
- `COALESCE` and `NULLIF`

### Type coercion (tests 9–12)
- `INT4 + INT8` promotion
- `NUMERIC` precision
- `text + integer` (should be an error, SQLSTATE 42883)
- Date + INTERVAL arithmetic

### JSONB operators (tests 13–17)
- `->` object field extraction (jsonb)
- `->>` text extraction
- `@>` containment
- `#>` path extraction
- `jsonb_each()` SRF — **ignored** pending #139

### Window functions (tests 18–20)
- `SUM(x) OVER (ORDER BY id)` running sum
- `lag(x, 1) OVER (ORDER BY id)`
- `percentile_disc(0.5) WITHIN GROUP (ORDER BY x)`

### Aggregate FILTER (tests 21–22)
- Plain aggregate `FILTER (WHERE …)`
- Window `FILTER (WHERE …) OVER (PARTITION BY …)` — **ignored** pending #110

### Error SQLSTATE codes (tests 23–25)
- Division by zero → 22012
- Unique violation → 23505
- NOT NULL violation → 23502

### Schema isolation (test 26)
- Multi-schema qualified tables — **ignored** pending #116

---

## What is NOT yet covered (planned follow-up)

- **Extended-protocol (parameterized queries)**: `$1` binding — add tests
  using `tokio_postgres::Client::query()` rather than `simple_query()`
- **COPY FROM / COPY TO** divergence
- **Sequence / SERIAL** behaviour across both engines
- **CTE correctness** (`WITH RECURSIVE`, data-modifying CTEs)
- **Subquery shapes** (correlated, lateral, scalar)
- **String functions** (`regexp_match`, `split_part`, `format`, etc.)
- **Array operators** (`ANY`, `ALL`, `@>`)
- **Ordering edge cases** (`NULLS FIRST` / `NULLS LAST`)
- **Transaction semantics** (ROLLBACK, savepoints)
- **CAST matrix** (all PG type-cast combinations)
