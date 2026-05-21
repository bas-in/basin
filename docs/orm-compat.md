# ORM Compatibility — Basin pgwire coverage

This document summarises which SQL shapes emitted by popular ORMs Basin handles
correctly, which return a typed error (known unsupported feature), and which are
flagged for future work.

Source of truth: `tests/integration/tests/orm_compat.rs` — the `orm_compat_corpus`
test runs on every CI pass and writes a machine-readable report to
`benchmark/data/orm_compat.json`.

## Summary (as of last test run)

| ORM     | Priority | Shapes | OK  | Typed-Err | Regression | OK-Rate |
|---------|----------|--------|-----|-----------|------------|---------|
| Drizzle | HIGH     | 14     | 12  | 2         | 0          | 86 %    |
| Prisma  | HIGH     | 11     | 5   | 6         | 0          | 45 %    |
| sqlx    | MEDIUM   | 8      | 5   | 3         | 0          | 62 %    |
| Diesel  | MEDIUM   | 8      | 8   | 0         | 0          | 100 %   |
| TypeORM | FUTURE   | 6      | 3   | 3         | 0          | 50 %    |
| **Total** |        | **47** | **33** | **14** | **0**    | **70 %** |

**Regression = 0** across all ORMs. This is the critical invariant: no shape
causes a server panic or garbled wire response.

---

## Drizzle ORM (TypeScript)

Drizzle emits double-quoted identifiers and `$N` positional parameters. Shapes
without bound parameters execute end-to-end.

### Works

- `SELECT ... WHERE col = 'literal' LIMIT 1`
- `INSERT ... RETURNING id` (literal values)
- `UPDATE ... SET ... WHERE ... RETURNING *`
- Correlated scalar subquery `(SELECT count(*) FROM orders WHERE ...) AS col`
- `ORDER BY ... DESC LIMIT n OFFSET m`
- `INNER JOIN ... ON ... WHERE status = 'literal'`
- `WHERE id IN (1, 2)` (literal list)
- `COUNT(*) AS "count"`
- `INSERT ... ON CONFLICT DO NOTHING`
- `WHERE name IS NULL`
- `WHERE total BETWEEN 10.0 AND 100.0`
- Aliased subquery in FROM with GROUP BY

### Typed error (expected)

- `WHERE email = $1 LIMIT 1` — `$1` unbound via simple_query; the full
  extended-protocol path (Prepare/Bind/Execute) works correctly and is covered
  by `prepared_statement_reuse_rebind` in the same test file.
- `DELETE ... WHERE id = ANY($1::int[])` — `ANY($1::int[])` requires parameter
  binding; returns `XX000` from the planner.

---

## Prisma (Node)

Prisma is the most demanding ORM because it probes `pg_catalog` and
`information_schema` during schema introspection. Several catalog functions
are not yet wired up in Basin.

### Works

- `SELECT ... FROM "User" WHERE "id" = 1 LIMIT 1` (findUnique)
- `SELECT ... FROM "User"` (findMany, no predicates)
- `SELECT count(*) FROM "Order"`
- `CREATE TABLE IF NOT EXISTS` with camelCase columns
- `INSERT ... ON CONFLICT ("id") DO UPDATE SET ... RETURNING`
- `SELECT "userId", count(*) ... GROUP BY "userId" ORDER BY "userId"`
- `DELETE ... WHERE "id" = 999 RETURNING "id"`

### Typed error (expected / known unsupported)

- `SELECT column_name, data_type FROM information_schema.columns WHERE ...`  
  Basin's `information_schema` implementation is partial; this probe returns a
  typed error rather than crashing.
- `SELECT json_agg(o.*) FROM "Order" o WHERE ...` (correlated json_agg)  
  Correlated `json_agg` over a row wildcard `.*` is not yet fully supported.
  Tracked internally.
- `SELECT pg_get_serial_sequence('"User"', 'id')`  
  Catalog function not wired up. Returns `XX000`. Prisma uses this to detect
  auto-increment sequences; Basin uses a different mechanism internally.
- `SELECT relname, relkind FROM pg_class WHERE relname = 'User' AND relkind = 'r'`  
  `pg_class` is not fully exposed. Returns `XX000`.

---

## sqlx (Rust)

sqlx generates clean, well-structured SQL. Most shapes work; gaps are limited
to unsupported protocol extensions.

### Works

- `INSERT ... VALUES (literal, ...) RETURNING id, email`
- `UPDATE ... SET name = 'literal' WHERE id = literal`
- `DELETE ... WHERE id = literal`
- `SHOW server_version` (Basin returns its version string)
- `BEGIN` (noop-accepted; Basin is auto-commit)

### Typed error (expected / known unsupported)

- `SELECT ... WHERE id = $1` — unbound `$1` via simple_query; correct
  via the extended-protocol path.
- `SELECT ... WHERE email = $1 AND id > $2` — same as above.
- `LISTEN mychannel` — **known unsupported**: Basin does not implement the
  NOTIFY fanout channel. Returns `0A000 (feature_not_supported)`. This is
  intentional; LISTEN/NOTIFY requires stateful async subscription infrastructure.

### Out of scope (not tested)

- `COPY FROM STDIN` / `COPY TO STDOUT` — bulk-loader sub-protocol not
  implemented. Returns a typed error.

---

## Diesel (Rust)

Diesel generates unquoted snake_case SQL from a compile-time schema. Basin
handles all Diesel shapes tested, including `pg_catalog.pg_tables` probes used
by the migration system.

### Works (all 8 shapes)

- `SELECT col, ... FROM table WHERE id = literal`
- `INSERT ... VALUES (...) RETURNING id, title`
- `UPDATE ... SET ... WHERE ... RETURNING id, title`
- `DELETE ... WHERE id = literal`
- `INNER JOIN ... ON ... WHERE col = 'literal'`
- `SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'`
- `ORDER BY id ASC LIMIT 10 OFFSET 0`
- `SELECT count(*) FROM table WHERE col = literal`

Diesel is the best-covered ORM in this corpus (100 % of shapes execute without
error), partly because it avoids parameterised shapes in the simple_query corpus
and partly because its SQL style is close to Basin's primary test surface.

---

## TypeORM (Node) — FUTURE

TypeORM mixes snake_case and camelCase aliases and emits verbose LEFT JOIN
chains. Coverage is intentionally lighter pending validation of camelCase alias
round-trips and full extended-protocol parameterised bind coverage.

### Works

- `SELECT col AS "snake_case_alias" FROM "table" WHERE id = literal LIMIT 1`
- `LEFT JOIN ... ON ... WHERE status = 'literal'`
- `SELECT count("col") AS "alias" FROM "table"`

### Typed error (expected)

- `WHERE "id" = $1` — unbound param (same as other ORMs).
- `WHERE "id" IN ($1)` — parse error on `IN ($1)` with a single param and no
  trailing comma; returns `42601`. TypeORM normally emits this only for single-
  element arrays which it unrolls to `= $1` instead; this shape is a
  degenerate case.
- `INSERT ... VALUES (...) RETURNING "id"` in the same connection context as
  the above parse error — connection was in an aborted-transaction state from
  the `42601`, causing `25P02`. Isolated connection per ORM section would clean
  this up; tracked for a follow-up.

---

## What "Typed Error" means

A "typed error" outcome means Basin returned a well-formed PostgreSQL
`ErrorResponse` message with:
- `Severity` field non-empty (e.g. `"ERROR"`)
- `SQLSTATE` code exactly 5 characters
- `Message` field non-empty

This is always acceptable — drivers can parse and route these correctly. The
regression gate is **zero connection-level failures** (panics, garbled bytes,
connection drops), which Basin currently achieves for all 47 shapes.

---

## How to run

```sh
cargo test --test orm_compat -- orm_compat_corpus --nocapture
```

The test writes `benchmark/data/orm_compat.json` with per-shape detail on every
run, which the Basin dashboard picks up for trend visualisation.

## Raising the bar

To improve ORM coverage:

1. **Drizzle**: The `ANY($1::int[])` typed error requires array-type parameter
   binding support in the extended-protocol path.
2. **Prisma**: Wire up `json_agg` for correlated subqueries returning row
   wildcards (`.*`). Expose `pg_get_serial_sequence` and `information_schema`
   catalog views.
3. **sqlx**: LISTEN/NOTIFY is deferred until Basin implements async notification
   infrastructure (see roadmap).
4. **TypeORM**: Fix the `IN ($1)` parse edge case and isolate connection state
   per ORM section in the corpus runner.

When a shape moves from "typed error" to "ok", its count increments automatically
in the next test run and the JSON report reflects it.
