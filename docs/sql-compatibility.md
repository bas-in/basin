# SQL / ORM compatibility — survey

Output of `tests/integration/tests/orm_smoke.rs`. The test drives seven
representative query patterns that real ORMs (Diesel, SeaORM, Prisma,
SQLAlchemy, ActiveRecord) emit through the Postgres extended-query
protocol against a fresh `basin-server`. The numbers are protocol-level:
if a pattern works here, the chance every ORM that funnels through
`tokio-postgres` / `asyncpg` / `pgjdbc` works is high.

This file is **survey-grade**. The agent's job was to find gaps, not
patch them. Each failure has a one-line "next step" so a follow-up agent
can pick the work up.

Result snapshot (run 2026-04-30): **5 / 7 patterns pass** = 71.4 %, below
the 85 % bar set on the dashboard's `orm_compat` card.

| # | Pattern | ORM analogue | Result | Notes |
|---|---|---|---|---|
| 1 | `INSERT … VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)` | Diesel `insert_into(t).values(&vec)`, ActiveRecord `insert_all` | PASS | Multi-row INSERT with parameter placeholders works end-to-end. |
| 2 | `client.prepare(sql)` + 100× `client.query(&stmt, &[…])` | Any pooled prepared-statement cache (sqlx, asyncpg, HikariCP) | PASS | Cached statement handle survives 100 Bind/Execute/Sync cycles with varying parameters. |
| 3 | `WHERE id = $1 AND name = $2 AND active = $3` (i64, &str, bool) | ActiveRecord `where(...)`, Prisma `findMany({where:{...}})` | PASS | Mixed-type predicate inference works for INT8, TEXT, BOOL. |
| 4 | `INSERT … VALUES ($1, $2)` with `Option::<&str>::None`, then assert NULL on read | Diesel `Option<String>`, Prisma optional fields | PASS | NULL parameters round-trip cleanly. |
| 5 | `SELECT … LIMIT $1` with `&2i64` | Diesel `.limit(?)`, Prisma `take(n)`, SeaORM `limit(n)` | **FAIL** | See below. |
| 6 | `INSERT … VALUES ($1, $2, $3)` with `"O'Brien"` | Any text-bound input | PASS | Single-quote in TEXT parameter survives — the binary protocol path bypasses textual substitution. |
| 7 | `CREATE TABLE blobs (… data BYTEA NOT NULL)` + `INSERT/SELECT` `Vec<u8>` | Diesel `Vec<u8>`, Prisma `Bytes` | **FAIL** | See below. |

## Failure 1 — `LIMIT $1`

Verbatim driver error:

```
error serializing parameter 0: cannot convert between the Rust type `i64`
and the Postgres type `text`
```

The error fires **client-side** in `tokio-postgres`. Basin's `Describe`
response for `SELECT … LIMIT $1` types the placeholder as `text` (the
default when our parameter-type inference can't resolve it). The driver
sees the param-type list, refuses to encode `i64` as `text`, and never
sends the `Bind`. The server log is silent — the query never reaches it.

Why it happens: the inference pass in `basin-engine` (see `prepared.rs`)
covers `INSERT VALUES` and `WHERE col OP $N`, but does **not** cover
`LIMIT $N` / `OFFSET $N`.

**Next step:** small, contained fix in the placeholder-typing pass —
when the placeholder is the direct child of a `LIMIT` or `OFFSET` node,
type it as `int8`. Likely <30 LoC. ORM-blocker for Diesel and SeaORM
pagination.

## Failure 2 — `BYTEA` columns

Verbatim server error (returned to the driver as `db error` and visible
in the router log):

```
ERROR 42601: invalid schema: unsupported column type in PoC: BYTEA
```

`CREATE TABLE blobs (id BIGINT NOT NULL, data BYTEA NOT NULL)` is
rejected at DDL parse time. The engine's column-type table doesn't list
BYTEA as a creatable type — even though the protocol layer can decode
BYTEA *parameters* and encode BYTEA *result values*. The two halves
landed independently.

**Next step:** wire BYTEA through `basin-engine`'s DDL parser to map to
Arrow `BinaryArray` (`DataType::Binary` or `LargeBinary`). Likely also
needs a `LiteralValue::Bytea` rendering for `INSERT VALUES` literals
and a path through DataFusion's `binary_expressions`. Slightly larger
than the LIMIT fix — probably a half-day's work.

Defer? **No.** Almost every modern Prisma / Drizzle schema has at least
one `Bytes` column (file uploads, hashed passwords, JWT signatures).
Without BYTEA the migration step in any non-trivial app fails, before
the application ever runs a query.

## Surprising finding — single-quote in text parameters works

Pattern 6 (`"O'Brien"`) was included specifically because Basin's
`prepared.rs` substitutes parameters textually for paths that don't
flow through DataFusion's binary-row APIs. The pattern passes, so
either we're already going through a binary path for the round-trip, or
the substitution helper escapes single quotes correctly. Either way,
no work needed here — but worth keeping the test as a regression guard
because it would silently break under a future refactor.

## Verdict

The protocol surface is solid: prepared-statement reuse, multi-row
binds, mixed-type WHERE, and NULL all work. The two failures are both
in **the type / schema layer**, not in the protocol layer:

1. Parameter-type inference is missing the `LIMIT`/`OFFSET` case.
2. DDL-side type list is missing BYTEA.

Both are independently fixable in <1 day each, and either alone is
enough to block any real ORM running its bootstrap migration. Once they
land, the same survey should jump to 7 / 7. At that point the same
ORM-compat scoreboard can be expanded to include UPDATE / DELETE,
TIMESTAMPTZ, and JSONB — each of which is its own issue.

## How to re-run

```bash
cargo test -p basin-integration-tests --test orm_smoke -- --nocapture
```

The test seeds a fresh `TempDir` per run and binds an ephemeral port,
so it's safe to run in parallel with other integration tests. The
machine-readable result lands at
`dashboard/data/viability_orm_compat.json`.
