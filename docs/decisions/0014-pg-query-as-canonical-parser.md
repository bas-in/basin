# 0014 — libpg_query as canonical SQL parser (retire sqlparser-rs)

- **Status:** Accepted, in progress (Phase 1 in flight 2026-05-14)
- **Tags:** architecture, sql-parser, pg-compat, technical-debt

## Context

Basin currently parses SQL with `sqlparser-rs` 0.52 (PostgreSQL dialect). The
dialect is materially incomplete: fourteen textual pre-screens sit at the top
of `crates/basin-engine/src/executor.rs` (lines ~42–171) that redirect
statements before sqlparser ever sees them. The pre-screens exist because
sqlparser cannot model the following forms at all:

- `ALTER TYPE … ADD VALUE` (no `AlterType` AST node)
- `CREATE DOMAIN` / `DROP DOMAIN` (not recognised by sqlparser's CREATE/DROP
  parser)
- `REFRESH MATERIALIZED VIEW` (no AST node)
- `CREATE PROCEDURE … LANGUAGE sql AS $$ … $$` (only parses T-SQL `AS BEGIN
  … END`)
- Basin-specific `ALTER TABLE` extensions (`SET cold_after`, `SET BLOOM
  FILTERS ON`, etc.)
- `DROP MATERIALIZED VIEW`, `DROP REACTOR`, various `ALTER TABLE` reactor +
  webhook arms

In addition, sqlparser accepts or mis-parses several standard PG forms that
end up surfacing as confusing error messages at execution time (subquery in
WHERE, `ON CONFLICT … DO UPDATE`, CTEs with side-effecting arms). The textual
pre-screens are increasingly brittle — they depend on exact whitespace and
capitalisation norms that real drivers don't guarantee — and they represent a
hard ceiling on PG-compat: every new feature that involves non-standard SQL
syntax must either add another pre-screen or wait for a sqlparser upstream
fix.

`pg_query` is a Rust crate that vendored `libpg_query`, which is a
distillation of PostgreSQL's own parser (`gram.y` + `scan.l`) into a
standalone C library. It is the same parser that `psql`, `pg_dump`, every
PostgreSQL-derived system, and (notably) DuckDB, CockroachDB, Spanner-PG,
and YugabyteDB all use as their parse foundation. It returns a complete,
protobuf-serialised parse tree for every SQL statement Postgres 16 accepts.

## Decision

Adopt libpg_query (via `pg_query = "6"` in the workspace) as the canonical
SQL frontend for Basin, migrating in three phases.

### Phase 1 — Statement classification + rejection (in flight, 2026-05-14)

`pg_query` parses every incoming statement first. The parse tree drives
dispatch for the fourteen textual pre-screens in `executor.rs`: each
pre-screen is replaced by an AST match against the parse tree that
`pg_ast::stmt_kind` returns. Additionally, a `reject_unsupported` guard
rejects the following "known-but-not-yet-shipped" statement kinds with
SQLSTATE 0A000 (`feature_not_supported`) before sqlparser or any handler
sees them:

`LISTEN`, `NOTIFY`, `PREPARE`, `DECLARE CURSOR`, `LOCK`, `VACUUM`,
`CLUSTER`, `ANALYZE`, `CREATE EXTENSION`, `CREATE TRIGGER`,
`BEGIN`/`COMMIT`/`ROLLBACK`.

sqlparser stays as a **transitional fallback** for the SELECT/DML/DDL node
bodies it already handles. The gate `BASIN_PG_QUERY` env-var enables the
Phase 1 path; Agent 5 flips it unconditional once Agents 2–4 complete the
pre-screen migrations.

Foundation lives in `crates/basin-engine/src/pg_ast.rs`:
- `parse(sql) -> Result<ParseTree>` — wraps `pg_query::parse`; maps parse
  errors to `BasinError::InvalidSchema` (SQLSTATE 42601).
- `ParseTree` — newtype over `pg_query::protobuf::ParseResult`; exposes
  `stmts()` as an iterator over top-level `Node` references.
- `stmt_kind(node) -> StmtKind` — coarse classification; returns `Other`
  for anything not yet enumerated.
- `StmtKind` enum — exhaustive list of statement kinds Basin cares about.
- `reject_unsupported(tree) -> Result<()>` — rejects any kind in
  `StmtKind::is_unsupported()` with `BasinError::FeatureNotSupported`.

### Phase 2 — PgNode → DataFusion LogicalPlan (Q3 2026)

Own a translator that converts the pg_query protobuf AST for SELECT
(including subqueries, CTEs, window functions, LATERAL) into a DataFusion
`LogicalPlan` directly. DataFusion is demoted from "SQL frontend" to
"logical-plan executor and optimiser." sqlparser is removed from the
executor's hot path entirely.

### Phase 3 — Long-tail SQL features (Q4 2026+)

Fill the remaining PG-compat gaps using the pg_query AST as the authoritative
source: `GROUPING SETS`, `ROLLUP`, `CUBE`, recursive CTEs, `MERGE`, lateral
joins, `TABLESAMPLE`. Each feature becomes a translator pass, not a textual
pre-screen.

## Alternatives considered

**Stay on sqlparser-rs and patch/upgrade.**
sqlparser 0.53–0.55 closes some of the gaps (AlterType is now modelled;
CREATE DOMAIN partially works) but the lag between PG releases and sqlparser
merges is structural, not accidental. The project is community-maintained and
Basin's needs are niche. Maintaining Basin-specific patches is equivalent
maintenance load to owning the translator, without the compat ceiling being
lifted. Closes maybe half the pre-screens at best.

**pg_query parse, then re-emit SQL for DataFusion.**
Parse with pg_query, normalise, stringify back to SQL, hand to DataFusion's
own SQL parser. This is double-parsing and the re-emitted SQL is still
DataFusion-flavoured (not PG-flavoured), so DataFusion still rejects
PG-specific constructs. Silly.

**Fork PostgreSQL (CockroachDB / YugabyteDB path).**
Full PG compatibility at the cost of permanently tracking PG source releases.
~10+ engineers to maintain. Not viable for a small team; not necessary for
Basin's wedge (new multi-tenant SaaS, not legacy PG migrations).

## Consequences

**Positive**

- 100% PG syntax acceptance at parse time the moment Phase 1 lands —
  libpg_query accepts everything PG 16 accepts.
- The fourteen textual pre-screens in `executor.rs` are replaced by typed
  AST matches; brittle regex-style matchers go away permanently.
- `reject_unsupported` gives every client a clean SQLSTATE 0A000 error with
  a human-readable message instead of a confusing sqlparser failure or a
  silent wrong-path execution.
- AST-level dispatch consolidates RLS injection, function inlining, sequence
  rewrite, and generated-column substitution into explicit AST passes rather
  than a mix of textual pre-screens + sqlparser-AST mangling.
- Owning the parser surface makes Phase 2/3 features (window functions, CTEs,
  MERGE, LATERAL) a matter of extending the translator, not negotiating with
  upstream parser maintainers.

**Negative / risks**

- C dependency (vendored libpg_query, ~4 MB static library): acceptable for
  a PG-compat database; same trade DuckDB made. Cross-compilation requires
  a C toolchain. CI time for the first compile is ~60s, cached afterwards.
- `pg_catalog` UDF gap surfaces next: `psql \dt` probes
  `pg_table_is_visible`, `pg_get_userbyid`, `pg_get_function_arguments` —
  these are scalar functions, not syntax. Deferred to Phase 2.5 (alongside
  `information_schema` expansion). Workaround documented in `CAPABILITIES.md`.
- Phase 1 runs `pg_query::parse` on every statement even when the
  `BASIN_PG_QUERY` gate is on; the cost is ~10–50 µs per statement on a
  modern core — negligible at Basin's target QPS.

## Precedent

| System | Parser strategy |
|---|---|
| **DuckDB** | Vendored libpg_query; wrote their own `PgNode → LogicalPlan` translator. Most relevant analogue — OLAP + embeddable + PG-compat. |
| **CockroachDB** | Forked PG's parser (`gram.y`) and transpiled to Go. |
| **Spanner-PG** | Built a PG-query-to-Spanner translator layer. |
| **YugabyteDB** | Forked PG outright; tracks PG minor releases. |

Basin's Phase 2 translator follows DuckDB's path most closely — libpg_query
as parse frontend, own the AST-to-plan translation, avoid forking PG.

## References

- [`TASK.md`](../../TASK.md) — Phase 5 / WEDGE item 4: pg_query migration
- [`WEDGE.md`](../../WEDGE.md) — item 4: "pg_query migration (ADR 0014)"
- [`CAPABILITIES.md`](../../CAPABILITIES.md) — SQL surface table; "Parser:
  libpg_query" row tracks Phase 1 status
- [`crates/basin-engine/src/pg_ast.rs`](../../crates/basin-engine/src/pg_ast.rs)
  — Phase 1 foundation: `parse`, `ParseTree`, `stmt_kind`, `StmtKind`,
  `reject_unsupported`
- [`crates/basin-engine/src/executor.rs`](../../crates/basin-engine/src/executor.rs)
  — 14 textual pre-screens that Phase 1 replaces
