---
title: "DF removal — pg_catalog fidelity"
nav_section: migration
sidebar_position: 11
summary: "Basin's catalog is more complete than expected — ~65 relations backed by real queries — but every scan materializes a whole relation and discards pushdown. The gaps that matter are pg_operator, pg_cast and pg_attrdef, and the first two are what the owned engine needs internally anyway."
tags: [migration, pg-compat, catalog, postgres]
---

# 11 — `pg_catalog` fidelity

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. Answers the goal of getting the schema layer "as close to real
Postgres as possible", with the one accepted exception of `.so`/dlopen
extension loading (ADR 0002).

## 1. What Basin exposes today

Better than the "faked through shims" framing suggested. Two mechanisms in
`crates/basin-engine/src/info_schema_provider.rs` (2,568 LOC):

**~20 hand-written providers** — `PgClassProvider` (:524),
`PgAttributeProvider` (:368), `PgNamespaceProvider` (:442), `PgTypeProvider`
(:1282), `PgProcProvider` (:592), `PgIndexProvider` (:727),
`PgConstraintProvider` (:793), `PgDependProvider` (:1348), `PgAuthidProvider`
(:1415), plus the `information_schema` core (`tables`, `columns`, `views`,
`schemata`, `table_constraints`, `key_column_usage`,
`referential_constraints`, `routines`) and two live views
(`pg_stat_activity`, `pg_locks`).

**~45 macro-generated** via `simple_provider!` (:1486) — `pg_database`,
`pg_roles`, `pg_tables`, `pg_indexes`, `pg_settings`, `pg_extension`,
`pg_description`, `pg_sequence`, `pg_locks`, the `pg_stat_*` family, and a
broad `information_schema` tail (`check_constraints`, `column_privileges`,
`sequences`, `triggers`, `parameters`, `user_defined_types`, the
`foreign_*` and `role_*` families).

**These are not stubs.** The macro body calls
`InfoSchemaQuery::$query_fn(catalog, project)` — real queries against
`basin-catalog`. Coverage is genuinely broad.

### The actual problem is the scan shape, not the content

```rust
async fn scan(&self, _state, projection, _filters: &[Expr], _limit: Option<usize>)
```

`_filters` and `_limit` are **discarded**. Every scan of every catalog relation:

1. queries `basin-catalog` for the *entire* relation,
2. materializes it into one Arrow `RecordBatch`,
3. hands it to `MemorySourceConfig` and lets the engine filter afterwards.

So `SELECT … FROM pg_attribute WHERE attrelid = 16384` builds every column of
every table in the project and then throws almost all of it away. Doc 06
confirms **none** of the ~89 providers implements `statistics()` and only three
implement `supports_filters_pushdown`.

That is survivable at toy scale and quadratic in practice: psql's `\d` issues
several catalog queries, each rebuilding whole relations, and ORM introspection
issues dozens. It is also why the comment at `info_schema_provider.rs:80` notes
DataFusion's `TableProvider` "can't see through the `Arc<dyn Catalog>`" — the
shim shape forces materialization because it cannot push a predicate down into
the catalog query.

**In the owned engine this whole mechanism disappears.** There is no
`TableProvider` to implement; a catalog relation becomes
`fn(catalog, project, pushed_predicates) -> RecordBatch`, and `attrelid = 16384`
becomes a parameter of the catalog query rather than a filter applied after it.

## 2. The gaps that matter

Missing relations, ranked by consequence rather than alphabetically:

| Relation | Why it matters | Priority |
|---|---|---|
| **`pg_operator`** | Operator resolution. This is what replaces the 9,546-line string rewriter in `pg_operators.rs` — `~`, `@>`, `&&`, `#>>` become catalog lookups by argument type. **The engine needs it internally regardless of compatibility.** | **1** |
| **`pg_cast`** | The three cast categories and the cast matrix. Same story: [`basin-pgtype`](../../../crates/basin-pgtype) needs this to resolve coercions, and `pg_catalog` needs to report it. | **1** |
| **`pg_attrdef`** | Column defaults. **`pg_dump` cannot round-trip a schema without it** — a dumped table loses every `DEFAULT`. | **2** |
| `pg_am` | Access methods. `\d` reports index types from it. | **3** |
| `pg_inherits` | Table inheritance / partitioning topology. | **3** |
| `pg_collation` | Collations; needed once text comparison is not just bytewise. | **3** |
| `pg_trigger` | Only the `information_schema.triggers` view exists. | **4** |
| `pg_enum`, `pg_range` | Enum labels and range subtypes. **UNVERIFIED** whether these exist — a `PgEnumProvider` is referenced in survey notes but does not appear in the macro list. | verify |

The ordering makes a point worth stating plainly: **`pg_operator` and `pg_cast`
are not compatibility work that competes with the engine — they are engine work
that happens to also be compatibility work.** The owned planner must resolve
operators and casts by argument type against *some* table. Making that table
`pg_catalog.pg_operator` rather than a private Rust map costs nearly nothing
extra and delivers introspection for free.

## 3. OID allocation

Builtin type OIDs are done — `crates/basin-pgtype/src/oid.rs` carries
Postgres's real fixed values (16=bool, 23=int4, 25=text, 1043=varchar,
1114=timestamp, 2950=uuid, 3802=jsonb), pinned by test because a driver
switching on `23` expects `int4`.

User objects are the open problem, with a constraint Postgres does not have:
**Basin is multi-tenant, and a project is an S3 prefix.** Two requirements pull
against each other —

- OIDs must be **stable across restarts**, because clients cache them and
  `pg_dump` emits them.
- OIDs must not **collide across projects**, since each project has its own
  `pg_class`.

Options, with the recommendation stated:

1. **Per-project OID space, allocated from a catalog sequence.** Each project
   starts user OIDs at 16384 (Postgres's `FirstNormalObjectId`) and allocates
   monotonically. Two projects both have a relation with OID 16384; they never
   meet, because a session is scoped to one project. **Recommended** — it is
   what a client expects, and the isolation is already structural.
2. Global OID space partitioned by project id. Avoids theoretical collision at
   the cost of OIDs that look nothing like Postgres's and exhaust `u32` far
   faster.
3. Hash of (project, name). Stable without a sequence, but collides, and a
   collision in `pg_class` is a correctness bug not a performance one.

Option 1 requires the allocation be **durable and monotonic** — a restart must
not reissue an OID a client still holds. That belongs in `basin-catalog`
alongside existing sequence machinery.

## 4. `search_path` and resolution

ADR 0022 already made system schemas first-class with `(schema, table)` keying
and a real `search_path`, replacing prefix hacks. What remains for fidelity:

- Default `search_path` of `"$user", public`, with `$user` resolving to the
  session role and silently skipped when no such schema exists.
- `pg_catalog` **implicitly first** unless explicitly placed later — the rule
  that makes `SELECT * FROM pg_class` work unqualified.
- `pg_temp` ahead of everything, once temp tables exist.
- Interaction with ADR 0013's per-project `auth` schema: it must be resolvable
  unqualified when in the path and never shadow a user table named `users`.

## 5. Payoff, ranked by value per unit effort

What faithful catalog behaviour actually buys, and what each consumer needs:

| Consumer | Requires | Effort |
|---|---|---|
| **psql `\d` / `\dt`** | `pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_attrdef`, `pg_am` | Mostly present — **`pg_attrdef` and `pg_am` are the gap** |
| **`pg_dump`** | All of the above plus `pg_depend` for ordering, `pg_description` for comments, `pg_proc` for functions | `pg_attrdef` is the blocker; without it every DEFAULT is lost |
| **ORM introspection** (Prisma, Drizzle, SQLAlchemy, ActiveRecord) | `pg_class`, `pg_attribute`, `pg_constraint`, `pg_index`, `pg_attrdef`, and **correct `atttypmod`** | Needs `basin-pgtype` typmods to reach `pg_attribute.atttypmod` — currently they do not exist |
| **Migration tools** (Flyway, Alembic, Atlas) | The above plus reliable `information_schema` | Closest to working today |
| **GUI clients** (DataGrip, TablePlus, pgAdmin) | Everything above plus `pg_settings`, `pg_roles`, `pg_stat_activity` | Those three already exist |

The recurring item is **`pg_attrdef` + real `atttypmod`**. Both are small, both
block schema round-tripping, and `atttypmod` is already half-built now that
`basin-pgtype` encodes typmods — it just has nowhere to be reported yet.

## 6. `CREATE EXTENSION` without dlopen

Basin ships extension functionality as native crates (`basin-geo`,
`basin-trgm`, `basin-cv`, `basin-cron`), and ADR 0002 rules out loading
Postgres `.so` files. That decision stands.

But ADR 0002 rejects **loading upstream binaries**, not the **statement**.
`CREATE EXTENSION pg_trgm` can be a pure catalog operation: mark the extension
installed in `pg_extension`, register its already-compiled functions into
`pg_proc` and its operators into `pg_operator`, and let name resolution find
them. No dynamic loading, no new attack surface, no unsafe code — and the
client-visible behaviour is that the statement works.

This **extends** ADR 0002 rather than contradicting it, and is worth a short
successor ADR when it lands so the distinction is on record.

## 7. Estimate and order

| Increment | LOC | Notes |
|---|---|---|
| `pg_operator` + `pg_cast` as real relations | 2–3k | Engine needs them anyway; count once |
| Catalog relations re-hosted off `TableProvider` onto a `SystemView` trait with pushdown | 4–6k | ~65 relations, mechanical, but the pushdown is the point |
| OID allocation (durable, per-project, monotonic) | 0.5–1k | In `basin-catalog` |
| `pg_attrdef`, `pg_am`, `pg_inherits`, `pg_collation` | 1–1.5k | `pg_attrdef` first |
| `atttypmod` wired from `basin-pgtype` through `pg_attribute` and `RowDescription` | 0.5k | Small, high payoff |
| `search_path` fidelity | 0.5–1k | Builds on ADR 0022 |
| `CREATE EXTENSION` as catalog op | 0.5k | |

**Total ~9–13.5k LOC**, of which 2–3k is work the engine requires regardless.

Order: `pg_operator`/`pg_cast` first (unblocks the planner), then `pg_attrdef`
and `atttypmod` (unblocks `pg_dump` and ORM introspection — the largest
user-visible win for the least code), then the `SystemView` re-hosting, then
the tail.
