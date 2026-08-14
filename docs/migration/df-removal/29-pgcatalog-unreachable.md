# 29 — `basin-pgcatalog` is unreachable

Status: complete (all five questions answered).
Branch: `feat/own-engine-remove-datafusion`. Date: 2026-08-13.

No build was run for this document (`cargo build`/`test`/`check` were out of
scope). Everything below is from `cargo tree`, grep, file reads, and a live
`psql`. Claims that would need a build are on the NEEDS VERIFICATION list at the
bottom.

---

## HEADLINE — the oracle validates the code nobody runs

`catalog_fidelity` — the differential harness that passes at 228 columns /
15,113 cells against a live PostgreSQL 18 — lives at
`crates/basin-pgcatalog/tests/catalog_fidelity.rs` and imports **only** the
dead crate:

```
crates/basin-pgcatalog/tests/catalog_fidelity.rs:112
    use basin_pgcatalog::mock::MockCatalog;
crates/basin-pgcatalog/tests/catalog_fidelity.rs:113-118
    use basin_pgcatalog::{
        pg_am::PgAm, pg_attrdef::PgAttrDef, pg_attribute::PgAttribute, pg_cast::PgCast,
        pg_class::PgClass, pg_constraint::PgConstraint, pg_depend::PgDepend,
        pg_description::PgDescription, pg_enum::PgEnum, pg_index::PgIndex, pg_inherits::PgInherits,
        pg_namespace::PgNamespace, pg_operator::PgOperator, pg_proc::PgProc, pg_sequence::PgSequence,
        pg_type::PgType, SystemView,
    };
```

It imports nothing from `basin-engine`, and in particular nothing from
`crates/basin-engine/src/info_schema_provider.rs`, which is what actually
answers `pg_catalog` queries (§2). It also runs against
`basin_pgcatalog::mock::MockCatalog`, not the real catalog.

So the strongest catalog oracle in the repo — a genuine live-server
differential, CI-enforced (`.github/workflows/ci.yml:170` sets
`PG_DIFF_TEST_DSN`; `scripts/check-differential.sh:147` refuses to report green
without it) — proves the fidelity of 12,450 lines that **no query can reach**,
while the ~36 `pg_catalog` relations users actually hit go unchecked by it.

The green tick is real. It is measuring the wrong artifact.

Note also what the harness's own docs say it was built to catch
(`catalog_fidelity.rs:4-16`): eight relations audited by hand, seven wrong in
the same way, every per-file unit test passing throughout because those tests
"check the crate's schema against itself, not against the server." That is the
same defect one level down — a test validating an artifact rather than the
behaviour. This document is that defect one level *up*: an oracle validating an
artifact rather than the system.

---

## 1. Is it actually inert? — Yes. Nothing depends on it.

```
$ cargo tree -i -p basin-pgcatalog
basin-pgcatalog v0.1.10 (/Users/pc/code/vulos/basin/crates/basin-pgcatalog)
```

That is the entire output. `cargo tree -i` prints the crate plus every reverse
dependency; here there are zero lines after the crate itself.

Every mention of the crate name in any `Cargo.toml`:

```
$ grep -rn "basin-pgcatalog" --include="Cargo.toml" .
Cargo.toml:6:    "crates/basin-pgcatalog",
Cargo.toml:67:basin-pgcatalog = { path = "crates/basin-pgcatalog" }
crates/basin-pgcatalog/Cargo.toml:2:name = "basin-pgcatalog"
crates/basin-exec/Cargo.toml:53:# as crates/basin-pgcatalog/tests/catalog_fidelity.rs and
```

- `Cargo.toml:6` — workspace member, so it *compiles*.
- `Cargo.toml:67` — a `[workspace.dependencies]` entry, i.e. an offer no crate
  ever accepts. No `[dependencies]` table anywhere names it.
- `crates/basin-exec/Cargo.toml:53` — inside a comment about
  `tokio-postgres` in `[dev-dependencies]`, citing `catalog_fidelity.rs` as
  precedent for the `PG_DIFF_TEST_DSN` convention. Not a dependency edge.

Every mention of the crate *as a Rust path*, outside its own tree:

```
$ grep -rn "basin_pgcatalog" crates/ tests/ src 2>/dev/null | grep -v "^crates/basin-pgcatalog/"
(no output)
```

Zero. No `use basin_pgcatalog::…`, no `extern crate`, nothing.

Size of what this makes unreachable:

```
$ find crates/basin-pgcatalog -name "*.rs" -exec wc -l {} + | tail -1
   13231 total          # 12450 of that is src/, the rest tests/
```

Verdict: **inert**. It is compiled by `cargo build --workspace` and its tests
run under `cargo test --workspace`, but no binary, no server, and no query path
can reach a single line of it.

---

## 2. What serves `pg_catalog` today? — DataFusion `TableProvider`s in `basin-engine`

The live implementation is `crates/basin-engine/src/info_schema_provider.rs`
(2,568 lines). It is a second, independent implementation of the same
relations, written against DataFusion's `TableProvider` trait.

Registration entry points:

- `crates/basin-engine/src/info_schema_provider.rs:2096`
  `pub(crate) fn register_info_schema_providers(...)`
- `crates/basin-engine/src/info_schema_provider.rs:2448`
  `pub(crate) fn register_cdc_providers(...)`

Both are called from the session builder:

```
crates/basin-engine/src/session.rs:3217   crate::info_schema_provider::register_info_schema_providers(
crates/basin-engine/src/session.rs:3265   if let Err(e) = crate::info_schema_provider::register_cdc_providers(
```

Counts (`grep -c` on `info_schema_provider.rs`):

```
$ grep -c 'pg_catalog_schema.register_table' crates/basin-engine/src/info_schema_provider.rs
36
$ grep -c 'info_schema.register_table' crates/basin-engine/src/info_schema_provider.rs
32
```

36 `pg_catalog` relations and 32 `information_schema` relations are registered
into the live `SessionContext`. Named `pg_catalog` relations include
`pg_class`, `pg_attribute`, `pg_namespace`, `pg_proc`, `pg_index`,
`pg_constraint`, `pg_type`, `pg_sequence`, `pg_enum`, `pg_depend`, `pg_authid`,
`pg_database`, `pg_roles`, `pg_views`, `pg_indexes`, `pg_tables`, `pg_settings`,
`pg_extension`, `pg_description`, `pg_locks`, `pg_stat_activity`,
`pg_stat_database`, `pg_stat_bgwriter`, `pg_stat_archiver`, plus the CDC three
(`pg_replication_slots` at :2475, `pg_publication` at :2482,
`pg_publication_tables` at :2491).

There are ~15 hand-written `impl TableProvider` blocks in that file plus a
`simple_provider!` macro at `info_schema_provider.rs:1456` that generates the
rest (`impl TableProvider for $struct_name` at :1486).

The data behind both implementations comes from
`crates/basin-catalog/src/info_schema.rs` (`InfoSchemaQuery`) — the DataFusion
providers call `InfoSchemaQuery::*` directly.

**So the duplication is: 12,450 unreachable lines in `basin-pgcatalog`, ~2,568
reachable lines in `info_schema_provider.rs`, covering an overlapping set of
relations.** The dead crate covers 22 relation modules; the live one covers 36
`pg_catalog` names. Neither is a superset (the dead crate has `pg_attrdef`,
`pg_operator`, `pg_am`, `pg_cast`, `pg_inherits`, which the live registration
list does not name; the live one has all the `pg_stat_*`, `pg_settings`,
`pg_roles`, `pg_database` etc. that the dead crate has no module for).

---

## 3. Which does `catalog_fidelity` test? — the OWNED (dead) crate

See the HEADLINE section at the top of this document.

Supporting file:line, beyond the imports quoted there:

- The harness is declared as a test target of the dead crate itself:
  `crates/basin-pgcatalog/Cargo.toml:50-51`
  (`name = "catalog_fidelity"`, `path = "tests/catalog_fidelity.rs"`).
  A test target of crate X links X and its dependencies; `basin-engine` is not
  among them, so the live providers are not even in the harness's link graph.
- It compares each relation's `arrow_schema()` (the dead crate's trait method)
  against the live server's `pg_attribute`
  (`catalog_fidelity.rs:282  async fn real_columns(...)`,
  `catalog_fidelity.rs:618` / `:688` loops).
- Row content is diffed for the five static relations only, and the
  catalog-dependent ones are fed `MockCatalog` (`catalog_fidelity.rs:112`),
  per the harness's own "What this harness does NOT check" section
  (`catalog_fidelity.rs:61-70`).

Nothing in `crates/basin-engine/` has an equivalent live-Postgres fidelity
harness for `info_schema_provider.rs`'s 36 registered relations. (See NEEDS
VERIFICATION — I confirmed by grep that no test file imports both
`info_schema_provider` and `tokio_postgres`, but did not run the suite.)

---

## 4. What does the owned engine answer for psql's `\d`? — it declines, and the incumbent behind it is missing a relation

### The actual query

`psql -E` against the reference server shows exactly what `\d` issues:

```
$ psql "postgres://pc@127.0.0.1:5432/postgres" -Ec '\d'
SELECT n.nspname as "Schema", c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' ... END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

Three relations: `pg_class`, `pg_namespace`, **`pg_am`**.

### What the owned engine does: falls back (not wrong)

Tracing `crates/basin-engine/src/owned_engine.rs`:

1. `collect_range_var` (`owned_engine.rs:1906-1923`) pushes
   `["pg_catalog", "pg_class"]` — it keeps the schema segment.
2. `build_resolver` (`owned_engine.rs:1459-1471`) then **throws the schema
   away**:
   ```
   owned_engine.rs:1460    let Some(last) = parts.last() else { continue };
   owned_engine.rs:1461    let key = last.to_ascii_lowercase();
   owned_engine.rs:1466    let table_name = TableName::new(last.as_str())
   owned_engine.rs:1469    let (meta, view_present) = crate::session::load_table_meta_cached(sess, &table_name)
   owned_engine.rs:1471        .await
   owned_engine.rs:1471        .ok_or(Fallback::Ineligible("table not found in the catalog"))?;
   ```
   `load_table_meta_cached` looks up the project's user tables. `pg_class` is
   not one, so this returns `None` and the whole statement declines with
   `Fallback::Ineligible("table not found in the catalog")`, which
   `classify` (`owned_engine.rs:524`) turns into `Declined` and routing falls
   through to DataFusion.

So the answer to "served, fallen back, or wrong" for the owned engine is
**fallen back** — safely, and for the right reason. No client-visible wrongness
from the owned engine on this query.

### But: `pg_am` is not implemented on the live path either

```
$ grep -rn "pg_am" crates/ tests/ | grep -v "^crates/basin-pgcatalog/"
(no output)
```

`pg_am` exists **nowhere** in the reachable tree — not in
`info_schema_provider.rs`'s 36 registered relations, not in `basin-catalog`,
not in `basin-router` (which has no `pg_catalog` handling at all:
`grep -rln "pg_class\|pg_catalog" crates/basin-router/src` returns nothing, so
there is no wire-level interception of `\d` to rescue it).

The only `pg_am` in the repo is `crates/basin-pgcatalog/src/pg_am.rs` (339
lines) — in the dead crate. Its declared schema (`pg_am.rs:169-172`):

```
Field::new("oid",       DataType::UInt32, false),
Field::new("amname",    DataType::Utf8,   false),
Field::new("amhandler", DataType::UInt32, false),
Field::new("amtype",    DataType::Utf8,   false),
```

PostgreSQL's reference answer:

```
$ psql "postgres://pc@127.0.0.1:5432/postgres" -tAc \
    "select oid, amname, amhandler::text, amtype from pg_catalog.pg_am order by oid"
2|heap|heap_tableam_handler|t
403|btree|bthandler|i
405|hash|hashhandler|i
783|gist|gisthandler|i
2742|gin|ginhandler|i
3580|brin|brinhandler|i
4000|spgist|spghandler|i
```

That is the shape of the finding: **the relation psql's most-used command needs
is implemented, tested against a live PostgreSQL, and unreachable — while the
path that actually answers the query does not have it at all.**

Expected client-visible behaviour: `\d` against Basin resolves
`pg_catalog.pg_class` and `pg_catalog.pg_namespace` (both registered,
`info_schema_provider.rs:2138` and `:2147`) and then fails to resolve
`pg_catalog.pg_am`, so DataFusion raises a table-not-found error for the whole
statement. **This is on the NEEDS VERIFICATION list** — it follows from the
registration list and the grep, but I did not start a server to observe the
error text.

### A latent correctness hazard in the owned resolver

Because `build_resolver` keys on the **last** name segment only
(`owned_engine.rs:1461`) and never checks `parts[0]`, a user table named
`pg_class` in the project's own schema would make `FROM pg_catalog.pg_class`
resolve to the *user's* table and be **served** by the owned engine with the
wrong rows. Today this is unreachable-in-practice only because no such table
normally exists — it is not prevented by any check in this function.
`crates/basin-catalog/src/reserved_schema.rs` reserves the *schema* name
(`reserved_schema.rs:69,87`), which does not stop a same-named table in
`public`. Cheap fix: decline in `build_resolver` when `parts.len() > 1` and
`parts[0]` is a reserved schema.

---

## 5. Why unreachable? — no concept of a virtual relation in the owned resolver

This is **not** a missing `Cargo.toml` dependency line. Adding
`basin-pgcatalog = { workspace = true }` to `crates/basin-engine/Cargo.toml`
would link the crate and change nothing about which queries it answers, because
there is no seam for it to plug into.

The evidence is the two resolver traits the owned path uses:

```
crates/basin-plan/src/lower/select.rs:117-119
    pub trait TableResolver {
        fn resolve_table(&self, name: &[String]) -> Option<(TableId, Schema)>;
    }
```

and its only real implementation:

```
crates/basin-engine/src/owned_engine.rs:1423-1428
    impl PlanTableResolver for CatalogTableResolver {
        fn resolve_table(&self, name: &[String]) -> Option<(TableId, PlanSchema)> {
            let key = name.last()?.to_ascii_lowercase();
            self.plan_tables.get(&key).cloned()
        }
    }
```

`plan_tables` is populated exclusively from `load_table_meta_cached` — real,
stored, file-backed tables (`owned_engine.rs:1469`). The execution side is the
same shape:

```
crates/basin-engine/src/owned_engine.rs:1430-1439
    impl ExecTableResolver for CatalogTableResolver {
        fn open(&self, table: TableId, projection: &[usize], filters: &[PlanExpr])
            -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
            self.exec.open(table, projection, filters)
        }
    }
```

delegating to `basin_exec::storage_source::StorageTableResolver`, whose
registration signature is `(table_id, project, arrow_schema, live_files)`
(`owned_engine.rs:1408-1420`) — a **set of object-store data files**. There is
no variant, no enum arm, no trait object for "a relation whose rows are
computed in memory rather than read from files."

`basin-plan/src/lower/select.rs:114-116` says so in its own doc comment:

> There is no catalog to back this yet — see the module docs. A mock
> implementation makes `FROM` lowering testable today; a real one (backed by
> `pg_class`/`pg_attribute`) plugs in later without this file changing.

So the gap is architectural: the owned engine's scan layer speaks files, and a
catalog relation is not a file.

### Sizing

**Not fully sized** — but the shape is clear, and it decomposes into three
pieces of very different cost:

1. **A virtual-relation arm in the exec resolver.** `ExecTableResolver::open`
   must be able to return a `BatchSource` backed by an in-memory
   `RecordBatch` rather than by `live_files`. `basin-pgcatalog` already
   produces `RecordBatch`es (that is what `catalog_fidelity` diffs), so this is
   an adapter, not new logic. Small — I would estimate low hundreds of lines,
   mostly in `basin-exec/src/storage_source.rs` plus the `CatalogTableResolver`
   in `owned_engine.rs`.
2. **Schema-qualified name resolution.** `resolve_table` must stop discarding
   `parts[0]` so `pg_catalog.pg_class` and `public.pg_class` are different
   relations (this also closes the hazard in §4). Small, but it touches a trait
   used by `basin-plan`'s lowering tests, so the blast radius is wider than the
   line count suggests.
3. **Wiring `basin-pgcatalog`'s `CatalogSource` to Basin's real catalog.** The
   crate is written against its own `CatalogSource` trait
   (`crates/basin-pgcatalog/src/catalog_source.rs`, with
   `crates/basin-pgcatalog/src/real_source.rs`, 872 lines, apparently the
   intended real implementation, and `mock.rs` the one every test uses). **This
   is the unsized piece.** Whether `real_source.rs` actually works against the
   live catalog is unknown: it is dead code that has never executed against
   anything but its own unit tests, and the whole point of this document is
   that "compiles and passes tests" has not predicted "works" here.

To size (3) I would need: to read `real_source.rs` against
`crates/basin-catalog/src/info_schema.rs` to see how far apart the two data
models are, and a build (out of scope here) to check whether `real_source.rs`
even wires to today's `Catalog` trait or to an older shape of it.

### The cheaper alternative worth stating

Nothing above is required to fix psql's `\d`. That needs **one** relation
(`pg_am`, 9 static rows, no catalog dependency at all) added to
`info_schema_provider.rs`'s registration list. The dead crate's `pg_am.rs` is a
ready-made, live-diffed source for the values. Deciding between "port the dead
crate in" and "keep growing `info_schema_provider.rs`" is the real question this
document raises; it should not block the one-relation fix.

---

## NEEDS VERIFICATION

Everything here needs a build or a running server, which were out of scope.

1. **`\d` actually errors against Basin.** Predicted from the registration list
   (`info_schema_provider.rs:2096-2447`) and `grep -rn "pg_am"` returning
   nothing outside the dead crate. Not observed. Run a Basin server and issue
   psql `\d`; capture the exact error.
2. **The same for `\dt`, `\di`, `\dv`, `\df`, `\d <table>`.** Each issues a
   different catalog query; `\d <table>` in particular joins far more relations.
   The set of psql commands that work today is unmeasured.
3. **`basin-pgcatalog` still compiles.** `cargo tree` resolved it, so its
   manifest is valid, but a workspace member that nothing imports can rot in
   ways only `cargo build -p basin-pgcatalog` shows. Note it *is* built by
   `cargo build --workspace`, so this is likely fine — worth one command to be
   sure.
4. **`catalog_fidelity`'s 228 columns / 15,113 cells figure.** Taken from the
   task brief, not reproduced here. `PG_DIFF_TEST_DSN=... cargo test -p
   basin-pgcatalog --test catalog_fidelity`.
5. **Whether `real_source.rs` compiles against today's `Catalog` trait.** The
   crux of the §5 sizing. It compiles inside its own crate; whether it can be
   handed a real `Arc<dyn Catalog>` from `basin-engine` is untested by anything.
6. **No live-Postgres fidelity harness exists for `info_schema_provider.rs`.**
   Largely confirmed by inspection, listed here because I did not run the
   suite. The live providers *are* tested — 2,114 lines across seven
   `crates/basin-engine/tests/info_schema_*_routing.rs` harnesses — but those
   are routing tests: `info_schema_routing.rs:4-6` says each "opens a
   `ProjectSession` and runs SQL through the same `execute()` entry point a
   pgwire connection would hit," asserting rows come back and the project
   boundary holds. They pin expectations written by hand, against no oracle.
   Only three `basin-engine` harnesses read `PG_DIFF_TEST_DSN`
   (`differential_pg.rs`, `owned_scan_liveness.rs`,
   `scan_predicate_column_alignment.rs`) and none of them is catalog-shaped.

   So the asymmetry is exact: **the dead crate has a live-server differential
   and no routing tests; the live path has routing tests and no live-server
   differential.** Each half of the catalog work has precisely the kind of test
   the other half needs.
