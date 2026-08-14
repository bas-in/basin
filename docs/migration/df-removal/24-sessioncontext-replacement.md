---
title: "DF removal — what it takes to delete SessionContext"
nav_section: migration
sidebar_position: 24
summary: "SessionContext supplies nine distinct services to basin-engine. Four are already fully owned (GUC state, object-store routing, optimizer rules, scan liveness) — measured, not asserted. Three are partly owned. Two — a function-hosting ABI and the virtual-relation catalog — have no owned counterpart on any reachable path, and basin-pgcatalog's 12,450 lines are wired to nothing (cargo tree -i returns no dependents). The behaviour switch can be gradual; the dependency cut is atomic, and its point of no return is the commit that makes ProjectSession.ctx optional. Cheapest honest milestone is flipping BASIN_OWNED_ENGINE on by default, not a cargo feature. Total, for one engineer: multi-month, not multi-week."
tags: [migration, datafusion, session, planning, estimates]
---

# 24 — What it takes to delete `SessionContext`

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [22](./22-removal-surface-measured.md) established that the
DataFusion dependency lattice is totally ordered and that the umbrella cannot
leave while `crates/basin-engine/src/session.rs` builds a
`datafusion::prelude::SessionContext`. It counted 802 name-resolution errors
and correctly called that a floor.

This document answers the question 22 stopped short of: **what would have to be
true for `SessionContext` to be deleted, in what order, and what does it cost?**

Every number below is followed by the command that produced it. Where something
could not be sized, it says "not sized" and says what would be needed. Measured
on the working tree at `e762b942` — which carries uncommitted edits from
concurrent work (`git status --porcelain` lists 8 modified files), so the census
figures drift slightly from 22's (1,041 → 1,044 reference lines). The drift is
noted where it matters and is immaterial everywhere else.

---

## 0. The headline, before the detail

Three findings change the shape of the plan.

1. **The owned path is genuinely independent.** Not "mostly". `basin-plan`,
   `basin-exec`, `basin-pgtype`, `basin-storage` and `basin-catalog` each
   measure **zero** `datafusion-*` crates in their normal dependency trees, and
   `owned_engine.rs` contains **zero** `use datafusion::` imports — every
   occurrence of the string is a comment. Its scan path does its own stripe
   routing and its own bucket warm. §2 audits this claim service by service and
   it survives.

2. **`basin-pgcatalog` is wired to nothing.** 12,450 lines, 22 relation
   modules, and `cargo tree -p basin-pgcatalog -i` returns no dependents. The
   owned pg_catalog exists, is tested, and has never answered a query. §1's
   service S7 is therefore the largest *unmeasured* gap in the plan, not the
   largest *unbuilt* one — a materially different risk.

3. **The behaviour switch is gradual; the dependency cut is atomic.** Those are
   two different switches and conflating them has been costing the plan
   clarity. §4 names the exact line whose change is the point of no return.

And the estimate, said plainly up front: **for one senior engineer this is
roughly 190 person-days — nine to ten person-months. It is a multi-month
project, not a multi-week one.** §3 shows the arithmetic. §3.4 notes why this
branch's observed throughput does not translate into calendar months at the
same ratio.

---

## 1. The service inventory

`session::open` (`session.rs:3009-3439`) is 430 lines that build one
`SessionContext` and hang nine distinct services off it. Read end to end, plus
the census of what the rest of the crate asks of the result:

```
$ grep -rhoE "\b(ctx|sess\.ctx|session\.ctx|self\.ctx|_ctx)\.[a-z_]+\(" \
    crates/basin-engine/src --include="*.rs" | sed 's/^.*\.\([a-z_]*\)($/\1/' | sort | uniq -c | sort -rn
 345 register_udf      27 register_table    23 sql            21 deregister_table
  14 register_udtf     13 register_udaf      8 state           4 catalog
   3 state_ref          1 udf                1 task_ctx        1 table_exist
   1 register_object_store   1 register_batch   1 lookup_enum   1 load_schema
   1 execute_logical_plan
```

That census undercounts `.sql()` because many sites go through a `DataFrame`
binding rather than a `ctx.` receiver. The honest count:

```
$ grep -rn "\.sql(" crates/basin-engine/src --include="*.rs" | wc -l
41
```

Split by production vs `#[cfg(test)]` **by hand**, because the obvious
brace-matching script misclassifies `executor.rs` (its first column-0
`#[cfg(test)]` is at line 193, long before the production sites at 4817–13538):

| | Sites |
|---|---:|
| Production `.sql()` | **20** |
| Inside `#[cfg(test)]` | 21 |

Production sites: `executor.rs` 4817 (CTAS output schema), 7659/7674 (MERGE
source), 11404/11414 (**the main SELECT path**), 13538 (recursive CTE
expansion); `dml_mutate.rs` 108/169/7787; `prepared.rs` 2516/2527/2762;
`rls.rs` 466/827; `explain.rs` 76; `constraints.rs` 1664;
`generated_cols.rs` 112; `type_ddl.rs` 539; `lifecycle.rs` 572.

### The nine services

| # | Service | Where it is built | Call sites | Owned counterpart |
|---|---|---|---:|---|
| S1 | SQL parse → logical plan | `ctx.sql()` | 20 prod / 21 test | `basin_plan::lower::{select,dml}` — **exists, SELECT-only reachable** |
| S2 | Base-table registration | `refresh_table_inner` (`session.rs:3777`) | 123 register/deregister | `CatalogTableResolver` (`owned_engine.rs:1372`) — **exists, base tables only** |
| S3 | Scalar / aggregate / table-function registry | `build_stateless_udf_cache` (`session.rs:365`) + 9 per-session calls | 372 | `basin_exec::eval` OID `match` — **structurally different, no registration** |
| S4 | Session GUC / config state | Basin's own `SessionState` (`session.rs:766`) | 8 `ctx.state()` prod | **already owned, ~95%** |
| S5 | RuntimeEnv: object store, caches, memory pool | `session.rs:3089-3185` | 1 `register_object_store` | **object store + caches fully owned; memory pool not** |
| S6 | `TaskContext` / physical execution | `executor.rs:11571` | 1 prod drive point | synchronous drain (`owned_engine.rs:1334`) — **exists, much weaker** |
| S7 | Virtual / system relations | 6 `register_*_providers` calls in `session::open` | 15 `TableProvider` impls | `basin-pgcatalog` — **exists, reachable from nothing** |
| S8 | Optimizer / analyzer / physical-optimizer rules | `session.rs:3074-3146` | 4 + 1 + 2 impls | `basin_plan::opt::optimize_default` — **exists; 4 of 5 die with the cut** |
| S9 | Error taxonomy / SQLSTATE | `map_df_plan_error`, `map_df_exec_error` (`executor.rs:10708`, `10783`) | 2 mappers | none — **string-matching today** |

Counts behind the table:

```
$ grep -rcE "register_ud(f|af|tf)\(" crates/basin-engine/src --include="*.rs" | grep -v ":0" | awk -F: '{s+=$2} END {print s}'
372
$ grep -rcE "\.(de)?register_table\(" crates/basin-engine/src --include="*.rs" | grep -v ":0" | awk -F: '{s+=$2} END {print s}'
123
$ grep -rc "impl ScalarUDFImpl for" crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
245
$ grep -rc "impl AggregateUDFImpl for" crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
11
$ grep -rc "impl OptimizerRule for"   crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
4
$ grep -rc "impl AnalyzerRule for"    crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
1
$ grep -rc "impl PhysicalOptimizerRule for" crates/basin-engine/src --include='*.rs' | awk -F: '{s+=$2} END {print s}'
2
```

Where the register-table sites live is worth stating, because it reframes S2 vs
S7:

```
$ grep -rcE "\.(de)?register_table\(" crates/basin-engine/src --include="*.rs" | grep -v ":0" | sort -t: -k2 -rn | head -4
crates/basin-engine/src/info_schema_provider.rs:68
crates/basin-engine/src/session.rs:29
crates/basin-engine/src/executor.rs:8
crates/basin-engine/src/project_usage_view.rs:3
```

**More than half the table-registration surface is virtual relations, not user
tables.** S7 is bigger than S2.

### S4 is nearly done, and nobody has said so

`SessionState` (`session.rs:766-971`) is Basin's own struct, not DataFusion's.
It carries every session GUC the engine exposes: `statement_timeout`,
`lock_timeout`, `idle_in_transaction_session_timeout`,
`basin.synchronous_commit`, both `pg_trgm` thresholds, `basin.read_tier`,
`TimeZone`, the schema/`search_path` registry, transaction state, cursors,
prepared statements, advisory locks, the sequence `currval` cache, LISTEN
subscriptions, and five caches. It imports nothing from DataFusion.

What DataFusion's `SessionConfig` actually carries for Basin is **two knobs**
(`session.rs:3047-3052`): `listing_table_ignore_subdirectory=false` and
`target_partitions=1` — plus the runtime mutation of `target_partitions` and
three `repartition_*` flags in `TargetPartitionsGuard`
(`executor.rs:10670-10688`, applied at `11155-11216`), and the default catalog
name read at four sites. Every one of those is a DataFusion-execution concern
that **has no meaning in the owned executor**, which is single-partition by
construction. They are deletions, not ports.

---

## 2. What the owned path does instead — the independence audit

This is the section the request asked to be sceptical in, so it is written as
an attempt to *falsify* the independence claim rather than to confirm it.

### 2.1 The crate-level result

```
$ for c in basin-exec basin-plan basin-pgtype basin-storage basin-pgcatalog basin-catalog; do \
    echo "$c: $(cargo tree -p $c -e normal --prefix none | grep -cE '^datafusion')"; done
basin-exec: 0
basin-plan: 0
basin-pgtype: 0
basin-storage: 0
basin-pgcatalog: 0
basin-catalog: 0
```

```
$ grep -rn "datafusion" crates/basin-exec/src crates/basin-plan/src \
      crates/basin-pgtype/src crates/basin-storage/src
```
Six hits, all of them comments (`eval.rs:484,486,1635`; `lib.rs:6`;
`lib.rs:24`; `predicate.rs:3`). Zero code.

```
$ grep -c "^use datafusion" crates/basin-engine/src/owned_engine.rs
0
```

### 2.2 Service by service, on the owned path

**Catalog resolution — independent.** `build_resolver`
(`owned_engine.rs:1433`) walks the parse tree, then per table calls
`crate::session::load_table_meta_cached`. That function
(`session.rs:2041-2073`) reads `engine.config().catalog` — `basin-catalog`,
zero DataFusion — through a per-session cache keyed on the catalog epoch. It
does not touch `ctx`. **No leaning.**

**File-set selection — independent, and correct for the same reason the
incumbent is.** `build_resolver` takes `meta.live_data_files()`, the same
catalog-authoritative source `refresh_table_inner` uses (`session.rs:3863`),
not an object-store LIST. That was the `553f4f8b` fix and it holds.

**Object store and stripe routing — independent.** This is the one that looked
most likely to be a hidden lean, because `session::open` registers a
*project-scoped, striping-aware* store on the DataFusion `RuntimeEnv`
(`session.rs:3184-3185`, `Storage::scan_object_store`) and the owned path never
sees that registration. It does not need to: `basin_storage::reader`'s
`store_for_data_file` (`reader.rs:286-296`) re-derives each file's partition
from its key and resolves the stripe bucket **inside `basin-storage`**, on every
read, so `read_paths_with_schema` stripes natively
(`reader.rs:1118-1124`). And the warm the registration depends on is performed
by the owned path itself:

```
crates/basin-exec/src/storage_source.rs:340
    match storage.ensure_bucket_assignment(&project).await {
```

with a comment saying exactly why (`Storage::read` warms it;
`read_paths_with_schema` does not, "so do it here to keep the two paths
byte-identical in where they route their GETs"). **No leaning.**

**Type coercion — independent.** `pgtype_of` maps Arrow fields to `PgType`
locally; operator and function resolution go to
`basin_pgtype::operator::resolve` / `func::resolve` — the real
`pg_operator` / `pg_proc` tables, zero DataFusion.

**Evaluation context — independent, and the `6264603c` seam is the reason.**
`crate::session::eval_session(&sess.state)` (`session.rs:1353-1370`) reads
Basin's `SessionState` — `TimeZone`, statement and transaction timestamps — and
returns a `basin_exec::eval::EvalSession`. `SessionState` is Basin's struct
(§1). The `SET TimeZone` validator (`session.rs:1321`) asks
`EvalSession::with_time_zone(...).time_zone()` — the evaluator's own parser — so
"accepted by SET" and "resolvable during evaluation" cannot drift. **No
leaning.** This is the single cleanest piece of the migration and it should be
the template for the rest.

### 2.3 Where it *does* lean — three real findings

**(a) Every owned-path query pays a full DataFusion session-open.**
`owned_engine::try_execute` takes `&ProjectSession`, and `ProjectSession`
(`lib.rs:1787`) has a non-optional field
`pub(crate) ctx: datafusion::prelude::SessionContext`. Constructing one runs all
430 lines of `session::open`: builds the `SessionState` with the full UDF
registry, registers the object store, registers six sets of virtual providers,
loads the project's WASM functions from the catalog, and — the expensive part —
**pre-registers every table in the project** by building a `ListingTable` each
(`session.rs:3359-3375`). A session that only ever runs owned-path SELECTs pays
all of it. This is lifecycle and cost coupling, not correctness coupling, but it
means any latency comparison of the two engines that includes session-open is
measuring DataFusion on both sides.

**(b) The owned path serves only `StmtKind::Select`, and only single-statement.**
The gate is one line, `executor.rs:2098`:

```rust
if matches!(kind, crate::pg_ast::StmtKind::Select) {
    crate::region::region_read_guard(sess).await?;
    let node = tree.stmts().next().expect("kinds[0] implies stmts[0]");
    let outcome = crate::owned_engine::try_execute(sess, node, sql).await;
```

DDL, DML, SET, transaction control, EXPLAIN, COPY, cursors, prepared statements
and every TimescaleDB/hypertable/cagg intercept are 100% incumbent. And even
inside SELECT, `try_execute_inner` passes `None` as `build_in_session`'s
`Option<&dyn DmlResolver>`, so `lower_dml`'s output — which *is* wired
(`owned_engine.rs:1289`) — always dies at `BuildError::Unsupported`. The probe's
DML column reading 0 is structural, not incidental.

**(c) The served ratio is over a SELECT-weighted corpus.** The probe corpus is
231 shapes:

```
$ sed -n '147,412p' crates/basin-engine/tests/fallback_histogram.rs | grep -cE '^\s*\('
231
```

of which 14 are DML — and DML cannot be served at all. So a headline like
"206/231 served" is a statement about **SELECT** coverage against a corpus that
is 94% SELECT by construction. It does not overstate *SELECT* independence — §2.2
says that independence is real — but it says nothing at all about the other five
sixths of the statement surface by source volume (`dml_mutate.rs` 10,747 lines +
`dml.rs` 4,605 + `ddl.rs` 3,200 = 18,552 lines of write path against
`owned_engine.rs`'s 3,137).

> **Verdict on the honesty question.** The owned SELECT path is genuinely
> DataFusion-free — audited on five axes and it holds on all five. The
> overstatement is not in the "served" number; it is in reading a
> SELECT-corpus ratio as a migration-completion ratio.

---

## 3. The sequenced plan

### 3.1 The dependency order between services

The services are not independent, and the graph is shallow but has one hard
serialisation:

```
S4 GUC state ────────────────┐
S5 object store / runtime ───┤
S8 optimizer rules ──────────┼──> (already satisfied for SELECT)
S2 base-table resolution ────┘
                                    │
S3a  FUNCTION-HOSTING ABI ──────────┼──> S3b rehost 329 functions
       │                            │        │
       ├──> S7 virtual relations ───┤        │
       └──> S6b SRF-as-relation ────┘        │
                                             ▼
                              S9 write path (DML + DDL)
                                             │
                                             ▼
                              S6a physical-execution hardening
                                (streaming, cancellation, memory)
                                             │
                                             ▼
                                     THE CUT (atomic)
```

**S3a is the critical path's head.** `basin-exec` dispatches scalars by
matching on `pg_proc` OID:

```
$ grep -cE "^\s*(pub )?const OID_" crates/basin-exec/src/eval.rs
130
```

(up from 85 at `c8061346` — doc 22 §8's growth continued.) A hard-coded `match`
does not admit registration, and three things need registration: per-session
UDFs that capture session state (auth, advisory locks, `pg_cancel_backend`,
timestamps, TABLESAMPLE), per-project WASM UDFs whose names come from the
catalog at session-open, and the virtual relations. Until an ABI exists, none of
S3b, S7 or S6b can start. It is the one item that is genuine design work rather
than porting.

### 3.2 Sizing, with the reasoning visible

Person-days for one senior engineer. "Mechanical" means the shape of the answer
is already decided and the work is transcription; "engineering" means a design
decision is still open.

| # | Track | Kind | Size (p-d) | Reasoning |
|---|---|---|---:|---|
| S4 | GUC / config | mechanical | **1–2** | 8 production `ctx.state()` sites; `TargetPartitionsGuard` and the 3 `repartition_*` flags are *deleted*, not ported, because the owned executor is single-partition. `SessionState` already holds every GUC. |
| S8 | Optimizer rules | mechanical | **1** | Doc 22's follow-up already measured it: 4 of 5 rules die (the owned IR has each one's substance natively), `citext_analyzer` is blocked on a CITEXT oid in `basin-pgtype`. A ready 6-file patch exists and is deliberately unapplied. |
| S5 | Object store / caches | done | **0** | §2.2. Measured independent. |
| S5b | Global memory pool | engineering | **3–5** | `engine.inner.query_memory_pool` caps the *sum* of concurrent working sets. `basin_exec::build::DEFAULT_OPERATOR_BUDGET` is **per-operator**. There is no owned global cap, so cutting DataFusion without this replaces a bounded-OOM failure mode with an unbounded one. Not a port — new code. |
| S2 | Base-table resolution | mechanical | **2–3** | `CatalogTableResolver` exists and works; what it lacks is views, RLS, promoted-JSONB shadow columns and hot-tier overlay — each of which is currently an `Ineligible` decline, and each of which is really a *different* track (see S9 for overlay, S3b for RLS's predicate injection). |
| **S3a** | **Function-hosting ABI** | **engineering** | **5–10** | Design + land a registration surface in `basin-exec` that accepts (i) stateless scalars, (ii) session-capturing closures, (iii) catalog-sourced dynamic functions, (iv) aggregates with a groups-accumulator tier. Four consumer shapes, one trait family. **Critical path head.** |
| S3b | Rehost 329 functions | mostly mechanical | **40–70** | 344 names harvested across the function-hosting files; 15 covered by `basin-exec` today (doc 22 §4.2), so 329 remain, plus 29 orphans still answered only by `datafusion-functions`. Empirical rate: `basin-exec` went 52 → 95 → 130 OID constants across `6f0d9630` → `c8061346` → `e762b942`, but **that growth was the cheap half** — orphans with no Basin code to port. The 329 have bodies to move: `jsonb_udf.rs` 7,524 lines, `geo_glue.rs` 3,820, `fts_udf.rs` 2,926, `pg_catalog_udf.rs` 1,314. At 5–8 names/day for bodies that are already Arrow-in/Arrow-out, plus the ABI's own settling. |
| S3c | `wasm_udf.rs` | engineering | **not sized** | 1,782 lines; its function names come from the project catalog at session-open, so the surface is unbounded per project. Needs S3a's dynamic-registration shape before it can be scoped at all. What would be needed to size it: the ABI decision from S3a. |
| S6b | SRF as a relation | engineering | **5–10** | `register_udtf` + `TableProvider` in `jsonb_udf.rs` / `jsonb_path_udf.rs` (49 errors in doc 22's classification). `basin-exec` has `lateral.rs`, so the receiving shape half-exists; the SRF→relation lowering does not. |
| S7 | Virtual / system relations | mixed | **10–20 + unmeasured** | 15 `TableProvider` impls, 68 of the 123 register-table sites. `basin-pgcatalog` exists at 12,450 lines across 22 relation modules — **and is reachable from nothing** (§3.3). Wiring it into `build_resolver` as a second resolver source: 3–5 days. Achieving fidelity with the 15 incumbent providers: **nobody has diffed them**, so unsized. |
| S9 | Write path (DML + DDL) | engineering | **30–60** | Doc 21 owns the detail. Needs a `DmlResolver`, a storage-backed staging sink, `SideEffects::note_published` wired before its first write, constraint enforcement, RETURNING, sequences, generated columns, reactors, RLS-on-write. 18,552 lines of incumbent write path, whose DataFusion dependency is 12 of the 20 production `.sql()` sites (`executor.rs` 4817/7659/7674, `dml_mutate.rs` ×3, `rls.rs` ×2, `constraints.rs`, `generated_cols.rs`, `type_ddl.rs`, `lifecycle.rs`) plus `refresh_table` registration. |
| S6a | Physical-execution hardening | engineering | **15–25** | Three gaps, all real: (1) **no streaming** — the owned drain (`owned_engine.rs:1334-1361`) accumulates every batch into a `Vec` and constructs `ExecResult::Rows` only after `Ok(None)`; the incumbent's `df.collect()` at least runs inside a cancellable future. (2) **no cancellation** — the incumbent races the collect against `statement_timeout` *and* the per-session `pg_cancel_backend` `Notify` (`executor.rs:11632-11640`); a synchronous `next_batch()` loop with no `.await` cannot be raced at all. (3) **no spill.** Restructuring the drain touches the `ExecResult` contract, which has 388 sites across the workspace. |
| S9b | Error taxonomy / SQLSTATE | mechanical | **2–4** | `map_df_plan_error` recovers 42P01 by string-matching four exact DataFusion planner messages. Under the owned path these become native — cheaper, not harder. Risk: the full set of SQLSTATEs the incumbent derives from DF messages is not enumerated anywhere. |
| S10 | `ExecutionPlan` nodes | removal | **2** (or 10–20) | 7 files, 4,475 lines (`hot_tombstone.rs` 1,512, `vortex_listing_format.rs` 1,202, then five scans). Doc 22's follow-up is right that these are removals, not ports — **but** deleting them deletes the index-assisted file pruning (GIN, R-tree, trigram, secondary B-tree) that the owned path deliberately does not have (`owned_engine.rs:55-59` says so explicitly: "costs performance and not correctness"). Delete: 2 days. Port the pruning into `basin_exec::storage_source`'s file prune: 10–20 days, precision not established. |

**Sum of midpoints: ≈ 190 person-days ≈ 9–10 person-months**, excluding the two
unsized items (S3c, S7's fidelity half) and taking S10's cheap branch.

### 3.3 The finding that most changes S7

```
$ cargo tree -p basin-pgcatalog -i -e normal --prefix none
basin-pgcatalog v0.1.10 (/Users/pc/code/vulos/basin/crates/basin-pgcatalog)
$ grep -rn "basin-pgcatalog" --include="Cargo.toml" . | grep -v target | grep -v ':#'
Cargo.toml:6:    "crates/basin-pgcatalog",
Cargo.toml:67:basin-pgcatalog = { path = "crates/basin-pgcatalog" }
crates/basin-pgcatalog/Cargo.toml:2:name = "basin-pgcatalog"
$ grep -rn "basin_pgcatalog" --include="*.rs" crates services cli | grep -v "^crates/basin-pgcatalog/"
(no output)
```

`basin-pgcatalog` appears in `basin-exec/Cargo.toml` **only inside a comment**
(line 53, pointing at its test file as a convention precedent). It is a
workspace member with no dependents and no importers. Twenty-two relation
modules — `pg_class`, `pg_attribute`, `pg_proc`, `pg_type`, `pg_operator`,
`pg_index`, `pg_constraint`, `pg_depend`, `pg_inherits` and the rest — that no
query has ever reached.

That is good news and bad news in equal measure. Good: the hardest-looking part
of S7 is already written. Bad: **it has never been executed against a client
query, so its fidelity against the incumbent's 15 providers is entirely
unmeasured**, and "written" and "correct" are not the same claim. The cheapest
way to find out is to wire it into `owned_engine::build_resolver` behind the
existing shadow-compare flag and let the incumbent be the oracle — which is a
few days of work and would convert an unsized risk into a measured one.

### 3.4 On the unit

This branch is 181 commits over three calendar days
(`git log --oneline main..HEAD | wc -l`, `main..HEAD` merge-base dated
2026-08-11), producing `basin-plan` at 23,819 lines and `basin-exec` at 40,716.
That throughput does not make the 190 person-days smaller — it means the
*calendar* mapping is not 1:1 with the *effort* mapping, and person-days remain
the honest unit for the effort. Anyone converting to calendar time should do it
with their own observed parallelism, not by assuming the last three days
generalise: the work completed so far has been concentrated in the two tracks
with no legacy to port (new IR, new operators, orphan functions), and every
track in §3.2 above except S4 and S8 has legacy to port.

---

## 4. The point of no return

### 4.1 Two switches, not one

The single most useful correction this document can make is that "when does the
incumbent stop working" has two different answers.

**The behaviour switch is gradual, and the machinery for it already exists.**
`owned_engine::try_execute` returns an opaque `Outcome`, and
`Outcome::into_result` distinguishes "declined — nothing happened, safe to
re-run below" from "attempted, published something, must NOT re-run"
(`owned_engine.rs:73-119`). The `SideEffects` latch is threaded through
`try_execute_inner` for exactly this. The gate at `executor.rs:2098` can widen
one `StmtKind` at a time — Select, then Insert, then Update/Delete, then DDL —
with the incumbent as the fallback for every kind not yet widened, and with
shadow-compare running as a differential oracle the whole way. **At no point in
that sequence does the incumbent stop working.** The risk profile of the
behaviour migration is therefore low, and it is low by design rather than by
luck.

**The dependency switch is atomic.** Doc 22's lattice is the reason: the only
tree-moving cuts are the umbrella (31 → 18) and then `vortex-datafusion`
(18 → 0), and every intermediate deletion removes exactly zero crates. Confirmed
at this tip:

```
$ cargo tree -p basin-engine -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l
31
$ cargo tree -p vortex-datafusion -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l
18
$ cargo tree -p basin-server -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l
31
```

So the shipped server binary carries all 31, and it carries all 31 until the
last one leaves.

### 4.2 The exact line

```
crates/basin-engine/src/lib.rs:1787
    pub(crate) ctx: datafusion::prelude::SessionContext,
```

**The point of no return is the commit that makes this field optional or
removes it.** Not the manifest edit — the field.

Before that commit: both paths coexist, `sess.ctx` is always constructible,
every not-yet-migrated code path keeps working, and rollback is a one-line
revert of the gate.

After it: every one of the 101 `.ctx` references across 9 files
(`grep -rn "\.ctx\b" crates/basin-engine/src --include="*.rs" | wc -l` → 101;
same grep with `-rln … | wc -l` → 9 files) must have an owned answer, including
the 20 production `.sql()` sites, all 15 virtual relations, and the SQLSTATE
mapping. There is no half state, because the field is not an `Option` and 66
function signatures take `ctx: &SessionContext`
(`grep -rn "ctx: &SessionContext\|ctx: &datafusion::prelude::SessionContext" crates/basin-engine/src --include="*.rs" | wc -l` → 66).

The single practical mitigation is to make that field `Option<SessionContext>`
*early* — while both paths still work — so the compiler enumerates every site
that assumes it exists. That converts an invisible risk into a mechanical one,
and it is the cheapest de-risking action available in this whole plan. It is not
in §3.2's sizing because it is a day's work whose value is entirely in what it
reveals.

---

## 5. The cheapest honest milestone

### 5.1 The cargo-feature idea, taken seriously

The proposal: `datafusion`, `vortex-datafusion` and the three literal deps
become `optional = true`, gated behind a non-default feature, so the default
build is DataFusion-free while the fallback stays available under a flag.

**The dependency mechanics work. This is not cosmetic.** Measured, because the
request was rightly sceptical of a change that moves a declaration without
moving the tree:

```
$ cargo tree -p basin-fn -e normal --prefix none | awk '{print $1}' | sort -u | wc -l
592
$ cargo tree -p basin-fn -e normal --prefix none --no-default-features | awk '{print $1}' | sort -u | wc -l
1
$ cargo tree -p basin-fn -e normal --prefix none | grep -c "^wasmtime"
36
$ cargo tree -p basin-fn -e normal --prefix none --no-default-features | grep -c "^wasmtime"
0
```

592 crates to 1. An unenabled optional dependency is genuinely absent from the
build graph — it stays in `Cargo.lock`, but the lock is not the graph. So unlike
doc 22 §5's 4-to-1 declaration re-route (which moved zero crates) and unlike the
`[dev-dependencies]` move (which doc 22 §6 showed is 81% of the work anyway),
this one would really deliver "the default build has no DataFusion in it".

**The cost is the whole problem.** For `cargo check --no-default-features` to
pass, all ~805 name-resolution sites must be `cfg`-gated or live in gated
modules. Module-level gating is fine for the leaf files — the 27 function-hosting
modules, the 7 `ExecutionPlan` files, the optimizer rules. It does not work for
the three that matter:

- `lib.rs:1787` — `ProjectSession.ctx` is a struct **field**. Gating it gates
  every constructor and all 101 `.ctx` references.
- `executor.rs` — 19,099 lines in which the DataFusion path, the DF-free fast
  paths and the owned bridge are interleaved *inside single functions*.
  `exec_select` alone contains the owned-engine gate, the partition-pruning
  call, the `TargetPartitionsGuard`, `ctx.sql()`, and the
  `physical_plan::collect` drive.
- `session.rs` — 8,507 lines in which `session::open` builds **both** the
  DataFusion context and Basin's own `SessionState`, and `SessionState` is
  exactly what the owned path needs.

Making those three gateable means extracting a DataFusion-free session-open, a
DataFusion-free executor spine, and a DataFusion-free `ProjectSession`. **That
is the same architectural work as removal.** The feature flag buys the ability
to keep the fallback compiling afterwards; it does not buy an earlier date.

> **Verdict:** the feature gate is a good *mechanism for the cut* and a bad
> *milestone before it*. Adopt it at the end — it lets the fallback survive one
> release for rollback, at the price of a second CI matrix leg and permanent
> `cfg` maintenance. Do not schedule it as an intermediate deliverable; it
> cannot be reached any sooner than deletion can.

### 5.2 What the cheapest honest milestone actually is

**Flip `BASIN_OWNED_ENGINE` to default-ON for SELECT, with the fallback
intact.**

It changes no dependency and no line of the manifest. What it changes is which
engine answers the statement that matters most, and therefore what the residual
DataFusion surface *is*: after the flip, DataFusion is the fallback rather than
the path, and every remaining `datafusion` reference is serving either a
statement kind the owned engine has not reached or a shape it declined. That is
a materially more honest description of the state than today's, and it is
reachable now.

Its cost is not engineering, it is evidence: the shadow-compare campaign
(`BASIN_OWNED_ENGINE_SHADOW_COMPARE`, already built) run broadly enough that the
divergence count is a number someone would sign. Doc 22's follow-up already
found four bugs by reading, two of them on the shipping path — which is a
reasonable prior for what a broad differential run will surface.

Two smaller items are cheaper still and worth taking in the same pass, because
each converts an unsized risk into a measured one:

1. **Make `ProjectSession.ctx` an `Option`** (§4.2). One day. The compiler then
   enumerates the real removal surface, which is the type-level measurement doc
   22 §10 correctly identified as missing and did not build.
2. **Wire `basin-pgcatalog` into `build_resolver` behind shadow-compare**
   (§3.3). Three to five days. Converts S7's unsized fidelity half into a
   divergence count against the incumbent oracle.

---

## 6. What this document does not know

- **The type-check surface.** Still unmeasured, exactly as doc 22 §10 left it:
  103 distinct non-arrow DataFusion symbols
  (`grep -rhoE "datafusion(_[a-z_]+)?::[A-Za-z0-9_:]+" ... | wc -l` → 103,
  unchanged at this tip) are named in signatures nothing has yet had to satisfy.
  Every size in §3.2 is a size for *replacing a service*, not for *satisfying a
  signature*, and the second number is strictly larger than zero. Making
  `ctx` an `Option` (§5.2) is a cheaper probe of the same surface than the stub
  crate doc 22 proposed.
- **S7's fidelity.** 15 incumbent providers vs 22 `basin-pgcatalog` relation
  modules, never diffed. The count mismatch alone says the mapping is not 1:1.
- **`wasm_udf.rs`'s scope** (S3c), which is unbounded per project by
  construction.
- **The SQLSTATE set** the incumbent derives from DataFusion message strings.
  `map_df_plan_error` handles four documented patterns; whether four is the
  whole set has not been checked against the pgwire surface.
- **Whether S10's pruning matters.** Deleting the seven `ExecutionPlan` files
  removes GIN / R-tree / trigram / B-tree file pruning that the owned path does
  not have. Whether that is a 5% regression or a 100× one on indexed shapes is
  not measured, and the `pg_matrix` harness is the instrument that could measure
  it.

---

## 7. Reproducing everything here

```sh
# service census
grep -rhoE "\b(ctx|sess\.ctx|self\.ctx)\.[a-z_]+\(" crates/basin-engine/src --include="*.rs" \
  | sed 's/^.*\.\([a-z_]*\)($/\1/' | sort | uniq -c | sort -rn
grep -rn "\.sql(" crates/basin-engine/src --include="*.rs" | wc -l            # 41
grep -rcE "register_ud(f|af|tf)\(" crates/basin-engine/src --include="*.rs" \
  | grep -v ":0" | awk -F: '{s+=$2} END {print s}'                           # 372
grep -rcE "\.(de)?register_table\(" crates/basin-engine/src --include="*.rs" \
  | grep -v ":0" | awk -F: '{s+=$2} END {print s}'                           # 123
grep -rn "\.ctx\b" crates/basin-engine/src --include="*.rs" | wc -l          # 101

# owned-path independence
for c in basin-exec basin-plan basin-pgtype basin-storage basin-pgcatalog basin-catalog; do
  echo "$c: $(cargo tree -p $c -e normal --prefix none | grep -cE '^datafusion')"; done   # all 0
grep -c "^use datafusion" crates/basin-engine/src/owned_engine.rs            # 0

# basin-pgcatalog is unwired
cargo tree -p basin-pgcatalog -i -e normal --prefix none                     # itself only

# the lattice, re-verified
cargo tree -p basin-engine       -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l  # 31
cargo tree -p vortex-datafusion  -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l  # 18
cargo tree -p basin-server       -e normal --prefix none | grep -E "^datafusion" | awk '{print $1}' | sort -u | wc -l  # 31

# optional deps really do leave the tree
cargo tree -p basin-fn -e normal --prefix none | awk '{print $1}' | sort -u | wc -l                       # 592
cargo tree -p basin-fn -e normal --prefix none --no-default-features | awk '{print $1}' | sort -u | wc -l # 1

# probe corpus size
sed -n '147,412p' crates/basin-engine/tests/fallback_histogram.rs | grep -cE '^\s*\('   # 231
```
