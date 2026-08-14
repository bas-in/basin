---
title: "DF removal — the write path, and what it would take to open the gate"
nav_section: migration
sidebar_position: 21
summary: "DML is 0 of 15 served and the gate at executor.rs is deliberately shut. The write path turns out not to be mostly DataFusion — 7 mentions in 10,608 lines — so deleting DataFusion does not require the owned engine to own writes. This is the seam a storage-backed sink would need, why statement atomicity falls out of the catalog primitive that already exists, why the bridge's fallback contract is itself a double-write bug, and a sequencing that says which preconditions are a week and which are a quarter."
tags: [migration, datafusion, dml, storage, atomicity, oracles]
---

# 21 — The write path, and what it would take to open the gate

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [06](./06-scan-and-storage.md) covers the read side of storage;
[20](./20-oracles.md) covers what we test against. This document covers the one
thing neither does: **writes**, which are the last wall between this branch and
deleting `datafusion = "53"`.

It is a design, not an implementation plan with tickets. It exists because a
wrong answer on a write is unrecoverable rather than merely wrong: a SELECT that
returns the wrong number can be re-run once fixed, and an INSERT that wrote the
wrong number cannot.

**Every claim about current behaviour below is followed by the file and line it
was read from, or the command that produced it.** Where the answer is "this does
not exist", it says so rather than describing what it would do.

---

## 0. The standing measurement

From commit `3a11a4d4`, unchanged as of `d6302c1e`:

- The probe reads **193 served / 26 fallback of 231**, but **DML is 0 served of
  15**. The `2` the probe displays is an attribution artifact: two DataFusion DML
  implementations re-enter `sess.execute()` with helper SELECTs the owned engine
  serves, and `owned_engine_served_count()` is engine-global. No DML statement
  was ever offered to the bridge.
- The gate is at `crates/basin-engine/src/executor.rs:2098`, which reaches
  `crate::owned_engine::try_execute` only inside
  `if matches!(kind, crate::pg_ast::StmtKind::Select)`.

Opening that `matches!` today changes nothing, for a reason that is worth
restating precisely because it is easy to mis-state:
`crates/basin-engine/src/owned_engine.rs:1060` calls
`basin_exec::build::build_in_session(&plan, &resolver, None, …)` — the third
argument is the `Option<&dyn DmlResolver>`, and it is `None`. Every DML plan
therefore dies at `build.rs:945` / `:983` / `:1048` with
`BuildError::Unsupported("INSERT (no write resolver configured)")`. The only
`DmlResolver` in the repository is `MemDmlResolver` (`build.rs:274`) and the
only `RowSink`s are `MemoryRowSink` (`dml.rs:203`) and `SharedMemoryRowSink`
(`build.rs:228`), both backed by `HashMap`.

A grep census of `crates/basin-exec/src/dml.rs` — the file that would do the
writing — for the words that matter, run for this document:

| term | hits | what they are |
|---|---|---|
| `wal` | 0 | — |
| `commit` | 0 | — |
| `rls` | 0 | — |
| `generated` | 0 | — |
| `trigger` | 0 | — |
| `transaction` | 0 | — |
| `atomic` | 0 | — |
| `rollback` | 0 | — |
| `snapshot` | 2 | `MemoryRowSink::snapshot()`, a test helper (`dml.rs:187`) |
| `constraint` | 1 | a word inside an error string (`dml.rs:324`) |
| `sequence` | 1 | "a fixed sequence of pre-built batches", test doc (`dml.rs:556`) |
| `identity` | 1 | "a row's identity", module doc (`dml.rs:44`) |

Every non-zero hit is incidental. The file has no write-path semantics in it at
all, and that is by design — see §2.

---

## 1. What the DataFusion write path actually does today

### 1.1 It is not, mostly, DataFusion

This is the single most important correction in this document, because the whole
program has been calling it "the DataFusion write path" and that name has been
setting expectations about how much work replacing it is.

Measured (`grep -c "datafusion\|SessionContext\|DataFrame\|ctx.sql"`):

| file | lines | DataFusion mentions |
|---|---:|---:|
| `basin-engine/src/dml_mutate.rs` | 10,608 | 7 |
| `basin-engine/src/dml.rs` | 4,605 | **0** |
| `basin-engine/src/constraints.rs` | 2,549 | 4 |
| `basin-engine/src/generated_cols.rs` | — | 6 |
| `basin-engine/src/rls.rs` | — | 20 |
| `basin-engine/src/hot_tombstone.rs` | — | (an entire `ExecutionPlan` impl) |

The write path is overwhelmingly Basin-owned code. DataFusion appears in it in
exactly three roles, and each one is a *string-SQL expression evaluator*:

1. **CHECK constraints.** `constraints::enforce_check_constraints`
   (`constraints.rs:1623`) builds a fresh `SessionContext`, registers the batch
   as a `MemTable` named `t`, and runs `SELECT ({predicate}) AS ok FROM t`
   (`constraints.rs:1663`). The predicate is stored in the catalog as *text*.
2. **Generated columns.** `generated_cols::materialise_generated_columns`
   (`generated_cols.rs:88`–`:112`) does the same thing with the expression text
   from the column's `BASIN_GENERATED_AS` field metadata (`types.rs:151`).
3. **RLS.** `rls.rs` both injects `datafusion::logical_expr::Expr` predicates
   into the read plan (`rls.rs:481`, `:495`) and evaluates `WITH CHECK`
   predicates on the post-image (`rls::enforce_with_check`, `rls.rs:749`).

Plus a fourth role that is not evaluation: **`hot_tombstone::TombstoneFilterExec`
is a DataFusion `ExecutionPlan`** (`hot_tombstone.rs:20`, `:37`–`:47`). It is
the read-side half of the DELETE hot-tier fast path. There is no owned-engine
equivalent — see §1.5.

The consequence for sequencing is large and is stated up front: **re-hosting
CHECK / generated / RLS-`WITH CHECK` expression evaluation off DataFusion is
required to delete DataFusion whether or not the owned engine ever writes a
row.** It is not a write-path precondition. It is a removal-surface item that
the write path happens to need too.

Two further shape facts, both from code:

- The incumbent DML path is driven by **`sqlparser::ast::Statement`**
  (`executor.rs:2720`, `:3956`, `:3975`; `dml.rs:26` imports `sqlparser::ast`),
  while the owned path is driven by **`pg_query` protobuf nodes**
  (`owned_engine.rs:1033`). Two parsers for the same statement text. Literal
  parsing, error text and edge-case acceptance are not guaranteed to agree, and
  nothing today compares them.
- `dml_mutate.rs` re-enters `sess.execute()` / `exec_select` at **13 sites**
  (`grep -c "sess.execute\|session.execute\|exec_select"`). That is the
  mechanism behind the probe's phantom `2`: those helper SELECTs are already
  being served by the owned engine.

### 1.2 The INSERT pipeline, in order

`INSERT` does not live in `dml_mutate.rs`. It lives in `executor.rs`:
`exec_insert` (`executor.rs:6795`), `exec_insert_prebuilt` (`:7117`),
`exec_insert_select` (`:7482`), `exec_insert_default_values` (`:7978`).

Read from `exec_insert` and `exec_insert_prebuilt`, the order is:

1. **Column-list expansion.** `expand_insert_rows` (`executor.rs:9991`, called at
   `:6898`) widens the user's row to schema width, rejects direct writes to a
   generated column (`:10011`), and inserts NULL slots at generated positions.
2. **DEFAULT / identity / sequence.** `apply_column_defaults`
   (`executor.rs:10112`, called at `:6921`) stamps every column carrying
   `BASIN_COLUMN_DEFAULT` metadata (`types.rs:181`) that the user omitted, and
   dispatches `nextval('seq')` defaults to `Catalog::nextval` **once per row**
   (`executor.rs:10143`, `:10191`). Identity columns are the same mechanism via
   `BASIN_IDENTITY` / `BASIN_IDENTITY_SEQ` (`types.rs:164`, `:172`).
   **This function is `async`** — that is load-bearing for §2.
3. `generated_cols::materialise_generated_columns` — DataFusion (`:7124`).
4. `type_ddl::enforce_enum_labels`, `type_ddl::enforce_domain_checks` (`:7130`,
   `:7132`).
5. `constraints::enforce_check_constraints` — DataFusion (`:7139`).
6. `rls::enforce_with_check` on the post-image (`:7150`).
7. FK: drain any referenced table's shard tail so the FK scan sees a consistent
   cold base (`:7166`–`:7180`), then `constraints::enforce_fk_on_insert`
   (`:7183`).
8. `constraints::enforce_pk_on_insert` (`:7191`) — a full-table scan, memoised by
   `PkSetCache` (`constraints.rs:73`).
9. `constraints::enforce_unique_on_insert` (`:7203`).
10. `promoted_columns::materialize_promoted_columns` (`:7218`).
11. **The write itself**, three mutually exclusive branches:
    - **Shard path** (`:7235`, when a shard is configured and no explicit
      transaction is open): `write_batch_striped` appends to the shard's WAL and
      acks once durable; the background compactor drains into a data file and
      commits through the catalog later. The code is explicit that it does *not*
      call `append_data_files` itself, because that would race the compactor
      (`:7222`–`:7225`). Then `write_through_insert_residency` (`:7261`) keeps
      the rows resident in the hot tier as clean entries for read-your-own-write,
      and `table_meta_cache.invalidate` (`:7269`) forces the next SELECT to
      re-read the catalog.
    - **In-transaction path** (`:7296`): `enforce_intra_tx_uniqueness`, a lazy
      WAL `Begin`, and `tx_htap_push_batch` into a session-local buffer. Nothing
      reaches storage until COMMIT.
    - **Legacy synchronous path**: write a data file, then commit it through the
      catalog.
12. **The tag.** `Ok(ExecResult::Empty { tag: format!("INSERT 0 {row_count}") })`
    (`:7276`, `:7333`) — or, when `RETURNING` is present,
    `Ok(ExecResult::Rows { … })` (`:7327`) with **no tag at all**. See §1.6.

### 1.3 UPDATE and DELETE

`exec_update` (`dml_mutate.rs:3539`) and `exec_delete` (`dml_mutate.rs:1273`).
Both are heavily forked into fast paths and a slow path, gated by a set of
"is this table simple enough" predicates: no soft-delete column
(`dml_mutate.rs:710`, `:1497`, `:2262`), no AUTO_UPDATE columns (`:2277`), no
per-table UPDATE/DELETE reactor (`:722`, `:2363`), no referencing FK (`:2374`),
no RLS, no audit, no generated columns, no secondary index (`:823`).

Two removal representations exist and they are not equivalent:

- **Hot-tier tombstone.** `exec_delete`'s `BASIN_HOTTIER_DELETE_FASTPATH` writes
  `MemRowValue::Tombstone` entries into the process-wide `MemTableRegistry`,
  keyed by an encoded PK `RowKey`, and skips the cold rewrite entirely
  (`hot_tombstone.rs:4`–`:9`; `dml_mutate.rs:800`–`:892`). An UPDATE analogously
  parks a full-row `MemRowValue::Update` override.
- **Cold copy-on-write.** The slow path rewrites the affected data files without
  the removed rows and publishes the swap through the catalog
  (`dml_mutate.rs:7125`).

Iceberg-style merge-on-read deletion vectors and positional deletes are
explicitly out of scope (`dml_mutate.rs:39`–`:41`), and **no `DeleteFile` type
exists in `basin-catalog` or `basin-storage`.** There is no third option to
inherit.

### 1.3.1 The WAL is on the INSERT path only, and the overlay is not durable

Worth stating separately because "share the incumbent's machinery" would
otherwise quietly inherit a hole.

`basin-wal` is a per-`(project, partition)` append-only log of Arrow-IPC
payloads (`basin-wal/src/lib.rs:87`–`:137`). It has **exactly one append site
outside its own crate**: `InProcessProjectHandle::write_batch_inner`
(`basin-shard/src/in_process.rs:5567`–`:5622`), reached from `Shard::write_batch*`
— i.e. from `INSERT` (`executor.rs:7245`) and a handful of internal writers
(`cagg.rs`, `lifecycle.rs`, `constraint_union.rs`). Replay
(`basin-wal/src/lib.rs:199`, driven from `basin-shard/src/in_process.rs:5979`)
reconstructs the shard tail from INSERT batches.

**UPDATE and DELETE are not WAL-logged.** The fast path is stated in code to be
"registry-only — no WAL" (`dml_mutate.rs:896`–`:897`), and the cold path's
durability is the catalog `Replace` commit rather than the log. The engine's own
docs are blunt about the consequence: overlay entries "are NOT WAL-logged; a
crash loses every committed-but-unmaterialized overlay write"
(`overlay_reconcile.rs:11`–`:13`, and `basin-engine/src/lib.rs:1017`–`:1030`),
bounded by `BASIN_OVERLAY_RECONCILE_SECS`, default 5 seconds. An acked DELETE can
resurrect its rows after a SIGKILL.

A clean INSERT-residency row, by contrast, *is* durable before it is promoted:
`write_through_insert_residency` (`executor.rs:14126`) runs only after the shard
WAL ack, so it is pure read cache.

One more thing that is not wired: `basin-hottier`'s `FlushTask`
(`basin-hottier/src/flush.rs`) has no production `FlushBackend` implementation —
the only two are test doubles (`flush.rs:735`, `:959`). And
`basin_hottier::merge::merge_scan` (`merge.rs:65`) is dead on the production
path; the real merge is `HtapUnionTable::scan` (`session.rs:4262`–`:4340`),
which stacks `TombstoneFilterExec` and `UpdateOverlayExec` over a `UnionExec` of
cold and hot. Both are DataFusion `ExecutionPlan`s.

### 1.4 How a write becomes visible

The commit primitive is one call on the `Catalog` trait
(`basin-catalog/src/lib.rs:593`):

```rust
async fn replace_data_files(
    &self, project, table,
    expected_snapshot: SnapshotId,
    removed_paths: Vec<String>,
    added_files: Vec<DataFileRef>,
) -> Result<TableMetadata>;
```

Documented as "atomically replace the data file set: result =
(parent.data_files − removed_paths) ∪ added_files" (`lib.rs:580`–`:592`).
**Adds and removes go in one call and produce one new snapshot.** Append-only
writes use `append_data_files(…, expected_snapshot, files)` (`lib.rs:567`).

Concurrency is **optimistic, not locked and not single-writer**: the caller
passes the snapshot id it last read; a stale id returns
`BasinError::CommitConflict`, and `dml_mutate.rs:7092`–`:7125` deliberately
propagates that all the way up to the router's `execute_with_conflict_retry`
(`basin-router/src/protocol.rs:2085`) so the whole statement re-evaluates its
`WHERE`. Swallowing it lower down previously let two writers of
`WHERE version = $3` both win.

Liveness is **snapshot-chain replay**, not a flag:
`TableMetadata::live_data_files()` (`basin-catalog/src/metadata.rs:665`)
delegates to `live_data_files_at(current_snapshot)` (`:689`), which walks the
retained snapshot chain from genesis, applying each snapshot's `removed_paths`
and `data_files` to a map. There is no `superseded` column, no `expires_at`, no
per-file version. A file is live iff some snapshot ≤ head added it and no later
snapshot ≤ head removed it.

**This is the invariant commit `553f4f8b` had to re-establish.** With
`BASIN_OWNED_ENGINE=1`, `storage_source` read through `Storage::read`, which
LISTs the table prefix — and *existence is not liveness*. `basin-shard` retains
superseded compaction inputs for `superseded_delete_grace()`, 300 s by default
(`basin-shard/src/in_process.rs:335`), so for five minutes after every
compaction the owned scan returned both pre- and post-update values:
`[(1,10),(1,11),(2,20),(2,20),(3,30),(3,30)]` where the truth was
`[(1,11),(2,20),(3,30)]`, and `count(*)` = 6 where the truth was 3. Dedup could
not have saved it — the stale value is a genuine row that was genuinely written
once. The fix moved the file set to `live_data_files()`, pinned once per
statement in `owned_engine.rs:1258`–`:1271`.

The counterpart hazard is `bc57fa48`: a file exists on the object store the
moment `write_batch` returns, but is not in the catalog until its commit lands,
so a purely catalog-sourced view *loses* rows in that window.
`Storage::note_uncommitted_file` (`basin-storage/src/lib.rs:1796`) exists for
exactly that gap. `owned_engine.rs:1243`–`:1247` names both bugs and states the
rule that makes `live_data_files()` safe at the bridge and unsafe inside
`Storage::read`: **the engine commits every write before making it visible.**

> **The invariant any owned write path inherits.** Visibility is decided by one
> function, `live_data_files()`, over one chain, published by one call,
> `replace_data_files`. A second implementation of visibility is precisely how
> `553f4f8b` happened. The owned write path must reach storage through
> `Catalog::append_data_files` / `Catalog::replace_data_files` and nothing else.

One honest caveat on "atomic": in `ObjectStoreCatalog`, when removed paths span
multiple chains (the META chain plus per-partition segments),
`replace_data_files` (`object_store_catalog.rs:3245`–`:3300`) degenerates into
several CAS commits with compensating undo, and if the undo also fails it logs
"those rows are DUPLICATED (count > distinct). Manual reconciliation required."
(`:5054`, `:5095`). Multi-chain replace is atomic-by-compensation, not atomic.

### 1.5 What the write path does *not* do

**Triggers do not exist and cannot exist.** `CREATE TRIGGER` and
`CREATE CONSTRAINT TRIGGER` are rejected with SQLSTATE 0A000 at
`pg_ast.rs:1490`–`:1502`: "Basin has no PL/pgSQL trigger runtime (ADR 0012)."
`DROP TRIGGER IF EXISTS` is a no-op specifically because "no trigger can ever
exist in Basin" (`pg_ast.rs:1504`). The `3a11a4d4` precondition list names
"triggers"; **as a Postgres feature that precondition is zero work.**

It is the wrong name for a real precondition, though. Basin has *declarative*
write-path effects that a trigger would otherwise carry, and there are more of
them than Postgres has: soft-delete columns, AUTO_UPDATE columns, `AUDIT TO`,
`REACT ON` reactors, secondary-index maintenance, and promoted-JSONB shadow
columns. A census of the distinct engine modules `dml_mutate.rs` calls into
(`grep -o "crate::[a-z_]*::" | sort | uniq -c`):

```
19 crate::session::      13 crate::index_probe::    4 crate::secondary_index::
17 crate::types::        13 crate::constraints::    4 crate::rls::
14 crate::hot_tombstone:: 7 crate::executor::       4 crate::generated_cols::
 4 crate::convert::       3 crate::udf::            3 crate::promoted_columns::
 3 crate::dml::           2 crate::lifecycle::      2 crate::fast_select::
 1 crate::sql_functions:: 1 crate::pk_row_cache::   1 crate::events::
```

Nineteen modules. That is the real size of the write-path surface, and it is the
number that should be quoted instead of "triggers".

### 1.6 The command tag, measured

`ExecResult` has two variants (`basin-engine/src/lib.rs:2331`): `Empty { tag }`
and `Rows { schema, batches }`. `Rows` carries **no tag**.

The simple query protocol (`basin-router/src/protocol.rs:395`–`:409`) emits
`tag` verbatim for `Empty`, and unconditionally `format!("SELECT {row_count}")`
for `Rows`. The extended protocol (`:2136`–`:2141`) patches this up by sniffing
the SQL verb at Parse time (`entry.dml_tag_prefix`, `:584`–`:586`) and emitting
`INSERT 0 {emitted}` / `{verb} {emitted}`.

Two conclusions, and they are different:

- **`INSERT … RETURNING` over the *simple* protocol already answers
  `SELECT 1` where PostgreSQL answers `INSERT 0 1`.** That is a pre-existing
  divergence shared with the incumbent, not something the owned path would
  introduce. Verified against live PostgreSQL 18.2: `INSERT INTO atom VALUES
  (98,1) RETURNING id` returns the row and the tag `INSERT 0 1`.
- **`try_execute_inner` returning `ExecResult::Rows` unconditionally
  (`owned_engine.rs:1088`) is a *new* break**, and worse than a wrong tag: for an
  INSERT with no `RETURNING`, the simple protocol would emit a `RowDescription`
  and a `SELECT 0` for a statement that must produce neither. Under the extended
  protocol it would emit `INSERT 0 0` — a tag with the right shape and the wrong
  number, which is the more dangerous of the two because a driver will believe it.

And the count that would fix it is currently unreachable. `Insert::affected_rows`
(`dml.rs:307`), `Update::affected_rows` (`:449`) and `Delete::affected_rows`
(`:519`) exist, but the operator is immediately boxed as `Box<dyn Operator>`
(`build.rs:962`, `:1029`, `:1069`) and wrapped by `wrap_returning`. `Operator`
(`operator.rs:114`) has `schema` and `next_batch` and nothing else.
`grep -rn affected_rows crates/` returns **20 hits, all inside `dml.rs`, and 15
of them are assertions in that file's own unit tests**. Zero production callers.

> This is the "harness that appears to run but compares nothing" class from §7,
> in its purest form: five tests assert the affected count is right, and no
> production code path can ever read it.

### 1.7 What on the write path actually depends on DataFusion — and what does not

This is the section that should change the plan, so it is stated as two lists.

**Load-bearing DataFusion on the write path.** Every item is a
`SessionContext` + `MemTable` + `ctx.sql(…)` round-trip, or a DataFusion
`ExecutionPlan`:

1. `INSERT … SELECT` row production — `executor.rs:7581`, `:7585`.
2. CHECK constraint evaluation — `constraints.rs:1637`–`:1663`.
3. GENERATED column and `UPDATE … SET <expr>` evaluation — `generated_cols.rs:87`–`:140`,
   shared by the cold UPDATE and the hot-tier RMW fast path (`dml_mutate.rs:7343`).
4. `RETURNING` projection — `dml_mutate.rs:7638`–`:7657`.
5. Subquery resolution in `WHERE` / `SET` RHS — `dml_mutate.rs:108`, `:169`.
6. Correlated `EXISTS` decorrelation — `exec_delete_via_df_rowset`
   (`dml_mutate.rs:4950`), `exec_update_via_df_rowset` (`:5137`).
7. `UPDATE … FROM` / `DELETE … USING` joins — `dml_mutate.rs:4486`, `:4317`.
8. `MERGE`'s match computation — the single join in `merge.rs`.
9. The read-side overlay nodes — `TombstoneFilterExec` (`hot_tombstone.rs:352`),
   `UpdateOverlayExec` (`:500`), `HtapUnionTable` (`session.rs:4227`).
10. RLS predicate injection on SELECT — `rls.rs:481`, `:495`.

**Basin-owned and reusable as-is — no DataFusion anywhere in it:**

1. `basin-engine/src/dml.rs` entire (4,605 lines): literal-`Expr` → Arrow
   coercion, ~60 per-type coercers, `check_null_allowed`, `enforce_charlen_array`.
2. `values_fast.rs`: the hand-written `VALUES` tokenizer straight to Arrow.
3. The **compound-predicate engine** — `parse_compound_predicate`
   (`dml_mutate.rs:8243`), `parse_atom` (`:8364`), `file_outcome` (`:5757`).
   Basin's own `WHERE` evaluator for the cold rewrite.
4. The whole hot-tier overlay — `basin-hottier`, `hot_tier_delete_by_pk`
   (`dml_mutate.rs:878`), `hot_tier_update_by_pk` (`:2795`),
   `write_overlay_post_images` (`:3264`), `materialize_overlay_for_table` (`:6116`).
5. The whole cold copy-on-write machinery — `write_replacement_per_file`
   (`dml_mutate.rs:6944`), `commit_replace` (`:7095`), `filter_to_live_data_files`
   (`:6605`).
6. The whole catalog commit protocol — `SnapshotId`, `append_data_files`,
   `replace_data_files`, OCC.
7. `basin-storage` write/read, `basin-shard` WAL ingest, `basin-wal`.
8. PK / UNIQUE / FK scanning in `constraints.rs` (all of it except CHECK),
   `index_probe`, `secondary_index`, the GIN/GIST maintainers.
9. `prepared.rs`'s bind-direct paths — `try_insert_bind_direct`
   (`executor.rs:6510`), which builds batches from decoded bind values with no
   SQL text and no AST at all.

> **The write *mechanics* are already DataFusion-free.** What is not free is
> **expression evaluation** (items 2–5), **set-oriented source computation**
> (items 1, 6–8) and the **read-side overlay plan** (item 9). §8 takes this
> seriously, because it means deleting DataFusion does not require the owned
> engine to own the write path.

Also worth recording, because it is the storage format the docs sometimes get
wrong: the default on-disk format is **Vortex**, not Parquet — `FileFormat`'s
`#[default]` is `Vortex` (`basin-storage/src/writer.rs:103`–`:107`), overridable
per table via `meta.file_format` (`executor.rs:10502`).

---

## 2. The seam

### 2.1 Correcting the premise

The brief for this document said `basin-exec` "deliberately has no storage or
engine dependency". Half of that is right. Read from
`crates/basin-exec/Cargo.toml`:

- It **does** depend on `basin-storage`, and the manifest says so explicitly:
  "`storage_source` is the one module allowed to depend on `basin-storage` …
  see that module's docs for why the dependency is confined there instead of
  leaking into `scan.rs` or `build.rs`."
- It **does not** depend on `basin-catalog`, and it does not depend on
  `basin-engine`. Its dependency list is `basin-pgtype`, `basin-plan`,
  `basin-common`, `basin-storage`, the arrow family, `chrono`, `tokio`,
  `futures`, `object_store`.

So the rule is not "no storage". The rule is **"one module owns the storage
dependency, and `build.rs` negotiates with traits."** That is a rule the write
side can follow exactly as the read side does — and it is also the rule that
says the seam cannot be "`dml.rs` calls the catalog", because the catalog is not
in the graph and putting it there would pull `basin-catalog` into `build.rs`'s
world.

### 2.2 Why `RowSink` as written is the wrong shape

> **Amended — `dml.rs` is two files, and this document uses the name for both.**
> Every `dml.rs:NNN` citation in this section is
> `crates/basin-**exec**/src/dml.rs` — the **owned** engine's sink, which serves
> no writes today. §7.3a's `dml.rs` citations are
> `crates/basin-**engine**/src/dml.rs`, the shipping path. Reading them as one
> file cost a wasted agent brief (see commit `33406d6f`): two bugs this section
> motivates P2 with were reported as defects in the *shipping* write path, and
> neither reproduces there — the incumbent's fast path declines any assignment
> touching the primary key, so a PK rewrite always takes cold copy-on-write and
> re-derives keys from the post-image. There is no "project the key from the new
> batch" bug to fix in the code that serves writes today.
>
> That does **not** weaken the argument for reshaping `RowSink` — it is still the
> wrong shape for the owned engine to grow a write path on. It does mean this
> section is describing a design defect in code that has never run, not a live
> corruption, and P2 should be scoped from that reading.
>
> The live corruption `33406d6f` did find is in the shipping path and is
> unrelated to `RowSink`: `check_update_pk` was set-based where PostgreSQL is
> row-at-a-time, so `UPDATE t SET id = id + 1` answered `UPDATE 2` where
> PostgreSQL raises `23505`.

`RowSink` (`dml.rs:126`) is:

```rust
pub trait RowSink {
    fn insert(&mut self, batch: &RecordBatch) -> Result<u64, ExecError>;
    fn delete(&mut self, keys: &RecordBatch) -> Result<u64, ExecError>;
}
```

Four properties make it unusable against real storage as it stands.

**(a) It is synchronous, and every storage and catalog call is `async`.**
`Operator::next_batch` is sync by construction, and the trait is deliberately
not `Send` (`operator.rs:60`–`:61`). A sink cannot `.await` a `put` or a
`replace_data_files` from inside `next_batch`.

**(b) It commits per batch.** `Insert::next_batch` calls `self.sink.insert(&batch)`
once per input batch (`dml.rs:390`–`:394`). If batch 2 fails, batch 1 is already
written. `3a11a4d4` said this; it is confirmed.

**(c) It has no notion of what a removal *is*.** `delete` takes a key batch.
Against `MemoryRowSink` that is a `HashMap::remove`. Against cold storage a
removal is either a tombstone keyed by an encoded PK (`hot_tombstone.rs`) or a
copy-on-write rewrite of the specific files that contained those rows — and the
sink is handed neither the file provenance nor the row positions it would need
for the second. **The owned scan does not produce row identity.** Adding it is a
physical-plan change, not a sink change; see §4.4.

**(d) Its `Update` decomposition is wrong in a way that silently corrupts.**
`Update::next_batch` (`dml.rs:465`–`:473`) projects `key_cols` out of the **new**
batch, deletes those keys, then inserts the new rows. For
`UPDATE w SET id = id + 1000 WHERE b = 'x'` on a table keyed by `id`, that
deletes key `1001` (which does not exist), inserts `1001`, and **leaves the old
row `1` in place** — one row becomes two, no error. Live PostgreSQL 18.2 answers
`UPDATE 1` and leaves one row. The module doc concedes the case ("an `UPDATE`
that rewrites its own primary key is not modelled at this layer", `dml.rs:50`)
but the operator does not refuse it; it produces a wrong answer.

The mirror-image case is also wrong and in the opposite direction. On live PG:

```
INSERT INTO w (id,a,b) VALUES (1,1,'p'),(2,2,'q');
UPDATE w SET id = id + 1 WHERE id IN (1,2);
ERROR:  duplicate key value violates unique constraint "w_pkey"
DETAIL:  Key (id)=(2) already exists.
```

Basin's delete-then-insert deletes `{1,2}` and inserts `{2,3}`, succeeding
silently where PostgreSQL errors.

The decomposition is not even what the incumbent does. `dml.rs:57`–`:63` claims
delete-then-insert is "the same decomposition many MVCC stores use natively".
Basin's hot-tier UPDATE is **not** that: `write_overlay_post_images`
(`dml_mutate.rs:3264`, `:3313`, `:3322`) writes a `MemRowValue::Update` — a
**full-row replacement blob keyed by PK** (`basin-hottier/src/memtable.rs:118`)
that shadows the stale cold row on read. One keyed override, not a removal
followed by an insertion. That representation is immune to both failures above,
because the key never moves and the old row is never separately reachable.
**`RowSink` should grow an `update(key, row)` rather than keep the comment
explaining why it deliberately has none** (`dml.rs:57`).

### 2.3 The seam this design proposes

Three changes, in the order they constrain each other.

**(i) `RowSink` grows a `finish`, and stops meaning "write".** Rename the
contract in the docs even if the method names stay: `insert`/`delete` mean
*stage*, not *write*. Add:

```rust
/// Everything staged since the sink was opened, as one publishable unit.
/// Returns the affected-row count. Never partially applied.
fn finish(&mut self) -> Result<StagedWrite, ExecError>;
```

`StagedWrite` is Arrow-shaped and storage-shaped but not catalog-shaped: the
added rows as `RecordBatch`es, and the removals as whatever the removal
representation turns out to be (§4.4). It contains no object paths and no
snapshot ids — those belong to the caller.

**(ii) The bridge, not the operator, does the I/O.** `try_execute_inner` already
has the right shape for this: a synchronous drain loop with no `.await` inside
it (`owned_engine.rs:1069`–`:1086`), sitting inside an `async fn`. The write
sequence becomes:

```
  async   build_resolver                      (already exists, already async)
  async   resolve DEFAULT / identity / nextval (§2.4 — must be async)
  sync    build_in_session(&plan, &tables, Some(&dml), …)
  sync    drain the operator tree              (stages into the sink)
  async   sink.finish() → StagedWrite
  async   enforce constraints / RLS WITH CHECK on the staged post-image
  async   encode + write data file(s) to storage
  async   ONE Catalog::append_data_files / replace_data_files
  →       ExecResult::Empty { tag } or Rows + the affected count
```

The sink handle is held by the bridge, exactly the way `MemDmlResolver` already
hands its caller an `Rc<RefCell<MemoryRowSink>>` back from `insert_table`
(`build.rs:263`) precisely because "`RowSink` itself is consumed by value into
the operator tree, so nothing else can reach it there" (`build.rs:221`–`:223`).
**That existing pattern is the answer to the unreachable-`affected_rows`
problem** — the count comes off the handle, not through `Operator`. No trait
change to `Operator` is needed, and none should be made.

**(iii) `basin-exec` gains nothing new.** No catalog dependency, no async in
`Operator`. The sink implementation that stages into memory can live in
`basin-exec` beside `MemoryRowSink`; the implementation that turns a
`StagedWrite` into files and a catalog commit lives in `basin-engine`, where the
catalog already is. That respects the manifest's stated rule and keeps the
`build.rs` seam trait-shaped.

### 2.4 What must move to the bridge, and why it cannot stay in lowering

Three things `basin-plan` currently refuses, all for the same reason, and all of
which resolve at the bridge rather than in lowering:

- `INSERT … DEFAULT VALUES` (`lower/dml.rs:913`)
- `VALUES (DEFAULT, …)` (`lower/dml.rs:854`, `:863`)
- `SET col = DEFAULT` (`lower/dml.rs:510`)
- `RETURNING *` / `RETURNING <omitted col>` on a partial column list
  (`lower/dml.rs:939`–`:989`)

Each refusal says the same thing: "no column-default catalog available to
lowering". The comment at `lower/dml.rs:878` is precise about why —
`TableResolver` (`lower/select.rs:113`) is
`fn resolve_table(&self, name: &[String]) -> Option<(TableId, Schema)>` and
`Schema` is `Vec<(String, PgType)>` (`basin-plan/src/lib.rs:46`). Names and
types, nothing else.

**But the default catalog does exist.** It is on the Arrow `Field` metadata:
`BASIN_COLUMN_DEFAULT` holds the default's source text (`types.rs:181`),
`BASIN_IDENTITY` / `BASIN_IDENTITY_SEQ` hold identity mode and backing sequence
(`types.rs:164`, `:172`), `BASIN_GENERATED_AS` holds the generated expression
(`types.rs:151`). And `build_resolver` already has the full
`meta.schema` in hand at `owned_engine.rs:1221`; it throws the metadata away
when it flattens to `(name, PgType)` pairs at `:1225`.

The tempting move is to widen `TableResolver` to carry defaults. **Do not.**
`apply_column_defaults` is `async` (`executor.rs:10112`) because a
`nextval('seq')` default calls `Catalog::nextval` **once per row**
(`:10143`, `:10191`) — a sequence is shared mutable state with an allocation
side effect, and it is not a pure expression that lowering can inline. Making
lowering async to accommodate it would be a far larger change than the problem
warrants, and it would put a side effect inside a pure pass.

The design instead resolves defaults **before lowering, in the bridge**, and
hands lowering a plan whose `VALUES` rows are already complete. Concretely: the
bridge walks the parsed `InsertStmt`, and for each column the statement omits or
sets to `DEFAULT`, substitutes a literal — evaluating `nextval` against the
catalog, and constant expressions through `basin_exec::eval`. The three
`lower/dml.rs` refusals then simply stop being reachable, and lowering stays
sync, pure, and catalog-free.

This also fixes a second problem for free. `build.rs:951` requires
`input_op.schema() == write_schema` exactly, and a `VALUES` relation's columns
are named `column1`, `column2`, … (`lower/select.rs:1785`, asserted at `:4226`).
`Schema` equality in Arrow compares field names, so **`INSERT INTO t VALUES
(1, 2)` cannot build today even with a resolver wired**, because the schemas
never match. Any narrow gate (§5) has to solve this regardless; resolving
defaults into a bridge-built row set solves it at the same time, by constructing
the input with the target table's own field names.

### 2.5 The seam, stated in one paragraph

> An owned DML plan hands rows to storage through a `RowSink` that **stages**
> rather than writes, in `basin-exec`, with no async and no catalog. The bridge
> in `basin-engine` holds the sink handle, drains the operator tree
> synchronously, takes one `StagedWrite`, runs the existing enforcement
> functions over it, writes the data file, and publishes it with exactly one
> `Catalog::append_data_files` / `replace_data_files` call. Row counts come off
> the handle, not through `Operator`. Defaults, identity and `nextval` are
> resolved in the bridge before lowering, because they are async and have side
> effects. `basin-exec` acquires no new dependency.

---

## 3. Statement-level atomicity

### 3.1 What PostgreSQL actually does — measured, not recalled

Run against `postgres://pc@127.0.0.1:5432/postgres`, PostgreSQL 18.2, for this
document:

```sql
CREATE TABLE atom (id int primary key, v int check (v < 100));
INSERT INTO atom SELECT g, g FROM generate_series(1,5) g;          -- INSERT 0 5
INSERT INTO atom SELECT g, g FROM generate_series(10,200) g;
-- ERROR:  new row for relation "atom" violates check constraint "atom_v_check"
-- DETAIL:  Failing row contains (100, 100).
SELECT count(*) FROM atom;                                          -- 5
```

Rows 10..99 satisfied the CHECK and were processed before row 100 failed. **None
of them survived.** The same holds for a mid-statement NOT NULL violation 1,000
rows into a 2,001-row `INSERT … SELECT` (count after: 0 of the new rows), and for
`UPDATE atom SET v = v * 50`, which failed on the second row and left all five
rows at their original values.

Two more measured rules the design must carry:

- A statement that conflicts **with itself** errors:
  `INSERT INTO w (id,a) VALUES (100,1),(100,2)` →
  `duplicate key value violates unique constraint "w_pkey"`, zero rows written.
- `ON CONFLICT DO UPDATE` that would touch one row twice in one command errors
  rather than applying twice: `ON CONFLICT DO UPDATE command cannot affect row a
  second time`.

And the tags, measured: `INSERT 0 1`; `UPDATE 1`; `DELETE 1`; `UPDATE 0` for no
match; `INSERT 0 0` for `ON CONFLICT DO NOTHING` that skipped.

### 3.2 What Basin can offer, and where it comes from

The good news is that the primitive already exists and is exactly the right
shape. `replace_data_files(project, table, expected_snapshot, removed_paths,
added_files)` takes adds and removes **together** and produces **one** snapshot
(`basin-catalog/src/lib.rs:593`). If the owned path stages the entire statement
and publishes once, statement atomicity is not something to build — it is what
the primitive already does.

That is also why the design in §2.3 puts enforcement *after* the drain and
*before* the write: a constraint violation discovered at that point has nothing
to unwind, because nothing has been written. It matches what the router already
assumes: `execute_with_conflict_retry` (`protocol.rs:2085`) transparently
**re-executes the whole statement** on `CommitConflict`, which is only sound if
the failed attempt wrote nothing.

It is also, importantly, **what the incumbent already achieves for INSERT**.
`dml::batch_from_rows` (`dml.rs:46`) builds the entire batch and errors on the
offending row *before* any constraint check runs, and every `enforce_*` runs
against the whole batch before `write_batch_with_options`. The hot-tier UPDATE
fast path is likewise atomic — batched expression evaluation completes before the
first overlay write (`dml_mutate.rs:3130`–`:3136`), and a budget decline returns
before it too (`:3296`–`:3306`). So the design's stage-then-publish shape is not
a new invention; it is the incumbent's shape, named.

Three places where the incumbent is **not** atomic, which the owned path should
not copy:

- **After the catalog swap on the cold UPDATE/DELETE path**, index maintenance
  (`dml_mutate.rs:4165`–`:4272`), `delete_objects` (`:4274`), audit rows
  (`:4278`) and FK `CASCADE` child DELETEs (`:1899`, each its own independent
  statement and commit) all run post-commit with **no compensation**.
- **If `commit_with_retry` itself fails** on the auto-commit INSERT path
  (`executor.rs:7418`), the already-written data file is *not* deleted. The two
  earlier failure points do clean up (`:7387`–`:7397`, `:7405`–`:7415`); this one
  leaves an orphan. `commit_with_retry` retries exactly once (`:10559`–`:10574`).
- **Multi-table statements are not atomic at all**: `TRUNCATE`
  (`truncate.rs:27`–`:30`), `MERGE` (compiled to N independent statements), FK
  `CASCADE` chains.

And `INSERT … ON CONFLICT` is **probe-then-mutate**, not an atomic upsert:
`try_on_conflict_do_update` (`executor.rs:8857`) probes for existence with
generated SQL and then re-issues an `UPDATE` through the normal pipeline. It is
racy under concurrency by construction. An owned implementation that used
`ConflictAction::DoUpdate` (`dml.rs:265`) would be racy in the same way, and
would additionally not reproduce PostgreSQL's measured
`ON CONFLICT DO UPDATE command cannot affect row a second time` (§3.1).

### 3.3 The price, stated honestly

Staging the whole statement means **the statement's entire write set is held in
memory**. `Operator` is sync and not `Send`, so the sink cannot spill
asynchronously; there is no way to write a file mid-drain without either making
`Operator` async or handing the sink a channel to a `Send` writer task, and both
are larger changes than this design should smuggle in.

So the honest answer is: **bound it and refuse above the bound.** A staged write
that exceeds a byte budget returns `BuildError::Unsupported`-equivalent and
falls back to the incumbent, the same way `MAX_QUANTIFIED_SUBQUERY_ROWS`
(`build.rs:1997`) already bounds a materialised subquery. The bound is
principled rather than arbitrary — it is the exact price of statement atomicity
under a synchronous operator model — and it should be documented as such, not
buried as a constant.

This is a real capability gap against the incumbent, which streams a bulk
`INSERT … SELECT` through the shard WAL without ever holding it all
(`executor.rs:7246`). A 10 M-row `INSERT … SELECT` will fall back, and should.

### 3.4 The fallback contract is itself a double-write bug

This is the most urgent finding in the document and it is independent of
everything else.

`try_execute` returns `Option<ExecResult>` and returns `None` on **every**
failure, including `Fallback::Exec` — an error raised *during* execution
(`owned_engine.rs:305`–`:318`, `Fallback` at `:342`). The caller at
`executor.rs:2109` treats `None` as "this module was never called" and falls
through to the DataFusion path.

For a SELECT that is harmless. For a DML statement it is catastrophic: if the
owned path staged and wrote, then failed on a later step and returned `None`,
the incumbent re-executes the same statement from scratch and **writes it a
second time**.

The design's rule, which must land before any DML statement reaches the bridge:

> **Once a statement has published anything, `try_execute` may never return
> `None`.** The return type has to distinguish "declined, nothing happened, safe
> to retry elsewhere" from "attempted, something happened, this error is the
> answer". Everything up to and including `sink.finish()` is safely declinable;
> everything from the first storage write onward is not.

Mechanically that is a third state — `Option<Result<ExecResult>>`, or an enum
with `Declined` / `Served` / `Failed` — and it is a small change. It is small
*now*. It is small only while the answer to "has anything been written?" is
always "no".

**Landed.** `try_execute` returns an opaque `Outcome`
(`owned_engine.rs`), whose only accessor `into_result() -> Result<Option<ExecResult>, BasinError>`
renders "declined, nothing happened" as `Ok(None)` and "attempted, published,
then failed" as `Err`. Two things make it structural rather than documentary:

- The retry decision is not the call site's. `classify(reason, &effects)`
  reads a `SideEffects` ledger — a one-way latch carrying *what* was
  published — and can only return a retryable answer while that ledger is
  clean. The stage that raised the error is irrelevant to it, which is the
  correction this section asked for: a `Fallback::Exec` before the first write
  is retryable and a `Fallback::Build` after one is not.
- The safe call site is now the shortest one. `executor.rs:2109` reads
  `outcome.into_result()?`; retrying a failure would take a deliberately
  written `.ok()`, `unwrap_or`, or `Err(_) => {}`, and a unit test in
  `owned_engine.rs` pins the single call site against exactly those.

Routing is unchanged, and that too is structural rather than hoped for:
nothing on this path can publish (the bridge still passes `None` as
`build_in_session`'s `DmlResolver`), so `classify` returns `Declined` for
every fallback and `into_result()` yields the same `Ok(None)` where the old
signature yielded `None`. What P2 inherits is one rule with one call:
`SideEffects::note_published(what)` **before** the first storage write or
catalog commit. Everything from that call onward is un-retryable
automatically, with no change at the call site.

One thing this section did not raise, established while landing it: the
`exec_error` bucket cannot represent a partially-emitted result set either.
`try_execute_inner` accumulates batches into a local `Vec` and builds
`ExecResult::Rows` only after `next_batch()` returns `Ok(None)`, and
`protocol.rs:394`–`:409` emits `RowDescription`/`DataRow` only from a returned
`ExecResult`. A mid-drain failure therefore drops every accumulated batch
before the caller sees anything, so "the client already saw rows" is not a
state this bridge can reach — for reads, the retry was only ever wasted work.

### 3.5 Multi-statement transactions are out of scope, and already are

`try_execute` declines outright when `crate::session::tx_is_active(&sess.state)`
(`owned_engine.rs:295`–`:302`). That should stay, and the reason is stronger
than "not built yet".

There is no `TransactionState` type and no `begin_transaction` anywhere in
`crates/`. What exists is `TxState` (`basin-engine/src/session.rs:642`) with
`pre_tx_snapshots`, `read_snapshots`, `pending_files`, `savepoints`,
`tx_overlay`, `htap_rows`. `tx_rollback` (`session.rs:2348`–`:2385`) discards
in-memory buffers — genuinely free, because they were never shared. For anything
that already touched storage, `executor.rs:1732`–`:1755` does two best-effort
things with the result discarded via `let _ =`: delete each pending file, then
`catalog.rollback_to_snapshot(project, table, pre_tx_snap)`. That method's trait
default returns `Internal("rollback_to_snapshot not implemented for this catalog
backend")` (`basin-catalog/src/lib.rs:831`–`:841`), and where it is implemented
it works by **pruning snapshots with id > target** (`metadata.rs:656`–`:659`) —
a destructive rewind of the shared table head that also discards snapshots other
sessions committed after this BEGIN. And a failure mid-COMMIT is unrecoverable
by design: `executor.rs:1484`–`:1490` states that after a catalog append fails
"the session is now in a dirty state (files written but not catalogued)".

**Basin has statement atomicity available and transaction atomicity not
available.** The owned write path should offer the first and continue to decline
the second. Claiming otherwise would be the "code trusting a false document"
failure mode from §7, with the document being this one.

---

## 4. The eight preconditions, re-derived

`3a11a4d4` listed eight. Reading the code changes the list: one of them is
approximately zero, one of them is not a write-path item at all, and two items
that were not on the list are larger than several that were.

Sizes are rough person-weeks for one agent working the way this branch has been
worked, and they are estimates, not measurements. `blocks` means the gate cannot
open for the named shape without it.

### P0 — The fallback contract (§3.4). **LANDED. Blocked everything; no longer does.**

Change `try_execute`'s return type to distinguish declined from failed-after-
writing, and assert in a test that no code path can return `None` after a
publish. Independent of every other item; can land today while the gate is still
shut, and is *cheaper* to land now than later.

Done — see §3.4's "Landed" note for the shape. What P2 must not forget is one
line: `SideEffects::note_published(what)` before the first irreversible write.

### P1 — Re-host CHECK / generated / RLS-`WITH CHECK` evaluation off DataFusion. **~3 wk. Not a write-path precondition.**

`constraints.rs:1663`, `generated_cols.rs:112`, `rls.rs:749` each evaluate a
stored predicate *string* through `SessionContext::sql`. Replacing that means:
parse the stored text with `pg_query`, lower with `basin_plan::lower::expr`
against the batch's schema, evaluate with `basin_exec::eval`. All three share one
helper. **This must happen to delete DataFusion regardless of whether the owned
engine ever writes**, so it belongs on the removal-surface track
([18](./18-removal-surface.md)), not this one — but the owned write path cannot
enforce a CHECK without it, so it blocks P4.

Risk: the stored predicate text was written by DDL and parsed by *sqlparser*;
re-parsing with `pg_query` is a second parser change on the same string. Expect
divergences on exactly the expressions nobody writes tests for.

### P2 — A storage-backed `DmlResolver` + staging `RowSink` + one-shot commit (§2.3). **~2 wk. Blocks everything.**

`StagedWrite`, `finish()`, the bridge sequence, the memory bound, the handle that
carries the affected count. Depends on nothing but P0. This is the item the
`3a11a4d4` list called "a storage-backed RowSink and DmlResolver sharing the
DataFusion path's commit/visibility machinery", and the reading in §1.4 makes it
smaller than it sounded: the machinery to share is two catalog methods, not a
subsystem.

### P3 — Command tag and result shape (§1.6). **~0.5 wk. Blocks everything.**

`try_execute_inner` must return `ExecResult::Empty { tag }` when `RETURNING` is
absent, with the count read off the sink handle. Do **not** attempt to fix the
pre-existing simple-protocol `SELECT n` tag for `… RETURNING` in the same
change — that is an incumbent bug, it should be fixed on its own, and folding it
in here makes the owned path's diff impossible to read.

### P4 — Constraint, NOT NULL, PK/UNIQUE and FK enforcement. **~1.5 wk given P1. Blocks the general gate.**

The functions already exist and are already batch-shaped:
`enforce_check_constraints`, `enforce_fk_on_insert`, `enforce_pk_on_insert`,
`enforce_unique_on_insert` (`constraints.rs:1623`, `:2005`, `:224`, `:1103`),
`enforce_enum_labels` / `enforce_domain_checks` (`type_ddl`). The work is
calling them from the bridge on the staged post-image in the incumbent's order
(§1.2) and **not reimplementing any of them**.

The genuinely new piece is error plumbing. `ExecError` (`operator.rs:8`) has
`OutOfMemory`, `Cancelled`, `TypeMismatch`, `Overflow`, `DivisionByZero`,
`CardinalityViolation`, `Internal` — and **no constraint-violation variant at
all**, while `BasinError` has `UniqueViolation`, `CheckViolation`,
`ForeignKeyViolation`, `NotNullViolation`, `RlsViolation`
(`basin-common/src/error.rs:95`–`:127`) carrying the SQLSTATEs the wire needs.
Because enforcement runs in the bridge (where `BasinError` is available) and not
inside an operator, this is plumbing rather than a trait change — but it must be
deliberate, or every violation arrives at the client as SQLSTATE XX000.

### P5 — DEFAULT / identity / sequence resolution in the bridge (§2.4). **~1.5 wk. Blocks any INSERT that omits a column.**

Also unblocks four `lower/dml.rs` refusals and the `VALUES` schema-name mismatch
at `build.rs:951`. Independent of P4.

### P6 — Row identity for UPDATE and DELETE. **~4 wk. Blocks UPDATE and DELETE entirely. This is the big one.**

Not on the `3a11a4d4` list, and larger than most items that were.

A removal must name what to remove. The incumbent has two answers and the owned
engine can inherit neither for free:

- **Hot tombstones.** `MemRowValue::Tombstone` in the `MemTableRegistry`, keyed
  by encoded PK, suppressed at read time by `TombstoneFilterExec` — which **is a
  DataFusion `ExecutionPlan`** (`hot_tombstone.rs:20`). Writing tombstones the
  owned read path cannot see is worse than not writing them.
- **Cold copy-on-write.** Requires knowing which data file each matched row came
  from. The owned scan does not produce row identity: `Scan` is built from
  `projection`/`filters` only (`build.rs:381`–`:395`) and `BatchSource` yields
  bare `RecordBatch`es.

And there is a **circularity** that has to be designed around rather than
discovered — one that already bites today, before any owned write exists.
`build_resolver` declines any table whose memtable is non-empty
(`owned_engine.rs:1207`–`:1217`, "table has pending hot-tier rows or
tombstones"). But `total_count()` is `self.inner.read().len()`
(`basin-hottier/src/memtable.rs:690`) — it counts **clean, already-durable
INSERT-residency rows too**, not only dirty tombstones and overrides. Any
ordinary small OLTP INSERT (≤128 rows, single-column PK) calls
`write_through_insert_residency` (`executor.rs:7259`, `:14126`) and leaves clean
entries behind, so from that moment the table is **ineligible for owned-engine
reads** until the retention sweep evicts them — `BASIN_HOTTIER_RETAIN_SECS`,
default 300 s (`basin-hottier/src/budget.rs:162`–`:163`,
`registry.rs:500`, `:539`).

So the owned engine is already effectively limited to read-mostly or idle
tables, and an owned *write* would make that worse: the table it just wrote
becomes unreadable by the engine that wrote it. Whatever P6 chooses, it has to
choose in a way that does not deepen this — and the cheapest real fix is
orthogonal to P6 entirely: **make the ineligibility gate count dirty entries
rather than all entries**, since clean entries are byte-identical to cold by
construction.

Three options, none cheap, and the choice is the main open design question this
document does not close:

1. **Add a row-identity column to the owned scan** (file path + row index,
   projected only when the plan is a DML plan). Enables cold CoW. Touches
   `BatchSource`, `storage_source`, `build.rs`'s `Scan` arm, and the sink.
   Largest, cleanest, and the only one that makes `UPDATE` a real operator
   rather than a delete-then-insert.
2. **Key-based CoW**: collect matched PKs, rewrite every file whose column stats
   admit those keys. Reuses the pruning the resolver already does
   (`owned_engine.rs:1256`). Correct but potentially rewrites the whole table for
   a one-row `DELETE`.
3. **Teach the owned read path to apply hot tombstones and update overrides**,
   then write them. This matches the incumbent exactly — a `Tombstone` or a
   full-row `Update` keyed by encoded PK (`basin-hottier/src/memtable.rs:77`,
   `:118`) — and it is the only option that removes the circularity rather than
   working around it. The cost is porting `TombstoneFilterExec` and
   `UpdateOverlayExec` (`hot_tombstone.rs:352`, `:500`) to owned operators. That
   is P6 work *plus* read-path work, and it is the option most likely to
   reintroduce a `553f4f8b`-shaped "two implementations of visibility" bug — but
   note that porting those two nodes is on the **removal-surface** critical path
   anyway (§1.7, item 9), so a large part of its cost is already owed.

Given that last point, option 3 is the current preference, but the decision
should be taken with the ported overlay operators in hand rather than before.

Whichever is chosen, `Update`'s delete-then-insert decomposition (§2.2(d)) must
be replaced by a keyed full-row override, or at minimum must **refuse** when
`key_cols` appear in the `SET` list rather than silently duplicating the row.

### P7 — The declarative write-path effects (§1.5). **~3 wk for a refusal, unbounded for support. Blocks the general gate; free for a narrow one.**

Triggers: **zero**, they cannot exist (`pg_ast.rs:1490`). Soft-delete columns,
AUTO_UPDATE columns, `AUDIT TO`, `REACT ON` reactors, secondary-index
maintenance, promoted-JSONB shadow columns, CDC: each is a real behaviour a
table can carry, and each is a reason to decline. The cheap and correct version
of P7 is **a per-table eligibility predicate that refuses any table carrying any
of them**, mirroring what `build_resolver` already does for RLS
(`owned_engine.rs:1189`) and promoted JSONB (`:1194`). Supporting them is a
different, much larger project and is not on this critical path.

The failure mode to design against: an eligibility predicate that forgets one
effect is silent data loss (an audit row that never gets written), not a visible
error. It should be written as an **allow-list over `TableMetadata` fields**, so
that a metadata field added later fails closed.

### P8 — A write-capable oracle. **~2 wk. Blocks trusting any of the above. See §6.**

### Sequencing

```
P0 fallback contract ──┬─► P2 staging sink ──┬─► P3 command tag ──┬─► NARROW GATE
                       │                     │                    │   (INSERT only)
P5 defaults/identity ──┘                     │                    │
                                             │                    │
P1 expression re-hosting ─► P4 constraints ──┘                    │
   (removal-surface track)                                        │
                                                                  ▼
P7 eligibility predicate ──────────────────────────────► GENERAL INSERT GATE
                                                                  │
P6 row identity ────────────────────────────────────────► UPDATE / DELETE GATE
                                                                  │
P8 differential oracle ─── required before ANY of the above is believed
```

Independent and parallelisable: P0, P1, P5, P7, P8. On the critical path in
order: P0 → P2 → P3. Blocking only the general gate: P4 (which needs P1).
Blocking only `UPDATE`/`DELETE`: P6.

**Total, honestly: on the order of 15–18 person-weeks, and P6 is a quarter of it
on its own.** That is a quarter of work for one agent, not a sprint. Anyone
planning around "the write path is the last item" should plan around it being
larger than the SELECT work that preceded it, not smaller.

---

## 5. Is a narrow gate worth it?

`3a11a4d4` suggests a subset might open safely: INSERT with plain literals, no
constraints. Costing it out:

**What it needs:** P0, P2, P3, P5. Roughly 4.2 weeks — about a quarter of the
total.

**What the predicate keeping it narrow has to check.** Not "is this an INSERT
with literals". It has to check, per statement and per table:

- the statement is `InsertStmt`, no `WITH`, no `ON CONFLICT`, no `RETURNING`,
  a `VALUES` list of literals only (no subquery, no `INSERT … SELECT`);
- the table has no CHECK constraints, no FK either direction, no UNIQUE beyond
  the PK, no NOT NULL the statement does not satisfy;
- no generated columns, no promoted-JSONB shadow columns, no RLS, no
  soft-delete column, no AUTO_UPDATE column, no audit target, no reactor
  subscribing to INSERT on this table, no secondary index;
- an empty memtable (already checked, `owned_engine.rs:1207`);
- no active transaction (already checked, `owned_engine.rs:295`);
- the staged write is under the memory bound.

That is fifteen conditions. Twelve of them are exactly the conditions P4 and P7
would remove, which means **the narrow-gate predicate is throwaway work in
proportion to how narrow it is** — the narrower it is, the more of it gets
deleted.

**But it is worth it anyway, for one reason that has nothing to do with the
predicate.** Not one line of storage-backed write code in this repository has
ever run. P0/P2/P3/P5 is the smallest change that puts a real committed row on
disk through the owned engine, and until that has happened every estimate in §4
— including P6's — is guesswork about an untested path. The narrow gate's value
is that it converts the rest of the plan from estimated to measured.

Two conditions on it:

1. It must be **default-off behind its own flag**, not folded into
   `BASIN_OWNED_ENGINE`. A user who turns on the owned engine for reads must not
   silently acquire an experimental write path.
2. The predicate must be **one named function with its own unit tests**, in the
   shape `shadow_target` / `is_side_effect_free` already established
   (`owned_engine.rs:618`, `:640`) — including that file's stated discipline of
   being **conservative about node kinds that do not exist yet**
   (`owned_engine.rs:636`–`:639`). A predicate that fails open on an unrecognised
   construct is the whole risk of a narrow gate.

---

## 6. The oracle problem

### 6.1 Shadow-compare cannot cover DML, and the guard that says so is correct

`shadow_target` (`owned_engine.rs:618`) matches only `NodeEnum::SelectStmt`, and
only when `is_side_effect_free` (`:640`) has walked the `WITH` list and every
set-operation arm's own `WITH` and confirmed each CTE body is itself a
side-effect-free `SelectStmt`. Anything it cannot positively identify as a
`SelectStmt` — "including node kinds that do not exist yet" — is treated as
writable (`:636`–`:639`).

That guard is right and must not be weakened. Running an INSERT through both
engines double-writes; there is no version of shadow-compare that is safe for
DML, because the incumbent's execution *is* the side effect.

So: **the instrument that de-risked every SELECT fix in this program does not
exist for writes.** [20](./20-oracles.md) calls the in-process incumbent oracle
"the highest-value instrument in the whole program". For the write path there is
no such instrument, at any price, and the design must not pretend otherwise.

### 6.2 What the design needs from the live-PostgreSQL DML harness

A DML differential harness against live PostgreSQL is under construction in
`tests/integration/`. It is the *only* correctness oracle writes will have.
What this design needs from it, in priority order:

1. **Final-state comparison, not just result comparison.** After each statement,
   `SELECT * FROM t ORDER BY <pk>` on both sides and compare the full table. A
   harness that compares only what the statement returned would pass an INSERT
   that wrote the right `RETURNING` row and the wrong table row. (`INSERT`
   without `RETURNING` returns nothing at all — a harness that compares only
   returned rows compares nothing, which is failure class 2 in §7.)
2. **Command tag comparison.** The tag string, exactly: `INSERT 0 1`,
   `UPDATE 0`, `INSERT 0 0`. §3.1 has the measured values. This is the only
   automated check that would catch P3 regressing.
3. **Error comparison, including SQLSTATE.** Both the fact of the error and its
   code. The measured cases from §3.1 and §2.2 — self-conflicting INSERT, PK-
   rewriting UPDATE, `ON CONFLICT` touching a row twice — are all cases where
   Basin's current operator *succeeds silently* where PG errors. A harness that
   only compares successful outcomes would score all three as passes.
4. **A post-failure state assertion.** After every expected error: assert the
   table is byte-identical to its pre-statement state on both sides. This is the
   only mechanical check of §3's atomicity claim.
5. **Sequence/identity state.** After an INSERT that consumed a sequence, compare
   `currval`. A `nextval` allocated per row (`executor.rs:10143`) that
   double-allocates or skips is invisible in the table contents.
6. **A skip that is loud.** Follow the convention `catalog_fidelity.rs` and
   `function_equivalence.rs` already set (`basin-exec/Cargo.toml`,
   `harness = false`): a missing `PG_DIFF_TEST_DSN` must print a banner, never
   look like a pass.
7. **A stated answer to non-idempotence.** A write cannot be run twice and
   compared, which is the structural problem that harness is solving. What this
   design needs from whatever it chooses: the reseed must be **identical on both
   sides and deterministic**, including any sequence state (`Catalog::nextval`
   allocates per row — `executor.rs:10143`), so that "Basin and PG disagree" can
   never mean "the two fixtures started differently". A harness that reseeds by
   re-running the setup statements rather than by restoring a snapshot has to
   prove that the setup itself is deterministic, or its first divergence report
   will be about its own fixture. `compare_postgres_ext_dml.rs` already
   establishes the reseed-per-shape discipline for timing; correctness needs the
   stricter version.
8. **An assertion that it compared something.** A run that compared zero
   statements must fail. `compare_postgres_matrix.rs` already states this
   discipline in its module docs ("a measurement cannot fail … the test fails if
   any of them disagree, **or if the run somehow compared nothing at all**") —
   the DML harness needs the same clause.

### 6.3 What stays unverifiable even with it

Say this plainly, because the harness will feel like more coverage than it is.

- **Anything Postgres has no opinion about.** Vortex/Parquet file layout,
  compaction, the hot-tier overlay, tombstone representation, the catalog
  snapshot chain. `553f4f8b` was a Basin-only bug in a Basin-only mechanism, and
  no Postgres oracle would have flagged it.
- **Crash and partial-failure behaviour.** The harness runs statements to
  completion. It cannot test "the process died between the object-store `put` and
  the `replace_data_files`". That window is real (`bc57fa48`,
  `note_uncommitted_file`) and is testable only by fault injection, which does
  not exist.
- **Concurrency.** `CommitConflict` retry (`protocol.rs:2085`), OCC on
  `expected_snapshot`, two sessions writing the same table. A single-threaded
  differential harness sees none of it. The `ObjectStoreCatalog` multi-chain
  compensation path (§1.4) has a documented state where rows are duplicated and
  "manual reconciliation" is required; nothing tests it.
- **Durability.** Whether a row acked by the shard WAL survives a restart.
- **The 20 known baseline divergences.** Basin already differs from Postgres in
  20 tracked places ([16](./16-differential-baseline.md)). Every one of them is a
  place where "the harness disagrees" means nothing until a human classifies it,
  and a write-path baseline will grow its own entries.
- **Anything both engines get wrong.** The harness compares Basin to Postgres,
  which is better than comparing Basin to Basin — but where the *incumbent*
  Basin write path is already wrong, a faithful owned reimplementation will be
  wrong identically and the harness will flag it as a Basin-vs-PG divergence with
  no way to tell whether the owned path introduced it.

The last one has a cheap mitigation worth taking: **run the DML harness against
the incumbent first and record its output as a baseline, before the gate opens.**
That baseline is free today and impossible after DataFusion is deleted — the same
argument [20](./20-oracles.md) makes for the in-process oracle, applied to the one
oracle writes can actually have.

---

## 7. What would go wrong that a test would not catch

This session found 19 silent wrong answers. The recurring classes were: code
trusting a false or absent document (eleven, two of them *tests* pinning
behaviour that had become a bug); harnesses that appear to run but compare
nothing (five); and numbers measuring something other than what they claim.
Applied to a write path:

### 7.1 Code trusting a false document

- **`dml.rs`'s module docs are the design document for a sink that does not
  exist.** "Storage is a seam, not a dependency … `basin-storage` plugs in a real
  implementation later; **nothing here changes when it does**" (`dml.rs:19`–`:23`).
  §2.2 establishes that at least four things must change: async, `finish`,
  removal representation, and `Update`'s decomposition. An implementer who trusts
  that sentence will write a per-batch, non-atomic sink and believe the docs
  blessed it. **This document supersedes that paragraph, and that paragraph
  should be edited to point here.**
- **`Update`'s conceded limitation is a comment, not a guard.** "an `UPDATE` that
  rewrites its own primary key is not modelled at this layer" (`dml.rs:50`) is
  true and inert. The operator does not refuse; it duplicates the row. A
  limitation recorded only in prose is the exact shape of the eleven.
- **"Triggers" on the `3a11a4d4` precondition list.** Reading `pg_ast.rs:1490`
  shows the precondition is empty and the real one (nineteen modules of
  declarative effects) was never named. A precondition list that names the wrong
  thing is worse than a short one.

### 7.2 Harnesses that appear to run but compare nothing

- **`affected_rows` is the archetype.** Five unit tests in `dml.rs` assert the
  affected count. `grep -rn affected_rows crates/` finds no production caller
  (§1.6). The tests are green, the number is right, and nothing can read it.
- **An INSERT returns no rows.** Any DML harness modelled on the SELECT harnesses
  — compare the returned result sets — compares two empty sets and passes. This
  is not hypothetical; it is the default shape a harness lands in.
- **A write test on a table nobody reads back.** Asserting `INSERT 0 1` proves a
  tag was formatted, not that a row is retrievable, and definitely not that it is
  retrievable *after a restart or a compaction*. Every write test needs a read
  that goes through `live_data_files()`.
- **A fallback that looks like a pass.** If the gate is open and the eligibility
  predicate declines, the incumbent serves the statement and the harness compares
  DataFusion to Postgres — green, and measuring nothing about the owned path.
  **Every DML harness assertion must be paired with an assertion that the owned
  engine actually served it**, the way `compare_postgres_matrix.rs` reads
  `owned_engine_served_count` per shape. Without that pairing a 100%-green DML
  suite is compatible with zero DML statements ever reaching the owned engine —
  which is, precisely, the situation `3a11a4d4` discovered for the probe.

### 7.3 Numbers measuring something other than what they claim

- **`owned_engine_served_count` is engine-global.** That is what produced the
  phantom `2`. The moment DML flows through the bridge, a statement that
  internally re-enters `sess.execute()` will still inflate it. **DML needs its
  own counter, or the served count needs a per-statement-kind breakdown**, or the
  first number anyone quotes after the gate opens will be wrong in the same way
  the last one was.
- **Rows written ≠ rows visible.** A count taken from the sink is a count of
  rows staged. Between staging and a subsequent `SELECT` sit the catalog commit,
  the memtable, the hot-tier residency promotion and the metadata cache
  invalidation (`executor.rs:7261`, `:7269`). The only trustworthy count is one
  read back.
- **A row count from a table with a non-empty memtable is a fiction for the
  owned engine**, because `build_resolver` declines that table
  (`owned_engine.rs:1212`) and something else answered.

### 7.3a Six things the incumbent already gets wrong, found by reading it

Recorded here because the design's rule is "share the incumbent's machinery" and
these are the places where sharing it would inherit a bug. Each was found by
reading, not by a failing test — which is the point.

1. **`UPDATE` never enforces NOT NULL.** `check_null_allowed` (`dml.rs:1268`) is
   called only from the INSERT literal path. Grepped for a call on the UPDATE
   path: none.
2. **`UPDATE` never enforces enum labels or domain CHECKs.**
   `enforce_enum_labels` / `enforce_domain_checks` appear only on INSERT and COPY
   paths (`executor.rs:7130`, `:7132`). An `UPDATE` can write a value outside its
   domain.
3. **`INSERT … SELECT` does not apply column DEFAULTs.** Unsupplied columns are
   filled with `new_null_array` (`executor.rs:7766`) rather than routed through
   `apply_column_defaults`. It also never NULL-checks, and hard-errors on tables
   with generated columns (`:7597`).
4. **`INSERT … VALUES … RETURNING` ignores the `RETURNING` list.** The auto-commit
   path returns the whole inserted batch verbatim (`executor.rs:7327`–`:7333`);
   only the `dml_mutate` paths run `project_returning` (`:7638`). `RETURNING id`
   on a 6-column table returns 6 columns.
5. **A failed catalog commit orphans its data file** (`executor.rs:7418`), while
   the two earlier failure points clean up. `commit_with_retry` retries once.
6. **`ON CONFLICT` is probe-then-mutate** (`executor.rs:8857`) — racy by
   construction, and it does not reproduce PostgreSQL's "cannot affect row a
   second time".

None of these is this design's to fix. All of them are this design's to **not
copy**, and each is a case where the differential harness (§6.2) would earn its
cost immediately.

### 7.4 The two failure modes unique to writes

- **Silent success where Postgres errors.** §2.2 has three measured examples
  (PK-rewriting UPDATE, key-shifting UPDATE, self-conflicting INSERT). This
  direction is more dangerous than the reverse: an owned engine that errors where
  PG succeeds gets a bug report; an owned engine that succeeds where PG errors
  gets a corrupted table and a happy client.
- **A bug with a latency fuse.** `553f4f8b`'s wrong answers appeared only within
  the 300-second superseded-file grace window
  (`basin-shard/src/in_process.rs:335`). A write-path equivalent — a row visible
  through the memtable but never committed, correct until the process restarts —
  passes every test that runs in one process in under five minutes. **Any write
  test must include at least one read taken after a forced flush/compaction, not
  just immediately after the write.**

---

## 8. Recommendation

### 8.1 The gate is not the wall

The brief for this document called the write path "the last wall to deleting
DataFusion". Reading the code says that is the wrong framing, and the correction
is the most useful thing here.

**Deleting DataFusion does not require the owned engine to own the write path.**
§1.7 has the two lists. The write *mechanics* are already DataFusion-free —
literal coercion, the compound-predicate `WHERE` evaluator, the hot-tier overlay,
cold copy-on-write, the catalog commit protocol, the WAL, PK/UNIQUE/FK scanning.
What is not free is three things, and none of them is "the owned engine must
write":

- **Expression evaluation** (CHECK, GENERATED, `SET` RHS, `RETURNING`
  projection). Replaceable **in place**, inside `constraints.rs` /
  `generated_cols.rs` / `dml_mutate.rs`, by parsing the stored text with
  `pg_query`, lowering with `basin_plan::lower::expr` and evaluating with
  `basin_exec::eval`. The incumbent write path keeps its shape; only its
  evaluator changes.
- **Set-oriented source computation** (`INSERT … SELECT`, `UPDATE … FROM` /
  `DELETE … USING` joins, `EXISTS` rowsets, MERGE's match join). These are
  already `sess.execute()` / `ctx.sql()` re-entries — 13 of them
  (`grep -c` on `dml_mutate.rs`). They are **SELECTs**, and the owned engine
  serves SELECTs today. This is the phantom `2` in the probe, seen from the other
  side: the owned engine is *already* doing some of this work.
- **The read-side overlay plan** (`TombstoneFilterExec`, `UpdateOverlayExec`,
  `HtapUnionTable`). This is scan-track work, owed regardless
  ([06](./06-scan-and-storage.md)), and it is also most of the cost of P6
  option 3.

That reframes the sequencing question. Of the 18 probe fallbacks remaining, 15
are DML — but "DML falls back" and "DataFusion cannot be deleted" are not the
same statement, and conflating them will buy a quarter of irreversible write work
to close a number that could be closed another way. **The probe measures which
engine served a statement. It does not measure what still links DataFusion.**
Those two numbers should be tracked separately from here on, because they are
about to diverge.

### 8.2 The gate should stay shut, and the first thing to do is not to open it

The single first action is **P0: fix the fallback contract** (§3.4). It is about
a fifth of a week. It is the only item that is strictly cheaper to do now than
later, because it is small exactly while the answer to "has anything been
written?" is unconditionally "no" — the change becomes a careful audit the moment
that stops being true. It removes a latent double-write from a code path that is
one `matches!` away from being reachable. And it does not require a single design
decision that this document leaves open.

Then P2 → P3 → P5, and open a narrow, separately-flagged INSERT gate (§5), not
because the narrow gate is valuable in itself but because it converts every
remaining estimate — P6's above all — from guess to measurement.

P8 (the differential harness) should be built in parallel and, critically,
**baselined against the incumbent before the gate opens**, while that comparison
is still possible.

P6 is the item to be honest about: **row identity for UPDATE and DELETE is
roughly a quarter of the total effort on its own**, it was not on the original
list of eight, and it has an unresolved design choice at its centre
(§4, P6, options 1–3) that this document deliberately does not close. `UPDATE`
and `DELETE` should not be planned as arriving with `INSERT`. They will not.

### 8.3 If the goal is deleting DataFusion rather than moving the probe number

Then the order is different, cheaper, and reversible at every step — and it is
the order this document recommends:

1. **P0**, the fallback contract. Unchanged; still first; still ~0.2 wk. It is
   the only item that is strictly cheaper now than later.
2. **P1 in place** (~3 wk): re-host CHECK, GENERATED, `SET` RHS and `RETURNING`
   expression evaluation onto `basin_exec::eval` **inside the existing write
   path**. No new sink, no new commit path, no new visibility implementation, no
   irreversible risk. Every existing write test is the oracle, because the
   behaviour is meant to be identical.
3. **Route the 13 `sess.execute()` re-entries and `INSERT … SELECT` through the
   owned engine** — they are SELECTs, the bridge already serves SELECTs, and the
   safety net that covers them is shadow-compare, which *does* work for a SELECT
   even when the SELECT is a DML statement's source. This is the one place where
   the write path gets the differential instrument §6.1 says writes cannot have.
4. **Port the overlay `ExecutionPlan`s** to owned operators (scan-track work,
   owed anyway, and most of P6 option 3's cost).
5. **Then**, with DataFusion's write-path dependencies gone and the overlay
   operators in hand, decide whether the owned engine should own the write path
   at all — with P6's design choice informed by working code rather than by this
   document's guess.

Steps 2–4 delete DataFusion's write-path dependencies without a single new byte
reaching storage through untested code. Step 5 is where the irreversible risk
lives, and it is the only step that should wait for the oracle in §6.

The narrow gate (§5) remains worth doing in parallel with 2–4, for the reason
given there and no other: it is the only way to convert P6's estimate into a
measurement. It should not be on the critical path to removal, because §8.1 says
it is not.
