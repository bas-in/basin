---
title: "The owned Vortex reader already exists"
nav_section: migration
sidebar_position: 25
summary: "\"Can we not make our own\" Vortex reader has already been answered by the code: basin-storage reads Vortex through vortex-file / vortex-array directly, with projection and filter pushdown, zone-map chunk pruning, a footer cache and a decode cache, and zero DataFusion crates in its dependency closure. vortex-datafusion is used only by the incumbent ListingTable path in basin-engine, and every line of that wrapper is DataFusion interface plumbing that gets deleted, not reimplemented. The remaining gap is pushdown expression coverage — a performance gap, not a correctness one — and the cheapest part of it is four lines in basin-exec, not a new reader."
tags: [migration, datafusion, vortex, storage, pushdown]
---

# 25 — The owned Vortex reader already exists

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. This document answers a question asked directly: *"don't use
vortex-datafusion, can we not make our own for that too."*

The answer is that we already did, in `basin-storage`, and it has been the
owned engine's only read path since `basin-exec` learned to read files at all.
The work left is deletion of the adapter alongside the umbrella cut, plus a
bounded amount of pushdown coverage. It is not construction of a reader.

Every claim below says whether it comes from code read or a command run.

---

## 0. The headline, stated first because it changes the question

**`basin-storage` has no DataFusion in its dependency closure at all** — not
the adapter, not the umbrella, nothing:

```
$ cargo tree -p basin-storage -e normal | grep -c datafusion
0
$ cargo tree -p basin-exec -e normal | grep -i datafusion
(no output)
```

(Both commands run for this document.) `crates/basin-storage/Cargo.toml`
declares `vortex-array`, `vortex-file`, `vortex-btrblocks`, `vortex-session`,
`vortex-io`, `vortex-layout` and `vortex-buffer` at `0.71`, and no
`vortex-datafusion`. That is the same finding commit `44a1d217` recorded from
the other direction: every core Vortex crate `basin-storage` uses pulls zero
DataFusion.

So the owned read path does not *bypass* the adapter as a design choice it
might later regret — it has never been able to reach it. The question is not
"can we build our own", it is "what does the incumbent path's adapter still do
that ours does not", and that turns out to be a short list.

---

## 1. What the adapter actually does at its call sites

`vortex-datafusion` is named in exactly two files, both in `basin-engine`
(`grep` for `vortex_datafusion`, run for this document):

| Site | What it is |
|---|---|
| `crates/basin-engine/src/session.rs:3474` | `VortexFormat::new_with_options(...)` inside `listing_file_format` — the `TableFileFormat::Vortex` arm that hands DataFusion a `FileFormat` for a `ListingTable` |
| `crates/basin-engine/src/vortex_listing_format.rs` | `BasinVortexFormat`, a wrapper that delegates to `vortex_datafusion::VortexFormat` |

Reading `vortex_listing_format.rs` (1202 lines) and splitting it three ways as
the task asks:

**(a) Vortex bytes → Arrow `RecordBatch`.** Zero lines. Not one line of that
file decodes anything. Decode happens inside `vortex-datafusion`'s
`persistent/opener.rs`, which itself calls the same `vortex-file` /
`vortex-array` scan API `basin-storage` calls.

**(b) Pushdown.** Also zero lines of its own. `UdfPushdownGuard`
(`vortex_listing_format.rs:411-529`) *forwards* `try_pushdown_filters`,
`try_pushdown_sort` and `try_pushdown_projection` to the inner source and
re-wraps the result; its only original behaviour is to **decline**
expression-bearing projections so DataFusion cannot fuse a JSONB UDF into the
scan and run it on pre-filter rows. That is a workaround for a DataFusion
optimizer rule. It has no meaning outside DataFusion.

**(c) DataFusion interface plumbing.** Everything else, i.e. effectively the
whole file:

- `impl FileFormat for BasinVortexFormat` — `infer_schema`, `infer_stats`,
  `infer_ordering`, `infer_stats_and_ordering`, `create_writer_physical_plan`,
  `file_source`, `create_physical_plan`.
- The `infer_stats` patch (W2-1): substitutes `Precision::Inexact(object.size)`
  when the inner format returns `Precision::Absent`, so DataFusion's
  `join_selection` rule stops mis-planning byte-skewed joins.
- `UuidDecimal256RestoreExec` and `PointFsbRestoreExec`
  (`vortex_listing_format.rs:842` and `:975`) — two full `ExecutionPlan`
  implementations whose entire job is to undo the ADR-0024 type disguises on
  the way out of a `DataSourceExec`, because DataFusion's physical-expr adapter
  would otherwise attempt an impossible `Decimal256(39,0)` →
  `FixedSizeBinary(16)` cast.
- `Partitioning`, `PlanProperties`, `EquivalenceProperties`,
  `gather_filters_for_pushdown`, `handle_child_pushdown_result` — trait
  obligations.

Category (c) is not work we would redo in an owned engine; it is work that
exists *because* of DataFusion. The ADR-0024 restore execs in particular have
an owned-path counterpart that is not an `ExecutionPlan` at all — it is a
function call inside the reader (`vortex_project_and_filter`,
`crates/basin-storage/src/reader.rs:3197`, and the schema disguises applied at
`reader.rs:1339-1352`).

The one thing in category (c) with genuine value beyond DataFusion is
`infer_stats` — row counts and byte sizes for join planning. In the owned
stack that value lives in the catalog (`live_data_files()` carries
`row_count`, `size_bytes` and `column_stats` per file; see
`owned_engine.rs:1531-1545`) and is a `basin-plan` concern, not a reader
concern.

---

## 2. The owned call chain, verified

From code read, `basin-exec`'s `Scan` down to the Vortex API:

```
basin_exec::scan::Scan
  └─ Box<dyn BatchSource> = StorageBatchSource            crates/basin-exec/src/storage_source.rs
       └─ Storage::read_paths_with_schema                 crates/basin-storage/src/lib.rs:2108
            └─ reader::read_paths_with_schema             crates/basin-storage/src/reader.rs:1086
                 └─ read_paths_inner  →  read_one         reader.rs:1104, reader.rs:1247
                      └─ `if path.ends_with(".vortex")`   reader.rs:1328
                           ├─ vortex_read_projection      reader.rs:3636
                           ├─ vortex_filter_expr          reader.rs:3506   → vortex_array::expr::Expression
                           └─ vortex_format::decode_with_cache             crates/basin-storage/src/vortex_format.rs:524
                                └─ decode_inner                            vortex_format.rs:554
                                     ├─ session().open_options().open_buffer(bytes)      :579/:584
                                     ├─ vf.scan()                                        :621
                                     ├─ sb.with_projection(select(names, root()))        :692
                                     ├─ sb.with_some_filter(Some(f))                     :696
                                     ├─ sb.into_array_stream()                           :743
                                     └─ chunk.execute_record_batch(schema, &mut ctx)      :758
```

The terminal calls are `vortex-session`, `vortex-file` and `vortex-array`
directly. There is no `vortex-datafusion` type, no `TableProvider`, no
`ExecutionPlan`, and no `datafusion` crate anywhere on that chain — which
§0's `cargo tree` output independently guarantees, since neither crate can
depend on something absent from its closure.

The owned reader is also, by a wide margin, the more capable of the two on
everything except expression coverage. From `reader.rs` and `vortex_format.rs`,
the owned path has and the adapter path does not:

- a **footer cache** keyed on `(path, size_bytes)` that skips the flatbuffer
  footer parse (`vortex_footer_cache.rs`; `with_footer(footer)` at
  `vortex_format.rs:576`);
- an **unfiltered-decode cache** with a serve-side row gate, including a
  pre-GET short-circuit that answers a warm point read without issuing the
  object-store GET at all (`reader.rs:1352-1440`);
- **envelope decryption** (AES-GCM `.wrapped` sidecar) feeding the same decoder
  (`reader.rs:1452-1471`);
- the **ADR-0024 / POINT / INTERVAL** physical-schema disguises and their
  post-decode inverse, as function calls rather than plan nodes;
- a **truncated-body tripwire**: a no-filter decode that returns fewer rows
  than the file's footer declares is a hard error rather than a silent short
  read (`vortex_format.rs:593-607`, `:764-773`);
- **catalog-stats file pruning** before any file is opened
  (`prune_live_files`, `crates/basin-exec/src/storage_source.rs:443`), using
  per-file `column_stats` the catalog has carried since Phase 5.7 A4;
- **liveness-correct file sets** — `live_data_files()` rather than an
  object-store LIST (commit `553f4f8b`; `owned_engine.rs:1516-1545`).

None of that came from `vortex-datafusion`, and none of it survives in the
adapter path.

---

## 3. What is lost, and of what kind

One thing, in two parts, and both are performance.

### 3.1 Pushdown expression coverage

`vortex-datafusion`'s `convert/exprs.rs` (1008 lines,
`~/.cargo/registry/src/*/vortex-datafusion-0.71.0/src/convert/exprs.rs`, read
for this document) converts DataFusion `PhysicalExpr` trees to Vortex
expressions and covers: `Eq NotEq Lt Lte Gt Gte And Or Plus Minus Multiply
Divide`, plus `Column`, `Literal`, `LikeExpr`, `CastExpr`, `CastColumnExpr`,
`IsNullExpr`, `IsNotNullExpr`, `InListExpr`, `CaseExpr` and
`ScalarFunctionExpr`.

Basin's `vortex_filter_expr` (`reader.rs:3506`, ~130 lines) covers:

| Shape | Pushed? |
|---|---|
| `col = lit`, `col > lit`, `col < lit` | yes, but **only** when the catalog schema proves the column is exactly `Int64`, `Float64`, `Boolean`, `UInt64` or `Utf8` |
| `col LIKE 'prefix%'` (case-sensitive) | yes, `Utf8` only |
| `col ILIKE 'prefix%'` | no (Vortex `stat_falsification` does not handle case-insensitive) |
| `col IN (…)` | no — Arrow-side only, by explicit decision |
| `>=`, `<=`, `<>`, `IS NULL`, `OR`, `CAST`, arithmetic, `CASE`, scalar functions | no |

The `Int32`/`Int16` exclusion is deliberate and documented at `reader.rs:3489`:
a mixed-DType comparison **panics inside the spawned Vortex scan task**, where
the `Result`-based fallback in `decode_with_cache` cannot catch it. So the
narrowness is a safety margin against an upstream panic, not an oversight, and
widening it needs care rather than enthusiasm.

**Correctness or performance?** Performance, unambiguously, and the code makes
that structural rather than incidental:

- `vortex_filter_expr` returns `all_pushed: bool`, true only if *every*
  predicate was type-safe-pushed (`reader.rs:3502-3505`);
- `decode_with_cache` returns whether the scan actually ran with pushdown, and
  retries once with a plain full decode if the pushed scan errored
  (`vortex_format.rs:524-547`);
- the Arrow post-filter pass (`vortex_project_and_filter_limited`,
  `reader.rs:3347`) is skipped only when both say yes.

A predicate that fails to push is re-applied post-decode. The row set is
identical; the cost is that the column decoded in full instead of Vortex's zone
maps pruning chunks first.

### 3.2 All-or-nothing filter pushdown in `basin-exec`

This is the sharper of the two, and it is not in the reader:

```rust
// crates/basin-exec/src/storage_source.rs:573-580
let translated: Vec<_> = filters
    .iter()
    .filter_map(|f| expr_to_predicate(f, &entry.schema, projection))
    .collect();
if !filters.is_empty() && translated.len() == filters.len() {
    opts.filters = translated;
    pushed.filters_applied = true;
}
```

If one conjunct of two fails to translate, `opts.filters` stays **empty**. That
loses more than the Vortex expression push: `prune_live_files` is called with
`&opts.filters` on the very next line (`storage_source.rs:585`), so the
whole-file catalog-stats prune is lost too. `WHERE id = 5 AND name ILIKE 'a%'`
opens every live file in the table, because of the second conjunct.

`expr_to_predicate` (`storage_source.rs:632`) is itself narrow in a way that
makes this fire often: only `column OP literal` (never the commuted form), only
the `=`/`>`/`<` operator OIDs, and it never emits `Predicate::StartsWith` or
`Predicate::InInt64` even though `basin_storage::Predicate` has both.

The all-or-nothing rule is justified in a comment as "a partial translation
would mean the scan has to re-apply all of them anyway, so there is nothing to
gain by pushing some". That reasoning holds for the *post-filter*, and not for
pruning — a subset of a conjunction is a sound pruning predicate, and pruning
is where the win is.

**The fix is small and its safety comes from code that already exists.**
`build.rs:421-427` re-applies *every* filter when `pushed.filters_applied` is
false, and the storage reader re-applies `opts.filters` post-decode
regardless. So assigning the translated subset to `opts.filters` while leaving
`filters_applied = false` is correct by construction: the scan still filters
everything, and storage gains the file prune plus whatever zone-map pruning
the pushed subset earns. It is roughly four lines.

It is not written here because it lives in
`crates/basin-exec/src/storage_source.rs`, which is outside this task's write
scope. It is filed as the concrete next step rather than performed.

### 3.3 Size: unmeasured

The size of both gaps is **unmeasured**, and this document does not guess it.
No benchmark in the repo isolates "same query, pushed vs not-pushed on Vortex".
The one adjacent number found is a code comment at `reader.rs:252-254`
reporting a 1M-row LocalFS point read at ~23 ms via cached-then-Arrow-filter
versus ~0.8 ms via GET + zone-map-pruned decode. That is a real measurement of
what pruned decode is worth on that shape, but it was taken for a different
comparison (cache-serve vs pruned decode), it is a comment rather than a
committed harness result, and it should not be quoted as the size of §3.1 or
§3.2. If the §3.2 change is made, the honest measurement is a two-conjunct
query where one conjunct is untranslatable, asserting on
`Storage::read_counters().files_opened` — the counter that pruning moves, and
the one `storage_source.rs`'s existing tests already assert against.

---

## 4. Verdict: do not build a Vortex reader

Sized against the three things one might build:

| Candidate | Verdict | Size |
|---|---|---|
| An owned Vortex **reader** (bytes → `RecordBatch`) | **Already exists.** `vortex_format.rs` (1748 lines) + the `.vortex` branch of `reader.rs`. Do not write a second one. | 0 days |
| Owned **projection + filter pushdown** into Vortex | **Already exists.** `with_projection` / `with_some_filter` at `vortex_format.rs:692`/`:696`. | 0 days |
| Partial-conjunct pushdown in `basin-exec` (§3.2) | **Worth doing, next.** Correctness is already guaranteed by `build.rs:421`; the gain is the file prune. | ~4 lines + one counter-asserting test |
| Wider `expr_to_predicate` (`>=`, `<=`, commuted forms, `IN`, prefix `LIKE`) | Worth doing, after §3.2 | ~1 day; `>=`/`<=` need `Predicate::Ge`/`Le` variants, which touch `predicate.rs`'s pruning **and** its Arrow evaluator |
| Wider `vortex_filter_expr` dtypes (`Int32`, `Int16`, `Utf8View`) | Worth doing, carefully | ~1 day, gated on confirming Vortex 0.71 will not panic on a widened literal; the current narrowness guards a panic in a spawned task |
| Reimplementing anything in `vortex_listing_format.rs` | **No.** It is DataFusion interface satisfaction. It gets deleted with the umbrella. | 0 days |

And on the dependency question specifically, commit `e762b942` already settled
it: `vortex-datafusion`'s 18-crate DataFusion closure is a strict subset of the
umbrella's 31, so removing the adapter *alone* removes nothing from the build.
It comes out when `session.rs`'s `ListingTable` path comes out, not before, and
not separately.

**The answer to "can we not make our own for that too" is: we did, it is what
the owned engine reads through today, and `vortex-datafusion` is dead weight
attached to the incumbent path rather than a capability we still need to
replace.**
