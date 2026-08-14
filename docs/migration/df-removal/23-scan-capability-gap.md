---
title: "DF removal — the scan capability gap, and three wrong answers already shipping"
nav_section: migration
sidebar_position: 23
summary: "The five index/overlay ExecutionPlans behind the umbrella cut are two different things. The index scans (GIN, R-tree, JSONB posting) are pure prune — the owned path loses speed, never rows, and their computation is already DataFusion-free. The visibility nodes (tombstone, update overlay, HTAP union) are correctness, and the owned engine has none of them; it survives only because build_resolver declines any table with a memtable footprint. That gate is not complete: it does not check for a SOFT DELETE column, and the owned path has no citext and no enum-ordinal semantics either. With BASIN_OWNED_ENGINE=1 those three return wrong rows today."
tags: [migration, datafusion, correctness, scan, blockers]
---

# 23 — The scan capability gap

Part of the [ADR 0030](../../decisions/0030-own-query-engine-remove-datafusion.md)
migration map. [22](./22-removal-surface-measured.md) established that the only
tree-moving cut is the umbrella, and that behind it sit **89 errors across 7
files implementing `ExecutionPlan`**. This document asks the question that
measurement does not: for each capability those nodes provide, **does the owned
path have an equivalent at all**, and if not, what does a user lose?

**Method.** Everything below is either a file and line I read or a command I
ran, and each claim says which. Nothing here was executed: peer sessions hold
the build lock, so no `cargo build`/`cargo test` was run and no query was
issued against a live engine. The three defects in §0 are therefore
**code-read findings with a written reproduction each**, not observed failures.
That distinction matters and is not softened anywhere below.

---

## 0. Three wrong answers, today, with the flag on

`build_resolver` (`owned_engine.rs:1433`–`:1558`) is the bridge's entire
safety gate. It declines a statement when a referenced table:

| # | condition | line |
|---|---|---|
| 1 | resolves to a view | `:1459` |
| 2 | has `rls_enabled` | `:1464` |
| 3 | carries promoted-JSONB shadow columns | `:1469` |
| 4 | has any memtable entry (`total_count() != 0`) | `:1482`–`:1492` |

Plus one statement-level gate: an open explicit transaction
(`owned_engine.rs:358`).

That list is a copy of the module docs' list of DataFusion-side machinery the
owned scan does not have (`owned_engine.rs:28`–`:41`). **The list is
incomplete.** The DataFusion `SELECT` path applies more read-time rewrites
than those four gates cover, and each missing one is silent — no error, just
different rows.

### 0.1 A table with a `SOFT DELETE` column returns its deleted rows

This is the one to fix first. It is the same class of defect as `553f4f8b`
(the owned scan reading superseded files) and it is live now.

**What the incumbent does.** `SOFT DELETE` is a real Basin column attribute
(`lifecycle.rs:46`, `<col> <type> SOFT DELETE`), recorded as field metadata
`BASIN_SOFT_DELETE=1` (`types.rs:483`). A `DELETE` against such a table does
not remove the row: `dml_mutate.rs:1497`–`:1498` routes to `exec_soft_delete`
(`:9006`), which rewrites the file with `<sd_col> = now()` stamped on the
matched rows (`:9028`–`:9031`). **The row stays in the live cold files.**
Visibility is restored purely at read time: `exec_select` calls
`apply_soft_delete_to_select` (`executor.rs:11483`, guarded by
`if !include_deleted`), which loads each referenced table's metadata, finds
the soft-delete column (`:11807`) and AND-merges `<sd_col> IS NULL` into every
`TableScan` (`lifecycle.rs:553`).

The incumbent takes this seriously enough to gate its *own* fast path on it —
the PK point-lookup path declines with the comment "no soft-delete column (the
normal gate routes those to DataFusion)" (`executor.rs:6723`, `:6731`).

**What the owned path does.** Nothing. `build_resolver` does not check
`soft_delete_column`; grep for it across the workspace returns `ddl.rs`,
`executor.rs`, `lifecycle.rs`, `types.rs` and `dml_mutate.rs` — never
`owned_engine.rs`, never `basin-exec`, never `basin-plan`. The bridge runs at
`executor.rs:2121`, ~9,300 lines upstream of the injection at `:11483`, so a
served statement never reaches it.

**Consequence.** With `BASIN_OWNED_ENGINE=1`:

```sql
CREATE TABLE posts (id BIGINT, body TEXT, deleted_at TIMESTAMPTZ SOFT DELETE);
INSERT INTO posts VALUES (1,'a',NULL),(2,'b',NULL);
DELETE FROM posts WHERE id = 2;      -- stamps deleted_at, keeps the row
SELECT id FROM posts;                -- incumbent: {1}.  owned engine: {1,2}
```

The owned engine returns a deleted row. `posts` has no PRIMARY KEY in that
sketch, so no memtable residency is created and gate 4 does not save it; the
soft delete itself is a cold copy-on-write (`exec_soft_delete` →
`list_data_files_with_stats` + rewrite, `:9050`–`:9055`), which leaves the
memtable empty by construction. **A table declared with `SOFT DELETE` is
exactly the shape gate 4 cannot catch.**

Unverified by execution, and untested in the repo: `owned_engine_bridge.rs`
has no soft-delete case (grep for `soft`/`deleted_at` in that file: 0 hits),
and `fallback_histogram.rs`'s fixture declares no such column.

### 0.2 A `CITEXT` column compares case-sensitively — rows go missing

`CITEXT` is stored as Arrow `Utf8` + `BASIN_TYPE=CITEXT`, and the
case-insensitive contract is delivered by a DataFusion `AnalyzerRule`
(`citext_analyzer.rs:1`–`:32`) registered on the `SessionContext`
(`session.rs:3145`). It rewrites `=`, `<>`, ordered comparisons, `LIKE` and
sort keys on citext columns into `lower(...)` form.

`grep -rni citext crates/basin-exec/ crates/basin-plan/ crates/basin-pgtype/`
returns **zero hits**. The owned engine compares the raw `Utf8` bytes. So
`WHERE email = 'Alice@Example.com'` on a citext column returns *fewer* rows
than it should, and `ORDER BY` on one orders by byte value. No gate covers it.

Missing rows are worse than extra ones to detect: there is no row to point at.

### 0.3 An enum column orders lexicographically

Enum columns are `Utf8` + `BASIN_ENUM_TYPE=<name>`, and PG declaration-order
semantics for `ORDER BY` / `<` / `>` / `BETWEEN` come from a **SQL-text**
rewrite into a `CASE` ordinal expression (`enum_ordinal.rs:1`–`:20`). It runs
at `executor.rs:2654`–`:2673` — *downstream* of the bridge at `:2121`. Grep
for `BASIN_ENUM`/`enum_ordinal` in `basin-exec`/`basin-plan`: zero hits.

`SELECT ... ORDER BY status` on an enum column therefore sorts
`'active','archived','pending'` alphabetically instead of in declaration
order. Same class, same absent gate.

### 0.4 The cheapest fix, and why it is not the real fix

All three are **hours, not weeks**, if the answer is a gate: three more
conditions in `build_resolver`'s loop next to the RLS check, each a metadata
scan over `meta.schema` that the function already holds. That restores the
"declines rather than lies" contract immediately.

It does not survive the umbrella. After DataFusion is deleted there is nothing
to decline *to*, so each one becomes real work (§4). The gate is the correct
action today precisely because it is the action that stops shipping wrong
answers this week.

---

## 1. What the DataFusion-side nodes actually do

Read, not inferred from filenames.

### 1.1 `gin_rowgroup_scan.rs` (396 lines) — GIN row-group/row prune for JSONB `@>`

A `TableProvider` + leaf `ExecutionPlan` that exists for one reason stated in
its own header: DataFusion's `ListingTable` uses DataFusion's Parquet reader,
which cannot accept Basin's `ReadOptions.row_group_selection`, so a computed
allowlist "has nowhere to deliver" (`:6`–`:12`). The node drives
`storage.read_paths_with_schema` directly with
`ReadOptions { row_group_selection, row_selection }` (`:15`–`:24`,
`:92`–`:99`).

**Correctness contract, quoted:** the allowlist is "a **conservative
superset**… The full `jsonb_contains` UDF still re-evaluates every emitted
row… **No false negatives are possible**" (`:26`–`:32`). Files absent from the
map are read in full (`:34`–`:36`); Vortex files ignore row-group selection
entirely and are read whole (`:38`–`:43`).

**It is a prune. Losing it costs I/O, never rows.**

### 1.2 `rtree_rowgroup_scan.rs` (335 lines) — spatial prune

Explicitly "direct analogue of `gin_rowgroup_scan` for the spatial pushdown
path" (`:15`–`:16`), for `ST_DWithin` / `ST_Contains` / `=` on GIST-indexed
POINT columns. Exact at row-group granularity for `BboxIntersects`/`PointEq`,
conservative superset for `DWithin` with the Haversine UDF culling above the
scan; "No false negatives are possible" (`:30`–`:44`). **Prune.**

### 1.3 `jsonb_posting_scan.rs` (272 lines) — posting-list prune

Same bridging, different driver: `JsonbPostingRegistry::probe` AND-merges
per-atom posting lists instead of consulting a bloom, for the workload where
the bloom says "maybe" everywhere (`:5`–`:14`). Conservative superset,
`jsonb_contains` re-checks each row (`:16`–`:24`). **Prune.**

### 1.4 `tombstone_cold_scan.rs` (422 lines) — this one is visibility, not prune

A `TableProvider` + `ExecutionPlan` registered by `refresh_table` **only when
the memtable holds tombstones for the table and the session has no live HTAP
rows** (`:1`–`:21`, `:52`–`:59`). It drives `read_paths_with_schema` and
applies `filter_batch` inline in `execute()` (`:351`–`:353`), dropping every
row whose encoded PK matches a snapshotted tombstone.

Two details worth carrying into any port:

- If the caller's projection omits the PK, it **augments** the projection to
  include it and strips it back off with a `ProjectionExec` (`:138`–`:210`).
- It **drops limit pushdown** when it augments, because "the limit applies to
  post-filter rows; passing it to the cold scan would truncate before
  tombstone removal and under-count survivors" (`:143`–`:145`).

The second is a correctness rule any owned implementation has to reproduce.

### 1.5 `hot_tombstone.rs` — `TombstoneFilterExec` (`:352`) and `UpdateOverlayExec` (`:500`)

- `TombstoneFilterExec` wraps *any* cold scan and drops tombstoned rows
  (`:19`–`:21`, execute at `:470`–`:481`). Claims `Inexact` filter pushdown,
  since suppression commutes with a row filter.
- `UpdateOverlayExec` does **two** things: drops every cold row whose PK
  matches an `Update` override key, **then emits the override post-SET rows
  once the inner stream is exhausted** (`:486`–`:498`). The header notes that
  (1) commutes with a row filter but (2) does not — the appended rows must
  still be filtered, which is why the DataFusion twin always has a
  `FilterExec` above it (`:579`–`:585`).
- `TombstoneFilteringTable` (`:1095`) is the provider that wires both above a
  cold scan for auto-commit reads (`:1216`, `:1228`).

Good news for a port: the **algorithms** are already batch-level and mostly
DataFusion-free — `filter_batch` (`:703`), `apply_tombstone_filter_to_batches`
(`:747`), `apply_update_overlay_to_batches` (`:791`),
`normalize_batch_to_schema` (`:962`) operate on `RecordBatch`/`arrow` and
carry only `DFResult` as an error type. The `ExecutionPlan` shells are the
disposable part.

### 1.6 `HtapUnionTable` (`session.rs:4227`, docs `:4214`–`:4226`)

Unions a cold `ListingTable` with a hot `MemTable` into one logical table so
that in-transaction reads see both committed cold data and this session's
uncommitted writes, without a SQL-level `UNION ALL`. Registered per session
(`:4191`) and carrying `tx_overlay_peek` and `tx_hot_seq_watermark_peek`
(`:4188`–`:4189`) — i.e. it is also the carrier of **hot-tier snapshot
isolation**, not merely a union.

### 1.7 `interval_storage.rs` (336 lines) — not a scan at all

Worth correcting in the brief: this is the DataFusion half of the
INTERVAL-as-bytes storage disguise. Its header states the reason plainly:
"the DataFusion fallback path reads those files through `ListingTable`, **NOT**
through basin-storage's reader, so it needs its own half of the inverse"
(`:14`–`:16`).

basin-storage's reader has the other half already — `restore_interval` and
friends at `reader.rs:3761`–`:3849`, applied inside the Vortex/Parquet read
paths (`:2894`, `:3106`, `:3453`–`:3477`) that `read_paths_with_schema`
(`:1086` → `read_paths_inner` → `read_one`) drives. The same is true of the
UUID and POINT disguises (`reader.rs:3459`–`:3473`).

**The owned scan inherits interval/UUID/POINT decoding for free.**
`interval_storage.rs` is dead the day `ListingTable` is.

---

## 2. Does the owned path have an equivalent?

The owned scan is `basin-exec/src/storage_source.rs` →
`basin_storage::read_paths_with_schema`, with its file set pinned from
`meta.live_data_files()` by `owned_engine.rs:1533`–`:1546` (that is what
`553f4f8b` fixed).

`grep -n "tombstone|hot_tier|hottier|memtable|overlay|gin|rtree|index"
crates/basin-exec/src/storage_source.rs` returns **no implementation hits** —
only the doc-comment mention that an empty live set "is what DataFusion's path
does too (it registers an empty `MemTable`)" (`:262`) and the two-index-spaces
prose about projection numbering. The resolver's `open` (`:536`–`:596`) builds
`ReadOptions` with exactly two things: a projection and, if *every* filter
translates, a predicate list. Then `prune_live_files` (`:443`) drops files by
catalog column stats.

| capability | DF-side | owned path | verdict |
|---|---|---|---|
| GIN row-group/row prune | `GinRowGroupPrunedTable` | `ReadOptions.row_group_selection`/`row_selection` exist (`basin-storage/src/lib.rs:335`, `:347`) and are **never set** by `basin-exec` (grep: 0 hits) | **absent — performance only** |
| R-tree bbox prune | `RTreePrunedTable` | same | **absent — performance only** |
| JSONB posting prune | `JsonbPostingPrunedTable` | same | **absent — performance only** |
| Secondary B-tree point lookup | `secondary_index.rs` registry | absent | **absent — performance only** |
| Interval / UUID / POINT decode | `interval_storage.rs` + format wrappers | **present**, in the storage reader the owned scan already calls | **already have it** |
| File-level stats prune | inside `Storage::read` | **present** — `prune_live_files` (`storage_source.rs:443`) | **already have it** |
| Live-vs-existing file set | `live_data_files()` since bug #41 | **present** since `553f4f8b` | **already have it** |
| Hot tombstone suppression | `TombstoneFilterExec`, `TombstoneColdScanExec` | **absent** | **gated, not solved** |
| Hot update overlay | `UpdateOverlayExec` | **absent** | **gated, not solved** |
| Unflushed hot INSERT rows | `HtapUnionTable` MemTable half | **absent** | **gated, not solved** |
| Transaction overlay + hot seq watermark | `HtapUnionTable` + `tx_overlay_peek` | **absent** | **gated at statement level** |
| RLS predicate injection | `apply_rls_to_select` | **absent** | **gated** (`owned_engine.rs:1464`) |
| Soft-delete visibility | `apply_soft_delete_to_select` | **absent** | **NOT GATED — §0.1** |
| CITEXT comparison semantics | `CitextAnalyzerRule` | **absent** | **NOT GATED — §0.2** |
| Enum ordinal ordering | `enum_ordinal::rewrite_enum_ordering` | **absent** | **NOT GATED — §0.3** |

On the three "gated, not solved" rows, the gate is genuinely load-bearing and
genuinely conservative. `total_count()` is `self.inner.read().len()`
(`basin-hottier/src/memtable.rs:690`) — every entry, including tombstones
(`:713`), `Update` overrides (`:720`), dirty rows and **clean, already-flushed
residency rows** (`mark_flushed` retains flushed `Row` entries clean,
`:756`–`:764`). So any footprint at all declines. [21](./21-write-path.md)
§P6 already names the cost of that bluntness: any ordinary small OLTP INSERT
makes the table ineligible for owned reads for `BASIN_HOTTIER_RETAIN_SECS`
(300 s default).

And the flush path closes the loop rather than leaving stale state: tombstones
are applied to the cold tier via copy-on-write at flush
(`basin-hottier/src/flush.rs:477`–`:480`) and then **removed** from the
memtable (`memtable.rs:756`–`:758`), so after a flush the catalog's live file
set is the whole truth again. I found no path where a tombstone outlives its
memtable entry.

**Answer to the sharpest question in the brief:** on tombstones and hot-tier
updates specifically, the owned engine does **not** silently serve deleted or
stale rows — it declines. The silent-wrong-answer risk in this family is real
but sits one row down the table, in soft delete, which nobody gated because it
is a *cold-tier* visibility filter and every gate in `build_resolver` was
written thinking about the hot tier.

---

## 3. What happens today when a query needs one of these

**Index scans: unreachable, not silently slow.** `@>` resolves in the owned
type system (`basin-pgtype/src/operator.rs:608`, OID 3246 jsonb/jsonb→bool),
but `grep -rn "3246\|2751" crates/basin-exec/src/` returns nothing — no
evaluator implements it — and `grep -rni jsonb crates/basin-exec/src/` finds
only comments and the `->`/`->>` operator work. There are no `st_*` functions
in `basin-pgtype` at all. So a GIN, JSONB-posting or R-tree query dies at
build/exec and falls back today; the missing prune has not cost anything yet
because the *predicate* is missing too. That flips the moment the function gap
closes — see §4's ordering note.

**Reachable prune loss exists in exactly one place worth naming:** `LIKE` is
implemented (`basin-exec/src/eval.rs:843`), so a trigram-indexed
`LIKE '%needle%'` **is** servable by the owned engine and **does** silently
skip the trigram row-group prune. That is slow, not wrong — the correct
outcome per the module docs' own promise (`owned_engine.rs:55`–`:59`).

**The probe measures none of this.** The 231-query corpus lives in
`crates/basin-engine/tests/fallback_histogram.rs`. Its fixture
(`:67`–`:140`) creates six tables — `t`, `u`, `d`, `e`, `mb`, `p` — with **no
`CREATE INDEX` of any kind, no JSONB column, no spatial column, no citext, no
enum, and no `SOFT DELETE` column**. So:

- **No served query touches a GIN or R-tree index.** There are none to touch.
- **No served query reads a table with tombstones or hot-tier rows.** The DML
  block sits at `:369`–`:383` and mutates `t` and `p`; running
  `grep -n "FROM p\b\|FROM t\b" … | awk -F: '$1>385'` returns **nothing** —
  every category after DML reads `mb`, `e` and `u` only. The corpus never
  reads a table it mutated.

That is a blind spot in the probe, not a clean bill of health: `206/231`
served says nothing about visibility. The oracle that *does* cover it is
`tests/integration/tests/golden_answers.rs`, whose `storage` suite is
explicitly "the hot-tier/tombstone overlay (UPDATE + DELETE + unflushed INSERT
read back)" (`:52`–`:55`) and which records per-statement whether the owned
engine served (`:600`–`:605`). Any new gate from §0.4 should be recorded there
the same way.

---

## 4. Ranked: what must be built before the umbrella can go

Sizes are estimates, calibrated against the line counts of the incumbent
implementations they replace and [21](./21-write-path.md)'s P-series. Nothing
here was built or benchmarked.

### Tier A — correctness, wrong answers **today**, hours each

| | item | fix now | fix properly |
|---|---|---|---|
| A1 | **Soft-delete rows returned** (§0.1) | gate in `build_resolver`, ~1 h + test | inject `<sd_col> IS NULL` during lowering, ~2 d |
| A2 | **CITEXT compares case-sensitively** (§0.2) | gate, ~1 h | citext-aware `=`/`<>`/ordered/`LIKE`/sort in `basin-exec`, ~1 wk |
| A3 | **Enum orders lexicographically** (§0.3) | gate, ~1 h | ordinal-aware compare/sort keyed on `BASIN_ENUM_TYPE`, ~3 d |

Do the gates this week. They are strictly smaller than one function
implementation and they stop the only *currently shipping* wrong answers this
audit found.

### Tier B — correctness, blocking the umbrella, weeks each

These are gated today and cannot be gated after removal, because there is
nothing left to decline to.

| | item | replaces | est. |
|---|---|---|---|
| B1 | Hot tombstone suppression as an owned operator | `TombstoneFilterExec`, `TombstoneColdScanExec` | **~1.5 wk** |
| B2 | Update overlay as an owned operator (drop-by-PK **and** append post-SET rows, with the filter-interaction rule of `hot_tombstone.rs:579`) | `UpdateOverlayExec` | **~1.5 wk** |
| B3 | Unflushed hot rows unioned into the owned scan | `HtapUnionTable`'s MemTable half | **~1 wk** |
| B4 | Transaction visibility: tx overlay + hot seq watermark + snapshot pin | `HtapUnionTable` + `tx_*_peek` + `snapshot_tombstones(watermark)` | **~3 wk** |
| B5 | Replace the blunt `total_count() != 0` gate with a dirty-only check once B1–B3 land | `owned_engine.rs:1487` | ~2 d |

**B1–B4 ≈ 7 weeks**, and the discount is real but modest: the batch-level
algorithms already exist DataFusion-free (`filter_batch`,
`apply_tombstone_filter_to_batches`, `apply_update_overlay_to_batches`,
`normalize_batch_to_schema`), so the work is the operator shells, the snapshot
plumbing into `StorageTableResolver`, and the projection-augmentation /
limit-suppression rules of `tombstone_cold_scan.rs:138`–`:210`.

This is the same body of work [21](./21-write-path.md) §P6 option 3 identified
from the write side, which is the strongest argument for doing it once,
read-side first: P6 needs a row-identity story, and "teach the owned read path
to apply hot tombstones and update overrides" is the option that removes the
circularity instead of working around it. Nothing in this audit contradicts
that preference; §2 strengthens it, because B1–B4 are owed for **reads** even
if no owned write is ever built.

### Tier C — performance only, and cheaper than it looks

| | item | est. |
|---|---|---|
| C1 | Thread `row_group_selection` / `row_selection` from the existing detectors into the owned scan's `ReadOptions` (covers GIN bloom tier, GIN row tier, JSONB posting, R-tree, trigram) | **~1 wk for all five** |
| C2 | Secondary B-tree point-lookup probe | ~3 d |

C1 is small for a specific, checkable reason: **the prune computation is
already DataFusion-free.** `crates/basin-engine/src/index_probe.rs` is 6,411
lines and `grep -c datafusion` on it returns **0**; the registries live in
`basin-storage/src/index/` (`gin_rowgroup.rs`, `jsonb_posting.rs`, `rtree.rs`,
`trigram_rowgroup.rs`, `btree_citext.rs`); and the delivery channel —
`ReadOptions.row_group_selection` / `row_selection` — is already a
basin-storage concept the owned scan constructs on every open. What is
DataFusion-shaped is only the `TableProvider`/`ExecutionPlan` wrapper that
carries the map, which the owned path does not need at all.

One rule must come along: `apply_gin_pruning_for_query` skips pruning entirely
while the table has live overlay entries, because a pruned provider is a bare
cold reader that neither suppresses stale images nor appends overrides
(`session.rs:5385`–`:5395`, using `table_has_live_overlay` at `:5327`). The
owned port needs the same interlock — or B1–B3 first, which subsumes it.

**Ordering note.** C1 has no user-visible value until the *predicates* exist
(§3: `@>` and `st_*` are unimplemented in `basin-exec`). But the day the
function work lands `@>`, a jsonb-containment query becomes servable **and**
becomes a full table decode. C1 should ship in the same phase as the JSONB
operator work, not before it and not long after.

### Tier D — dead with DataFusion, delete rather than port

| file | lines | why it disappears |
|---|---:|---|
| `interval_storage.rs` | 336 | exists only because `ListingTable` bypasses basin-storage's reader; the reader's own inverse (`reader.rs:3761`–`:3849`) already serves the owned scan |
| `gin_rowgroup_scan.rs` | 396 | delivery shell; the prune computation survives in `index_probe.rs` + `basin-storage/src/index/` |
| `rtree_rowgroup_scan.rs` | 335 | same |
| `jsonb_posting_scan.rs` | 272 | same |
| `tombstone_cold_scan.rs` | 422 | shell; `filter_batch` survives and is what B1 reuses |
| `TombstoneFilteringTable`, `HtapUnionTable` | — | provider shells; the batch helpers and the watermark logic survive into B1–B4 |

**~1,761 lines of the 89-error `ExecutionPlan` surface are shells whose
substance lives elsewhere.** That is the cheerful half of this audit: the
umbrella cut is smaller than the file list suggests, *provided* B1–B4 are
built first.

---

## 5. Is this a bigger blocker than the function work?

Split the answer, because the two halves point opposite ways.

**The index half is not a blocker at all.** It is ~1.5 weeks, it is
performance-only by the DataFusion nodes' own stated contracts ("no false
negatives are possible", three times over), the computation is already
DataFusion-free, and today the queries that would use it cannot be served
anyway. Anyone budgeting the umbrella cut from the file list is
over-estimating this half substantially.

**The visibility half is a bigger blocker than the function work, yes.**
Roughly 7 weeks of new operators plus snapshot plumbing, against
[19](./19-expires-at-removal.md)'s function backlog, which is real but is a
list of independent, individually-small, individually-testable items with a
clear oracle (a live PostgreSQL). B1–B4 have no such oracle — PostgreSQL has
no opinion about Basin's hot tier, which is exactly why
`golden_answers.rs`'s `storage` suite exists — and they carry the risk
`553f4f8b` demonstrated: a second implementation of visibility that disagrees
with the first in a window nobody thought to test.

**But the finding that should change this week's plan is neither of those.**
It is §0: the gate that makes "the owned engine declines rather than lies"
true was written against the hot tier and never extended to the cold-tier
read-time rewrites — soft delete, citext, enum ordinal. Three conditions,
about an hour each, sitting next to the RLS check that is already there. Until
they land, `BASIN_OWNED_ENGINE=1` returns deleted rows on any table declared
with `SOFT DELETE`, and that is not a migration cost — it is a bug.

**A rule worth adopting alongside them:** every read-time rewrite the
DataFusion `SELECT` pipeline applies is a candidate silent divergence, and the
pipeline is enumerable — `run_full_rewrite_pipeline` (`executor.rs:874`, whose
enum pass sits at `:1091`) and the `apply_*_to_select` chain
(`executor.rs:11481`–`:11484`). This
audit walked that chain by hand and found three gaps. A test that walks it
mechanically would find the fourth before a user does.
