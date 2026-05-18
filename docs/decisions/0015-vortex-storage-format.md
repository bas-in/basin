# 0015 — Vortex as opt-in per-table storage format (Parquet stays default)

- **Status:** Accepted (Parquet remains the default this phase; lead may revise if the default flips later)
- **Tags:** architecture, storage, file-format, performance, read-compat

## Context

Basin writes table data as ZSTD-compressed Parquet through a `FileFormat`
abstraction in `crates/basin-storage`. Parquet is the lingua franca of the
open lakehouse: Iceberg metadata points at Parquet data files, and external
readers — Athena, Spark, DuckDB — consume Basin's tables today precisely
because the bytes on object storage are standard Parquet. That read-compat is
load-bearing for the wedge ("drop your existing schema in, query it from the
tools you already have").

At the same time, Basin's target workload is audit-log / agent-trace data:
high-cardinality string columns, low-cardinality enum-like columns, and a wide
JSON payload column. This is exactly the shape Vortex's BtrBlocks cascade
compresses and scans best. The established `benchmark/vortex_compare` harness
quantifies the gap on a 1,000,000-row synthetic audit-log dataset
(`id i64, ts utf8, actor utf8 (8-card), event utf8 (4-card), body ~120 B
JSON`), re-run post arrow58/df53 migration at commit `5e21d13` and recorded in
[`benchmark/RESULTS_vortex_migration.md`](../../benchmark/RESULTS_vortex_migration.md):

- **On-disk size:** ZSTD Parquet = 3,174,879 bytes (3.03 MiB); Vortex
  (BtrBlocks + `.with_compact()`) = 1,628,404 bytes (1.55 MiB) — a **1.950×**
  size reduction, deterministic across all five runs.
- **Full-scan latency (5 cold runs, macOS LocalFS):** Parquet p50 = 66.5 ms /
  Vortex p50 = 3.3 ms → **20.1× p50**; Parquet p95 = 75.5 ms / Vortex p95 =
  3.5 ms → **24.3× p95**. This exceeds the prior 18× baseline; the migration
  introduced no regression (size ratio 1.950× matches baseline exactly).
- **Transitive CSV/Vortex bound:** ≥ **39×** (20.22× CSV/Parquet from the main
  integration suite × 1.95× Parquet/Vortex).

(The benchmark note flags that macOS buffer-cache behaviour likely widens the
scan margin from the ~18× baseline to ~20× p50; the result is comfortably
above the 18× bar regardless. No figure is cited here that is not in
`RESULTS_vortex_migration.md`.)

Forcing every table onto Vortex would forfeit external read-compat for tables
that need it. Keeping every table on Parquet leaves the audit-log win on the
table. The resolution is per-table format selection with Parquet as the safe
default.

## Decision

Ship Vortex as an **opt-in, per-table** storage format. Parquet stays the
default for every table that does not explicitly request otherwise.

### Selection surface

A table opts into Vortex at creation:

```sql
CREATE TABLE audit (...) WITH (basin.file_format = 'vortex');
```

The chosen format is recorded in catalog table metadata. Absent or
unrecognised → Parquet. The format is fixed for the life of the table: every
data file Basin writes for that table uses the recorded format.

### Write path — codec

Vortex data files are produced by the `vortex` 0.70 crate, pinned on the
workspace's arrow 58 / DataFusion 53 toolchain. Compression is the **BtrBlocks
cascade** with `.with_compact()` enabled: Zstd for string columns, Pco for
numeric columns. This is the exact configuration the benchmark measured, so
the 1.950× size figure is the production codec, not a tuned-for-benchmark
variant.

### Read path

Reads go through `vortex-datafusion` 0.70's `VortexFormat`, which implements
the DataFusion 53 `FileFormat` trait — the same trait the Parquet path
implements. The query engine selects the format implementation per table from
the recorded catalog metadata; the planner, optimiser, and scan operators are
otherwise unchanged. New module: `crates/basin-storage/src/vortex_format.rs`.

### Single format per table

A table is wholly Parquet or wholly Vortex. **Mixed-format tables (some data
files Parquet, some Vortex within one table) are explicitly DEFERRED.** This
keeps the read-side provider trivial — one `FileFormat` per table, chosen once
— and bounds the migration surface.

### Back-compatibility

Every table created before this ADR has no recorded `basin.file_format` in its
catalog metadata. The resolver treats absent/unrecognised format as Parquet,
so all existing tables and their existing data files continue to read and
write as Parquet with zero migration. Iceberg / Athena / Spark / DuckDB
read-compat is preserved for every default (Parquet) table.

### Deferred

- **Native Vortex predicate pushdown.** The current read path is
  decode-then-filter: Vortex blocks are decoded and DataFusion applies the
  predicate. Pushing predicates into Vortex's own pruning/zone-map machinery
  is deferred to a follow-up.
- **In-place conversion of a populated table.** There is no
  `ALTER TABLE … SET (basin.file_format = …)` that rewrites existing data
  files. Format is chosen at `CREATE TABLE`. Migrating a populated table means
  create-new + copy.
- **Mixed-format provider.** Per the "single format per table" decision above;
  revisit only if in-place conversion ships and needs a transitional
  both-formats-readable window.
- **Vortex as the global default.** Blocked, empirically (see Alternatives →
  "Flip the default to Vortex now"): the schema-less internal read paths
  (`read_file`/`read_paths`: continuous-view refresh, cron state, system
  tables) cannot decode Vortex without an externally-supplied Arrow schema, so
  a blanket default regressed 33 test targets. Prerequisite: a self-describing
  Vortex decode path (or schema-threading for every internal reader) plus a
  Parquet export lane for external read-compat. Until then Vortex is opt-in
  only and Parquet remains the default.

## Alternatives considered

**Flip the default to Vortex now.**
The benchmark margin (1.950× smaller, 20.1× p50 faster scan) is large enough
to be tempting, and the flip was prototyped and measured end-to-end. Rejected
this phase on two grounds, the second empirically proven:

1. It silently breaks Iceberg / Athena / Spark / DuckDB read-compat for every
   table, which is a wedge-load-bearing property.
2. **Vortex is not self-describing on Basin's non-table-aware read paths.**
   The Vortex decode path requires the table's Arrow schema to be supplied
   externally (Parquet embeds its schema in the file footer; Vortex, as
   integrated, does not recover it standalone). Basin's table-aware SELECT
   path threads the catalog schema and works (validated by codec round-trip
   and the end-to-end `vortex_select` test). But internal/system read paths —
   continuous-view incremental refresh, cron-job state, and other
   `read_file`/`read_paths` callers that pass no schema — error with
   `catalog schema required to decode Vortex files`. Flipping the global
   default routed those paths onto Vortex and regressed **33 test targets**
   (vs. ~8 pre-existing baseline failures); the opt-in path itself has **zero**
   regressions. The numbers were captured on the prototype flip and the flip
   was reverted.

Revisiting the default requires either a self-describing Vortex read path for
the schema-less internal callers, or routing every internal reader through a
catalog-schema-aware path, plus a Parquet export lane for external read-compat.
Tracked under Deferred below.

**Mixed-format tables (per-file format within a table).**
Would allow gradual in-place migration of a populated table. Rejected for now:
the read provider has to fan out to two `FileFormat` implementations and merge,
which complicates projection/predicate handling for a capability nobody has
asked for yet. Deferred, not designed out.

**A separate Vortex-only table type / storage engine.**
Parallel storage engine with its own catalog and planner integration. Far more
surface than reusing the existing `FileFormat` trait; the per-table-format
approach gets the same user-visible outcome by changing one trait
implementation, not a second engine.

**Side-car Vortex copies of Parquet tables (dual-write).**
Write both formats, read whichever is faster. Doubles write cost and storage,
and the catalog has to track two file sets per table. The opt-in single-format
approach delivers the win without the dual-write tax.

## Consequences

**Positive**

- Audit-log / trace-shaped tables can opt into a **1.950×** smaller on-disk
  footprint and a **20.1× p50 / 24.3× p95** faster full scan (figures from
  `RESULTS_vortex_migration.md`, post-migration commit `5e21d13`), with no
  change to query SQL — only the `CREATE TABLE` clause.
- Default tables keep standard ZSTD Parquet, so Iceberg / Athena / Spark /
  DuckDB read-compat is unaffected for everything that does not opt in.
- Reuses the existing `FileFormat` trait surface; the engine selects the
  implementation per table. No second storage engine, no planner fork.
- Zero migration for existing tables: absent recorded format → Parquet,
  byte-for-byte unchanged.
- The opt-in single-format decision keeps the read provider trivial — one
  format per table, resolved once from catalog metadata.

**Negative / risks**

- Vortex tables are not externally readable by Iceberg / Athena / Spark /
  DuckDB; a user who opts in trades read-compat for size/scan. This is
  documented as the explicit trade of the `WITH` clause.
- Predicate pushdown is decode-then-filter today (see Deferred). Selective
  point queries on a Vortex table do not yet benefit from Vortex zone-map
  pruning; the measured win is a full-scan / size win.
- A populated table cannot be converted in place; switching format means
  create-new + copy until in-place conversion ships.
- New pinned dependency surface: `vortex` 0.70 and `vortex-datafusion` 0.70
  must track the workspace arrow 58 / DataFusion 53 toolchain on every upgrade.
- The benchmark is single-node macOS LocalFS with partial buffer-cache
  warming between runs; object-store cold numbers will differ. The size ratio
  (1.950×) is deterministic and toolchain-independent; the scan ratio is the
  softer of the two figures.

## Precedent

| System | Storage-format strategy |
|---|---|
| **SmithDB** (LangChain) | Rust + DataFusion + Vortex + object-store; ships LangSmith's agent-trace workload on Vortex in production. Closest analogue — same engine stack, same trace-shaped workload, same object-store constraints. |
| **DuckDB** | Native columnar format internally; reads/writes Parquet for interop. Validates "fast native format + Parquet as the interop boundary." |
| **Apache Iceberg** | Format-agnostic table metadata (Parquet / ORC / Avro per file); Basin's per-table single-format choice is the conservative subset of Iceberg's per-file flexibility. |

Basin follows SmithDB's path most closely — Vortex on the same Rust /
DataFusion / object-store stack for the same audit-/trace-shaped workload —
while keeping Parquet as the default for the read-compat the wedge depends on.

## References

- [`benchmark/RESULTS_vortex_migration.md`](../../benchmark/RESULTS_vortex_migration.md)
  — post-migration size (1.950×) and scan (p50 20.1×, p95 24.3×) figures cited
  here; harness `benchmark/vortex_compare`, commit `5e21d13`
- [`WEDGE.md`](../../WEDGE.md) — item 8, Phase 5.12.C: "Vortex as opt-in
  storage format (ADR 0015)"
- [`TASK.md`](../../TASK.md) — Phase 5.12 SmithDB-inspired storage
  optimizations
- [`crates/basin-storage/src/vortex_format.rs`](../../crates/basin-storage/src/vortex_format.rs)
  — `VortexFormat` (`vortex-datafusion` 0.70) implementing the DataFusion 53
  `FileFormat` trait; BtrBlocks + `.with_compact()` write path
- [`docs/decisions/0014-pg-query-as-canonical-parser.md`](./0014-pg-query-as-canonical-parser.md)
  — sibling ADR; same house structure
