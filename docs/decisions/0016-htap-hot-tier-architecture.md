---
title: "ADR 0016 — HTAP hot tier architecture"
nav_section: decisions
sidebar_position: 16
summary: "Row-format LSM-style memtable for recent writes, in a new basin-hottier crate. Closes the OLTP gap (point_eq, single-row UPDATE) without sacrificing OLAP wins on Vortex."
tags: [storage, performance, htap]
---

# 0016 — HTAP hot tier architecture

- **Status:** Accepted (2026-05-19) — implementation Phase 5.14.C, sub-items C1–C6 in TASK.md.
- **Tags:** storage, performance, htap, multi-project
- **Supersedes:** none
- **Cross-references:** [ADR 0015 (Vortex storage format)](./0015-vortex-storage-format.md), [ADR 0007 (Connection pooling)](./0007-connection-pooling.md), [ADR 0008 (Noisy-neighbor fairness)](./0008-noisy-neighbor-fairness.md), [ADR 0010 (Catalog replication)](./0010-catalog-replication.md)

## Context

Basin today writes every INSERT and UPDATE as an immutable Vortex (or
Parquet) object-store file. The 88-shape `vortex_vs_parquet_smoke`
benchmark in release mode shows Vortex wins decisively on analytical
workloads (aggregate_full 38×, four-way join 1.43×, three-way join
1.30×, dict equality 1.29×) but loses on point queries at scale:
`point_eq` is 0.94× at 10k rows, 0.85× at 25k, **0.49× at 100k**.
Single-row UPDATE / DELETE pay full Vortex chunk-rewrite cost via
`dml_mutate.rs`'s copy-on-write path.

This is the structural OLTP gap. Columnar object-store files are the
wrong shape for row-at-a-time random access — and the gap widens
exactly as a project's data grows. Without an architectural answer,
Basin remains an OLAP-first system that has to disclose the OLTP
tax to every customer.

The standard HTAP recipe — used by SingleStore, ClickHouse's
ReplacingMergeTree, MariaDB ColumnStore, and Apache Pinot — is to
pair the columnar cold tier with a row-format hot buffer: recent
writes land in an in-memory memtable, reads merge memtable + columnar,
the memtable flushes to columnar on size/age thresholds. The cold
tier keeps its OLAP wins; the hot tier closes the OLTP gap for
recent + frequently-accessed rows.

## Decision

Add a row-format LSM-style memtable, in a new `crates/basin-hottier/`
crate. One memtable per `(project_id, table_name)` keyed by primary-key
columns, backed by `parking_lot::RwLock<BTreeMap<RowKey, MemRowValue>>`.

### Memtable data structure

- **Backing structure:** Rust `std::collections::BTreeMap`, wrapped in
  `parking_lot::RwLock`. Custom in-process B-tree map, not RocksDB or
  sled. Rationale below.
- **Grain:** one memtable per `(project_id, table_name)`. Matches
  Vortex's read unit and `PartitionState`'s ownership grain.
- **Key:** big-endian encoded primary-key columns in declaration
  order, packaged as a `RowKey(Vec<u8>)` newtype. Lexicographic byte
  order matches PK sort order for integer types; same encoding the
  Parquet/Vortex writer already produces for cluster-key sort.
- **Value:** `MemRowValue::Row(Vec<u8>)` (encoded row) or
  `MemRowValue::Tombstone`. No version chain — last write wins
  (Basin's existing isolation level is snapshot isolation, not
  row-level MVCC).
- **Fallback for tables with no declared PK:** monotonic `rowid: u64`
  assigned at insert time. UPDATE on no-PK tables cannot upsert —
  the row's new value is inserted under a new rowid and the old
  rowid is tombstoned. Document this as a HTAP user requirement:
  declare a PK to get UPDATE p99 in the OLTP target band.

### Multi-project isolation

Follows the same pattern as `ProjectCounterRegistry`
(from the multi-project isolation feedback review; the note itself was a
working file outside this repository and is not tracked here):
**shared heavy resource + cheap per-project primitive.**

- **Shared heavy resource:** one `MemTableRegistry` per process. Lazy
  `DashMap<(ProjectId, TableName), Arc<MemTableEntry>>`. Allocation
  only on first write to a `(project, table)` pair; idle projects
  cost zero memory.
- **Cheap per-project primitive:** per-`MemTableEntry`
  `AtomicU64 bytes_allocated` counter + per-project `Semaphore` for
  hard-cap back-pressure. No per-project thread pool, no per-project
  allocator.
- **Cost:** O(bytes of actual data) + one counter + one semaphore per
  active project. At 10k inactive projects, the registry is a 10k-entry
  `DashMap` with zero-byte counters — sub-MB overhead.

### Memory budgets

```
project_memtable_hard_cap = 256 MB   (default, ALTER PROJECT configurable)
project_memtable_soft_cap = 192 MB   (75% of hard cap; triggers flush)
table_memtable_soft_cap   = 16 MB    (per-table flush target)
memtable_max_age_secs     = 60       (age-based flush trigger)
```

Hard cap blocks new writes on the per-project semaphore until flush
completes. Soft cap fires a background flush of the project's
largest single-table memtable (largest-first scheduling; alternative
LRU policy is configurable but penalises write-light projects, so
not the default).

### Write path

```
INSERT row:
  1. WAL append (durability anchor, existing basin-wal path)
  2. MemTableRegistry::get_or_insert(project, table) → Arc<MemTableEntry>
  3. MemTableEntry::insert(pk_key, row_bytes) under write lock
     (lock held for O(log n) BTreeMap insert; never held for I/O)
  4. bytes_allocated += row_bytes.len() (atomic)
  5. Ack to client

UPDATE row:
  1. WAL append
  2. MemTableEntry::upsert(pk_key, new_row_bytes) — overwrites or
     inserts (the row may have been in Vortex; the memtable version
     becomes authoritative for that PK)
  3. Ack

DELETE row:
  1. WAL append
  2. MemTableEntry::delete(pk_key) → MemRowValue::Tombstone
  3. Ack
```

### Transaction integration

The current `TxState` tracks `pending_files: HashMap<TableName, Vec<DataFileRef>>`
(committed object-store files awaiting catalog commit). With the hot
tier, within-transaction writes go to the memtable, not to Vortex files.

- `BEGIN`: snapshot the per-table memtable watermark (`(project, table)
  → memtable.size()`) into `TxState::memtable_watermarks`.
- Statement execution: writes land in the memtable, visible to
  within-tx reads immediately (read-your-own-writes).
- `ROLLBACK`: truncate each touched table's memtable back to its
  watermark. O(rows inserted in tx); no object-store I/O.
- `COMMIT`: no special action — memtable rows are already there.
  Background flush drains them on its normal schedule.
- **SAVEPOINT/ROLLBACK TO**: extend `SavepointFrame` with
  `memtable_watermarks: HashMap<TableName, usize>`, parallel to
  `file_offsets`.

**Critical correctness requirement:** WAL replay on crash recovery
currently replays every entry without transaction awareness. The hot
tier requires adding `BEGIN` / `ROLLBACK` markers to the WAL and
teaching `replay_wal_into` to suppress entries between
`BEGIN` and matching `ROLLBACK`. This is a pre-condition; if deferred,
rolled-back writes would be re-applied on restart. Scoped into
sub-item C2.

### Read path

```
Point lookup (fast_select.rs::execute_simple_select):
  1. Probe MemTableEntry by PK — O(log n)
  2. Hit + Row(_):       return memtable row (sub-ms)
  3. Hit + Tombstone:    return empty (row was deleted)
  4. Miss:               fall through to Vortex scan
                         (existing bloom-filter + decode path)

Range scan:
  1. memtable.range(lo..=hi) → in-order iterator (BTreeMap)
  2. Vortex scan for same range (existing path)
  3. Merge in PK order; memtable wins on duplicate PKs; tombstones
     suppress Vortex rows.

Full table scan:
  1. Collect non-tombstone memtable rows (one RecordBatch)
  2. Vortex full scan (existing path)
  3. Merge with PK-dedup; for OLAP shapes that don't care about
     order (aggregate_full, group_by), skip the sort.
```

### Flush

Three-condition trigger fires a flush for a `(project, table)`:

1. **Size**: `MemTableEntry.bytes_allocated > table_memtable_soft_cap`
2. **Age**: oldest entry exceeds `memtable_max_age_secs`
3. **Scan pressure**: a full-table scan sees > 100k rows in memtable

Algorithm (background Tokio task, one per process, NOT one per
table — bounded concurrency):

```
1. Snapshot memtable under write lock (clone generation-bounded view;
   release lock); never hold the lock for I/O
2. Partition snapshot rows into:
   - New (PK not previously in Vortex)
   - Updated (PK present in Vortex)
   - Tombstones (PK present in Vortex, now deleted)
3. Write new rows via the existing write_batch_with_options path
4. Apply updates/tombstones via the existing dml_mutate copy-on-write
   path (which already prunes files with no matching rows)
5. Atomic catalog commit (existing replace_data_files + append_data_files
   under optimistic concurrency control, with one retry on conflict)
6. GC flushed rows from live memtable (write lock briefly held)
7. Truncate WAL segment for flushed entries
```

The flush is **non-blocking for writes**: the write lock is held only
during snapshot clone (step 1) and final GC (step 6); object-store
I/O happens lock-free. New writes during a flush land in the
memtable's next generation and are not part of the in-flight flush.

### Compaction interaction

Each flush creates one Vortex file. A busy table at 1 flush/minute
accumulates 60 files/hour. The existing `compact_all` background
loop (`InProcessShard`) already merges small files; the hot-tier
flushed files are picked up automatically. Recommend compaction
policy: merge files < 128 MB into one per table, triggered every
5 flushes.

### Catalog integration

`TableMetadata` gains `memtable_stats: Option<MemtableStats>` with
`{ last_known_bytes, last_flush_row_count, last_flush_at }`. The
memtable itself is in-process; the catalog only sees persisted
Vortex files. Catalog snapshots include only committed Vortex files,
NOT in-flight memtable rows — this is intentional: the memtable is
a durability-anchored buffer (WAL is durable), not a catalog snapshot.

## Alternatives considered

### RocksDB-backed memtable

**Rejected.** RocksDB is a process-level singleton with shared
background threads, block cache, and WAL. Wiring it for Basin would
require either:

- One RocksDB instance per `(project, table)` — multiplies file
  descriptors and threads by the table count, violating the
  multi-project isolation constraint (O(bytes), not O(pool)).
- One shared RocksDB with a key-prefix scheme — makes per-project
  memory budgets difficult and introduces noisy-neighbor contention
  on the shared compaction thread.

Plus: C++ FFI build complexity, second WAL conflicts with `basin-wal`,
shared block cache makes per-project accounting hard.

### sled-backed memtable

**Rejected.** sled is on-disk by design — adds a second durability
path that duplicates `basin-wal`'s job. Its 1.0-alpha-for-years
maintenance status is a dependency risk for a load-bearing component.

### crossbeam-skiplist `SkipMap`

**Reserved as fallback.** Lock-free reads, O(log n). If the C1
benchmark shows BTreeMap p99 point lookup exceeds 500 µs at 1M
rows, switch to `crossbeam-skiplist` before C2 lands. Don't switch
without the benchmark — premature optimisation.

### MVCC row versioning (multi-version per PK)

**Rejected for Phase 5.14.C.** Basin's current isolation level is
snapshot isolation at the catalog snapshot level, not row-level MVCC.
Latest-write-wins per PK in the memtable matches that model. If a
future phase requires read-your-own-writes across concurrent sessions
or PostgreSQL-style MVCC, add a version chain then; not now.

## Decomposition (Phase 5.14.C sub-items)

Six PR-sized sub-items, total ~8 engineer-weeks. Detailed file
scopes + acceptance gates in [`TASK.md`](../TASK.md) Phase 5.14.C.

| Sub-item | What | Depends on | ~Effort |
|---|---|---|---|
| C1 | `MemTable` + `MemTableRegistry` (new crate) | none | 1w |
| C2 | Write-path integration (INSERT/UPDATE/DELETE → memtable; tx watermarks; WAL boundary markers) | C1 | 1.5w |
| C3 | Read-merge path (point lookup probe + range merge + full-scan merge) | C1+C2 | 1.5w |
| C4 | Flush task (size/age/scan triggers; non-blocking algorithm) | C1+C2+C3 | 2w |
| C5 | Per-project memory budget enforcement (caps + back-pressure + largest-first flush) | C1 | 1w |
| C6 | Differential harness (all 88 shapes × 3 modes: empty/memtable/split) | C1+C2+C3 | 1w |

## Risks and open questions

- **PK requirement for HTAP UPDATE p99.** Tables without a declared
  PK can use the rowid fallback but cannot upsert. Document this as
  a HTAP user requirement.
- **Large transactions exceeding hard cap.** Currently blocks (semaphore
  back-pressure). For transactions larger than the total project cap,
  return a clear `resources_exceeded` error. Spill-to-temp-Vortex
  semantics are deferred to a future phase.
- **Schema evolution during memtable lifetime.** If `ALTER TABLE ADD
  COLUMN` runs after memtable rows are written but before flush, the
  merge reader must handle schema mismatch (cast to new schema, NULL
  for absent columns). Open design point for C3.
- **Crash recovery time at pathological scale.** Worst case 16 GB of
  WAL replay (1k tables × 16 MB soft cap). Local NVMe ~8s; object
  storage with parallel replay ~30s. Mitigate by keeping soft cap
  small and age threshold tight (30s).
- **Flush-vs-concurrent-write contention.** Multiple retries possible
  on `commit_with_retry`. Per-table flush-exclusive lock prevents
  thrashing; design into C4.

## Out of scope

1. Cross-table secondary indexes from the memtable (separate phase)
2. Memtable persistence to a separate on-disk format (durability is
   `basin-wal`; memtable is in-memory only)
3. MVCC row versioning (latest-write-wins per PK)
4. Cross-region memtable replication (memtable is process-local;
   cross-region is ADR 0009 / 0010 territory; flushed Vortex files
   replicate via the existing object-store replication path)
5. User-configurable per-table memtable cap (project-level caps only
   in Phase 5.14.C; per-table caps deferred)
6. Vectorised/SIMD memtable scans (plain Rust BTreeMap iteration)

## Schema evolution policy (addendum 2026-05-19)

`ALTER TABLE` semantics when memtable rows are present.  The earlier
draft noted this as an open question; this addendum locks it.

| ALTER form | Memtable handling |
|---|---|
| `ADD COLUMN col TYPE` (no default) | Read NULL for pre-ALTER memtable rows; no rewrite needed. |
| `ADD COLUMN col TYPE DEFAULT <constant>` | Read the literal constant for pre-ALTER rows; catalog stores the DEFAULT alongside the column schema; merge reader applies at read time. |
| `ADD COLUMN col TYPE DEFAULT <non-constant>` (e.g. `now()`, `random()`) | **Reject** with `cannot ALTER while hot tier non-empty; flush first` if any project memtable for the table is non-empty.  User invokes `SELECT pg_force_flush('schema.table')` (Phase 5.14.C4 ships this helper) then retries.  Same path Postgres takes internally when it must rewrite the heap. |
| `DROP COLUMN` | Hide the column on read.  Catalog records the column as dropped at `schema_version`; merge reader skips it. |
| `ALTER COLUMN ... TYPE ...` | Same as non-constant DEFAULT: reject if memtable non-empty, force flush. |

Implementation: every memtable row carries a `schema_version: u32`
written at insert time (cheap — 4 bytes, fits in row metadata).  The
catalog tracks `schema_version -> SchemaDelta { added: Vec<(name,
type, default)>, dropped: Vec<name>, retyped: ... }`.  On read, the
merge path applies the delta chain forward from the row's
`schema_version` to the current schema.  This is the same pattern
Iceberg uses for schema evolution at the file level — we're applying
it to in-memory rows.

The catalog DDL path already serialises with the engine's write path
(both go through `Catalog::commit_with_retry`), so there is no
race where ALTER lands mid-write.

## What would change our mind

- **BTreeMap benchmark fails the 500 µs p99 target at 1M rows** → switch
  to `crossbeam-skiplist::SkipMap`.
- **Multi-region requirements arrive before Phase 5.14.C ships** →
  re-evaluate memtable durability story; may need process-level
  replication of memtable state.
- **MVCC required for concurrent multi-session reads of recent writes**
  → add a version chain to `MemRowValue`.
- **Customer demand for table-level memtable caps** → add
  `ALTER TABLE … SET memtable_cap` DDL.
