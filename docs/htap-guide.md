---
title: "HTAP guide — hybrid transactional + analytical queries in Basin"
nav_section: operations
sidebar_position: 4
summary: "Hot-tier + columnar storage for point queries, recent writes, and aggregates in one engine. When to declare basin.sort_by, when to ALTER PROJECT memtable caps, how to read the latency story."
---

# HTAP guide — hybrid transactional + analytical queries in Basin

## What HTAP means in Basin

Most analytical databases are built around one assumption: queries read a lot of data, touch many rows, and return aggregated results. That shape — columnar files on disk, scans that read only the columns they need — is exactly what makes tools like BigQuery or DuckDB fast for `SUM(revenue) GROUP BY month` over a billion rows. But ask those same databases for a single row by primary key and the answer comes back slowly, because finding one row inside a columnar file means touching a lot of irrelevant data first.

Most transactional databases face the mirror problem. Postgres is extraordinarily good at "fetch row 42" or "update user 99's balance". It is much slower at "compute the rolling seven-day average across every order for every customer" — not because of a bug, but because row-oriented heap storage requires reading every column of every row even if the query only touches two.

Basin runs both workloads in one engine. The key architectural move is the **hot tier**: a RAM-resident, row-oriented memtable that holds recent writes, indexed by primary key. The **cold tier** is the existing Vortex columnar store on object storage. When you query Basin, the read path merges both tiers transparently. You do not choose a mode, set a flag, or route queries differently — the planner observes what the query needs and decides whether to start from the hot tier, the cold tier, or merge both.

The practical effect is that Basin handles "what is the current balance for user 42?" and "what was the total payment volume by country last year?" from the same table, without you maintaining a separate OLTP sidecar. Point queries on recent inserts are sub-millisecond. Aggregate scans over years of history are sub-second on terabyte-scale data. Range queries that straddle both — say, "all orders from the last two hours with subtotals" — are automatically merged without any query rewriting from you.

This design follows the same architectural recipe used by SingleStore, ClickHouse's ReplacingMergeTree, and Apache Pinot, applied within Basin's multi-project, object-store-native architecture. The reference design is [ADR 0016](./decisions/0016-htap-hot-tier-architecture.md).

---

## The two tiers

### Hot tier

The hot tier is a row-oriented in-memory memtable per `(project, table)` pair. Every INSERT, UPDATE, and DELETE lands here first after being recorded in the WAL. The memtable is backed by a sorted B-tree keyed on the table's primary key columns, which means point lookups are O(log n) and range scans return results in PK order without sorting.

Because the memtable is RAM-resident and indexed, point queries against recently written rows are sub-millisecond: the engine probes the B-tree, finds the row (or a tombstone, if the row was deleted), and returns immediately without touching any disk file. UPDATE semantics in the hot tier are last-write-wins per primary key — the memtable row becomes the authoritative version of that PK, overriding whatever the cold tier holds, until the next flush.

Sub-millisecond hot-tier performance for UPDATE and point lookup requires a declared primary key. Tables without a `PRIMARY KEY` clause fall back to an internal `rowid` and cannot upsert — each UPDATE inserts a new rowid and tombstones the old one. This is correct but slower. **Declare a primary key on any table where OLTP latency matters.**

### Cold tier

The cold tier is the Vortex columnar file store that Basin has used since its initial storage design ([ADR 0015](./decisions/0015-vortex-storage-format.md)). Data is stored column-by-column inside object-storage files. A `SUM(amount)` query reads only the `amount` column; the `name`, `address`, and `metadata` columns are never touched. At terabyte scale this distinction is the difference between scanning gigabytes and scanning megabytes.

Each Vortex file carries zone maps (min/max statistics per column per file) and bloom filters for high-cardinality columns. When you declare `basin.sort_by = 'created_at'` on a table, the cold-tier files are written in that sort order, and the zone maps become tightly packed: a point query for a specific timestamp range can skip the vast majority of files without reading them. Without `basin.sort_by`, zone maps still help on many queries but are less selective.

Cold-tier performance for point queries depends heavily on whether you have declared a sort key. A point query on a sorted table takes 1–10 ms (file skip + bloom probe + one chunk decode). The same query on an unsorted table at large scale requires scanning more files and takes longer.

### Compaction

Every flush of the hot tier produces one Vortex file. A table receiving continuous writes will accumulate many small files over time. A background compaction loop (one per process, bounded concurrency) merges files smaller than 128 MB into consolidated files, triggered approximately every five flushes. This happens automatically — you do not manage it. The only effect visible to you is that query performance on cold-tier data is consistently good regardless of write cadence. Compaction does not block reads or writes.

### Read path

Every Basin query merges the hot and cold tiers. For a point lookup, the engine probes the memtable first. A hit (row or tombstone) returns immediately without touching the cold tier. A miss falls through to the Vortex bloom-filter + zone-map path. For a range scan, the engine takes the ordered memtable range in parallel with the Vortex file range, merges on primary key, and resolves conflicts: the memtable version wins. Tombstones suppress any matching Vortex rows. For full-table analytical scans, the memtable contributes one small batch of in-flight rows; the bulk of the data comes from Vortex. Recent writes — inserts committed seconds ago — are visible immediately in all query shapes.

---

## What you do as a user

Basin's HTAP behaviour is on by default. For most tables you do not need to change anything. The following DDL knobs let you tune for your workload.

### `WITH (basin.sort_by = 'column')`

Declare the natural ordering of your data at table creation time. Basin writes cold-tier files in this column's sort order and builds tighter zone maps against it.

```sql
CREATE TABLE events (
    id         BIGINT PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    payload    JSONB
) WITH (basin.sort_by = 'created_at');
```

When to use it: on any column your queries commonly filter by range, or any column you use for point lookups after the row has left the hot tier. `created_at`, `order_id`, `user_id` on time-series and event tables are the most common choices.

When to skip it: columns with very low cardinality (e.g., a boolean `is_active` with two values) produce no useful zone map compression — the min/max range spans the entire table and no files can be skipped.

`basin.sort_by` is declared at `CREATE TABLE` and affects all future writes. It does not retroactively re-sort existing data; a `VACUUM FULL` or a manual data reload is required to re-sort historical files.

### `WITH (basin.row_block_size = N)`

Controls the row-block granularity inside cold-tier Vortex files. The default is tuned for the common case (mixed scan and point workload). Only adjust this if you have profiled a specific query and have a clear hypothesis. Increasing `row_block_size` improves sequential scan throughput at the cost of larger minimum read amplification for point queries; decreasing it does the reverse.

### `ALTER PROJECT SET memtable_hard_cap = '256MB'`

Sets the total hot-tier memory budget across all tables in the project. Default is 256 MB. When the project's memtable reaches this limit, new writes block until the background flush makes room. This is enforced per-project — other projects are unaffected.

Increase this if your workload has bursty write spikes and you see write latency spikes at flush time. Do not set this above 50% of your process RAM — the memtable competes with query buffers, connection state, and OS page cache.

### `ALTER PROJECT SET memtable_soft_cap = '192MB'`

The threshold at which Basin begins proactively flushing the project's largest single-table memtable. Default is 192 MB (75% of the default hard cap). Flushing at the soft cap prevents the hard cap from being hit under normal steady-state load. If you increase `memtable_hard_cap`, increase `memtable_soft_cap` to roughly 75% of the new value.

### `ALTER TABLE SET basin.memtable_table_cap = '32MB'`

Per-table flush target within the project budget. Default is 16 MB. When a single table's memtable exceeds this value, it becomes the first candidate for flush under soft-cap pressure. Increase it for OLTP-heavy tables that receive frequent single-row writes; leave it at the default for append-only or analytics tables.

```sql
-- Example: a high-frequency OLTP table that needs more hot buffer
ALTER TABLE orders SET (basin.memtable_table_cap = '64MB');
```

Note: per-table caps count toward the project hard cap. Setting ten tables to 64 MB each requires a project hard cap of at least 640 MB plus headroom.

---

## Query examples

The same table, the same SQL — but the execution path and latency differ based on where the data lives.

### Point query on a recent insert

```sql
-- Row was inserted 50ms ago; still in the hot tier
SELECT * FROM orders WHERE id = 9999999;
```

The engine probes the memtable B-tree. One O(log n) lookup, no disk I/O. Expected latency: **< 1 ms**.

### Point query on a row inserted yesterday

```sql
-- Row was flushed to Vortex overnight
SELECT * FROM orders WHERE id = 9999999;
```

The memtable probe misses. The engine consults Vortex bloom filters for the `id` column and, with `basin.sort_by = 'id'`, skips the vast majority of files before decoding one small chunk. Expected latency: **1–10 ms**.

### Aggregate over the last hour

```sql
SELECT SUM(amount) FROM orders
WHERE created_at > now() - interval '1 hour';
```

Most of this data is still in the hot tier (or was very recently flushed). The memtable contributes the bulk of the result. Expected latency: **1–5 ms**.

### Aggregate over the last year

```sql
SELECT date_trunc('month', created_at), SUM(amount)
FROM orders
WHERE created_at > now() - interval '1 year'
GROUP BY 1
ORDER BY 1;
```

The hot tier contributes a small batch of recent rows. The cold tier carries the year's history. With `basin.sort_by = 'created_at'`, zone maps prune files outside the time range and the columnar scan reads only `created_at` and `amount`. Expected latency: **100 ms – 1 s** at TB scale.

### Range query spanning both tiers

```sql
SELECT * FROM orders
WHERE created_at BETWEEN now() - interval '2 hours' AND now()
ORDER BY created_at;
```

The engine takes the memtable range and the Vortex range for the same interval, merges on `created_at` order, resolves duplicate PKs in favour of the memtable, and returns a unified result set. If a row was inserted an hour ago and has not been flushed yet, it appears. If it was flushed, it comes from Vortex. The caller cannot tell the difference. Expected latency: **5–50 ms** depending on data volume.

---

## Multi-project isolation

Every project in Basin has its own memtable memory budget, enforced independently. When project A's memtable hits its hard cap and writes block, project B is unaffected. The underlying mechanism is a per-project semaphore backed by atomic byte counters — shared bookkeeping structure, zero per-project thread pool or allocator overhead. An inactive project costs one entry in a hash map; no memory for the memtable itself.

This is a deliberate architectural property. Most HTAP systems share a global memtable across all projects: one busy project can spike write latency for everyone else. Basin's multi-project design (see [ADR 0016 §"Multi-project isolation"](./decisions/0016-htap-hot-tier-architecture.md)) eliminates that class of noisy-neighbor problem at the storage layer. It complements the connection-pool fairness already provided by [ADR 0008](./decisions/0008-noisy-neighbor-fairness.md).

The practical consequence: you can safely colocate OLTP-heavy and analytics-heavy projects on the same Basin instance. Set each project's `memtable_hard_cap` according to its write pattern. Caps are enforced in memory accounting, not in wall-clock time, so a project doing large batch inserts does not eat into another project's latency budget.

---

## Transactions and the hot tier

Basin's `BEGIN` / `COMMIT` / `ROLLBACK` semantics extend naturally to the hot tier.

When a transaction opens with `BEGIN`, Basin records a watermark for each table the transaction touches. Writes during the transaction land in the memtable and are visible to subsequent reads *within the same transaction* (read-your-own-writes). Other concurrent sessions do not see the writes until `COMMIT`.

`COMMIT` requires no special action beyond what Basin already does. The memtable rows are already durable — the WAL was written before the client was acknowledged — and the background flush will drain them on its normal schedule.

`ROLLBACK` truncates each touched table's memtable back to its pre-transaction watermark. The rolled-back rows are removed from memory and will never reach Vortex. This is O(rows written in the transaction) and involves no object-store I/O.

`SAVEPOINT` and `ROLLBACK TO SAVEPOINT` work the same way: each savepoint records a memtable watermark alongside the existing file-offset bookmark, and a rollback to savepoint restores that specific watermark.

**Crash safety** is guaranteed by WAL transaction markers ([ADR 0020](./decisions/0020-wal-transaction-markers.md)). The WAL records explicit `BEGIN` and `ROLLBACK` entries. On process restart, WAL replay suppresses all entries enclosed by a `BEGIN` / `ROLLBACK` pair, so rolled-back writes are never re-applied to a fresh memtable. Pre-marker WAL files (written before this format) replay identically — the format is back-compatible.

---

## Observability

### `basin_memtable_stats`

A SQL view (available after Phase 5.14.C5) that shows current hot-tier memory usage per `(project, table)`:

```sql
SELECT project_id, table_name, bytes_allocated, row_count, last_flush_at
FROM basin_memtable_stats
ORDER BY bytes_allocated DESC;
```

Use this to identify tables approaching their `memtable_table_cap`, or to confirm that a flush ran after a batch load.

### `basin_stat_statements`

A SQL view (Phase 5.16.D) for per-query statistics including execution count, mean latency, and p99 latency. Useful for identifying queries that are hitting the cold tier unexpectedly (high p99 relative to the query shape).

```sql
SELECT query, calls, mean_exec_ms, p99_exec_ms
FROM basin_stat_statements
WHERE p99_exec_ms > 10
ORDER BY p99_exec_ms DESC
LIMIT 20;
```

### Metrics

> **Status — read before wiring a dashboard.** Basin does not serve a
> Prometheus scrape endpoint today. The names below are the planned
> OTLP / Prometheus-convention names; the engine emits structured `tracing`
> records that become OTLP metrics when an OpenTelemetry layer is attached
> (`BASIN_OTLP_ENDPOINT`, default `http://localhost:4318`). The only HTTP
> metrics route that exists is `GET /metrics/inflight`, which returns a small
> JSON in-flight/latency snapshot. Treat every `curl .../metrics` recipe on
> this page as the intended shape, not a working command.

| Metric | Description |
|---|---|
| `basin_memtable_bytes{project, table}` | Current allocated bytes per memtable |
| `basin_memtable_flush_total{project, table}` | Cumulative flush count |
| `basin_memtable_flush_duration_seconds` | Flush duration histogram |
| `basin_memtable_hot_hit_ratio` | Fraction of point lookups satisfied by the hot tier |

A `hot_hit_ratio` below 0.5 on an OLTP workload usually means the memtable cap is too small for the write rate — rows are flushing to Vortex before subsequent reads arrive. Increase `memtable_table_cap` or `memtable_hard_cap`.

### Query Insights (managed customers)

The managed console includes a Query Insights UI that surfaces hot-tier vs cold-tier execution breakdowns per query, memtable pressure over time, and flush event timelines. Available on all managed plans; not available for self-hosted deployments.

---

## Performance expectations

| Query shape | Hot tier expected | Cold tier expected |
|---|---|---|
| Point query (recent write) | < 1 ms | n/a |
| Point query (historical row, with `basin.sort_by` + bloom) | n/a | 1–10 ms |
| Range query (last hour) | 1–5 ms | n/a |
| Aggregate over last year | n/a | 100–1000 ms |
| Mixed range (spans both tiers) | Auto-merged; 5–50 ms end-to-end | Auto-merged |

These numbers assume: a table with a declared `PRIMARY KEY`, `basin.sort_by` set on the filter column, default memtable caps, and data at the tens-to-hundreds of GB range for cold-tier figures. TB-scale cold scans at the upper end of the aggregate range. Single-digit-ms point queries require the row to be in the hot tier or in a well-sorted Vortex file; degenerate cases (unsorted large table, no bloom filter hit) can exceed 10 ms.

---

## Anti-patterns and workarounds

**Do not insert one row at a time in a tight loop without batching.** Each INSERT acquires the memtable write lock, increments the byte counter, and checks pressure. At high single-row-insert rates this saturates the lock and creates back-pressure before the memtable is meaningfully full. Use `COPY` or multi-row `INSERT INTO ... VALUES (...), (...), (...)` instead. A single `COPY` of 10,000 rows acquires the lock once.

**Do not set `memtable_hard_cap` above 50% of your process RAM.** The memtable competes with query execution buffers, the OS page cache (which speeds cold-tier reads), and connection state. Setting the cap too high causes the OS to page-fault under query load and increases latency across all query shapes.

**Do not declare `basin.sort_by` on a column with very low cardinality.** A boolean column has two possible values; zone maps on it span the entire table and skip nothing. A column with a handful of enum values is similarly unhelpful. Use `basin.sort_by` on high-cardinality columns: timestamps, IDs, UUIDs, numeric identifiers.

**Do not expect HTAP to absorb 100k inserts/sec on the default 16 MB per-table cap.** At 500 bytes per row, 16 MB holds about 32,000 rows. If your ingest rate fills the table cap in under a second, flush overhead dominates and write latency rises. Increase `memtable_table_cap` (and `memtable_hard_cap` proportionally) to hold several seconds of ingest before flushing. Flush is non-blocking for writes, but frequent small flushes create many small Vortex files that slow cold-tier scans until compaction catches up.

**Do not skip the `PRIMARY KEY` declaration on OLTP tables.** Tables without a declared PK use an internal `rowid`. UPDATE on a rowid table tombstones the old rowid and inserts a new one — correct but slower than a true upsert. More importantly, without a PK, the engine cannot skip duplicate rows efficiently during merge, which adds overhead on read paths that touch both tiers.

---

## FAQ

**Q: Do I need to enable HTAP?**
A: No. HTAP is on by default for all tables. The first write to any table allocates a memtable entry automatically. There is no flag to set and no migration to run.

**Q: What if I only do analytics — no point queries, no single-row updates?**
A: The hot tier adds a small overhead: each write lands in the memtable (tiny in-memory insert) before flushing to Vortex. The default per-table cap is 16 MB, which fills quickly under a bulk-load pattern and flushes frequently. Cold-tier analytical performance is unchanged. If you are loading data in large batches (millions of rows at a time), consider using `COPY` — it batches the memtable writes and reduces flush frequency.

**Q: What if I only do OLTP — no aggregates, no range scans?**
A: Increase `memtable_table_cap` to hold more rows before flushing (e.g., 64 MB) and ensure `memtable_hard_cap` is set to accommodate all your active tables. Set `basin.sort_by` to your primary lookup column. Most hot-path reads will be sub-millisecond memtable hits and never touch Vortex.

**Q: How does this differ from Postgres?**
A: Postgres is row-oriented end-to-end. A `SUM` query over 100M rows reads every column of every row even if you only need one column, because the heap file interleaves all columns. At scale, analytical queries on Postgres are slow because of this. Basin's cold tier is columnar: analytical scans read only the columns they need. Basin also runs HTAP queries from object storage, which scales horizontally in a way a single Postgres instance cannot.

**Q: How does this differ from DuckDB?**
A: DuckDB is an excellent single-node analytical engine but it is a library, not a multi-project server. It has no concept of per-project memory isolation, no built-in hot/cold split, and no WAL-backed durability for a shared write workload. Basin is designed to run as a multi-project database-as-a-service where dozens of projects share one process with hard-enforced memory budgets and independent crash recovery.
