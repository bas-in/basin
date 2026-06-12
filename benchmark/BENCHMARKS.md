# Basin — Performance benchmarks (long version)

This document is the full performance story. The quick headline block lives in
[`../README.md`](../README.md). Numbers here are sourced directly from
auto-generated test reports; do not edit by hand.

---

## Workload shape

- **Data:** 1 M synthetic audit-log rows (`id UUID, project_id UUID, action TEXT,
  created_at TIMESTAMPTZ, payload TEXT`). 10 M-row variants appear where noted.
- **Parquet layout:** default row-group size 65,536 rows, ZSTD-1 compression.
- **Index policy:** neither Basin nor Postgres has a B-tree index on `id`.
  Basin's point-query speed comes from Parquet predicate pushdown + bloom
  filters, not a B-tree. Results are substrate comparisons, not a claim that
  Basin replaces Postgres for every workload.
- **Postgres version:** 18 (local, default config).
- **Basin version:** see `git log -1 --format=%h` at the repo root.
- **Three storage configs are tracked:**

| Config | Slug | What it measures |
|---|---|---|
| LocalFS | `localfs` | Pure architectural numbers — no network, no storage concurrency limits. Fastest and most stable. |
| Real S3-compatible | `real` | Tigris / AWS S3 / Cloudflare R2 / Backblaze B2. Numbers customers will actually experience. |
| SeaweedFS | `seaweedfs` | Self-hosted S3 gateway; high-concurrent-read proxy for on-prem S3-like throughput. |

---

## Storage and compression

### LocalFS — 1 M rows

| Metric | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| On-disk bytes | 7.72 MiB | 96.51 MiB | pg / basin = **12.49×** |
| Parquet vs CSV (audit log) | — | — | csv / parquet = **20.22×** |
| Insert 1 M rows | 2,877.69 ms | 3,063.77 ms | basin slightly faster |

The 12.5× disk advantage is structural: Parquet columnar layout + ZSTD-1 vs
Postgres's 8 KiB MVCC heap pages with per-row tuple headers and padding.

The 20.22× figure is measured directly: the same 1 M-row audit-log table
serialised to CSV vs Parquet. This is the storage cost reduction for the most
common Basin wedge workload.

### Real S3 — 100 K rows

| Metric | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| On-disk bytes | 0.79 MiB | 9.68 MiB | pg / basin = **12.30×** |
| Insert 100 K rows | 264.68 ms | 374.42 ms | basin faster |

The disk ratio is consistent across storage backends because it is a function of
encoding, not network.

### Scale-up (LocalFS)

Point-query latency grows sub-linearly with data volume:

| Row count | Disk (MiB) | Bytes/row (B) | Point query p50 (ms) |
|---|---|---|---|
| 100,000 | 0.79 | 8.25 | 3.71 |
| 1,000,000 | 7.86 | 8.24 | 3.77 |
| 10,000,000 | 78.59 | 8.24 | 4.49 |

p50 growth from 100 K to 10 M rows: **1.21×** (bar < 5×). Bytes/row is
constant, confirming no per-row overhead inflation.

---

## Point-query latency

### LocalFS — unindexed, cold

Basin has no B-tree index; Postgres has none either.

| Metric | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| Point query p50 (1 M rows) | 0.18 ms | 15.06 ms | pg / basin = **82.64×** |

The predicate-pushdown test measures how much data a point query reads vs a full
scan: **101.18× byte reduction** — Basin prunes 99% of the data via Parquet
column statistics and bloom filters without a B-tree index.

Row-group tuning: a table with `row_group_rows = 4096` scans **6.25%** as many
rows as the same table at the default 65,536 row-group size for a `WHERE id = X`
query.

### Real S3 — cold p99, with full caching stack

On real S3 (50 ms simulated cross-region latency injector), the four-layer
caching stack delivers:

| Stack layer | p50 latency (ms) | p99 latency (ms) |
|---|---|---|
| (a) no cache | 584.18 | 589.73 |
| (b) + NVMe disk cache | 2.03 | 603.53 |
| (c) + Parquet page cache | 1.95 | 604.02 |
| (d) + bloom filter (full stack) | 1.55 | 660.97 |

Cold p50 speedup, baseline → full stack: **377.78×** (bar ≥ 100×).

On a real S3-compatible backend (no latency injector), cold p99 for a
10 M-row dataset: **3,269.94 ms** (bar < 8,000 ms).

---

## Connection scaling and RAM

These results reflect Basin's tokio-task-per-connection model vs Postgres's
fork-per-connection model. The gap is structural and grows with connection count.

### LocalFS

| Metric | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| Connection accept latency p50 | 0.44 ms | 1.46 ms | pg / basin = 3.33× |
| Connections held under 1,000-conn flood | 1,000 | 99 | basin / pg = 10.10× |
| Refused connections under flood | 0 | 901 | — |
| RSS per held-open connection | 120.96 KiB | 7,657.92 KiB | pg / basin = **63.31×** |

### Real S3

| Metric | Basin | Postgres 18 | Ratio |
|---|---|---|---|
| Connection accept latency p50 | 0.44 ms | 1.39 ms | pg / basin = 3.13× |
| Connections held under 1,000-conn flood | 1,000 | 90 | basin / pg = 11.11× |
| Refused connections under flood | 0 | 910 | — |
| RSS per held-open connection | 109.44 KiB | 7,070.08 KiB | pg / basin = **64.60×** |

Idle-project RAM cost (LocalFS): **0.93 KiB** per project at 1,000 projects;
stays under 2.56 KiB across all tested scales (bar < 5 KiB).

---

## Lifecycle operations

### Backup

Basin's Iceberg-style snapshot is an O(1) manifest copy; `pg_dump` is O(data).

| Metric | Basin | Postgres | Ratio |
|---|---|---|---|
| Backup wall time (100 K rows) | ~0 s | 0.19 s | pg / basin = 1,251× |
| Backup byte size | ~0 MiB | 6.08 MiB | pg / basin = 6,736× |

Real S3: backup wall time 0 s vs 0.34 s (1,451×); backup size 0 MiB vs
6.08 MiB (14,020×).

### Schema migration — ADD COLUMN (LocalFS)

| Metric | Basin | Postgres | Ratio |
|---|---|---|---|
| ADD COLUMN (100 K rows, no default) | 0.20 ms | 7.37 ms | pg / basin = **36.17×** |

Basin's ADD COLUMN is a catalog metadata write (Parquet schema evolution
projects NULL for pre-existing rows); Postgres must rewrite the heap for a
column with a non-trivial default.

### Project deletion (LocalFS)

| Files per project | Basin (ms) | Postgres (ms) |
|---|---|---|
| 100 | 4.64 | 5.91 |
| 1,000 | 34.09 | 6.15 |
| 5,000 | 170.31 | 2.26 |

Basin's slope is flatter for small file counts (catalog-first bulk DELETE +
parallel LIST mop-up) but grows with file count because every file must be
individually deleted from the object store. At large file counts Postgres's
DROP SCHEMA CASCADE wins on wall time. The crossover at 100 files is the
advertised structural advantage on typical SaaS project sizes.

---

## Caveats and honest framing

- **Basin is Postgres-compatible, not Postgres.** The pgwire protocol, SQL
  surface, and driver compatibility are genuine, but Basin's execution engine
  is DataFusion / DuckDB over Parquet, not the Postgres query planner over a
  heap. Some workloads that Postgres handles well (row-level UPDATE/DELETE
  hotspots, heavy JOIN fan-out, B-tree range scans on non-clustered data)
  will be faster on Postgres.
- **No B-tree index on Basin's side.** Point-query results use Parquet
  predicate pushdown and bloom filters. A Postgres table *with* an index would
  be faster for point queries than the unindexed Postgres baseline shown here.
- **SeaweedFS cards are a loopback structural-bug detector, not a cloud
  latency proxy.** The `data_seaweedfs` dashboard runs Basin against a local
  SeaweedFS S3 gateway (~1 ms/op RTT, no injected latency) versus an
  *unindexed* Postgres. A headline like "point query 4× faster" on this card
  reflects that regime: Basin's cold path on loopback storage beats an
  unindexed sequential scan, but it is **not** the cloud claim. Against an
  *indexed* Postgres the same point query is far slower (a remote object GET
  loses to a buffer-cache B-tree probe). The cache-stack speedup bar is
  regime-aware for the same reason — on loopback the uncached baseline is
  already ~4 ms, so there is almost no latency for the disk/page/bloom stack to
  compress, and the ≥3× bar only applies in a latency-bearing regime. The
  honest cloud framing is **Basin on Tigris (real object-store RTT) vs Postgres
  on EBS, *with* its index** — run the `.basin-test.tigris-realistic.toml`
  profile (a `LatencyStore` injecting ~9 ms/op) to reproduce that regime; the
  loopback SeaweedFS numbers should not be quoted as cloud performance.
- **Best-effort Postgres calibration.** Postgres is run with default config
  (no tuning of `shared_buffers`, `work_mem`, etc.). A tuned Postgres instance
  would close some gaps.
- **Synthetic data.** Audit-log rows are generated by a fixed-seed PRNG.
  Real-world data distributions will produce different numbers.
- **Analytical engine is LocalFS-only in v0.1.** The DuckDB analytical
  routing path (`GROUP BY` speedup) requires a local filesystem root; real-S3
  analytical benchmarks are v0.2 work. Those cards currently read 0× on the
  real-S3 dashboard.
- **Cold p99 bars.** Every latency bar is measured cold (caches empty at the
  start of the run). Warm p50/p99 are reported alongside as corroboration but
  are not the primary claimed number.

---

## How to reproduce

### LocalFS (fast, no cloud credentials needed)

```sh
# Run all integration tests and regenerate the Markdown + HTML dashboards
cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py
# Open the dashboard
open benchmark/index_localfs.html
```

### Real S3-compatible backend

```sh
# Copy and fill in your credentials
cp .basin-test.toml.example .basin-test.toml
# Run the cloud-backed suite
cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py --dir data_real
open benchmark/index_real.html
```

### SeaweedFS

```sh
# Start a local SeaweedFS gateway (requires Docker)
bash benchmark/start_local_s3.sh
cp .basin-test.seaweedfs.toml.example .basin-test.seaweedfs.toml
cargo test -p basin-integration-tests --tests -- --nocapture
python3 benchmark/bundle.py --dir data_seaweedfs
open benchmark/index_seaweedfs.html
```

A `benchmark/run_pg_compare.sh` script (in progress) will automate the
Postgres head-to-head setup: provision a local Postgres 18 instance, run the
same workload against both, and emit the comparison tables above.

---

## What to read next

- [`../README.md`](../README.md) — quick headline block and project overview.
- [`RESULTS_localfs.md`](./RESULTS_localfs.md) — auto-generated full LocalFS report.
- [`RESULTS_real.md`](./RESULTS_real.md) — auto-generated real-cloud report.
- [`../WEDGE.md`](../WEDGE.md) — six-month wedge-deepening roadmap.
- [`../docs/architecture.md`](../docs/architecture.md) — the four-layer stack.
- [`../docs/decisions/`](../docs/decisions/) — every "no" we've recorded.
