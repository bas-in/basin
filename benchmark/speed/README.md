# Basin speed benchmarks

Five latency / throughput scenarios measured across Neon, Supabase, and Basin
Cloud.  Each scenario is **self-contained** (its own schema, data load, and
runner) and writes a `compare`-shaped JSON card that `benchmark/bundle.py` can
render without any pipeline changes.

---

## Scenarios

| Scenario | Table size | Metric | Key question |
|---|---|---|---|
| `point_read` | 100k rows | p50/p95/p99 latency (ms) | How fast is a single-row lookup? |
| `range_scan` | 10M rows | p50/p95/p99 at 3 window sizes | How does scan latency scale with window size? |
| `bulk_insert` | — | rows/sec at 3 batch sizes | How fast can we ingest data? |
| `aggregate` | 10M rows | p50/p95/p99 for 4 query shapes | How efficient is column-level aggregation? |
| `concurrent` | 1M rows | ops/sec + p50/p95/p99 at N=1/8/32/64 | How does performance hold under concurrency? |

---

## Running all scenarios

```sh
# Run all (reads .basin-three-way.toml or env vars):
./benchmark/speed/run_all_speed.sh

# Dry-run (no connections, no data):
./benchmark/speed/run_all_speed.sh --dry-run

# Single scenario:
./benchmark/speed/run_all_speed.sh --scenario point_read

# Single scenario directly:
./benchmark/speed/point_read/run.sh
```

## Running a single scenario

```sh
cd /path/to/basin
./benchmark/speed/point_read/run.sh [--dry-run]
./benchmark/speed/range_scan/run.sh [--dry-run]
./benchmark/speed/bulk_insert/run.sh [--dry-run]
./benchmark/speed/aggregate/run.sh [--dry-run]
./benchmark/speed/concurrent/run.sh [--dry-run]
```

---

## DSN configuration

Same as `benchmark/three_way/` — uses the same `.basin-three-way.toml` at the
repo root (gitignored):

```toml
[endpoints]
neon         = "postgres://user:pass@host/db?sslmode=require"
supabase     = "postgres://user:pass@host/db?sslmode=require"
basin        = "postgres://user:pass@host/db?sslmode=disable"
region_label = "fra"
# Optional speed-benchmark overrides:
# speed_iterations = 200
# speed_out_dir    = "/tmp/speed_out"
```

Or via env vars:

```sh
export NEON_DATABASE_URL='postgres://...'
export SUPABASE_DATABASE_URL='postgres://...'
export BASIN_DATABASE_URL='postgres://...'
export REGION_LABEL=fra
```

**Partial runs:** if only some DSNs are set, the missing targets appear as
`null` in the JSON output.  All present targets are measured.

**No DSNs:** if all three DSNs are absent, every scenario skips cleanly and
`run_all_speed.sh` exits 0 with an informative message.

---

## Methodology

### Warm-up

Each scenario discards a configurable warm-up batch before recording samples.
This removes cold-connection and OS page-cache noise from the measurements.

| Scenario | Default warm-up | Default timed iterations |
|---|---|---|
| point_read | 100 | 1000 |
| range_scan | 5 | 50 per window |
| bulk_insert | 1 trial | 5 trials per batch |
| aggregate | 3 | 20 per query |
| concurrent | 10 ops/worker | 100 ops/worker |

### Percentile calculation

All percentiles (p50/p95/p99) use **linear interpolation** on the sorted
per-iteration millisecond sample list.  The implementation is in `_lib.sh`
(`percentile_of`).

### Timing precision

Timing uses `epoch_ms` from `_lib.sh`:
1. `date +%s%3N` (Linux / GNU date / macOS with coreutils) — millisecond precision.
2. `python3 time.time() * 1000` — millisecond precision on all platforms.
3. `$SECONDS * 1000` — 1-second resolution fallback (rare).

Per-iteration timing captures the full client round-trip (query dispatch
→ result receipt), consistent with how end-user latency is experienced.

### Sequential target execution

Targets (Neon, Supabase, Basin) are always benchmarked sequentially to avoid
cross-endpoint interference on shared network paths.  For `concurrent/`, the
parallelism is **worker processes against a single target** — measured one
target at a time.

### Idempotent teardown

Every scenario registers a `trap cleanup EXIT` that drops its schema
(`DROP SCHEMA IF EXISTS ... CASCADE`) on all prepared targets, including on
`Ctrl-C` or failure.

---

## Output

Each scenario writes to `benchmark/speed/out/` (gitignored):

```
benchmark/speed/out/
  compare_speed_point_read_fra_20260517T120000.json
  compare_speed_range_scan_fra_20260517T120100.json
  compare_speed_bulk_insert_fra_20260517T120500.json
  compare_speed_aggregate_fra_20260517T121000.json
  compare_speed_concurrent_fra_20260517T122000.json
```

### JSON shape

All output files follow the existing `compare` shape consumed by
`benchmark/bundle.py`:

```jsonc
{
  "kind": "compare",
  "id": "speed_point_read_fra",
  "name": "Point-read latency (fra, 100000 rows)",
  "claim": "...",
  "available": true,
  "region": "fra",
  "row_count": 100000,
  "iterations": 1000,
  "warmup": 100,
  "generated_at": "@1747483200",
  "metrics": [
    { "label": "p50 (ms)", "neon": 1.2, "supabase": 1.4, "basin": 0.8,
      "unit": "ms", "better": "basin", "ratio_text": "1.5x faster than Neon", "note": null }
  ],
  "note": "..."
}
```

The `bundle.py` `render_compare()` function detects the 3-column shape
(`neon`/`supabase`/`basin`) automatically and renders a 3-column Markdown
table.  No changes to the bundler are needed.

---

## File structure

```
benchmark/speed/
├── README.md                 ← this file
├── _lib.sh                   ← shared helpers (timing, percentiles, JSON, TOML loader)
├── run_all_speed.sh          ← top-level runner
│
├── point_read/
│   ├── README.md
│   ├── schema.sql            ← 100k-row table (no index)
│   ├── queries.sql           ← SELECT WHERE id = N
│   └── run.sh                ← 1000 iterations, p50/p95/p99
│
├── range_scan/
│   ├── README.md
│   ├── schema.sql            ← 10M-row table (no index)
│   ├── queries.sql           ← COUNT(*) WHERE id BETWEEN lo AND hi
│   └── run.sh                ← 3 window sizes × p50/p95/p99
│
├── bulk_insert/
│   ├── README.md
│   ├── schema.sql            ← empty table, truncated between trials
│   ├── queries.sql           ← INSERT VALUES pattern documentation
│   └── run.sh                ← rows/sec for 10k/100k/1M batches
│
├── aggregate/
│   ├── README.md
│   ├── schema.sql            ← 10M-row table
│   ├── queries.sql           ← COUNT / SUM / AVG / GROUP BY shapes
│   └── run.sh                ← 4 query shapes × p50/p95/p99
│
└── concurrent/
    ├── README.md
    ├── schema.sql            ← 1M-row table
    ├── queries.sql           ← 70/20/10 read/range/insert mix
    └── run.sh                ← N=1/8/32/64 workers × ops/sec + p50/p95/p99
```
