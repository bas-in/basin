# Speed benchmark — point_read

Measures **single-row lookup latency** (p50 / p95 / p99) across Neon, Supabase,
and Basin Cloud.

## What it measures

`SELECT * FROM events WHERE id = <N>` against a preloaded 100k-row table.
No index on `id` — this compares storage-engine **predicate pushdown** (e.g.
Vortex zone maps vs. Postgres heap scan), not B-tree retrieval.

## Methodology

| Parameter | Default | Override via |
|---|---|---|
| Preloaded rows | 100,000 | `SPEED_ROW_COUNT` |
| Timed iterations | 1,000 | `SPEED_ITERATIONS` |
| Warm-up iterations (discarded) | 100 | `SPEED_WARMUP` |
| Percentile calc | linear interpolation on sorted per-iteration ms | — |

The first `SPEED_WARMUP` iterations are discarded to remove cold-connection
and OS page-cache noise.  Each timed iteration records the full client
round-trip (query dispatch → result receipt) in milliseconds.

## Running

```sh
# All three targets (reads .basin-three-way.toml or env vars):
./benchmark/speed/point_read/run.sh

# Single target:
BASIN_DATABASE_URL='postgres://...' REGION_LABEL=fra \
  ./benchmark/speed/point_read/run.sh

# Dry run (no connections):
./benchmark/speed/point_read/run.sh --dry-run
```

## Output

`benchmark/speed/out/compare_speed_point_read_<region>_<ts>.json` — a
`compare` card consumable by `benchmark/bundle.py`.
