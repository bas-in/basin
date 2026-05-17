# Speed benchmark — range_scan

Measures **range-scan latency** at three window sizes (1k / 100k / 1M rows)
against a 10M-row table.

## What it measures

`SELECT COUNT(*) FROM events WHERE id BETWEEN <lo> AND <hi>` at three
granularities.  Using `COUNT(*)` eliminates network-bandwidth asymmetry between
targets; the bottleneck measured is storage-engine **scan throughput and
predicate evaluation**, not wire transfer.

No index on `id` — compares raw scan performance (Vortex zone maps vs.
Postgres sequential scan).

## Methodology

| Parameter | Default | Override via |
|---|---|---|
| Preloaded rows | 10,000,000 | `SPEED_ROW_COUNT` |
| Timed iterations per window | 50 | `SPEED_ITERATIONS` |
| Warm-up iterations (discarded) | 5 | `SPEED_WARMUP` |
| Window sizes | 1k, 100k, 1M rows | — |
| Window anchor | N/4 (avoids boundary effects) | — |

## Running

```sh
./benchmark/speed/range_scan/run.sh [--dry-run]

# Single target:
BASIN_DATABASE_URL='postgres://...' REGION_LABEL=fra \
  ./benchmark/speed/range_scan/run.sh
```

## Output

`benchmark/speed/out/compare_speed_range_scan_<region>_<ts>.json`
