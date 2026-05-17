# Speed benchmark — bulk_insert

Measures **bulk-insert throughput** (rows/second) at three batch sizes:
10k / 100k / 1M rows.

## What it measures

Each batch is a single `BEGIN...COMMIT` block with multi-row `INSERT VALUES`
statements (1k-row chunks to avoid statement-size limits).  The table is
`TRUNCATE`d between trials so cumulative heap growth does not affect timings.

Metric: `rows/sec = batch_size / (p50_wall_clock_ms / 1000)`.  Higher is better.

## Methodology

| Parameter | Default | Override via |
|---|---|---|
| Trials per batch size | 5 | `SPEED_ITERATIONS` |
| Warm-up trials (discarded) | 1 | `SPEED_WARMUP` |
| Batch sizes | 10k, 100k, 1M rows | — |
| Insert chunk size (VALUES list) | 1,000 rows | — |

## Running

```sh
./benchmark/speed/bulk_insert/run.sh [--dry-run]
```

## Output

`benchmark/speed/out/compare_speed_bulk_insert_<region>_<ts>.json`
