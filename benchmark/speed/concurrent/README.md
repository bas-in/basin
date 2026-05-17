# Speed benchmark — concurrent

Measures **concurrent mixed-workload throughput and latency** at N = 1 / 8 / 32 / 64
parallel workers against a 1M-row table.

## What it measures

| Dimension | Value |
|---|---|
| Workload mix | 70% point reads / 20% range scans / 10% inserts |
| Concurrency levels | N = 1, 8, 32, 64 workers |
| Metrics | ops/sec throughput + p50/p95/p99 per-operation latency |

Each target is measured **one at a time** (sequential targets, parallel workers
within each target run).  This avoids cross-target interference on shared
network paths and connection-pool limits.

Workers are sub-processes; each writes its per-operation timing to a temp file.
The parent collects all timing files and computes honest per-operation latency
distribution across all workers for the given N, not an average.

## Methodology

| Parameter | Default | Override via |
|---|---|---|
| Preloaded rows | 1,000,000 | `SPEED_ROW_COUNT` |
| Ops per worker | 100 | `SPEED_OPS_PER_WORKER` |
| Warm-up ops per worker (discarded) | 10 | `SPEED_WARMUP_OPS` |
| Concurrency levels | 1 8 32 64 | `SPEED_CONCURRENCY` |

## Running

```sh
./benchmark/speed/concurrent/run.sh [--dry-run]

# Limit concurrency levels for a quick run:
SPEED_CONCURRENCY="1 8" ./benchmark/speed/concurrent/run.sh
```

## Output

`benchmark/speed/out/compare_speed_concurrent_<region>_<ts>.json`
