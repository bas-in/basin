# Speed benchmark — aggregate

Measures **aggregate query latency** (p50 / p95 / p99) for four query shapes
against a 10M-row table.

## What it measures

| Query | SQL shape |
|---|---|
| A1 — Full COUNT | `SELECT COUNT(*) FROM events` |
| A2 — Full SUM+AVG | `SELECT SUM(id), AVG(id) FROM events` |
| A3 — GROUP BY bucket | `SELECT id/1000 AS bucket, COUNT(*), SUM(id) GROUP BY bucket` |
| A4 — Windowed COUNT | `COUNT(*) WHERE id BETWEEN lo AND hi` (~10% of table) |

Full-table aggregates stress the entire scan + aggregation pipeline.
Windowed COUNT reveals how predicate pushdown interacts with column-level
aggregation (Vortex zone maps vs. Postgres heap scan).

## Methodology

| Parameter | Default | Override via |
|---|---|---|
| Preloaded rows | 10,000,000 | `SPEED_ROW_COUNT` |
| Timed iterations per query | 20 | `SPEED_ITERATIONS` |
| Warm-up iterations (discarded) | 3 | `SPEED_WARMUP` |
| Window (A4) | N/4 to N/4 + N/10 | — |

## Running

```sh
./benchmark/speed/aggregate/run.sh [--dry-run]
```

## Output

`benchmark/speed/out/compare_speed_aggregate_<region>_<ts>.json`
