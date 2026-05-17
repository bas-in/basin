-- benchmark/speed/range_scan/schema.sql
--
-- Large audit-log table (~10M rows) used for range-scan latency benchmarks.
-- No index on `id` — measures raw scan/pushdown performance across storage
-- engines at three window sizes (1k / 100k / 1M rows).
-- Teardown is handled by run.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_speed_range_scan;

CREATE TABLE bench_speed_range_scan.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
