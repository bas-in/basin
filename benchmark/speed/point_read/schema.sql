-- benchmark/speed/point_read/schema.sql
--
-- Small audit-log table (~100k rows) used for point-read latency benchmarks.
-- Intentionally unindexed on `id` so we compare storage-engine predicate
-- pushdown performance, not B-tree lookup speed.
-- Teardown is handled by run.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_speed_point_read;

CREATE TABLE bench_speed_point_read.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
