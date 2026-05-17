-- benchmark/speed/concurrent/schema.sql
--
-- 1M-row audit-log table for mixed concurrent workload benchmarks.
-- No index on id — tests storage-engine performance under concurrent access.
-- Teardown is handled by run.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_speed_concurrent;

CREATE TABLE bench_speed_concurrent.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
