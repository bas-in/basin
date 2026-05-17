-- benchmark/speed/aggregate/schema.sql
--
-- 10M-row audit-log table reused from range_scan, reproduced here so the
-- aggregate scenario is self-contained (no dependency on range_scan running first).
-- Teardown is handled by run.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_speed_aggregate;

CREATE TABLE bench_speed_aggregate.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
