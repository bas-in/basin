-- benchmark/speed/bulk_insert/schema.sql
--
-- Empty table used for bulk-insert throughput benchmarks.
-- A fresh table is created at the start of each batch-size trial to
-- avoid index/fillfactor effects from previous inserts.
-- Teardown is handled by run.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_speed_bulk_insert;

CREATE TABLE bench_speed_bulk_insert.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
