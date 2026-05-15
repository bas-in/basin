-- benchmark/three_way/three_way_schema.sql
--
-- Identical schema applied to all three endpoints (Neon, Supabase, Basin Cloud).
-- This file is sourced verbatim by run_three_way.sh — do not add psql meta-commands
-- that are not universally supported (\set, \copy, etc.).
--
-- The table models a SaaS audit log: sequential integer id, a UUID project tag,
-- a short action label, an RFC-3339 timestamp, and a variable-length payload.
-- This shape mirrors the Basin OSS compare_postgres workload so results are
-- comparable across the two harnesses.
--
-- The schema name bench_three_way is chosen to avoid collision with any
-- application schema. Teardown is handled by run_three_way.sh (DROP SCHEMA CASCADE).

CREATE SCHEMA IF NOT EXISTS bench_three_way;

CREATE TABLE bench_three_way.events (
    id         BIGINT       NOT NULL,
    project_id UUID         NOT NULL DEFAULT gen_random_uuid(),
    action     TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    payload    TEXT         NOT NULL
);
