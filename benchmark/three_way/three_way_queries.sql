-- benchmark/three_way/three_way_queries.sql
--
-- Query battery executed identically against Neon, Supabase, and Basin Cloud.
-- run_three_way.sh drives these via psql \timing and captures wall-clock ms.
-- The queries are intentionally unindexed so we compare storage-engine substrate,
-- not B-tree optimisation.  A Postgres endpoint with an index would be faster for
-- Q1 and Q2; that is documented in the methodology.
--
-- Substitution tokens replaced by run_three_way.sh before dispatch:
--   :target_id   — a BIGINT in the middle of the inserted range  (N/2 + 7)
--   :range_lo    — BIGINT  (N/4)
--   :range_hi    — BIGINT  (N/4 + 10000)
--
-- Q1: Point query — single row lookup by id (no index; tests predicate pushdown)
SELECT id, project_id, action, created_at, payload
FROM   bench_three_way.events
WHERE  id = :target_id;

-- Q2: Range scan — ~10,000 rows (matches the OSS harness range window)
SELECT id, action, created_at
FROM   bench_three_way.events
WHERE  id BETWEEN :range_lo AND :range_hi;

-- Q3: Aggregate — COUNT + SUM grouped into 1,000-row buckets
--     (id / 1000 produces ~N/1000 groups; consistent with OSS ts/1000000 bucket)
SELECT id / 1000 AS bucket,
       COUNT(*)    AS event_count,
       SUM(id)     AS id_sum
FROM   bench_three_way.events
GROUP  BY bucket
ORDER  BY bucket;

-- Q4: On-disk size via pg_total_relation_size (works on Neon, Supabase, PG).
--     Basin Cloud: same query is attempted; if the function is unavailable the
--     harness catches the error and records null with a note.
SELECT pg_total_relation_size('bench_three_way.events')::bigint AS disk_bytes;
