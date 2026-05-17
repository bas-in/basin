-- benchmark/speed/point_read/queries.sql
--
-- Point-read query battery: single-row lookup by id.
-- The :target_id substitution is replaced by run.sh before dispatch.
--
-- No index on `id` — this tests full-table predicate pushdown (or scan).
-- An indexed variant would show B-tree vs. Vortex zone-map efficiency; that
-- is a separate benchmark concern.

-- Q1: Primary point-read — fetch all columns for a single id in the middle
-- of the inserted range (N/2 + 7, avoids off-by-one artifacts).
SELECT id, project_id, action, created_at, payload
FROM   bench_speed_point_read.events
WHERE  id = :target_id;
