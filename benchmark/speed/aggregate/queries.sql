-- benchmark/speed/aggregate/queries.sql
--
-- Aggregate query battery: COUNT / SUM / AVG over the full table
-- and over a windowed subset.
--
-- Full-table aggregates stress the entire scan + aggregation pipeline.
-- Windowed aggregates reveal how predicate pushdown interacts with
-- column-level aggregation (Vortex zone maps + dictionary encoding vs.
-- Postgres heap scan).

-- A1: Full-table COUNT — baseline scan cost.
SELECT COUNT(*) AS total_rows
FROM   bench_speed_aggregate.events;

-- A2: Full-table SUM + AVG of id.
SELECT SUM(id) AS id_sum, AVG(id) AS id_avg
FROM   bench_speed_aggregate.events;

-- A3: GROUP BY bucket (id / 1000) — matches the three_way Q3 workload
--     so results are directly comparable.
SELECT id / 1000         AS bucket,
       COUNT(*)           AS event_count,
       SUM(id)            AS id_sum
FROM   bench_speed_aggregate.events
GROUP  BY bucket
ORDER  BY bucket;

-- A4: Windowed COUNT over ~10 % of the table.
--     :lo_window and :hi_window are substituted by run.sh.
SELECT COUNT(*) AS windowed_count
FROM   bench_speed_aggregate.events
WHERE  id BETWEEN :lo_window AND :hi_window;
