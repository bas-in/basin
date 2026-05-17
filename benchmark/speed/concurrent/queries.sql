-- benchmark/speed/concurrent/queries.sql
--
-- Mixed workload for concurrent benchmark: 70% point reads / 20% range scans / 10% inserts.
-- run.sh distributes these proportions across N parallel worker processes.
--
-- Each worker process picks a query type by hashing its (worker_id, iteration):
--   0..69  → point read
--   70..89 → range scan (~1k rows)
--   90..99 → single-row INSERT (new id > existing range, no conflict)
--
-- The proportions mirror a typical online analytics workload where reads
-- dominate but writes are continuously present.

-- Point read (70% probability):
SELECT id, action, created_at FROM bench_speed_concurrent.events
WHERE id = :target_id;

-- Range scan (20% probability — ~1k rows):
SELECT COUNT(*) FROM bench_speed_concurrent.events
WHERE id BETWEEN :range_lo AND :range_hi;

-- Insert (10% probability):
INSERT INTO bench_speed_concurrent.events (id, project_id, action, created_at, payload)
VALUES (:new_id, gen_random_uuid(), 'concurrent_insert', now(), 'concurrent-payload');
