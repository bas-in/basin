-- benchmark/speed/bulk_insert/queries.sql
--
-- Bulk-insert query shape: multi-row INSERT VALUES batches.
-- The actual SQL is generated programmatically in run.sh for each batch size;
-- this file documents the pattern for reference.
--
-- Batch sizes: 10k / 100k / 1M rows.
-- Each batch is wrapped in a single transaction (BEGIN...COMMIT) to avoid
-- per-statement overhead and measure genuine ingestion throughput.
--
-- Metric: rows/second = batch_size / (wall_clock_ms / 1000).
-- Table is TRUNCATED between trials of the same batch size to prevent
-- cumulative heap growth from distorting measurements.

-- Representative pattern for a 10-row mini-batch:
INSERT INTO bench_speed_bulk_insert.events (id, project_id, action, created_at, payload)
VALUES
  (0, gen_random_uuid(), 'action_0', now(), 'payload-0000000000000000000000000000000000000000'),
  (1, gen_random_uuid(), 'action_1', now(), 'payload-0000000000000000000000000000000000000001');
-- ... repeated to target batch size.
