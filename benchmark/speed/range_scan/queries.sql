-- benchmark/speed/range_scan/queries.sql
--
-- Range-scan query battery: SELECT COUNT(*) over a contiguous id window.
-- Three window sizes: 1k / 100k / 1M rows (anchored in the table middle).
-- The :lo_1k / :hi_1k etc. substitution tokens are replaced by run.sh.
--
-- Using COUNT(*) rather than returning all rows avoids network-bandwidth
-- asymmetry between targets; the bottleneck measured is storage-engine scan
-- throughput and predicate evaluation, not wire transfer.

-- Small window — 1,000 rows
SELECT COUNT(*) AS cnt FROM bench_speed_range_scan.events
WHERE id BETWEEN :lo_1k AND :hi_1k;

-- Medium window — 100,000 rows
SELECT COUNT(*) AS cnt FROM bench_speed_range_scan.events
WHERE id BETWEEN :lo_100k AND :hi_100k;

-- Large window — 1,000,000 rows
SELECT COUNT(*) AS cnt FROM bench_speed_range_scan.events
WHERE id BETWEEN :lo_1m AND :hi_1m;
