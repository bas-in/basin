//! A MEASUREMENT, not an assertion.
//!
//! The owned engine falls back to DataFusion for anything it cannot serve, and
//! records why in a five-bucket histogram. That histogram is the migration's
//! steering instrument: a single served/fallback ratio says how far along the
//! engine is, but the per-reason breakdown says what to build next.
//!
//! This runs a spread of ordinary application SQL and prints the result. It is
//! `#[ignore]`d because its output is a number to read, not a property to
//! enforce — a threshold here would either be met trivially or block the branch
//! for reasons unrelated to correctness.
//!
//! The corpus is organised by feature area (see the `// === ... ===` section
//! markers below) rather than by whatever came to mind, because a flat list
//! that happens to read 100% served tells you nothing about WHERE the gaps
//! are. Every shape here was checked against a live PostgreSQL 18.2 server
//! before being added — a shape Postgres itself rejects proves nothing about
//! this engine's coverage.
//!
//! Run with:
//!   cargo test -p basin-engine --test fallback_histogram -- --ignored --nocapture

use std::sync::Arc;

use futures::FutureExt;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Running counts for one feature-area bucket ("Joins", "Aggregates", ...).
/// Mirrors the four verdicts the per-query loop already distinguishes.
#[derive(Default)]
struct AreaStats {
    total: u32,
    served: u32,
    fallback: u32,
    errored: u32,
    panicked: u32,
}

#[tokio::test]
#[ignore = "measurement probe; run with --ignored --nocapture"]
async fn fallback_histogram_over_representative_sql() {
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = eng.open_session(ProjectId::new()).await.unwrap();
    s.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT, amt DOUBLE PRECISION)")
        .await
        .unwrap();
    s.execute("INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5),(3,'c',3.5)")
        .await
        .unwrap();
    // A NULL-bearing row (NULL name, NULL amt) and a row that collides on
    // `name` with an existing one, for the NULL-semantics and GROUP BY
    // collision shapes below. IDs are well clear of the ones the DML section
    // inserts later (4, 5, 6, 7) so nothing collides on that axis either.
    s.execute("INSERT INTO t VALUES (100,NULL,NULL)").await.unwrap();
    s.execute("INSERT INTO t VALUES (101,'a',10.5)").await.unwrap();
    // A second relation, so joins are between different tables rather than
    // self-joins. A self-join hides whole classes of bug: identical schemas on
    // both sides make a left/right mix-up invisible.
    s.execute("CREATE TABLE u (uid BIGINT NOT NULL, tid BIGINT, tag TEXT, n INTEGER)")
        .await
        .unwrap();
    s.execute("INSERT INTO u VALUES (10,1,'x',7),(11,1,'y',8),(12,2,'z',9)")
        .await
        .unwrap();
    // A row with a NULL join key, so join/NULL-key shapes have something to
    // find (or fail to find, correctly).
    s.execute("INSERT INTO u VALUES (13,NULL,'w',NULL)")
        .await
        .unwrap();
    // Temporal and boolean columns, because date/time is where Postgres
    // semantics diverge most sharply and the gate already has a DATE + INTERVAL
    // divergence open.
    s.execute("CREATE TABLE d (id BIGINT NOT NULL, day DATE, ts TIMESTAMP, flag BOOLEAN)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO d VALUES (1,'2024-01-15','2024-01-15 10:30:00',true),\
         (2,'2024-06-30','2024-06-30 23:59:59',false)",
    )
    .await
    .unwrap();
    // Wider type coverage: NUMERIC, INTERVAL, TIMESTAMPTZ, a TEXT[] array
    // column (including a NULL array and an empty array), and BIGINT/SMALLINT
    // near their range edges.
    s.execute(
        "CREATE TABLE e (id BIGINT NOT NULL, n NUMERIC(10,2), iv INTERVAL, \
         tz TIMESTAMPTZ, tags TEXT[], big BIGINT, small SMALLINT)",
    )
    .await
    .unwrap();
    s.execute(
        "INSERT INTO e VALUES \
         (1, 12345.67, INTERVAL '1 day 2 hours', TIMESTAMPTZ '2024-01-15 10:30:00+00', ARRAY['x','y'], 9223372036854775807, 32767), \
         (2, -12345.67, INTERVAL '-3 days', TIMESTAMPTZ '2024-06-30 23:59:59+00', ARRAY[]::TEXT[], -9223372036854775807, -32768), \
         (3, 0.00, INTERVAL '0', TIMESTAMPTZ '2024-01-01 00:00:00+00', NULL, 0, 0)",
    )
    .await
    .unwrap();
    // Multibyte UTF-8 text: accented Latin, CJK, and an emoji (astral-plane
    // code point) — the three cases that most easily break byte-vs-character
    // string functions.
    s.execute("CREATE TABLE mb (id BIGINT NOT NULL, s TEXT)")
        .await
        .unwrap();
    s.execute(
        "INSERT INTO mb VALUES (1,'héllo'), (2,'日本語'), (3,'🎉party🎉'), (4,'naïve café')",
    )
    .await
    .unwrap();
    // A table with a real PRIMARY KEY, for ON CONFLICT / RETURNING / UPDATE
    // ... FROM / DELETE ... USING — none of which have a conflict target
    // without a unique constraint to aim at.
    s.execute("CREATE TABLE p (id BIGINT PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0, tag TEXT)")
        .await
        .unwrap();
    s.execute("INSERT INTO p VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')")
        .await
        .unwrap();

    // Shapes an application actually issues, not shapes the engine is known to
    // handle — the point is to find the gaps, so a flattering list is useless.
    // Grouped by feature area (the `(area, sql)` tag), because the coverage
    // ratio alone says how far along the engine is but not what to build next.
    let queries: &[(&str, &str)] = &[
        // === Basics ===
        ("Basics", "SELECT id FROM t"),
        ("Basics", "SELECT DISTINCT name FROM t"),
        ("Basics", "SELECT generate_series(1,3)"),
        ("Basics", "SELECT day, ts FROM d"),
        ("Basics", "SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i, s)"),

        // === Predicates ===
        ("Predicates", "SELECT id FROM t WHERE id > 1"),
        ("Predicates", "SELECT id FROM t WHERE name LIKE 'a%'"),
        ("Predicates", "SELECT id FROM t WHERE id IN (1,2)"),
        ("Predicates", "SELECT id FROM t WHERE amt BETWEEN 1.0 AND 3.0"),
        ("Predicates", "SELECT id FROM t WHERE name IS NOT NULL"),
        ("Predicates", "SELECT id FROM t WHERE name IS DISTINCT FROM 'a'"),
        ("Predicates", "SELECT id FROM d WHERE flag"),
        ("Predicates", "SELECT id FROM t WHERE id >= 2"),
        ("Predicates", "SELECT id FROM t WHERE id != 2"),
        ("Predicates", "SELECT id FROM t WHERE id <> 2"),
        ("Predicates", "SELECT id FROM t WHERE name ILIKE 'A%'"),
        ("Predicates", "SELECT id FROM t WHERE name NOT LIKE 'a%'"),
        ("Predicates", "SELECT id FROM t WHERE name NOT ILIKE 'A%'"),
        ("Predicates", "SELECT id FROM t WHERE id IS NULL"),
        ("Predicates", "SELECT id FROM t WHERE name IS NOT DISTINCT FROM 'a'"),
        ("Predicates", "SELECT id FROM t WHERE id NOT BETWEEN 1 AND 2"),
        ("Predicates", "SELECT id FROM t WHERE id > 1 AND name = 'b'"),
        ("Predicates", "SELECT id FROM t WHERE id > 1 OR name = 'a'"),
        ("Predicates", "SELECT id FROM t WHERE NOT (id > 1)"),
        ("Predicates", "SELECT id FROM t WHERE id NOT IN (1,2)"),
        ("Predicates", "SELECT id FROM t WHERE (id > 1 AND name IS NOT NULL) OR id = 1"),
        ("Predicates", "SELECT id FROM t WHERE amt > ANY (SELECT amt FROM t WHERE id < 3)"),
        ("Predicates", "SELECT id FROM t WHERE amt > ALL (SELECT amt FROM t WHERE id < 2)"),
        ("Predicates", "SELECT id FROM t WHERE amt = SOME (SELECT amt FROM t)"),
        ("Predicates", "SELECT id FROM t WHERE id::TEXT LIKE '%2%'"),

        // === NULL semantics ===
        ("NULL semantics", "SELECT (NULL AND FALSE) IS NULL"),
        ("NULL semantics", "SELECT (NULL OR TRUE) AS x"),
        ("NULL semantics", "SELECT (NOT NULL) IS NULL"),
        ("NULL semantics", "SELECT NULL = NULL"),
        ("NULL semantics", "SELECT count(*), count(name), count(amt) FROM t"),
        ("NULL semantics", "SELECT sum(amt) FROM t WHERE name IS NULL"),
        ("NULL semantics", "SELECT DISTINCT name FROM t ORDER BY name"),
        ("NULL semantics", "SELECT name, count(*) FROM t GROUP BY name ORDER BY name NULLS FIRST"),
        ("NULL semantics", "SELECT id FROM t ORDER BY name NULLS FIRST"),
        ("NULL semantics", "SELECT id FROM t ORDER BY name ASC NULLS FIRST, id DESC"),
        ("NULL semantics", "SELECT id FROM t WHERE id IN (1, NULL)"),
        ("NULL semantics", "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u)"),
        ("NULL semantics", "SELECT COALESCE(NULL, NULL, 3)"),

        // === Aggregates ===
        ("Aggregates", "SELECT count(*) FROM t"),
        ("Aggregates", "SELECT name, sum(amt) FROM t GROUP BY name"),
        ("Aggregates", "SELECT name, count(*) FROM t GROUP BY name HAVING count(*) > 0"),
        ("Aggregates", "SELECT sum(amt) FILTER (WHERE id > 1) FROM t"),
        ("Aggregates", "SELECT min(amt), max(amt), avg(amt) FROM t"),
        ("Aggregates", "SELECT count(DISTINCT name) FROM t"),
        ("Aggregates", "SELECT name, count(*) FROM t GROUP BY name ORDER BY count(*) DESC"),
        ("Aggregates", "SELECT tid, count(*) FROM u GROUP BY tid HAVING count(*) > 1"),
        ("Aggregates", "SELECT string_agg(name, ',') FROM t"),
        ("Aggregates", "SELECT array_agg(id) FROM t"),
        ("Aggregates", "SELECT variance(amt), stddev(amt) FROM t"),
        ("Aggregates", "SELECT var_pop(amt), var_samp(amt) FROM t"),
        ("Aggregates", "SELECT stddev_pop(amt), stddev_samp(amt) FROM t"),
        ("Aggregates", "SELECT bool_and(flag), bool_or(flag) FROM d"),
        ("Aggregates", "SELECT every(flag) FROM d"),
        ("Aggregates", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amt) FROM t"),
        ("Aggregates", "SELECT mode() WITHIN GROUP (ORDER BY name) FROM t"),
        ("Aggregates", "SELECT sum(amt), count(*), avg(amt) FROM t WHERE id > 1000"),
        ("Aggregates", "SELECT max(id) FROM t WHERE id > 1000"),
        ("Aggregates", "SELECT count(DISTINCT name) FILTER (WHERE id > 1) FROM t"),
        ("Aggregates", "SELECT tid, tag, count(*) FROM u GROUP BY ROLLUP (tid, tag)"),
        ("Aggregates", "SELECT tid, count(*) FROM u GROUP BY CUBE (tid)"),
        ("Aggregates", "SELECT tid, tag, count(*) FROM u GROUP BY GROUPING SETS ((tid), (tag), ())"),
        ("Aggregates", "SELECT name, sum(amt) FROM t GROUP BY name HAVING sum(amt) > 1 AND count(*) >= 1"),
        ("Aggregates", "SELECT min(name), max(name) FROM t"),
        ("Aggregates", "SELECT sum(id) FILTER (WHERE amt IS NOT NULL) FROM t"),
        ("Aggregates", "SELECT array_agg(DISTINCT name) FROM t"),
        ("Aggregates", "SELECT string_agg(DISTINCT name, ',') FROM t"),

        // === Windows ===
        ("Windows", "SELECT id, row_number() OVER (ORDER BY id) FROM t"),
        ("Windows", "SELECT id, lag(id) OVER (ORDER BY id) FROM t"),
        ("Windows", "SELECT tid, n, rank() OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT tid, sum(n) OVER (PARTITION BY tid) FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u"),
        ("Windows", "SELECT n, first_value(n) OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT n, ntile(2) OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, percent_rank() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, cume_dist() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, lead(n, 1, 0) OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, last_value(n) OVER (PARTITION BY tid ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM u"),
        ("Windows", "SELECT n, nth_value(n, 2) OVER (PARTITION BY tid ORDER BY n) FROM u"),
        ("Windows", "SELECT n, dense_rank() OVER (ORDER BY n) FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM u"),
        ("Windows", "SELECT n, avg(n) OVER w FROM u WINDOW w AS (PARTITION BY tid ORDER BY n)"),
        ("Windows", "SELECT n, sum(n) OVER (PARTITION BY tid ORDER BY n ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM u"),
        ("Windows", "SELECT n, count(*) OVER () FROM u"),
        ("Windows", "SELECT n, sum(n) OVER (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u"),

        // === Joins ===
        ("Joins", "SELECT a.id FROM t a JOIN t b ON a.id = b.id"),
        ("Joins", "SELECT a.id FROM t a LEFT JOIN t b ON a.id = b.id"),
        ("Joins", "SELECT t.id, u.tag FROM t JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t RIGHT JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t FULL JOIN u ON u.tid = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t CROSS JOIN u"),
        ("Joins", "SELECT t.id FROM t JOIN u ON u.tid = t.id AND u.n > 7"),
        ("Joins", "SELECT t.id FROM t JOIN u ON u.tid = t.id WHERE u.tag <> 'x'"),
        ("Joins", "SELECT t.id, u.tag, d.flag FROM t JOIN u ON u.tid = t.id JOIN d ON d.id = t.id"),
        ("Joins", "SELECT t.id, g.i FROM t, LATERAL generate_series(1, 2) AS g(i)"),
        ("Joins", "SELECT t.id, d.day FROM t JOIN d USING (id)"),
        ("Joins", "SELECT t.id, d.day FROM t NATURAL JOIN d"),
        ("Joins", "SELECT t.id, u.n FROM t JOIN u ON t.id < u.n"),
        ("Joins", "SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tid IS NULL"),
        ("Joins", "SELECT t.id, u.tag FROM t JOIN u ON t.id IS NOT DISTINCT FROM u.tid"),
        ("Joins", "SELECT u.uid FROM u WHERE u.tid IS NULL"),
        ("Joins", "SELECT a.id, b.id FROM t a JOIN t b ON a.id <> b.id AND a.id < b.id"),
        ("Joins", "SELECT t.id, u.tag, d.flag FROM t LEFT JOIN u ON u.tid = t.id LEFT JOIN d ON d.id = t.id"),
        ("Joins", "SELECT t.id, u.tag FROM t, u WHERE t.id = u.tid"),
        ("Joins", "SELECT count(*) FROM t CROSS JOIN d"),
        ("Joins", "SELECT t.name, sum(u.n) FROM t JOIN u ON u.tid = t.id GROUP BY t.name"),

        // === Subqueries ===
        ("Subqueries", "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.id = t.id)"),
        ("Subqueries", "SELECT id FROM t WHERE id = (SELECT max(id) FROM t)"),
        ("Subqueries", "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)"),
        ("Subqueries", "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.n > 7)"),
        ("Subqueries", "SELECT id FROM t WHERE id IN (SELECT tid FROM u)"),
        ("Subqueries", "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL)"),
        ("Subqueries", "SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t"),
        ("Subqueries", "SELECT id FROM t WHERE amt > (SELECT avg(amt) FROM t)"),
        ("Subqueries", "SELECT id, (SELECT name FROM t t2 WHERE t2.id = t.id + 1) FROM t"),
        ("Subqueries", "SELECT * FROM (SELECT id, name FROM t WHERE id > 1) AS sub"),
        ("Subqueries", "SELECT id FROM t WHERE amt > ALL (SELECT n FROM u WHERE u.tid = t.id)"),
        ("Subqueries", "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM t) x WHERE rn = 1"),
        ("Subqueries", "SELECT t.id FROM t WHERE t.id = ANY (SELECT tid FROM u WHERE tid IS NOT NULL)"),
        ("Subqueries", "SELECT (SELECT count(*) FROM t) AS total"),
        ("Subqueries", "SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.amt > t1.amt)"),

        // === Set operations ===
        ("Set operations", "SELECT id FROM t UNION SELECT id FROM t"),
        ("Set operations", "SELECT id FROM t EXCEPT SELECT id FROM t"),
        ("Set operations", "SELECT id FROM t UNION ALL SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t INTERSECT SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t UNION SELECT tid FROM u ORDER BY 1"),
        ("Set operations", "SELECT id FROM t UNION ALL SELECT id FROM t ORDER BY id LIMIT 3"),
        ("Set operations", "SELECT tid FROM u INTERSECT ALL SELECT tid FROM u"),
        ("Set operations", "SELECT id FROM t EXCEPT ALL SELECT tid FROM u"),
        ("Set operations", "(SELECT id FROM t ORDER BY id LIMIT 2) UNION (SELECT tid FROM u ORDER BY tid LIMIT 2)"),
        ("Set operations", "SELECT id FROM t WHERE id < 3 UNION SELECT id FROM t WHERE id > 1"),
        ("Set operations", "SELECT id::TEXT FROM t UNION SELECT name FROM t"),

        // === CTEs ===
        ("CTEs", "WITH x AS (SELECT id FROM t) SELECT id FROM x"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT tid FROM u) SELECT a.id FROM a JOIN b ON b.tid = a.id"),
        ("CTEs", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a WHERE id > 1) SELECT * FROM b"),
        ("CTEs", "WITH x(a, b) AS (SELECT id, name FROM t) SELECT a, b FROM x"),
        ("CTEs", "WITH a AS (SELECT id FROM t), b AS (SELECT tid AS id FROM u), c AS (SELECT id FROM a UNION SELECT id FROM b) SELECT * FROM c"),
        ("CTEs", "WITH RECURSIVE r AS (SELECT id, name FROM t WHERE id = 1 UNION ALL SELECT t.id, t.name FROM t JOIN r ON t.id = r.id + 1) SELECT * FROM r"),
        ("CTEs", "WITH a AS MATERIALIZED (SELECT id FROM t) SELECT * FROM a"),
        ("CTEs", "WITH a AS NOT MATERIALIZED (SELECT id FROM t) SELECT * FROM a"),
        ("CTEs", "WITH a AS (SELECT id FROM t) SELECT (SELECT count(*) FROM a)"),

        // === Expressions ===
        ("Expressions", "SELECT upper(name) FROM t"),
        ("Expressions", "SELECT name || '!' FROM t"),
        ("Expressions", "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM t"),
        ("Expressions", "SELECT COALESCE(name, 'none'), NULLIF(id, 1) FROM t"),
        ("Expressions", "SELECT GREATEST(id, 2), LEAST(id, 2) FROM t"),
        ("Expressions", "SELECT id::TEXT, amt::INTEGER FROM t"),
        ("Expressions", "SELECT substring(name FROM 1 FOR 1), length(name), trim(name) FROM t"),
        ("Expressions", "SELECT replace(name, 'a', 'A'), position('a' IN name) FROM t"),
        ("Expressions", "SELECT extract(YEAR FROM day) FROM d"),
        ("Expressions", "SELECT date_trunc('month', ts) FROM d"),
        ("Expressions", "SELECT day + INTERVAL '10 days' FROM d"),
        ("Expressions", "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM t"),
        ("Expressions", "SELECT CASE WHEN id = 1 THEN 'a' WHEN id = 2 THEN 'b' ELSE 'c' END FROM t"),
        ("Expressions", "SELECT id % 2 FROM t"),
        ("Expressions", "SELECT id ^ 2 FROM t"),
        ("Expressions", "SELECT amt + id * 2 - 1 FROM t"),
        ("Expressions", "SELECT id::FLOAT8 / 2 FROM t"),
        ("Expressions", "SELECT lower(name), upper(name), initcap(name) FROM t"),
        ("Expressions", "SELECT lpad(name, 5, '*'), rpad(name, 5, '*') FROM t"),
        ("Expressions", "SELECT split_part('a,b,c', ',', 2)"),
        ("Expressions", "SELECT left(name, 1), right(name, 1) FROM t"),
        ("Expressions", "SELECT repeat(name, 2) FROM t"),
        ("Expressions", "SELECT name ~ '^a' FROM t"),
        ("Expressions", "SELECT name ~* '^A' FROM t"),
        ("Expressions", "SELECT name SIMILAR TO 'a%' FROM t"),
        ("Expressions", "SELECT ROW(id, name) FROM t"),
        ("Expressions", "SELECT (id, name) = (1, 'a') FROM t"),
        ("Expressions", "SELECT ARRAY[1,2,3]"),
        ("Expressions", "SELECT ARRAY['x','y']"),
        ("Expressions", "SELECT 1 = ANY(ARRAY[1,2,3])"),
        ("Expressions", "SELECT extract(EPOCH FROM ts) FROM d"),
        ("Expressions", "SELECT extract(DOW FROM day) FROM d"),
        ("Expressions", "SELECT date_part('month', day) FROM d"),
        ("Expressions", "SELECT age(ts, '2024-01-01'::timestamp) FROM d"),
        ("Expressions", "SELECT ts - INTERVAL '1 hour' FROM d"),
        ("Expressions", "SELECT day - INTERVAL '1 day' FROM d"),
        ("Expressions", "SELECT to_char(day, 'YYYY-MM-DD') FROM d"),
        ("Expressions", "SELECT CURRENT_DATE = CURRENT_DATE"),
        ("Expressions", "SELECT INTERVAL '1 day' + INTERVAL '2 hours'"),
        ("Expressions", "SELECT INTERVAL '1 month' > INTERVAL '1 day'"),

        // === Ordering and pagination ===
        ("Ordering/Pagination", "SELECT id, name FROM t ORDER BY id LIMIT 2"),
        ("Ordering/Pagination", "SELECT id FROM t LIMIT 2 OFFSET 1"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY amt DESC NULLS LAST"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY amt / 2"),
        ("Ordering/Pagination", "SELECT DISTINCT ON (tid) tid, n FROM u ORDER BY tid, n"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY id DESC, name ASC"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY 1 DESC"),
        ("Ordering/Pagination", "SELECT id FROM t LIMIT ALL"),
        ("Ordering/Pagination", "SELECT id FROM t OFFSET 1"),
        ("Ordering/Pagination", "SELECT id FROM t FETCH FIRST 2 ROWS ONLY"),
        ("Ordering/Pagination", "SELECT id FROM t ORDER BY id FETCH FIRST 1 ROW WITH TIES"),
        ("Ordering/Pagination", "SELECT DISTINCT ON (tid, tag) tid, tag, n FROM u ORDER BY tid, tag, n"),

        // === DML ===
        ("DML", "INSERT INTO t VALUES (4,'d',4.5)"),
        ("DML", "INSERT INTO t SELECT 5, 'e', 5.5"),
        ("DML", "UPDATE t SET amt = amt + 1 WHERE id = 1"),
        ("DML", "DELETE FROM t WHERE id = 5"),
        ("DML", "INSERT INTO t VALUES (6,'f',6.5) RETURNING id"),
        ("DML", "INSERT INTO p VALUES (4,40,'d') ON CONFLICT (id) DO NOTHING"),
        ("DML", "INSERT INTO p VALUES (1,99,'z') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"),
        ("DML", "UPDATE p SET val = val + 1 WHERE id = 1 RETURNING id, val"),
        ("DML", "DELETE FROM p WHERE id = 2 RETURNING *"),
        ("DML", "UPDATE p SET val = u.n FROM u WHERE p.id = u.tid AND u.n IS NOT NULL"),
        ("DML", "DELETE FROM p USING u WHERE p.id = u.tid AND u.n > 100"),
        ("DML", "INSERT INTO p SELECT id, amt::INTEGER, name FROM t WHERE id > 100"),
        ("DML", "INSERT INTO p (id, tag) VALUES (5, 'e')"),
        ("DML", "INSERT INTO t (id, name) VALUES (7, 'g')"),
        ("DML", "UPDATE t SET name = upper(name) WHERE id IN (SELECT tid FROM u)"),

        // === Literals and types ===
        ("Literals/Types", "SELECT 9223372036854775807::BIGINT"),
        ("Literals/Types", "SELECT -9223372036854775807::BIGINT"),
        ("Literals/Types", "SELECT 2147483647::INTEGER"),
        ("Literals/Types", "SELECT 'Infinity'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT '-Infinity'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT 'NaN'::DOUBLE PRECISION"),
        ("Literals/Types", "SELECT 0.0 = -0.0"),
        ("Literals/Types", "SELECT true, false, NULL::BOOLEAN"),
        ("Literals/Types", "SELECT s FROM mb WHERE s = 'héllo'"),
        ("Literals/Types", "SELECT length(s), char_length(s) FROM mb"),
        ("Literals/Types", "SELECT upper(s) FROM mb"),
        ("Literals/Types", "SELECT n, iv, tz, tags, big, small FROM e"),
        ("Literals/Types", "SELECT n FROM e WHERE tags IS NULL"),
        ("Literals/Types", "SELECT tags[1] FROM e WHERE id = 1"),
        ("Literals/Types", "SELECT array_length(tags, 1) FROM e WHERE id = 1"),
        ("Literals/Types", "SELECT big + 0 FROM e"),
        ("Literals/Types", "SELECT '2024-01-15'::DATE + 1"),
        ("Literals/Types", "SELECT '{1,2,3}'::INTEGER[]"),
    ];

    // Catch panics per query. A panic here is itself a finding — a query that
    // aborts rather than returning an error would take down a real session —
    // so one bad shape must not hide the histogram for all the others.
    // ONE pass. An earlier version ran the list twice — once for the
    // histogram and once for attribution — which doubled every count and made
    // the coverage figure meaningless.
    let total = queries.len();
    let mut panicked: Vec<&str> = Vec::new();
    // A query that ERRORS is a worse finding than one that falls back, and the
    // served/fallback counters cannot tell them apart — both simply fail to
    // increment `served`. Before this was tracked, a shape that Basin rejects
    // outright and a shape it hands to DataFusion looked identical in the
    // output, which flatters the engine: a fallback still returns the right
    // answer to the user, and an error does not.
    let mut errored: Vec<(&str, String)> = Vec::new();
    // Per-area buckets, in first-seen order (i.e. the section order above),
    // so the summary reads as a punch list of where coverage is thin rather
    // than just a single aggregate ratio.
    let mut areas: Vec<(&str, AreaStats)> = Vec::new();
    // Per-query attribution. The aggregate histogram says WHAT KIND of gaps
    // remain; this says WHICH query hit which, which is what actually picks the
    // next piece of work.
    eprintln!("\n─── per query ───");
    for &(area, q) in queries {
        let before = eng.owned_engine_served_count();
        let res = std::panic::AssertUnwindSafe(s.execute(q))
            .catch_unwind()
            .await;
        match &res {
            Err(_) => panicked.push(q),
            Ok(Err(e)) => errored.push((q, e.to_string())),
            Ok(Ok(_)) => {}
        }
        let served_it = eng.owned_engine_served_count() > before;
        let verdict = match (&res, served_it) {
            (Err(_), _) => "PANIC",
            (Ok(Err(_)), _) => "ERROR",
            (Ok(Ok(_)), true) => "served",
            (Ok(Ok(_)), false) => "FELL BACK",
        };
        eprintln!("  {verdict:>9}  [{area}] {q}");

        let idx = areas.iter().position(|(a, _)| *a == area).unwrap_or_else(|| {
            areas.push((area, AreaStats::default()));
            areas.len() - 1
        });
        let stats = &mut areas[idx].1;
        stats.total += 1;
        match verdict {
            "PANIC" => stats.panicked += 1,
            "ERROR" => stats.errored += 1,
            "served" => stats.served += 1,
            "FELL BACK" => stats.fallback += 1,
            _ => unreachable!(),
        }
    }

    if !errored.is_empty() {
        eprintln!("\nERRORED — {} of {total}:", errored.len());
        for (q, e) in &errored {
            eprintln!("  {q}\n      {e}");
        }
    }

    if !panicked.is_empty() {
        eprintln!(
            "\nPANICKED (not merely errored) — {} of {total}:",
            panicked.len()
        );
        for q in &panicked {
            eprintln!("  {q}");
        }
    }

    eprintln!("\n─── per-area coverage ───");
    for (area, st) in &areas {
        eprintln!(
            "  {area:<20} served {:>3} / {:<3} fallback {:>3}  errored {:>3}  panicked {:>3}",
            st.served, st.total, st.fallback, st.errored, st.panicked
        );
    }

    let served = eng.owned_engine_served_count();
    let fallback = eng.owned_engine_fallback_count();
    eprintln!("\n─── owned-engine coverage over {total} representative queries ───");
    eprintln!("served   : {served}");
    eprintln!("fallback : {fallback}");
    eprintln!("reasons  : {:?}", eng.owned_engine_fallback_reason_counts());
    eprintln!(
        "optimizer: {} plans, {} productive passes, {} converged in zero",
        eng.owned_engine_optimizer_plans_count(),
        eng.owned_engine_optimizer_passes_total(),
        eng.owned_engine_optimizer_zero_pass_count(),
    );
}
