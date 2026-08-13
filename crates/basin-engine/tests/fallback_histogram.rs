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
    // A second relation, so joins are between different tables rather than
    // self-joins. A self-join hides whole classes of bug: identical schemas on
    // both sides make a left/right mix-up invisible.
    s.execute("CREATE TABLE u (uid BIGINT NOT NULL, tid BIGINT, tag TEXT, n INTEGER)")
        .await
        .unwrap();
    s.execute("INSERT INTO u VALUES (10,1,'x',7),(11,1,'y',8),(12,2,'z',9)")
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

    // Shapes an application actually issues, not shapes the engine is known to
    // handle — the point is to find the gaps, so a flattering list is useless.
    let queries: &[&str] = &[
        "SELECT id FROM t",
        "SELECT id FROM t WHERE id > 1",
        "SELECT id, name FROM t ORDER BY id LIMIT 2",
        "SELECT id FROM t LIMIT 2 OFFSET 1",
        "SELECT count(*) FROM t",
        "SELECT name, sum(amt) FROM t GROUP BY name",
        "SELECT name, count(*) FROM t GROUP BY name HAVING count(*) > 0",
        "SELECT DISTINCT name FROM t",
        "SELECT upper(name) FROM t",
        "SELECT name || '!' FROM t",
        "SELECT id FROM t WHERE name LIKE 'a%'",
        "SELECT id FROM t WHERE id IN (1,2)",
        "SELECT id FROM t WHERE amt BETWEEN 1.0 AND 3.0",
        "SELECT id FROM t WHERE name IS NOT NULL",
        "SELECT id, row_number() OVER (ORDER BY id) FROM t",
        "SELECT id, lag(id) OVER (ORDER BY id) FROM t",
        "WITH x AS (SELECT id FROM t) SELECT id FROM x",
        "SELECT a.id FROM t a JOIN t b ON a.id = b.id",
        "SELECT a.id FROM t a LEFT JOIN t b ON a.id = b.id",
        "SELECT id FROM t UNION SELECT id FROM t",
        "SELECT id FROM t EXCEPT SELECT id FROM t",
        "SELECT generate_series(1,3)",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t u WHERE u.id = t.id)",
        "SELECT id FROM t WHERE id = (SELECT max(id) FROM t)",
        "SELECT id FROM t ORDER BY amt DESC NULLS LAST",
        "SELECT sum(amt) FILTER (WHERE id > 1) FROM t",
        // --- joins between DIFFERENT relations, and the kinds not yet covered
        "SELECT t.id, u.tag FROM t JOIN u ON u.tid = t.id",
        "SELECT t.id, u.tag FROM t RIGHT JOIN u ON u.tid = t.id",
        "SELECT t.id, u.tag FROM t FULL JOIN u ON u.tid = t.id",
        "SELECT t.id, u.tag FROM t CROSS JOIN u",
        "SELECT t.id FROM t JOIN u ON u.tid = t.id AND u.n > 7",
        "SELECT t.id FROM t JOIN u ON u.tid = t.id WHERE u.tag <> 'x'",
        "SELECT t.id, u.tag, d.flag FROM t JOIN u ON u.tid = t.id JOIN d ON d.id = t.id",
        // --- correlated subqueries beyond the plain equality case
        "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.n > 7)",
        "SELECT id FROM t WHERE id IN (SELECT tid FROM u)",
        "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL)",
        "SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t",
        "SELECT id FROM t WHERE amt > (SELECT avg(amt) FROM t)",
        // --- aggregates
        "SELECT min(amt), max(amt), avg(amt) FROM t",
        "SELECT count(DISTINCT name) FROM t",
        "SELECT name, count(*) FROM t GROUP BY name ORDER BY count(*) DESC",
        "SELECT tid, count(*) FROM u GROUP BY tid HAVING count(*) > 1",
        "SELECT string_agg(name, ',') FROM t",
        "SELECT array_agg(id) FROM t",
        // --- set operations
        "SELECT id FROM t UNION ALL SELECT tid FROM u",
        "SELECT id FROM t INTERSECT SELECT tid FROM u",
        "SELECT id FROM t UNION SELECT tid FROM u ORDER BY 1",
        // --- CTEs
        "WITH a AS (SELECT id FROM t), b AS (SELECT tid FROM u) SELECT a.id FROM a JOIN b ON b.tid = a.id",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r",
        // --- window functions with partitions and frames
        "SELECT tid, n, rank() OVER (PARTITION BY tid ORDER BY n) FROM u",
        "SELECT tid, sum(n) OVER (PARTITION BY tid) FROM u",
        "SELECT n, sum(n) OVER (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM u",
        "SELECT n, first_value(n) OVER (PARTITION BY tid ORDER BY n) FROM u",
        // --- conditional and null-handling expressions
        "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END FROM t",
        "SELECT COALESCE(name, 'none'), NULLIF(id, 1) FROM t",
        "SELECT GREATEST(id, 2), LEAST(id, 2) FROM t",
        "SELECT id FROM t WHERE name IS DISTINCT FROM 'a'",
        // --- casts and string functions
        "SELECT id::TEXT, amt::INTEGER FROM t",
        "SELECT substring(name FROM 1 FOR 1), length(name), trim(name) FROM t",
        "SELECT replace(name, 'a', 'A'), position('a' IN name) FROM t",
        // --- date/time
        "SELECT day, ts FROM d",
        "SELECT extract(YEAR FROM day) FROM d",
        "SELECT date_trunc('month', ts) FROM d",
        "SELECT day + INTERVAL '10 days' FROM d",
        "SELECT id FROM d WHERE flag",
        // --- ordering and pagination variants
        "SELECT id FROM t ORDER BY amt / 2",
        "SELECT DISTINCT ON (tid) tid, n FROM u ORDER BY tid, n",
        "SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i, s)",
        // --- LATERAL, which the differential gate has open in three tests
        "SELECT t.id, g.i FROM t, LATERAL generate_series(1, 2) AS g(i)",
        // --- DML, which is lowered as a relation rather than a separate path
        "INSERT INTO t VALUES (4,'d',4.5)",
        "INSERT INTO t SELECT 5, 'e', 5.5",
        "UPDATE t SET amt = amt + 1 WHERE id = 1",
        "DELETE FROM t WHERE id = 5",
        "INSERT INTO t VALUES (6,'f',6.5) RETURNING id",
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
    // Per-query attribution. The aggregate histogram says WHAT KIND of gaps
    // remain; this says WHICH query hit which, which is what actually picks the
    // next piece of work.
    eprintln!("\n─── per query ───");
    for q in queries {
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
        eprintln!("  {verdict:>9}  {q}");
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
