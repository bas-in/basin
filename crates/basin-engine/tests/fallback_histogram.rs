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
    ];

    // Catch panics per query. A panic here is itself a finding — a query that
    // aborts rather than returning an error would take down a real session —
    // so one bad shape must not hide the histogram for all the others.
    let total = queries.len();
    let mut panicked: Vec<&str> = Vec::new();
    for q in queries {
        let res = std::panic::AssertUnwindSafe(s.execute(q))
            .catch_unwind()
            .await;
        if res.is_err() {
            panicked.push(q);
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
