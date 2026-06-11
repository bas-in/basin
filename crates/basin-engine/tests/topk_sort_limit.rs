//! Top-K sort (`ORDER BY … LIMIT k`) plan-shape + correctness tests.
//!
//! ## Why these tests exist (premise verification, 2026-06)
//!
//! The "deep top-K sort" benchmark card (`SELECT * FROM events ORDER BY
//! amount DESC, id LIMIT 1000` at 1M) was suspected of losing DataFusion's
//! TopK optimization to Basin's custom providers (`HtapUnionTable` /
//! `TombstoneFilteringTable` / `UnionExec`). Code-reading shows that premise
//! is stale:
//!
//! * In DataFusion 53 a `SortExec` with `fetch=Some(k)` **is** the TopK
//!   operator — `SortExec::execute` routes through `topk::TopK` (heap
//!   selection) whenever `fetch` is set, and displays as
//!   `SortExec: TopK(fetch=k)`.
//! * The fetch reaches the sort regardless of the table provider: the
//!   logical `PushDownLimit` rule folds `LIMIT k` into `Sort.fetch`, and the
//!   physical planner builds `SortExec::with_fetch` from it. The provider
//!   only shapes the plan **below** the sort.
//!
//! The remaining gap vs Postgres on that card is whole-row decode: `SELECT *`
//! forces the cold scan to materialize every column (incl. JSONB payload) of
//! every row before the TopK heap discards 99.9% of them, and Vortex 0.71
//! declines DataFusion's TopK *dynamic* filter (`is_dynamic_physical_expr` →
//! unsupported, vortex-data/vortex#4034), so no mid-scan skipping occurs.
//! Fixing that needs late materialization (top-k over sort key + PK, then
//! point re-fetch) — out of scope here.
//!
//! These tests pin the verified behaviour so a future regression (a wrapper
//! node that strips `fetch`, a provider change that defeats `PushDownLimit`)
//! is caught immediately:
//!
//! * EXPLAIN shows `SortExec: TopK(fetch=k)` — fetch reaches the sort — both
//!   on freshly-inserted (hot-union) data and with the UPDATE/DELETE overlay
//!   wrappers engaged;
//! * `ORDER BY … LIMIT` is correct over multi-batch data with a hot-tier
//!   UPDATE override and DELETE tombstone applied (row-coherent results);
//! * ties resolve deterministically via the `id` tiebreaker, DESC and ASC;
//! * `LIMIT` larger than the table returns every row, sorted;
//! * `ORDER BY` *without* LIMIT still plans a full (fetch-less) sort and
//!   returns the complete ordered result — no regression from any top-K work.
//!
//! All queries use a TWO-column ORDER BY (`amount …, id`) on purpose: the
//! single-column form is intercepted by `fast_select`'s own top-k path, and
//! the benchmark shape under investigation is the two-column DataFusion path.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// Collect the full `EXPLAIN` text (Basin renders a single `QUERY PLAN`
/// column, one line per row — see `src/explain.rs`).
async fn explain_text(sess: &ProjectSession, sql: &str) -> String {
    let batches = rows(sess, &format!("EXPLAIN {sql}")).await;
    let mut plan = String::new();
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            plan.push_str(arr.value(i));
            plan.push('\n');
        }
    }
    plan
}

/// Shared dataset: single-column BIGINT PK (`id`), a coherence column
/// (`user_id = id * 100`), and the sort column (`amount = id * 10`). Inserted
/// in three separate statements so the read path sees multiple batches/files.
async fn seed_topk_events(sess: &ProjectSession) {
    sess.execute(
        "CREATE TABLE topk_ev (id BIGINT NOT NULL PRIMARY KEY, \
         user_id BIGINT NOT NULL, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO topk_ev VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)")
        .await
        .unwrap();
    sess.execute("INSERT INTO topk_ev VALUES (4, 400, 40), (5, 500, 50)")
        .await
        .unwrap();
    sess.execute("INSERT INTO topk_ev VALUES (6, 600, 60)")
        .await
        .unwrap();
}

/// Fetch reaches the sort on freshly-written data (the union/hot read path):
/// the physical plan must contain the TopK form of `SortExec` and no
/// fetch-less `SortExec` may remain anywhere in the plan.
#[tokio::test]
async fn explain_topk_fetch_reaches_sort_over_fresh_data() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_topk_events(&sess).await;

    let plan = explain_text(
        &sess,
        "SELECT * FROM topk_ev ORDER BY amount DESC, id LIMIT 5",
    )
    .await;

    assert!(
        plan.contains("TopK(fetch=5)"),
        "LIMIT must reach the SortExec as fetch (TopK); plan:\n{plan}"
    );
    // `SortExec: expr=` is the fetch-less display form (DataFusion 53,
    // sorts/sort.rs DisplayAs). A fetch-less sort under the limit would mean
    // a full 1M-row sort at scale — the exact regression this test guards.
    assert!(
        !plan.contains("SortExec: expr="),
        "no fetch-less SortExec may remain under ORDER BY+LIMIT; plan:\n{plan}"
    );
}

/// Fetch still reaches the sort when the hot-tier UPDATE/DELETE overlay
/// wrappers (`UpdateOverlayExec` / `TombstoneFilterExec`) are in the plan.
#[tokio::test]
async fn explain_topk_fetch_reaches_sort_with_update_delete_overlay() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_topk_events(&sess).await;

    // Single-row PK UPDATE + DELETE: the canonical hot-tier fast-path shapes
    // that put the overlay wrappers on every subsequent scan.
    sess.execute("UPDATE topk_ev SET amount = 999 WHERE id = 2")
        .await
        .unwrap();
    sess.execute("DELETE FROM topk_ev WHERE id = 6").await.unwrap();

    let plan = explain_text(
        &sess,
        "SELECT * FROM topk_ev ORDER BY amount DESC, id LIMIT 3",
    )
    .await;

    assert!(
        plan.contains("TopK(fetch=3)"),
        "LIMIT must reach the SortExec as fetch with overlay wrappers; plan:\n{plan}"
    );
    assert!(
        !plan.contains("SortExec: expr="),
        "no fetch-less SortExec may remain under ORDER BY+LIMIT; plan:\n{plan}"
    );
}

/// Correctness over multi-batch data + overlay: an UPDATE that promotes a row
/// into the top-k and a DELETE that removes the previous maximum must both be
/// reflected, and every returned column must come from the same source row.
#[tokio::test]
async fn order_by_limit_reflects_overlay_update_and_delete() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_topk_events(&sess).await;

    // id 2 jumps from amount 20 to the global maximum; id 6 (old maximum,
    // amount 60) is deleted.
    sess.execute("UPDATE topk_ev SET amount = 999 WHERE id = 2")
        .await
        .unwrap();
    sess.execute("DELETE FROM topk_ev WHERE id = 6").await.unwrap();

    let batches = rows(
        &sess,
        "SELECT * FROM topk_ev ORDER BY amount DESC, id LIMIT 3",
    )
    .await;

    // Survivors by amount DESC: id 2 (999), id 5 (50), id 4 (40).
    assert_eq!(col_i64(&batches, "id"), vec![2, 5, 4]);
    assert_eq!(col_i64(&batches, "amount"), vec![999, 50, 40]);
    // Row coherence: user_id was never updated, so the override row for id 2
    // must still carry its original user_id (200) — a mixed/stale row breaks
    // the id*100 invariant.
    assert_eq!(col_i64(&batches, "user_id"), vec![200, 500, 400]);
}

/// Ties on the sort column resolve deterministically through the `id`
/// tiebreaker — both directions.
#[tokio::test]
async fn order_by_limit_ties_resolved_by_id_tiebreaker() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE tied_topk (id BIGINT NOT NULL PRIMARY KEY, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    // All five rows tie on amount.
    sess.execute("INSERT INTO tied_topk VALUES (1, 7), (2, 7), (3, 7)")
        .await
        .unwrap();
    sess.execute("INSERT INTO tied_topk VALUES (4, 7), (5, 7)")
        .await
        .unwrap();

    let batches = rows(
        &sess,
        "SELECT * FROM tied_topk ORDER BY amount DESC, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3]);

    let batches = rows(
        &sess,
        "SELECT * FROM tied_topk ORDER BY amount ASC, id DESC LIMIT 3",
    )
    .await;
    assert_eq!(col_i64(&batches, "id"), vec![5, 4, 3]);
}

/// LIMIT larger than the table: every row comes back, fully sorted, and the
/// plan still takes the TopK form (fetch is set even when it exceeds the
/// input cardinality).
#[tokio::test]
async fn order_by_limit_exceeding_row_count_returns_all_sorted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_topk_events(&sess).await;

    let batches = rows(
        &sess,
        "SELECT * FROM topk_ev ORDER BY amount DESC, id LIMIT 100",
    )
    .await;
    assert_eq!(col_i64(&batches, "id"), vec![6, 5, 4, 3, 2, 1]);
    assert_eq!(col_i64(&batches, "amount"), vec![60, 50, 40, 30, 20, 10]);

    let plan = explain_text(
        &sess,
        "SELECT * FROM topk_ev ORDER BY amount DESC, id LIMIT 100",
    )
    .await;
    assert!(
        plan.contains("TopK(fetch=100)"),
        "fetch must reach the sort even when LIMIT > row count; plan:\n{plan}"
    );
}

/// ORDER BY without LIMIT: full sort, complete result, and the plan must use
/// the fetch-less `SortExec` (a spurious fetch here would silently truncate).
#[tokio::test]
async fn order_by_without_limit_remains_full_sort() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_topk_events(&sess).await;

    let batches = rows(&sess, "SELECT * FROM topk_ev ORDER BY amount ASC, id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(col_i64(&batches, "amount"), vec![10, 20, 30, 40, 50, 60]);

    let plan = explain_text(&sess, "SELECT * FROM topk_ev ORDER BY amount ASC, id").await;
    assert!(
        plan.contains("SortExec: expr="),
        "ORDER BY without LIMIT must plan a fetch-less full sort; plan:\n{plan}"
    );
    assert!(
        !plan.contains("TopK(fetch="),
        "no fetch may be invented for an un-limited ORDER BY; plan:\n{plan}"
    );
}
