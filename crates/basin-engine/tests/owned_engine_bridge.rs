//! End-to-end guard for the `BASIN_OWNED_ENGINE` bridge (`owned_engine.rs`).
//!
//! Before this bridge existed, `basin-plan` (lowering + IR + optimizer) and
//! `basin-exec` (operators, real Vortex/Parquet reads) were a fully tested
//! pipeline that no client SELECT had ever reached. These tests exercise the
//! bridge exactly the way a real client would — through `ProjectSession::execute`
//! — and check the three properties the flag's safety depends on:
//!
//! 1. Flag off (the default) leaves `execute()` byte-for-byte unchanged and
//!    never touches either counter.
//! 2. Flag on serves a real, simple SELECT against a real table end to end,
//!    and its answer matches what the same SQL returns via DataFusion (flag
//!    off) — proving the bridge doesn't just "not crash" but returns the
//!    right rows.
//! 3. A construct the owned pipeline does not support (`SELECT DISTINCT`,
//!    per `basin-plan/src/lower/select.rs`'s documented scope) still returns
//!    the correct answer — it must fall back to DataFusion rather than
//!    error — and the fallback counter, not the served counter, is the one
//!    that advances.
//!
//! `BASIN_OWNED_ENGINE` is a process-wide env var, so every test that
//! touches it serializes on [`ENV_LOCK`] for the full window during which
//! its value matters, and restores it to unset afterwards — otherwise a
//! concurrently-running test in this same binary could observe the wrong
//! value.

use std::sync::{Arc, Mutex};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Serializes every test in this binary that sets/reads `BASIN_OWNED_ENGINE`
/// — see the module docs.
static ENV_LOCK: Mutex<()> = Mutex::new(());

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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Flatten `(id, name)` batches — the two-column shape every test below
/// selects — into a plain `Vec` so assertions don't depend on batch
/// boundaries, and two runs of the *same* logical result (one served by
/// each engine) compare equal regardless of how each happened to batch rows.
fn flatten_id_name(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..b.num_rows() {
            let id = ids.value(r);
            let name = (!names.is_null(r)).then(|| names.value(r).to_string());
            out.push((id, name));
        }
    }
    out
}

async fn seed_table(sess: &ProjectSession) {
    exec(sess, "CREATE TABLE t (id BIGINT, name TEXT)").await;
    exec(
        sess,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta')",
    )
    .await;
}

/// Property 1: with the flag unset, `execute()` never invokes the bridge at
/// all — both counters stay at zero and the query still returns the right
/// rows via the unchanged DataFusion path.
#[tokio::test]
async fn flag_off_by_default_leaves_behaviour_and_counters_untouched() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("BASIN_OWNED_ENGINE");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    let batches = rows(&sess, "SELECT id, name FROM t WHERE id > 1 ORDER BY id").await;
    let got = flatten_id_name(&batches);
    assert_eq!(
        got,
        vec![
            (2, Some("beta".to_string())),
            (3, Some("gamma".to_string())),
            (4, Some("delta".to_string())),
        ]
    );

    assert_eq!(
        eng.owned_engine_served_count(),
        0,
        "the bridge must not be attempted at all with the flag unset"
    );
    assert_eq!(
        eng.owned_engine_fallback_count(),
        0,
        "no attempt means no fallback either — flag-off is not 'always falls back'"
    );
}

/// Property 2: flag on serves a real `WHERE ... ORDER BY` SELECT end to end
/// through the owned pipeline, reading the same real Vortex/Parquet files
/// DataFusion would, and returns the identical answer.
#[tokio::test]
async fn flag_on_serves_a_simple_select_matching_datafusion() {
    let _guard = ENV_LOCK.lock().unwrap();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    const SQL: &str = "SELECT id, name FROM t WHERE id > 1 ORDER BY id";

    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned_batches = rows(&sess, SQL).await;
    let owned = flatten_id_name(&owned_batches);
    assert_eq!(
        eng.owned_engine_served_count(),
        1,
        "the owned pipeline must have actually served this query"
    );
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let df_batches = rows(&sess, SQL).await;
    let df = flatten_id_name(&df_batches);

    assert_eq!(
        owned, df,
        "the owned engine's answer must match DataFusion's for the same SQL"
    );
    assert_eq!(
        owned,
        vec![
            (2, Some("beta".to_string())),
            (3, Some("gamma".to_string())),
            (4, Some("delta".to_string())),
        ]
    );
    // The DataFusion-served re-run above must not have moved either counter:
    // the flag was off for it, so `execute()` never called the bridge.
    assert_eq!(eng.owned_engine_served_count(), 1);
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 3: a construct outside the owned pipeline's documented scope
/// (`SELECT DISTINCT` — `basin-plan/src/lower/select.rs` names this
/// explicitly as `LowerError::Unsupported`) must still produce the correct
/// answer, via a silent fallback to DataFusion, and record itself as a
/// fallback rather than a served query or a client-visible error.
#[tokio::test]
async fn unsupported_construct_falls_back_instead_of_erroring() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, "CREATE TABLE d (id BIGINT, name TEXT)").await;
    exec(
        &sess,
        "INSERT INTO d (id, name) VALUES (1, 'x'), (1, 'x'), (2, 'y')",
    )
    .await;

    // DISTINCT is explicitly unsupported by `lower_select_stmt` today (see
    // that function's `stmt.distinct_clause` guard) — must fall back, not
    // error, and still return the deduplicated rows.
    let batches = rows(&sess, "SELECT DISTINCT id, name FROM d ORDER BY id").await;
    let got = flatten_id_name(&batches);
    assert_eq!(
        got,
        vec![(1, Some("x".to_string())), (2, Some("y".to_string()))],
        "a fallback must still return the correct (deduplicated) answer"
    );

    assert_eq!(
        eng.owned_engine_served_count(),
        0,
        "an unsupported construct must not be reported as served"
    );
    assert_eq!(
        eng.owned_engine_fallback_count(),
        1,
        "the fallback counter is exactly the one that should move here"
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
}
