//! #113 — correlated `LATERAL generate_series(<lo>, t.<col>[, <step>])`.
//!
//! `SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g` expands each
//! row of `t` into the integer series `1 .. t.id`. DataFusion 53's built-in
//! `generate_series` table function rejects a non-literal argument (a
//! correlated column ref OR a scalar subquery — empirically the same error),
//! so the textbook `generate_series(1, (SELECT max(t.id) FROM t))` rewrite is
//! NOT viable on this engine. `rewrite_lateral_generate_series` instead
//! decorrelates the shape into a bounded recursive-CTE JOIN that reproduces
//! exact PostgreSQL semantics.
//!
//! These tests assert the **exact** row set (ordered) for: per-row expansion,
//! empty table, id = 0 / NULL edges, the explicit step variant, and — as a
//! regression guard — that the already-working non-correlated
//! `generate_series(1, 3)` form is left untouched.
//!
//! The expanded column is referenced as `g.g` (and `gs.i` under an `AS gs(i)`
//! column-alias list) because that is what PostgreSQL names it: a scalar
//! SRF's output column takes the FROM-item's alias. `g.value` — DataFusion's
//! own name for it, which these cases used to write — is rejected by a real
//! server. See `pg_operators::rewrite_srf_from_alias_colname` for the rule
//! and `tests/differential_pg.rs::diff_srf_output_column_naming` for the
//! shape-by-shape parity assertions against a live PostgreSQL.

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array};
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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

/// Pull a single integer column (Int32 or Int64) as `Vec<i64>`.
fn col_i64(c: &Arc<dyn Array>, r: usize) -> i64 {
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        a.value(r)
    } else if let Some(a) = c.as_any().downcast_ref::<Int32Array>() {
        a.value(r) as i64
    } else {
        panic!("expected an integer column");
    }
}

/// Run a two-integer-column SELECT and return `(col0, col1)` pairs in order.
async fn pairs(sess: &ProjectSession, sql: &str) -> Vec<(i64, i64)> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut v = Vec::new();
    for b in &batches {
        let c0 = b.column(0);
        let c1 = b.column(1);
        for r in 0..b.num_rows() {
            v.push((col_i64(c0, r), col_i64(c1, r)));
        }
    }
    v
}

async fn count_rows(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn cross_join_lateral_per_row_expansion() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (2), (3)").await.unwrap();

    // 2 → {1,2}, 3 → {1,2,3}.
    let got = pairs(
        &sess,
        "SELECT t.id, g.g \
         FROM t CROSS JOIN LATERAL generate_series(1, t.id) g \
         ORDER BY t.id, g.g",
    )
    .await;
    assert_eq!(got, vec![(2, 1), (2, 2), (3, 1), (3, 2), (3, 3)]);
}

#[tokio::test]
async fn comma_lateral_with_column_alias_list() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1), (4)").await.unwrap();

    // Comma form + `AS gs(i)` column-alias list, which — as in PostgreSQL —
    // names the expanded column `i`.
    let got = pairs(
        &sess,
        "SELECT t.id, gs.i \
         FROM t, LATERAL generate_series(1, t.id) AS gs(i) \
         ORDER BY t.id, gs.i",
    )
    .await;
    assert_eq!(got, vec![(1, 1), (4, 1), (4, 2), (4, 3), (4, 4)]);
}

#[tokio::test]
async fn empty_table_yields_no_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();

    let n = count_rows(
        &sess,
        "SELECT t.id, g.g FROM t CROSS JOIN LATERAL generate_series(1, t.id) g",
    )
    .await;
    assert_eq!(n, 0, "empty driving table ⇒ zero expanded rows");
}

#[tokio::test]
async fn zero_and_null_ids_contribute_no_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE z (id INT)").await.unwrap();
    sess.execute("INSERT INTO z VALUES (0), (NULL), (3)")
        .await
        .unwrap();

    // id = 0 ⇒ no rows (1 > 0); id = NULL ⇒ no rows; id = 3 ⇒ {1,2,3}.
    let got = pairs(
        &sess,
        "SELECT z.id, g.g \
         FROM z CROSS JOIN LATERAL generate_series(1, z.id) g \
         ORDER BY z.id, g.g",
    )
    .await;
    assert_eq!(got, vec![(3, 1), (3, 2), (3, 3)]);
}

#[tokio::test]
async fn explicit_step_one_variant() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (3)").await.unwrap();

    // 3-arg form with literal step 1 is supported (same as the 2-arg form).
    let got = pairs(
        &sess,
        "SELECT t.id, g.g \
         FROM t CROSS JOIN LATERAL generate_series(1, t.id, 1) g \
         ORDER BY g.g",
    )
    .await;
    assert_eq!(got, vec![(3, 1), (3, 2), (3, 3)]);
}

#[tokio::test]
async fn lower_bound_other_than_one() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (5)").await.unwrap();

    // generate_series(3, 5) ⇒ {3,4,5}.
    let got = pairs(
        &sess,
        "SELECT t.id, g.g \
         FROM t CROSS JOIN LATERAL generate_series(3, t.id) g \
         ORDER BY g.g",
    )
    .await;
    assert_eq!(got, vec![(5, 3), (5, 4), (5, 5)]);
}

#[tokio::test]
async fn outer_where_and_other_join_preserved() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (2), (3), (4)")
        .await
        .unwrap();

    // The outer WHERE must still filter after decorrelation.
    let got = pairs(
        &sess,
        "SELECT t.id, g.g \
         FROM t CROSS JOIN LATERAL generate_series(1, t.id) g \
         WHERE t.id >= 3 \
         ORDER BY t.id, g.g",
    )
    .await;
    assert_eq!(
        got,
        vec![(3, 1), (3, 2), (3, 3), (4, 1), (4, 2), (4, 3), (4, 4)]
    );
}

/// REGRESSION GUARD: the non-correlated `generate_series(1, 3)` form must NOT
/// be rewritten (DataFusion already handles it) and must keep working.
#[tokio::test]
async fn non_correlated_generate_series_unaffected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (10), (20)")
        .await
        .unwrap();

    // 2 t-rows × series {1,2,3} = 6 rows; series values independent of t.id.
    let got = pairs(
        &sess,
        "SELECT t.id, g.g \
         FROM t CROSS JOIN LATERAL generate_series(1, 3) g \
         ORDER BY t.id, g.g",
    )
    .await;
    assert_eq!(
        got,
        vec![(10, 1), (10, 2), (10, 3), (20, 1), (20, 2), (20, 3)]
    );

    // Plain (non-LATERAL) generate_series also still works.
    let n = count_rows(&sess, "SELECT 0, g FROM generate_series(1, 5) g").await;
    assert_eq!(n, 5, "bare generate_series(1,5) unaffected");
}
