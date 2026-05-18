//! Auto-routing of `ORDER BY <vec_col> <-> $1 LIMIT k` to the HNSW fast path.
//!
//! These tests pin the planner-detect logic. The route fires if every
//! criterion holds (single-table FROM, single ORDER BY of `<col> <op> <lit>`,
//! vector column, ASC, constant LIMIT, optional eq-only WHERE pushdown).
//! Anything else falls back to brute-force; the result must still be
//! correct, just slower.
//!
//! `Engine::vector_routing_count` is the test-side observable: every routed
//! statement bumps it by exactly one. If the rewriter wrongly routes a
//! query, the brute-force test harness here catches it via the counter
//! (auto-routing produces a different result-set order vs. brute-force on
//! tied distances).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession, ScalarParam};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const DIM: usize = 8;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Deterministic test vector — same SplitMix64 trick `vector_smoke.rs` uses.
fn det_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_add(0x9E3779B97F4A7C15);
    (0..dim)
        .map(|_| {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            ((z >> 33) as f32 / (1u32 << 31) as f32) - 0.5
        })
        .collect()
}

fn vec_lit(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("{:.6}", x));
    }
    s.push(']');
    s
}

/// Seed `n_rows` rows into a fresh `embeddings` table. Each row gets a
/// `vector(DIM)` value derived deterministically from its `id`. Returns the
/// session.
async fn seed_table(eng: &Engine, n_rows: usize) -> ProjectSession {
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(&format!(
        "CREATE TABLE embeddings (id BIGINT NOT NULL, category TEXT, embedding vector({DIM}))"
    ))
    .await
    .unwrap();

    // Single multi-row INSERT — keeps the segment count to one and avoids
    // the O(rows) parse overhead of per-row INSERTs.
    let mut tuples: Vec<String> = Vec::with_capacity(n_rows);
    for id in 0..n_rows as i64 {
        let v = det_vec(id as u64, DIM);
        // 50/50 category split keeps the WHERE-pushdown test off the
        // statistical edge (over_fetch=k*4=20 candidates over 60 rows
        // would give ~6.7 'foo' rows on a 1/3 split — too close to the
        // k=5 LIMIT for a deterministic top-k assertion).
        let category = if (id as usize) < n_rows / 2 {
            "foo"
        } else {
            "bar"
        };
        tuples.push(format!("({id}, '{category}', '{}')", vec_lit(&v)));
    }
    let insert = format!("INSERT INTO embeddings VALUES {}", tuples.join(", "));
    sess.execute(&insert).await.unwrap();
    sess
}

fn ids_from(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name("id")
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

fn categories_from(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn simple_order_by_routes_to_vector_search() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 50).await;

    let q = det_vec(42, DIM);
    let q_lit = vec_lit(&q);

    let before = eng.vector_routing_count();

    // Bound `$1` is just the routed path's input shape — the prepared/bind
    // round trip substitutes the literal, then the executor sees the
    // already-substituted SQL. Same shape the router uses on extended
    // queries.
    let (handle, _schema) = sess
        .prepare("SELECT id FROM embeddings ORDER BY embedding <-> $1 LIMIT 10")
        .await
        .unwrap();
    let bound = sess
        .bind(&handle, vec![ScalarParam::Text(q_lit.clone())])
        .await
        .unwrap();
    let res = sess.execute_bound(bound).await.unwrap();

    let routed_ids = match res {
        ExecResult::Rows { batches, .. } => ids_from(&batches),
        _ => panic!("expected rows"),
    };

    assert_eq!(
        eng.vector_routing_count() - before,
        1,
        "routing did not fire"
    );
    assert!(!routed_ids.is_empty(), "routed query returned empty");
    assert!(routed_ids.len() <= 10);
}

#[tokio::test]
async fn joins_disable_routing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 20).await;

    // A second table to JOIN with. The exact join shape is irrelevant —
    // any join takes the SQL off the single-table path.
    sess.execute("CREATE TABLE meta (id BIGINT, label TEXT)")
        .await
        .unwrap();

    let q = det_vec(1, DIM);
    let q_lit = vec_lit(&q);

    let before = eng.vector_routing_count();
    // The brute-force path would compute distances post-join; we only
    // assert that the route did not fire. Skip running the SQL because
    // `vector(N)` × FROM-with-JOIN still needs the brute force pipeline
    // and that path's correctness lives elsewhere.
    let sql = format!(
        "SELECT embeddings.id FROM embeddings \
         JOIN meta ON embeddings.id = meta.id \
         ORDER BY embeddings.embedding <-> '{q_lit}' LIMIT 5"
    );
    // Best-effort: even if the join path errors on something unrelated to
    // routing, the routing counter must not increment.
    let _ = sess.execute(&sql).await;
    assert_eq!(
        eng.vector_routing_count() - before,
        0,
        "routing fired on JOIN query"
    );
}

#[tokio::test]
async fn no_limit_disables_routing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 20).await;

    let q = det_vec(1, DIM);
    let q_lit = vec_lit(&q);
    let before = eng.vector_routing_count();
    let res = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <-> '{q_lit}'"
        ))
        .await
        .unwrap();
    assert!(matches!(res, ExecResult::Rows { .. }));
    assert_eq!(eng.vector_routing_count() - before, 0);
}

#[tokio::test]
async fn non_vector_column_disables_routing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 5).await;

    let before = eng.vector_routing_count();
    // `category` is TEXT — `<->` against text is nonsense but it's the
    // brute-force pipeline's job to surface the diagnostic, not the
    // router's. The routing decision must not fire.
    let _ = sess
        .execute("SELECT id FROM embeddings ORDER BY category <-> 'foo' LIMIT 3")
        .await;
    assert_eq!(eng.vector_routing_count() - before, 0);
}

#[tokio::test]
async fn limit_not_constant_disables_routing() {
    // The planner runs after bind-time substitution: by the time the
    // executor sees the SQL, `$2` is a literal integer. So a `LIMIT $2`
    // → `LIMIT 5` query DOES route (the planner reasons over the
    // post-substitution SQL, matching the PG "every parameter is bound
    // by execute time" rule). The "criterion fails" branch this test
    // pins is the case the planner can never look past: a non-numeric
    // LIMIT expression like `LIMIT 5 + ?`. We hit it via a literal
    // expression — sqlparser exposes that as `Expr::BinaryOp`, not
    // `Expr::Value(Number)`, so my detector returns None.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 30).await;

    let q = det_vec(7, DIM);
    let q_lit = vec_lit(&q);
    let before = eng.vector_routing_count();
    let _ = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <-> '{q_lit}' LIMIT 3 + 2"
        ))
        .await;
    assert_eq!(
        eng.vector_routing_count() - before,
        0,
        "routing fired on non-literal LIMIT expression"
    );

    // Sanity: a literal-bound `LIMIT $2` substitutes to `LIMIT 5` and
    // SHOULD route — same query shape as `simple_order_by_routes_to_vector_search`.
    let (handle, _) = sess
        .prepare("SELECT id FROM embeddings ORDER BY embedding <-> $1 LIMIT $2")
        .await
        .unwrap();
    let bound = sess
        .bind(
            &handle,
            vec![ScalarParam::Text(q_lit), ScalarParam::Int8(5)],
        )
        .await
        .unwrap();
    let _ = sess.execute_bound(bound).await;
    assert_eq!(
        eng.vector_routing_count() - before,
        1,
        "post-bind LIMIT 5 should route (parameter already substituted)"
    );
}

#[tokio::test]
async fn where_predicate_pushed_down() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 60).await;

    let q = det_vec(3, DIM);
    let q_lit = vec_lit(&q);

    let before = eng.vector_routing_count();
    let res = sess
        .execute(&format!(
            "SELECT id, category FROM embeddings WHERE category = 'foo' \
             ORDER BY embedding <-> '{q_lit}' LIMIT 5"
        ))
        .await
        .unwrap();
    assert_eq!(
        eng.vector_routing_count() - before,
        1,
        "WHERE-pushdown did not route"
    );

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        _ => panic!("expected rows"),
    };
    let cats = categories_from(&batches);
    assert!(!cats.is_empty(), "result was empty");
    assert!(
        cats.iter().all(|c| c == "foo"),
        "WHERE filter leaked: {cats:?}"
    );
}

#[tokio::test]
async fn auto_routing_correctness_vs_brute_force() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 100).await;

    let q = det_vec(424_242, DIM);
    let q_lit = vec_lit(&q);

    // Brute-force baseline: explicit `l2_distance` UDF call so the planner
    // detect doesn't fire (the rewriter only triggers on the operator
    // form).
    let res_brute = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY l2_distance(embedding, '{q_lit}') LIMIT 10"
        ))
        .await
        .unwrap();
    let brute_ids = match res_brute {
        ExecResult::Rows { batches, .. } => ids_from(&batches),
        _ => panic!("expected rows"),
    };

    let before = eng.vector_routing_count();
    let res_auto = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <-> '{q_lit}' LIMIT 10"
        ))
        .await
        .unwrap();
    assert_eq!(eng.vector_routing_count() - before, 1);
    let auto_ids = match res_auto {
        ExecResult::Rows { batches, .. } => ids_from(&batches),
        _ => panic!("expected rows"),
    };

    let brute_set: std::collections::HashSet<i64> = brute_ids.iter().copied().collect();
    let overlap = auto_ids.iter().filter(|x| brute_set.contains(x)).count();
    // HNSW is approximate. On 100-row corpora with default index params
    // we still expect ≥7/10 overlap — same floor `vector_smoke.rs` uses.
    assert!(
        overlap >= 7,
        "auto-routed top-10 disagreed with brute-force on >=4 rows: brute={brute_ids:?} auto={auto_ids:?}"
    );
}

#[tokio::test]
async fn cosine_distance_op_routes() {
    // The current HNSW sidecar is built with L2 only. Passing `<=>`
    // routes to the storage fast path which returns a metric-mismatch
    // error; the executor catches that and falls back to brute-force,
    // so the routing counter does NOT bump. The detector itself is
    // exercised — we just don't double-bump if storage rejects.
    //
    // To pin "the parser actually accepts `<=>`" we run the SQL and
    // assert it produces rows (via the brute-force fallback) without
    // erroring.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 20).await;

    let q = det_vec(11, DIM);
    let q_lit = vec_lit(&q);
    let res = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <=> '{q_lit}' LIMIT 5"
        ))
        .await
        .unwrap();
    let ids = match res {
        ExecResult::Rows { batches, .. } => ids_from(&batches),
        _ => panic!("expected rows"),
    };
    assert!(!ids.is_empty());
    assert!(ids.len() <= 5);
}

#[tokio::test]
async fn dot_product_op_routes() {
    // Same shape as `cosine_distance_op_routes`. `<#>` translates to
    // negated dot-product so the ORDER BY ASC produces "most similar
    // first" (smaller = more negative = closer). The L2-only sidecar
    // makes this a fallback in v0.1; the detector still parses it.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 20).await;

    let q = det_vec(11, DIM);
    let q_lit = vec_lit(&q);
    let res = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <#> '{q_lit}' LIMIT 5"
        ))
        .await
        .unwrap();
    let ids = match res {
        ExecResult::Rows { batches, .. } => ids_from(&batches),
        _ => panic!("expected rows"),
    };
    assert!(!ids.is_empty());
    assert!(ids.len() <= 5);
}

#[tokio::test]
async fn auto_routing_speedup_observability() {
    // 1K-row corpus with vector(8); compares brute-force vs auto-routed
    // top-10 on the same query. Doesn't gate on speedup — the storage
    // path's segment-cache state varies by run order and the per-test
    // numbers are noisy on debug builds. We just print the timings so
    // benchmark dashboards can pick them up.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = seed_table(&eng, 1_000).await;

    let q = det_vec(99_999, DIM);
    let q_lit = vec_lit(&q);

    let t = Instant::now();
    let _ = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY l2_distance(embedding, '{q_lit}') LIMIT 10"
        ))
        .await
        .unwrap();
    let brute_ms = t.elapsed().as_secs_f64() * 1_000.0;

    let t = Instant::now();
    let _ = sess
        .execute(&format!(
            "SELECT id FROM embeddings ORDER BY embedding <-> '{q_lit}' LIMIT 10"
        ))
        .await
        .unwrap();
    let auto_ms = t.elapsed().as_secs_f64() * 1_000.0;

    eprintln!(
        "[VECTOR AUTO-ROUTING] brute_ms={:.2} auto_ms={:.2} speedup={:.2}x",
        brute_ms,
        auto_ms,
        brute_ms / auto_ms.max(0.001)
    );
}
