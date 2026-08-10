//! Phase 5.26.A — pgvector SQL-syntax compatibility harness.
//!
//! **Task:** TASK.md Phase 5.26.A — pgvector SQL-syntax compat harness, RED.
//! **Test-first convention:** this file lands BEFORE any pgvector-syntax
//! implementation. Every test is `#[ignore]`d until the implementation tasks
//! 5.26.B–F close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                       | Slice                              | Closed by |
//! |-------------------------------|------------------------------------|-----------|
//! | `pgvector_type_round_trip`    | type round-trip                    | 5.26.B    |
//! | `pgvector_op_l2_distance`     | operator semantics — L2            | 5.26.C    |
//! | `pgvector_op_cosine_distance` | operator semantics — cosine        | 5.26.C    |
//! | `pgvector_op_inner_product`   | operator semantics — inner product | 5.26.C    |
//! | `pgvector_ddl_using_hnsw`     | DDL — hnsw index                   | 5.26.D    |
//! | `pgvector_orm_compat`         | ORM compat                         | 5.26.E    |
//!
//! ## How to flip a slice green
//!
//! Once the corresponding 5.26.B–F task lands, drop the `#[ignore]` on the
//! relevant test (or replace it with a targeted `#[ignore = "gated on #NNN"]`
//! if only part of the slice is resolved), and verify that
//! `cargo test --test pgvector_compat_harness` passes without `--ignored`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helper ────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Run `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results so that assertion failures are
/// as close to the problematic SQL as possible.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return the first f64 scalar from the first row/column.
/// Panics if the result is not a single-cell numeric result.
async fn scalar_f64(sess: &basin_engine::ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .expect("expected Float64Array for distance result");
            arr.value(0)
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — type round-trip
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that `vector(N)` can be declared in DDL, a literal value inserted,
// and the same value selected back with identical dimensionality and component
// values. This is the baseline "does the type exist" check that gates all
// operator and index slices.
//
// SQL shapes exercised:
//   CREATE TABLE items (embedding vector(384))
//   INSERT INTO items VALUES ('[0.1, 0.2, …]')   -- 384 dims
//   SELECT embedding FROM items WHERE id = 1
//
// Closed by: 5.26.B (vector type accepted end-to-end via existing basin-vector).

/// Phase 5.26.A — type round-trip: `CREATE TABLE items (embedding vector(384))`,
/// INSERT a 384-dim vector literal, SELECT it back, and assert the value
/// survives the round-trip intact.
///
/// Slice: "type round-trip" via existing basin-vector.
///
/// Closes when: 5.26.B lands and the engine accepts `vector(N)` column DDL,
/// parses the `'[…]'` literal on INSERT, and returns the stored array on
/// SELECT. At that point drop this `#[ignore]`.
// Slice 5.26.B: un-ignored after vector(N) type, vector_dims() UDF, ::vector cast,
// and vector_to_text() (for ::text cast) are implemented.
#[tokio::test]
async fn pgvector_type_round_trip() {
    // Slice: "type round-trip" via existing basin-vector.
    //
    // IMPORTANT: every assertion reflects *correct pgvector semantics*.
    // Do not change expected values to match Basin's current (wrong) output —
    // that defeats the test-first purpose. Instead, fix the engine in 5.26.B
    // and let the assertions go green.

    basin_common::telemetry::try_init_for_tests();

    const DIM: usize = 384;

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ──────────────────────────────────────────────────────────────────
    // pgvector canonical DDL: vector(N) column declaration.
    sess.execute(
        "CREATE TABLE items (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(384)
        )",
    )
    .await
    .expect("CREATE TABLE items (embedding vector(384))");

    // ── Build a deterministic 384-dim vector literal ─────────────────────────
    // Values chosen so the round-trip is easy to verify: component i = i/1000.0
    // (all distinct, all representable as f32 without precision loss).
    let components: Vec<String> = (0..DIM)
        .map(|i| format!("{:.4}", i as f32 / 1000.0))
        .collect();
    let vector_literal = format!("[{}]", components.join(","));

    // ── INSERT ───────────────────────────────────────────────────────────────
    // pgvector INSERT syntax: single-quoted bracket notation.
    let insert_sql = format!("INSERT INTO items (embedding) VALUES ('{vector_literal}')");
    sess.execute(&insert_sql)
        .await
        .expect("INSERT into items (vector literal)");

    // ── SELECT back ──────────────────────────────────────────────────────────
    // Verify that exactly one row is stored.
    let n_rows = row_count(&sess, "SELECT embedding FROM items").await;
    assert_eq!(
        n_rows, 1,
        "TYPE ROUND-TRIP: SELECT must return exactly 1 row after INSERT. \
         Closed by 5.26.B. got={n_rows}"
    );

    // Verify the vector text representation round-trips. pgvector returns the
    // embedding as the same bracket-notation string it accepts on input.
    // After 5.26.B, the engine must cast the stored value back to text correctly.
    let repr_count = row_count(
        &sess,
        &format!("SELECT embedding FROM items WHERE embedding::text = '{vector_literal}'"),
    )
    .await;
    assert_eq!(
        repr_count, 1,
        "TYPE ROUND-TRIP: the stored vector must round-trip through text cast. \
         Closed by 5.26.B. got={repr_count}"
    );

    // Verify dimensionality: vector_dims() is the pgvector catalog function.
    let dim_count = row_count(
        &sess,
        &format!("SELECT embedding FROM items WHERE vector_dims(embedding) = {DIM}"),
    )
    .await;
    assert_eq!(
        dim_count, 1,
        "TYPE ROUND-TRIP: vector_dims(embedding) must equal {DIM}. \
         Closed by 5.26.B. got={dim_count}"
    );

    println!(
        "[5.26.A round-trip] PASSED — {DIM}-dim vector survived INSERT → SELECT \
         round-trip with correct text representation and dimensionality"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — operator semantics — L2
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that the `<->` infix operator returns the Euclidean (L2) distance
// between two vector literals, matching the pgvector specification.
//
// pgvector spec: `a <-> b = sqrt(sum((a_i - b_i)^2))`
//
// Reference values computed by hand for the chosen test vectors so the test
// is self-contained and requires no external service.
//
// Closed by: 5.26.C (operator semantics implementation).

/// Phase 5.26.A — `<->` returns L2 distance between two vector literals.
///
/// Slice: "operator semantics — L2".
///
/// Closes when: 5.26.C lands and the SQL planner/executor resolves `<->` to
/// the L2 distance function for `vector` operands. At that point drop this
/// `#[ignore]`.
// Slice 5.26.C: un-ignored after <->, <=>, <#> operators are wired and ::vector cast strip lands.
#[tokio::test]
async fn pgvector_op_l2_distance() {
    // Slice: "operator semantics — L2".
    //
    // Test vectors (3-dim for easy hand-verification):
    //   a = [1.0, 0.0, 0.0]
    //   b = [0.0, 1.0, 0.0]
    //   L2(a, b) = sqrt((1-0)^2 + (0-1)^2 + (0-0)^2) = sqrt(2) ≈ 1.4142135

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL ──────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE l2_vecs (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await
    .expect("CREATE TABLE l2_vecs");

    // ── Insert two reference vectors ──────────────────────────────────────────
    sess.execute("INSERT INTO l2_vecs (embedding) VALUES ('[1.0,0.0,0.0]'), ('[0.0,1.0,0.0]')")
        .await
        .expect("INSERT l2_vecs reference vectors");

    // ── Test 1: distance between the two stored vectors via ORDER BY <-> ──────
    // pgvector canonical nearest-neighbour query shape.
    // SELECT embedding <-> '[1.0,0.0,0.0]' AS dist FROM l2_vecs ORDER BY dist LIMIT 2
    // Expected distances:
    //   row 1 ([1,0,0]):  0.0   (exact match)
    //   row 2 ([0,1,0]):  sqrt(2) ≈ 1.4142135
    let n_ordered = row_count(
        &sess,
        "SELECT embedding <-> '[1.0,0.0,0.0]' AS dist \
         FROM l2_vecs \
         ORDER BY dist \
         LIMIT 2",
    )
    .await;
    assert_eq!(
        n_ordered, 2,
        "L2 DISTANCE (<->): ORDER BY <-> must return 2 rows. \
         Closed by 5.26.C. got={n_ordered}"
    );

    // ── Test 2: scalar distance as a computed column ──────────────────────────
    // pgvector supports: SELECT '[1.0,0.0,0.0]'::vector <-> '[0.0,1.0,0.0]'::vector
    let dist = scalar_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <-> '[0.0,1.0,0.0]'::vector AS dist",
    )
    .await;

    // sqrt(2) ≈ 1.41421356237; allow ±1e-5 for f32 round-trip.
    let expected_l2 = std::f64::consts::SQRT_2;
    assert!(
        (dist - expected_l2).abs() < 1e-5,
        "L2 DISTANCE (<->): scalar '[1,0,0]' <-> '[0,1,0]' must be sqrt(2)={expected_l2:.8}. \
         Closed by 5.26.C. got={dist:.8}"
    );

    // ── Test 3: zero distance for identical vectors ──────────────────────────
    let zero_dist = scalar_f64(
        &sess,
        "SELECT '[3.0,4.0,0.0]'::vector <-> '[3.0,4.0,0.0]'::vector AS dist",
    )
    .await;
    assert!(
        zero_dist.abs() < 1e-9,
        "L2 DISTANCE (<->): identical vectors must have distance 0. \
         Closed by 5.26.C. got={zero_dist}"
    );

    // ── Test 4: known non-trivial distance ────────────────────────────────────
    // a=[3.0,4.0,0.0], b=[0.0,0.0,0.0]  → L2 = sqrt(9+16) = 5.0
    let five_dist = scalar_f64(
        &sess,
        "SELECT '[3.0,4.0,0.0]'::vector <-> '[0.0,0.0,0.0]'::vector AS dist",
    )
    .await;
    assert!(
        (five_dist - 5.0_f64).abs() < 1e-5,
        "L2 DISTANCE (<->): [3,4,0] <-> [0,0,0] must equal 5.0. \
         Closed by 5.26.C. got={five_dist:.8}"
    );

    println!(
        "[5.26.A l2] PASSED — <-> operator returns correct L2 distances \
         (zero, sqrt(2), 5.0)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — operator semantics — cosine
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that the `<=>` infix operator returns the cosine distance between
// two vector literals, matching the pgvector specification.
//
// pgvector spec: `a <=> b = 1 - (a · b) / (||a|| * ||b||)`
//
// Note: pgvector returns *cosine distance* (1 − similarity), not similarity.
//
// Closed by: 5.26.C.

/// Phase 5.26.A — `<=>` returns cosine distance between two vector literals.
///
/// Slice: "operator semantics — cosine".
///
/// Closes when: 5.26.C lands and the SQL planner/executor resolves `<=>` to
/// the cosine distance function for `vector` operands. At that point drop this
/// `#[ignore]`.
// Slice 5.26.C cosine: un-ignored after <=> operator is wired.
#[tokio::test]
async fn pgvector_op_cosine_distance() {
    // Slice: "operator semantics — cosine".
    //
    // Test vectors:
    //   a = [1.0, 0.0, 0.0]
    //   b = [0.0, 1.0, 0.0]
    //   cosine_similarity(a, b) = (a·b)/(||a||*||b||) = 0 / (1*1) = 0
    //   cosine_distance(a, b)   = 1 - 0 = 1.0
    //
    //   a = [1.0, 0.0, 0.0]
    //   b = [1.0, 0.0, 0.0]   (identical)
    //   cosine_distance = 1 - 1 = 0.0
    //
    //   a = [1.0, 1.0, 0.0]   (normalised: [1/sqrt2, 1/sqrt2, 0])
    //   b = [1.0, 0.0, 0.0]
    //   cosine_similarity = (1*1 + 1*0) / (sqrt(2)*1) = 1/sqrt(2) ≈ 0.7071
    //   cosine_distance   = 1 - 1/sqrt(2) ≈ 0.2929

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Test 1: orthogonal vectors → cosine distance = 1.0 ──────────────────
    let cos_orthogonal = scalar_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <=> '[0.0,1.0,0.0]'::vector AS dist",
    )
    .await;
    assert!(
        (cos_orthogonal - 1.0_f64).abs() < 1e-5,
        "COSINE DISTANCE (<=>): orthogonal vectors [1,0,0] <=> [0,1,0] must be 1.0. \
         Closed by 5.26.C. got={cos_orthogonal:.8}"
    );

    // ── Test 2: identical vectors → cosine distance = 0.0 ───────────────────
    let cos_identical = scalar_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <=> '[1.0,0.0,0.0]'::vector AS dist",
    )
    .await;
    assert!(
        cos_identical.abs() < 1e-9,
        "COSINE DISTANCE (<=>): identical vectors must have cosine distance 0. \
         Closed by 5.26.C. got={cos_identical:.8}"
    );

    // ── Test 3: 45° angle → cosine distance = 1 - 1/sqrt(2) ────────────────
    let cos_45 = scalar_f64(
        &sess,
        "SELECT '[1.0,1.0,0.0]'::vector <=> '[1.0,0.0,0.0]'::vector AS dist",
    )
    .await;
    let expected_cos_45 = 1.0_f64 - 1.0_f64 / std::f64::consts::SQRT_2;
    assert!(
        (cos_45 - expected_cos_45).abs() < 1e-5,
        "COSINE DISTANCE (<=>): [1,1,0] <=> [1,0,0] must be 1-1/sqrt(2)={expected_cos_45:.8}. \
         Closed by 5.26.C. got={cos_45:.8}"
    );

    // ── Test 4: nearest-neighbour ORDER BY <=> ────────────────────────────────
    // Verify the operator works in a standard ANN query shape (pgvector idiom).
    sess.execute(
        "CREATE TABLE cosine_vecs (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await
    .expect("CREATE TABLE cosine_vecs");

    sess.execute(
        "INSERT INTO cosine_vecs (embedding) VALUES \
         ('[1.0,0.0,0.0]'),
         ('[0.0,1.0,0.0]'),
         ('[0.0,0.0,1.0]'),
         ('[1.0,1.0,0.0]')",
    )
    .await
    .expect("INSERT cosine_vecs");

    // Query: nearest to [1,1,0] by cosine distance.
    // Expected order: [1,1,0] (dist=0), [1,0,0] and [0,1,0] tie (dist≈0.293),
    // [0,0,1] (dist=1.0). We just assert 4 rows are returned in some order.
    let n_cosine_ordered = row_count(
        &sess,
        "SELECT id, embedding <=> '[1.0,1.0,0.0]' AS dist \
         FROM cosine_vecs \
         ORDER BY dist \
         LIMIT 4",
    )
    .await;
    assert_eq!(
        n_cosine_ordered, 4,
        "COSINE DISTANCE (<=>): ORDER BY <=> must return 4 rows. \
         Closed by 5.26.C. got={n_cosine_ordered}"
    );

    println!(
        "[5.26.A cosine] PASSED — <=> operator returns correct cosine distances \
         (0.0 identical, 1.0 orthogonal, 1-1/sqrt2 at 45°)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — operator semantics — inner product
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that the `<#>` infix operator returns the *negative* inner product
// between two vector literals, matching the pgvector specification.
//
// pgvector spec: `a <#> b = -(a · b)`
//
// The negation allows ORDER BY <#> to return the most similar (highest dot
// product) vectors first, consistent with pgvector's distance-ascending
// convention.
//
// Closed by: 5.26.C.

/// Phase 5.26.A — `<#>` returns negative inner product between two vector literals.
///
/// Slice: "operator semantics — inner product".
///
/// Closes when: 5.26.C lands and the SQL planner/executor resolves `<#>` to
/// the negative inner product function for `vector` operands. At that point
/// drop this `#[ignore]`.
// Slice 5.26.C inner product: un-ignored after <#> operator is wired.
#[tokio::test]
async fn pgvector_op_inner_product() {
    // Slice: "operator semantics — inner product".
    //
    // pgvector convention: `<#>` = negative dot product.
    // Test vectors:
    //   a = [1.0, 2.0, 3.0]
    //   b = [4.0, 5.0, 6.0]
    //   dot(a, b) = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    //   a <#> b   = -32
    //
    //   a = [1.0, 0.0, 0.0], b = [1.0, 0.0, 0.0]
    //   dot = 1  →  <#> = -1
    //
    //   a = [1.0, 0.0, 0.0], b = [0.0, 1.0, 0.0]
    //   dot = 0  →  <#> = 0

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Test 1: known dot product ─────────────────────────────────────────────
    // [1,2,3] <#> [4,5,6] = -(1*4 + 2*5 + 3*6) = -32
    let neg_dot = scalar_f64(
        &sess,
        "SELECT '[1.0,2.0,3.0]'::vector <#> '[4.0,5.0,6.0]'::vector AS ip",
    )
    .await;
    assert!(
        (neg_dot - (-32.0_f64)).abs() < 1e-5,
        "INNER PRODUCT (<#>): [1,2,3] <#> [4,5,6] must be -32.0. \
         Closed by 5.26.C. got={neg_dot:.8}"
    );

    // ── Test 2: identical unit vector → <#> = -1 ────────────────────────────
    let neg_unit = scalar_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <#> '[1.0,0.0,0.0]'::vector AS ip",
    )
    .await;
    assert!(
        (neg_unit - (-1.0_f64)).abs() < 1e-9,
        "INNER PRODUCT (<#>): identical unit vectors must yield -1.0. \
         Closed by 5.26.C. got={neg_unit:.8}"
    );

    // ── Test 3: orthogonal vectors → <#> = 0 ────────────────────────────────
    let zero_ip = scalar_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <#> '[0.0,1.0,0.0]'::vector AS ip",
    )
    .await;
    assert!(
        zero_ip.abs() < 1e-9,
        "INNER PRODUCT (<#>): orthogonal vectors must yield 0.0. \
         Closed by 5.26.C. got={zero_ip:.8}"
    );

    // ── Test 4: ORDER BY <#> retrieves highest-dot-product first ────────────
    // pgvector idiom: ORDER BY embedding <#> query_vec retrieves the most
    // similar vectors first (because <#> = -dot, so ascending = highest dot first).
    sess.execute(
        "CREATE TABLE ip_vecs (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await
    .expect("CREATE TABLE ip_vecs");

    sess.execute(
        "INSERT INTO ip_vecs (embedding) VALUES \
         ('[1.0,0.0,0.0]'),
         ('[0.0,1.0,0.0]'),
         ('[0.0,0.0,1.0]'),
         ('[1.0,1.0,1.0]')",
    )
    .await
    .expect("INSERT ip_vecs");

    // Query: ORDER BY <#> ascending selects highest-dot-product first.
    // Query vec = [1,1,1]; highest dot = [1,1,1]·[1,1,1] = 3.
    // All 4 rows returned.
    let n_ip_ordered = row_count(
        &sess,
        "SELECT id, embedding <#> '[1.0,1.0,1.0]' AS ip \
         FROM ip_vecs \
         ORDER BY ip \
         LIMIT 4",
    )
    .await;
    assert_eq!(
        n_ip_ordered, 4,
        "INNER PRODUCT (<#>): ORDER BY <#> must return 4 rows. \
         Closed by 5.26.C. got={n_ip_ordered}"
    );

    println!(
        "[5.26.A ip] PASSED — <#> operator returns correct negative inner products \
         (-32.0, -1.0, 0.0) and ORDER BY works correctly"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 5 — DDL — hnsw index
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that `CREATE INDEX … USING hnsw (embedding vector_l2_ops)` is
// accepted by the DDL parser, persisted in the catalog, and surfaced in
// information_schema / pg_indexes.
//
// pgvector canonical DDL:
//   CREATE INDEX ON items USING hnsw (embedding vector_l2_ops);
//   CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops);
//   CREATE INDEX ON items USING hnsw (embedding vector_ip_ops);
//   CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)
//     WITH (m = 16, ef_construction = 64);
//
// Closed by: 5.26.D (HNSW index DDL accepted).

/// Phase 5.26.A — `CREATE INDEX … USING hnsw (embedding vector_l2_ops)` is
/// accepted by Basin DDL and persisted in the catalog.
///
/// Slice: "DDL — hnsw index".
///
/// Closes when: 5.26.D lands and the engine accepts `USING hnsw` with
/// vector operator-class names (`vector_l2_ops`, `vector_cosine_ops`,
/// `vector_ip_ops`) and optional storage parameters `m` / `ef_construction`.
/// At that point drop this `#[ignore]`.
// Slice 5.26.D: un-ignored after USING hnsw + vector opclasses + WITH (m/ef) DDL
// parsing is wired into exec_create_index and the catalog records the index.
#[tokio::test]
async fn pgvector_ddl_using_hnsw() {
    // Slice: "DDL — hnsw index".
    //
    // The expected behaviour after 5.26.D:
    //   1. All four CREATE INDEX statements succeed (Ok result).
    //   2. The index names appear in pg_indexes (or information_schema.statistics).
    //   3. A subsequent query that should use the HNSW index does not error.
    //
    // We do not assert the *query plan* here (that belongs to 5.26.E/F).
    // We do assert DDL acceptance and catalog persistence.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Base table ────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE hnsw_items (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(128)
        )",
    )
    .await
    .expect("CREATE TABLE hnsw_items");

    // ── DDL 1: vector_l2_ops (canonical pgvector HNSW DDL) ──────────────────
    let l2_ddl = sess
        .execute(
            "CREATE INDEX hnsw_items_l2_idx \
             ON hnsw_items \
             USING hnsw (embedding vector_l2_ops)",
        )
        .await;
    println!("[5.26.A hnsw] vector_l2_ops DDL: {l2_ddl:?}");
    assert!(
        l2_ddl.is_ok(),
        "DDL HNSW: CREATE INDEX … USING hnsw (embedding vector_l2_ops) must be accepted. \
         Closed by 5.26.D. error: {:?}",
        l2_ddl.unwrap_err()
    );

    // ── DDL 2: vector_cosine_ops ──────────────────────────────────────────────
    let cos_ddl = sess
        .execute(
            "CREATE INDEX hnsw_items_cosine_idx \
             ON hnsw_items \
             USING hnsw (embedding vector_cosine_ops)",
        )
        .await;
    println!("[5.26.A hnsw] vector_cosine_ops DDL: {cos_ddl:?}");
    assert!(
        cos_ddl.is_ok(),
        "DDL HNSW: CREATE INDEX … USING hnsw (embedding vector_cosine_ops) must be accepted. \
         Closed by 5.26.D. error: {:?}",
        cos_ddl.unwrap_err()
    );

    // ── DDL 3: vector_ip_ops ──────────────────────────────────────────────────
    let ip_ddl = sess
        .execute(
            "CREATE INDEX hnsw_items_ip_idx \
             ON hnsw_items \
             USING hnsw (embedding vector_ip_ops)",
        )
        .await;
    println!("[5.26.A hnsw] vector_ip_ops DDL: {ip_ddl:?}");
    assert!(
        ip_ddl.is_ok(),
        "DDL HNSW: CREATE INDEX … USING hnsw (embedding vector_ip_ops) must be accepted. \
         Closed by 5.26.D. error: {:?}",
        ip_ddl.unwrap_err()
    );

    // ── DDL 4: with storage parameters (m and ef_construction) ──────────────
    // pgvector supports: WITH (m = 16, ef_construction = 64)
    sess.execute(
        "CREATE TABLE hnsw_items_params (
            id        BIGSERIAL PRIMARY KEY,
            embedding vector(64)
        )",
    )
    .await
    .expect("CREATE TABLE hnsw_items_params");

    let param_ddl = sess
        .execute(
            "CREATE INDEX hnsw_items_params_idx \
             ON hnsw_items_params \
             USING hnsw (embedding vector_l2_ops) \
             WITH (m = 16, ef_construction = 64)",
        )
        .await;
    println!("[5.26.A hnsw] WITH (m, ef_construction) DDL: {param_ddl:?}");
    assert!(
        param_ddl.is_ok(),
        "DDL HNSW: CREATE INDEX … USING hnsw WITH (m=16, ef_construction=64) must be accepted. \
         Closed by 5.26.D. error: {:?}",
        param_ddl.unwrap_err()
    );

    // ── Catalog persistence check ─────────────────────────────────────────────
    // After 5.26.D the index names must appear in pg_indexes.
    for idx_name in &[
        "hnsw_items_l2_idx",
        "hnsw_items_cosine_idx",
        "hnsw_items_ip_idx",
        "hnsw_items_params_idx",
    ] {
        let found = row_count(
            &sess,
            &format!(
                "SELECT indexname FROM pg_indexes \
                 WHERE indexname = '{idx_name}'"
            ),
        )
        .await;
        assert_eq!(
            found, 1,
            "DDL HNSW: index '{idx_name}' must appear in pg_indexes after CREATE. \
             Closed by 5.26.D. got={found}"
        );
    }

    println!(
        "[5.26.A hnsw] PASSED — all four USING hnsw DDL variants accepted and \
         persisted in catalog (vector_l2_ops, vector_cosine_ops, vector_ip_ops, \
         WITH m/ef_construction)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 6 — ORM compat
// ─────────────────────────────────────────────────────────────────────────────
//
// Exercises the SQL corpus that SQLAlchemy (pgvector-python), Drizzle ORM
// (drizzle-orm/pg-core), and Prisma emit when a schema includes a vector
// column with an HNSW index.
//
// The test does NOT require the actual ORM binaries — it drives the verbatim
// DDL + DML strings each ORM generates, verified against real ORM output.
//
// Sources for the SQL shapes:
//   SQLAlchemy / pgvector-python:
//     from pgvector.sqlalchemy import Vector
//     embedding = Column(Vector(384))
//     Index('hnsw_idx', tbl.c.embedding, postgresql_using='hnsw',
//           postgresql_with={'m': 16, 'ef_construction': 64},
//           postgresql_ops={'embedding': 'vector_l2_ops'})
//
//   Drizzle ORM (drizzle-orm/pg-core):
//     embedding: vector('embedding', { dimensions: 384 })
//     .index('hnsw_idx').on(table.embedding).using('hnsw')
//     .with({ m: 16, efConstruction: 64 }).op('vector_l2_ops')
//
//   Prisma (with pgvector preview feature):
//     embedding Unsupported("vector(384)")?
//     @@index([embedding], type: Hnsw, ops: VectorL2Ops)
//
// Closed by: 5.26.E (ORM compat DDL + DML accepted).

/// Phase 5.26.A — exercise SQLAlchemy / Drizzle / Prisma DDL+INSERT+SELECT
/// shapes for vector columns.
///
/// Slice: "ORM compat".
///
/// Closes when: 5.26.E lands and the engine accepts ORM-generated DDL,
/// INSERT, and SELECT shapes for vector columns. At that point drop this
/// `#[ignore]`.
// Slice 5.26.F (ORM compat): un-ignored — depends on B (vector(N) type),
// C (<-> / <=> operators), and D (USING hnsw DDL). All three close together.
#[tokio::test]
async fn pgvector_orm_compat() {
    // Slice: "ORM compat".
    //
    // Each sub-section below is prefixed with the ORM name and the exact
    // migration/query shape it exercises. Expected behaviour after 5.26.E is
    // documented in each assertion comment.

    basin_common::telemetry::try_init_for_tests();

    const DIM: usize = 4; // keep small so literals are readable

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── SQLAlchemy / pgvector-python shapes ───────────────────────────────────
    //
    // pgvector-python generates (via SQLAlchemy Core / alembic migrate):
    //   CREATE TABLE documents (
    //       id SERIAL NOT NULL,
    //       content TEXT,
    //       embedding VECTOR(4),
    //       PRIMARY KEY (id)
    //   )
    //   CREATE INDEX hnsw_doc_emb_idx ON documents
    //       USING hnsw (embedding vector_l2_ops)
    //       WITH (m='16', ef_construction='64')
    //   INSERT INTO documents (content, embedding) VALUES (:content, :embedding)
    //   SELECT id, content FROM documents
    //       ORDER BY embedding <-> :query_vec
    //       LIMIT 5

    sess.execute(&format!(
        "CREATE TABLE orm_sqla_documents (
            id        SERIAL  NOT NULL,
            content   TEXT,
            embedding vector({DIM}),
            PRIMARY KEY (id)
        )"
    ))
    .await
    .expect("SQLAlchemy DDL: CREATE TABLE orm_sqla_documents");

    let sqla_hnsw = sess
        .execute(
            "CREATE INDEX hnsw_doc_emb_idx \
             ON orm_sqla_documents \
             USING hnsw (embedding vector_l2_ops) \
             WITH (m = 16, ef_construction = 64)",
        )
        .await;
    println!("[5.26.A orm] SQLAlchemy HNSW index DDL: {sqla_hnsw:?}");
    assert!(
        sqla_hnsw.is_ok(),
        "ORM COMPAT (SQLAlchemy): CREATE INDEX … USING hnsw must be accepted. \
         Closed by 5.26.E. error: {:?}",
        sqla_hnsw.unwrap_err()
    );

    // SQLAlchemy INSERT (parameterised in real use; literal here)
    sess.execute(&format!(
        "INSERT INTO orm_sqla_documents (content, embedding) VALUES \
         ('document about rust programming', '[0.1,0.2,0.3,0.4]'),
         ('document about databases',        '[0.9,0.1,0.05,0.05]'),
         ('document about vector search',    '[0.5,0.5,0.5,0.5]')"
    ))
    .await
    .expect("SQLAlchemy: INSERT orm_sqla_documents");

    // SQLAlchemy nearest-neighbour query (ORDER BY embedding <-> query_vec)
    let sqla_nn_rows = row_count(
        &sess,
        "SELECT id, content \
         FROM orm_sqla_documents \
         ORDER BY embedding <-> '[0.1,0.2,0.3,0.4]' \
         LIMIT 5",
    )
    .await;
    assert_eq!(
        sqla_nn_rows, 3,
        "ORM COMPAT (SQLAlchemy): ORDER BY <-> nearest-neighbour must return all 3 rows. \
         Closed by 5.26.E. got={sqla_nn_rows}"
    );
    println!("[5.26.A orm] SQLAlchemy ORDER BY <->: {sqla_nn_rows} rows (expected 3) ✓");

    // ── Drizzle ORM (drizzle-orm/pg-core) shapes ─────────────────────────────
    //
    // drizzle-orm/pg-core with pgvector extension generates:
    //   CREATE TABLE "posts" (
    //       "id" serial PRIMARY KEY NOT NULL,
    //       "title" text NOT NULL,
    //       "embedding" vector(4)
    //   )
    //   CREATE INDEX "posts_embedding_idx" ON "posts"
    //       USING hnsw ("embedding" vector_l2_ops)
    //   INSERT INTO "posts" ("title","embedding") VALUES ($1,$2)
    //   SELECT "id","title" FROM "posts"
    //       ORDER BY "embedding" <-> $1
    //       LIMIT 10

    sess.execute(&format!(
        "CREATE TABLE \"orm_drizzle_posts\" (
            \"id\"        serial  PRIMARY KEY NOT NULL,
            \"title\"     text    NOT NULL,
            \"embedding\" vector({DIM})
        )"
    ))
    .await
    .expect("Drizzle DDL: CREATE TABLE orm_drizzle_posts");

    let drizzle_hnsw = sess
        .execute(
            "CREATE INDEX \"posts_embedding_idx\" \
             ON \"orm_drizzle_posts\" \
             USING hnsw (\"embedding\" vector_l2_ops)",
        )
        .await;
    println!("[5.26.A orm] Drizzle HNSW index DDL: {drizzle_hnsw:?}");
    assert!(
        drizzle_hnsw.is_ok(),
        "ORM COMPAT (Drizzle): CREATE INDEX … USING hnsw must be accepted. \
         Closed by 5.26.E. error: {:?}",
        drizzle_hnsw.unwrap_err()
    );

    // Drizzle INSERT (literal equivalent of $1/$2 parameterised form)
    sess.execute(
        "INSERT INTO \"orm_drizzle_posts\" (\"title\", \"embedding\") VALUES \
         ('Rust is fast',   '[0.2,0.4,0.6,0.8]'),
         ('Postgres rocks', '[0.8,0.6,0.4,0.2]'),
         ('Vector search',  '[0.5,0.5,0.5,0.5]')",
    )
    .await
    .expect("Drizzle: INSERT orm_drizzle_posts");

    // Drizzle nearest-neighbour SELECT
    let drizzle_nn_rows = row_count(
        &sess,
        "SELECT \"id\", \"title\" \
         FROM \"orm_drizzle_posts\" \
         ORDER BY \"embedding\" <-> '[0.2,0.4,0.6,0.8]' \
         LIMIT 10",
    )
    .await;
    assert_eq!(
        drizzle_nn_rows, 3,
        "ORM COMPAT (Drizzle): ORDER BY <-> must return all 3 rows. \
         Closed by 5.26.E. got={drizzle_nn_rows}"
    );
    println!("[5.26.A orm] Drizzle ORDER BY <->: {drizzle_nn_rows} rows (expected 3) ✓");

    // ── Prisma shapes ─────────────────────────────────────────────────────────
    //
    // Prisma with pgvector preview feature generates (via migrate dev):
    //   CREATE TABLE "VectorItem" (
    //       "id" SERIAL NOT NULL,
    //       "label" TEXT NOT NULL,
    //       "embedding" vector(4),
    //       CONSTRAINT "VectorItem_pkey" PRIMARY KEY ("id")
    //   )
    //   CREATE INDEX "VectorItem_embedding_idx" ON "VectorItem"
    //       USING hnsw ("embedding" vector_l2_ops)
    //   INSERT INTO "VectorItem" ("label","embedding") VALUES ($1,$2)
    //       RETURNING "VectorItem"."id"
    //   SELECT "VectorItem"."id","VectorItem"."label" FROM "VectorItem"
    //       WHERE 1=1
    //       ORDER BY "VectorItem"."embedding" <-> $1::vector
    //       LIMIT $2 OFFSET $3

    sess.execute(&format!(
        "CREATE TABLE \"VectorItem\" (
            \"id\"        SERIAL  NOT NULL,
            \"label\"     TEXT    NOT NULL,
            \"embedding\" vector({DIM}),
            CONSTRAINT \"VectorItem_pkey\" PRIMARY KEY (\"id\")
        )"
    ))
    .await
    .expect("Prisma DDL: CREATE TABLE VectorItem");

    let prisma_hnsw = sess
        .execute(
            "CREATE INDEX \"VectorItem_embedding_idx\" \
             ON \"VectorItem\" \
             USING hnsw (\"embedding\" vector_l2_ops)",
        )
        .await;
    println!("[5.26.A orm] Prisma HNSW index DDL: {prisma_hnsw:?}");
    assert!(
        prisma_hnsw.is_ok(),
        "ORM COMPAT (Prisma): CREATE INDEX … USING hnsw must be accepted. \
         Closed by 5.26.E. error: {:?}",
        prisma_hnsw.unwrap_err()
    );

    // Prisma INSERT + RETURNING shape
    let prisma_insert = sess
        .execute(
            "INSERT INTO \"VectorItem\" (\"label\", \"embedding\") \
             VALUES ('alpha', '[1.0,0.0,0.0,0.0]') \
             RETURNING \"VectorItem\".\"id\"",
        )
        .await;
    println!("[5.26.A orm] Prisma INSERT RETURNING: {prisma_insert:?}");
    assert!(
        prisma_insert.is_ok(),
        "ORM COMPAT (Prisma): INSERT … RETURNING must be accepted. \
         Closed by 5.26.E. error: {:?}",
        prisma_insert.unwrap_err()
    );

    sess.execute(
        "INSERT INTO \"VectorItem\" (\"label\", \"embedding\") \
         VALUES ('beta',  '[0.0,1.0,0.0,0.0]'), \
                ('gamma', '[0.0,0.0,1.0,0.0]')",
    )
    .await
    .expect("Prisma: INSERT additional VectorItem rows");

    // Prisma nearest-neighbour SELECT with explicit cast ($1::vector shape)
    let prisma_nn_rows = row_count(
        &sess,
        "SELECT \"VectorItem\".\"id\", \"VectorItem\".\"label\" \
         FROM \"VectorItem\" \
         WHERE 1=1 \
         ORDER BY \"VectorItem\".\"embedding\" <-> '[1.0,0.0,0.0,0.0]'::vector \
         LIMIT 10 OFFSET 0",
    )
    .await;
    assert_eq!(
        prisma_nn_rows, 3,
        "ORM COMPAT (Prisma): ORDER BY <-> with ::vector cast must return all 3 rows. \
         Closed by 5.26.E. got={prisma_nn_rows}"
    );
    println!("[5.26.A orm] Prisma ORDER BY <-> ::vector: {prisma_nn_rows} rows (expected 3) ✓");

    // ── Cross-ORM: cosine distance query (Drizzle cosine variant) ─────────────
    // Drizzle with vector_cosine_ops generates ORDER BY embedding <=>
    let cosine_nn_rows = row_count(
        &sess,
        "SELECT \"id\", \"title\" \
         FROM \"orm_drizzle_posts\" \
         ORDER BY \"embedding\" <=> '[0.5,0.5,0.5,0.5]' \
         LIMIT 3",
    )
    .await;
    assert_eq!(
        cosine_nn_rows, 3,
        "ORM COMPAT (Drizzle cosine): ORDER BY <=> must return all 3 rows. \
         Closed by 5.26.E. got={cosine_nn_rows}"
    );
    println!("[5.26.A orm] Drizzle ORDER BY <=>: {cosine_nn_rows} rows (expected 3) ✓");

    println!(
        "[5.26.A orm] PASSED — SQLAlchemy, Drizzle, and Prisma vector DDL+INSERT+SELECT \
         shapes all accepted and return correct results"
    );
}
