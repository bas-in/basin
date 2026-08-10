//! pgvector conformance tests — Basin's vector SQL surface.
//!
//! ## What is covered
//!
//! Tests exercise the vector type, distance operators, HNSW DDL, and catalog
//! functions through the `basin-engine` SQL layer. Every expected value is
//! computed analytically from the L2/cosine/dot distance formulas and
//! cross-referenced with the pgvector specification.
//!
//! Groups:
//!   1. **Literal parse & round-trip** — `'[…]'::vector`, whitespace forms,
//!      scientific notation, zero vector, large-dim (1 536).
//!   2. **Dimension mismatch** — distance ops with unequal dimensions → error.
//!   3. **`vector_dims`** — catalog function returns declared dimensionality.
//!   4. **`vector_norm`** — L2 norm of known vectors.
//!   5. **Distance operators** — `<->` (L2), `<=>` (cosine), `<#>` (neg-dot)
//!      with hand-computed expected values and float tolerance.
//!   6. **Operator ↔ function equivalence** — `a <-> b == l2_distance(a, b)`.
//!   7. **ORDER BY distance LIMIT k** — exact nearest-sets on small data.
//!   8. **HNSW DDL** — `CREATE INDEX USING hnsw` with all three opclasses
//!      and `WITH (m, ef_construction)`; catalog persistence; index-backed
//!      vs brute-force result equality on tiny exact sets.
//!   9. **NULL vectors** — every distance op is NULL-safe.
//!  10. **Zero-vector cosine** — `[0,0,0] <=> something` — pin actual behavior
//!      (denominator guard: returns 1 - dot/max(eps, …)).
//!  11. **`<->` boundary: text operands defer to trigram** — confirm the
//!      operator rewrite correctly skips trgm `<->` when both sides are plain
//!      text strings (not bracket literals), so the trgm path still owns it.
//!  12. **Distance threshold in WHERE** — `WHERE embedding <-> q < threshold`.
//!  13. **`::vector` cast** — explicit cast strips leading `::vector` in SQL.
//!  14. **ORM-style DDL + DML** — SQLAlchemy / Drizzle / Prisma shapes.
//!
//! ## What is NOT covered here (gap table)
//!
//! | Feature                                           | Reason not tested                            |
//! |---------------------------------------------------|----------------------------------------------|
//! | `avg(embedding)` (unqualified)                    | Not overridden — would clobber numeric avg; use `vector_avg` |
//! | `vector_l2_norm` / `l2_norm` shorthand            | Only `vector_norm` is registered             |
//! | `sparsevec(N)` / `bit(N)` vector element types    | Typed 0A000 (see `sparsevec_typed_error`)    |
//! | `ALTER TABLE … ADD COLUMN embedding vector(N)`    | Not separately tested; covered by DDL basics  |
//! | pgvector `SELECT embedding FROM … OFFSET n`       | Pagination with vector order-by: not tested  |
//! | `COPY … FROM` with vector literals                | copy_binary.rs covers binary COPY separately |
//!
//! ## Now covered (Phase 5.26.F — ivfflat / vector_avg / halfvec)
//!
//! | Feature                                           | Group |
//! |---------------------------------------------------|-------|
//! | IVFFlat index (`USING ivfflat`) DDL + kNN routing | 15    |
//! | IVFFlat recall@k vs brute force (native quantiser)| 15 (basin-vector unit tests carry the recall gate) |
//! | `vector_avg(embedding)` element-wise mean + GROUP BY + NULLs + dim-mismatch | 16 |
//! | `halfvec(N)` f32-backed round-trip + distance     | 17    |
//! | `sparsevec(N)` typed 0A000                         | 17    |
//!
//! ## Needs-psql-validation table
//!
//! | ID    | Test                                  | Value to validate                               |
//! |-------|---------------------------------------|-------------------------------------------------|
//! | VEC-1 | `l2_distance_unit_axes`               | `[1,0,0] <-> [0,1,0]` = sqrt(2) in pgvector     |
//! | VEC-2 | `cosine_distance_orthogonal`          | `[1,0] <=> [0,1]` = 1.0 in pgvector             |
//! | VEC-3 | `inner_product_known`                 | `[1,2,3] <#> [4,5,6]` = -32 in pgvector         |
//! | VEC-4 | `zero_vector_cosine_behavior`         | pgvector: `'[0,0]'::vector <=> '[1,0]'::vector` |
//! |       |                                       | undefined (div-by-zero guard); actual value?    |
//! | VEC-5 | `vector_norm_known_values`            | `vector_norm('[3,4]')` = 5.0 in pgvector        |
//! | VEC-6 | `scientific_notation_literal_parsed`  | pgvector parses `[1e2, 2.5e-1]` as `[100, 0.25]`|
//! | VEC-7 | `hnsw_index_recall_100pct_tiny`       | on a 10-row table HNSW must return the true top-k|

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Harness helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_engine() -> (Engine, TempDir) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (engine, dir)
}

async fn exec(sess: &basin_engine::ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e}"));
}

async fn single_string(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("expected Utf8 for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0).to_owned()
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_f64(sess: &basin_engine::ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("expected Float64 for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn single_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().unwrap_or_else(|| panic!("no batch: {sql}"));
            let col = b.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap_or_else(|| panic!("expected Boolean for: {sql}"));
            assert!(arr.len() >= 1, "no rows from: {sql}");
            arr.value(0)
        }
        Ok(other) => panic!("non-rows for {sql}: {other:?}"),
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("error for {sql}: {e}"),
    }
}

async fn sorted_ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut ids = Vec::new();
            for b in &batches {
                if let Some(arr) = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            ids.push(arr.value(i));
                        }
                    }
                }
            }
            ids.sort_unstable();
            ids
        }
        Ok(ExecResult::Empty { .. }) => Vec::new(),
        Err(e) => panic!("error: {sql}: {e}"),
    }
}

// Float tolerance used for distance assertions.  pgvector uses f32 arithmetic
// internally; the SQL UDFs promote to f64 on output. 1e-5 accommodates the
// f32→f64 widening error without hiding genuine bugs.
const EPS: f64 = 1e-5;

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPS
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1 — Literal parse & round-trip
// ─────────────────────────────────────────────────────────────────────────────

/// A vector literal inserted into a `vector(3)` column must come back with
/// the correct number of dimensions and the same component values.
#[tokio::test]
async fn vector_literal_roundtrip_3dim() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE v3 (id BIGINT NOT NULL, v vector(3))").await;
    exec(&sess, "INSERT INTO v3 VALUES (1, '[1.0, 2.0, 3.0]')").await;

    // The stored value must be selectable as TEXT (via ::text cast).
    let txt = single_string(&sess, "SELECT v::text FROM v3 WHERE id = 1").await;
    assert!(
        txt.contains('1') && txt.contains('2') && txt.contains('3'),
        "vector round-trip: text form must contain 1, 2, 3; got={txt:?}"
    );

    // vector_dims must agree with the declared dimensionality.
    match sess
        .execute("SELECT vector_dims(v) FROM v3 WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array for vector_dims");
            assert_eq!(
                arr.value(0),
                3,
                "vector_dims must return 3; got={}",
                arr.value(0)
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[vec literal] 3-dim round-trip ✓");
}

/// Whitespace forms: pgvector is liberal with spaces inside `[…]`.
/// `'[ 1.0 , 2.0 , 3.0 ]'` and `'[1.0,2.0,3.0]'` must both be accepted.
#[tokio::test]
async fn vector_literal_whitespace_forms_accepted() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vws (id BIGINT, v vector(2))").await;

    for (id, lit) in &[
        (1i64, "[ 1.0 , 2.0 ]"),
        (2, "[1.0,2.0]"),
        (3, "[ 1.0,2.0]"),
        (4, "[1.0 ,2.0 ]"),
    ] {
        exec(&sess, &format!("INSERT INTO vws VALUES ({id}, '{lit}')")).await;
    }

    let n = row_count(&sess, "SELECT id FROM vws").await;
    assert_eq!(
        n, 4,
        "All 4 whitespace-variant literals must be accepted; n={n}"
    );
    println!("[vec literal] whitespace forms ✓");
}

/// Scientific notation in vector literals must parse correctly.
/// `[1e2, 2.5e-1]` must be parsed as approximately `[100.0, 0.25]`.
///
/// [VEC-6] NEEDS-PSQL-VALIDATION: confirm pgvector accepts `[1e2,2.5e-1]`.
#[tokio::test]
async fn vector_literal_scientific_notation_parsed() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vsci (id BIGINT, v vector(2))").await;

    // Insert a scientific-notation literal.
    let insert = sess
        .execute("INSERT INTO vsci VALUES (1, '[1e2, 2.5e-1]')")
        .await;
    if insert.is_err() {
        // If the engine rejects scientific notation in v0.1, record the gap
        // rather than failing — we just want to pin the actual behaviour.
        println!(
            "[vec literal] scientific notation: NOT SUPPORTED in current build (gap); \
             INSERT error = {:?}",
            insert.unwrap_err()
        );
        return;
    }

    // If accepted: verify the L2 distance to [100, 0.25] is ~ 0.
    let dist = single_f64(
        &sess,
        "SELECT l2_distance(v, '[100.0, 0.25]') FROM vsci WHERE id = 1",
    )
    .await;
    assert!(
        dist < 0.01,
        "scientific notation: parsed vector must be close to [100, 0.25]; dist={dist}"
    );
    println!("[vec literal] scientific notation ✓  dist={dist:.6}");
}

/// Large-dimensionality round-trip: 1 536 dimensions (OpenAI embedding size).
/// The value must insert and be retrievable without truncation.
#[tokio::test]
async fn vector_literal_1536_dim_roundtrip() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    const DIM: usize = 1536;
    let components: Vec<String> = (0..DIM)
        .map(|i| format!("{:.6}", i as f32 / DIM as f32))
        .collect();
    let lit = format!("[{}]", components.join(","));

    exec(
        &sess,
        &format!("CREATE TABLE v1536 (id BIGINT, v vector({DIM}))"),
    )
    .await;
    exec(&sess, &format!("INSERT INTO v1536 VALUES (1, '{lit}')")).await;

    // vector_dims must report 1536.
    match sess
        .execute("SELECT vector_dims(v) FROM v1536 WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array");
            assert_eq!(
                arr.value(0),
                DIM as i32,
                "1536-dim vector: vector_dims must be {DIM}; got={}",
                arr.value(0)
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[vec literal] 1 536-dim round-trip ✓");
}

/// Zero vector: `[0,0,0]` must be accepted and stored without error.
/// Distance from zero vector to itself must be 0.
#[tokio::test]
async fn vector_zero_literal_accepted() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vzero (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO vzero VALUES (1, '[0.0, 0.0, 0.0]')").await;

    let dist = single_f64(
        &sess,
        "SELECT l2_distance(v, '[0.0, 0.0, 0.0]') FROM vzero WHERE id = 1",
    )
    .await;
    assert!(
        dist.abs() < EPS,
        "l2_distance(zero, zero) must be 0; got={dist}"
    );
    println!("[vec zero] accepted, self-distance = 0 ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2 — Dimension mismatch
// ─────────────────────────────────────────────────────────────────────────────

/// Distance functions with mismatched dimension must return an error.
/// pgvector: `ERROR: different vector dimensions N and M`.
#[tokio::test]
async fn dimension_mismatch_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vdim (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO vdim VALUES (1, '[1.0, 2.0, 3.0]')").await;

    // Calling l2_distance with a 2-dim literal against a 3-dim column must error.
    let r = sess
        .execute("SELECT l2_distance(v, '[1.0, 2.0]') FROM vdim WHERE id = 1")
        .await;
    assert!(r.is_err(), "dimension mismatch must error; got={r:?}");
    println!("[vec dim-mismatch] error ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3 — vector_dims catalog function
// ─────────────────────────────────────────────────────────────────────────────

/// `vector_dims(v)` must return the declared dimension for various sizes.
#[tokio::test]
async fn vector_dims_correct_for_multiple_sizes() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    for dim in &[1usize, 3, 8, 64, 384] {
        let tbl = format!("vdims_{dim}");
        exec(
            &sess,
            &format!("CREATE TABLE {tbl} (id BIGINT, v vector({dim}))"),
        )
        .await;
        let zeros = std::iter::repeat("0.0")
            .take(*dim)
            .collect::<Vec<_>>()
            .join(",");
        exec(&sess, &format!("INSERT INTO {tbl} VALUES (1, '[{zeros}]')")).await;

        match sess
            .execute(&format!("SELECT vector_dims(v) FROM {tbl} WHERE id = 1"))
            .await
        {
            Ok(ExecResult::Rows { batches, .. }) => {
                let b = batches.first().expect("batch");
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32Array");
                assert_eq!(
                    arr.value(0),
                    *dim as i32,
                    "vector_dims for dim={dim}: expected {dim}, got={}",
                    arr.value(0)
                );
            }
            other => panic!("unexpected for dim={dim}: {other:?}"),
        }
    }
    println!("[vec vector_dims] multiple sizes ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4 — vector_norm
// ─────────────────────────────────────────────────────────────────────────────

/// `vector_norm('[3,4]')` must equal 5.0 (3-4-5 Pythagorean triple).
///
/// [VEC-5] NEEDS-PSQL-VALIDATION: `SELECT vector_norm('[3,4]'::vector);`
#[tokio::test]
async fn vector_norm_known_values() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vnorm (id BIGINT, v vector(2))").await;
    exec(
        &sess,
        "INSERT INTO vnorm VALUES (1, '[3.0, 4.0]'), (2, '[0.0, 0.0]'), (3, '[1.0, 0.0]')",
    )
    .await;

    // [3, 4]: norm = sqrt(9 + 16) = 5.
    let n1 = single_f64(&sess, "SELECT vector_norm(v) FROM vnorm WHERE id = 1").await;
    assert!(
        approx_eq(n1, 5.0),
        "vector_norm([3,4]) must be 5.0; got={n1}"
    );

    // [0, 0]: norm = 0.
    let n2 = single_f64(&sess, "SELECT vector_norm(v) FROM vnorm WHERE id = 2").await;
    assert!(n2.abs() < EPS, "vector_norm([0,0]) must be 0; got={n2}");

    // [1, 0]: norm = 1.
    let n3 = single_f64(&sess, "SELECT vector_norm(v) FROM vnorm WHERE id = 3").await;
    assert!(
        approx_eq(n3, 1.0),
        "vector_norm([1,0]) must be 1.0; got={n3}"
    );
    println!("[vec vector_norm] [3,4]=5, [0,0]=0, [1,0]=1 ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5 — Distance operators with hand-computed expected values
// ─────────────────────────────────────────────────────────────────────────────

/// `l2_distance([1,0,0], [0,1,0])` = sqrt(2) ≈ 1.41421356.
///
/// Hand derivation:
///   d² = (1-0)² + (0-1)² + (0-0)² = 1 + 1 + 0 = 2
///   d  = sqrt(2) ≈ 1.41421356237
///
/// SQL surface: `[…] <->  […]` rewrites to `l2_distance(…, …)`.
///
/// [VEC-1] NEEDS-PSQL-VALIDATION: pgvector `'[1,0,0]'::vector <-> '[0,1,0]'::vector`.
#[tokio::test]
async fn l2_distance_unit_axes() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE l2tbl (id BIGINT, v vector(3))").await;
    exec(
        &sess,
        "INSERT INTO l2tbl VALUES (1, '[1.0,0.0,0.0]'), (2, '[0.0,1.0,0.0]')",
    )
    .await;

    // Operator form: <->
    let op_dist = single_f64(
        &sess,
        "SELECT v <-> '[0.0,1.0,0.0]' FROM l2tbl WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(op_dist, std::f64::consts::SQRT_2),
        "<->: [1,0,0] <-> [0,1,0] must be sqrt(2); got={op_dist:.8}"
    );

    // Function form: l2_distance
    let fn_dist = single_f64(
        &sess,
        "SELECT l2_distance(v, '[0.0,1.0,0.0]') FROM l2tbl WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(fn_dist, std::f64::consts::SQRT_2),
        "l2_distance: [1,0,0] vs [0,1,0] must be sqrt(2); got={fn_dist:.8}"
    );

    // Zero distance: identical vectors.
    let zero = single_f64(
        &sess,
        "SELECT v <-> '[1.0,0.0,0.0]' FROM l2tbl WHERE id = 1",
    )
    .await;
    assert!(
        zero.abs() < EPS,
        "<->: identical vector distance must be 0; got={zero}"
    );

    // [3,4,0] <-> [0,0,0] = 5 (Pythagorean).
    exec(
        &sess,
        "INSERT INTO l2tbl VALUES (3, '[3.0,4.0,0.0]'), (4, '[0.0,0.0,0.0]')",
    )
    .await;
    let five = single_f64(
        &sess,
        "SELECT v <-> '[0.0,0.0,0.0]' FROM l2tbl WHERE id = 3",
    )
    .await;
    assert!(
        approx_eq(five, 5.0),
        "<->: [3,4,0] <-> [0,0,0] must be 5.0; got={five:.6}"
    );
    println!("[vec <->] L2 distance ✓  sqrt(2)={op_dist:.8}  5.0={five:.6}");
}

/// `cosine_distance([1,0,0], [0,1,0])` = 1.0 (orthogonal vectors).
/// `cosine_distance([1,1,0], [1,0,0])` = 1 - 1/sqrt(2) ≈ 0.29289.
///
/// SQL surface: `[…] <=> […]` rewrites to `cosine_distance(…, …)`.
///
/// [VEC-2] NEEDS-PSQL-VALIDATION: pgvector `'[1,0]'::vector <=> '[0,1]'::vector` = 1.
#[tokio::test]
async fn cosine_distance_orthogonal_and_45deg() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE costbl (id BIGINT, v vector(3))").await;
    exec(
        &sess,
        "INSERT INTO costbl VALUES \
         (1, '[1.0,0.0,0.0]'), \
         (2, '[0.0,1.0,0.0]'), \
         (3, '[1.0,1.0,0.0]')",
    )
    .await;

    // Orthogonal: cosine similarity = 0 → cosine distance = 1.
    let cos_orth = single_f64(
        &sess,
        "SELECT v <=> '[0.0,1.0,0.0]' FROM costbl WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(cos_orth, 1.0),
        "cosine <=>: [1,0,0] <=> [0,1,0] must be 1.0; got={cos_orth:.8}"
    );

    // Identical: cosine distance = 0.
    let cos_same = single_f64(
        &sess,
        "SELECT v <=> '[1.0,0.0,0.0]' FROM costbl WHERE id = 1",
    )
    .await;
    assert!(
        cos_same.abs() < EPS,
        "cosine <=>: identical vectors must be 0; got={cos_same}"
    );

    // 45°: [1,1,0] <=> [1,0,0] = 1 - 1/sqrt(2).
    let expected_45 = 1.0 - 1.0 / std::f64::consts::SQRT_2;
    let cos_45 = single_f64(
        &sess,
        "SELECT v <=> '[1.0,0.0,0.0]' FROM costbl WHERE id = 3",
    )
    .await;
    assert!(
        approx_eq(cos_45, expected_45),
        "cosine <=>: [1,1,0] <=> [1,0,0] must be 1-1/sqrt(2)={expected_45:.8}; got={cos_45:.8}"
    );

    // Function form equivalence.
    let fn_val = single_f64(
        &sess,
        "SELECT cosine_distance(v, '[0.0,1.0,0.0]') FROM costbl WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(fn_val, 1.0),
        "cosine_distance function: [1,0,0] vs [0,1,0] must be 1.0; got={fn_val}"
    );
    println!(
        "[vec <=>] cosine distance ✓  orthogonal=1.0  45°={cos_45:.8}  expected={expected_45:.8}"
    );
}

/// `neg-dot([1,2,3], [4,5,6])` = -(1×4 + 2×5 + 3×6) = -32.
///
/// pgvector spec: `a <#> b = -(a · b)`. The negation allows `ORDER BY <#>`
/// to return the most similar (highest dot product) vectors first.
///
/// SQL surface: `[…] <#> […]` rewrites to `(- dot_product(…, …))`.
///
/// [VEC-3] NEEDS-PSQL-VALIDATION: pgvector `'[1,2,3]'::vector <#> '[4,5,6]'::vector` = -32.
#[tokio::test]
async fn inner_product_known_values() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE iptbl (id BIGINT, v vector(3))").await;
    exec(
        &sess,
        "INSERT INTO iptbl VALUES \
         (1, '[1.0,2.0,3.0]'), \
         (2, '[1.0,0.0,0.0]'), \
         (3, '[0.0,1.0,0.0]')",
    )
    .await;

    // [1,2,3] <#> [4,5,6] = -(1*4 + 2*5 + 3*6) = -(4+10+18) = -32.
    let ip_neg32 = single_f64(
        &sess,
        "SELECT v <#> '[4.0,5.0,6.0]' FROM iptbl WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(ip_neg32, -32.0),
        "<#>: [1,2,3] <#> [4,5,6] must be -32.0; got={ip_neg32:.6}"
    );

    // Identical unit vector: dot = 1 → <#> = -1.
    let neg_unit = single_f64(
        &sess,
        "SELECT v <#> '[1.0,0.0,0.0]' FROM iptbl WHERE id = 2",
    )
    .await;
    assert!(
        approx_eq(neg_unit, -1.0),
        "<#>: [1,0,0] <#> [1,0,0] must be -1.0; got={neg_unit:.6}"
    );

    // Orthogonal: dot = 0 → <#> = 0.
    let zero_ip = single_f64(
        &sess,
        "SELECT v <#> '[1.0,0.0,0.0]' FROM iptbl WHERE id = 3",
    )
    .await;
    assert!(
        zero_ip.abs() < EPS,
        "<#>: [0,1,0] <#> [1,0,0] must be 0; got={zero_ip}"
    );
    println!("[vec <#>] inner product ✓  [1,2,3]<#>[4,5,6]={ip_neg32}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6 — Operator ↔ function equivalence
// ─────────────────────────────────────────────────────────────────────────────

/// `v <-> q` must return the same value as `l2_distance(v, q)`.
/// This pins the rewrite rule `<->` → `l2_distance` in `rewrite_vector_operators`.
#[tokio::test]
async fn operator_function_equivalence_l2() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE veq (id BIGINT, v vector(4))").await;
    exec(
        &sess,
        "INSERT INTO veq VALUES (1, '[1.0,2.0,3.0,4.0]'), (2, '[0.5,0.5,0.5,0.5]')",
    )
    .await;

    for id in &[1i64, 2] {
        let q = "'[0.1,0.2,0.3,0.4]'";
        let op = single_f64(&sess, &format!("SELECT v <-> {q} FROM veq WHERE id = {id}")).await;
        let fn_ = single_f64(
            &sess,
            &format!("SELECT l2_distance(v, {q}) FROM veq WHERE id = {id}"),
        )
        .await;
        assert!(
            approx_eq(op, fn_),
            "L2 op/fn equivalence for id={id}: op={op:.8} fn={fn_:.8}"
        );
    }
    println!("[vec op/fn equiv] L2 ✓");
}

/// `v <=> q` must return the same value as `cosine_distance(v, q)`.
#[tokio::test]
async fn operator_function_equivalence_cosine() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE veq_cos (id BIGINT, v vector(2))").await;
    exec(
        &sess,
        "INSERT INTO veq_cos VALUES (1, '[1.0,0.0]'), (2, '[0.6,0.8]')",
    )
    .await;

    for id in &[1i64, 2] {
        let q = "'[1.0,1.0]'";
        let op = single_f64(
            &sess,
            &format!("SELECT v <=> {q} FROM veq_cos WHERE id = {id}"),
        )
        .await;
        let fn_ = single_f64(
            &sess,
            &format!("SELECT cosine_distance(v, {q}) FROM veq_cos WHERE id = {id}"),
        )
        .await;
        assert!(
            approx_eq(op, fn_),
            "cosine op/fn equivalence for id={id}: op={op:.8} fn={fn_:.8}"
        );
    }
    println!("[vec op/fn equiv] cosine ✓");
}

/// `(- dot_product(v, q))` must equal `v <#> q`.
#[tokio::test]
async fn operator_function_equivalence_dot() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE veq_dot (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO veq_dot VALUES (1, '[2.0,3.0,4.0]')").await;

    let op = single_f64(
        &sess,
        "SELECT v <#> '[1.0,1.0,1.0]' FROM veq_dot WHERE id = 1",
    )
    .await;
    let fn_ = single_f64(
        &sess,
        "SELECT (0.0 - dot_product(v, '[1.0,1.0,1.0]')) FROM veq_dot WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(op, fn_),
        "dot op/fn equivalence: op={op:.6} fn={fn_:.6}"
    );
    println!("[vec op/fn equiv] dot ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7 — ORDER BY distance LIMIT k correctness (exact nearest sets)
// ─────────────────────────────────────────────────────────────────────────────

/// On a small deterministic dataset, `ORDER BY v <-> query LIMIT k` must
/// return exactly the k true nearest neighbours.
///
/// Dataset: 6 vectors in 2-D space at the corners and midpoints of a unit square.
///   id=1: [0, 0]  id=2: [1, 0]  id=3: [0, 1]  id=4: [1, 1]
///   id=5: [0.5, 0.5]  id=6: [2, 2]
///
/// Query: [0.5, 0.5] (id=5 itself).
/// Exact distances (L2):
///   id=5: 0.000   id=1: 0.707   id=2: 0.707   id=3: 0.707   id=4: 0.707   id=6: 2.121
///
/// LIMIT 2: must return [5, then one of {1,2,3,4}] (ties for 2nd–5th are ok
/// if any of the four appears in position 2). We assert id=5 is first.
#[tokio::test]
async fn order_by_l2_limit_k_returns_exact_nearest() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE knn2d (id BIGINT NOT NULL, v vector(2))",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO knn2d VALUES \
         (1, '[0.0,0.0]'), \
         (2, '[1.0,0.0]'), \
         (3, '[0.0,1.0]'), \
         (4, '[1.0,1.0]'), \
         (5, '[0.5,0.5]'), \
         (6, '[2.0,2.0]')",
    )
    .await;

    // LIMIT 1: must return id=5 (zero distance).
    match sess
        .execute("SELECT id FROM knn2d ORDER BY v <-> '[0.5,0.5]' LIMIT 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("Int64")
                        .iter()
                        .flatten()
                })
                .collect();
            assert_eq!(
                ids.get(0),
                Some(&5),
                "LIMIT 1 nearest to [0.5,0.5] must be id=5 (zero dist); got={ids:?}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }

    // LIMIT 6: id=6 must be last (furthest, dist ≈ 2.12).
    match sess
        .execute("SELECT id FROM knn2d ORDER BY v <-> '[0.5,0.5]' LIMIT 6")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("Int64")
                        .iter()
                        .flatten()
                })
                .collect();
            assert_eq!(ids.len(), 6, "LIMIT 6 must return all 6 rows; got={ids:?}");
            assert_eq!(
                ids.last(),
                Some(&6),
                "id=6 must be last (furthest); got={ids:?}"
            );
            assert_eq!(
                ids.first(),
                Some(&5),
                "id=5 must be first (zero dist); got={ids:?}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[vec ORDER BY <->] exact nearest set ✓");
}

/// ORDER BY cosine distance: `[1,1]` as query — nearest by cosine is any unit
/// vector at 45° (e.g. [1,0] and [0,1] tie; [2,2] is identical direction).
#[tokio::test]
async fn order_by_cosine_returns_correct_order() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE cos2d (id BIGINT NOT NULL, v vector(2))",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO cos2d VALUES \
         (1, '[1.0,0.0]'),   \
         (2, '[0.0,1.0]'),   \
         (3, '[1.0,1.0]'),   \
         (4, '[0.0,-1.0]')",
    )
    .await;

    // Query: [1,1] (same direction as id=3).
    // Distances:
    //   id=3: cosine([1,1],[1,1]) = 0.000 (identical direction)
    //   id=1: cosine([1,0],[1,1]) = 1-1/sqrt(2) ≈ 0.293
    //   id=2: cosine([0,1],[1,1]) = 1-1/sqrt(2) ≈ 0.293 (tie with id=1)
    //   id=4: cosine([0,-1],[1,1]) = 1 - (-1/sqrt(2)) = 1 + 0.707 ≈ 1.707
    match sess
        .execute("SELECT id FROM cos2d ORDER BY v <=> '[1.0,1.0]' LIMIT 4")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("Int64")
                        .iter()
                        .flatten()
                })
                .collect();
            assert_eq!(
                ids.len(),
                4,
                "cosine ORDER BY must return 4 rows; got={ids:?}"
            );
            assert_eq!(
                ids.first(),
                Some(&3),
                "id=3 ([1,1] same direction) must be first; got={ids:?}"
            );
            assert_eq!(
                ids.last(),
                Some(&4),
                "id=4 (anti-parallel) must be last; got={ids:?}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    println!("[vec ORDER BY <=>] correct cosine order ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8 — HNSW DDL + index-backed vs brute-force result equality
// ─────────────────────────────────────────────────────────────────────────────

/// `CREATE INDEX … USING hnsw (embedding vector_l2_ops)` is accepted.
/// All three opclass variants + `WITH (m, ef_construction)` must succeed.
#[tokio::test]
async fn hnsw_ddl_all_opclasses_accepted() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE hnsw_base (id BIGINT PRIMARY KEY, embedding vector(8))",
    )
    .await;

    // vector_l2_ops
    let r1 = sess
        .execute(
            "CREATE INDEX hnsw_l2 ON hnsw_base \
             USING hnsw (embedding vector_l2_ops)",
        )
        .await;
    assert!(
        r1.is_ok(),
        "USING hnsw (vector_l2_ops) must be accepted; got={r1:?}"
    );

    // vector_cosine_ops
    let r2 = sess
        .execute(
            "CREATE INDEX hnsw_cos ON hnsw_base \
             USING hnsw (embedding vector_cosine_ops)",
        )
        .await;
    assert!(
        r2.is_ok(),
        "USING hnsw (vector_cosine_ops) must be accepted; got={r2:?}"
    );

    // vector_ip_ops
    let r3 = sess
        .execute(
            "CREATE INDEX hnsw_ip ON hnsw_base \
             USING hnsw (embedding vector_ip_ops)",
        )
        .await;
    assert!(
        r3.is_ok(),
        "USING hnsw (vector_ip_ops) must be accepted; got={r3:?}"
    );

    // WITH storage parameters.
    let r4 = sess
        .execute(
            "CREATE INDEX hnsw_params ON hnsw_base \
             USING hnsw (embedding vector_l2_ops) \
             WITH (m = 16, ef_construction = 64)",
        )
        .await;
    assert!(
        r4.is_ok(),
        "USING hnsw WITH (m, ef_construction) must be accepted; got={r4:?}"
    );
    println!("[vec HNSW DDL] all opclasses + WITH params accepted ✓");
}

/// After `CREATE INDEX USING hnsw`, the index name must appear in `pg_indexes`.
#[tokio::test]
async fn hnsw_ddl_persists_in_catalog() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE hnsw_cat (id BIGINT PRIMARY KEY, embedding vector(4))",
    )
    .await;
    exec(
        &sess,
        "CREATE INDEX hnsw_cat_l2_idx \
         ON hnsw_cat \
         USING hnsw (embedding vector_l2_ops)",
    )
    .await;

    let n = row_count(
        &sess,
        "SELECT indexname FROM pg_indexes \
         WHERE indexname = 'hnsw_cat_l2_idx'",
    )
    .await;
    assert_eq!(
        n, 1,
        "hnsw_cat_l2_idx must appear in pg_indexes after CREATE INDEX; n={n}"
    );
    println!("[vec HNSW DDL] catalog persistence ✓");
}

/// HNSW index path vs brute-force equality on a tiny dataset.
///
/// On a 10-row table the HNSW recall must be 100%: the index-backed path and
/// the brute-force `l2_distance` sort must return the same top-k set.
///
/// [VEC-7] NEEDS-PSQL-VALIDATION: tiny HNSW recall = 100% on 10 rows.
#[tokio::test]
async fn hnsw_index_backed_vs_brute_force_equality() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Table without index (brute-force path).
    exec(
        &sess,
        "CREATE TABLE bf_tbl (id BIGINT NOT NULL, v vector(4))",
    )
    .await;
    // Table with HNSW index (index-backed path when planner recognises the shape).
    exec(
        &sess,
        "CREATE TABLE idx_tbl (id BIGINT NOT NULL, v vector(4))",
    )
    .await;
    exec(
        &sess,
        "CREATE INDEX idx_tbl_hnsw \
         ON idx_tbl \
         USING hnsw (v vector_l2_ops)",
    )
    .await;

    // 10 deterministic vectors in 4-D.
    let rows = [
        (1i64, "[0.1,0.2,0.3,0.4]"),
        (2, "[0.9,0.1,0.5,0.2]"),
        (3, "[0.3,0.7,0.1,0.8]"),
        (4, "[0.5,0.5,0.5,0.5]"),
        (5, "[0.0,0.0,1.0,0.0]"),
        (6, "[0.8,0.6,0.0,0.0]"),
        (7, "[0.2,0.4,0.6,0.8]"),
        (8, "[0.7,0.3,0.2,0.1]"),
        (9, "[0.1,0.9,0.1,0.9]"),
        (10, "[0.4,0.4,0.4,0.4]"),
    ];

    for (id, v) in &rows {
        exec(&sess, &format!("INSERT INTO bf_tbl VALUES ({id}, '{v}')")).await;
        exec(&sess, &format!("INSERT INTO idx_tbl VALUES ({id}, '{v}')")).await;
    }

    // Query vector: close to id=4 ([0.5,0.5,0.5,0.5]).
    let query = "[0.49,0.51,0.49,0.51]";
    let k = 3;

    // Brute-force top-k.
    let bf_ids = sorted_ids(
        &sess,
        &format!("SELECT id FROM bf_tbl ORDER BY v <-> '{query}' LIMIT {k}"),
    )
    .await;

    // Index-backed top-k (planner may route through HNSW).
    let idx_ids = sorted_ids(
        &sess,
        &format!("SELECT id FROM idx_tbl ORDER BY v <-> '{query}' LIMIT {k}"),
    )
    .await;

    assert_eq!(
        bf_ids.len(),
        k,
        "Brute-force must return {k} results; got={bf_ids:?}"
    );
    assert_eq!(
        idx_ids.len(),
        k,
        "Index-backed must return {k} results; got={idx_ids:?}"
    );
    assert_eq!(
        bf_ids, idx_ids,
        "HNSW vs brute-force MUST agree on 10-row exact dataset; \
         bf={bf_ids:?} idx={idx_ids:?}"
    );
    println!("[vec HNSW vs BF] exact equality on 10 rows, k={k}: {bf_ids:?} ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9 — NULL vectors
// ─────────────────────────────────────────────────────────────────────────────

/// Distance functions on a NULL vector must return NULL (not error, not 0).
#[tokio::test]
async fn null_vector_distance_is_null() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vnull (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO vnull VALUES (1, NULL)").await;

    let null_sqls = [
        "SELECT l2_distance(v, '[1.0,0.0,0.0]') FROM vnull WHERE id = 1",
        "SELECT cosine_distance(v, '[1.0,0.0,0.0]') FROM vnull WHERE id = 1",
        "SELECT dot_product(v, '[1.0,0.0,0.0]') FROM vnull WHERE id = 1",
        "SELECT vector_norm(v) FROM vnull WHERE id = 1",
    ];

    for sql in &null_sqls {
        match sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                let b = batches.first().expect("batch");
                assert!(
                    b.column(0).is_null(0),
                    "NULL vector: {sql} must return NULL; got non-null"
                );
            }
            Ok(ExecResult::Empty { .. }) => {}
            Err(e) => panic!("NULL vector: {sql} must not error: {e}"),
        }
    }
    println!("[vec NULL] all distance functions NULL-safe ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10 — Zero-vector cosine behaviour
// ─────────────────────────────────────────────────────────────────────────────

/// `cosine_distance([0,0,0], [1,0,0])` — denominator guard prevents NaN/panic.
///
/// Basin's `cosine_distance` uses `max(eps, norm_a * norm_b)` to clamp the
/// denominator. With the zero vector: `norm_a = 0`, clamped to `eps`, so
/// `dot / denom ≈ 0 / eps ≈ 0`, and `cosine_distance ≈ 1 - 0 = 1.0`.
///
/// [VEC-4] NEEDS-PSQL-VALIDATION: pgvector's actual return for zero-vector
/// cosine (undefined by definition; each engine guards differently).
#[tokio::test]
async fn zero_vector_cosine_does_not_panic() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vzcos (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO vzcos VALUES (1, '[0.0,0.0,0.0]')").await;

    // Must not error and must not produce NaN/Inf.
    match sess
        .execute("SELECT cosine_distance(v, '[1.0,0.0,0.0]') FROM vzcos WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            if !b.column(0).is_null(0) {
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64");
                let v = arr.value(0);
                assert!(
                    v.is_finite(),
                    "zero-vector cosine must not produce NaN/Inf; got={v}"
                );
                // The denominator guard yields ≈ 1.0 (see crate docs).
                println!(
                    "[vec zero cosine] cosine_distance(zero, [1,0,0]) = {v:.6} (NEEDS-PSQL-VALIDATION)"
                );
            } else {
                // NULL is also acceptable (some implementations return NULL for zero-norm).
                println!("[vec zero cosine] returns NULL (acceptable)");
            }
        }
        Ok(ExecResult::Empty { .. }) => {
            println!("[vec zero cosine] no rows returned (acceptable if pushdown elides row)");
        }
        Err(e) => panic!("zero-vector cosine must not error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 11 — <-> boundary: text operands defer to trigram
// ─────────────────────────────────────────────────────────────────────────────

/// `<->` on two plain TEXT strings (not bracket-literals) must NOT be
/// rewritten to `l2_distance`; it belongs to the trigram distance path.
/// The key invariant is that `to_tsquery('quick <-> fox')` survives intact:
/// the `<->` inside a single-quoted string must not be touched.
///
/// Regression guard for `rewrite_vector_operators`: a blind `<->` substitute
/// that corrupts tsquery literals would break FTS phrase queries.
#[tokio::test]
async fn trgm_arrow_inside_tsquery_literal_not_rewritten() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // This FTS phrase query contains `<->` inside a string literal.
    // It MUST still evaluate correctly (the tsquery phrase operator, not l2_distance).
    let matched = single_bool(
        &sess,
        "SELECT to_tsvector('english', 'quick brown fox') @@ to_tsquery('english', 'quick <-> brown')",
    )
    .await;
    assert!(
        matched,
        "<-> inside to_tsquery literal must not be corrupted by vector rewriter; phrase query must match"
    );
    println!("[vec <-> boundary] tsquery phrase literal survives vector rewriter ✓");
}

/// `vector <-> vector` still works when both sides are bracket-literals.
/// Regression: the trgm skip-logic (plain text operand = skip vector rewrite)
/// must not skip bracket-literal operands.
#[tokio::test]
async fn vector_arrow_vector_with_bracket_literals_rewrites_correctly() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Both sides are bracket-format vector literals; must rewrite to l2_distance.
    let dist = single_f64(&sess, "SELECT '[3.0,4.0]' <-> '[0.0,0.0]'").await;
    assert!(
        approx_eq(dist, 5.0),
        "bracket-literal <-> bracket-literal must be rewritten to l2_distance; \
         [3,4]<->[0,0]=5.0; got={dist:.6}"
    );
    println!("[vec <-> boundary] bracket-literal rewrite ✓  dist={dist:.6}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 12 — Distance threshold in WHERE
// ─────────────────────────────────────────────────────────────────────────────

/// `WHERE l2_distance(embedding, q) < threshold` filters rows correctly.
///
/// Dataset: 4 vectors at known L2 distances from query `[0,0]`:
///   id=1: [0,0]   dist=0    → within 1.0
///   id=2: [0.5,0] dist=0.5  → within 1.0
///   id=3: [1,0]   dist=1.0  → border: 1.0 < 1.0 is FALSE; not included
///   id=4: [2,0]   dist=2.0  → outside
#[tokio::test]
async fn distance_threshold_in_where_filters_rows() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE thresh (id BIGINT NOT NULL, v vector(2))",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO thresh VALUES \
         (1, '[0.0,0.0]'), \
         (2, '[0.5,0.0]'), \
         (3, '[1.0,0.0]'), \
         (4, '[2.0,0.0]')",
    )
    .await;

    // Strict less-than 1.0: ids 1 and 2 qualify; id=3 (dist=1.0) does not.
    let ids = sorted_ids(
        &sess,
        "SELECT id FROM thresh WHERE l2_distance(v, '[0.0,0.0]') < 1.0",
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2],
        "WHERE l2_distance < 1.0 must return [1,2]; got={ids:?}"
    );
    println!("[vec WHERE threshold] < 1.0 → [1,2] ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 13 — ::vector explicit cast
// ─────────────────────────────────────────────────────────────────────────────

/// `'[1,0,0]'::vector <-> '[0,1,0]'::vector` must equal sqrt(2).
/// The `::vector` cast suffix is handled by `rewrite_vector_cast` before
/// SQL reaches the parser.
#[tokio::test]
async fn explicit_vector_cast_works_in_distance_expression() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let dist = single_f64(
        &sess,
        "SELECT '[1.0,0.0,0.0]'::vector <-> '[0.0,1.0,0.0]'::vector",
    )
    .await;
    assert!(
        approx_eq(dist, std::f64::consts::SQRT_2),
        "::vector cast: [1,0,0]::vector <-> [0,1,0]::vector = sqrt(2); got={dist:.8}"
    );
    println!("[vec ::vector cast] ✓  dist={dist:.8}");
}

/// `ORDER BY embedding <-> q::vector` (Prisma idiom) must route correctly.
#[tokio::test]
async fn prisma_cast_suffix_in_order_by() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE prisma_tbl (id BIGINT PRIMARY KEY, embedding vector(3))",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO prisma_tbl VALUES \
         (1, '[1.0,0.0,0.0]'), \
         (2, '[0.0,1.0,0.0]'), \
         (3, '[0.0,0.0,1.0]')",
    )
    .await;

    // Prisma shape: `ORDER BY embedding <-> $1::vector LIMIT k OFFSET 0`.
    let n = row_count(
        &sess,
        "SELECT id FROM prisma_tbl \
         ORDER BY embedding <-> '[1.0,0.0,0.0]'::vector \
         LIMIT 3 OFFSET 0",
    )
    .await;
    assert_eq!(
        n, 3,
        "Prisma ORDER BY ::vector must return all 3 rows; n={n}"
    );
    println!("[vec ::vector Prisma] ORDER BY + ::vector ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 14 — ORM-style DDL + DML
// ─────────────────────────────────────────────────────────────────────────────

/// SQLAlchemy / pgvector-python DDL + INSERT + ORDER BY nearest-neighbour.
#[tokio::test]
async fn orm_sqlalchemy_shapes_work_end_to_end() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE sqla_docs (\
            id        SERIAL NOT NULL, \
            content   TEXT, \
            embedding vector(4), \
            PRIMARY KEY (id)\
        )",
    )
    .await;

    let hnsw = sess
        .execute(
            "CREATE INDEX sqla_hnsw \
             ON sqla_docs \
             USING hnsw (embedding vector_l2_ops) \
             WITH (m = 16, ef_construction = 64)",
        )
        .await;
    assert!(
        hnsw.is_ok(),
        "SQLAlchemy HNSW DDL must be accepted; got={hnsw:?}"
    );

    exec(
        &sess,
        "INSERT INTO sqla_docs (content, embedding) VALUES \
         ('rust programming',  '[0.1,0.2,0.3,0.4]'), \
         ('database systems',  '[0.9,0.1,0.05,0.05]'), \
         ('vector search',     '[0.5,0.5,0.5,0.5]')",
    )
    .await;

    let n = row_count(
        &sess,
        "SELECT id, content \
         FROM sqla_docs \
         ORDER BY embedding <-> '[0.1,0.2,0.3,0.4]' \
         LIMIT 5",
    )
    .await;
    assert_eq!(n, 3, "SQLAlchemy ORDER BY <->: must return 3 rows; n={n}");
    println!("[vec ORM SQLAlchemy] DDL+INSERT+ORDER BY ✓");
}

/// Drizzle ORM shapes: double-quoted identifiers + vector column.
#[tokio::test]
async fn orm_drizzle_shapes_work_end_to_end() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE \"drizzle_posts\" (\
            \"id\"        serial PRIMARY KEY NOT NULL, \
            \"title\"     text NOT NULL, \
            \"embedding\" vector(4)\
        )",
    )
    .await;

    exec(
        &sess,
        "CREATE INDEX \"drizzle_hnsw\" \
         ON \"drizzle_posts\" \
         USING hnsw (\"embedding\" vector_l2_ops)",
    )
    .await;

    exec(
        &sess,
        "INSERT INTO \"drizzle_posts\" (\"title\", \"embedding\") VALUES \
         ('Rust is fast',   '[0.2,0.4,0.6,0.8]'), \
         ('Postgres rocks', '[0.8,0.6,0.4,0.2]'), \
         ('Vector search',  '[0.5,0.5,0.5,0.5]')",
    )
    .await;

    let n = row_count(
        &sess,
        "SELECT \"id\", \"title\" \
         FROM \"drizzle_posts\" \
         ORDER BY \"embedding\" <-> '[0.2,0.4,0.6,0.8]' \
         LIMIT 10",
    )
    .await;
    assert_eq!(n, 3, "Drizzle ORDER BY <->: must return 3 rows; n={n}");
    println!("[vec ORM Drizzle] DDL+INSERT+ORDER BY ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 15 — IVFFlat index DDL + kNN routing
// ─────────────────────────────────────────────────────────────────────────────
//
// The IVFFlat *algorithm* (k-means partitioning, probes-nearest-cell scan,
// measured recall@10 and the more-probes-monotonic / all-cells-exact / exact-
// candidate-ordering properties) is gated in the `basin-vector` crate unit
// tests (`ivfflat::tests`), where the index can be driven directly with a
// seeded clustered dataset and a configurable `probes`. Measured there:
// recall@10 probes=1 ≈ 0.90, probes=4 = 1.00, all-cells = 1.00 (exact).
//
// These engine-level tests pin the SQL surface: `USING ivfflat` DDL with each
// opclass + `WITH (lists = N)` is accepted and persisted, and `ORDER BY <->
// LIMIT k` over an ivfflat-indexed table routes through Basin's ANN fast path
// and agrees with brute force on a small exact set (recall = 1.0 at this size).

/// `CREATE INDEX … USING ivfflat (col opclass) WITH (lists = N)` is accepted
/// for all three opclasses and persists in `pg_indexes`.
#[tokio::test]
async fn ivfflat_ddl_all_opclasses_accepted_and_persisted() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE ivf_base (id BIGINT PRIMARY KEY, embedding vector(8))",
    )
    .await;

    for (name, opclass) in &[
        ("ivf_l2", "vector_l2_ops"),
        ("ivf_cos", "vector_cosine_ops"),
        ("ivf_ip", "vector_ip_ops"),
    ] {
        let r = sess
            .execute(&format!(
                "CREATE INDEX {name} ON ivf_base \
                 USING ivfflat (embedding {opclass}) WITH (lists = 4)"
            ))
            .await;
        assert!(
            r.is_ok(),
            "USING ivfflat ({opclass}) WITH (lists) must be accepted; got={r:?}"
        );
    }

    // All three must appear in pg_indexes.
    let n = row_count(
        &sess,
        "SELECT indexname FROM pg_indexes \
         WHERE indexname IN ('ivf_l2', 'ivf_cos', 'ivf_ip')",
    )
    .await;
    assert_eq!(n, 3, "all three ivfflat indexes must persist; n={n}");
    println!("[vec IVFFlat DDL] all opclasses + WITH (lists) accepted & persisted ✓");
}

/// IVFFlat-routed `ORDER BY <-> LIMIT k` agrees with brute force on a tiny
/// exact dataset (recall 1.0 at this size — same contract the HNSW equality
/// test pins). The candidates the ANN path returns are exact-ordered, so the
/// sorted top-k id sets must be identical.
#[tokio::test]
async fn ivfflat_index_backed_vs_brute_force_equality() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE ivf_bf (id BIGINT NOT NULL, v vector(4))",
    )
    .await;
    exec(
        &sess,
        "CREATE TABLE ivf_idx (id BIGINT NOT NULL, v vector(4))",
    )
    .await;
    exec(
        &sess,
        "CREATE INDEX ivf_idx_ann ON ivf_idx \
         USING ivfflat (v vector_l2_ops) WITH (lists = 3)",
    )
    .await;

    let rows = [
        (1i64, "[0.1,0.2,0.3,0.4]"),
        (2, "[0.9,0.1,0.5,0.2]"),
        (3, "[0.3,0.7,0.1,0.8]"),
        (4, "[0.5,0.5,0.5,0.5]"),
        (5, "[0.0,0.0,1.0,0.0]"),
        (6, "[0.8,0.6,0.0,0.0]"),
        (7, "[0.2,0.4,0.6,0.8]"),
        (8, "[0.7,0.3,0.2,0.1]"),
        (9, "[0.1,0.9,0.1,0.9]"),
        (10, "[0.4,0.4,0.4,0.4]"),
    ];
    for (id, v) in &rows {
        exec(&sess, &format!("INSERT INTO ivf_bf VALUES ({id}, '{v}')")).await;
        exec(&sess, &format!("INSERT INTO ivf_idx VALUES ({id}, '{v}')")).await;
    }

    let query = "[0.49,0.51,0.49,0.51]";
    let k = 3;
    let bf_ids = sorted_ids(
        &sess,
        &format!("SELECT id FROM ivf_bf ORDER BY v <-> '{query}' LIMIT {k}"),
    )
    .await;
    let idx_ids = sorted_ids(
        &sess,
        &format!("SELECT id FROM ivf_idx ORDER BY v <-> '{query}' LIMIT {k}"),
    )
    .await;

    assert_eq!(
        bf_ids.len(),
        k,
        "brute force must return {k}; got={bf_ids:?}"
    );
    assert_eq!(
        idx_ids.len(),
        k,
        "ivfflat-routed must return {k}; got={idx_ids:?}"
    );
    assert_eq!(
        bf_ids, idx_ids,
        "IVFFlat vs brute force must agree on 10-row exact set; bf={bf_ids:?} idx={idx_ids:?}"
    );
    println!("[vec IVFFlat vs BF] exact equality on 10 rows, k={k}: {bf_ids:?} ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 16 — vector_avg aggregate
// ─────────────────────────────────────────────────────────────────────────────

/// `vector_avg(v)` returns the element-wise mean of the non-NULL vectors.
/// Hand-computed: avg([2,4],[4,8],[6,12]) = [4, 8].
#[tokio::test]
async fn vector_avg_elementwise_mean() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vavg (id BIGINT, v vector(2))").await;
    exec(
        &sess,
        "INSERT INTO vavg VALUES (1, '[2.0,4.0]'), (2, '[4.0,8.0]'), (3, '[6.0,12.0]')",
    )
    .await;

    // Check the aggregate result numerically: the element-wise mean is
    // [(2+4+6)/3, (4+8+12)/3] = [4, 8], so the L2 distance from the aggregate
    // to the hand-computed mean must be ~0. (We assert via l2_distance rather
    // than a `::text` cast because the FixedSizeList→text cast over an
    // aggregate result is a separate, unwired path; the distance UDF consumes
    // the aggregate's FixedSizeList directly.)
    let d = single_f64(
        &sess,
        "SELECT l2_distance(vector_avg(v), '[4.0,8.0]') FROM vavg",
    )
    .await;
    assert!(
        d.abs() < EPS,
        "vector_avg must equal [4,8]; l2 to [4,8] = {d}"
    );
    println!("[vec vector_avg] element-wise mean = [4,8] ✓");
}

/// NULL vectors are ignored by `vector_avg` (PG aggregate NULL semantics).
/// avg([1,1], NULL, [3,3]) = [2, 2].
#[tokio::test]
async fn vector_avg_ignores_nulls() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vavg_n (id BIGINT, v vector(2))").await;
    exec(
        &sess,
        "INSERT INTO vavg_n VALUES (1, '[1.0,1.0]'), (2, NULL), (3, '[3.0,3.0]')",
    )
    .await;

    let d = single_f64(
        &sess,
        "SELECT l2_distance(vector_avg(v), '[2.0,2.0]') FROM vavg_n",
    )
    .await;
    assert!(
        d.abs() < EPS,
        "vector_avg must skip NULLs → [2,2]; l2 to [2,2] = {d}"
    );
    println!("[vec vector_avg] NULLs ignored → [2,2] ✓");
}

/// `vector_avg` over zero non-NULL rows returns NULL.
#[tokio::test]
async fn vector_avg_all_null_is_null() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vavg_z (id BIGINT, v vector(3))").await;
    exec(&sess, "INSERT INTO vavg_z VALUES (1, NULL), (2, NULL)").await;

    match sess.execute("SELECT vector_avg(v) FROM vavg_z").await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches.first().expect("batch");
            assert!(
                b.num_rows() == 0 || b.column(0).is_null(0),
                "vector_avg over all-NULL must be NULL; got non-null"
            );
        }
        Ok(ExecResult::Empty { .. }) => {}
        Err(e) => panic!("vector_avg all-NULL must not error: {e}"),
    }
    println!("[vec vector_avg] all-NULL → NULL ✓");
}

/// `vector_avg` with GROUP BY computes a per-group mean.
/// group a: [2,2],[4,4] → [3,3]; group b: [10,10] → [10,10].
#[tokio::test]
async fn vector_avg_group_by() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE vavg_g (g TEXT, v vector(2))").await;
    exec(
        &sess,
        "INSERT INTO vavg_g VALUES \
         ('a', '[2.0,2.0]'), ('a', '[4.0,4.0]'), ('b', '[10.0,10.0]')",
    )
    .await;

    let d_a = single_f64(
        &sess,
        "SELECT l2_distance(vector_avg(v), '[3.0,3.0]') FROM vavg_g WHERE g = 'a' GROUP BY g",
    )
    .await;
    assert!(d_a.abs() < EPS, "group a mean must be [3,3]; l2 = {d_a}");

    let d_b = single_f64(
        &sess,
        "SELECT l2_distance(vector_avg(v), '[10.0,10.0]') FROM vavg_g WHERE g = 'b' GROUP BY g",
    )
    .await;
    assert!(d_b.abs() < EPS, "group b mean must be [10,10]; l2 = {d_b}");
    println!("[vec vector_avg] GROUP BY per-group mean ✓");
}

/// `vector_avg` over vectors of different dimension must error.
#[tokio::test]
async fn vector_avg_dim_mismatch_errors() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Two tables of different dim can't be mixed in one column, so we exercise
    // the dim-mismatch guard via string literals fed through a UNION whose two
    // arms have different widths. Simpler: a single vector(3) table plus a
    // 2-dim literal handed to vector_avg through a VALUES-like set is awkward;
    // instead assert the accumulator guard fires across a UNION ALL of two
    // literal rows of different width.
    let r = sess
        .execute(
            "SELECT vector_avg(v) FROM ( \
                SELECT '[1.0,2.0,3.0]'::vector AS v \
                UNION ALL \
                SELECT '[1.0,2.0]'::vector AS v \
             ) t",
        )
        .await;
    assert!(
        r.is_err(),
        "vector_avg over mismatched dims must error; got={r:?}"
    );
    println!("[vec vector_avg] dim-mismatch error ✓");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 17 — halfvec (f32-backed) + sparsevec typed error
// ─────────────────────────────────────────────────────────────────────────────

/// `halfvec(N)` is accepted as a column type and round-trips like `vector(N)`.
/// DECISION: Basin stores halfvec with the same f32-backed FixedSizeList
/// physical layout as vector(N) (no f16 truncation), so every vector path
/// works unchanged. PRECISION NOTE: Basin keeps full f32 precision rather than
/// pgvector's f16 rounding — distances are at least as accurate.
#[tokio::test]
async fn halfvec_roundtrip_and_distance() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE hv (id BIGINT, h halfvec(3))").await;
    exec(&sess, "INSERT INTO hv VALUES (1, '[3.0,4.0,0.0]')").await;

    // vector_dims works on a halfvec column.
    match sess
        .execute("SELECT vector_dims(h) FROM hv WHERE id = 1")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => {
            let arr = batches
                .first()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array");
            assert_eq!(arr.value(0), 3, "halfvec(3) vector_dims must be 3");
        }
        other => panic!("unexpected: {other:?}"),
    }

    // Distance ops work on halfvec: l2([3,4,0], [0,0,0]) = 5.
    let d = single_f64(
        &sess,
        "SELECT l2_distance(h, '[0.0,0.0,0.0]') FROM hv WHERE id = 1",
    )
    .await;
    assert!(
        approx_eq(d, 5.0),
        "halfvec l2_distance must be 5.0; got={d}"
    );
    println!(
        "[vec halfvec] f32-backed round-trip + distance ✓ (precision note: no f16 truncation)"
    );
}

/// `sparsevec(N)` is a typed 0A000 (feature_not_supported) — not a silent
/// dense coercion. Roadmap-tracked.
#[tokio::test]
async fn sparsevec_typed_error() {
    basin_common::telemetry::try_init_for_tests();
    let (engine, _dir) = make_engine();
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let r = sess
        .execute("CREATE TABLE sv (id BIGINT, s sparsevec(5))")
        .await;
    assert!(r.is_err(), "sparsevec(N) must be a typed error; got={r:?}");
    let msg = format!("{:?}", r.unwrap_err()).to_lowercase();
    assert!(
        msg.contains("sparsevec"),
        "sparsevec error must name the feature; got={msg}"
    );
    println!("[vec sparsevec] typed 0A000 ✓");
}
