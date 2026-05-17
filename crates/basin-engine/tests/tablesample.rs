//! `TABLESAMPLE` end-to-end acceptance (BUG #134).
//!
//! Before the fix, `SELECT ... FROM t TABLESAMPLE BERNOULLI(p)` / `SYSTEM(p)`
//! silently stripped the clause and returned **every** row — statistically
//! wrong results for any sampling / approx-aggregate query. These tests pin
//! the corrected behaviour:
//!
//! * `BERNOULLI(p)`  — independent per-row Bernoulli(p/100) trial. Over a
//!   10_000-row table `BERNOULLI(10)` returns ~1000 rows; we assert a wide
//!   tolerance band so the statistical test never flakes.
//! * `BERNOULLI(0)`  — zero rows. `BERNOULLI(100)` — all rows.
//! * `SYSTEM(p)`     — block-level (one decision per ~8192-row record
//!   batch). We assert plausible subset + the 0 / 100 edges.
//! * `REPEATABLE(s)` — a fresh session re-running the same seeded sample on
//!   the same data yields an identical row set.
//! * Out-of-range `p` is a hard error (PG parity).
//!
//! The bands are deliberately loose: the contract is "≈p% in expectation",
//! not an exact count, so we only fail on a gross deviation that would
//! indicate the sample isn't being taken (e.g. all-or-nothing).

use std::sync::Arc;

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

/// Bulk-load `n` rows `(id, val)` into a fresh `samp` table on `sess`.
async fn seed_table(sess: &ProjectSession, n: usize) {
    sess.execute("CREATE TABLE samp (id BIGINT, val BIGINT)")
        .await
        .unwrap();
    // Batch the INSERT into chunks so the VALUES list stays a sane size.
    let chunk = 1000;
    let mut i = 0;
    while i < n {
        let end = (i + chunk).min(n);
        let mut sql = String::from("INSERT INTO samp (id, val) VALUES ");
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            sql.push_str(&format!("({}, {})", j, j * 7));
        }
        sess.execute(&sql).await.unwrap();
        i = end;
    }
}

/// Run `sql` and return the total number of rows in the result set.
async fn count_rows(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Run `sql` and collect the `id` column values (used for REPEATABLE set
/// equality).
async fn collect_ids(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    use arrow_array::Int64Array;
    let batches = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut ids = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for k in 0..arr.len() {
            ids.push(arr.value(k));
        }
    }
    ids.sort_unstable();
    ids
}

const N: usize = 10_000;

#[tokio::test]
async fn bernoulli_10pct_is_roughly_a_tenth() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, N).await;

    let got = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE BERNOULLI(10)").await;
    // Expectation 1000; ~3.0 sigma is ≈ ±90, so a [700, 1300] band is
    // astronomically unlikely to flake while still catching "returns all
    // 10000" (the original bug) and "returns nothing".
    assert!(
        (700..=1300).contains(&got),
        "BERNOULLI(10) over {N} rows should be ≈1000, got {got}"
    );
}

#[tokio::test]
async fn bernoulli_zero_returns_no_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, N).await;

    let got = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE BERNOULLI(0)").await;
    assert_eq!(got, 0, "BERNOULLI(0) must return zero rows");
}

#[tokio::test]
async fn bernoulli_hundred_returns_all_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, N).await;

    let got = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE BERNOULLI(100)").await;
    assert_eq!(got, N, "BERNOULLI(100) must return all {N} rows");
}

#[tokio::test]
async fn bernoulli_repeatable_is_deterministic_across_fresh_sessions() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    // Two independent fresh sessions over identical data: the seeded sample
    // must produce the same row set (REPEATABLE's contract; basin's
    // documented guarantee is per-fresh-session).
    let s1 = open(&eng).await;
    seed_table(&s1, N).await;
    let q = "SELECT id FROM samp TABLESAMPLE BERNOULLI(15) REPEATABLE(424242)";
    let a = collect_ids(&s1, q).await;

    let s2 = open(&eng).await;
    seed_table(&s2, N).await;
    let b = collect_ids(&s2, q).await;

    assert_eq!(a, b, "REPEATABLE(seed) must yield an identical row set");
    // And it must actually be a *sample* (≈1500), not all rows / none.
    assert!(
        (1100..=1900).contains(&a.len()),
        "REPEATABLE BERNOULLI(15) should be ≈1500, got {}",
        a.len()
    );
}

#[tokio::test]
async fn system_returns_a_plausible_subset_and_edges() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, N).await;

    // SYSTEM is block-granular (per record batch), so for a small number of
    // batches the count is coarse — we only assert it's a valid subset and
    // the hard edges are exact.
    let s = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE SYSTEM(50)").await;
    assert!(s <= N, "SYSTEM(50) cannot exceed the table ({s} > {N})");

    let zero = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE SYSTEM(0)").await;
    assert_eq!(zero, 0, "SYSTEM(0) must return zero rows");

    let all = count_rows(&sess, "SELECT * FROM samp TABLESAMPLE SYSTEM(100)").await;
    assert_eq!(all, N, "SYSTEM(100) must return all {N} rows");
}

#[tokio::test]
async fn system_repeatable_is_deterministic_across_fresh_sessions() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);

    let s1 = open(&eng).await;
    seed_table(&s1, N).await;
    let q = "SELECT id FROM samp TABLESAMPLE SYSTEM(40) REPEATABLE(7)";
    let a = collect_ids(&s1, q).await;

    let s2 = open(&eng).await;
    seed_table(&s2, N).await;
    let b = collect_ids(&s2, q).await;

    assert_eq!(a, b, "SYSTEM REPEATABLE(seed) must be deterministic");
    assert!(a.len() <= N, "SYSTEM sample cannot exceed the table");
}

#[tokio::test]
async fn out_of_range_percentage_is_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, 10).await;

    for bad in ["SELECT * FROM samp TABLESAMPLE BERNOULLI(150)",
                "SELECT * FROM samp TABLESAMPLE SYSTEM(-5)"] {
        let r = sess.execute(bad).await;
        assert!(
            r.is_err(),
            "out-of-range percentage must error, query: {bad}"
        );
    }
}

#[tokio::test]
async fn tablesample_composes_with_outer_where() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed_table(&sess, N).await;

    // Outer predicate (`id < 5000`) must still apply on top of the sample;
    // result is bounded by both the sample (~10% of 10000) and the filter.
    let got = count_rows(
        &sess,
        "SELECT * FROM samp TABLESAMPLE BERNOULLI(10) WHERE id < 5000",
    )
    .await;
    assert!(
        got <= 1300,
        "sample+filter must not exceed the sample band, got {got}"
    );
}
