//! Keyset-pagination fast-path correctness + timing probe.
//!
//! Exercises the `execute_simple_select` keyset variant added to
//! `fast_select.rs`: a `SELECT … FROM t WHERE k > $1 ORDER BY k ASC LIMIT n`
//! (and the DESC + `k < $1` mirror) where `k` is the table's single effective
//! cluster column.  The fast path pushes a PER-FILE LIMIT into the storage
//! reader (each cold file is internally sorted ASC on the cluster column, so
//! its first `n` matching rows are a superset of its global top-`n`
//! contribution) and then merges the tiny per-file runs via the existing
//! concat + sort_to_indices + take.
//!
//! Every correctness test compares the fast-path result against an independent
//! ground truth computed in-test from the full inserted data set, so a wrong
//! merge / wrong per-file cap / dropped backfill row is caught regardless of
//! which physical path (fast or DataFusion fallback) actually served the query.
//!
//! Rows are inserted in several separate `INSERT` statements with INTERLEAVED
//! id ranges so the on-disk files each span the full PK range (the round-robin
//! striping the keyset path must merge across), not neat disjoint segments.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
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
        page_cache: Some(basin_storage::PageCacheConfig::new(256 * 1024 * 1024)),
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

/// Run a `SELECT id, amount …` and flatten the result into `(id, amount)`
/// pairs in the order the engine returned them (the keyset path returns them
/// already in ORDER BY order).
async fn id_amount_rows(sess: &ProjectSession, sql: &str) -> Vec<(i64, i64)> {
    let batches: Vec<RecordBatch> = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let amount = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount is Int64");
        for r in 0..b.num_rows() {
            out.push((id.value(r), amount.value(r)));
        }
    }
    out
}

/// Build the `events` table and populate it with `n` rows whose
/// `amount = id * 10`, inserted across several interleaved INSERT statements
/// so multiple cold files each span the full id range.
async fn seed_events(sess: &ProjectSession, n: i64) {
    exec(
        sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)",
    )
    .await;
    // Insert in `stripes` passes, each pass taking every `stripes`-th id, so
    // file 0 gets ids {0, S, 2S, …}, file 1 gets {1, S+1, …}, etc. Each file
    // therefore spans [near-0, near-max] — zone maps prune nothing mid-range.
    let stripes = 5i64;
    for offset in 0..stripes {
        let mut vals = String::new();
        let mut id = offset;
        let mut first = true;
        while id < n {
            if !first {
                vals.push_str(", ");
            }
            first = false;
            vals.push_str(&format!("({id}, {})", id * 10));
            id += stripes;
        }
        if !first {
            exec(sess, &format!("INSERT INTO events (id, amount) VALUES {vals}")).await;
        }
    }
}

/// Independent ground truth: rows with `id > threshold`, ascending by id,
/// first `limit`, as `(id, amount=id*10)` pairs.
fn ground_truth_asc(n: i64, threshold: i64, limit: usize) -> Vec<(i64, i64)> {
    (0..n)
        .filter(|&id| id > threshold)
        .take(limit)
        .map(|id| (id, id * 10))
        .collect()
}

/// Independent ground truth for DESC + `id < threshold`.
fn ground_truth_desc(n: i64, threshold: i64, limit: usize) -> Vec<(i64, i64)> {
    (0..n)
        .rev()
        .filter(|&id| id < threshold)
        .take(limit)
        .map(|id| (id, id * 10))
        .collect()
}

const N: i64 = 10_000;

#[tokio::test]
async fn keyset_mid_range_threshold() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 5000 ORDER BY id LIMIT 50",
    )
    .await;
    let want = ground_truth_asc(N, 5000, 50);
    assert_eq!(got, want, "mid-range keyset window mismatch");
    // First row must be 5001 (strictly greater than threshold), 50 rows total.
    assert_eq!(got.len(), 50);
    assert_eq!(got[0], (5001, 50010));
    assert_eq!(got[49], (5050, 50500));
}

#[tokio::test]
async fn keyset_threshold_beyond_max_is_empty() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 999999 ORDER BY id LIMIT 50",
    )
    .await;
    assert!(got.is_empty(), "threshold past max id must yield no rows, got {got:?}");
}

#[tokio::test]
async fn keyset_threshold_below_min_returns_first_page() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    // id > -1 admits every row; the page is the global first 50 by id.
    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > -1 ORDER BY id LIMIT 50",
    )
    .await;
    let want = ground_truth_asc(N, -1, 50);
    assert_eq!(got, want, "below-min keyset window mismatch");
    assert_eq!(got[0], (0, 0));
    assert_eq!(got[49], (49, 490));
}

#[tokio::test]
async fn keyset_desc_with_lt() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    // DESC variant: largest ids below the threshold, descending. The fast
    // path does NOT push a per-file head limit here (files are ASC-sorted, so
    // the largest matching keys live at the tail) — it reads full files and
    // the existing concat+sort+take produces the correct answer.
    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id < 5000 ORDER BY id DESC LIMIT 50",
    )
    .await;
    let want = ground_truth_desc(N, 5000, 50);
    assert_eq!(got, want, "DESC keyset window mismatch");
    assert_eq!(got[0], (4999, 49990));
    assert_eq!(got[49], (4950, 49500));
}

#[tokio::test]
async fn keyset_window_reflects_fast_path_update_overlay() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    // UPDATE a row INSIDE the [5001, 5050] window. A fast-path UPDATE writes a
    // hot-tier overlay; the keyset SELECT must reflect the NEW amount. (When an
    // overlay is live the keyset per-file pushdown is disabled — the read falls
    // back to full files + overlay merge — so this proves correctness of that
    // fallback too.)
    exec(&sess, "UPDATE events SET amount = 777777 WHERE id = 5025").await;

    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 5000 ORDER BY id LIMIT 50",
    )
    .await;
    let mut want = ground_truth_asc(N, 5000, 50);
    // id=5025 is the 25th row past 5000 (index 24): (5025, 50250) → updated.
    let pos = want.iter().position(|&(id, _)| id == 5025).expect("5025 in window");
    want[pos] = (5025, 777777);
    assert_eq!(got, want, "updated row must show new amount in keyset window");
    assert_eq!(got.len(), 50, "window size unchanged by an in-place UPDATE");
}

#[tokio::test]
async fn keyset_window_excludes_deleted_row_and_backfills() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    // DELETE a row inside the first page. The deleted row must be absent AND
    // the page must backfill from the next matching id so it still has 50 rows
    // (5051 enters at the tail). With a live tombstone the per-file pushdown is
    // disabled; this validates the full-file fallback's tombstone suppression.
    exec(&sess, "DELETE FROM events WHERE id = 5025").await;

    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 5000 ORDER BY id LIMIT 50",
    )
    .await;

    // Ground truth: ids in (5000, ∞) excluding 5025, first 50 ascending.
    let want: Vec<(i64, i64)> = (0..N)
        .filter(|&id| id > 5000 && id != 5025)
        .take(50)
        .map(|id| (id, id * 10))
        .collect();
    assert_eq!(got, want, "deleted row must be excluded and backfilled");
    assert!(
        !got.iter().any(|&(id, _)| id == 5025),
        "deleted id 5025 must not appear"
    );
    assert_eq!(got.len(), 50, "page must backfill to full LIMIT after a delete");
    assert_eq!(got[49], (5051, 50510), "backfill row 5051 enters at the tail");
}

#[tokio::test]
async fn keyset_limit_larger_than_remaining_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, N).await;

    // Only ids 9990..9999 remain past the threshold (10 rows); LIMIT 50 must
    // return exactly those 10, not pad or error.
    let got = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 9989 ORDER BY id LIMIT 50",
    )
    .await;
    let want = ground_truth_asc(N, 9989, 50);
    assert_eq!(want.len(), 10, "sanity: only 10 rows remain past 9989");
    assert_eq!(got, want, "limit larger than remaining must return all remaining");
}

/// Diagnostic timing probe (ignored by default). Reports p50 keyset-page
/// latency over 200 iterations at 100k rows. Run with:
///   cargo test -p basin-engine --test keyset_pagination_probe -- --ignored --nocapture
#[ignore]
#[tokio::test]
async fn keyset_latency_p50_100k() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_events(&sess, 100_000).await;

    // Warm up (file discovery, metadata caches).
    let _ = id_amount_rows(
        &sess,
        "SELECT id, amount FROM events WHERE id > 50000 ORDER BY id LIMIT 50",
    )
    .await;

    let iters = 200usize;
    let mut samples: Vec<std::time::Duration> = Vec::with_capacity(iters);
    for i in 0..iters {
        // Vary the threshold so we don't always hit the same warm path.
        let threshold = 1000 + (i as i64 * 17) % 90_000;
        let sql = format!(
            "SELECT id, amount FROM events WHERE id > {threshold} ORDER BY id LIMIT 50"
        );
        let start = std::time::Instant::now();
        let got = id_amount_rows(&sess, &sql).await;
        samples.push(start.elapsed());
        assert!(got.len() <= 50);
    }
    samples.sort();
    let p50 = samples[iters / 2];
    let p95 = samples[(iters * 95) / 100];
    println!(
        "[keyset-latency] n=100000 iters={iters} p50={:?} p95={:?}",
        p50, p95
    );
}
