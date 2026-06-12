//! Scale-invariance gate for `UPDATE … FROM` (and `DELETE … USING`).
//!
//! Regression guard for the P0 `UPDATE … FROM` quadratic: the old executor
//! ran the join SELECT once, then issued ONE full `UPDATE … WHERE pk = X` SQL
//! statement PER matched row. Each inner statement re-read the matched row's
//! pre-image (whole-file decode when PK pushdown couldn't prune) and bumped
//! the hot epoch, forcing a provider cache rebuild over every live file — so
//! the per-statement cost grew with both file count and live-file count while
//! the *number of matched rows stayed fixed*. The result was a clean
//! O(M·F) blow-up: visible as ~150 s at 10k rows and ~51 min at 1M.
//!
//! The fix makes `UPDATE … FROM` set-oriented: ONE join projects every matched
//! row's full POST-image, and ONE batched overlay write (with a cold-drain
//! fallback) applies them — no per-row SQL re-execution. With the WRITE size
//! pinned (a fixed ~`MATCHED` rows updated at every table size), a correct
//! implementation is SCALE-INVARIANT in the target table's size: t(10k) and
//! t(100k) are both small and their ratio is ~1. A per-row-loop regression is
//! >100× over and blows every bound below.
//!
//! Budgets are deliberately generous absolute numbers (overridable via
//! `BASIN_GATE_BUDGET_MS`) so the gate catches the quadratic without being
//! flaky on a slow/loaded box; the *ratio* bound is the scale-invariance
//! statement and is what really pins the fix.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Rows per cold file. Each flush cycle seeds a disjoint id range and flushes,
/// so file count grows with total rows while per-file size stays fixed — the
/// layout that made the per-row loop pay an ever-growing decode per matched
/// row.
const ROWS_PER_FILE: i64 = 5_000;
/// Number of target rows the bench statement actually updates / deletes. FIXED
/// across table sizes so the write cost is constant and any growth in wall
/// time is the per-row-loop quadratic, not honest write work.
const MATCHED: i64 = 200;

/// Per-op wall-clock budget (ms). Overridable so a slow box can relax it
/// without editing the test; the default is loose enough to never flake on a
/// correct (set-oriented) implementation yet ~100× tighter than the per-row
/// loop at 100k.
fn budget_ms(default: f64) -> f64 {
    std::env::var("BASIN_GATE_BUDGET_MS")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| *v > 0.0)
        .unwrap_or(default)
}

type Built = (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
);

async fn build() -> Built {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

async fn exec(sess: &ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql).await.unwrap_or_else(|e| panic!("exec failed: {sql}\n  -> {e:?}"))
}

fn tag_of(res: &ExecResult) -> String {
    match res {
        ExecResult::Empty { tag } => tag.clone(),
        ExecResult::Rows { .. } => panic!("expected Empty tag, got Rows"),
    }
}

fn rows_of(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

async fn int_at(sess: &ProjectSession, sql: &str) -> i64 {
    match exec(sess, sql).await {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int column")
                    .value(0)
            })
            .unwrap_or_else(|| panic!("no rows for {sql}")),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag} for {sql}"),
    }
}

async fn text_at(sess: &ProjectSession, sql: &str) -> String {
    match exec(sess, sql).await {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("text column")
                    .value(0)
                    .to_string()
            })
            .unwrap_or_else(|| panic!("no rows for {sql}")),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag} for {sql}"),
    }
}

/// Seed an `events` table of `total_rows` rows spread across
/// `total_rows / ROWS_PER_FILE` flushed cold files, plus a GIN index on the
/// JSONB payload (so the table carries a secondary index — the shape the cold
/// DELETE…USING path and the overlay fast path must both handle). Returns the
/// number of cold files produced.
async fn seed_events(sess: &ProjectSession, shard: &Shard, total_rows: i64) -> i64 {
    exec(
        sess,
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            status TEXT, \
            val BIGINT, \
            payload JSONB)",
    )
    .await;

    let mut id = 0i64;
    let mut files = 0i64;
    while id < total_rows {
        let lo = id;
        let hi = (id + ROWS_PER_FILE).min(total_rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 48);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k}, 'pending', {k}, '{{\"tag\":\"t{m}\",\"score\":{k}}}')",
                m = k % 7
            ));
        }
        exec(sess, &stmt).await;
        shard.flush_to_parquet().await.unwrap();
        id = hi;
        files += 1;
    }
    exec(sess, "CREATE INDEX events_payload_gin ON events USING gin (payload)").await;
    files
}

/// Seed the `src` join source with `MATCHED` rows whose ids land in the MIDDLE
/// of the target keyspace (never a flush boundary), each carrying a distinct
/// `delta` and a `new_status` the join will copy into the target. Returns the
/// vector of (target_id, delta, new_status) so correctness can be checked
/// against the exact values the join must apply.
async fn seed_src(sess: &ProjectSession, total_rows: i64) -> Vec<(i64, i64, String)> {
    exec(
        sess,
        "CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, delta BIGINT, new_status TEXT)",
    )
    .await;
    let base = total_rows / 2; // interior of the keyspace
    let mut rows: Vec<(i64, i64, String)> = Vec::with_capacity(MATCHED as usize);
    let mut stmt = String::from("INSERT INTO src VALUES ");
    for i in 0..MATCHED {
        let id = base + i;
        let delta = 1_000_000 + i; // unmistakable sentinel, distinct per row
        let new_status = format!("done-{i}");
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({id}, {delta}, '{new_status}')"));
        rows.push((id, delta, new_status));
    }
    exec(sess, &stmt).await;
    rows
}

/// Run the `UPDATE … FROM` bench statement and return its wall-clock ms +
/// affected-row tag.
async fn time_update_from(sess: &ProjectSession) -> (f64, String) {
    let t0 = Instant::now();
    let res = exec(
        sess,
        "UPDATE events SET val = src.delta, status = src.new_status \
         FROM src WHERE events.id = src.id",
    )
    .await;
    (t0.elapsed().as_secs_f64() * 1000.0, tag_of(&res))
}

/// `UPDATE … FROM` is scale-invariant in the target table's size: a fixed
/// `MATCHED`-row write costs the same at 10k and 100k, and the join projection
/// applies EXACTLY the source rows' values to EXACTLY the matched targets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_is_scale_invariant() {
    let mut timings: Vec<(i64, f64)> = Vec::new();

    for &total in &[10_000i64, 100_000] {
        let (_sd, _wd, engine, shard, bg, wal) = build().await;
        let sess = engine.open_session(ProjectId::new()).await.unwrap();
        let files = seed_events(&sess, &shard, total).await;
        let expected = seed_src(&sess, total).await;

        let (ms, tag) = time_update_from(&sess).await;
        timings.push((total, ms));

        // ── Affected-row correctness ─────────────────────────────────────────
        assert_eq!(
            tag,
            format!("UPDATE {MATCHED}"),
            "UPDATE … FROM at {total} rows must report exactly {MATCHED} updated"
        );

        // ── RHS values from the join landed on EXACTLY the matched rows ───────
        // Spot-check the first, a middle, and the last matched row through the
        // PK point-read fast path (which serves the overlay override).
        for &probe in &[0usize, (MATCHED as usize) / 2, (MATCHED as usize) - 1] {
            let (id, delta, ref new_status) = expected[probe];
            let got_val = int_at(&sess, &format!("SELECT val FROM events WHERE id = {id}")).await;
            assert_eq!(
                got_val, delta,
                "matched row id={id} must carry the joined delta {delta} (got {got_val})"
            );
            let got_status =
                text_at(&sess, &format!("SELECT status FROM events WHERE id = {id}")).await;
            assert_eq!(
                &got_status, new_status,
                "matched row id={id} status must be the joined value {new_status:?}"
            );
        }

        // ── Exactly MATCHED rows changed; non-matched untouched ──────────────
        let changed = int_at(
            &sess,
            "SELECT COUNT(*) FROM events WHERE status <> 'pending'",
        )
        .await;
        assert_eq!(
            changed, MATCHED,
            "exactly {MATCHED} rows must have a non-'pending' status at {total} rows"
        );
        // A row just below the matched window keeps its seed values.
        let below = (total / 2) - 1;
        assert_eq!(
            int_at(&sess, &format!("SELECT val FROM events WHERE id = {below}")).await,
            below,
            "non-matched row id={below} val must be untouched"
        );
        assert_eq!(
            text_at(&sess, &format!("SELECT status FROM events WHERE id = {below}")).await,
            "pending",
            "non-matched row id={below} status must be untouched"
        );
        // Total row count is unchanged (UPDATE never adds/drops rows).
        assert_eq!(
            int_at(&sess, "SELECT COUNT(*) FROM events").await,
            total,
            "UPDATE … FROM must not change the row count at {total} rows"
        );

        // ── Per-op budget ────────────────────────────────────────────────────
        let bud = budget_ms(50.0);
        assert!(
            ms < bud,
            "UPDATE … FROM at {total} rows ({files} files) took {ms:.1} ms, budget {bud:.0} ms \
             — a per-row-loop regression is >100× over this",
        );

        println!(
            "[update-from-gate] total={total} files={files} matched={MATCHED} t={ms:.1}ms"
        );

        bg.shutdown().await;
        wal.close().await.unwrap();
    }

    // ── Scale-invariance: t(100k) must not balloon vs t(10k) ─────────────────
    let t10 = timings.iter().find(|(n, _)| *n == 10_000).unwrap().1;
    let t100 = timings.iter().find(|(n, _)| *n == 100_000).unwrap().1;
    // Guard against a divide-by-tiny producing a spurious ratio on a fast box:
    // both are below the absolute budget already (asserted above), so only
    // enforce the ratio when the smaller timing is meaningfully measurable.
    if t10 >= 1.0 {
        let ratio = t100 / t10;
        assert!(
            ratio <= 3.0,
            "UPDATE … FROM is not scale-invariant: t(10k)={t10:.1}ms t(100k)={t100:.1}ms \
             ratio={ratio:.2} (bound 3.0). A ratio that scales with table size is the \
             per-row-loop quadratic.",
        );
        println!("[update-from-gate] ratio t(100k)/t(10k) = {ratio:.2} (bound 3.0)");
    } else {
        println!(
            "[update-from-gate] t(10k)={t10:.2}ms too small to ratio-test; absolute budgets held"
        );
    }
}

/// `DELETE … USING` shares the join-then-set-oriented family. At 100k rows
/// across many GIN-indexed cold files, deleting a fixed `MATCHED` rows must
/// stay well under budget (the cold path narrows to candidate files via
/// zone-map pruning and maintains only the touched files' GIN postings — it
/// must not rewrite/re-index every live file).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_using_at_100k_is_bounded() {
    let total = 100_000i64;
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    let files = seed_events(&sess, &shard, total).await;
    let expected = seed_src(&sess, total).await;

    let before = int_at(&sess, "SELECT COUNT(*) FROM events").await;
    assert_eq!(before, total);

    let t0 = Instant::now();
    let res = exec(
        &sess,
        "DELETE FROM events USING src WHERE events.id = src.id",
    )
    .await;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(
        tag_of(&res),
        format!("DELETE {MATCHED}"),
        "DELETE … USING must report exactly {MATCHED} deleted",
    );

    // Correctness: matched rows gone, non-matched intact, count down by MATCHED.
    assert_eq!(
        int_at(&sess, "SELECT COUNT(*) FROM events").await,
        total - MATCHED,
        "DELETE … USING must drop exactly {MATCHED} rows",
    );
    for &probe in &[0usize, (MATCHED as usize) - 1] {
        let (id, _, _) = expected[probe];
        assert_eq!(
            rows_of(&exec(&sess, &format!("SELECT id FROM events WHERE id = {id}")).await),
            0,
            "deleted row id={id} must be gone",
        );
    }
    let kept = (total / 2) - 1; // just below the deleted window
    assert_eq!(
        rows_of(&exec(&sess, &format!("SELECT id FROM events WHERE id = {kept}")).await),
        1,
        "non-matched row id={kept} must survive",
    );

    let bud = budget_ms(100.0);
    assert!(
        ms < bud,
        "DELETE … USING at {total} rows ({files} files) took {ms:.1} ms, budget {bud:.0} ms",
    );
    println!("[delete-using-gate] total={total} files={files} matched={MATCHED} t={ms:.1}ms");

    bg.shutdown().await;
    wal.close().await.unwrap();
}
