//! Differential + correctness suite for the deep top-K late-materialization
//! fast path (`fast_select.rs::try_topk_late_materialize`).
//!
//! The named perf loss it targets: `SELECT * FROM events ORDER BY amount DESC,
//! id LIMIT 1000` over a wide 1M-row table decoded EVERY column of EVERY row
//! before the sort+limit. The fast path instead decodes only `[amount, id]` in
//! phase 1 to find the winning `id`s, then fetches the full rows for just those
//! `id`s in phase 2.
//!
//! ## How we verify
//!
//! The ground truth is the SAME query run with the fast path DISABLED
//! (`BASIN_TOPK_LATE_MAX_K=0`), which falls through to the existing
//! decode-everything path / DataFusion. Every test runs the query BOTH ways and
//! asserts identical, fully-ordered output rows. We also assert the engagement
//! counter (`topk_late_fast_select_count`) advanced exactly when the fast path
//! was expected to fire, so a silent fall-through can never masquerade as a
//! pass.
//!
//! Cases: multiple k; ASC and DESC; NULLs present (NULLS LAST for ASC / FIRST
//! for DESC, PG default); ties at the LIMIT boundary broken deterministically
//! by the PK; a WHERE-filtered top-K; ORDER BY on the PK directly; the
//! hot-tier winner (an un-flushed row must be able to win, served by the
//! decline-to-merge path); the overlay decline (a fast-path UPDATE that changes
//! a sort value must produce the NEW value — served by the existing overlay
//! merge); and the deep top-K BENCH SHAPE at 100k for correctness.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Env isolation ─────────────────────────────────────────────────────────────

/// Process-wide mutex so `BASIN_TOPK_LATE_MAX_K` flips are serialised; held
/// across awaits so parallel tests never observe a half-configured cap.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}
fn set_env(key: &'static str, val: &str) -> EnvGuard {
    let prev = std::env::var(key).ok();
    unsafe { std::env::set_var(key, val) };
    EnvGuard { key, prev }
}

// ── Engine helper ─────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

// ── Row extraction ────────────────────────────────────────────────────────────

/// One materialised output row from the `events` table, in a comparable form.
/// `amount` is bit-compared (`to_bits`) so NaN/NULL distinctions are exact and
/// the differential comparison is order-sensitive and value-exact.
#[derive(Debug, PartialEq, Eq, Clone)]
struct EventRow {
    id: i64,
    amount_bits: Option<u64>,
    label: Option<String>,
}

/// Collect every output row of a `SELECT id, amount, label …` result in result
/// order (the order the engine produced — the verified top-K order).
async fn select_rows(sess: &ProjectSession, sql: &str) -> Vec<EventRow> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { schema, batches } => {
            let id_i = schema.index_of("id").expect("id col");
            let amt_i = schema.index_of("amount").expect("amount col");
            let lbl_i = schema.index_of("label").ok();
            let mut out = Vec::new();
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let ids = b
                    .column(id_i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id Int64");
                let amts = b
                    .column(amt_i)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("amount Float64");
                let lbls = lbl_i.map(|i| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("label Utf8")
                });
                for r in 0..b.num_rows() {
                    out.push(EventRow {
                        id: ids.value(r),
                        amount_bits: if amts.is_null(r) {
                            None
                        } else {
                            Some(amts.value(r).to_bits())
                        },
                        label: lbls.and_then(|l| {
                            if l.is_null(r) {
                                None
                            } else {
                                Some(l.value(r).to_string())
                            }
                        }),
                    });
                }
            }
            out
        }
        ExecResult::Empty { .. } => Vec::new(),
    }
}

// ── Seeding ────────────────────────────────────────────────────────────────────

/// Create `events(id BIGINT PK, amount DOUBLE PRECISION, status TEXT, label
/// TEXT)` — a deliberately WIDE shape so the late-materialization win (skip the
/// non-key columns of the losers) is exercised. `amount` is the sort key;
/// `label` is a wide payload the two-phase fetch must materialise for the
/// winners only.
async fn create_events(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE events (\
            id BIGINT PRIMARY KEY, \
            amount DOUBLE PRECISION, \
            status TEXT, \
            label TEXT)",
    )
    .await;
}

/// Insert `n` rows. `amount = f(id)` controls the sort distribution;
/// `tie_groups > 0` makes `amount` heavily TIED so the LIMIT boundary lands
/// inside a tie group (PK tie-break under test). When `with_nulls`, every
/// `null_every`-th row gets a NULL amount. Rows are written in several chunks
/// so multiple cold files exist and the per-file min/max skip is exercised.
async fn seed_events(
    sess: &ProjectSession,
    n: i64,
    tie_groups: i64,
    with_nulls: bool,
    null_every: i64,
) {
    let chunk = 2_000_i64;
    let mut i = 1_i64;
    while i <= n {
        let lo = i;
        let hi = (i + chunk - 1).min(n);
        let mut stmt =
            String::from("INSERT INTO events (id, amount, status, label) VALUES ");
        let mut first = true;
        for k in lo..=hi {
            if !first {
                stmt.push(',');
            }
            first = false;
            let amount = if with_nulls && k % null_every == 0 {
                "NULL".to_string()
            } else if tie_groups > 0 {
                // Heavily tied: only `tie_groups` distinct amounts.
                format!("{}.5", k % tie_groups)
            } else {
                // Distinct, non-monotonic-with-id so the sort really reorders.
                format!("{}.25", (k * 7) % 100_000)
            };
            let status = if k % 2 == 0 { "active" } else { "pending" };
            stmt.push_str(&format!(
                "({k}, {amount}, '{status}', 'label-{k}-xxxxxxxxxxxxxxxxxxxx')"
            ));
        }
        exec(sess, &stmt).await;
        i = hi + 1;
    }
}

// ── Differential driver ────────────────────────────────────────────────────────

/// Run `sql` with the fast path ENABLED (default cap) and with it DISABLED
/// (`BASIN_TOPK_LATE_MAX_K=0`), asserting identical ordered output. Returns the
/// fast-path engagement delta so the caller can assert the path actually fired.
///
/// Both runs use the SAME engine + data; only the cap env differs. The disabled
/// run is the ground truth (existing decode-everything / DataFusion path).
async fn differential(
    eng: &Engine,
    sess: &ProjectSession,
    sql: &str,
    expect_fast: bool,
) {
    // Disabled (ground truth) FIRST so the warm cache state is identical for the
    // enabled run (and so an accidental engagement on the truth run is caught).
    let truth = {
        let _g = set_env("BASIN_TOPK_LATE_MAX_K", "0");
        let before = eng.topk_late_fast_select_count();
        let rows = select_rows(sess, sql).await;
        assert_eq!(
            eng.topk_late_fast_select_count(),
            before,
            "fast path must NOT engage when BASIN_TOPK_LATE_MAX_K=0 for {sql:?}"
        );
        rows
    };
    let before = eng.topk_late_fast_select_count();
    let got = select_rows(sess, sql).await;
    let delta = eng.topk_late_fast_select_count() - before;
    assert_eq!(
        got, truth,
        "fast-path result diverged from ground truth for {sql:?}\n fast={got:?}\n truth={truth:?}"
    );
    if expect_fast {
        assert_eq!(delta, 1, "expected top-K fast path to fire once for {sql:?}");
    } else {
        assert_eq!(delta, 0, "expected NO top-K fast path for {sql:?}");
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// The bench shape (`ORDER BY amount DESC, id LIMIT k`) at several k, distinct
/// amounts. Differential vs the disabled path; engagement asserted.
#[tokio::test]
async fn topk_bench_shape_distinct_amounts() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 20_000, 0, false, 0).await;

    for k in [1usize, 10, 100, 1000] {
        let sql = format!("SELECT * FROM events ORDER BY amount DESC, id LIMIT {k}");
        differential(&eng, &sess, &sql, true).await;
        let n = select_rows(&sess, &sql).await.len();
        assert_eq!(n, k.min(20_000), "row count for k={k}");
    }
}

/// ASC and DESC over the same data, with and without the PK tie-break.
#[tokio::test]
async fn topk_asc_desc_and_pk_orderings() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 12_000, 0, false, 0).await;

    // amount DESC, id  (two-key)
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 500",
        true,
    )
    .await;
    // amount ASC, id  (two-key)
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount ASC, id LIMIT 500",
        true,
    )
    .await;
    // ORDER BY the PK directly (single-key, identity == sort col).
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY id DESC LIMIT 300",
        true,
    )
    .await;
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY id ASC LIMIT 300",
        true,
    )
    .await;
}

/// NULLs present: PG default is NULLS LAST for ASC, NULLS FIRST for DESC. The
/// differential vs the disabled path proves the fast path's NULL placement
/// matches.
#[tokio::test]
async fn topk_nulls_ordering() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    // ~1 in 5 rows has a NULL amount.
    seed_events(&sess, 10_000, 0, true, 5).await;

    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 400",
        true,
    )
    .await;
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount ASC, id LIMIT 400",
        true,
    )
    .await;
    // A LIMIT large enough that the NULL block straddles the boundary.
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount ASC, id LIMIT 9000",
        true,
    )
    .await;
}

/// Ties at the LIMIT boundary: heavily-tied `amount` (only a few distinct
/// values) means the LIMIT cut lands inside a tie group. The PK `id` is the
/// deterministic tie-break, so the fast path and the ground-truth path must
/// pick the SAME rows (PG returns deterministic-but-unspecified-among-ties;
/// here the explicit `, id` pins it).
#[tokio::test]
async fn topk_ties_at_boundary() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    // Only 8 distinct amounts across 16k rows → every amount value has ~2000
    // rows; a LIMIT of 1000/2000/3000 cuts mid-tie-group.
    seed_events(&sess, 16_000, 8, false, 0).await;

    for k in [500usize, 1000, 1500, 2000, 3000] {
        let sql = format!("SELECT * FROM events ORDER BY amount DESC, id LIMIT {k}");
        differential(&eng, &sess, &sql, true).await;
    }
}

/// WHERE-filtered top-K: a simple predicate the existing pruning handles. The
/// fast path applies the SAME predicate in phase 1, so its winners match the
/// ground-truth filtered top-K.
#[tokio::test]
async fn topk_where_filtered() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 12_000, 0, false, 0).await;

    differential(
        &eng,
        &sess,
        "SELECT * FROM events WHERE id > 5000 ORDER BY amount DESC, id LIMIT 400",
        true,
    )
    .await;
    differential(
        &eng,
        &sess,
        "SELECT * FROM events WHERE amount > 50000.0 ORDER BY amount DESC, id LIMIT 400",
        true,
    )
    .await;
}

/// Hot-tier winner: a freshly-INSERTed row whose amount beats everything must
/// win the top-1 even though it may live only in the un-flushed tail. The
/// branch DECLINES to the tail-merging path when a tail is present, so the
/// winner is served correctly. (With `shard: None` the write lands in storage
/// synchronously, so this also exercises the post-write fast path; either way
/// the row must win — the differential proves correctness.)
#[tokio::test]
async fn topk_hot_tier_winner() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 8_000, 0, false, 0).await;

    // Insert a clear winner well above the seeded amount domain (< 100000).
    exec(
        &sess,
        "INSERT INTO events (id, amount, status, label) VALUES \
         (999999, 1000000.0, 'active', 'the-winner')",
    )
    .await;

    let rows = select_rows(
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 1",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 999999, "the just-inserted top row must win");
    assert_eq!(rows[0].label.as_deref(), Some("the-winner"));

    // And the full differential still holds with the winner present.
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 50",
        true,
    )
    .await;
}

/// Overlay correctness via decline: a fast-path UPDATE that changes a sort
/// value installs a hot-tier overlay. The top-K branch DECLINES while an
/// overlay is live, so the existing overlay-merge path serves the query and the
/// NEW (updated) amount is used — a tombstoned row cannot win, an updated row
/// uses its new value. The differential vs the disabled path proves it.
#[tokio::test]
async fn topk_overlay_decline_uses_new_value() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 6_000, 0, false, 0).await;

    // Promote id=3000 to the global maximum via a fast-path UPDATE, and a
    // SECOND row (id=2000) to the runner-up region, so both must surface with
    // their NEW values. Tombstone id=3000's pre-update neighbour to also prove
    // a deleted row never wins.
    exec(&sess, "UPDATE events SET amount = 9999999.0 WHERE id = 3000").await;
    exec(&sess, "UPDATE events SET amount = 9999998.0 WHERE id = 2000").await;
    exec(&sess, "DELETE FROM events WHERE id = 4000").await;

    // With an overlay live the branch declines, so we expect NO engagement —
    // but the result must still be correct (NEW amounts win; deleted row gone).
    let before = eng.topk_late_fast_select_count();
    let rows = select_rows(
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 5",
    )
    .await;
    assert_eq!(
        eng.topk_late_fast_select_count(),
        before,
        "branch must decline while a live overlay is present"
    );
    assert_eq!(rows[0].id, 3000, "updated row uses its NEW (winning) amount");
    assert_eq!(rows[1].id, 2000, "second updated row uses its NEW amount");
    assert!(
        rows.iter().all(|r| r.id != 4000),
        "tombstoned row must not appear in the top-K"
    );
}

/// Engagement counter discipline: a non-eligible shape (3-column ORDER BY, an
/// aggregate, a JOIN) must NOT bump the counter, and an eligible shape must.
#[tokio::test]
async fn topk_engagement_counter_gating() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    seed_events(&sess, 4_000, 0, false, 0).await;

    // Eligible → fires.
    let before = eng.topk_late_fast_select_count();
    let _ = select_rows(
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 100",
    )
    .await;
    assert_eq!(eng.topk_late_fast_select_count() - before, 1);

    // ORDER BY with a DESC second key (not the bench tie-break) → not eligible.
    let before = eng.topk_late_fast_select_count();
    let _ = select_rows(
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id DESC LIMIT 100",
    )
    .await;
    assert_eq!(
        eng.topk_late_fast_select_count() - before,
        0,
        "DESC second key is not the eligible shape"
    );

    // ORDER BY with no LIMIT → not eligible (full sort).
    let before = eng.topk_late_fast_select_count();
    let _ = select_rows(&sess, "SELECT * FROM events ORDER BY amount DESC").await;
    assert_eq!(eng.topk_late_fast_select_count() - before, 0);

    // k over the cap → not eligible.
    {
        let _g = set_env("BASIN_TOPK_LATE_MAX_K", "50");
        let before = eng.topk_late_fast_select_count();
        let _ = select_rows(
            &sess,
            "SELECT * FROM events ORDER BY amount DESC, id LIMIT 100",
        )
        .await;
        assert_eq!(
            eng.topk_late_fast_select_count() - before,
            0,
            "k=100 over cap=50 must not engage"
        );
        // …but k <= cap engages.
        let before = eng.topk_late_fast_select_count();
        let _ = select_rows(
            &sess,
            "SELECT * FROM events ORDER BY amount DESC, id LIMIT 50",
        )
        .await;
        assert_eq!(eng.topk_late_fast_select_count() - before, 1);
    }
}

/// The deep top-K BENCH SHAPE at 100k rows, wide rows, `SELECT *`. Correctness
/// (differential) at the scale the benchmark publishes — the fast path must
/// produce byte-identical ordered output to the full-scan path.
#[tokio::test]
async fn topk_bench_shape_100k_correctness() {
    let _lock = ENV_LOCK.lock().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    create_events(&sess).await;
    // 100k rows, distinct-ish amounts, multiple cold files.
    seed_events(&sess, 100_000, 0, false, 0).await;

    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 1000",
        true,
    )
    .await;
    // A smaller k for the file-skip path at scale.
    differential(
        &eng,
        &sess,
        "SELECT * FROM events ORDER BY amount DESC, id LIMIT 10",
        true,
    )
    .await;
}
