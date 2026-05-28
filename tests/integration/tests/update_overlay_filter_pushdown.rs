//! Regression for #88: `UpdateOverlayExec` blocks filter pushdown.
//!
//! Before the fix, when a table had pending hot-tier UPDATE overlays a
//! selective `SELECT ... WHERE pk = $1` degraded to a full cold scan because
//! `UpdateOverlayExec` did not implement DataFusion's
//! `gather_filters_for_pushdown` / `handle_child_pushdown_result`, so every
//! predicate stopped above the overlay and the cold Vortex/Parquet reader saw
//! none of them. This pinned a perf bug worth ~tens of ms on small tables and
//! orders of magnitude on million-row tables.
//!
//! ## Safety properties pinned by this test
//!
//! The fix uses a *two-axis* pushdown strategy:
//!   * To the cold CHILD: declare filters supported iff they reference only
//!     the PK column (overlay cannot change the PK — the fast-path writer
//!     rejects assignments touching it). Non-PK filters stay unsupported at
//!     the child because the overlay is a full-row replacement and may have
//!     mutated any non-PK column.
//!   * To the PARENT: ALWAYS report unsupported so the upper `FilterExec` is
//!     retained. This is required because `UpdateOverlayExec::execute`
//!     unconditionally appends every override (post-SET) row at the tail; the
//!     upper FilterExec must re-evaluate the predicate so non-matching
//!     overrides don't leak into the output.
//!
//! Three assertions on the EXPLAIN plan + four assertions on result-row sets
//! cover correctness AND that the I/O-reduction predicate actually reaches
//! the cold scan.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialise env-var mutation across test threads in this binary.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
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
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

/// Run with `BASIN_HOTTIER_UPDATE_FASTPATH=1` for the duration of `fut`.
async fn with_update_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", "1");
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
    out
}

/// `EXPLAIN <sql>` → flattened plan text (every string column joined by '\n').
async fn explain_text(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    let res = sess
        .execute(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must not error");
    let ExecResult::Rows { batches, .. } = res else {
        panic!("EXPLAIN returned non-Rows");
    };
    let mut lines = Vec::new();
    for b in &batches {
        for c in 0..b.num_columns() {
            if let Some(arr) = b
                .column(c)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        lines.push(arr.value(i).to_string());
                    }
                }
            }
        }
    }
    lines.join("\n")
}

/// `SELECT id FROM …` returning the (sorted) id values.
async fn ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        out.push(col.value(i));
                    }
                }
            }
            out.sort_unstable();
            out
        }
        _ => vec![],
    }
}

/// `SELECT v FROM …` returning the (sorted) v values.
async fn vs(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        out.push(col.value(i));
                    }
                }
            }
            out.sort_unstable();
            out
        }
        _ => vec![],
    }
}

/// Find the line index of the first occurrence of `needle` in `plan` (split
/// on newlines).
fn line_of(plan: &str, needle: &str) -> Option<usize> {
    plan.lines().position(|l| l.contains(needle))
}

/// #88 regression. Builds a table with a cold UPDATE overlay, runs EXPLAIN on
/// both a PK predicate and a non-PK predicate, and asserts the new physical-
/// pushdown wiring on `UpdateOverlayExec`:
///
///   * PK filter (`WHERE id = X`)  → the cold child receives it (visible in
///     the leaf scan node's predicate / filters string) AND a `FilterExec`
///     remains above the overlay (correctness re-check for appended overrides).
///   * Non-PK filter on a mutated column (`WHERE v = X`) → the cold child
///     must NOT receive it (overlay could have moved a row INTO the
///     predicate). The FilterExec stays above the overlay.
///   * Result rows match the overlay-aware semantics in both cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_overlay_pushes_pk_filter_retains_non_pk_filter() {
    let (_sd, _wd, engine, shard, bg, wal) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // 1000 cold rows: (k, k*10).
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for k in 0..1000i64 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{})", k * 10));
    }
    sess.execute(&stmt).await.unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Fast-path UPDATE: id=42's v becomes 99 (was 420). The cold value (420)
    // does NOT match `WHERE v = 99`; the overlay value DOES. This is the
    // hazard case for naive non-PK pushdown: pushing `v = 99` to the cold
    // scan would drop id=42 and the appended override would not get a chance.
    with_update_fastpath_on(async {
        sess.execute("UPDATE t SET v = 99 WHERE id = 42")
            .await
            .unwrap();
    })
    .await;

    // Force a refresh of the catalog-driven cold listing. `flush_to_parquet`
    // and the fast-path UPDATE both skip `refresh_table`, so without this an
    // EXPLAIN still resolves against the stale empty-listing `MemTable` that
    // was registered by the INSERTs (the cold files exist but aren't bound).
    // A no-op `SELECT 1 FROM t LIMIT 0` is the canonical kick: `exec_select`
    // calls `refresh_table` per referenced table before planning.
    let _ = sess.execute("SELECT 1 FROM t LIMIT 0").await.unwrap();

    // ── EXPLAIN: PK predicate ──────────────────────────────────────────────
    //
    // The PK filter is pushable to the cold child (PK cannot be mutated by
    // the overlay). The cold scan should show the predicate; the FilterExec
    // should remain above the overlay (we declared unsupported-to-parent so
    // the upper filter survives to guard the unconditional override append).
    let plan_pk = explain_text(&sess, "SELECT id, v FROM t WHERE id = 42").await;
    let overlay_line = line_of(&plan_pk, "UpdateOverlayExec")
        .unwrap_or_else(|| panic!("UpdateOverlayExec not present in plan:\n{plan_pk}"));
    let filter_above_overlay = plan_pk
        .lines()
        .enumerate()
        .take(overlay_line)
        .any(|(_, l)| l.contains("FilterExec"));
    assert!(
        filter_above_overlay,
        "FilterExec must stay above UpdateOverlayExec to re-filter appended override rows; plan:\n{plan_pk}"
    );
    // The cold scan (Vortex/Parquet DataSourceExec) should carry the predicate
    // `id@0 = 42` in its `predicate:` field. Match the exact `@N = 42` form so
    // we don't false-positive on file paths that happen to contain "42" or
    // "id" digits.
    let below_overlay: String = plan_pk
        .lines()
        .skip(overlay_line + 1)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        below_overlay.contains("predicate: id@0 = 42")
            || below_overlay.contains("predicate=id@0 = 42"),
        "PK predicate `id = 42` must reach the cold DataSourceExec's `predicate:` field (below UpdateOverlayExec); below-overlay slice:\n{below_overlay}\nfull plan:\n{plan_pk}"
    );

    // ── EXPLAIN: non-PK predicate on a column the overlay mutated ──────────
    //
    // Filters on a non-PK column must NOT be pushed past the overlay (the
    // overlay can change v for any row). The FilterExec must be at or above
    // the overlay.
    let plan_v = explain_text(&sess, "SELECT id, v FROM t WHERE v = 99").await;
    let overlay_line_v = line_of(&plan_v, "UpdateOverlayExec")
        .unwrap_or_else(|| panic!("UpdateOverlayExec not present in plan:\n{plan_v}"));
    let below_overlay_v: String = plan_v
        .lines()
        .skip(overlay_line_v + 1)
        .collect::<Vec<_>>()
        .join("\n");
    // The cold DataSourceExec must NOT carry a `predicate:` field referencing
    // `v`. We check the literal `predicate: v` / `predicate=v` substring — the
    // overlay's pushdown gates exclude non-PK columns, so the cold scan should
    // receive zero predicates here.
    assert!(
        !(below_overlay_v.contains("predicate: v")
            || below_overlay_v.contains("predicate=v")),
        "non-PK filter on mutated column `v = 99` must NOT be pushed past UpdateOverlayExec into the cold DataSourceExec; below-overlay slice:\n{below_overlay_v}\nfull plan:\n{plan_v}"
    );
    let filter_above_overlay_v = plan_v
        .lines()
        .enumerate()
        .take(overlay_line_v)
        .any(|(_, l)| l.contains("FilterExec"));
    assert!(
        filter_above_overlay_v,
        "FilterExec must stay above the overlay for non-PK predicate (overlay-aware re-eval); plan:\n{plan_v}"
    );

    // ── Correctness: result rows ──────────────────────────────────────────
    //
    // PK predicate after overlay: id=42 still has its overlay-set v=99.
    let by_pk_v: Vec<i64> = match sess
        .execute("SELECT v FROM t WHERE id = 42")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..c.len()).filter(|i| !c.is_null(*i)).map(|i| c.value(i))
            })
            .collect(),
        _ => vec![],
    };
    assert_eq!(
        by_pk_v,
        vec![99],
        "PK-predicate read after fast-path UPDATE must see overlay value 99 (not cold 420)"
    );

    // Non-PK predicate matches the overlay row exactly (overlay sets v=99).
    let by_v = ids(&sess, "SELECT id FROM t WHERE v = 99 ORDER BY id").await;
    assert_eq!(
        by_v,
        vec![42],
        "non-PK predicate must see overlay-set row id=42 with v=99 (overlay-aware)"
    );

    // Non-PK predicate that the OLD cold value satisfies but the overlay
    // does NOT: `v = 420` should match NO row (id=42's cold v=420 is
    // shadowed by the overlay v=99). This is the "filter on mutated column"
    // correctness test — pushing `v = 420` to the cold scan would have
    // (wrongly) surfaced the stale row.
    let stale = ids(&sess, "SELECT id FROM t WHERE v = 420 ORDER BY id").await;
    assert!(
        stale.is_empty(),
        "stale cold value 420 must not surface — overlay shadows it; got {stale:?}"
    );

    // Range predicate sanity: `v >= 9000` matches all cold rows with k*10 >= 9000
    // (i.e. k in [900, 999]) PLUS no overlay (id=42 has v=99). 100 rows.
    let big = ids(&sess, "SELECT id FROM t WHERE v >= 9000 ORDER BY id").await;
    let expected: Vec<i64> = (900..1000).collect();
    assert_eq!(big, expected, "range on non-PK column composes with overlay");

    // PK-range read: `id < 100` → 100 rows, with id=42's v=99 (overlay), not 420.
    let small_ids = ids(&sess, "SELECT id FROM t WHERE id < 100 ORDER BY id").await;
    assert_eq!(small_ids, (0..100).collect::<Vec<i64>>());
    let small_vs = vs(&sess, "SELECT v FROM t WHERE id = 42 ORDER BY id").await;
    assert_eq!(small_vs, vec![99]);

    bg.shutdown().await;
    wal.close().await.unwrap();
}
