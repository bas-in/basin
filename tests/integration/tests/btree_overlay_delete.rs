//! Single-column B-tree-indexed tables on the hot-tier DELETE/UPDATE fast
//! path: routing + the adversarial allowlist oracle + post-drain registry
//! maintenance.
//!
//! ## The contract pinned here
//!
//! `delete_fastpath_table_eligible` (and `try_resolve_fast_path_update`) ADMIT
//! tables whose every secondary index is `USING gin` OR a single-column
//! `btree` onto the overlay fast path. A DELETE/UPDATE by PK on a
//! btree-indexed table now writes a hot-tier overlay entry instead of a cold
//! copy-on-write rewrite + index rebuild.
//!
//! That is sound because the B-tree read consumer — `fast_select`'s
//! secondary-index allowlist probe (`WHERE indexed_col = <lit>` → file +
//! row-group prune) — is made overlay-aware the same way the GIN consumers
//! are (see `gin_overlay_delete.rs`):
//!
//!   1. OVERLAY GUARD: the probe DECLINES entirely while
//!      `table_has_live_overlay` is true (the count includes `tombstone_count`
//!      and `update_count`), so a HIT can never prune to a cold-file set that
//!      a live tombstone / override row could escape. Reads fall through to
//!      the overlay-aware (TombstoneFilter / UpdateOverlay) scan.
//!   2. DRAIN MAINTENANCE: `materialize_overlay_for_table` rewrites the
//!      tombstoned/updated rows out of their cold file and re-registers the
//!      replacement file's B-tree locations (purge replaced paths +
//!      `backfill_btree_batch`), mirroring the cold CoW
//!      `maintain_btree_secondary_on_replace`. So a drained overlay leaves the
//!      location sets COMPLETE over the live file set and pruning re-engages.
//!
//! ## The adversarial layout
//!
//! The reverted attempt leaked because a purge-without-re-register drain left
//! the replacement file UNINDEXED: a value present in BOTH an untouched file
//! and the (unregistered) replacement file would HIT on the untouched file
//! alone and the pruned read would DROP the replacement file's rows. This
//! oracle reproduces exactly that: indexed value `grp = 100` lives in
//!   * file 1, row id=1  — DELETED (rewritten out by the drain),
//!   * file 1, rows id=2,3,4 — SURVIVE (land in the replacement file),
//!   * file 2, row id=5  — UNTOUCHED.
//! A correct post-drain `WHERE grp = 100` must return {2,3,4,5}. The leak
//! returns {5} only (the untouched file), dropping the replacement file's
//! survivors.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn with_delete_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
    out
}

async fn with_fastpath_off<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev_d = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    let prev_g = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::set_var("BASIN_HOTTIER_FASTPATH_DISABLE", "1");
    let out = fut.await;
    match prev_d {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
    }
    match prev_g {
        Some(v) => std::env::set_var("BASIN_HOTTIER_FASTPATH_DISABLE", v),
        None => std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE"),
    }
    out
}

fn tombstone_count(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.tombstone_count(),
        None => 0,
    }
}

fn overlay_pending(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.update_count() + e.memtable.tombstone_count(),
        None => 0,
    }
}

async fn live_paths(engine: &Engine, project: &ProjectId, table: &TableName) -> Vec<String> {
    let meta = engine
        .config()
        .catalog
        .load_table(project, table)
        .await
        .expect("load_table for live_paths");
    let mut paths: Vec<String> = meta
        .live_data_files()
        .iter()
        .map(|f| f.path.to_string())
        .collect();
    paths.sort();
    paths
}

async fn ids_for(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    let batches = match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let mut ids: Vec<i64> = Vec::new();
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| panic!("expected Int64 ids from {sql:?}"));
        for r in 0..col.len() {
            ids.push(col.value(r));
        }
    }
    ids.sort_unstable();
    ids
}

/// Seed the adversarial two-file layout with a single-column btree index on
/// `grp`. Two separate INSERTs → two separate cold files. `grp = 100` spans
/// file 1 (ids 1..=4) and file 2 (id 5); `grp = 200` lives only in file 2
/// (ids 6,7,8).
async fn seed_btree_table(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT, note TEXT)",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES \
         (1, 100, 'f1'), (2, 100, 'f1'), (3, 100, 'f1'), (4, 100, 'f1')",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES \
         (5, 100, 'f2'), (6, 200, 'f2'), (7, 200, 'f2'), (8, 200, 'f2')",
    )
    .await;
    exec(sess, "CREATE INDEX docs_grp_idx ON docs (grp)").await;
}

/// Drain `docs`'s overlay through `materialize_overlay_for_table` (the routine
/// the background reconciler ticks): a cold-forced DELETE whose predicate
/// matches NOTHING. The cold path's materialize prologue settles the overlay;
/// the no-match predicate rewrites no file of its own — so the post-drain
/// registry state is attributable to the materialize maintenance ALONE.
async fn force_materialize_drain(sess: &ProjectSession) {
    with_fastpath_off(async {
        exec(sess, "DELETE FROM docs WHERE id = 987654321").await;
    })
    .await;
}

/// ROUTING + ADVERSARIAL ALLOWLIST ORACLE + DRAIN: a point DELETE by PK on a
/// single-column btree-indexed table takes the tombstone overlay fast path,
/// stays correct on the indexed `WHERE grp = 100` read DURING the overlay
/// window (overlay guard), and STILL returns every survivor after the
/// materialize drain re-registers the replacement file (no stale-allowlist
/// row drop — the reverted-attempt failure mode).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_point_delete_routes_tombstone_and_serves_allowlist_reads() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_btree_table(&sess).await;

    // Pre-DELETE sanity: grp=100 spans both files, grp=200 only file 2.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 100").await,
        vec![1, 2, 3, 4, 5]
    );
    let files_before = live_paths(&eng, &project, &table).await;
    assert_eq!(
        files_before.len(),
        2,
        "two cold files expected from two INSERTs"
    );

    // DELETE id=1 (file 1, grp=100) on the fast path → tombstone overlay.
    with_delete_fastpath_on(async {
        exec(&sess, "DELETE FROM docs WHERE id = 1").await;
    })
    .await;
    assert!(
        tombstone_count(&eng, &project, &table) > 0,
        "DELETE by PK on a single-column btree-indexed table must ROUTE to the tombstone overlay"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        files_before,
        "the overlay fast path must write ZERO replacement files"
    );

    // ORACLE during the overlay window: the allowlist probe declines, so the
    // indexed read excludes the tombstoned row while every survivor remains.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 100").await,
        vec![2, 3, 4, 5],
        "overlay-window read must drop only the tombstoned row"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 200").await,
        vec![6, 7, 8]
    );

    // DRAIN through materialize_overlay_for_table (rewrites file 1 without
    // id=1 and re-registers the replacement file's grp locations).
    force_materialize_drain(&sess).await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "the drain must settle the overlay"
    );

    // POST-DRAIN: the allowlist probe re-engages. grp=100 now HITs the
    // replacement file (survivors 2,3,4) + the untouched file 2 (row 5). A
    // purge-without-re-register drain would prune to file 2 alone and return
    // {5}, dropping {2,3,4} — the leak this oracle exists to catch.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 100").await,
        vec![2, 3, 4, 5],
        "post-drain allowlist read must keep every survivor (no stale-allowlist drop)"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 200").await,
        vec![6, 7, 8],
        "untouched-file rows must be unaffected"
    );
    assert!(
        ids_for(&sess, "SELECT id FROM docs WHERE id = 1")
            .await
            .is_empty(),
        "the deleted row must stay gone after the drain"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs").await,
        vec![2, 3, 4, 5, 6, 7, 8]
    );
}

/// UPDATE twin: an UPDATE by PK that MOVES a row's indexed value across the
/// adversarial layout stays correct on the old and new values through a drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_point_update_moves_indexed_value_and_serves_reads() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_btree_table(&sess).await;

    // Move id=2 (file 1) from grp=100 → grp=300 on the fast path.
    with_delete_fastpath_on(async {
        exec(&sess, "UPDATE docs SET grp = 300 WHERE id = 2").await;
    })
    .await;
    assert!(
        overlay_pending(&eng, &project, &table) > 0,
        "UPDATE by PK on a btree-indexed table must route to the overlay"
    );

    // Overlay window: old value loses the row, new value gains it.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 100").await,
        vec![1, 3, 4, 5]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 300").await,
        vec![2]
    );

    force_materialize_drain(&sess).await;
    assert_eq!(overlay_pending(&eng, &project, &table), 0);

    // Post-drain: same logical state, now served through the re-registered
    // allowlist.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 100").await,
        vec![1, 3, 4, 5]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 300").await,
        vec![2]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 200").await,
        vec![6, 7, 8]
    );
}
