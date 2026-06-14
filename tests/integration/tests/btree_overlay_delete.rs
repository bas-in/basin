//! B-tree-indexed tables on the hot-tier DELETE fast path: routing + adversarial
//! point-read oracle + post-drain index maintenance.
//!
//! ## The contract pinned here
//!
//! `delete_fastpath_table_eligible` now ADMITS tables whose every secondary
//! index is `USING gin` OR B-tree onto the tombstone overlay fast path. A point
//! DELETE on a B-tree-indexed table writes a `MemRowValue::Tombstone` instead of
//! a cold copy-on-write rewrite + index rebuild (the rewrite is what made the
//! 1M-row `DELETE … USING` shape ~500x slower than Postgres on btree tables).
//!
//! That is sound because the B-tree read path's only overlay hazard — the
//! `fast_select` secondary-index allowlist probe, which treats an index HIT as
//! an authoritative file/row-group allowlist — now skips pruning while
//! `table_has_live_overlay` is true. A row deleted via the tombstone overlay is
//! still physically present in the cold file the index points at (the file is
//! not rewritten until the overlay drains), so pruning to that file and scanning
//! it raw would resurrect the deleted row; the guard instead falls through to
//! the overlay-aware (TombstoneFilter) scan during the overlay window.
//! `materialize_overlay_for_table` then purges the replaced file's entries from
//! the B-tree registry on drain, so a probe for a key whose locations were all
//! in a replaced file becomes a miss → full (correct) scan, and pruning
//! re-engages for the untouched files.
//!
//! ## What is asserted
//!
//!   * ROUTING: a point DELETE on a B-tree-only table plants a tombstone
//!     (`tombstone_count > 0`) and writes ZERO replacement files.
//!   * THE ORACLE: an indexed-column `=` read is correct DURING the overlay
//!     window — the deleted row is EXCLUDED while its still-live file-mates with
//!     the same key remain — and stays correct after the drain.
//!   * `DELETE … USING`: a join DELETE takes the same tombstone path and serves
//!     indexed reads correctly.

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

async fn with_delete_fastpath_off<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_DELETE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", "0");
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_DELETE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_DELETE_FASTPATH"),
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

/// Seed the adversarial two-file layout with a B-tree index on `grp`:
///   * file 1 (first INSERT): ids 1..=4, `grp = 'one'`
///   * file 2 (second INSERT): ids 5..=8, `grp = 'two'`
/// Deleting id 1 removes ONE of four file-1 rows that share `grp = 'one'`; the
/// still-live file-mates (2,3,4) keep the key's index locations pointing at
/// file 1, so an index-pruned `grp = 'one'` read still targets file 1 and the
/// deleted row must be excluded by the overlay TombstoneFilter — NOT by pruning.
async fn seed_btree_table(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, grp TEXT)",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES (1, 'one'), (2, 'one'), (3, 'one'), (4, 'one')",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES (5, 'two'), (6, 'two'), (7, 'two'), (8, 'two')",
    )
    .await;
    exec(sess, "CREATE INDEX docs_grp_btree ON docs (grp)").await;
}

/// Drain `docs`'s overlay through `materialize_overlay_for_table` (the routine
/// the background reconciler ticks): a cold-forced DELETE whose predicate
/// matches NOTHING. The cold path's materialize prologue settles the overlay.
async fn force_materialize_drain(sess: &ProjectSession) {
    with_delete_fastpath_off(async {
        exec(sess, "DELETE FROM docs WHERE id = 987654321").await;
    })
    .await;
}

/// ROUTING + ORACLE (excluded) + DRAIN for a point DELETE on a B-tree table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_point_delete_routes_tombstone_and_serves_indexed_read() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_btree_table(&sess).await;

    // Sanity: the `grp = 'one'` key matches the whole of file 1 before the DELETE.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 'one'").await,
        vec![1, 2, 3, 4],
        "all of file 1 matches grp='one' before the DELETE"
    );
    let paths_before = live_paths(&eng, &project, &table).await;

    // The hot shape: point DELETE by PK on the B-tree-indexed table.
    with_delete_fastpath_on(async {
        exec(&sess, "DELETE FROM docs WHERE id = 1").await;
    })
    .await;

    // ROUTING: the relaxed gate must ADMIT the tombstone fast path — a tombstone
    // is live and NO cold replacement file was written.
    assert!(
        tombstone_count(&eng, &project, &table) > 0,
        "point DELETE on a B-tree-indexed table must take the tombstone fast \
         path (tombstone_count > 0)"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        paths_before,
        "tombstone routing must write ZERO replacement files"
    );

    // ORACLE: the deleted row must be EXCLUDED from the indexed `=` read DURING
    // the overlay window. If the allowlist guard failed, the index would prune
    // to file 1 (still pruned-in by live mates 2,3,4) and scan it raw, leaking
    // the tombstoned id 1.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 'one'").await,
        vec![2, 3, 4],
        "deleted row must be excluded from the indexed read DURING the overlay window"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE id = 1").await,
        Vec::<i64>::new(),
        "deleted row must be gone from a point read"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs").await,
        vec![2, 3, 4, 5, 6, 7, 8]
    );

    // DRAIN, then post-drain correctness (the replaced file's index entries are
    // purged → a probe for 'one' falls through to a correct full scan).
    force_materialize_drain(&sess).await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "cold-forced statement must drain the overlay via its materialize prologue"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 'one'").await,
        vec![2, 3, 4],
        "post-drain indexed read must still exclude the deleted row"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE id = 1").await,
        Vec::<i64>::new(),
        "post-drain point read must still exclude the deleted row"
    );
}

/// USING variant: a `DELETE … USING` join on a B-tree-indexed table takes the
/// same tombstone path and serves indexed reads correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_delete_using_join_routes_tombstone_and_serves_indexed_read() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_btree_table(&sess).await;
    exec(&sess, "CREATE TABLE doomed (id BIGINT NOT NULL PRIMARY KEY)").await;
    exec(&sess, "INSERT INTO doomed VALUES (5), (6)").await;

    let paths_before = live_paths(&eng, &project, &table).await;

    with_delete_fastpath_on(async {
        exec(
            &sess,
            "DELETE FROM docs USING doomed WHERE docs.id = doomed.id",
        )
        .await;
    })
    .await;

    assert!(
        tombstone_count(&eng, &project, &table) >= 2,
        "DELETE … USING on a B-tree table must tombstone the matched PKs"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        paths_before,
        "DELETE … USING tombstone routing must write ZERO replacement files"
    );

    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 'two'").await,
        vec![7, 8],
        "deleted join rows must be excluded from the indexed read during the overlay window"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs").await,
        vec![1, 2, 3, 4, 7, 8]
    );

    force_materialize_drain(&sess).await;
    assert_eq!(overlay_pending(&eng, &project, &table), 0);
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE grp = 'two'").await,
        vec![7, 8],
        "post-drain indexed read must still exclude the deleted join rows"
    );
}
