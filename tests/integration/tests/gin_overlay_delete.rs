//! GIN-indexed tables on the hot-tier DELETE fast path: routing + adversarial
//! containment-read oracle + post-drain registry maintenance.
//!
//! ## The contract pinned here
//!
//! `delete_fastpath_table_eligible` (shared by the point `pk IN (…)` DELETE and
//! the `DELETE … USING` join DELETE) ADMITS tables whose every secondary index
//! is `USING gin` onto the tombstone overlay fast path — the same relaxation
//! `try_resolve_fast_path_update` already carries for UPDATE. A DELETE by PK on
//! a GIN-indexed table now writes a `MemRowValue::Tombstone` instead of a cold
//! copy-on-write rewrite + index rebuild (the 1M-row `DELETE … USING` shape was
//! ~500x slower than Postgres because of that rewrite).
//!
//! That is sound because the SAME three read-path blockers that gate the UPDATE
//! relaxation are tombstone-aware (see `gin_overlay_update.rs` for the UPDATE
//! oracle and the full analysis in the `dml_mutate.rs` gate comment):
//!
//!   1. The executor's posting-probe `Empty` short-circuit fires only when
//!      `gin_empty_probe_is_trustworthy` holds, which requires NO live overlay
//!      — and `table_has_live_overlay` reads `tombstone_count`, so a live
//!      tombstone vetoes the short-circuit.
//!   2. `apply_gin_pruning_for_query` / `apply_jsonb_posting_pruning_for_query`
//!      skip swapping in the unwrapped pruned reader while
//!      `table_has_live_overlay` is true — the deleted row's stale cold image is
//!      suppressed by the overlay-aware (TombstoneFilter) scan for every
//!      containment SELECT during the overlay window.
//!   3. `materialize_overlay_for_table` rewrites the tombstoned row out of its
//!      cold file and performs GIN registry maintenance on the replacement
//!      (purge replaced paths, rebuild + completeness-seal), so a drained
//!      tombstone leaves the posting list COMPLETE and pruning re-engages.
//!
//! ## What is asserted
//!
//!   * ROUTING: a point DELETE on a GIN-only table plants a tombstone
//!     (`tombstone_count > 0`) and writes ZERO replacement files.
//!   * THE ORACLE: `@>` reads are correct DURING the overlay window — the
//!     deleted row is EXCLUDED (the unwrapped-pruned-provider trap) while its
//!     still-live file-mates with the same payload remain — and stay correct
//!     after the drain.
//!   * MAINTENANCE: after `materialize_overlay_for_table` drains the overlay the
//!     posting registry's indexed-files set covers every live file again
//!     (pruning re-engages post-drain).
//!   * USING: a `DELETE … USING` join on a GIN-only table takes the same
//!     tombstone path and serves containment correctly.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across the parallel test threads in this binary.
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

/// Run `fut` with `BASIN_HOTTIER_DELETE_FASTPATH=1` so any DELETE inside takes
/// the tombstone fast path WHEREVER the gates admit it (explicit, in case the
/// ambient environment carries a kill switch), and `BASIN_HOTTIER_UPDATE_FASTPATH`
/// left at its default for the drain helper.
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

/// Force the cold copy-on-write DELETE path (and its `materialize` prologue,
/// which drains any live overlay for the table) by disabling the DELETE fast
/// path for the duration of `fut`.
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

async fn gin_completeness_holds(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    col: &str,
) -> bool {
    let meta = engine
        .config()
        .catalog
        .load_table(project, table)
        .await
        .expect("load_table for completeness check");
    let indexed = engine
        .gin_index_registry_for_test()
        .indexed_files_for(project, table, col);
    let live = meta.live_data_files();
    !live.is_empty() && live.iter().all(|f| indexed.contains(f.path.as_str()))
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

/// Seed the adversarial two-file layout with a GIN index:
///   * file 1 (first INSERT): ids 1..=4, payload `{"a":1,"grp":"one"}`
///   * file 2 (second INSERT): ids 5..=8, payload `{"b":2,"grp":"two"}`
/// A DELETE of id 1 removes ONE of four file-1 rows that share the `a:1` /
/// `grp:one` posting terms — the still-live file-mates (2,3,4) keep the postings
/// alive, so a GIN-pruned read still scans file 1 and the deleted row must be
/// excluded by the overlay TombstoneFilter, not by the pruning (the
/// unwrapped-pruned-provider trap).
async fn seed_gin_table(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, status TEXT, payload JSONB)",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES \
         (1, 'active', '{\"a\":1,\"grp\":\"one\"}'), \
         (2, 'active', '{\"a\":1,\"grp\":\"one\"}'), \
         (3, 'active', '{\"a\":1,\"grp\":\"one\"}'), \
         (4, 'active', '{\"a\":1,\"grp\":\"one\"}')",
    )
    .await;
    exec(
        sess,
        "INSERT INTO docs VALUES \
         (5, 'active', '{\"b\":2,\"grp\":\"two\"}'), \
         (6, 'active', '{\"b\":2,\"grp\":\"two\"}'), \
         (7, 'active', '{\"b\":2,\"grp\":\"two\"}'), \
         (8, 'active', '{\"b\":2,\"grp\":\"two\"}')",
    )
    .await;
    exec(
        sess,
        "CREATE INDEX docs_payload_gin ON docs USING gin (payload)",
    )
    .await;
}

/// Drain `docs`'s overlay through `materialize_overlay_for_table` (the routine
/// the background reconciler ticks): a cold-forced DELETE whose predicate
/// matches NOTHING. The cold path's materialize prologue settles the overlay;
/// the no-match predicate rewrites no file of its own — so the post-drain
/// registry state is attributable to the materialize maintenance ALONE.
async fn force_materialize_drain(sess: &ProjectSession) {
    with_delete_fastpath_off(async {
        exec(sess, "DELETE FROM docs WHERE id = 987654321").await;
    })
    .await;
}

/// ROUTING + ORACLE (excluded) + DRAIN: a point DELETE on a GIN-only table must
/// take the tombstone overlay fast path (`tombstone_count > 0`, zero replacement
/// files), the deleted row must be EXCLUDED from the `@>` needle DURING the
/// overlay window while its still-live file-mates remain, and after the
/// materialize drain the read must STILL be correct with the posting registry's
/// completeness restored (pruning re-engaged).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_point_delete_routes_tombstone_and_serves_containment() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Sanity: the `a:1` needle matches the whole of file 1, and the freshly
    // backfilled index is complete.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 2, 3, 4],
        "all of file 1 matches {{a:1}} before the DELETE"
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "CREATE INDEX backfill must leave the posting registry complete"
    );
    let paths_before = live_paths(&eng, &project, &table).await;

    // The hot shape: point DELETE by PK on the GIN-indexed table.
    with_delete_fastpath_on(async {
        exec(&sess, "DELETE FROM docs WHERE id = 1").await;
    })
    .await;

    // ROUTING: the GIN-only gate must ADMIT the tombstone fast path — a
    // tombstone is live and NO cold replacement file was written.
    assert!(
        tombstone_count(&eng, &project, &table) > 0,
        "point DELETE on a GIN-only table must take the tombstone fast path \
         (tombstone_count > 0) — see the relaxed meta.indexes gate in \
         delete_fastpath_table_eligible"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        paths_before,
        "tombstone routing must write ZERO replacement files (cold file set \
         unchanged)"
    );

    // ORACLE: the deleted row must DISAPPEAR from the `@>` containment read
    // DURING the overlay window — guard #2 keeps the overlay-aware provider
    // registered while the tombstone is live, so the GIN-pruned scan of file 1
    // (still pruned-in by its live file-mates 2,3,4) excludes id 1.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![2, 3, 4],
        "deleted row must be excluded from @> DURING the overlay window"
    );
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"grp\":\"one\"}'"
        )
        .await,
        vec![2, 3, 4],
        "deleted row must be excluded from the grp:one needle too"
    );
    // Point read of the deleted row returns nothing; the table is down one row.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE id = 1").await,
        Vec::<i64>::new(),
        "deleted row must be gone from a point read"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs").await,
        vec![2, 3, 4, 5, 6, 7, 8]
    );

    // DRAIN: settle the overlay through materialize_overlay_for_table.
    force_materialize_drain(&sess).await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "cold-forced statement must drain the overlay via its materialize \
         prologue"
    );

    // Post-drain the reads must hold AND the maintenance block must have
    // rewritten file 1 without id 1 and re-sealed the replacement: completeness
    // restored → pruning re-engages.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![2, 3, 4],
        "post-drain @> read must still exclude the deleted row"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE id = 1").await,
        Vec::<i64>::new(),
        "post-drain point read must still exclude the deleted row"
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "materialize must re-register the replacement file in the GIN posting \
         registry (indexed_files ⊇ live files) so pruning re-engages post-drain"
    );
}

/// USING variant: a `DELETE … USING` join on a GIN-only table must take the
/// same tombstone path and serve containment correctly. The join selects the
/// PKs to delete from a source table; the target is the GIN-indexed `docs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_delete_using_join_routes_tombstone_and_serves_containment() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;
    // Source table naming the PKs to delete: ids 5 and 6 (both in file 2).
    exec(
        &sess,
        "CREATE TABLE doomed (id BIGINT NOT NULL PRIMARY KEY)",
    )
    .await;
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

    // ROUTING: two tombstones, zero replacement files on the target.
    assert!(
        tombstone_count(&eng, &project, &table) >= 2,
        "DELETE … USING on a GIN-only table must tombstone the matched PKs \
         (tombstone_count >= 2)"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        paths_before,
        "DELETE … USING tombstone routing must write ZERO replacement files"
    );

    // ORACLE: the `b:2` needle (all of file 2) now excludes the deleted 5,6.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":2}'").await,
        vec![7, 8],
        "deleted join rows must be excluded from @> DURING the overlay window"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs").await,
        vec![1, 2, 3, 4, 7, 8]
    );

    // DRAIN + post-drain correctness.
    force_materialize_drain(&sess).await;
    assert_eq!(overlay_pending(&eng, &project, &table), 0);
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":2}'").await,
        vec![7, 8],
        "post-drain @> read must still exclude the deleted join rows"
    );
    assert!(gin_completeness_holds(&eng, &project, &table, "payload").await);
}
