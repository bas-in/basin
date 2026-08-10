//! GIN-indexed tables on the hot-tier UPDATE fast path: routing + adversarial
//! containment-read oracle + post-drain registry maintenance.
//!
//! ## The contract pinned here
//!
//! `try_resolve_fast_path_update` ADMITS tables whose every secondary index is
//! `USING gin` (jsonb_ops / jsonb_path_ops containment GIN, tsvector_ops FTS
//! GIN) onto the overlay fast path — the `jsonb_set` benchmark shape (GIN
//! index on `payload`, `SET payload = jsonb_set(...)` by PK) now writes a
//! `MemRowValue::Update` override instead of a cold copy-on-write rewrite.
//! That is sound because all three historical read-path blockers are closed
//! (see the gate comment in `dml_mutate.rs` for the full analysis):
//!
//!   1. The executor's `@>` / `?`-family posting-probe `Empty` short-circuits
//!      fire only when `gin_empty_probe_is_trustworthy` holds: no live
//!      overlay for the table AND every live file in the registry's
//!      indexed-files completeness set. An override whose post-SET document
//!      NEWLY matches a needle with cold-disjoint terms falls through to the
//!      overlay-aware scan instead of being short-circuited to zero rows.
//!   2. `apply_gin_pruning_for_query` / `apply_jsonb_posting_pruning_for_query`
//!      skip swapping the overlay-aware provider for an unwrapped pruned
//!      reader while `session::table_has_live_overlay` (O(1) counter reads)
//!      is true — override rows are appended and their stale cold images
//!      suppressed for every containment SELECT during the overlay window.
//!   3. CREATE INDEX settles the overlay (materialize) BEFORE backfilling,
//!      and `materialize_overlay_for_table` now performs GIN registry
//!      maintenance on its replacement files (purge replaced paths, rebuild
//!      + completeness-seal the replacement), so a drained overlay leaves
//!      the posting list COMPLETE and pruning re-engages instead of
//!      degrading to full scans forever.
//!
//! Also admitted: single-column b-tree indexes — the `fast_select` allowlist
//! probe declines while an overlay is live (overlay-emptiness guard) and
//! `materialize_overlay_for_table` re-registers the replacement file's btree
//! locations on drain. Still declined: GIST / vector (hnsw) and
//! multi-column / expression b-tree (their registries have no overlay guard).
//!
//! ## What is asserted
//!
//! * ROUTING: a `jsonb_set` UPDATE on a GIN-only table plants an overlay
//!   override (`update_count > 0`) and writes ZERO replacement files; an
//!   UPDATE on a single-column b-tree-indexed table ALSO routes to the overlay
//!   and stays correct through the drain.
//! * THE ORACLE: `@>` reads are correct DURING the overlay window — the
//!   newly-matching row is returned (Empty-short-circuit trap) and the
//!   no-longer-matching row is excluded (unwrapped-pruned-provider trap) —
//!   and stay correct after the drain.
//! * MAINTENANCE: after `materialize_overlay_for_table` drains the overlay,
//!   the GIN posting registry's indexed-files set covers every live file
//!   again (the completeness signal both the Empty short-circuit and the
//!   pruning paths require), i.e. pruning RE-ENGAGES post-drain.
//! * EQUIVALENCE: the benchmark `jsonb_set` statement on a GIN-indexed table
//!   (overlay path) and on a cold-FORCED index-free twin produces
//!   byte-identical `id, status, payload` row sets.

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// Serialises env-var mutation across the parallel test threads in this binary
// (same pattern as update_hottier.rs / jsonb_overlay_rewrite.rs).
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

/// Run `fut` with `BASIN_HOTTIER_UPDATE_FASTPATH=1` so any UPDATE inside takes
/// the overlay fast path WHEREVER the gates admit it (explicit, in case the
/// ambient environment carries a kill switch).
async fn with_fastpath_on<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    with_fastpath_env("1", fut).await
}

/// Run `fut` with `BASIN_HOTTIER_UPDATE_FASTPATH=0` — the per-shape kill
/// switch — so every UPDATE inside takes the cold copy-on-write path. The
/// cold path's prologue (`materialize_hot_overlay_into_cold`) drains any live
/// overlay for the target table first, which makes this the deterministic
/// stand-in for a background-reconciler tick in the drain tests below.
async fn with_fastpath_off<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    with_fastpath_env("0", fut).await
}

async fn with_fastpath_env<F, R>(value: &str, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let _g = ENV_LOCK.lock().await;
    let prev = std::env::var("BASIN_HOTTIER_UPDATE_FASTPATH").ok();
    std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", value);
    let out = fut.await;
    match prev {
        Some(v) => std::env::set_var("BASIN_HOTTIER_UPDATE_FASTPATH", v),
        None => std::env::remove_var("BASIN_HOTTIER_UPDATE_FASTPATH"),
    }
    out
}

/// Pending overlay entries (`update_count + tombstone_count`) for a table —
/// 0 means every mutation so far took the cold path (or has fully drained).
fn overlay_pending(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.update_count() + e.memtable.tombstone_count(),
        None => 0,
    }
}

/// Live UPDATE overrides only (the routing signal the fast path leaves).
fn update_count(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.update_count(),
        None => 0,
    }
}

/// Sorted live data-file paths for a table (catalog truth) — the
/// "zero replacement files" routing assert compares this before/after an
/// overlay UPDATE.
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

/// Whether the file-level GIN posting registry's indexed-files completeness
/// set covers EVERY live file of `(table, col)`. This is exactly the
/// completeness half of `gin_empty_probe_is_trustworthy` and the write side
/// of the per-file pruning guards — when it holds (and the overlay is empty)
/// both the Empty short-circuit and the posting-list prune path re-engage.
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

/// Sorted `id` list returned by `sql` (first column must be Int64).
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

/// Raw JSON text of `payload` for one id, whichever physical type the engine
/// returns (LargeBinary on the catalog path, Binary after a Vortex cold read,
/// Utf8 if a projection stringified it).
async fn payload_text(sess: &ProjectSession, table: &str, id: i64) -> String {
    let sql = format!("SELECT payload FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("query failed for {sql:?}: {other:?}"),
    };
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no row for id={id} in {table}"));
    let col = b.column(0);
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        return String::from_utf8_lossy(arr.value(0)).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        return String::from_utf8_lossy(arr.value(0)).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        return arr.value(0).to_string();
    }
    panic!("payload column unexpected type {:?}", col.data_type());
}

/// Seed the adversarial two-file layout:
///   * file 1 (first INSERT): ids 1..=4, payload `{"a":1,"grp":"one"}`
///   * file 2 (second INSERT): ids 5..=8, payload `{"b":2,"grp":"two"}`
/// The `a:1` and `b:2` posting terms live in DISJOINT cold files, so a needle
/// `{"a":1,"b":2}` has an EMPTY cold file intersection — the exact shape where
/// the executor's posting-probe Empty short-circuit would drop an overlay row
/// that newly matches. Then `CREATE INDEX ... USING gin` seals every live
/// file in both GIN registries (completeness guards pass → the pruned
/// re-registration paths are armed too).
async fn seed_gin_table(sess: &ProjectSession) {
    seed_docs_rows(sess).await;
    exec(
        sess,
        "CREATE INDEX docs_payload_gin ON docs USING gin (payload)",
    )
    .await;
}

/// The two-file seed WITHOUT the index — for the CREATE-INDEX-over-overlay
/// test, which needs an index-free window before the index lands.
async fn seed_docs_rows(sess: &ProjectSession) {
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
}

/// Deterministically drain `docs`'s overlay through the SAME
/// `materialize_overlay_for_table` the background reconciler drives: a
/// cold-forced UPDATE (fast path disabled via env) whose predicate matches
/// NOTHING. The cold path's materialize prologue settles the overlay; the
/// no-match predicate means the UPDATE itself rewrites no file and runs no
/// index maintenance of its own — so the post-drain registry state is
/// attributable to the materialize path's maintenance ALONE.
async fn force_materialize_drain(sess: &ProjectSession, table_sql: &str) {
    with_fastpath_off(async {
        exec(
            sess,
            &format!("UPDATE {table_sql} SET status = 'noop' WHERE id = 987654321"),
        )
        .await;
    })
    .await;
}

/// Plant a PK-keyed `MemRowValue::Update` override directly into the
/// process-wide memtable registry — the registry state a hot-tier fast-path
/// UPDATE leaves behind. The read-path guard tests below plant directly
/// (instead of going through SQL) so they keep pinning the executor/session
/// guards in isolation, independent of the write-path gate's admission rules.
/// The post-image batch is built against the CATALOG Arrow schema and
/// IPC-encoded exactly like the engine's own `encode_single_row_ipc` output
/// (StreamWriter wire format, `schema_version` 0 — what
/// `hot_tier_update_by_pk` writes).
async fn plant_update_override(
    eng: &Engine,
    project: &ProjectId,
    table: &TableName,
    id: i64,
    status: &str,
    payload_json: &str,
) {
    let meta = eng
        .config()
        .catalog
        .load_table(project, table)
        .await
        .expect("load_table for override plant");
    let mut cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(meta.schema.fields().len());
    for f in meta.schema.fields() {
        let col: arrow_array::ArrayRef = match f.name().as_str() {
            "id" => Arc::new(arrow_array::Int64Array::from(vec![id])),
            "status" => Arc::new(arrow_array::StringArray::from(vec![status])),
            "payload" => Arc::new(arrow_array::LargeBinaryArray::from_iter_values([
                payload_json.as_bytes(),
            ])),
            other => panic!("unexpected column {other:?} in docs schema"),
        };
        cols.push(col);
    }
    let batch = arrow_array::RecordBatch::try_new(meta.schema.clone(), cols)
        .expect("override batch must match catalog schema");
    let bytes = {
        use arrow::ipc::writer::StreamWriter;
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut buf, batch.schema_ref()).expect("IPC writer init");
        writer.write(&batch).expect("IPC write");
        writer.finish().expect("IPC finish");
        buf
    };
    let entry = eng
        .memtable_registry()
        .get_or_create(*project, table.clone());
    entry.memtable.insert(
        basin_hottier::RowKey::builder().append_i64(id).finish(),
        basin_hottier::MemRowValue::update(bytes, 0),
    );
}

/// ROUTING + ORACLE (newly matching) + DRAIN: a `jsonb_set` UPDATE on a
/// GIN-only table must take the overlay fast path (`update_count > 0`, zero
/// replacement files), the post-SET document must be visible to the
/// cross-file `@>` needle DURING the overlay window (the Empty-short-circuit
/// trap: `a:1` only in file 1, `b:2` only in file 2 → cold posting
/// intersection ∅), and after the materialize drain the read must STILL be
/// correct with the posting registry's completeness restored (pruning
/// re-engaged).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_update_jsonb_set_routes_overlay_and_serves_containment() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Sanity: needle matches nothing while the terms are file-disjoint, and
    // the freshly backfilled index is complete.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        Vec::<i64>::new(),
        "no row matches the cross-file needle before the UPDATE"
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "CREATE INDEX backfill must leave the posting registry complete"
    );
    let paths_before = live_paths(&eng, &project, &table).await;

    // The benchmark shape: RMW jsonb_set on the GIN-indexed column, by PK.
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{b}', '2'::jsonb) WHERE id = 1",
        )
        .await;
    })
    .await;

    // ROUTING: the GIN-only gate must ADMIT the overlay fast path — an
    // UPDATE override is live and NO cold replacement file was written.
    assert!(
        update_count(&eng, &project, &table) > 0,
        "jsonb_set UPDATE on a GIN-only table must take the overlay fast \
         path (update_count > 0) — see the relaxed meta.indexes gate in \
         dml_mutate.rs"
    );
    assert_eq!(
        live_paths(&eng, &project, &table).await,
        paths_before,
        "overlay routing must write ZERO replacement files (cold file set \
         unchanged)"
    );

    // ORACLE (a): the row NEWLY matches the cross-file needle → must be
    // returned while the override is live (guard #1 vetoes the Empty
    // short-circuit; guard #2 keeps the overlay-aware provider registered).
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "post-SET document must be visible to @> DURING the overlay window"
    );
    // Point read returns the new document.
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"a\":1") && doc.contains("\"b\":2"),
        "point read must return the post-jsonb_set document, got {doc}"
    );
    // Row count unchanged.
    assert_eq!(ids_for(&sess, "SELECT id FROM docs").await.len(), 8);

    // DRAIN: settle the overlay through materialize_overlay_for_table (the
    // same routine the background reconciler ticks).
    force_materialize_drain(&sess, "docs").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "cold-forced statement must drain the overlay via its materialize \
         prologue"
    );

    // Post-drain the same reads must hold AND the maintenance block in
    // `materialize_overlay_for_table` must have re-indexed + sealed the
    // replacement file: completeness restored → the Empty short-circuit and
    // the posting prune path re-engage instead of full-scanning forever.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "post-drain @> read must still see the updated document"
    );
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"b\":2"),
        "post-drain point read regressed: {doc}"
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "materialize must re-register the replacement file in the GIN \
         posting registry (indexed_files ⊇ live files) so pruning re-engages \
         post-drain"
    );
    // With completeness restored and the overlay empty, a no-match cross-file
    // needle is once again servable by the (re-armed) Empty short-circuit.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"zz\":9}'"
        )
        .await,
        Vec::<i64>::new()
    );
}

/// ORACLE (no longer matching): a `jsonb_set` UPDATE that makes a row STOP
/// matching a needle it used to match. The stale cold image (which still
/// matches) must be suppressed during the overlay window — the
/// unwrapped-pruned-provider trap: a GIN-pruned provider without
/// `UpdateOverlayExec` would keep returning the pre-UPDATE row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_update_no_longer_matching_containment_read_excludes_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 2, 3, 4]
    );

    // Overwrite id=2's `a` so it stops matching {"a":1}.
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{a}', '999'::jsonb) WHERE id = 2",
        )
        .await;
    })
    .await;

    assert!(
        update_count(&eng, &project, &table) > 0,
        "jsonb_set UPDATE on a GIN-only table must take the overlay fast path"
    );

    // (b) id=2 must be excluded DURING the overlay; its siblings still match.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 3, 4],
        "row whose post-SET document stopped matching must be excluded \
         (stale-cold-image suppression)"
    );
    // The new value is queryable.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":999}'").await,
        vec![2]
    );

    // Drain and re-assert: the materialized replacement must carry the
    // post-SET image and the registry must be complete again.
    force_materialize_drain(&sess, "docs").await;
    assert_eq!(overlay_pending(&eng, &project, &table), 0);
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 3, 4],
        "post-drain @> read must still exclude the updated row"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":999}'").await,
        vec![2]
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "post-drain completeness must hold (materialize reindex)"
    );
}

/// B-tree gate: an UPDATE on a single-column b-tree-indexed table is now
/// ADMITTED to the overlay fast path. The `fast_select` allowlist probe
/// declines while the overlay is live (overlay-emptiness guard), so indexed
/// reads fall through to the overlay-aware scan; and
/// `materialize_overlay_for_table` re-registers the replacement file's
/// locations on drain, so the allowlist re-engages without dropping rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_indexed_column_write_routes_overlay_and_serves_reads() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("items").unwrap();

    exec(
        &sess,
        "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, status TEXT, qty BIGINT)",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO items VALUES \
         (1, 'active', 10), (2, 'active', 20), (3, 'active', 30), (4, 'idle', 40)",
    )
    .await;
    exec(&sess, "CREATE INDEX items_status_idx ON items (status)").await;

    with_fastpath_on(async {
        exec(&sess, "UPDATE items SET status = 'archived' WHERE id = 3").await;
    })
    .await;

    assert!(
        overlay_pending(&eng, &project, &table) > 0,
        "UPDATE on a single-column b-tree-indexed table must ROUTE to the overlay fast path"
    );

    // WHILE the overlay is live: the allowlist probe declines, so reads fall
    // through to the overlay-aware scan and observe the override.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'archived'").await,
        vec![3]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'active'").await,
        vec![1, 2]
    );

    // DRAIN through materialize_overlay_for_table (re-registers the
    // replacement file's btree locations).
    force_materialize_drain(&sess, "items").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "the drain must settle the overlay"
    );

    // POST-DRAIN: the allowlist probe re-engages; reads must STILL be correct
    // (no stale-allowlist row drop — the reverted-attempt failure mode).
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'archived'").await,
        vec![3]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'active'").await,
        vec![1, 2]
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'idle'").await,
        vec![4]
    );
}

/// Mixed-index gate: GIN + single-column b-tree on the same table is now
/// ADMITTED — both read consumers have an overlay-emptiness guard and both
/// registries are re-maintained on drain, so neither can leak a stale read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_plus_btree_mixed_table_routes_overlay_and_serves_reads() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;
    exec(&sess, "CREATE INDEX docs_status_idx ON docs (status)").await;

    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{b}', '2'::jsonb) WHERE id = 1",
        )
        .await;
    })
    .await;

    assert!(
        overlay_pending(&eng, &project, &table) > 0,
        "GIN + single-column b-tree mixed table must ROUTE to the overlay fast path"
    );
    // Overlay-window correctness (both guards engaged): the cross-file needle
    // still finds the row.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1]
    );

    // DRAIN: both the GIN posting registry and the btree location registry are
    // re-maintained over the replacement file.
    force_materialize_drain(&sess, "docs").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "the drain must settle the overlay"
    );
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "containment read must stay correct after the drain"
    );
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "post-drain GIN completeness must hold"
    );
}

/// EQUIVALENCE (benchmark shape #37): the exact `jsonb_set` statement on
/// (i) a GIN-indexed table taking the OVERLAY fast path and (ii) an
/// index-free twin FORCED COLD (fast path kill switch) must produce
/// byte-identical `id, status, payload` contents — the cold copy-on-write
/// rewrite stays the semantics oracle for the overlay path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_set_update_overlay_equivalent_to_cold_forced_twin() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let gin_table = TableName::new("ev_gin").unwrap();
    let plain_table = TableName::new("ev_plain").unwrap();

    for t in ["ev_gin", "ev_plain"] {
        exec(
            &sess,
            &format!(
                "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, status TEXT, payload JSONB)"
            ),
        )
        .await;
        let mut stmt = format!("INSERT INTO {t} VALUES ");
        for k in 0..50i64 {
            if k > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({k}, 'active', '{{\"metadata\":{{\"score\":{k}}},\"tag\":\"t{m}\"}}')",
                m = k % 7
            ));
        }
        exec(&sess, &stmt).await;
    }
    exec(
        &sess,
        "CREATE INDEX ev_gin_payload_gin ON ev_gin USING gin (payload)",
    )
    .await;

    // The benchmark statement on both tables: GIN table with the fast path
    // ON (overlay), index-free twin with the fast path OFF (cold-forced).
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE ev_gin SET payload = \
             jsonb_set(payload, '{metadata,score}', '99'::jsonb) WHERE id < 10",
        )
        .await;
    })
    .await;
    with_fastpath_off(async {
        exec(
            &sess,
            "UPDATE ev_plain SET payload = \
             jsonb_set(payload, '{metadata,score}', '99'::jsonb) WHERE id < 10",
        )
        .await;
    })
    .await;

    // Routing asserts: GIN table overlaid, cold-forced twin did not.
    assert!(
        update_count(&eng, &project, &gin_table) > 0,
        "GIN-indexed table must take the overlay fast path — if this fails \
         the equivalence below compares cold vs cold and proves nothing"
    );
    assert_eq!(
        overlay_pending(&eng, &project, &plain_table),
        0,
        "cold-forced twin must take the copy-on-write path"
    );

    // Byte-equivalence of the full visible row set.
    let mut gin_rows: Vec<(i64, String)> = Vec::new();
    let mut plain_rows: Vec<(i64, String)> = Vec::new();
    for (t, out) in [("ev_gin", &mut gin_rows), ("ev_plain", &mut plain_rows)] {
        for id in 0..50i64 {
            out.push((id, payload_text(&sess, t, id).await));
        }
    }
    assert_eq!(
        gin_rows, plain_rows,
        "jsonb_set post-images must be byte-identical between the overlay \
         fast path (GIN-indexed) and the cold-forced twin"
    );
    // Spot-check the mutation actually happened on both.
    assert!(gin_rows[5].1.contains("\"score\":99"), "{}", gin_rows[5].1);
    assert!(
        gin_rows[20].1.contains("\"score\":20"),
        "{}",
        gin_rows[20].1
    );
}

/// CREATE-INDEX-OVER-OVERLAY (blocker #3, DDL half): an overlay planted
/// through the real fast path BEFORE any index exists must be settled into
/// cold storage by CREATE INDEX itself (materialize-before-backfill), so the
/// freshly built index covers the post-SET row images. The adversarial
/// needle `{"a":1,"b":2}` would otherwise be a guaranteed wrong answer:
/// without the materialize the backfill indexes the PRE-update cold images
/// (a:1 only in file 1, b:2 only in file 2 → posting intersection ∅ → Empty
/// short-circuit → zero rows), and the overlay row carrying both terms is
/// invisible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_index_over_live_overlay_settles_overlay_and_serves_containment() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_docs_rows(&sess).await; // NO index yet.

    // Plant the overlay through real SQL on the index-free table.
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{b}', '2'::jsonb) WHERE id = 1",
        )
        .await;
    })
    .await;
    assert!(
        overlay_pending(&eng, &project, &table) > 0,
        "index-free table must take the overlay fast path (env force-on) — \
         without a live overlay this test proves nothing"
    );

    // CREATE INDEX must settle the overlay BEFORE backfilling.
    exec(
        &sess,
        "CREATE INDEX docs_payload_gin ON docs USING gin (payload)",
    )
    .await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "CREATE INDEX must materialize the live overlay before the backfill \
         (otherwise the index is built over pre-update cold images)"
    );

    // The settled+indexed row must be served by the adversarial needle
    // immediately — this is the Empty-short-circuit trap shape.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "index built over settled data must serve the cross-file needle"
    );
    // Key-existence over the settled data.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload ? 'b'").await,
        vec![1, 5, 6, 7, 8],
        "settled overlay row must carry key 'b' for the ?-probe"
    );
    // Untouched rows still match their original needle.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 2, 3, 4]
    );
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"a\":1") && doc.contains("\"b\":2"),
        "post-settle point read must return the post-jsonb_set document, got {doc}"
    );
}

/// Guard #1a (containment): a live overlay override whose post-SET document
/// NEWLY matches a needle with cold-disjoint terms must be returned. The
/// posting probe for `{"a":1,"b":2}` intersects to ∅ (a:1 only in file 1,
/// b:2 only in file 2) → `ProbeResult::Empty` — pre-guard the executor
/// short-circuited to ZERO ROWS before any overlay merge. With the guard the
/// live-overlay counters veto the short-circuit and the overlay-aware scan
/// surfaces the row. (Planted directly so the guard is pinned independent of
/// the write-path gate.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_overlay_blocks_containment_empty_short_circuit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Sanity: with NO overlay the Empty short-circuit is trustworthy
    // (registries are complete post-backfill) and returns zero rows.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        Vec::<i64>::new()
    );

    // Plant the override the fast path would write for
    // `SET payload = jsonb_set(payload, '{b}', '2')` on id=1.
    plant_update_override(
        &eng,
        &project,
        &table,
        1,
        "active",
        "{\"a\":1,\"b\":2,\"grp\":\"one\"}",
    )
    .await;
    assert!(overlay_pending(&eng, &project, &table) > 0);

    // The Empty probe shape must now fall through to the overlay-aware scan
    // and return the override row.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "live overlay must veto the posting-probe Empty short-circuit"
    );
    // Point read serves the override image.
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"b\":2"),
        "point read must serve the override post-image, got {doc}"
    );
    // The aggregate variant of the same Empty shape must count the override.
    let batches = match sess
        .execute("SELECT count(*) FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'")
        .await
    {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("count query failed: {other:?}"),
    };
    let cnt = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
        })
        .map(|a| a.value(0))
        .expect("count(*) must return one Int64 row");
    assert_eq!(cnt, 1, "aggregate over the overlay row must count it");
}

/// Guard #1a (key existence): same trap for the `?&` all-keys probe. Keys
/// `a` and `b` live in disjoint cold files → AND-merge ∅ → `Empty`; the
/// override row carrying BOTH keys must veto the short-circuit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_overlay_blocks_key_exists_empty_short_circuit() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Sanity: no row holds both keys; the Empty short-circuit is trustworthy.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload ?& array['a', 'b']"
        )
        .await,
        Vec::<i64>::new()
    );

    plant_update_override(
        &eng,
        &project,
        &table,
        1,
        "active",
        "{\"a\":1,\"b\":2,\"grp\":\"one\"}",
    )
    .await;
    assert!(overlay_pending(&eng, &project, &table) > 0);

    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload ?& array['a', 'b']"
        )
        .await,
        vec![1],
        "live overlay must veto the ?& Empty short-circuit"
    );
}

/// Guard #1b (pruned re-registration): a containment query whose probe yields
/// `FileCandidates` (NOT Empty) reaches `exec_select`, where the GIN pruning
/// paths would deregister the overlay-aware provider and register a bare
/// pruned table. With a live overlay that registration must be skipped:
/// the override row appended (it newly matches) AND the stale cold image of
/// a second override suppressed (it no longer matches).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_overlay_blocks_pruned_path_registration() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":2}'").await,
        vec![5, 6, 7, 8]
    );

    // id=1 NEWLY matches {"b":2}; its cold file (file 1) has no b:2 posting,
    // so the pre-guard pruned registration would scan file 2 only and drop it.
    plant_update_override(
        &eng,
        &project,
        &table,
        1,
        "active",
        "{\"a\":1,\"b\":2,\"grp\":\"one\"}",
    )
    .await;
    // id=5 STOPS matching {"b":2}; its stale cold image (which still matches)
    // sits in exactly the file the prune keeps — a bare pruned provider would
    // resurrect it.
    plant_update_override(
        &eng,
        &project,
        &table,
        5,
        "active",
        "{\"b\":999,\"grp\":\"two\"}",
    )
    .await;
    assert!(overlay_pending(&eng, &project, &table) > 0);

    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":2}'").await,
        vec![1, 6, 7, 8],
        "live overlay must keep the overlay-aware provider registered: \
         override row 1 appended, row 5's stale cold image suppressed"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":999}'").await,
        vec![5],
        "the new value of the second override must be queryable"
    );
}

/// MAINTENANCE (blocker #3, materialize half): `materialize_overlay_for_table`
/// must perform GIN registry maintenance on its replacement files — purge the
/// replaced files' stale postings and rebuild + completeness-seal the
/// replacement — so a drained overlay leaves reads correct AND PRUNED.
///
/// Pre-fix, the drain left stale postings pointing at the replaced (dead)
/// file and an un-indexed replacement: the completeness guards degraded every
/// probe to a full scan FOREVER (correct-but-unpruned until a re-CREATE
/// INDEX). The asserts below pin both halves: read correctness through the
/// Empty-probe and pruned-path shapes, and completeness restoration via the
/// registry's indexed-files set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_drain_reindexes_gin_registry_and_reengages_pruning() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Plant through real SQL: the relaxed gate admits the GIN-only table.
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{b}', '2'::jsonb) WHERE id = 1",
        )
        .await;
    })
    .await;
    assert!(
        update_count(&eng, &project, &table) > 0,
        "overlay must be live"
    );
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "pre-drain: the overlay guards serve the override row"
    );

    // Drain through materialize_overlay_for_table (no-match cold-forced
    // statement → prologue settles the overlay; the statement itself
    // rewrites nothing, so the registry state below is the materialize
    // path's maintenance ALONE).
    force_materialize_drain(&sess, "docs").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "materialize prologue must drain the overlay"
    );

    // COMPLETENESS: the replacement file must be indexed + sealed — this is
    // the exact signal `gin_empty_probe_is_trustworthy` and the per-file
    // pruning guards consult, so its restoration IS pruning re-engagement.
    assert!(
        gin_completeness_holds(&eng, &project, &table, "payload").await,
        "materialize must rebuild + seal the replacement file in the GIN \
         posting registry (indexed_files ⊇ live files)"
    );

    // The Empty-probe shape post-drain: with the stale a:1 postings purged
    // and the replacement indexed, the probe now yields real candidates and
    // the scan returns the materialized row.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'"
        )
        .await,
        vec![1],
        "post-drain: the rebuilt postings must serve the materialized row"
    );
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload ?& array['a', 'b']"
        )
        .await,
        vec![1],
        "post-drain ?& must serve the materialized row"
    );
    // FileCandidates shape through the pruning paths (overlay empty →
    // pruned registration allowed again): no row may be lost or resurrected.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"b\":2}'").await,
        vec![1, 5, 6, 7, 8],
        "post-drain pruned-path query must include the materialized row"
    );
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"b\":2"),
        "post-drain point read must serve the materialized image, got {doc}"
    );
    // A needle that matches nothing must be (correctly) empty through the
    // re-armed short-circuit.
    assert_eq!(
        ids_for(
            &sess,
            "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"zz\":9}'"
        )
        .await,
        Vec::<i64>::new()
    );
}
