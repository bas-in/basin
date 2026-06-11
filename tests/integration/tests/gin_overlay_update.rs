//! GIN-indexed tables and the hot-tier UPDATE fast path: gate + adversarial
//! containment-read oracle.
//!
//! ## Why these tests exist
//!
//! `try_resolve_fast_path_update` unconditionally declines any table with a
//! secondary index (`!meta.indexes.is_empty()`), which keeps the `jsonb_set`
//! benchmark shape (GIN index on `payload`, `SET payload = jsonb_set(...)`)
//! on the cold copy-on-write path. The obvious relaxation — "GIN pruning only
//! narrows the COLD scan; `UpdateOverlayExec` appends override rows
//! unconditionally, so overlay UPDATEs are invisible to the prune" — is NOT
//! safe today (see the gate comment in `dml_mutate.rs` for the full
//! analysis):
//!
//!   1. The executor's `@>` / `?`-family posting-probe short-circuits return
//!      ZERO ROWS on `ProbeResult::Empty` from the cold-file-only
//!      `GinIndexRegistry`, before any overlay merge — an override whose
//!      post-SET document NEWLY matches a needle with cold-disjoint terms
//!      would be silently dropped.
//!   2. `apply_gin_pruning_for_query` swaps the overlay-aware provider for an
//!      UNWRAPPED `GinRowGroupPrunedTable` / pruned `ListingTable`, which
//!      neither appends override rows nor suppresses their stale cold images.
//!   3. `materialize_overlay_for_table` does no GIN registry maintenance, so
//!      a drained overlay would leave stale postings + an unindexed
//!      replacement file.
//!
//! ## What is pinned here
//!
//! * The GATE: an eligible-looking UPDATE on a GIN-indexed (or B-tree-indexed)
//!   table must NOT plant a memtable override — `update_count` stays 0. If a
//!   future change relaxes the gate without fixing the read paths above, the
//!   gate asserts flip first and the containment asserts in the same tests
//!   become the adversarial correctness oracle (the needle terms are arranged
//!   so the Empty short-circuit and the unwrapped-prune paths would both
//!   surface wrong answers).
//! * The ORACLE: after a `jsonb_set` UPDATE, `@>` reads see the post-image
//!   (newly-matching row returned, no-longer-matching row excluded), point
//!   reads see the new document, and a drain/poll cycle re-asserts all three.
//! * EQUIVALENCE: the same `jsonb_set` UPDATE on a GIN-indexed table (cold
//!   path) and an index-free twin (overlay fast path) produces byte-identical
//!   `id, status, payload` row sets.

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
/// the overlay fast path WHEREVER the gates admit it. The point of the gate
/// tests below is that on indexed tables the gates must NOT admit it even
/// with the env force-on.
async fn with_fastpath_on<F, R>(fut: F) -> R
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

/// Pending overlay entries (`update_count + tombstone_count`) for a table —
/// 0 means every mutation so far took the cold path (or has fully drained).
fn overlay_pending(engine: &Engine, project: &ProjectId, table: &TableName) -> u64 {
    match engine.memtable_registry().get(project, table) {
        Some(e) => e.memtable.update_count() + e.memtable.tombstone_count(),
        None => 0,
    }
}

/// Poll until the table's overlay is empty (or `budget` elapses). With the
/// index gate in place this returns immediately (nothing is ever planted);
/// it is here so the post-drain re-asserts stay meaningful if the gate is
/// ever relaxed and the background reconciler takes over.
async fn wait_overlay_drained(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    budget: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + budget;
    loop {
        if overlay_pending(engine, project, table) == 0 {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    exec(sess, "CREATE INDEX docs_payload_gin ON docs USING gin (payload)").await;
}

/// (a) + (c) + gate: a `jsonb_set` UPDATE that makes a row NEWLY match a
/// containment needle whose terms are split across disjoint cold files.
///
/// Today: the index gate forces the cold path (overlay stays empty — the gate
/// assert), the rewrite rebuilds the posting list, and the `@>` read finds the
/// row. If the gate is ever relaxed without overlay-hardening the GIN read
/// paths, the row sits in the overlay, `probe_containment` returns `Empty`
/// for `{"a":1,"b":2}` (a:1 only in file 1, b:2 only in file 2), and the
/// SELECT returns zero rows — failing this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gin_update_newly_matching_containment_read_returns_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    // Sanity: needle matches nothing while the terms are file-disjoint.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        Vec::<i64>::new(),
        "no row matches the cross-file needle before the UPDATE"
    );

    // The benchmark shape: RMW jsonb_set on the GIN-indexed column, by PK.
    with_fastpath_on(async {
        exec(
            &sess,
            "UPDATE docs SET payload = jsonb_set(payload, '{b}', '2'::jsonb) WHERE id = 1",
        )
        .await;
    })
    .await;

    // GATE: the secondary-index decline must hold — no overlay was planted.
    // This is the tripwire: it flips before any correctness assert can.
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "UPDATE on a GIN-indexed table must DECLINE the overlay fast path \
         (see the meta.indexes gate analysis in dml_mutate.rs) — if this \
         assert fails, the containment asserts below are the read-path oracle"
    );

    // (a) The row NEWLY matches the cross-file needle → must be returned.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        vec![1],
        "post-SET document must be visible to @> (Empty-short-circuit trap)"
    );
    // (c) Point read returns the new document.
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(
        doc.contains("\"a\":1") && doc.contains("\"b\":2"),
        "point read must return the post-jsonb_set document, got {doc}"
    );
    // Row count unchanged.
    assert_eq!(ids_for(&sess, "SELECT id FROM docs").await.len(), 8);

    // (d) After any drain settles (a no-op today — the cold path never plants
    // an overlay), the same reads must still hold: this re-assert exercises
    // the post-materialize GIN registry state if the gate is ever relaxed
    // (materialize_overlay_for_table currently re-registers NOTHING).
    assert!(
        wait_overlay_drained(&eng, &project, &table, std::time::Duration::from_secs(10)).await,
        "overlay must drain"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        vec![1],
        "post-drain @> read must still see the updated document"
    );
    let doc = payload_text(&sess, "docs", 1).await;
    assert!(doc.contains("\"b\":2"), "post-drain point read regressed: {doc}");
}

/// (b) + gate: a `jsonb_set` UPDATE that makes a row STOP matching a needle
/// it used to match. The stale cold image (which still matches) must be
/// suppressed — the unwrapped-pruned-provider trap: a GIN-pruned provider
/// without `UpdateOverlayExec` would keep returning the pre-UPDATE row.
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

    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "UPDATE on a GIN-indexed table must DECLINE the overlay fast path"
    );

    // (b) id=2 must be excluded; its siblings still match.
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

    // (d) Re-assert after the drain poll (no-op today, meaningful post-relax).
    assert!(
        wait_overlay_drained(&eng, &project, &table, std::time::Duration::from_secs(10)).await
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1}'").await,
        vec![1, 3, 4],
        "post-drain @> read must still exclude the updated row"
    );
}

/// (e) B-tree gate: an UPDATE whose assignment WRITES the b-tree-indexed
/// column must keep declining the fast path (stale secondary-index HIT +
/// new-value probe-miss interactions are unproven), and the indexed reads
/// must see the new value via the cold path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn btree_indexed_column_write_declines_fast_path() {
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

    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "UPDATE writing a b-tree-indexed column must DECLINE the overlay fast path"
    );

    // Probe on the NEW value (absent from the index pre-UPDATE) must find it.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'archived'").await,
        vec![3]
    );
    // Probe on the OLD value must no longer return the row.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM items WHERE status = 'active'").await,
        vec![1, 2]
    );
}

/// Byte-equivalence: the exact benchmark `jsonb_set` shape on (i) a
/// GIN-indexed table — which the gate keeps on the cold copy-on-write
/// path — and (ii) an index-free twin — which takes the overlay fast path —
/// must produce identical `id, status, payload` contents. This is the
/// differential that must keep holding if the gate is ever relaxed (the two
/// tables would then BOTH be overlay-path and the GIN table's containment
/// reads are covered by the tests above).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_set_update_equivalent_with_and_without_gin_index() {
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
    exec(&sess, "CREATE INDEX ev_gin_payload_gin ON ev_gin USING gin (payload)").await;

    // The benchmark statement (compare_postgres shape #37), on both tables.
    with_fastpath_on(async {
        for t in ["ev_gin", "ev_plain"] {
            exec(
                &sess,
                &format!(
                    "UPDATE {t} SET payload = \
                     jsonb_set(payload, '{{metadata,score}}', '99'::jsonb) WHERE id < 10"
                ),
            )
            .await;
        }
    })
    .await;

    // Routing asserts: GIN table declined (cold), index-free twin overlaid.
    assert_eq!(
        overlay_pending(&eng, &project, &gin_table),
        0,
        "GIN-indexed table must take the cold path"
    );
    assert!(
        overlay_pending(&eng, &project, &plain_table) > 0,
        "index-free twin must take the overlay fast path (env force-on) — \
         if this fails the equivalence below compares cold vs cold and \
         proves nothing"
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
        "jsonb_set post-images must be byte-identical between the cold path \
         (GIN-indexed) and the overlay fast path (index-free twin)"
    );
    // Spot-check the mutation actually happened on both.
    assert!(gin_rows[5].1.contains("\"score\":99"), "{}", gin_rows[5].1);
    assert!(gin_rows[20].1.contains("\"score\":20"), "{}", gin_rows[20].1);
}
