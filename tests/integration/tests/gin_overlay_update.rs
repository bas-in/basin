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
//! unconditionally, so overlay UPDATEs are invisible to the prune" — was NOT
//! safe when these holes were found (see the gate comment in `dml_mutate.rs`
//! for the full analysis); the READ-PATH halves are now guarded:
//!
//!   1. The executor's `@>` / `?`-family posting-probe short-circuits return
//!      ZERO ROWS on `ProbeResult::Empty` from the cold-file-only
//!      `GinIndexRegistry`, before any overlay merge — an override whose
//!      post-SET document NEWLY matches a needle with cold-disjoint terms
//!      would be silently dropped. GUARDED: `gin_empty_probe_is_trustworthy`
//!      (executor.rs) declines the short-circuit while the table has live
//!      overlay entries OR any live file missing from the indexed-files
//!      completeness set.
//!   2. `apply_gin_pruning_for_query` swaps the overlay-aware provider for an
//!      UNWRAPPED `GinRowGroupPrunedTable` / pruned `ListingTable`, which
//!      neither appends override rows nor suppresses their stale cold images.
//!      GUARDED: both it and `apply_jsonb_posting_pruning_for_query` skip the
//!      pruned re-registration while `session::table_has_live_overlay` (O(1)
//!      counter reads) is true.
//!   3. `materialize_overlay_for_table` does no GIN registry maintenance, so
//!      a drained overlay leaves stale postings + an unindexed replacement
//!      file. MITIGATED twice over: CREATE INDEX settles the overlay
//!      (materialize) BEFORE backfilling, and post-drain the (#1)
//!      completeness guard degrades the stale-posting Empty short-circuit to
//!      a full scan while the pruning paths force-scan the un-indexed
//!      replacement file — correct-but-unpruned. The write-side gate stays
//!      until materialize itself maintains the registries.
//!
//! The dml_mutate gate is still in place — these guards close the read paths
//! for the overlay states that ARE reachable today: an overlay planted before
//! CREATE INDEX, and a fast-path write racing CREATE INDEX (simulated below
//! by planting a registry override directly).
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
    seed_docs_rows(sess).await;
    exec(sess, "CREATE INDEX docs_payload_gin ON docs USING gin (payload)").await;
}

/// The two-file seed WITHOUT the index — for the CREATE-INDEX-over-overlay
/// test, which needs an index-free window where the UPDATE fast path is
/// admitted and an overlay entry can be planted through real SQL.
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

/// Plant a PK-keyed `MemRowValue::Update` override directly into the
/// process-wide memtable registry — the registry state a hot-tier fast-path
/// UPDATE leaves behind. This simulates the only way a GIN-indexed table can
/// carry a live overlay today: a fast-path write racing CREATE INDEX (the
/// writer loaded its table metadata before the catalog index row landed, so
/// the `meta.indexes` gate did not see the index). The post-image batch is
/// built against the CATALOG Arrow schema and IPC-encoded exactly like the
/// engine's own `encode_single_row_ipc` output (StreamWriter wire format,
/// `schema_version` 0 — what `hot_tier_update_by_pk` writes).
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
    let entry = eng.memtable_registry().get_or_create(*project, table.clone());
    entry.memtable.insert(
        basin_hottier::RowKey::builder().append_i64(id).finish(),
        basin_hottier::MemRowValue::update(bytes, 0),
    );
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

/// CREATE-INDEX-OVER-OVERLAY (blocker #3 fix): an overlay planted through the
/// REAL fast path on the index-free table must be settled into cold storage
/// by CREATE INDEX itself (materialize-before-backfill), so the freshly built
/// index covers the post-SET row images. The adversarial needle
/// `{"a":1,"b":2}` would otherwise be a guaranteed wrong answer: without the
/// materialize the backfill indexes the PRE-update cold images (a:1 only in
/// file 1, b:2 only in file 2 → posting intersection ∅ → Empty short-circuit
/// → zero rows), and the overlay row carrying both terms is invisible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_index_over_live_overlay_settles_overlay_and_serves_containment() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_docs_rows(&sess).await; // NO index yet — fast path is admissible.

    // Plant the overlay through real SQL: index-free table → overlay UPDATE.
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
    exec(&sess, "CREATE INDEX docs_payload_gin ON docs USING gin (payload)").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "CREATE INDEX must materialize the live overlay before the backfill \
         (otherwise the index is built over pre-update cold images)"
    );

    // The settled+indexed row must be served by the adversarial needle
    // immediately — this is the Empty-short-circuit trap shape.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
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

    // Post-drain re-assert (drain already happened at CREATE INDEX; the poll
    // also covers a racing background reconciler tick).
    assert!(
        wait_overlay_drained(&eng, &project, &table, std::time::Duration::from_secs(10)).await
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        vec![1]
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
/// surfaces the row.
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
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        Vec::<i64>::new()
    );

    // Simulate the fast-path write racing CREATE INDEX: plant the override
    // the relaxed gate would have written for
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
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
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
        .and_then(|b| b.column(0).as_any().downcast_ref::<arrow_array::Int64Array>())
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
        ids_for(&sess, "SELECT id FROM docs WHERE payload ?& array['a', 'b']").await,
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
        ids_for(&sess, "SELECT id FROM docs WHERE payload ?& array['a', 'b']").await,
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

/// Blocker #3 post-drain: `materialize_overlay_for_table` commits replacement
/// files with NO GIN registry maintenance — stale postings keep pointing at
/// the replaced (dead) files and the replacement file is never indexed. After
/// the drain the overlay counters are zero, so guard #1's overlay half no
/// longer fires; the COMPLETENESS half (every live file must be in the
/// indexed-files set) must degrade the stale-posting Empty short-circuit to a
/// full scan, and the pruning paths' per-file completeness guards must
/// force-scan the un-indexed replacement file. Correct-but-unpruned.
///
/// The drain is forced deterministically through a cold-path mutation's
/// materialize prologue — the same `materialize_overlay_for_table` the
/// background reconciler drives on its tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_drain_without_gin_maintenance_keeps_reads_correct() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();
    seed_gin_table(&sess).await;

    plant_update_override(
        &eng,
        &project,
        &table,
        1,
        "active",
        "{\"a\":1,\"b\":2,\"grp\":\"one\"}",
    )
    .await;
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        vec![1],
        "pre-drain: the overlay guard serves the override row"
    );

    // Force the drain: an UPDATE on a GIN-indexed table declines the fast
    // path (meta.indexes gate) and the cold path's prologue materializes the
    // overlay into a replacement file — withOUT GIN registry maintenance.
    exec(&sess, "UPDATE docs SET status = 'touched' WHERE id = 8").await;
    assert_eq!(
        overlay_pending(&eng, &project, &table),
        0,
        "cold-path mutation must drain the overlay via its materialize prologue"
    );

    // The Empty-probe shape: postings for a:1 point only at the REPLACED
    // (dead) file, so the AND-merge is still ∅ → Empty — but the
    // un-indexed replacement file holds the real match. The completeness
    // guard must force the full scan.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload @> '{\"a\":1,\"b\":2}'").await,
        vec![1],
        "post-drain: stale postings must not short-circuit away the \
         materialized row (completeness guard)"
    );
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE payload ?& array['a', 'b']").await,
        vec![1],
        "post-drain ?& must also fall through to the full scan"
    );
    // FileCandidates shape through the pruning paths: the un-indexed
    // replacement file must be force-scanned (per-file completeness).
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
    // The cold mutation that forced the drain landed too.
    assert_eq!(
        ids_for(&sess, "SELECT id FROM docs WHERE status = 'touched'").await,
        vec![8]
    );
}
