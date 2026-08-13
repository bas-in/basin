//! The owned engine's scan must read the table's LIVE files, not every file
//! that happens to be on the object store.
//!
//! # What went wrong
//!
//! `basin_exec::storage_source` opened its tables through
//! `basin_storage::Storage::read`, which LISTs the table prefix. A LIST
//! answers "which files physically exist". It does not answer "which files
//! are still part of this table", and the difference is not academic:
//!
//!   * a copy-on-write UPDATE/DELETE writes a replacement file, commits the
//!     swap to the catalog, and deletes the input from a DETACHED task
//!     (`dml_mutate::delete_objects_engine`);
//!   * `basin-shard`'s compaction keeps its superseded inputs for
//!     `BASIN_SUPERSEDED_DELETE_GRACE_SECS` — **300 seconds by default** —
//!     deliberately, so that in-flight scans do not 404.
//!
//! For that whole window both the superseded file and its replacement are
//! present, so a LIST-sourced scan returned every affected row once per
//! physically present copy — and because the copies are different VERSIONS of
//! the row, it returned the pre-UPDATE value alongside the post-UPDATE one.
//! `count(*)` came back 6 where the truth was 3. A third UPDATE made it 12.
//!
//! DataFusion never had this: `session::refresh_table_inner` builds its
//! `ListingTable` over `TableMetadata::live_data_files()` and its comment
//! names this exact failure as bug #41. The owned engine re-introduced #41 by
//! routing through a storage API with the same property.
//!
//! # Why this test is deterministic and the bug looked like a race
//!
//! In a local-filesystem fixture the detached delete almost always wins, so
//! the window is only microseconds and a plain "UPDATE then SELECT" reproduces
//! the corruption a few percent of the time. In production the window is five
//! minutes wide by configuration. So these tests do not race the deleter: they
//! run a REAL UPDATE through the engine, and then PUT the superseded object
//! back at the key it occupied. That is byte-for-byte the state the object
//! store is in for 300 seconds after every compaction — no mocking, no
//! sleeping, no flakiness — and each test asserts the fixture really is that
//! state (superseded object present, not live) before checking any SELECT.
//!
//! Both storage formats are covered: the defect was confirmed on Vortex (the
//! default) and on Parquet.
//!
//! The last test in the file replays the same shapes against a live
//! PostgreSQL 18 when `PG_DIFF_TEST_DSN` is set, and skips cleanly when it is
//! not.

use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use tempfile::TempDir;

/// `BASIN_OWNED_ENGINE` is process-wide; serialize every test that sets it.
/// Same convention (and same poison recovery, for the same reason) as
/// `owned_engine_bridge.rs`.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn engine_in(dir: &TempDir) -> Engine {
    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Flatten two BIGINT columns into `(id, n)` pairs. Every assertion in this
/// file is on VALUES, never on a count: the defect returns the PRE- and
/// POST-update image of the same row, so `1|11, 1|10` and `1|11, 2|20` have
/// the same cardinality and only one of them is the answer.
fn flatten_pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            assert!(!a.is_null(r) && !c.is_null(r));
            out.push((a.value(r), c.value(r)));
        }
    }
    out
}

fn one_i64(batches: &[RecordBatch]) -> i64 {
    let mut vals = Vec::new();
    for b in batches {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            vals.push(a.value(r));
        }
    }
    assert_eq!(vals.len(), 1, "expected a single scalar row, got {vals:?}");
    vals[0]
}

async fn live_paths(eng: &Engine, project: &ProjectId, table: &TableName) -> Vec<String> {
    let meta = eng
        .config()
        .catalog
        .load_table(project, table)
        .await
        .unwrap();
    let mut v: Vec<String> = meta.live_data_files().into_iter().map(|f| f.path).collect();
    v.sort();
    v
}

async fn listed_paths(eng: &Engine, project: &ProjectId, table: &TableName) -> Vec<String> {
    let mut v: Vec<String> = eng
        .config()
        .storage
        .list_data_files(project, table)
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path.to_string())
        .collect();
    v.sort();
    v
}

/// Snapshot every live object's bytes, so the caller can PUT them back after
/// the catalog has superseded them — reconstructing the retention window
/// exactly (see the module docs).
async fn snapshot_objects(
    eng: &Engine,
    project: &ProjectId,
    table: &TableName,
) -> Vec<(ObjectPath, bytes::Bytes)> {
    let store = eng.config().storage.object_store_handle();
    let mut out = Vec::new();
    for p in live_paths(eng, project, table).await {
        let path = ObjectPath::from(p.as_str());
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        out.push((path, bytes));
    }
    out
}

/// Put the superseded objects back where the grace period would have left
/// them.
async fn restore_objects(eng: &Engine, saved: &[(ObjectPath, bytes::Bytes)]) {
    let store = eng.config().storage.object_store_handle();
    for (path, bytes) in saved {
        store.put(path, bytes.clone().into()).await.unwrap();
    }
}

/// Wait for `dml_mutate`'s DETACHED cleanup task to finish removing `saved`,
/// so the restore below cannot be undone by a delete that lands after it.
///
/// This is the only reason the test sleeps at all, and it is waiting for a
/// deletion, not for the bug: with `BASIN_SUPERSEDED_DELETE_GRACE_SECS` at its
/// 300-second default the deletion would not have happened in the first place,
/// and the whole point is to reconstruct that state without waiting 300
/// seconds. If a path is never deleted (the task's live-set guard can veto),
/// the wait simply times out and the restore is a no-op overwrite.
async fn wait_until_deleted(eng: &Engine, saved: &[(ObjectPath, bytes::Bytes)]) {
    let store = eng.config().storage.object_store_handle();
    for _ in 0..600 {
        let mut any_present = false;
        for (path, _) in saved {
            if store.head(path).await.is_ok() {
                any_present = true;
                break;
            }
        }
        if !any_present {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// `WITH (...)` suffix pinning the table's storage format. Vortex is the
/// default and needs no clause.
fn format_clause(parquet: bool) -> &'static str {
    if parquet {
        " WITH (basin.file_format='parquet')"
    } else {
        ""
    }
}

/// Seed `t(id, n)` with three rows and return `(project, table)`.
async fn seed(sess: &ProjectSession, parquet: bool) -> TableName {
    exec(
        sess,
        &format!(
            "CREATE TABLE t (id BIGINT, n BIGINT){}",
            format_clause(parquet)
        ),
    )
    .await;
    exec(
        sess,
        "INSERT INTO t (id, n) VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    TableName::new("t").unwrap()
}

/// The core regression: one UPDATE, its superseded input still on the store,
/// and a SELECT that must not see the pre-UPDATE row.
///
/// Before the fix the owned scan LISTed the prefix and returned
/// `1|10, 1|11, 2|20, 3|30` — four rows, two of them the same row at two
/// different versions. `count(*)` returned 4 where the truth is 3.
async fn update_then_select(parquet: bool) {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = seed(&sess, parquet).await;

    // Snapshot the pre-UPDATE file(s), run a real UPDATE through the real DML
    // path (owned engine off — the fixture must be built the way production
    // builds it), then put the superseded objects back.
    let saved = snapshot_objects(&eng, &project, &table).await;
    assert!(
        !saved.is_empty(),
        "the INSERT must have produced a data file"
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
    exec(&sess, "UPDATE t SET n = 11 WHERE id = 1").await;
    wait_until_deleted(&eng, &saved).await;
    restore_objects(&eng, &saved).await;

    // The fixture is the production state, asserted rather than assumed: the
    // superseded object EXISTS and is NOT live.
    let live = live_paths(&eng, &project, &table).await;
    let listed = listed_paths(&eng, &project, &table).await;
    for (p, _) in &saved {
        let s = p.to_string();
        assert!(
            listed.contains(&s),
            "superseded object {s} must be physically present (that is the 300 s grace window)"
        );
        assert!(
            !live.contains(&s),
            "superseded object {s} must not be live in the catalog"
        );
    }
    assert!(
        listed.len() > live.len(),
        "the whole point: LIST ({}) sees more files than the catalog calls live ({})",
        listed.len(),
        live.len()
    );

    const SELECT: &str = "SELECT id, n FROM t ORDER BY id, n";
    const COUNT: &str = "SELECT count(*) FROM t";
    let truth = vec![(1i64, 11i64), (2, 20), (3, 30)];

    // DataFusion is the control: it has sourced its file set from
    // `live_data_files()` since bug #41, so it is immune, and a failure here
    // would mean the fixture — not the owned engine — is wrong.
    let df = flatten_pairs(&rows(&sess, SELECT).await);
    assert_eq!(df, truth, "DataFusion must be unaffected by the fixture");

    let served_before = eng.owned_engine_served_count();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned = flatten_pairs(&rows(&sess, SELECT).await);
    let owned_count = one_i64(&rows(&sess, COUNT).await);
    std::env::remove_var("BASIN_OWNED_ENGINE");

    assert_eq!(
        eng.owned_engine_served_count() - served_before,
        2,
        "both statements must have been SERVED by the owned engine — a fallback \
         to DataFusion would make the assertions below prove nothing"
    );
    assert_eq!(
        owned, truth,
        "the owned scan returned the superseded file's rows: it must read the \
         catalog's live set, not the object-store LIST"
    );
    assert_eq!(
        owned_count, 3,
        "count(*) over the live set, not over every file on disk"
    );
}

#[tokio::test]
async fn update_then_select_reads_only_live_files_vortex() {
    let _guard = env_lock();
    update_then_select(false).await;
}

#[tokio::test]
async fn update_then_select_reads_only_live_files_parquet() {
    let _guard = env_lock();
    update_then_select(true).await;
}

/// Three sequential UPDATEs, every superseded input retained. This is the
/// shape that showed the defect is multiplicative, not a doubling: the
/// diagnosing agent measured 6, then 9, then 12 rows where the truth was 3.
async fn sequential_updates(parquet: bool) {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = seed(&sess, parquet).await;

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let mut retained: Vec<(ObjectPath, bytes::Bytes)> = Vec::new();
    for sql in [
        "UPDATE t SET n = 11 WHERE id = 1",
        "UPDATE t SET n = 21 WHERE id = 2",
        "UPDATE t SET n = 31 WHERE id = 3",
    ] {
        let inputs = snapshot_objects(&eng, &project, &table).await;
        exec(&sess, sql).await;
        wait_until_deleted(&eng, &inputs).await;
        retained.extend(inputs);
        restore_objects(&eng, &retained).await;
    }

    let live = live_paths(&eng, &project, &table).await;
    let listed = listed_paths(&eng, &project, &table).await;
    assert!(
        listed.len() >= live.len() + 3,
        "three superseded generations must still be on disk: live={} listed={}",
        live.len(),
        listed.len()
    );

    let truth = vec![(1i64, 11i64), (2, 21), (3, 31)];
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned = flatten_pairs(&rows(&sess, "SELECT id, n FROM t ORDER BY id, n").await);
    let count = one_i64(&rows(&sess, "SELECT count(*) FROM t").await);
    let sum = one_i64(&rows(&sess, "SELECT sum(n) FROM t").await);
    std::env::remove_var("BASIN_OWNED_ENGINE");

    assert_eq!(owned, truth);
    assert_eq!(count, 3);
    assert_eq!(sum, 63, "11 + 21 + 31; the stale images would inflate this");
}

#[tokio::test]
async fn three_updates_do_not_multiply_rows_vortex() {
    let _guard = env_lock();
    sequential_updates(false).await;
}

#[tokio::test]
async fn three_updates_do_not_multiply_rows_parquet() {
    let _guard = env_lock();
    sequential_updates(true).await;
}

/// DELETE is affected identically — the deleted row comes back from the
/// superseded file.
async fn delete_then_select(parquet: bool) {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = seed(&sess, parquet).await;

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let saved = snapshot_objects(&eng, &project, &table).await;
    exec(&sess, "DELETE FROM t WHERE id = 3").await;
    wait_until_deleted(&eng, &saved).await;
    restore_objects(&eng, &saved).await;

    let live = live_paths(&eng, &project, &table).await;
    let listed = listed_paths(&eng, &project, &table).await;
    assert!(
        listed.len() > live.len(),
        "the superseded input must still be on disk: live={} listed={}",
        live.len(),
        listed.len()
    );

    let truth = vec![(1i64, 10i64), (2, 20)];
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned = flatten_pairs(&rows(&sess, "SELECT id, n FROM t ORDER BY id, n").await);
    let count = one_i64(&rows(&sess, "SELECT count(*) FROM t").await);
    std::env::remove_var("BASIN_OWNED_ENGINE");

    assert_eq!(
        owned, truth,
        "the deleted row must stay deleted even while its file is retained"
    );
    assert_eq!(count, 2);
}

#[tokio::test]
async fn delete_then_select_does_not_resurrect_rows_vortex() {
    let _guard = env_lock();
    delete_then_select(false).await;
}

#[tokio::test]
async fn delete_then_select_does_not_resurrect_rows_parquet() {
    let _guard = env_lock();
    delete_then_select(true).await;
}

/// The other direction, and the reason the obvious fix is a trap.
///
/// bc57fa48 made the READ catalog-authoritative down in `basin-storage` and
/// lost rows: `Storage::write_batch` puts an object on the store BEFORE its
/// commit lands (that window is exactly why `Storage::note_uncommitted_file`
/// exists), so a catalog-sourced file set inside storage makes a
/// flushed-but-uncommitted file invisible.
///
/// The fix in this branch does not touch `Storage::read`, and this test is
/// what says so: a file written straight through `write_batch`, with no
/// catalog row of its own, must still be readable exactly where it was
/// before — through `Storage::read`, and through the PK candidate set that
/// `note_uncommitted_file` feeds.
#[tokio::test]
async fn an_uncommitted_file_is_still_readable_through_storage_read() {
    use futures::StreamExt;

    let dir = TempDir::new().unwrap();
    let fs = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    storage.attach_catalog(catalog.clone());

    let project = ProjectId::new();
    let table = TableName::new("u").unwrap();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    catalog
        .create_table(&project, &table, &schema)
        .await
        .unwrap();

    // Written, NOT committed: the catalog has the table but no data files.
    let df = storage
        .write_batch(
            &project,
            &table,
            &basin_common::PartitionKey::default_key(),
            &RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![7i64, 8, 9]))],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let meta = catalog.load_table(&project, &table).await.unwrap();
    assert!(
        meta.live_data_files().is_empty(),
        "precondition: the write is deliberately not committed"
    );

    let mut stream = storage
        .read(&project, &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(b) = stream.next().await {
        rows += b.unwrap().num_rows();
    }
    assert_eq!(
        rows, 3,
        "Storage::read must still LIST for existence — making it \
         catalog-authoritative is the bc57fa48 row-loss regression"
    );

    let candidates: Vec<String> = storage
        .pk_candidate_files(&project, &table)
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path.to_string())
        .collect();
    assert!(
        candidates.contains(&df.path.to_string()),
        "note_uncommitted_file must still put the uncommitted write in the PK \
         candidate set; got {candidates:?}"
    );
}

/// The engine-level half of the same question, and the one that says what the
/// fix actually changed for uncommitted data.
///
/// Sourcing the scan's file set from the catalog does change one thing: an
/// object that is on the store but not yet committed is no longer scanned by
/// the owned engine. That is not a row-loss regression, it is CONVERGENCE —
/// DataFusion has never scanned such a file either, because it has always
/// built its `ListingTable` from `live_data_files()`. Reading uncommitted
/// data was the anomaly.
///
/// So the assertion is parity, not a fixed number: whatever the engine's
/// answer is for a table with an uncataloged file sitting next to its
/// committed ones, BOTH engines must give it. A divergence here would mean
/// the owned path had drifted from the reference implementation, in either
/// direction.
#[tokio::test]
async fn owned_and_datafusion_agree_about_an_uncataloged_file() {
    let _guard = env_lock();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let table = seed(&sess, false).await;

    // A file written straight through `Storage::write_batch`: physically
    // present, no catalog row — the same state a flushed-but-uncommitted
    // drain output is in.
    let schema = eng
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .schema;
    eng.config()
        .storage
        .write_batch(
            &project,
            &table,
            &basin_common::PartitionKey::default_key(),
            &RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![99i64])),
                    Arc::new(Int64Array::from(vec![990i64])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let live = live_paths(&eng, &project, &table).await.len();
    let listed = listed_paths(&eng, &project, &table).await.len();
    assert_eq!(
        (live, listed),
        (1, 2),
        "precondition: one committed file, one uncataloged one"
    );

    const SELECT: &str = "SELECT id, n FROM t ORDER BY id, n";
    let df = flatten_pairs(&rows(&sess, SELECT).await);

    let served_before = eng.owned_engine_served_count();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned = flatten_pairs(&rows(&sess, SELECT).await);
    let served = eng.owned_engine_served_count() - served_before;
    std::env::remove_var("BASIN_OWNED_ENGINE");

    assert_eq!(served, 1, "the owned engine must have served it");
    assert_eq!(
        owned, df,
        "the two engines must agree about an uncataloged file; owned={owned:?} datafusion={df:?}"
    );
    assert_eq!(
        owned,
        vec![(1, 10), (2, 20), (3, 30)],
        "and the agreed answer is the committed rows only"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// The same five shapes, against a live PostgreSQL
// ─────────────────────────────────────────────────────────────────────────

/// One scenario, five checkpoints, replayed statement-for-statement against a
/// real PostgreSQL 18 — and on the Basin side with every superseded input
/// still on the object store, i.e. inside the 300-second window in which the
/// defect used to be live.
///
/// Runs only when `PG_DIFF_TEST_DSN` is set (same convention as
/// `differential_pg.rs`); it SKIPS CLEANLY otherwise rather than being
/// `#[ignore]`d, so the deterministic tests above remain the CI guard.
///
/// The comparison is on ROW VALUES throughout. Cardinality alone cannot see
/// this bug: its symptom is the pre- and post-UPDATE image of one row sitting
/// next to each other.
async fn pg_client(tag: &str) -> Option<tokio_postgres::Client> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(d) if !d.trim().is_empty() => d,
        _ => return None,
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("PG_DIFF_TEST_DSN connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let schema = format!("basin_live_{}_{}", tag, std::process::id());
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; \
             SET search_path TO {schema};"
        ))
        .await
        .expect("PG schema bootstrap");
    Some(client)
}

async fn pg_pairs(pg: &tokio_postgres::Client, sql: &str) -> Vec<(i64, i64)> {
    pg.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("pg {sql:?}: {e}"))
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect()
}

/// Render a single-cell result as text, so `avg` compares across two engines
/// without this test having to guess whether it came back NUMERIC or FLOAT8.
fn one_cell_text(batches: &[RecordBatch]) -> String {
    let opts = arrow::util::display::FormatOptions::default();
    let mut out = Vec::new();
    for b in batches {
        let f = arrow::util::display::ArrayFormatter::try_new(b.column(0).as_ref(), &opts).unwrap();
        for r in 0..b.num_rows() {
            out.push(f.value(r).to_string());
        }
    }
    assert_eq!(out.len(), 1, "expected one cell, got {out:?}");
    out.pop().unwrap()
}

async fn basin_scalar_f64(sess: &ProjectSession, sql: &str) -> f64 {
    one_cell_text(&rows(sess, sql).await)
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("{sql:?} did not yield a number: {e}"))
}

async fn pg_scalar_f64(pg: &tokio_postgres::Client, sql: &str) -> f64 {
    let row = pg.query_one(sql, &[]).await.unwrap();
    row.get::<_, f64>(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn matches_live_postgres_across_the_retention_window() {
    let _guard = env_lock();
    let Some(pg) = pg_client("window").await else {
        println!("SKIP matches_live_postgres_across_the_retention_window: PG_DIFF_TEST_DSN unset");
        return;
    };

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    std::env::remove_var("BASIN_OWNED_ENGINE");
    exec(&sess, "CREATE TABLE t (id BIGINT, n BIGINT)").await;
    pg.batch_execute("CREATE TABLE t (id BIGINT, n BIGINT)")
        .await
        .unwrap();
    let table = TableName::new("t").unwrap();

    let seed_sql = "INSERT INTO t (id, n) VALUES (1, 10), (2, 20), (3, 30)";
    exec(&sess, seed_sql).await;
    pg.batch_execute(seed_sql).await.unwrap();

    const SELECT: &str = "SELECT id, n FROM t ORDER BY id, n";
    // Every superseded input stays on the object store for the whole run, so
    // each checkpoint below is read under strictly more retained garbage than
    // the last — exactly how the multiplicity grew to 6, 9 and 12.
    let mut retained: Vec<(ObjectPath, bytes::Bytes)> = Vec::new();

    // A checkpoint: run `dml` on both, retain Basin's superseded inputs, then
    // compare the SELECT's VALUES with the owned engine serving Basin's side.
    macro_rules! step {
        ($dml:expr, $label:expr) => {{
            std::env::remove_var("BASIN_OWNED_ENGINE");
            let inputs = snapshot_objects(&eng, &project, &table).await;
            exec(&sess, $dml).await;
            pg.batch_execute($dml).await.unwrap();
            wait_until_deleted(&eng, &inputs).await;
            retained.extend(inputs);
            restore_objects(&eng, &retained).await;

            let expected = pg_pairs(&pg, SELECT).await;
            let served_before = eng.owned_engine_served_count();
            std::env::set_var("BASIN_OWNED_ENGINE", "1");
            let got = flatten_pairs(&rows(&sess, SELECT).await);
            let served = eng.owned_engine_served_count() - served_before;
            std::env::remove_var("BASIN_OWNED_ENGINE");

            let live = live_paths(&eng, &project, &table).await.len();
            let listed = listed_paths(&eng, &project, &table).await.len();
            println!(
                "{}: live={live} listed={listed} owned_served={served} pg={expected:?} basin={got:?}",
                $label
            );
            assert_eq!(served, 1, "{}: the owned engine must have served it", $label);
            assert_eq!(got, expected, "{}: Basin must match live PG on VALUES", $label);
            expected
        }};
    }

    // Shape 1 — UPDATE then SELECT.
    let after_u1 = step!("UPDATE t SET n = 11 WHERE id = 1", "update#1");
    assert_eq!(after_u1, vec![(1, 11), (2, 20), (3, 30)]);

    // Shape 4 — the aggregates over that same state.
    {
        let served_before = eng.owned_engine_served_count();
        std::env::set_var("BASIN_OWNED_ENGINE", "1");
        let count = basin_scalar_f64(&sess, "SELECT count(*) FROM t").await;
        let sum = basin_scalar_f64(&sess, "SELECT sum(n) FROM t").await;
        let avg = basin_scalar_f64(&sess, "SELECT avg(n) FROM t").await;
        let served = eng.owned_engine_served_count() - served_before;
        std::env::remove_var("BASIN_OWNED_ENGINE");

        let pg_count = pg_scalar_f64(&pg, "SELECT count(*)::float8 FROM t").await;
        let pg_sum = pg_scalar_f64(&pg, "SELECT sum(n)::float8 FROM t").await;
        let pg_avg = pg_scalar_f64(&pg, "SELECT avg(n)::float8 FROM t").await;
        println!(
            "aggregates: owned_served={served}/3 basin=({count},{sum},{avg}) \
             pg=({pg_count},{pg_sum},{pg_avg})"
        );
        assert_eq!(count, pg_count);
        assert_eq!(sum, pg_sum);
        assert!(
            (avg - pg_avg).abs() < 1e-9,
            "avg {avg} vs pg {pg_avg} — the stale row images would drag this down"
        );
    }

    // Shape 2 — two more sequential UPDATEs, three retained generations.
    step!("UPDATE t SET n = 21 WHERE id = 2", "update#2");
    let after_u3 = step!("UPDATE t SET n = 31 WHERE id = 3", "update#3");
    assert_eq!(after_u3, vec![(1, 11), (2, 21), (3, 31)]);

    // Shape 3 — DELETE.
    let after_del = step!("DELETE FROM t WHERE id = 3", "delete");
    assert_eq!(after_del, vec![(1, 11), (2, 21)]);

    // Shape 5 — UPDATE inside an explicit transaction.
    //
    // The owned bridge DECLINES while a transaction is open (`try_execute`
    // returns early on `tx_is_active`), because the owned path takes no
    // snapshot and cannot see the transaction's own uncommitted writes. So the
    // in-transaction SELECT is served by DataFusion; what this checks is that
    // the hand-off is correct in both directions — DataFusion inside the
    // transaction, the owned engine on the very next statement after COMMIT,
    // reading a file set the transaction has just rewritten while every
    // pre-transaction generation is still on disk.
    let inputs = snapshot_objects(&eng, &project, &table).await;
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let served_before = eng.owned_engine_served_count();
    let fell_back_before = eng.owned_engine_fallback_count();
    exec(&sess, "BEGIN").await;
    exec(&sess, "UPDATE t SET n = n * 10").await;
    let in_tx = flatten_pairs(&rows(&sess, SELECT).await);
    let in_tx_served = eng.owned_engine_served_count() - served_before;
    exec(&sess, "COMMIT").await;
    std::env::remove_var("BASIN_OWNED_ENGINE");

    pg.batch_execute("BEGIN; UPDATE t SET n = n * 10; COMMIT;")
        .await
        .unwrap();

    wait_until_deleted(&eng, &inputs).await;
    retained.extend(inputs);
    restore_objects(&eng, &retained).await;

    let expected = pg_pairs(&pg, SELECT).await;
    assert_eq!(expected, vec![(1, 110), (2, 210)]);
    assert_eq!(
        in_tx, expected,
        "the in-transaction SELECT must already see the transaction's own UPDATE"
    );
    assert_eq!(
        in_tx_served, 0,
        "and it must have been served by DataFusion — the owned bridge declines \
         inside an explicit transaction"
    );
    assert!(
        eng.owned_engine_fallback_count() > fell_back_before,
        "that decline must be counted as a fallback, not silently skipped"
    );

    let served_before = eng.owned_engine_served_count();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let after_commit = flatten_pairs(&rows(&sess, SELECT).await);
    let sum = basin_scalar_f64(&sess, "SELECT sum(n) FROM t").await;
    let served = eng.owned_engine_served_count() - served_before;
    std::env::remove_var("BASIN_OWNED_ENGINE");

    let live = live_paths(&eng, &project, &table).await.len();
    let listed = listed_paths(&eng, &project, &table).await.len();
    println!(
        "tx-commit: live={live} listed={listed} owned_served={served}/2 \
         pg={expected:?} basin={after_commit:?} sum={sum}"
    );
    assert_eq!(served, 2, "post-COMMIT the owned engine serves again");
    assert_eq!(after_commit, expected);
    assert_eq!(
        sum,
        pg_scalar_f64(&pg, "SELECT sum(n)::float8 FROM t").await
    );
}
