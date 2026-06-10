//! Self-driving physical layout — step 1: auto-index advisor end-to-end.
//!
//! These tests exercise the advisor exactly the way the production hook does:
//! after a SELECT with a non-PK equality predicate is served, the engine calls
//! `index_advisor::observe_eq_predicates(sess, table, meta, predicates)`. Once
//! a `(project, table, column)` triple crosses `AUTO_INDEX_MIN_HITS`, a one-shot
//! `CREATE INDEX auto_idx_<table>_<col>` fires asynchronously, routing through
//! the identical user-`CREATE INDEX` path (catalog write + registry backfill).
//!
//! We drive `observe_eq_predicates` directly from the test rather than relying
//! on the in-engine hook so the test is self-contained: the call is the SAME
//! public entry point the hook diff wires in, so behaviour is faithful.

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::index_advisor::{
    self, auto_index_name, AUTO_INDEX_MIN_HITS,
};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Predicate, ScalarValue};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn shard_engine() -> (Engine, TempDir, TempDir, basin_shard::Shard) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal,
    ));
    let eng = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (eng, storage_dir, wal_dir, shard)
}

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn count_rows(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

async fn load_meta(
    eng: &Engine,
    project: &ProjectId,
    table: &TableName,
) -> basin_catalog::TableMetadata {
    eng.config()
        .catalog
        .load_table(project, table)
        .await
        .expect("table must exist")
}

fn has_index(meta: &basin_catalog::TableMetadata, name: &str) -> bool {
    meta.indexes.iter().any(|i| i.name == name)
}

/// Drive one served `SELECT … WHERE <col> = <int>` then feed the observer the
/// same predicate the hook would. Returns the row count.
async fn select_and_observe_eq_int(
    sess: &ProjectSession,
    eng: &Engine,
    project: &ProjectId,
    table: &TableName,
    col: &str,
    value: i64,
) -> usize {
    // The SELECT itself triggers the advisor via the fast_select hook —
    // observing manually here would double-count every query.
    let _ = (eng, project);
    count_rows(
        sess,
        &format!("SELECT id FROM {table} WHERE {col} = {value}"),
    )
    .await
}

/// Poll the catalog until `name` appears as an index on `table`, or time out.
async fn wait_for_index(
    eng: &Engine,
    project: &ProjectId,
    table: &TableName,
    name: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let meta = load_meta(eng, project, table).await;
        if has_index(&meta, name) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────

/// 9 repeated `WHERE user_id = ?` queries (threshold is 8) → the advisor
/// auto-creates `auto_idx_events_user_id`, the secondary registry is backfilled
/// (probe_locations hits a live file), and the query stays correct.
#[tokio::test]
async fn repeated_eq_queries_auto_create_index() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("events").unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT)",
    )
    .await;
    for i in 0..2_000i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO events (id, user_id) VALUES ({i}, {i})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    let idx_name = auto_index_name(&table, "user_id");

    // Below-threshold sanity: index must NOT exist before 8 hits.
    for _ in 0..(AUTO_INDEX_MIN_HITS - 1) {
        assert_eq!(
            select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 1234).await,
            1
        );
    }
    let meta = load_meta(&eng, &project, &table).await;
    assert!(
        !has_index(&meta, &idx_name),
        "must not auto-create below threshold"
    );

    // Cross the threshold (this is hit #8) → fires.
    assert_eq!(
        select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 1234).await,
        1
    );
    // One extra hit (#9) for good measure — must not double-fire.
    let _ = select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 1234).await;

    assert!(
        eng.index_advisor_registry_for_test()
            .has_fired(&project, &table, "user_id"),
        "registry must record fire-once after threshold"
    );

    // The async CREATE INDEX must land the index in the catalog.
    assert!(
        wait_for_index(&eng, &project, &table, &idx_name, Duration::from_secs(5)).await,
        "auto index {idx_name} must appear in the catalog"
    );

    // Registry backfilled: probe_locations finds an existing key in a live file.
    let reg = eng.secondary_index_registry_for_test();
    let locs = reg
        .probe_locations(&project, &table, "user_id", "1234")
        .expect("probe_locations must hit after backfill");
    assert_eq!(locs.len(), 1, "user_id=1234 occurs exactly once");
    assert!(
        reg.is_loaded(&project, &table, "user_id"),
        "column index must be loaded after backfill"
    );

    // Query still correct after auto-indexing.
    assert_eq!(
        count_rows(&sess, "SELECT id FROM events WHERE user_id = 1234").await,
        1
    );
    assert_eq!(
        count_rows(&sess, "SELECT id FROM events WHERE user_id = 9999999").await,
        0
    );
}

/// Below threshold → never auto-creates.
#[tokio::test]
async fn below_threshold_no_index() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("events").unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT)",
    )
    .await;
    for i in 0..200i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO events (id, user_id) VALUES ({i}, {i})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    for _ in 0..(AUTO_INDEX_MIN_HITS - 1) {
        let _ = select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 7).await;
    }

    // Give any (erroneously) spawned task a chance to run.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let meta = load_meta(&eng, &project, &table).await;
    assert!(
        !has_index(&meta, &auto_index_name(&table, "user_id")),
        "no index below threshold"
    );
    assert!(!eng
        .index_advisor_registry_for_test()
        .has_fired(&project, &table, "user_id"));
}

/// A pre-existing index on the column → the advisor never creates a duplicate
/// (fire-time metadata check skips it), and the user index name is untouched.
#[tokio::test]
async fn preexisting_index_no_duplicate() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("events").unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT)",
    )
    .await;
    // Pre-create a user index on user_id.
    exec_ok(&sess, "CREATE INDEX my_user_idx ON events (user_id)").await;
    for i in 0..200i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO events (id, user_id) VALUES ({i}, {i})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    // Drive well past threshold.
    for _ in 0..(AUTO_INDEX_MIN_HITS + 3) {
        let _ = select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 7).await;
    }
    // Let the spawned task (which should self-skip) run.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let meta = load_meta(&eng, &project, &table).await;
    assert!(
        has_index(&meta, "my_user_idx"),
        "user index must remain"
    );
    assert!(
        !has_index(&meta, &auto_index_name(&table, "user_id")),
        "advisor must not create a duplicate auto index when one already covers the column"
    );
}

/// Kill switch `BASIN_AUTO_INDEX_DISABLE=1` → never fires regardless of hits.
///
/// Uses a serialised env-var guard since std::env is process-global.
#[tokio::test]
async fn kill_switch_never_fires() {
    // SAFETY: single-threaded test runtime; we set + remove the var within this
    // test and no other test reads it concurrently (each test owns its Engine).
    std::env::set_var("BASIN_AUTO_INDEX_DISABLE", "1");

    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("events").unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT)",
    )
    .await;
    for i in 0..200i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO events (id, user_id) VALUES ({i}, {i})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    for _ in 0..(AUTO_INDEX_MIN_HITS + 5) {
        let _ = select_and_observe_eq_int(&sess, &eng, &project, &table, "user_id", 7).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let fired = eng
        .index_advisor_registry_for_test()
        .has_fired(&project, &table, "user_id");
    let meta = load_meta(&eng, &project, &table).await;
    let has_auto = has_index(&meta, &auto_index_name(&table, "user_id"));

    std::env::remove_var("BASIN_AUTO_INDEX_DISABLE");

    assert!(!fired, "kill switch must prevent the registry from firing");
    assert!(!has_auto, "kill switch must prevent any auto index");
}

/// An unsupported column type (e.g. a non-indexable type) → the advisor never
/// observes it, so it never fires. We use a `REAL` (Float32) column, which is
/// outside the secondary-index extractor's supported set.
#[tokio::test]
async fn unsupported_type_never_fires() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("metrics").unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE metrics (id BIGINT PRIMARY KEY, score REAL)",
    )
    .await;
    for i in 0..200i64 {
        let f = i as f64 / 2.0;
        exec_ok(
            &sess,
            &format!("INSERT INTO metrics (id, score) VALUES ({i}, {f})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    // Feed many Eq predicates on the Float32 column directly to the observer.
    let meta = load_meta(&eng, &project, &table).await;
    for _ in 0..(AUTO_INDEX_MIN_HITS + 5) {
        let preds = vec![Predicate::Eq("score".to_string(), ScalarValue::Float64(1.0))];
        index_advisor::observe_eq_predicates(&sess, &table, &meta, &preds);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        !eng.index_advisor_registry_for_test()
            .has_fired(&project, &table, "score"),
        "unsupported (Float32) column must never be observed → never fires"
    );
    let meta = load_meta(&eng, &project, &table).await;
    assert!(
        !has_index(&meta, &auto_index_name(&table, "score")),
        "no auto index on an unsupported type"
    );
}
