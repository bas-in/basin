//! FIX 2 — the secondary B-tree index registry is populated for compacted /
//! pre-existing data.
//!
//! Two gaps are covered:
//!   (a) compaction (`reindex_compacted_file_secondary`) registers a newly
//!       compacted file's single-column index values into the registry, and
//!   (b) `CREATE INDEX` backfills the registry over files that already exist.
//!
//! On the shard path INSERTs go to WAL+tail and the engine's
//! `maintain_secondary_indexes_on_insert` is bypassed, so without these fixes
//! the registry stays empty for shard-written data and every point query falls
//! back to a full scan.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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
            commit_delay: Duration::from_millis(2),
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

async fn live_file_paths(eng: &Engine, project: &ProjectId, table: &TableName) -> Vec<String> {
    eng.config()
        .catalog
        .load_table(project, table)
        .await
        .unwrap()
        .live_data_files()
        .iter()
        .map(|f| f.path.to_string())
        .collect()
}

/// FIX 2(b) — seed via shard, flush, then CREATE INDEX on user_id. The backfill
/// must register the column values so `probe_locations` returns hits for an
/// existing key and the point SELECT returns the correct rows.
#[tokio::test]
async fn secondary_create_index_backfills_preexisting_files() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("events").unwrap();

    exec_ok(&sess, "CREATE TABLE events (id BIGINT, user_id BIGINT)").await;
    for chunk in 0..10 {
        let mut values = String::new();
        for i in 0..200 {
            let id = chunk * 200 + i;
            // user_id ranges 0..2000 (unique per row).
            if i > 0 {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, {id})"));
        }
        exec_ok(
            &sess,
            &format!("INSERT INTO events (id, user_id) VALUES {values}"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();

    // Index does not exist yet → registry has no entry for this column.
    let reg = eng.secondary_index_registry_for_test();
    assert!(
        reg.probe(&project, &table, "user_id", "1234").is_none(),
        "registry must be empty before CREATE INDEX"
    );

    exec_ok(&sess, "CREATE INDEX events_user_id_idx ON events (user_id)").await;

    // probe_locations must now return a hit for an existing key, pointing at a
    // live file.
    let live = live_file_paths(&eng, &project, &table).await;
    assert!(!live.is_empty());
    let locs = reg
        .probe_locations(&project, &table, "user_id", "1234")
        .expect("probe_locations must find an existing key after backfill");
    assert_eq!(locs.len(), 1, "user_id=1234 occurs exactly once");
    assert!(
        live.contains(&locs[0].file_path),
        "indexed location {} must reference a live file",
        locs[0].file_path
    );

    // A never-inserted key is simply absent from the B-tree (`probe` → None).
    // The fast-select path treats "index loaded but key absent" as
    // definitely-empty; the registry itself reports None for a never-seen key.
    assert!(
        reg.probe(&project, &table, "user_id", "999999").is_none(),
        "a never-inserted key is absent from the index"
    );
    assert!(
        reg.is_loaded(&project, &table, "user_id"),
        "the column index must be loaded after backfill so absence → empty result"
    );

    // Correctness: the point SELECT returns exactly the matching row.
    assert_eq!(
        count_rows(&sess, "SELECT id FROM events WHERE user_id = 1234").await,
        1
    );
    assert_eq!(
        count_rows(&sess, "SELECT id FROM events WHERE user_id = 999999").await,
        0
    );
}

/// FIX 2(a) — compaction registers the COMPACTED file's index values.
///
/// Order matters: CREATE INDEX first (on an empty table), THEN insert more rows
/// through the shard, THEN flush. The compactor writes a new file and must
/// register its values under that file's path — proving the
/// `reindex_compacted_file_secondary` hook fires.
#[tokio::test]
async fn secondary_compaction_registers_new_file() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("accts").unwrap();

    exec_ok(&sess, "CREATE TABLE accts (id BIGINT, owner BIGINT)").await;
    // Create the index BEFORE any data exists (so backfill is a no-op and the
    // ONLY population path is compaction).
    exec_ok(&sess, "CREATE INDEX accts_owner_idx ON accts (owner)").await;

    let reg = eng.secondary_index_registry_for_test();
    assert!(
        reg.probe(&project, &table, "owner", "42").is_none(),
        "registry empty before any data"
    );

    // Insert rows through the shard (routes to WAL+tail, NOT the engine
    // index-maintenance path).
    for i in 0..500i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO accts (id, owner) VALUES ({i}, {i})"),
        )
        .await;
    }
    // Flush → compaction writes a new file and must re-index it.
    shard.flush_to_parquet().await.unwrap();

    let live = live_file_paths(&eng, &project, &table).await;
    assert!(!live.is_empty(), "compaction must have produced a file");

    // The registry must have an entry for an existing key, under a live
    // (compacted) file path.
    let locs = reg
        .probe_locations(&project, &table, "owner", "42")
        .expect("compaction must register the new file's index values");
    assert!(
        !locs.is_empty() && live.contains(&locs[0].file_path),
        "owner=42 must point at the compacted file; got {:?}",
        locs
    );

    // Correctness through the SQL path.
    assert_eq!(
        count_rows(&sess, "SELECT id FROM accts WHERE owner = 42").await,
        1
    );
    assert_eq!(
        count_rows(&sess, "SELECT id FROM accts WHERE owner = 100000").await,
        0
    );
}

/// When multiple files exist and the queried key lives in only one, the
/// secondary-index probe skips the other files — observable via the engine's
/// skip counter. This exercises the end-to-end prune (not just registry
/// population).
#[tokio::test]
async fn secondary_probe_skips_non_matching_files() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("k").unwrap();

    exec_ok(&sess, "CREATE TABLE k (id BIGINT, owner BIGINT)").await;
    for i in 0..1_000i64 {
        exec_ok(
            &sess,
            &format!("INSERT INTO k (id, owner) VALUES ({i}, {i})"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();
    exec_ok(&sess, "CREATE INDEX k_owner_idx ON k (owner)").await;

    // If the table flushed into more than one file (striping), a point query
    // for a key in one file should let the probe skip the others.
    let live = live_file_paths(&eng, &project, &table).await;
    let before = eng.secondary_index_skipped_count();
    assert_eq!(
        count_rows(&sess, "SELECT id FROM k WHERE owner = 7").await,
        1
    );
    let after = eng.secondary_index_skipped_count();

    if live.len() > 1 {
        assert!(
            after > before,
            "with {} files, the secondary index should skip the non-matching ones",
            live.len()
        );
    }
}
