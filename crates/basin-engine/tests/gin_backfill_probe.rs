//! FIX 1 — CREATE INDEX ... USING gin backfills the GIN row-group / posting
//! registries over PRE-EXISTING (shard-written, compacted) data files.
//!
//! Repro of the dead-index bug: on the shard path INSERTs go to WAL+tail and
//! the engine's `maintain_secondary_indexes_on_insert` is bypassed; the GIN
//! row-group registry is populated only by compaction for files written AFTER
//! the index exists. A CREATE INDEX on a table that already has flushed data
//! therefore never indexes those files, and the read-path completeness guard
//! (`gin_rowgroup_registry().is_file_indexed`) forces a full scan forever.
//!
//! These tests build a shard-backed engine, seed JSONB rows through the shard,
//! flush them to cold-tier files, THEN `CREATE INDEX ... USING gin`, and assert
//! that the backfill sealed every live file (so the prune path engages) and the
//! `@>` query still returns correct rows.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a shard-backed engine over tempdir-backed storage + WAL.
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

/// Seed 10k JSONB rows through the shard, flush, then CREATE INDEX USING gin.
/// After the backfill every live file must be sealed in the GIN row-group
/// registry, and the `@>` query must return the correct (full) row count.
#[tokio::test]
async fn gin_create_index_backfills_preexisting_shard_files() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();

    exec_ok(&sess, "CREATE TABLE docs (id BIGINT, body JSONB)").await;

    // Seed 10k rows. Half have {\"kind\":\"a\"}, half {\"kind\":\"b\"}.
    // Batch them into a handful of multi-row INSERTs to keep the test fast.
    for chunk in 0..20 {
        let mut values = String::new();
        for i in 0..500 {
            let id = chunk * 500 + i;
            let kind = if id % 2 == 0 { "a" } else { "b" };
            if i > 0 {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, '{{\"kind\":\"{kind}\"}}')"));
        }
        exec_ok(
            &sess,
            &format!("INSERT INTO docs (id, body) VALUES {values}"),
        )
        .await;
    }

    // Flush the shard tail into cold-tier files BEFORE the index exists. This
    // is the pre-existing-data condition that triggers the dead-index bug.
    shard.flush_to_parquet().await.unwrap();

    // Sanity: data is readable and the count is right.
    assert_eq!(count_rows(&sess, "SELECT id FROM docs").await, 10_000);

    // The index does not exist yet → no live file is sealed.
    let meta_before = eng
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let live_before: Vec<String> = meta_before
        .live_data_files()
        .iter()
        .map(|f| f.path.to_string())
        .collect();
    assert!(
        !live_before.is_empty(),
        "must have flushed at least one file"
    );
    let rg = eng.gin_rowgroup_registry_for_test();
    let any_sealed_before = live_before
        .iter()
        .any(|p| rg.is_file_indexed(&project, &table, "body", p));
    assert!(
        !any_sealed_before,
        "no file should be GIN-indexed before CREATE INDEX"
    );

    // Now create the index — this must backfill every pre-existing file.
    exec_ok(&sess, "CREATE INDEX docs_body_gin ON docs USING gin (body)").await;

    // Re-load live files (CREATE INDEX does not change them) and assert that
    // EVERY one is now sealed in the GIN row-group registry — the read-path
    // completeness guard now passes, so the prune path can engage.
    let meta_after = eng
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let live_after = meta_after.live_data_files();
    assert!(!live_after.is_empty());
    for f in &live_after {
        assert!(
            rg.is_file_indexed(&project, &table, "body", f.path.as_str()),
            "file {} must be GIN-indexed after CREATE INDEX backfill",
            f.path
        );
    }

    // Correctness: the `@>` containment query returns exactly the matching
    // rows (half the table) whether or not the prune engaged.
    let matches = count_rows(
        &sess,
        "SELECT id FROM docs WHERE body @> '{\"kind\":\"a\"}'",
    )
    .await;
    assert_eq!(
        matches, 5_000,
        "@> must return all matching rows post-backfill"
    );
}

/// A query for a value that exists in only some files still returns correct
/// rows after backfill (guards against the row-group ordinal mapping dropping
/// real matches).
#[tokio::test]
async fn gin_backfill_correctness_partial_match() {
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    exec_ok(&sess, "CREATE TABLE t (id BIGINT, body JSONB)").await;
    for i in 0..2_000i64 {
        // Only id==1234 carries {\"rare\":true}.
        let body = if i == 1234 {
            "{\"rare\":true}".to_string()
        } else {
            format!("{{\"n\":{i}}}")
        };
        exec_ok(
            &sess,
            &format!("INSERT INTO t (id, body) VALUES ({i}, '{body}')"),
        )
        .await;
    }
    shard.flush_to_parquet().await.unwrap();
    exec_ok(&sess, "CREATE INDEX t_body_gin ON t USING gin (body)").await;

    let n = count_rows(&sess, "SELECT id FROM t WHERE body @> '{\"rare\":true}'").await;
    assert_eq!(n, 1, "the single rare row must survive the prune");
}
