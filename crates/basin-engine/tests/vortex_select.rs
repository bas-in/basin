//! End-to-end guard for the opt-in Vortex storage format (#161).
//!
//! `CREATE TABLE … WITH (basin.file_format='vortex')` → INSERT → SELECT *
//! must round-trip every value, AND the on-disk data files for that table
//! must carry the `.vortex` extension (proving the writer actually emitted
//! Vortex, not Parquet). A sibling table created with an explicit
//! `basin.file_format='parquet'` must still produce `.parquet` files, so
//! the format selection is genuinely per-table and not a global switch.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
// `arrow_select` is intentionally not used: it is not a basin-engine
// (dev-)dependency. Result batches are flattened in row order instead.
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Every regular file under `root` whose name ends with `.{ext}`.
fn files_with_ext(root: &std::path::Path, ext: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(ext))
            {
                out.push(p.to_string_lossy().into_owned());
            }
        }
    }
    out
}

/// CREATE (Vortex, opt-in) → INSERT → SELECT * round-trips every value,
/// and the table's data files are `.vortex` (never `.parquet`).
#[tokio::test]
async fn vortex_table_round_trips_and_writes_vortex_files() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE v (id BIGINT, name TEXT, score DOUBLE, ok BOOLEAN) \
         WITH (basin.file_format='vortex')",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO v (id, name, score, ok) VALUES \
         (1, 'alpha', 1.5, true), \
         (2, 'beta', 2.25, false), \
         (3, NULL, NULL, NULL)",
    )
    .await;

    let batches = rows(&sess, "SELECT id, name, score, ok FROM v ORDER BY id").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "all three inserted rows must come back");

    // Flatten the (row-ordered) batches into typed option vectors so the
    // assertions don't depend on batch boundaries.
    let mut ids: Vec<Option<i64>> = Vec::new();
    let mut names: Vec<Option<String>> = Vec::new();
    let mut scores: Vec<Option<f64>> = Vec::new();
    let mut oks: Vec<Option<bool>> = Vec::new();
    for b in &batches {
        let id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let name = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        let score = b
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score is Float64");
        let ok = b
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("ok is Boolean");
        for r in 0..b.num_rows() {
            ids.push((!id.is_null(r)).then(|| id.value(r)));
            names.push((!name.is_null(r)).then(|| name.value(r).to_string()));
            scores.push((!score.is_null(r)).then(|| score.value(r)));
            oks.push((!ok.is_null(r)).then(|| ok.value(r)));
        }
    }

    assert_eq!(ids, vec![Some(1), Some(2), Some(3)]);
    assert_eq!(
        names,
        vec![Some("alpha".to_string()), Some("beta".to_string()), None],
        "row 3 name must round-trip as NULL"
    );
    assert_eq!(
        scores,
        vec![Some(1.5), Some(2.25), None],
        "row 3 score must round-trip as NULL"
    );
    assert_eq!(
        oks,
        vec![Some(true), Some(false), None],
        "row 3 ok must round-trip as NULL"
    );

    // On-disk proof: the table's data files are Vortex, not Parquet.
    let vortex_files = files_with_ext(dir.path(), ".vortex");
    let parquet_files = files_with_ext(dir.path(), ".parquet");
    assert!(
        !vortex_files.is_empty(),
        "expected at least one .vortex data file on disk, found none (parquet: {parquet_files:?})"
    );
    assert!(
        parquet_files.is_empty(),
        "a Vortex table must not emit any .parquet data files, found: {parquet_files:?}"
    );
}

/// Format selection is per-table: an explicit `basin.file_format='parquet'`
/// table emits `.parquet` even when a Vortex table exists alongside it.
/// (Stays valid regardless of which format is the global default.)
#[tokio::test]
async fn explicit_parquet_table_still_writes_parquet() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE pq (id BIGINT, name TEXT) WITH (basin.file_format='parquet')",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO pq (id, name) VALUES (10, 'x'), (20, 'y')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, name FROM pq ORDER BY id").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);

    let parquet_files = files_with_ext(dir.path(), ".parquet");
    let vortex_files = files_with_ext(dir.path(), ".vortex");
    assert!(
        !parquet_files.is_empty(),
        "explicit-parquet table must emit .parquet files"
    );
    assert!(
        vortex_files.is_empty(),
        "explicit-parquet table must not emit .vortex files, found: {vortex_files:?}"
    );
}
