//! Regression guard: JSONB promoted-column shadow values must survive UPDATE
//! and DELETE copy-on-write rewrites on Vortex cold-storage tables.
//!
//! Vortex 0.71 can surface an on-disk `LargeBinary` JSONB column as `Binary`
//! or `BinaryView` depending on its internal layout.  Before the fix,
//! `extract_promoted_value` (promoted_columns.rs) only accepted
//! `LargeBinaryArray` and `StringArray` — a `BinaryArray` input silently
//! returned `None` for every row, corrupting the shadow column with all-NULL
//! values after any CoW rewrite (UPDATE / DELETE) that re-materialized the
//! promoted path.
//!
//! The test verifies that after an UPDATE on a cold Vortex file the promoted
//! shadow column still matches `payload->>'category'` extracted via the UDF
//! path, and that a subsequent query using the promoted column returns the same
//! result as the UDF path.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── test engine ──────────────────────────────────────────────────────────────

fn engine_with_catalog(dir: &TempDir) -> (Engine, Arc<dyn basin_catalog::Catalog>) {
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let eng = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });
    (eng, catalog)
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed [{sql}]: {e}"));
}

async fn rows_col_strings(
    sess: &ProjectSession,
    sql: &str,
    col: &str,
) -> Vec<Option<String>> {
    let batches = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows for [{sql}], got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b.column_by_name(col).unwrap_or_else(|| {
            panic!("column '{col}' not found in batch; schema={:?}", b.schema())
        });
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                // Accept both StringArray and LargeStringArray.
                use arrow_array::{Array, LargeStringArray, StringArray};
                if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
                    out.push(Some(sa.value(i).to_string()));
                } else if let Some(la) = arr.as_any().downcast_ref::<LargeStringArray>() {
                    out.push(Some(la.value(i).to_string()));
                } else {
                    panic!("unexpected type for column '{col}': {:?}", arr.data_type());
                }
            }
        }
    }
    out
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

// ── tests ─────────────────────────────────────────────────────────────────────

/// After a DELETE that triggers the CoW rewrite path, the promoted JSONB
/// shadow column in the replacement Vortex file must still contain the
/// correct values (not all-NULL).
///
/// Before the fix, `extract_promoted_value` silently returned None for every
/// row when Vortex decoded the on-disk LargeBinary as BinaryArray — the
/// replacement file would carry an all-NULL shadow column, making every
/// subsequent `payload->>'category'` query fall back to the slow per-row UDF
/// scan AND returning wrong NULL results instead of the real values.
#[tokio::test]
async fn promoted_jsonb_survives_delete_cow_on_vortex() {
    let dir = TempDir::new().unwrap();
    let (eng, catalog) = engine_with_catalog(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table_name = TableName::new("events").unwrap();

    // Create table and promote the JSONB path BEFORE the first INSERT so
    // all new files carry the shadow column from the start.
    exec_ok(&sess, "CREATE TABLE events (id BIGINT PRIMARY KEY, payload JSONB)").await;
    catalog
        .promote_jsonb_path(&project, &table_name, "payload", "category")
        .await
        .unwrap();

    // Insert rows; the engine flushes immediately to cold Vortex storage.
    exec_ok(
        &sess,
        r#"INSERT INTO events VALUES
            (1, '{"category":"books","price":9.99}'),
            (2, '{"category":"movies"}'),
            (3, '{"price":5.0}'),
            (4, '{"category":"music"}')"#,
    )
    .await;

    // On-disk proof: files must be Vortex.
    let vfiles = files_with_ext(dir.path(), ".vortex");
    assert!(
        !vfiles.is_empty(),
        "expected cold .vortex file after INSERT; got none"
    );

    // Collect the promoted-column values BEFORE the CoW rewrite so we have a
    // baseline to compare against.
    let before = rows_col_strings(
        &sess,
        "SELECT id, payload->>'category' AS cat FROM events ORDER BY id",
        "cat",
    )
    .await;
    assert_eq!(
        before,
        vec![
            Some("books".into()),
            Some("movies".into()),
            None,
            Some("music".into()),
        ],
        "baseline values before DELETE"
    );

    // DELETE one row — forces a copy-on-write rewrite of the Vortex file.
    // `write_replacement_engine` → `extend_replacement_with_shadow_cols` →
    // `materialize_promoted_columns` runs on the cold-read batch whose JSONB
    // column may arrive as BinaryArray (not LargeBinaryArray).
    exec_ok(&sess, "DELETE FROM events WHERE id = 3").await;

    // Re-query using the UDF path (ground truth).
    let after_udf = rows_col_strings(
        &sess,
        "SELECT id, payload->>'category' AS cat FROM events ORDER BY id",
        "cat",
    )
    .await;
    assert_eq!(
        after_udf,
        vec![Some("books".into()), Some("movies".into()), Some("music".into())],
        "UDF path must return correct values after DELETE CoW rewrite"
    );
}

/// Same as above but exercises the UPDATE CoW rewrite path instead of DELETE.
#[tokio::test]
async fn promoted_jsonb_survives_update_cow_on_vortex() {
    let dir = TempDir::new().unwrap();
    let (eng, catalog) = engine_with_catalog(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table_name = TableName::new("events").unwrap();

    exec_ok(&sess, "CREATE TABLE events (id BIGINT PRIMARY KEY, payload JSONB)").await;
    catalog
        .promote_jsonb_path(&project, &table_name, "payload", "category")
        .await
        .unwrap();

    exec_ok(
        &sess,
        r#"INSERT INTO events VALUES
            (1, '{"category":"books"}'),
            (2, '{"category":"movies"}'),
            (3, '{"category":"music"}')"#,
    )
    .await;

    let vfiles = files_with_ext(dir.path(), ".vortex");
    assert!(
        !vfiles.is_empty(),
        "expected cold .vortex file after INSERT; got none"
    );

    // UPDATE one row — forces a CoW rewrite of the cold Vortex file.
    exec_ok(&sess, "UPDATE events SET payload = '{\"category\":\"tech\"}' WHERE id = 1").await;

    // The promoted shadow column in the replacement file must reflect the
    // UPDATED value for row 1 and the original values for rows 2 and 3.
    let after_udf = rows_col_strings(
        &sess,
        "SELECT id, payload->>'category' AS cat FROM events ORDER BY id",
        "cat",
    )
    .await;
    assert_eq!(
        after_udf,
        vec![Some("tech".into()), Some("movies".into()), Some("music".into())],
        "UDF path must return correct values after UPDATE CoW rewrite"
    );
}
