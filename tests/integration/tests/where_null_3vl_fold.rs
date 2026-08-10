//! `WHERE <col> = NULL` (and its kin) must plan-time fold to an empty
//! result under SQL three-valued logic — without touching storage.
//!
//! Background (commit ddfd8a8 bench): the `WHERE x = NULL returns 0 (3VL)`
//! shape was 180-300x slower than PG because the engine fell through to
//! DataFusion (the simple-select recogniser rejected the NULL literal in
//! `literal_value`), and DF then evaluated `x = NULL` per row over the
//! full table. PG, by contrast, recognises the tautology at plan time
//! and returns instantly.
//!
//! Fix (in `fast_select.rs`): `is_3vl_false_atom` detects the four shape
//! variants — `col = NULL`, `NULL = col`, `col <> NULL`, `NULL <> col` —
//! and sets `SimpleSelectPlan.always_empty = true`. `execute_simple_select`
//! short-circuits to an empty result without consulting storage.
//!
//! The fix is structural / scalable: the cost is O(1) regardless of table
//! size. These tests assert:
//!   1. row-returning shape returns zero rows (independent of data),
//!   2. aggregate shape returns the empty-relation answer (COUNT(*)=0,
//!      MIN/MAX/SUM=NULL),
//!   3. `<col> <> NULL` and the `NULL = <col>` mirror form fold identically,
//!   4. `col = NULL AND <other>` is still empty (3VL: F ∧ X ≡ F).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

async fn seed_table(sess: &basin_engine::ProjectSession, n: i64) {
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let values: Vec<String> = (1..=n).map(|k| format!("({k},{k})")).collect();
    sess.execute(&format!(
        "INSERT INTO t (id, v) VALUES {}",
        values.join(",")
    ))
    .await
    .unwrap();
}

fn row_total(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn first_i64(batches: &[RecordBatch], col_idx: usize) -> Option<i64> {
    let b = batches.first()?;
    let arr = b.column(col_idx).as_any().downcast_ref::<Int64Array>()?;
    if arr.is_null(0) {
        None
    } else {
        Some(arr.value(0))
    }
}

#[tokio::test]
async fn col_eq_null_returns_empty_rows_without_scan() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 1_000).await;

    let res = sess
        .execute("SELECT id, v FROM t WHERE v = NULL")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        row_total(&batches),
        0,
        "v = NULL must return zero rows under 3VL"
    );
}

#[tokio::test]
async fn null_eq_col_mirror_form_also_folds() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 1_000).await;

    let res = sess
        .execute("SELECT id FROM t WHERE NULL = v")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        row_total(&batches),
        0,
        "NULL = v must fold the same as v = NULL"
    );
}

#[tokio::test]
async fn col_ne_null_returns_empty_rows() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 1_000).await;

    let res = sess
        .execute("SELECT id FROM t WHERE v <> NULL")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        row_total(&batches),
        0,
        "v <> NULL must return zero rows under 3VL"
    );
}

#[tokio::test]
async fn col_eq_null_combined_with_and_still_empty() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 1_000).await;

    let res = sess
        .execute("SELECT id FROM t WHERE v = NULL AND id > 100")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        row_total(&batches),
        0,
        "F AND X must still be F: the matching id > 100 atom doesn't rescue v = NULL"
    );
}

#[tokio::test]
async fn aggregate_under_null_eq_returns_empty_relation_answer() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 1_000).await;

    // COUNT(*) over zero rows = 0; MIN/MAX/SUM = NULL.
    let res = sess
        .execute("SELECT COUNT(*), MIN(v), MAX(v), SUM(v) FROM t WHERE v = NULL")
        .await
        .unwrap();
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(batches.len(), 1, "aggregate result is exactly one batch");
    assert_eq!(batches[0].num_rows(), 1, "one row of aggregates");
    // count is non-null Int64 with value 0; min/max/sum are NULL.
    let count = first_i64(&batches, 0).expect("count(*) is non-null");
    assert_eq!(count, 0);
    assert!(
        first_i64(&batches, 1).is_none(),
        "MIN over empty must be NULL"
    );
    assert!(
        first_i64(&batches, 2).is_none(),
        "MAX over empty must be NULL"
    );
    assert!(
        first_i64(&batches, 3).is_none(),
        "SUM over empty must be NULL"
    );
}

/// Schema-shape assertion: even though no row is returned, the output
/// schema must match the projection (so client libraries can describe the
/// row type). For `SELECT id FROM t WHERE v = NULL` the projected schema
/// must contain a single `id` column of Int64.
#[tokio::test]
async fn schema_of_empty_result_matches_projection() {
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess, 10).await;

    let res = sess
        .execute("SELECT id FROM t WHERE v = NULL")
        .await
        .unwrap();
    let schema = match res {
        ExecResult::Rows { schema, .. } => schema,
        other => panic!("expected Rows, got {other:?}"),
    };
    let expected: Arc<Schema> =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    assert_eq!(
        schema.fields().len(),
        1,
        "exactly one column in the projected schema"
    );
    assert_eq!(schema.field(0).name(), expected.field(0).name());
    assert_eq!(schema.field(0).data_type(), expected.field(0).data_type());
}
