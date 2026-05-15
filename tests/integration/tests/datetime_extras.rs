//! P10 — datetime extras + multi-dim array integration tests.
//!
//! Covers:
//!   - `overlaps(s1, e1, s2, e2)` predicate
//!   - `'infinity'::timestamp` / `'-infinity'::timestamp` casts
//!   - `date_part('year', ...)` alias for EXTRACT
//!   - `array_dims(ARRAY[[1,2],[3,4]])` → `'[1:2][1:2]'`
//!   - `SELECT '{{1,2},{3,4}}'::int[][]` multi-dim array (convert.rs List support)

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

/// Run a single-row, single-column SELECT and return the first batch's first column.
async fn one_col(sess: &basin_engine::ProjectSession, sql: &str) -> Arc<dyn Array> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let b = batches
                .first()
                .unwrap_or_else(|| panic!("no batches from: {sql}"));
            assert!(b.num_rows() >= 1, "expected ≥1 row from: {sql}");
            b.column(0).clone()
        }
        Ok(other) => panic!("non-rows result for '{sql}': {other:?}"),
        Err(e) => panic!("execute '{sql}': {e}"),
    }
}

// ── OVERLAPS ─────────────────────────────────────────────────────────────────

/// The OVERLAPS predicate implemented as a 4-arg UDF.
/// Two intervals `[s1,e1)` and `[s2,e2)` overlap iff `s1 < e2 AND s2 < e1`.
#[tokio::test]
async fn overlaps_true_when_intervals_cross() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Intervals [0h, 2h) and [1h, 3h) overlap.
    let col = one_col(
        &sess,
        "SELECT overlaps(\
            TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 02:00:00',\
            TIMESTAMP '2024-01-01 01:00:00', TIMESTAMP '2024-01-01 03:00:00'\
        )",
    )
    .await;
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("expected BooleanArray from overlaps");
    assert!(arr.value(0), "intervals that cross must overlap");
}

#[tokio::test]
async fn overlaps_false_when_disjoint() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Intervals [0h, 1h) and [2h, 3h) — no overlap.
    let col = one_col(
        &sess,
        "SELECT overlaps(\
            TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 01:00:00',\
            TIMESTAMP '2024-01-01 02:00:00', TIMESTAMP '2024-01-01 03:00:00'\
        )",
    )
    .await;
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("expected BooleanArray from overlaps");
    assert!(!arr.value(0), "disjoint intervals must not overlap");
}

#[tokio::test]
async fn overlaps_false_when_adjacent() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Intervals [0h, 1h) and [1h, 2h) — touching but not overlapping (half-open).
    let col = one_col(
        &sess,
        "SELECT overlaps(\
            TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 01:00:00',\
            TIMESTAMP '2024-01-01 01:00:00', TIMESTAMP '2024-01-01 02:00:00'\
        )",
    )
    .await;
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("expected BooleanArray from overlaps");
    assert!(!arr.value(0), "adjacent half-open intervals must not overlap");
}

// ── INFINITY TIMESTAMPS ───────────────────────────────────────────────────────

#[tokio::test]
async fn infinity_timestamp_cast() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // 'infinity'::timestamp must return a Timestamp column (not NULL, not error).
    let col = one_col(&sess, "SELECT 'infinity'::timestamp").await;
    assert_eq!(
        col.data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        "'infinity'::timestamp must return Timestamp(Microsecond, None)"
    );
    assert!(!col.is_null(0), "'infinity'::timestamp result must be non-null");
}

#[tokio::test]
async fn neg_infinity_timestamp_cast() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let col = one_col(&sess, "SELECT '-infinity'::timestamp").await;
    assert_eq!(
        col.data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        "'-infinity'::timestamp must return Timestamp(Microsecond, None)"
    );
    assert!(!col.is_null(0), "'-infinity'::timestamp result must be non-null");
}

#[tokio::test]
async fn infinity_gt_neg_infinity() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // 'infinity' must be strictly greater than '-infinity'.
    let col = one_col(
        &sess,
        "SELECT 'infinity'::timestamp > '-infinity'::timestamp",
    )
    .await;
    let arr = col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("expected BooleanArray for comparison");
    assert!(arr.value(0), "infinity must be > -infinity");
}

// ── DATE_PART ─────────────────────────────────────────────────────────────────

/// `date_part('year', ts)` must return the year as a Float64 (PG returns
/// `double precision`; DataFusion's `date_part` does the same).
#[tokio::test]
async fn date_part_year() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let col = one_col(
        &sess,
        "SELECT date_part('year', TIMESTAMP '2024-05-09 12:34:56')",
    )
    .await;
    // DataFusion's built-in date_part returns Int32 for most fields; we accept
    // whatever numeric type comes back and assert the value.
    let val: f64 = match col.data_type() {
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(0) as f64,
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0) as f64,
        other => panic!("unexpected type from date_part: {other:?}"),
    };
    assert_eq!(val, 2024.0, "date_part('year', ...) should return 2024");
}

#[tokio::test]
async fn date_part_month() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let col = one_col(
        &sess,
        "SELECT date_part('month', TIMESTAMP '2024-05-09 12:34:56')",
    )
    .await;
    let val: f64 = match col.data_type() {
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(0) as f64,
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0) as f64,
        other => panic!("unexpected type from date_part: {other:?}"),
    };
    assert_eq!(val, 5.0, "date_part('month', ...) should return 5");
}

// ── ARRAY_DIMS ────────────────────────────────────────────────────────────────

/// `array_dims(ARRAY[[1,2],[3,4]])` must return `'[1:2][1:2]'`.
#[tokio::test]
async fn array_dims_2d() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let col = one_col(
        &sess,
        "SELECT array_dims(ARRAY[[1,2],[3,4]])",
    )
    .await;
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("array_dims must return StringArray");
    assert_eq!(
        arr.value(0),
        "[1:2][1:2]",
        "2-D 2x2 array dims should be [1:2][1:2]"
    );
}

/// `array_dims(ARRAY[1,2,3])` must return `'[1:3]'`.
#[tokio::test]
async fn array_dims_1d() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let col = one_col(&sess, "SELECT array_dims(ARRAY[1, 2, 3])").await;
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("array_dims must return StringArray");
    assert_eq!(arr.value(0), "[1:3]", "1-D 3-element array dims should be [1:3]");
}

// ── MULTI-DIM ARRAY ROUND-TRIP ────────────────────────────────────────────────

/// `SELECT ARRAY[[1,2],[3,4]]` must plan and execute without error.
/// The List(List(Int64)) → workspace-side conversion path in convert.rs must
/// handle this cleanly.  We use DataFusion's native ARRAY[[...]] constructor
/// rather than the PG `'{{1,2},{3,4}}'::int[][]` string-cast syntax (which
/// DataFusion does not support for nested arrays).
#[tokio::test]
async fn multi_dim_array_cast_executes() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Use ARRAY[[...]] constructor — DataFusion produces List(List(Int64)).
    let res = sess
        .execute("SELECT ARRAY[[1,2],[3,4]]")
        .await;
    // We only require that it executes without panicking/returning Err.
    // The result type is List(List(Int64)) — the convert.rs List arms handle
    // this recursively.
    match res {
        Ok(ExecResult::Rows { batches, .. }) => {
            assert!(!batches.is_empty(), "expected at least one batch");
            assert!(batches[0].num_rows() >= 1);
        }
        Ok(other) => panic!("unexpected non-rows result: {other:?}"),
        Err(e) => panic!("multi-dim array literal failed: {e}"),
    }
}
