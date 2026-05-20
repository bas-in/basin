//! Smoke test for ADR 0014 Phase 2: PgNode → DataFusion LogicalPlan translator.
//!
//! Verifies that:
//! 1. A simple `SELECT a FROM t WHERE b = 2` routes through the pg_plan
//!    translator (BASIN_PG_QUERY_PLAN=1) and returns the correct rows.
//! 2. `engine.pg_plan_routing_count() > 0` after the supported query.
//! 3. An unsupported query (`SELECT * FROM t GROUP BY a`) still succeeds via
//!    fallback to the existing sqlparser path, and does NOT increment the
//!    `pg_plan_routing_count` counter for that specific query.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn make_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn t_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]))
}

fn make_batch(rows: &[(i64, i64)]) -> RecordBatch {
    let a: Int64Array = rows.iter().map(|r| Some(r.0)).collect();
    let b: Int64Array = rows.iter().map(|r| Some(r.1)).collect();
    RecordBatch::try_new(t_schema(), vec![Arc::new(a), Arc::new(b)]).unwrap()
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_plan_smoke_select_with_where() {
    // Enable the Phase 2 pg_plan gate for the duration of this test.
    // SAFETY: no other thread in this test binary sets/reads the same var
    // concurrently in a way that would cause UB; std::env::set_var is safe
    // when called before any child threads access the var. We set it once
    // before spawning sessions.
    std::env::set_var("BASIN_PG_QUERY_PLAN", "1");

    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);

    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();

    // Provision the project namespace and create the table.
    engine
        .config()
        .catalog
        .create_namespace(&project)
        .await
        .unwrap();
    engine
        .config()
        .catalog
        .create_table(&project, &table, t_schema().as_ref())
        .await
        .unwrap();

    // Seed a few rows: (a=10, b=1), (a=20, b=2), (a=30, b=2), (a=40, b=3)
    let batch = make_batch(&[(10, 1), (20, 2), (30, 2), (40, 3)]);
    let df = engine
        .config()
        .storage
        .write_batch(&project, &table, &part, &batch)
        .await
        .unwrap();
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    engine
        .config()
        .catalog
        .append_data_files(
            &project,
            &table,
            meta.current_snapshot,
            vec![DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
                bloom_filters: ::std::collections::BTreeMap::new(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            }],
        )
        .await
        .unwrap();

    let sess = engine.open_session(project).await.unwrap();

    let count_before = engine.pg_plan_routing_count();

    // Run a SELECT that the Phase 2 translator supports.
    let result = sess.execute("SELECT a FROM t WHERE b = 2").await.unwrap();

    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };

    // Should have 2 rows: a=20 and a=30 (both have b=2).
    assert_eq!(
        total_rows(&batches),
        2,
        "expected 2 rows with b=2, got {}",
        total_rows(&batches)
    );
    let a_vals = {
        let mut v = col_i64(&batches, "a");
        v.sort();
        v
    };
    assert_eq!(
        a_vals,
        vec![20, 30],
        "expected a values [20, 30], got {a_vals:?}"
    );

    // The routing counter must have advanced.
    let count_after = engine.pg_plan_routing_count();
    assert!(
        count_after > count_before,
        "pg_plan_routing_count should have increased: before={count_before}, after={count_after}"
    );

    // -----------------------------------------------------------------------
    // Now run an UNSUPPORTED query (GROUP BY). It should still succeed via the
    // sqlparser fallback path, and the counter should NOT advance from this
    // query.
    // -----------------------------------------------------------------------
    let count_before_fallback = engine.pg_plan_routing_count();

    let result2 = sess
        .execute("SELECT a, COUNT(*) FROM t GROUP BY a")
        .await
        .unwrap();

    match result2 {
        ExecResult::Rows { batches, .. } => {
            // 4 distinct values of `a`, so 4 groups expected.
            assert_eq!(
                total_rows(&batches),
                4,
                "GROUP BY fallback should return 4 rows"
            );
        }
        other => panic!("expected Rows for GROUP BY fallback, got {other:?}"),
    }

    let count_after_fallback = engine.pg_plan_routing_count();
    assert_eq!(
        count_after_fallback, count_before_fallback,
        "pg_plan_routing_count should NOT advance for unsupported GROUP BY query: \
         before={count_before_fallback}, after={count_after_fallback}"
    );
}
