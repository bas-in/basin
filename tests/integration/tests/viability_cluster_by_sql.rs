//! Viability test: SQL surface for `CLUSTER BY` (Phase 5.7 B2).
//!
//! Card: `viability_cluster_by_sql`
//! Bar: every supported `CLUSTER BY` SQL form parses, runs, persists to
//!      the catalog, and (for the e2e leg) the resulting Parquet file is
//!      physically sorted by the cluster columns.
//!
//! Forms exercised:
//!
//! ```sql
//! CREATE TABLE foo (id BIGINT, ts BIGINT) CLUSTER BY (id, ts);
//! ALTER TABLE foo CLUSTER BY (ts);
//! ALTER TABLE foo RESET CLUSTER BY;
//! ```
//!
//! After each statement we re-`load_table` and assert `cluster_columns`
//! matches what the SQL claimed. The e2e leg writes rows in random order,
//! reads them back via `SELECT *`, and asserts the rows come out sorted
//! by `id` — the writer's `lexsort_to_indices` + `take` make every
//! flushed row group physically sorted, so a single-file scan is in
//! cluster-column order.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn viability_cluster_by_sql() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();

    let mut variants_passed: Vec<&'static str> = Vec::new();

    // 1) CREATE TABLE … CLUSTER BY (...). The trailing clause is stripped
    //    pre-parse, the table is created, then `set_cluster_columns` is
    //    invoked with the extracted column list.
    sess.execute(
        "CREATE TABLE foo (id BIGINT NOT NULL, ts BIGINT NOT NULL) CLUSTER BY (id, ts)",
    )
    .await
    .expect("CREATE TABLE … CLUSTER BY (...)");
    let table = TableName::new("foo").unwrap();
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(m.cluster_columns, vec!["id".to_string(), "ts".to_string()]);
    variants_passed.push("CREATE TABLE … CLUSTER BY (...)");

    // 2) ALTER TABLE … CLUSTER BY (...). Replaces the cluster spec.
    sess.execute("ALTER TABLE foo CLUSTER BY (ts)")
        .await
        .expect("ALTER TABLE … CLUSTER BY (...)");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(m.cluster_columns, vec!["ts".to_string()]);
    variants_passed.push("ALTER TABLE … CLUSTER BY (...)");

    // 3) ALTER TABLE … RESET CLUSTER BY. Clears the spec.
    sess.execute("ALTER TABLE foo RESET CLUSTER BY")
        .await
        .expect("ALTER TABLE … RESET CLUSTER BY");
    let m = catalog.load_table(&tenant, &table).await.unwrap();
    assert!(
        m.cluster_columns.is_empty(),
        "cluster_columns should be empty after RESET, got {:?}",
        m.cluster_columns
    );
    variants_passed.push("ALTER TABLE … RESET CLUSTER BY");

    // 4) Reject CLUSTER BY column not in the table schema. Catches the
    //    common typo before we silently produce unsorted Parquet files.
    let res = sess
        .execute(
            "CREATE TABLE bar (a BIGINT NOT NULL) CLUSTER BY (does_not_exist)",
        )
        .await;
    assert!(
        res.is_err(),
        "CLUSTER BY on a non-existent column should be rejected"
    );
    variants_passed.push("CLUSTER BY rejects unknown column");

    // 5) End-to-end: write rows in shuffled order, read them back, assert
    //    they come out sorted by the cluster column. The writer's lexsort
    //    runs on every Parquet flush so within a single row group the
    //    rows are in cluster-column order. A single-batch INSERT lands
    //    in one file, so the SELECT is in physical (sorted) order.
    sess.execute(
        "CREATE TABLE bench (id BIGINT NOT NULL, payload TEXT NOT NULL) CLUSTER BY (id)",
    )
    .await
    .expect("CREATE TABLE bench CLUSTER BY (id)");
    let bench_table = TableName::new("bench").unwrap();
    let m = catalog.load_table(&tenant, &bench_table).await.unwrap();
    assert_eq!(m.cluster_columns, vec!["id".to_string()]);

    // Insert 8 rows in deliberately shuffled order. The catalog reads
    // `cluster_columns = [id]`, the writer sorts the batch by id before
    // flushing, and the file is physically id-sorted on disk.
    sess.execute(
        "INSERT INTO bench VALUES \
         (5, 'e'), (3, 'c'), (7, 'g'), (1, 'a'), \
         (8, 'h'), (4, 'd'), (2, 'b'), (6, 'f')",
    )
    .await
    .expect("INSERT shuffled rows");

    let res = sess
        .execute("SELECT id FROM bench")
        .await
        .expect("SELECT id FROM bench");
    let ids = collect_int64_column(&res, "id");
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        "rows should come back physically sorted by cluster column"
    );
    variants_passed.push("e2e: cluster-by sorts file physically");

    let pass = variants_passed.len() == 5;
    println!(
        "[VIABILITY CLUSTER BY SQL] {} of 5 variants passed: {:?}",
        variants_passed.len(),
        variants_passed
    );

    report_viability(
        "cluster_by_sql",
        "CLUSTER BY SQL surface (Phase 5.7 B2)",
        "Every CLUSTER BY form (CREATE TABLE … CLUSTER BY, ALTER TABLE … CLUSTER BY, \
         ALTER TABLE … RESET CLUSTER BY) parses, persists to the catalog, validates \
         column membership, and the writer physically sorts flushed batches.",
        pass,
        PrimaryMetric {
            label: "cluster_by_variants_passed".into(),
            value: variants_passed.len() as f64,
            unit: "variants".into(),
            bar: BarOp::eq(5.0),
        },
        json!({
            "variants_passed": variants_passed,
        }),
    );

    assert!(pass, "expected all 5 CLUSTER BY variants to pass; got {variants_passed:?}");
}

/// Pull a single Int64 column from an `ExecResult::Rows` batch list, in
/// row-order. Helper kept local to this test — the integration crate
/// doesn't expose a generic shape for this.
fn collect_int64_column(res: &ExecResult, col_name: &str) -> Vec<i64> {
    let (schema, batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        _ => panic!("expected ExecResult::Rows, got {res:?}"),
    };
    let idx = schema
        .index_of(col_name)
        .unwrap_or_else(|_| panic!("column {col_name} not in schema"));
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64 column");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}
