//! Correctness tests for the low-cardinality GROUP BY fast path's SUM / AVG /
//! COUNT(col) support (`fast_aggregate::execute_groupby_low_card`).
//!
//! The fast path answers `SELECT k, COUNT(*), SUM(v), COUNT(v), AVG(v) FROM t
//! GROUP BY k` by reading only the referenced columns and hash-aggregating
//! in-process, bypassing DataFusion's planner. These tests assert the
//! fast-path answer equals the exact, hand-computed (== full-scan) answer for:
//!   * Int64 and Float64 value columns,
//!   * NULL value semantics (SUM/AVG/COUNT(col) ignore NULLs; an all-NULL
//!     group yields SUM=NULL, AVG=NULL, COUNT(col)=0, COUNT(*)=group size),
//!   * the output column names / types / nullability DataFusion 53 emits.
//!
//! A separate case forces the bail-out (key range above the low-cardinality
//! threshold) and confirms the DataFusion full scan returns the same values —
//! i.e. the fast path is a transparent optimisation, never a divergence.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::cache_defaults;
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: cache_defaults::default_test_disk_cache(),
        page_cache: cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Collect a result into `key -> (count_star, sum_i, count_i, avg)` keyed by
/// the Int64 grouping column (column 0). The remaining columns are matched by
/// the field name so the assertions don't depend on projection order.
struct Row {
    key: i64,
    cells: BTreeMap<String, Cell>,
}

#[derive(Debug, PartialEq)]
enum Cell {
    I(i64),
    F(f64),
    Null,
}

fn collect_rows(res: &ExecResult) -> Vec<Row> {
    let ExecResult::Rows { schema, batches } = res else {
        panic!("expected Rows");
    };
    let mut out = Vec::new();
    for b in batches {
        let key_arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("key col Int64");
        for i in 0..b.num_rows() {
            let mut cells = BTreeMap::new();
            for c in 1..b.num_columns() {
                let name = schema.field(c).name().clone();
                let col = b.column(c);
                let cell = if col.is_null(i) {
                    Cell::Null
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    Cell::I(a.value(i))
                } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                    Cell::F(a.value(i))
                } else {
                    panic!("unexpected col type for {name}: {:?}", col.data_type());
                };
                cells.insert(name, cell);
            }
            out.push(Row {
                key: key_arr.value(i),
                cells,
            });
        }
    }
    out.sort_by_key(|r| r.key);
    out
}

/// Seed a 6-row table with a 3-valued key, an Int64 value with one NULL, and a
/// Float64 value. Group layout (k -> rows):
///   k=1: v=(10, 20),        f=(1.5, 2.5)
///   k=2: v=(NULL, 5),       f=(NULL, 4.0)
///   k=3: v=(NULL, NULL),    f=(NULL, NULL)   ← all-NULL value group
async fn seed(sess: &basin_engine::ProjectSession) {
    sess.execute("CREATE TABLE t (k BIGINT NOT NULL, v BIGINT, f DOUBLE PRECISION)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t (k, v, f) VALUES \
            (1, 10, 1.5), \
            (1, 20, 2.5), \
            (2, NULL, NULL), \
            (2, 5, 4.0), \
            (3, NULL, NULL), \
            (3, NULL, NULL)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn groupby_sum_count_avg_with_nulls_is_exact() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute("SELECT k, COUNT(*), SUM(v), COUNT(v), AVG(v) FROM t GROUP BY k")
        .await
        .unwrap();

    // Output schema: DataFusion-equivalent names / types / nullability.
    if let ExecResult::Rows { schema, .. } = &res {
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["k", "count(*)", "sum(t.v)", "count(t.v)", "avg(t.v)"]
        );
        assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Int64);
        assert!(!schema.field(1).is_nullable()); // count(*)
        assert_eq!(schema.field(2).data_type(), &arrow_schema::DataType::Int64);
        assert!(schema.field(2).is_nullable()); // sum(int) nullable
        assert!(!schema.field(3).is_nullable()); // count(col)
        assert_eq!(
            schema.field(4).data_type(),
            &arrow_schema::DataType::Float64
        );
        assert!(schema.field(4).is_nullable()); // avg
    }

    let rows = collect_rows(&res);
    assert_eq!(rows.len(), 3);

    // k=1: 2 rows, v∈{10,20} → count*=2 sum=30 count(v)=2 avg=15.0
    assert_eq!(rows[0].key, 1);
    assert_eq!(rows[0].cells["count(*)"], Cell::I(2));
    assert_eq!(rows[0].cells["sum(t.v)"], Cell::I(30));
    assert_eq!(rows[0].cells["count(t.v)"], Cell::I(2));
    assert_eq!(rows[0].cells["avg(t.v)"], Cell::F(15.0));

    // k=2: 2 rows, v∈{NULL,5} → count*=2 sum=5 count(v)=1 avg=5.0 (NULL ignored)
    assert_eq!(rows[1].key, 2);
    assert_eq!(rows[1].cells["count(*)"], Cell::I(2));
    assert_eq!(rows[1].cells["sum(t.v)"], Cell::I(5));
    assert_eq!(rows[1].cells["count(t.v)"], Cell::I(1));
    assert_eq!(rows[1].cells["avg(t.v)"], Cell::F(5.0));

    // k=3: 2 rows, all v NULL → count*=2 sum=NULL count(v)=0 avg=NULL
    assert_eq!(rows[2].key, 3);
    assert_eq!(rows[2].cells["count(*)"], Cell::I(2));
    assert_eq!(rows[2].cells["sum(t.v)"], Cell::Null);
    assert_eq!(rows[2].cells["count(t.v)"], Cell::I(0));
    assert_eq!(rows[2].cells["avg(t.v)"], Cell::Null);
}

#[tokio::test]
async fn groupby_float_sum_avg_with_nulls_is_exact() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = sess
        .execute("SELECT k, SUM(f), AVG(f) FROM t GROUP BY k")
        .await
        .unwrap();

    if let ExecResult::Rows { schema, .. } = &res {
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["k", "sum(t.f)", "avg(t.f)"]);
        assert_eq!(
            schema.field(1).data_type(),
            &arrow_schema::DataType::Float64
        );
        assert!(schema.field(1).is_nullable());
        assert_eq!(
            schema.field(2).data_type(),
            &arrow_schema::DataType::Float64
        );
    }

    let rows = collect_rows(&res);
    assert_eq!(rows.len(), 3);
    // k=1: f∈{1.5,2.5} → sum=4.0 avg=2.0
    assert_eq!(rows[0].cells["sum(t.f)"], Cell::F(4.0));
    assert_eq!(rows[0].cells["avg(t.f)"], Cell::F(2.0));
    // k=2: f∈{NULL,4.0} → sum=4.0 avg=4.0
    assert_eq!(rows[1].cells["sum(t.f)"], Cell::F(4.0));
    assert_eq!(rows[1].cells["avg(t.f)"], Cell::F(4.0));
    // k=3: all NULL → sum=NULL avg=NULL
    assert_eq!(rows[2].cells["sum(t.f)"], Cell::Null);
    assert_eq!(rows[2].cells["avg(t.f)"], Cell::Null);
}

/// When the key range exceeds the low-cardinality threshold the fast path
/// bails and DataFusion answers the full scan. The result must be identical —
/// proving the fast path is a transparent optimisation, not a behaviour fork.
#[tokio::test]
async fn high_cardinality_key_bails_to_scan_with_same_answer() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w (k BIGINT NOT NULL, v BIGINT NOT NULL)")
        .await
        .unwrap();
    // Keys spread far beyond the 32-value low-card threshold (k = id*1000),
    // forcing the cardinality gate to bail to DataFusion.
    let mut vals = Vec::new();
    for i in 0..50i64 {
        vals.push(format!("({}, {})", i * 1000, i));
    }
    sess.execute(&format!("INSERT INTO w (k, v) VALUES {}", vals.join(", ")))
        .await
        .unwrap();

    let res = sess
        .execute("SELECT k, SUM(v), COUNT(*) FROM w GROUP BY k")
        .await
        .unwrap();
    let rows = collect_rows(&res);
    assert_eq!(rows.len(), 50);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r.key, i as i64 * 1000);
        assert_eq!(r.cells["sum(w.v)"], Cell::I(i as i64));
        assert_eq!(r.cells["count(*)"], Cell::I(1));
    }
}
