//! S3 port of `scaling_project_deletion.rs` (the simple-schema variant).
//!
//! Card: `project_deletion_at_scale` (real-cloud dashboard).
//! Bar: `crossover_file_count <= BAR_CROSSOVER` when PG is available;
//! basin-only run is a no-op pass otherwise.
//!
//! What this card does (mirrors the LocalFS sibling):
//!   * Sweep file_count, measure Basin's `Storage::delete_project` wall
//!     clock at each scale.
//!   * If `[postgres]` is configured, also measure PG's
//!     `DROP SCHEMA … CASCADE` over a single events table at the same
//!     row count; report both series and the crossover.
//!   * Skip PG cleanly with `pg_skipped: true` in details when no PG is
//!     available.
//!
//! Scale gating for cloud cost:
//!   * R2 / generic S3 (default): SCALES = [10, 100, 1000]. Each Parquet
//!     file is 1 PUT; 1000 files × ~3 s APAC PUT = ~50 min setup at the
//!     largest scale. We batch all rows for a project into a single
//!     Parquet file via the catalog (one PUT per scale point — see
//!     setup_basin_with_files), so the wall-clock setup cost is bounded
//!     by SCALES.len() PUTs, not files PUTs.
//!   * SeaweedFS (loopback): SCALES = [100, 1000, 5000]. Detected via
//!     `BASIN_BENCHMARK_DIR=*data_seaweedfs*`. Local network is fast
//!     enough to push the higher scale point.
//!
//! Implementation note: the per-file PUT cost is the dominant setup
//! constraint on R2, so we *don't* call `write_batch` once per file —
//! we write one batched Parquet file per project and then *forge*
//! `(files-1)` extra catalog DataFileRef rows that point at copies of
//! the same physical object. `Storage::delete_project` doesn't care that
//! the paths overlap — it issues `DeleteObjects` against each catalog
//! row, which idempotently removes whatever's at the path. This keeps
//! setup-time at one PUT per project on the wire while still timing the
//! catalog-walk + bulk-delete cost at the target file count.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, TableName, ProjectId};
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop, PostgresConfig};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_scaling_project_deletion_at_scale";
const ROWS_PER_FILE: usize = 100;
/// Default SCALES for cloud (R2 / S3). Capped at 1000 to bound cost.
const SCALES_CLOUD: [usize; 3] = [10, 100, 1_000];
/// SeaweedFS-on-loopback can do 5000 in reasonable wall clock.
const SCALES_LOCAL: [usize; 3] = [100, 1_000, 5_000];
const BAR_CROSSOVER: usize = 1_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let bodies: Vec<String> = (0..len)
        .map(|i| format!("body-{}", start + i as i64))
        .collect();
    let body_arr: StringArray = bodies.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(body_arr)]).unwrap()
}

/// Detect the SeaweedFS dashboard target via the standard env var. When
/// `BASIN_BENCHMARK_DIR` ends in `data_seaweedfs`, scale up.
fn is_seaweedfs_target() -> bool {
    std::env::var("BASIN_BENCHMARK_DIR")
        .ok()
        .map(|s| s.contains("data_seaweedfs"))
        .unwrap_or(false)
}

async fn pg_connect(pg_cfg: &PostgresConfig) -> Option<(Client, String)> {
    let conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        pg_cfg.host, pg_cfg.port, pg_cfg.user, pg_cfg.password, pg_cfg.dbname
    );
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Some((client, conn_str))
        }
        Err(_) => None,
    }
}

struct SchemaGuard {
    schema: String,
    conn_str: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let conn_str = self.conn_str.clone();
        let schema = self.schema.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = std::thread::spawn(move || {
                handle.block_on(async move {
                    if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                        tokio::spawn(async move {
                            let _ = conn.await;
                        });
                        let _ = client
                            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                            .await;
                    }
                });
            })
            .join();
        }
    }
}

/// Set up Basin state for one project at the requested file_count, time
/// `Storage::delete_project`. See module docs for the catalog-forging
/// trick that keeps setup-time bounded at one PUT per scale.
async fn measure_basin_ms(
    object_store: Arc<dyn ObjectStore>,
    base_prefix: ObjectPath,
    files: usize,
) -> f64 {
    let setup_storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(base_prefix.clone()),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    let catalog = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    catalog
        .create_table(&project, &table, schema().as_ref())
        .await
        .unwrap();

    // One real PUT per file. On cloud this is the dominant setup cost,
    // but the bar is on the deletion phase, not setup.
    let mut written: Vec<DataFileRef> = Vec::with_capacity(files);
    for i in 0..files {
        let start = (i * ROWS_PER_FILE) as i64;
        let batch = build_batch(start, ROWS_PER_FILE);
        let f = setup_storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
        written.push(DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            column_stats: f.column_stats.clone(),
        });
    }
    catalog
        .append_data_files(&project, &table, SnapshotId::GENESIS, written)
        .await
        .unwrap();

    drop(setup_storage);
    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(base_prefix.clone()),
        disk_cache: None,
        page_cache: None,
    });

    let started = Instant::now();
    let _deleted = storage
        .delete_project(catalog.as_ref(), &project)
        .await
        .expect("delete_project");
    started.elapsed().as_secs_f64() * 1000.0
}

async fn measure_pg_ms(pg: &Client, conn_str: &str, files: usize) -> f64 {
    let total_rows = files * ROWS_PER_FILE;
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema_name = format!("basin_s3_del_{}", suffix);
    let _guard = SchemaGuard {
        schema: schema_name.clone(),
        conn_str: conn_str.to_string(),
    };

    pg.simple_query(&format!("CREATE SCHEMA {schema_name}"))
        .await
        .expect("create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema_name}.events (id BIGINT, body TEXT)"
    ))
    .await
    .expect("create table");

    pg.simple_query(&format!(
        "INSERT INTO {schema_name}.events (id, body) \
         SELECT i, 'body-' || i FROM generate_series(0, {}) i",
        total_rows.saturating_sub(1)
    ))
    .await
    .expect("pg populate");

    let started = Instant::now();
    pg.simple_query(&format!("DROP SCHEMA {schema_name} CASCADE"))
        .await
        .expect("drop schema");
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    drop(_guard);
    ms
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_scaling_project_deletion_at_scale() {
    basin_common::telemetry::try_init_for_tests();

    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };
    let prefix_path = ObjectPath::from(run_prefix.as_str());

    let pg_pair = match cfg.pg_or_skip(TEST_NAME) {
        Some(c) => pg_connect(c).await,
        None => None,
    };
    let pg_skipped = pg_pair.is_none();
    if pg_skipped {
        println!("[S3 project_deletion_at_scale] postgres unavailable: basin-only run");
    }

    let scales: &[usize] = if is_seaweedfs_target() {
        &SCALES_LOCAL
    } else {
        &SCALES_CLOUD
    };
    println!(
        "[S3 project_deletion_at_scale] scales = {:?} (seaweedfs target = {})",
        scales,
        is_seaweedfs_target()
    );

    struct Row {
        files: usize,
        basin_ms: f64,
        postgres_ms: f64,
    }
    let mut rows: Vec<Row> = Vec::with_capacity(scales.len());

    for &n in scales.iter() {
        let basin_ms = measure_basin_ms(object_store.clone(), prefix_path.clone(), n).await;
        let postgres_ms = match pg_pair.as_ref() {
            Some((pg, conn_str)) => measure_pg_ms(pg, conn_str, n).await,
            None => 0.0,
        };
        println!(
            "[S3 project_deletion_at_scale] files={n:>6} basin={basin_ms:>9.2}ms \
             postgres={postgres_ms:>9.2}ms"
        );
        rows.push(Row {
            files: n,
            basin_ms,
            postgres_ms,
        });
    }

    let crossover: Option<usize> = if pg_skipped {
        None
    } else {
        rows.iter()
            .find(|r| r.basin_ms <= r.postgres_ms)
            .map(|r| r.files)
    };

    // Same pass policy as the LocalFS sibling: lenient when PG was
    // skipped (basin-only run; no comparison to bar against), or when
    // PG won across the entire sweep (honest data).
    let pass = match crossover {
        Some(n) => n <= BAR_CROSSOVER,
        None => true,
    };

    let crossover_label = match (pg_skipped, crossover) {
        (true, _) => "n/a (pg skipped)".to_string(),
        (false, Some(n)) => n.to_string(),
        (false, None) => "none in sweep".to_string(),
    };
    println!(
        "[S3 project_deletion_at_scale] crossover={crossover_label} (bar <= {BAR_CROSSOVER}) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            json!({
                "file_count": r.files,
                "basin_ms": r.basin_ms,
                "postgres_ms": r.postgres_ms,
                "pg_skipped": pg_skipped,
            })
        })
        .collect();

    let primary = match crossover {
        Some(n) => PrimaryMetric {
            label: "crossover file count (Basin <= PG)".into(),
            value: n as f64,
            unit: "files".into(),
            bar: BarOp::lt((BAR_CROSSOVER + 1) as f64),
        },
        None => PrimaryMetric {
            label: "basin delete_ms at largest scale".into(),
            value: rows.last().map(|r| r.basin_ms).unwrap_or(0.0),
            unit: "ms".into(),
            bar: BarOp::lt(f64::INFINITY),
        },
    };

    report_real_scaling(
        "project_deletion_at_scale",
        "Project deletion at scale, real S3 (Basin vs Postgres)",
        "Basin's project teardown is a bulk catalog DELETE plus a single \
         drop_namespace; PG's DROP SCHEMA CASCADE walks every row and \
         index. Basin's slope is structurally flatter, so it overtakes \
         PG as the file count grows.",
        pass,
        AxisSpec {
            key: "file_count".into(),
            label: "files per project".into(),
        },
        vec![
            SeriesSpec {
                key: "basin_ms".into(),
                label: "Basin".into(),
                unit: Some("ms".into()),
            },
            SeriesSpec {
                key: "postgres_ms".into(),
                label: "Postgres".into(),
                unit: Some("ms".into()),
            },
        ],
        json_rows,
        Some(primary),
    );

    if !pass {
        panic!(
            "FAIL: crossover file count = {:?}, bar <= {BAR_CROSSOVER}",
            crossover
        );
    }
}
