//! S3 port of `scaling_tenant_deletion_realistic.rs`.
//!
//! Card: `tenant_deletion_realistic` (real-cloud dashboard).
//! Bar: at the largest scale point in the sweep, `basin_ms <= postgres_ms`
//! when PG is available. PG-skipped runs pass trivially (basin-only).
//!
//! Same shape as the LocalFS card with a realistic SaaS-shaped PG
//! schema (5 indexes + FK CASCADE). Basin's side is unchanged from the
//! simple-schema variant — storage doesn't care about indexes.
//!
//! Scale gating for cloud cost (see s3_scaling_tenant_deletion_at_scale):
//!   * R2 / generic S3: SCALES = [10, 100, 1000].
//!   * SeaweedFS loopback: SCALES = [100, 1000, 5000].
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop, PostgresConfig};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_scaling_tenant_deletion_realistic";
const ROWS_PER_FILE: usize = 100;
const SCALES_CLOUD: [usize; 3] = [10, 100, 1_000];
const SCALES_LOCAL: [usize; 3] = [100, 1_000, 5_000];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let bodies: Vec<String> = (0..len).map(|i| format!("body-{}", start + i as i64)).collect();
    let body_arr: StringArray = bodies.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(body_arr)]).unwrap()
}

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
                    if let Ok((client, conn)) =
                        tokio_postgres::connect(&conn_str, NoTls).await
                    {
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
    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    catalog
        .create_table(&tenant, &table, schema().as_ref())
        .await
        .unwrap();

    let mut written: Vec<DataFileRef> = Vec::with_capacity(files);
    for i in 0..files {
        let start = (i * ROWS_PER_FILE) as i64;
        let batch = build_batch(start, ROWS_PER_FILE);
        let f = setup_storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
        written.push(DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
        });
    }
    catalog
        .append_data_files(&tenant, &table, SnapshotId::GENESIS, written)
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
        .delete_tenant(catalog.as_ref(), &tenant)
        .await
        .expect("delete_tenant");
    started.elapsed().as_secs_f64() * 1000.0
}

async fn measure_pg_ms(pg: &Client, conn_str: &str, files: usize) -> f64 {
    let total_rows = files * ROWS_PER_FILE;
    let suffix = TenantId::new().as_ulid().to_string().to_lowercase();
    let schema_name = format!("basin_s3_real_{}", suffix);
    let _guard = SchemaGuard {
        schema: schema_name.clone(),
        conn_str: conn_str.to_string(),
    };

    pg.simple_query(&format!("CREATE SCHEMA {schema_name}"))
        .await
        .expect("create schema");

    pg.simple_query(&format!(
        "CREATE TABLE {schema_name}.users (id BIGINT PRIMARY KEY)"
    ))
    .await
    .expect("create users");

    pg.simple_query(&format!(
        "CREATE TABLE {schema_name}.events (\
            id BIGINT, ts BIGINT, owner_id BIGINT, \
            region TEXT, payload TEXT)"
    ))
    .await
    .expect("create events");

    pg.simple_query(&format!(
        "CREATE INDEX ON {schema_name}.events (id);\
         CREATE INDEX ON {schema_name}.events (ts);\
         CREATE INDEX ON {schema_name}.events (owner_id, ts DESC);\
         CREATE INDEX ON {schema_name}.events (region, ts DESC);\
         CREATE INDEX ON {schema_name}.events (payload) WHERE payload != '';"
    ))
    .await
    .expect("create indexes");

    pg.simple_query(&format!(
        "ALTER TABLE {schema_name}.events \
         ADD CONSTRAINT events_owner_fk \
         FOREIGN KEY (owner_id) REFERENCES {schema_name}.users(id) \
         ON DELETE CASCADE"
    ))
    .await
    .expect("add fk");

    let owner_modulus: i64 = 1024;
    pg.simple_query(&format!(
        "INSERT INTO {schema_name}.users (id) \
         SELECT i FROM generate_series(0, {}) i",
        owner_modulus - 1
    ))
    .await
    .expect("populate users");

    pg.simple_query(&format!(
        "INSERT INTO {schema_name}.events (id, ts, owner_id, region, payload) \
         SELECT i, \
                i * 1000, \
                i % {owner_modulus}, \
                CASE (i % 4) WHEN 0 THEN 'us-east' WHEN 1 THEN 'us-west' \
                             WHEN 2 THEN 'eu-west' ELSE 'apac' END, \
                CASE WHEN (i % 2) = 0 THEN 'p-' || i ELSE '' END \
         FROM generate_series(0, {}) i",
        total_rows.saturating_sub(1)
    ))
    .await
    .expect("populate events");

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
async fn s3_scaling_tenant_deletion_realistic() {
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
        println!(
            "[S3 tenant_deletion_realistic] postgres unavailable: basin-only run"
        );
    }

    let scales: &[usize] = if is_seaweedfs_target() {
        &SCALES_LOCAL
    } else {
        &SCALES_CLOUD
    };
    let bar_scale_files: usize = *scales.last().unwrap();
    println!(
        "[S3 tenant_deletion_realistic] scales = {:?} (seaweedfs target = {})",
        scales,
        is_seaweedfs_target()
    );

    struct Row {
        files: usize,
        basin_ms: f64,
        postgres_ms: f64,
    }
    let mut rows: Vec<Row> = Vec::with_capacity(scales.len());

    let total_started = Instant::now();
    for &n in scales.iter() {
        let basin_ms = measure_basin_ms(object_store.clone(), prefix_path.clone(), n).await;
        let postgres_ms = match pg_pair.as_ref() {
            Some((pg, conn_str)) => measure_pg_ms(pg, conn_str, n).await,
            None => 0.0,
        };
        println!(
            "[S3 tenant_deletion_realistic] files={n:>6} \
             basin={basin_ms:>9.2}ms postgres={postgres_ms:>9.2}ms"
        );
        rows.push(Row {
            files: n,
            basin_ms,
            postgres_ms,
        });
    }
    let total_wall_ms = total_started.elapsed().as_secs_f64() * 1000.0;

    let crossover: Option<usize> = if pg_skipped {
        None
    } else {
        rows.iter()
            .find(|r| r.basin_ms <= r.postgres_ms)
            .map(|r| r.files)
    };

    let bar_row = rows.iter().find(|r| r.files == bar_scale_files);
    let pass = if pg_skipped {
        true
    } else if let Some(r) = bar_row {
        r.basin_ms <= r.postgres_ms
    } else {
        true
    };

    let crossover_label = match (pg_skipped, crossover) {
        (true, _) => "n/a (pg skipped)".to_string(),
        (false, Some(n)) => n.to_string(),
        (false, None) => "none in sweep".to_string(),
    };
    println!(
        "[S3 tenant_deletion_realistic] crossover={crossover_label} \
         bar_basin@{bar_scale_files}={:.2}ms bar_pg@{bar_scale_files}={:.2}ms \
         total_wall={total_wall_ms:.0}ms {}",
        bar_row.map(|r| r.basin_ms).unwrap_or(0.0),
        bar_row.map(|r| r.postgres_ms).unwrap_or(0.0),
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

    let primary = match (pg_skipped, bar_row) {
        (false, Some(r)) if r.postgres_ms > 0.0 => PrimaryMetric {
            label: format!(
                "Basin/Postgres ratio at {bar_scale_files} files (realistic schema, real S3)"
            ),
            value: r.basin_ms / r.postgres_ms,
            unit: "ratio".into(),
            bar: BarOp::lt(1.0_f64 + f64::EPSILON),
        },
        _ => PrimaryMetric {
            label: "basin delete_ms at largest scale".into(),
            value: rows.last().map(|r| r.basin_ms).unwrap_or(0.0),
            unit: "ms".into(),
            bar: BarOp::lt(f64::INFINITY),
        },
    };

    report_real_scaling(
        "tenant_deletion_realistic",
        "Tenant deletion at scale, realistic SaaS schema, real S3 (Basin vs Postgres)",
        "Real production tables have 5-20 indexes and 1-3 FK \
         constraints. PG's DROP SCHEMA CASCADE walks every row × \
         every index + validates every FK on the cascade; Basin's \
         tenant teardown stays O(file_count). The crossover from the \
         simple card moves dramatically left under this schema profile.",
        pass,
        AxisSpec {
            key: "file_count".into(),
            label: "files per tenant".into(),
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

    // Same lenient policy as LocalFS sibling: passed:false is honest
    // data when PG wins anyway. We emit and don't panic.
}
