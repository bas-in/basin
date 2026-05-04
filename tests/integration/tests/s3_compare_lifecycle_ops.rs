//! S3 port of `compare_lifecycle_ops.rs`.
//!
//! Tenant deletion + ADD COLUMN. Tenant deletion on S3 is dramatically
//! different from LocalFS (per-object DELETE round-trip vs an unlinked inode),
//! so the absolute numbers will diverge. We surface honestly: PG vs Basin-S3
//! with no fudging, and let the dashboard reader judge.
//!
//! ADD COLUMN is a catalog-only operation in Basin; backend doesn't change
//! it. We include it for parity with the LocalFS test.
//!
//! Skips cleanly when [s3] or [postgres] is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_real_postgres_compare, CompareMetric, WhichWins};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_compare_lifecycle_ops";
const PG_ROWS: usize = 100_000;
const BASIN_FILES: usize = 100;
const BASIN_ROWS_PER_FILE: usize = 1_000;

fn which_wins_lower(basin: f64, postgres: f64) -> WhichWins {
    if basin < postgres {
        WhichWins::Basin
    } else if basin > postgres {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

async fn pg_connect(pg_cfg: &basin_integration_tests::test_config::PostgresConfig) -> Option<(Client, String)> {
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

fn basin_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn basin_schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("tag", DataType::Utf8, true),
    ]))
}

fn build_basin_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let bodies: Vec<String> = (0..len).map(|i| format!("body-{}", start + i as i64)).collect();
    let body_arr: StringArray = bodies.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(basin_schema_v1(), vec![Arc::new(ids), Arc::new(body_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_compare_lifecycle_ops() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };
    let pg_cfg = match cfg.pg_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => {
            report_real_postgres_compare(
                "lifecycle_ops",
                "Lifecycle ops on real S3: tenant deletion + ADD COLUMN",
                "Basin makes tenant teardown a list+delete and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    let (pg, pg_conn_str) = match pg_connect(&pg_cfg).await {
        Some(v) => v,
        None => {
            println!("[S3 compare_lifecycle_ops] postgres unreachable: skipping");
            report_real_postgres_compare(
                "lifecycle_ops",
                "Lifecycle ops on real S3: tenant deletion + ADD COLUMN",
                "Basin makes tenant teardown a list+delete and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE.",
                false,
                vec![],
                Some("postgres unreachable"),
            );
            return;
        }
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let suffix = TenantId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_lifecycle_s3_{}", suffix);
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: pg_conn_str.clone(),
    };

    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.events (id BIGINT, body TEXT)"
    ))
    .await
    .expect("create table");

    pg.simple_query(&format!(
        "INSERT INTO {schema}.events (id, body) \
         SELECT i, 'body-' || i FROM generate_series(0, {}) i",
        PG_ROWS - 1
    ))
    .await
    .expect("pg populate");

    // ---- Metric A: tenant deletion --------------------------------------
    // Basin on S3: write 100 small Parquet files for one tenant under a
    // sub-prefix, then time the prefix list+delete.
    let del_prefix = format!("{run_prefix}/del");
    let storage = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(ObjectPath::from(del_prefix.as_str())),
    });
    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();
    // Fan-out the writes — sequential PUTs on S3 would dominate the test.
    let mut writers = tokio::task::JoinSet::new();
    for i in 0..BASIN_FILES {
        let storage = storage.clone();
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let start = (i * BASIN_ROWS_PER_FILE) as i64;
            let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
            storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap();
        });
    }
    while let Some(r) = writers.join_next().await {
        r.unwrap();
    }
    let tenant_prefix = ObjectPath::from(format!("{del_prefix}/tenants/{tenant}"));

    let basin_del_started = Instant::now();
    let paths_stream = object_store
        .list(Some(&tenant_prefix))
        .map_ok(|m| m.location)
        .boxed();
    let _deleted: Vec<ObjectPath> = object_store
        .delete_stream(paths_stream)
        .try_collect()
        .await
        .expect("delete_stream");
    let basin_del_ms = basin_del_started.elapsed().as_secs_f64() * 1000.0;

    let pg_del_started = Instant::now();
    pg.simple_query(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("drop schema");
    let pg_del_ms = pg_del_started.elapsed().as_secs_f64() * 1000.0;

    let del_ratio = pg_del_ms / basin_del_ms.max(1e-9);

    // ---- Metric B: ADD COLUMN -------------------------------------------
    let suffix2 = TenantId::new().as_ulid().to_string().to_lowercase();
    let schema2 = format!("basin_lifecycle_s3_{}", suffix2);
    let _guard2 = SchemaGuard {
        schema: schema2.clone(),
        conn_str: pg_conn_str.clone(),
    };
    pg.simple_query(&format!("CREATE SCHEMA {schema2}"))
        .await
        .expect("create schema 2");
    pg.simple_query(&format!(
        "CREATE TABLE {schema2}.events (id BIGINT, body TEXT)"
    ))
    .await
    .expect("create table 2");
    pg.simple_query(&format!(
        "INSERT INTO {schema2}.events (id, body) \
         SELECT i, 'body-' || i FROM generate_series(0, {}) i",
        PG_ROWS - 1
    ))
    .await
    .expect("pg populate 2");

    let pg_alter_started = Instant::now();
    pg.simple_query(&format!(
        "ALTER TABLE {schema2}.events ADD COLUMN tag TEXT"
    ))
    .await
    .expect("pg alter");
    let pg_alter_ms = pg_alter_started.elapsed().as_secs_f64() * 1000.0;

    // Basin: simulate ADD COLUMN by creating a new table with the wider
    // schema. Catalog-only; no data move. Same approach as LocalFS test.
    let alter_prefix = format!("{run_prefix}/alter");
    let storage2 = Storage::new(StorageConfig {
        object_store: object_store.clone(),
        root_prefix: Some(ObjectPath::from(alter_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let tenant2 = TenantId::new();
    let table_v1 = TableName::new("events").unwrap();
    let table_v2 = TableName::new("events_v2").unwrap();
    let part2 = PartitionKey::default_key();
    catalog.create_namespace(&tenant2).await.unwrap();
    catalog
        .create_table(&tenant2, &table_v1, basin_schema_v1().as_ref())
        .await
        .unwrap();
    let mut writers = tokio::task::JoinSet::new();
    for i in 0..BASIN_FILES {
        let storage = storage2.clone();
        let table = table_v1.clone();
        let part = part2.clone();
        writers.spawn(async move {
            let start = (i * BASIN_ROWS_PER_FILE) as i64;
            let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
            storage
                .write_batch(&tenant2, &table, &part, &batch)
                .await
                .unwrap();
        });
    }
    while let Some(r) = writers.join_next().await {
        r.unwrap();
    }

    let basin_alter_started = Instant::now();
    catalog
        .create_table(&tenant2, &table_v2, basin_schema_v2().as_ref())
        .await
        .unwrap();
    let basin_alter_ms = basin_alter_started.elapsed().as_secs_f64() * 1000.0;

    let alter_ratio = pg_alter_ms / basin_alter_ms.max(1e-9);

    println!(
        "{:>52} {:>14} {:>14} {:>22}",
        "metric", "basin (s3)", "postgres", "ratio"
    );
    println!(
        "{:>52} {:>12.2}ms {:>12.2}ms {:>22}",
        "tenant_deletion (1 tenant, 100K rows in 100 files)",
        basin_del_ms,
        pg_del_ms,
        format!("pg/basin = {:.2}x", del_ratio)
    );
    println!(
        "{:>52} {:>12.2}ms {:>12.2}ms {:>22}",
        "add_column (100K rows; basin = catalog-only sim)",
        basin_alter_ms,
        pg_alter_ms,
        format!("pg/basin = {:.2}x", alter_ratio)
    );

    println!(
        "[S3 compare_lifecycle_ops] tenant-delete ratio={:.2}x, add-column ratio={:.2}x",
        del_ratio, alter_ratio
    );

    let metrics = vec![
        CompareMetric {
            label: "Tenant deletion (1 tenant, 100K rows in 100 files; basin on S3)".into(),
            basin: basin_del_ms,
            postgres: pg_del_ms,
            unit: "ms".into(),
            better: which_wins_lower(basin_del_ms, pg_del_ms),
            ratio_text: Some(format!("pg / basin = {:.2}x", del_ratio)),
        },
        CompareMetric {
            label: "ADD COLUMN on 100K rows (Basin: catalog-only simulation; engine doesn't parse ALTER yet)".into(),
            basin: basin_alter_ms,
            postgres: pg_alter_ms,
            unit: "ms".into(),
            better: which_wins_lower(basin_alter_ms, pg_alter_ms),
            ratio_text: Some(format!("pg / basin = {:.2}x", alter_ratio)),
        },
    ];

    report_real_postgres_compare(
        "lifecycle_ops",
        "Lifecycle ops on real S3: tenant deletion + ADD COLUMN",
        "Basin makes tenant teardown a list+delete and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE and (in the general case) rewrite the heap.",
        true,
        metrics,
        Some("Basin storage on real S3 — tenant deletion is parallel DELETE round-trips, not unlinked inodes."),
    );

    drop(_guard2);
    drop(_guard);
}
