//! Compare 3: lifecycle ops — tenant deletion and ADD COLUMN.
//!
//! Two metrics:
//!   A. Tenant deletion. Basin = list+delete the tenant prefix; PG = DROP
//!      SCHEMA ... CASCADE on a 100K-row table.
//!   B. ADD COLUMN on a 100K-row table. PG runs `ALTER TABLE ... ADD COLUMN
//!      tag TEXT` (metadata-only fast path, no default). Basin doesn't
//!      currently parse ALTER, so we simulate the catalog-side cost: create
//!      a new table with the wider schema and time the `create_table` call.
//!      The metric label calls this out so the dashboard reader knows.
//!
//! Skip-rather-than-fail: PG unavailable -> emit a `compare` report with
//! `available=false` and exit Ok.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use basin_storage::{Storage, StorageConfig};
use futures::stream::StreamExt;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

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

async fn try_pg_connect() -> Option<(Client, String)> {
    for user in ["pc", "postgres"] {
        let conn_str = format!("host=127.0.0.1 port=5432 user={user} dbname=postgres");
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return Some((client, conn_str));
            }
            Err(_) => continue,
        }
    }
    None
}

/// RAII guard that drops one schema on Drop. Same shape as compare_postgres.rs.
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
    // V1 plus a new nullable `tag` column — the moral equivalent of
    // `ALTER TABLE events ADD COLUMN tag TEXT`.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_lifecycle_ops() {
    let (pg, pg_conn_str) = match try_pg_connect().await {
        Some(v) => v,
        None => {
            println!("[COMPARE lifecycle_ops] postgres unavailable: skipping");
            report_postgres_compare(
                "lifecycle_ops",
                "Lifecycle ops: tenant deletion + ADD COLUMN",
                "Basin makes tenant teardown a list+delete and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE and (in the general case) rewrite the heap.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    // Unique schema per run; Drop guard cleans up even on panic.
    let suffix = TenantId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_lifecycle_{}", suffix);
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

    // Populate PG with 100K rows. generate_series + INSERT is ~10x faster
    // than client-side multi-row INSERT for this size and is what a
    // production DBA would use.
    pg.simple_query(&format!(
        "INSERT INTO {schema}.events (id, body) \
         SELECT i, 'body-' || i FROM generate_series(0, {}) i",
        PG_ROWS - 1
    ))
    .await
    .expect("pg populate");

    // ---- Metric A: tenant deletion --------------------------------------
    // Basin: write 100 small Parquet files for one tenant (= 100K rows
    // across files), then time the prefix list+delete.
    let dir = TempDir::new().unwrap();
    let fs: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs.clone(),
        root_prefix: None,
    });
    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();
    for i in 0..BASIN_FILES {
        let start = (i * BASIN_ROWS_PER_FILE) as i64;
        let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
        storage
            .write_batch(&tenant, &table, &part, &batch)
            .await
            .unwrap();
    }
    let tenant_prefix = ObjectPath::from(format!("tenants/{tenant}"));

    let basin_del_started = Instant::now();
    let paths_stream = fs
        .list(Some(&tenant_prefix))
        .map_ok(|m| m.location)
        .boxed();
    let _deleted: Vec<ObjectPath> = fs
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
    // Set up a fresh PG schema for the column-add, since the previous one
    // was just dropped.
    let suffix2 = TenantId::new().as_ulid().to_string().to_lowercase();
    let schema2 = format!("basin_lifecycle_{}", suffix2);
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

    // PG: timed metadata-only ALTER (no default = fast-path on PG 18).
    let pg_alter_started = Instant::now();
    pg.simple_query(&format!(
        "ALTER TABLE {schema2}.events ADD COLUMN tag TEXT"
    ))
    .await
    .expect("pg alter");
    let pg_alter_ms = pg_alter_started.elapsed().as_secs_f64() * 1000.0;

    // Basin: simulate by creating a new table with the wider schema and
    // timing the `create_table` call. This is the catalog-only cost — the
    // moral equivalent of PG's metadata-only fast path. The data copy is
    // intentionally NOT included; PG's fast path doesn't copy either.
    // First, set up a tenant + populate the original table so we have
    // something analogous on disk.
    let dir2 = TempDir::new().unwrap();
    let fs2: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir2.path()).unwrap());
    let storage2 = Storage::new(StorageConfig {
        object_store: fs2.clone(),
        root_prefix: None,
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
    for i in 0..BASIN_FILES {
        let start = (i * BASIN_ROWS_PER_FILE) as i64;
        let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
        storage2
            .write_batch(&tenant2, &table_v1, &part2, &batch)
            .await
            .unwrap();
    }

    let basin_alter_started = Instant::now();
    catalog
        .create_table(&tenant2, &table_v2, basin_schema_v2().as_ref())
        .await
        .unwrap();
    let basin_alter_ms = basin_alter_started.elapsed().as_secs_f64() * 1000.0;

    let alter_ratio = pg_alter_ms / basin_alter_ms.max(1e-9);

    // ---- Print + report --------------------------------------------------
    println!(
        "{:>52} {:>14} {:>14} {:>22}",
        "metric", "basin", "postgres", "ratio"
    );
    println!(
        "{:>52} {:>12.2}ms {:>12.2}ms {:>22}",
        "tenant_deletion (1 tenant, 100K rows)",
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
        "[COMPARE lifecycle_ops] tenant-delete ratio={:.2}x, add-column ratio={:.2}x",
        del_ratio, alter_ratio
    );

    let metrics = vec![
        CompareMetric {
            label: "Tenant deletion (1 tenant, 100K rows)".into(),
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

    report_postgres_compare(
        "lifecycle_ops",
        "Lifecycle ops: tenant deletion + ADD COLUMN",
        "Basin makes tenant teardown a list+delete and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE and (in the general case) rewrite the heap.",
        true,
        metrics,
        None,
    );

    // Drop guards explicitly first; their Drop impls remain a safety net.
    drop(_guard2);
    drop(_guard);
}
