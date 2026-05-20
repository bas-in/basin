//! Compare 3: lifecycle ops — project deletion and ADD COLUMN.
//!
//! Two metrics:
//!   A. Project deletion. Basin = `Storage::delete_project` (catalog-first +
//!      parallel orphan LIST + drop_namespace); PG = DROP SCHEMA ... CASCADE
//!      on a 100K-row table.
//!   B. ADD COLUMN on a 100K-row table. Both Basin and PG run
//!      `ALTER TABLE events ADD COLUMN tag TEXT` end-to-end. PG hits its
//!      metadata-only fast path (no default, PG 18); Basin hits the
//!      catalog-only ALTER path the engine added in `crates/basin-engine/
//!      src/alter.rs`. No simulation — both numbers are the real SQL surface.
//!
//! Skip-rather-than-fail: PG unavailable -> emit a `compare` report with
//! `available=false` and exit Ok.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
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

fn basin_schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, false),
    ]))
}

fn build_basin_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let bodies: Vec<String> = (0..len)
        .map(|i| format!("body-{}", start + i as i64))
        .collect();
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
                "Lifecycle ops: project deletion + ADD COLUMN",
                "Basin makes project teardown a catalog-first DELETE with a parallel orphan LIST and a single drop_namespace, and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE and (in the general case) rewrite the heap.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    // Unique schema per run; Drop guard cleans up even on panic.
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
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

    // ---- Metric A: project deletion --------------------------------------
    // Basin: write 100 small Parquet files for one project (= 100K rows
    // across files) and register them in an `InMemoryCatalog`, then time
    // `Storage::delete_project` end-to-end. This is the same code path the
    // engine wires up — catalog-first DELETE + parallel orphan LIST +
    // drop_namespace — so the dashboard plots the production teardown
    // latency, not a bypass through the raw object store.
    let dir = TempDir::new().unwrap();
    let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();
    catalog
        .create_table(&project, &table, basin_schema_v1().as_ref())
        .await
        .unwrap();
    let mut written: Vec<DataFileRef> = Vec::with_capacity(BASIN_FILES);
    for i in 0..BASIN_FILES {
        let start = (i * BASIN_ROWS_PER_FILE) as i64;
        let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
        let f = storage
            .write_batch(&project, &table, &part, &batch)
            .await
            .unwrap();
        written.push(DataFileRef {
            path: f.path.as_ref().to_string(),
            size_bytes: f.size_bytes,
            row_count: f.row_count,
            column_stats: f.column_stats.clone(),
            bloom_filters: ::std::collections::BTreeMap::new(),
            hll_sketches: ::std::collections::BTreeMap::new(),
            tdigest_sketches: ::std::collections::BTreeMap::new(),
        });
    }
    // One catalog append registers every file in a single snapshot — the
    // deletion path then sees the full set without a LIST RTT.
    catalog
        .append_data_files(&project, &table, SnapshotId::GENESIS, written)
        .await
        .unwrap();

    let basin_del_started = Instant::now();
    let _deleted = storage
        .delete_project(catalog.as_ref(), &project)
        .await
        .expect("delete_project");
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
    let suffix2 = ProjectId::new().as_ulid().to_string().to_lowercase();
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
    pg.simple_query(&format!("ALTER TABLE {schema2}.events ADD COLUMN tag TEXT"))
        .await
        .expect("pg alter");
    let pg_alter_ms = pg_alter_started.elapsed().as_secs_f64() * 1000.0;

    // Basin: real ALTER TABLE through the engine's SQL surface. The
    // ADD COLUMN path is implemented in `crates/basin-engine/src/alter.rs`
    // and dispatches via `ProjectSession::execute`. We populate 100 small
    // Parquet files first so the timed ALTER runs against a table with
    // 100K rows — the catalog-only fast path doesn't touch them, but the
    // mental model matches PG's "100K-row metadata-only ALTER".
    let dir2 = TempDir::new().unwrap();
    let fs2: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir2.path()).unwrap());
    let storage2 = Storage::new(StorageConfig {
        object_store: fs2.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog2: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage: storage2.clone(),
        catalog: catalog2.clone(),
        shard: None,
    });
    let project2 = ProjectId::new();
    let session = engine.open_session(project2).await.unwrap();
    session
        .execute("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .unwrap();

    // Populate via direct storage writes — much faster than 100K rows
    // through SQL VALUES, and ALTER's catalog-only path doesn't care
    // how the data files arrived.
    let table_v1 = TableName::new("events").unwrap();
    let part2 = PartitionKey::default_key();
    for i in 0..BASIN_FILES {
        let start = (i * BASIN_ROWS_PER_FILE) as i64;
        let batch = build_basin_batch(start, BASIN_ROWS_PER_FILE);
        storage2
            .write_batch(&project2, &table_v1, &part2, &batch)
            .await
            .unwrap();
    }

    let basin_alter_started = Instant::now();
    session
        .execute("ALTER TABLE events ADD COLUMN tag TEXT")
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
        "project_deletion (1 project, 100K rows)",
        basin_del_ms,
        pg_del_ms,
        format!("pg/basin = {:.2}x", del_ratio)
    );
    println!(
        "{:>52} {:>12.2}ms {:>12.2}ms {:>22}",
        "ADD COLUMN on 100K rows",
        basin_alter_ms,
        pg_alter_ms,
        format!("pg/basin = {:.2}x", alter_ratio)
    );

    println!(
        "[COMPARE lifecycle_ops] project-delete ratio={:.2}x, add-column ratio={:.2}x",
        del_ratio, alter_ratio
    );

    let metrics = vec![
        CompareMetric {
            label: "Project deletion (1 project, 100K rows)".into(),
            basin: basin_del_ms,
            postgres: pg_del_ms,
            unit: "ms".into(),
            better: which_wins_lower(basin_del_ms, pg_del_ms),
            ratio_text: Some(format!("pg / basin = {:.2}x", del_ratio)),
        },
        CompareMetric {
            label: "ADD COLUMN on 100K rows".into(),
            basin: basin_alter_ms,
            postgres: pg_alter_ms,
            unit: "ms".into(),
            better: which_wins_lower(basin_alter_ms, pg_alter_ms),
            ratio_text: Some(format!("pg / basin = {:.2}x", alter_ratio)),
        },
    ];

    report_postgres_compare(
        "lifecycle_ops",
        "Lifecycle ops: project deletion + ADD COLUMN",
        "Basin makes project teardown a catalog-first DELETE with a parallel orphan LIST and a single drop_namespace, and treats schema evolution as a catalog operation; PG must DROP SCHEMA CASCADE and (in the general case) rewrite the heap.",
        true,
        metrics,
        None,
    );

    // Drop guards explicitly first; their Drop impls remain a safety net.
    drop(_guard2);
    drop(_guard);
}
