//! Compare: backup cost — Basin's Iceberg-style snapshot vs `pg_dump`.
//!
//! Card: `compare_backup_cost`
//! Claim: For wedge-shaped audit-log data, Basin's "backup" is an O(1)
//! metadata copy (the snapshot manifest, which references the same
//! immutable Parquet files) while Postgres `pg_dump` is O(data) — it
//! has to serialise every row.
//!
//! Setup: 1 project, 100k rows in 100 files (~1k rows / file).
//!
//! Basin path:
//!   - Insert 100k rows split into 100 INSERT batches so the writer flushes
//!     ~one Parquet file per batch.
//!   - "Backup" = load the table metadata, serialise the current snapshot's
//!     manifest (the list of `DataFileRef`s + schema metadata) to JSON.
//!     Time the load + serialise; report the manifest size in bytes.
//!     This is genuinely what an Iceberg-style backup commits to a backup
//!     bucket: the data files are already on object storage and immutable;
//!     only the manifest needs to land in a fresh location.
//!
//! Postgres path:
//!   - `pg_dump --schema=<basin_compare_<ulid>>` to a tempfile.
//!   - Time the shell-out; report the dump's byte size.
//!
//! Skip-rather-than-fail: missing `pg_dump` binary → emit a `compare`
//! report with `available=false` and return Ok.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, ProjectId};
use basin_engine::{Engine, EngineConfig};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::process::Command as TokioCommand;
use tokio_postgres::{Client, NoTls};

const ROWS: usize = 100_000;
const FILES: usize = 100;
const ROWS_PER_FILE: usize = ROWS / FILES;

fn payload_for(i: i64) -> String {
    format!("payload-{:040}", i)
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_backup_cost() {
    // ---- Basin path: insert 100k rows in 100 batches ------------------
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
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
    )
    .await
    .unwrap();

    // 100 batches × 1000 rows. Each INSERT goes through the legacy
    // synchronous Parquet write path (no shard configured), so each batch
    // produces one file.
    for batch in 0..FILES {
        let mut stmt = String::with_capacity(ROWS_PER_FILE * 80);
        stmt.push_str("INSERT INTO events VALUES ");
        for j in 0..ROWS_PER_FILE {
            if j > 0 {
                stmt.push(',');
            }
            let id = (batch * ROWS_PER_FILE + j) as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        sess.execute(&stmt).await.expect("basin insert batch");
    }

    // Sanity: confirm we got close to FILES files.
    let files_listed = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();
    println!(
        "[compare_backup_cost] basin: wrote {} rows in {} files",
        ROWS,
        files_listed.len()
    );

    // ---- Basin "backup" = load + serialise the snapshot manifest ------
    //
    // Iceberg-style backup is an O(1) metadata copy: the data files are
    // already on object storage and immutable, so the only thing the
    // backup needs to capture is the snapshot manifest (the list of
    // DataFileRef's + schema). We measure the wall time to load the
    // current TableMetadata and serialise the active snapshot to JSON;
    // the byte size is the backup-on-the-wire size.
    let basin_backup_start = Instant::now();
    let meta = catalog.load_table(&project, &table).await.unwrap();
    let current = meta
        .current()
        .expect("table must have at least the genesis snapshot")
        .clone();
    // Serialise the snapshot record. We include the table's schema as
    // the surrounding context an Iceberg manifest would also include.
    let manifest_json = serde_json::json!({
        "table": meta.table.as_str(),
        "current_snapshot": meta.current_snapshot,
        "snapshot": current,
    });
    let manifest_bytes = serde_json::to_vec(&manifest_json).expect("serialize manifest");
    let basin_backup_seconds = basin_backup_start.elapsed().as_secs_f64();
    let basin_backup_bytes = manifest_bytes.len() as u64;

    println!(
        "[compare_backup_cost] basin: backup_seconds={:.6}, manifest_bytes={}",
        basin_backup_seconds, basin_backup_bytes
    );

    // ---- Postgres path -----------------------------------------------------
    let (pg, conn_str) = match try_pg_connect().await {
        Some(v) => v,
        None => {
            println!("[compare_backup_cost] postgres unavailable: skipping");
            report_postgres_compare(
                "backup_cost",
                "Backup cost: Basin snapshot vs pg_dump (100k rows)",
                "Basin's Iceberg-style snapshot is an O(1) manifest copy while pg_dump is O(data).",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_backup_{}", suffix);
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: conn_str.clone(),
    };
    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.events (id BIGINT, ts BIGINT, payload TEXT)"
    ))
    .await
    .expect("create table");

    // Insert 100k rows into PG. Multi-row VALUES, batched so it's brisk.
    let pg_batch = 1_000;
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(pg_batch * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.events (id, ts, payload) VALUES "
        ));
        for j in 0..pg_batch {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        pg.simple_query(&stmt).await.expect("pg insert batch");
        row_idx += pg_batch as i64;
    }

    // ---- pg_dump -----------------------------------------------------------
    // We dump the schema only (no globals, no other DBs) to a tempfile.
    // The shell-out gives us the wall time and the file gives us the
    // byte size.
    let dump_dir = TempDir::new().unwrap();
    let dump_path = dump_dir.path().join("dump.sql");
    let pg_dump_start = Instant::now();
    let dump_status = TokioCommand::new("pg_dump")
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "5432",
            "--username",
            // Mirror try_pg_connect: prefer `pc` then `postgres`; tokio_postgres
            // already succeeded with one of them. The conn_str captures it.
            extract_user_from_conn(&conn_str).unwrap_or("postgres"),
            "--no-password",
            "--schema",
            &schema,
            "--file",
        ])
        .arg(&dump_path)
        .arg("postgres")
        .status()
        .await;
    let pg_dump_seconds = pg_dump_start.elapsed().as_secs_f64();

    let (pg_dump_seconds, pg_dump_bytes, available, note): (f64, u64, bool, Option<String>) =
        match dump_status {
            Ok(s) if s.success() => match std::fs::metadata(&dump_path) {
                Ok(m) => (pg_dump_seconds, m.len(), true, None),
                Err(e) => (
                    pg_dump_seconds,
                    0,
                    false,
                    Some(format!("pg_dump file stat failed: {e}")),
                ),
            },
            Ok(s) => (
                pg_dump_seconds,
                0,
                false,
                Some(format!("pg_dump exited with status {s}")),
            ),
            Err(e) => (0.0, 0, false, Some(format!("pg_dump spawn failed: {e}"))),
        };

    if !available {
        println!(
            "[compare_backup_cost] pg_dump unavailable: {}",
            note.as_deref().unwrap_or("?")
        );
        report_postgres_compare(
            "backup_cost",
            "Backup cost: Basin snapshot vs pg_dump (100k rows)",
            "Basin's Iceberg-style snapshot is an O(1) manifest copy while pg_dump is O(data).",
            false,
            vec![],
            note.as_deref(),
        );
        drop(_guard);
        return;
    }

    println!(
        "[compare_backup_cost] postgres: backup_seconds={:.3}, dump_bytes={}",
        pg_dump_seconds, pg_dump_bytes
    );

    let seconds_ratio = pg_dump_seconds / basin_backup_seconds.max(1e-9);
    let bytes_ratio = pg_dump_bytes as f64 / (basin_backup_bytes as f64).max(1.0);

    println!(
        "{:>22} {:>15} {:>15} {:>22}",
        "metric", "basin", "postgres", "ratio"
    );
    println!(
        "{:>22} {:>13.6}s {:>13.3}s {:>22}",
        "backup_seconds",
        basin_backup_seconds,
        pg_dump_seconds,
        format!("pg/basin = {:.0}x", seconds_ratio)
    );
    println!(
        "{:>22} {:>13} B {:>11.2} MiB {:>22}",
        "backup_bytes",
        basin_backup_bytes,
        pg_dump_bytes as f64 / (1024.0 * 1024.0),
        format!("pg/basin = {:.0}x", bytes_ratio)
    );

    let metrics = vec![
        CompareMetric {
            label: "Backup wall time".into(),
            basin: basin_backup_seconds,
            postgres: pg_dump_seconds,
            unit: "s".into(),
            better: which_wins_lower(basin_backup_seconds, pg_dump_seconds),
            ratio_text: Some(format!("pg / basin = {:.0}x", seconds_ratio)),
        },
        CompareMetric {
            label: "Backup byte size".into(),
            basin: basin_backup_bytes as f64,
            postgres: pg_dump_bytes as f64,
            unit: "bytes".into(),
            better: which_wins_lower(basin_backup_bytes as f64, pg_dump_bytes as f64),
            ratio_text: Some(format!("pg / basin = {:.0}x", bytes_ratio)),
        },
    ];

    report_postgres_compare(
        "backup_cost",
        "Backup cost: Basin snapshot vs pg_dump (100k rows)",
        "Basin's Iceberg-style snapshot is an O(1) manifest copy while pg_dump is O(data).",
        true,
        metrics,
        None,
    );

    drop(_guard);
}

fn extract_user_from_conn(s: &str) -> Option<&str> {
    for kv in s.split_whitespace() {
        if let Some(rest) = kv.strip_prefix("user=") {
            return Some(rest);
        }
    }
    None
}
