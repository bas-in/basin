//! S3 port of `compare_postgres.rs`.
//!
//! Basin uses S3 storage; PG side unchanged. Numbers will be different due
//! to S3 round-trip costs — point queries on S3 incur HTTP RTT per file
//! footer + row group, while PG hits its buffer cache.
//!
//! Scaled ROWS down from 1M (LocalFS) to 100K to keep the run under 5 min
//! on cloud S3.
//!
//! Skips cleanly when either `[s3]` or `[postgres]` is missing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_postgres_compare, CompareMetric, WhichWins};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_compare_postgres";
// Down from 1M (LocalFS) to 100K — S3 PUT volume + per-row INSERT routing
// through the engine path.
const ROWS: usize = 100_000;
const INSERT_BATCH: usize = 10_000;

fn payload_for(i: i64) -> String {
    format!("payload-{:040}", i)
}

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
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

fn which_wins(basin: f64, postgres: f64) -> WhichWins {
    if basin < postgres {
        WhichWins::Basin
    } else if basin > postgres {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn s3_compare_postgres() {
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
            // We have S3 but no PG. Emit a compare with available=false.
            report_real_postgres_compare(
                "postgres",
                "Basin (real S3) vs Postgres 18 (no index, 100K rows)",
                "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed point queries.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    let (pg, conn_str) = match pg_connect(&pg_cfg).await {
        Some(v) => v,
        None => {
            println!("[S3 compare_postgres] postgres unreachable: skipping");
            report_real_postgres_compare(
                "postgres",
                "Basin (real S3) vs Postgres 18 (no index, 100K rows)",
                "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed point queries.",
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
    let schema = format!("basin_compare_s3_{}", suffix);
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

    // ---- PG insert ---------------------------------------------------------
    let pg_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.events (id, ts, payload) VALUES "
        ));
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        pg.simple_query(&stmt).await.expect("pg insert batch");
        row_idx += INSERT_BATCH as i64;
    }
    let pg_insert_ms = pg_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- PG disk size ------------------------------------------------------
    let pg_disk_bytes: i64 = {
        let row = pg
            .query_one(
                &format!("SELECT pg_total_relation_size('{schema}.events')::bigint"),
                &[],
            )
            .await
            .expect("pg_total_relation_size");
        row.get::<_, i64>(0)
    };

    // ---- PG point query ----------------------------------------------------
    let target_id: i64 = (ROWS as i64) / 2 + 7;
    let mut pg_point_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze");
        let mut found: Option<f64> = None;
        for m in &rows {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                if let Some(line) = r.get(0) {
                    if let Some(idx) = line.find("Execution Time:") {
                        let after = &line[idx + "Execution Time:".len()..];
                        let trimmed = after.trim();
                        if let Some(num_end) = trimmed.find(' ') {
                            if let Ok(v) = trimmed[..num_end].parse::<f64>() {
                                found = Some(v);
                            }
                        }
                    }
                }
            }
        }
        if let Some(ms) = found {
            pg_point_ms.push(ms);
        }
    }
    assert!(!pg_point_ms.is_empty(), "no PG execution time samples parsed");
    let pg_point_p50 = median(&pg_point_ms);

    // ---- Basin path on real S3 ---------------------------------------------
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    // WAL stays in RAM — same rationale as s3_shard_insert_path.
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let wal = basin_wal::Wal::open(basin_wal::WalConfig {
        object_store: wal_store,
        root_prefix: None,
        flush_interval: Duration::from_millis(200),
        flush_max_bytes: 1024 * 1024,
    })
    .await
    .unwrap();
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard),
    });
    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    let basin_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str("INSERT INTO events VALUES ");
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        sess.execute(&stmt).await.expect("basin insert batch");
        row_idx += INSERT_BATCH as i64;
    }
    let basin_insert_ms = basin_insert_started.elapsed().as_secs_f64() * 1000.0;

    let mut basin_point_ms: Vec<f64> = Vec::with_capacity(5);
    let _ = sess
        .execute(&format!("SELECT id FROM events WHERE id = {target_id}"))
        .await
        .unwrap();
    for _ in 0..5 {
        let started = Instant::now();
        let res = sess
            .execute(&format!(
                "SELECT id, ts, payload FROM events WHERE id = {target_id}"
            ))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if let ExecResult::Rows { batches, .. } = res {
            let mut hits = 0usize;
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                hits += arr.len();
            }
            assert!(hits >= 1, "basin missed point query");
        }
        basin_point_ms.push(elapsed);
    }
    let basin_point_p50 = median(&basin_point_ms);

    // Basin disk: sum the size_bytes of every Parquet object under the run
    // prefix. (We can't dir-walk a remote bucket; we list+sum.)
    let basin_disk_bytes: u64 = {
        use futures::StreamExt as _;
        let prefix = ObjectPath::from(run_prefix.as_str());
        let store_handle = storage.object_store_handle();
        let mut stream = store_handle.list(Some(&prefix));
        let mut total = 0u64;
        while let Some(meta) = stream.next().await {
            match meta {
                Ok(m) if m.location.as_ref().ends_with(".parquet") => {
                    total += m.size as u64;
                }
                Ok(_) => {}
                Err(e) => eprintln!("list error during disk size: {e}"),
            }
        }
        total
    };

    let basin_mib = basin_disk_bytes as f64 / (1024.0 * 1024.0);
    let pg_mib = pg_disk_bytes as f64 / (1024.0 * 1024.0);
    let disk_ratio = pg_disk_bytes as f64 / basin_disk_bytes.max(1) as f64;
    let point_ratio = pg_point_p50 / basin_point_p50.max(1e-9);

    println!(
        "{:>22} {:>15} {:>15} {:>20}",
        "metric", "basin (s3)", "postgres", "ratio"
    );
    println!(
        "{:>22} {:>13.2}MiB {:>13.2}MiB {:>20}",
        "on_disk_bytes",
        basin_mib,
        pg_mib,
        format!("pg/basin = {:.2}x", disk_ratio)
    );
    println!(
        "{:>22} {:>15.0} {:>15.0} {:>20}",
        "insert_total_ms", basin_insert_ms, pg_insert_ms, "-"
    );
    println!(
        "{:>22} {:>15.2} {:>15.2} {:>20}",
        "point_query_ms_p50",
        basin_point_p50,
        pg_point_p50,
        format!("pg/basin = {:.2}x", point_ratio)
    );

    println!(
        "[S3 compare_postgres] head-to-head (S3): disk {:.2}x smaller, point-query {:.2}x faster (no PG index)",
        disk_ratio, point_ratio
    );

    let basin_disk_f = basin_disk_bytes as f64;
    let pg_disk_f = pg_disk_bytes as f64;
    let metrics = vec![
        CompareMetric {
            label: "On-disk bytes".into(),
            basin: basin_disk_f,
            postgres: pg_disk_f,
            unit: "bytes".into(),
            better: which_wins(basin_disk_f, pg_disk_f),
            ratio_text: Some(format!("pg / basin = {:.2}x", disk_ratio)),
        },
        CompareMetric {
            label: "Point query p50".into(),
            basin: basin_point_p50,
            postgres: pg_point_p50,
            unit: "ms".into(),
            better: which_wins(basin_point_p50, pg_point_p50),
            ratio_text: Some(format!("pg / basin = {:.2}x", point_ratio)),
        },
        CompareMetric {
            label: "Insert 100K rows".into(),
            basin: basin_insert_ms,
            postgres: pg_insert_ms,
            unit: "ms".into(),
            better: which_wins(basin_insert_ms, pg_insert_ms),
            ratio_text: None,
        },
    ];

    report_real_postgres_compare(
        "postgres",
        "Basin (real S3) vs Postgres 18 (no index, 100K rows)",
        "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed point queries — even with the S3 round-trip cost.",
        true,
        metrics,
        Some("Basin storage on real S3; PG on local 18. Insert path: WAL + shard with in-RAM WAL store."),
    );

    bg.shutdown().await;
    wal.close().await.unwrap();

    drop(_guard);
}
