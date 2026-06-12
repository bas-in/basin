//! Full 1M-row S3 mirror of the LocalFS `compare_postgres` shape.
//!
//! `s3_compare_postgres.rs` scaled rows down to 100K to keep cloud runs short
//! and ran the Basin WAL in RAM (an `InMemory` store) — which makes the insert
//! number understate durable-write cost, because nothing is flushed to the
//! object store before the ack. This card closes both gaps for the SeaweedFS
//! dashboard:
//!
//!   * **1M rows** — the same scale as the headline LocalFS card, so disk and
//!     point-query ratios are comparable across dashboards.
//!   * **`sync_insert` variant** — a second Basin path whose WAL lives on the
//!     SAME real object store and is `flush()`ed before each batch is counted,
//!     measuring the true cost of a durable write to S3 (vs the in-RAM WAL the
//!     default-insert metric uses). The report flagged the in-RAM WAL as a
//!     correctness caveat; this surfaces both numbers side by side.
//!
//! Output: `data_seaweedfs/compare_postgres_1m.json` (the `report_real_*`
//! helpers honour `BASIN_BENCHMARK_DIR`; `benchmark/run` points it at
//! `benchmark/data_seaweedfs`). Skips cleanly when `[s3]` or `[postgres]` is
//! missing. Registered in `benchmark/run/_lib.sh`'s `SEAWEEDFS_COMPARE` array.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_postgres_compare, CompareMetric, WhichWins};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_compare_postgres_1m";
const ROWS: usize = 1_000_000;
const INSERT_BATCH: usize = 10_000;
/// Number of rows the `sync_insert` (durable-WAL-to-S3) variant measures. The
/// full 1M durable run would dominate the suite wall-clock; a bounded slice is
/// enough to surface the per-batch durable-write cost the in-RAM WAL hides.
const SYNC_ROWS: usize = 50_000;

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

async fn pg_connect(
    pg_cfg: &basin_integration_tests::test_config::PostgresConfig,
) -> Option<(Client, String)> {
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

const CARD_NAME: &str = "Basin (real S3) vs Postgres 18 (no index, 1M rows)";
const CARD_CLAIM: &str = "On audit-log data at 1M rows, Basin uses much less disk than Postgres heap \
     and matches or beats unindexed point queries — even paying the S3 round-trip. \
     The `sync_insert` metric is the durable-write cost with the WAL flushed to S3 \
     before ack (the default insert metric runs the WAL in RAM).";

/// Build a Basin engine over the given object store. `wal_store` is where the
/// WAL is persisted — `InMemory` for the default (in-RAM) path, or the real S3
/// store for the durable `sync_insert` path.
async fn build_engine(
    object_store: Arc<dyn object_store::ObjectStore>,
    run_prefix: &str,
    wal_store: Arc<dyn object_store::ObjectStore>,
) -> (
    Engine,
    basin_storage::Storage,
    Arc<dyn basin_wal::Wal>,
    basin_shard::ShardBackgroundHandle,
) {
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix)),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: wal_store,
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
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
    (engine, storage, wal, bg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_compare_postgres_1m() {
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
                "postgres_1m",
                CARD_NAME,
                CARD_CLAIM,
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
            println!("[S3 compare_postgres_1m] postgres unreachable: skipping");
            report_real_postgres_compare(
                "postgres_1m",
                CARD_NAME,
                CARD_CLAIM,
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

    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_compare_s3_1m_{}", suffix);
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
    assert!(
        !pg_point_ms.is_empty(),
        "no PG execution time samples parsed"
    );
    let pg_point_p50 = median(&pg_point_ms);

    // ---- Basin path on real S3 (default insert: WAL in RAM) ----------------
    // WAL stays in RAM — same rationale as s3_shard_insert_path; the durable
    // cost is captured separately by the `sync_insert` variant below.
    let wal_ram: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let (engine, storage, wal, bg) =
        build_engine(object_store.clone(), &run_prefix, wal_ram).await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
    )
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

    // Basin disk: sum the size_bytes of every data object under the run prefix.
    let basin_disk_bytes: u64 = {
        use futures::StreamExt as _;
        let prefix = ObjectPath::from(run_prefix.as_str());
        let store_handle = storage.object_store_handle();
        let mut stream = store_handle.list(Some(&prefix));
        let mut total = 0u64;
        while let Some(meta) = stream.next().await {
            match meta {
                Ok(m) if {
                    let loc = m.location.as_ref();
                    loc.ends_with(".vortex") || loc.ends_with(".parquet")
                } =>
                {
                    total += m.size as u64;
                }
                Ok(_) => {}
                Err(e) => eprintln!("list error during disk size: {e}"),
            }
        }
        total
    };

    bg.shutdown().await;
    wal.close().await.unwrap();

    // ---- sync_insert variant: WAL durably flushed to the REAL store --------
    // Same insert shape over a bounded slice, but the WAL store is the real S3
    // store and every batch is `flush()`ed before it's counted — measuring the
    // durable-write latency the in-RAM WAL hides. Normalised to a per-1K-rows
    // figure so it's comparable regardless of `SYNC_ROWS`.
    let sync_prefix = format!("{run_prefix}/sync");
    let wal_s3: Arc<dyn object_store::ObjectStore> = object_store.clone();
    let (sync_engine, _sync_storage, sync_wal, sync_bg) =
        build_engine(object_store.clone(), &sync_prefix, wal_s3).await;
    let sync_project = ProjectId::new();
    let sync_sess = sync_engine.open_session(sync_project).await.unwrap();
    sync_sess
        .execute(
            "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
        )
        .await
        .unwrap();
    sync_wal.flush().await.expect("flush after DDL");

    let sync_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < SYNC_ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str("INSERT INTO events VALUES ");
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        sync_sess.execute(&stmt).await.expect("sync insert batch");
        // Durability point: the batch is not "done" until the WAL is on the
        // real object store.
        sync_wal.flush().await.expect("durable wal flush");
        row_idx += INSERT_BATCH as i64;
    }
    let sync_elapsed_ms = sync_started.elapsed().as_secs_f64() * 1000.0;
    // Normalise to ms per 1K rows so the headline is scale-free.
    let sync_per_1k_ms = sync_elapsed_ms / (SYNC_ROWS as f64 / 1000.0);
    let ram_per_1k_ms = basin_insert_ms / (ROWS as f64 / 1000.0);

    sync_bg.shutdown().await;
    sync_wal.close().await.unwrap();

    let basin_mib = basin_disk_bytes as f64 / (1024.0 * 1024.0);
    let pg_mib = pg_disk_bytes as f64 / (1024.0 * 1024.0);
    let disk_ratio = pg_disk_bytes as f64 / basin_disk_bytes.max(1) as f64;
    let point_ratio = pg_point_p50 / basin_point_p50.max(1e-9);

    println!(
        "{:>24} {:>15} {:>15} {:>20}",
        "metric", "basin (s3)", "postgres", "ratio"
    );
    println!(
        "{:>24} {:>13.2}MiB {:>13.2}MiB {:>20}",
        "on_disk_bytes",
        basin_mib,
        pg_mib,
        format!("pg/basin = {:.2}x", disk_ratio)
    );
    println!(
        "{:>24} {:>15.0} {:>15.0} {:>20}",
        "insert_total_ms", basin_insert_ms, pg_insert_ms, "-"
    );
    println!(
        "{:>24} {:>15.2} {:>15.2} {:>20}",
        "point_query_ms_p50",
        basin_point_p50,
        pg_point_p50,
        format!("pg/basin = {:.2}x", point_ratio)
    );
    println!(
        "[S3 compare_postgres_1m] sync (durable WAL→S3) = {:.2} ms/1K rows; \
         in-RAM WAL = {:.2} ms/1K rows ({:.1}x durability tax)",
        sync_per_1k_ms,
        ram_per_1k_ms,
        sync_per_1k_ms / ram_per_1k_ms.max(1e-9),
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
            label: "Insert 1M rows".into(),
            basin: basin_insert_ms,
            postgres: pg_insert_ms,
            unit: "ms".into(),
            better: which_wins(basin_insert_ms, pg_insert_ms),
            ratio_text: None,
        },
        // Basin-only variant: durable-WAL-to-S3 insert cost, per 1K rows. PG's
        // comparable figure is its own per-1K insert rate (it acks durably by
        // default), so we set the PG side to its measured per-1K rate.
        CompareMetric {
            label: "Sync insert (durable WAL→S3), per 1K rows".into(),
            basin: sync_per_1k_ms,
            postgres: pg_insert_ms / (ROWS as f64 / 1000.0),
            unit: "ms".into(),
            better: which_wins(sync_per_1k_ms, pg_insert_ms / (ROWS as f64 / 1000.0)),
            ratio_text: Some(format!(
                "in-RAM WAL = {:.2} ms/1K; durable = {:.2} ms/1K",
                ram_per_1k_ms, sync_per_1k_ms
            )),
        },
    ];

    report_real_postgres_compare(
        "postgres_1m",
        CARD_NAME,
        CARD_CLAIM,
        true,
        metrics,
        Some(
            "Basin storage on real S3; PG on local 18. Default insert: WAL in RAM. \
             sync_insert: WAL on the real S3 store, flushed before ack (durable).",
        ),
    );

    drop(_guard);
}
