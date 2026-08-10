//! Parity test: INSERT VALUES and COPY FROM STDIN must produce identical rows.
//!
//! Inserts 1000 rows into `t_insert` via `INSERT INTO … VALUES (…)` and the
//! same 1000 rows into `t_copy` via `COPY … FROM STDIN WITH (FORMAT CSV)`,
//! then asserts that every value in both tables is identical row-for-row
//! (order-insensitive, joining on `id`).
//!
//! Column types exercised:
//!   id         BIGINT NOT NULL PRIMARY KEY
//!   user_id    BIGINT
//!   amount     DOUBLE PRECISION
//!   label      TEXT
//!   score      INTEGER
//!   active     BOOLEAN
//!   ratio      REAL
//!
//! Run:
//!   cargo test -p basin-integration-tests --test copy_insert_parity -- --nocapture

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use bytes::Bytes;
use futures::SinkExt;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::NoTls;

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let alice = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

const ROWS: i64 = 1000;
const DDL: &str = "CREATE TABLE %TABLE% (
        id       BIGINT NOT NULL PRIMARY KEY,
        user_id  BIGINT,
        amount   DOUBLE PRECISION,
        label    TEXT,
        score    INTEGER,
        active   BOOLEAN,
        ratio    REAL
    )";

// ---------------------------------------------------------------------------
// Row value helpers
// ---------------------------------------------------------------------------

fn user_id(k: i64) -> Option<i64> {
    if k % 7 == 0 {
        None
    } else {
        Some(k % 500)
    }
}

fn amount(k: i64) -> Option<f64> {
    if k % 11 == 0 {
        None
    } else {
        Some(k as f64 * 0.25)
    }
}

fn label(k: i64) -> Option<String> {
    if k % 13 == 0 {
        None
    } else {
        Some(format!("label-{}", k % 100))
    }
}

fn score(k: i64) -> Option<i32> {
    if k % 17 == 0 {
        None
    } else {
        Some((k % 1000) as i32)
    }
}

fn active(k: i64) -> Option<bool> {
    if k % 19 == 0 {
        None
    } else {
        Some(k % 2 == 0)
    }
}

fn ratio(k: i64) -> Option<f32> {
    if k % 23 == 0 {
        None
    } else {
        Some((k as f32) * 0.01)
    }
}

// ---------------------------------------------------------------------------
// Row containers for comparison
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
struct Row {
    id: i64,
    user_id: Option<i64>,
    amount: Option<f64>,
    label: Option<String>,
    score: Option<i32>,
    active: Option<bool>,
    ratio: Option<f32>,
}

// ---------------------------------------------------------------------------
// Test: INSERT vs COPY parity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_and_insert_produce_identical_rows() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Create both tables.
    client
        .simple_query(&DDL.replace("%TABLE%", "t_insert"))
        .await
        .expect("CREATE TABLE t_insert");
    client
        .simple_query(&DDL.replace("%TABLE%", "t_copy"))
        .await
        .expect("CREATE TABLE t_copy");

    // ---- INSERT path --------------------------------------------------------
    // Use batches of 100 rows in a single VALUES(...) statement per batch.
    let batch = 100i64;
    let mut id = 0i64;
    while id < ROWS {
        let hi = (id + batch).min(ROWS);
        let mut stmt = String::with_capacity((hi - id) as usize * 120);
        stmt.push_str("INSERT INTO t_insert VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            // Nullable columns written as literal NULL or the value.
            let uid_s = match user_id(k) {
                None => "NULL".to_owned(),
                Some(v) => v.to_string(),
            };
            let amt_s = match amount(k) {
                None => "NULL".to_owned(),
                Some(v) => format!("{v}"),
            };
            let lbl_s = match label(k) {
                None => "NULL".to_owned(),
                Some(ref v) => format!("'{v}'"),
            };
            let sc_s = match score(k) {
                None => "NULL".to_owned(),
                Some(v) => v.to_string(),
            };
            let act_s = match active(k) {
                None => "NULL".to_owned(),
                Some(v) => if v { "true" } else { "false" }.to_owned(),
            };
            let rat_s = match ratio(k) {
                None => "NULL".to_owned(),
                Some(v) => format!("{v}"),
            };
            stmt.push_str(&format!(
                "({k},{uid_s},{amt_s},{lbl_s},{sc_s},{act_s},{rat_s})"
            ));
        }
        client.simple_query(&stmt).await.expect("INSERT batch");
        id = hi;
    }

    // ---- COPY path ----------------------------------------------------------
    // Build a single CSV payload and push it through copy_in.
    let mut csv = String::with_capacity(ROWS as usize * 80);
    for k in 0..ROWS {
        let uid_s = match user_id(k) {
            None => String::new(),
            Some(v) => v.to_string(),
        };
        let amt_s = match amount(k) {
            None => String::new(),
            Some(v) => format!("{v}"),
        };
        let lbl_s = match label(k) {
            None => String::new(),
            Some(v) => v,
        };
        let sc_s = match score(k) {
            None => String::new(),
            Some(v) => v.to_string(),
        };
        let act_s = match active(k) {
            None => String::new(),
            Some(v) => if v { "true" } else { "false" }.to_owned(),
        };
        let rat_s = match ratio(k) {
            None => String::new(),
            Some(v) => format!("{v}"),
        };
        csv.push_str(&format!(
            "{k},{uid_s},{amt_s},{lbl_s},{sc_s},{act_s},{rat_s}\n"
        ));
    }

    let sink = client
        .copy_in::<_, Bytes>("COPY t_copy FROM STDIN WITH (FORMAT CSV, NULL '')")
        .await
        .expect("copy_in start");
    futures::pin_mut!(sink);
    sink.send(Bytes::from(csv.into_bytes()))
        .await
        .expect("send copy data");
    let n = sink.as_mut().finish().await.expect("finish copy_in");
    assert_eq!(n, ROWS as u64, "COPY CommandComplete tag count");

    // ---- Read both tables ---------------------------------------------------
    async fn read_table(client: &tokio_postgres::Client, table: &str) -> HashMap<i64, Row> {
        let rows = client
            .query(
                &format!(
                    "SELECT id, user_id, amount, label, score, active, ratio \
                     FROM {table} ORDER BY id"
                ),
                &[],
            )
            .await
            .expect("SELECT");
        rows.into_iter()
            .map(|r| {
                let id: i64 = r.get(0);
                (
                    id,
                    Row {
                        id,
                        user_id: r.get(1),
                        amount: r.get(2),
                        label: r.get(3),
                        score: r.get(4),
                        active: r.get(5),
                        ratio: r.get(6),
                    },
                )
            })
            .collect()
    }

    let insert_rows = read_table(&client, "t_insert").await;
    let copy_rows = read_table(&client, "t_copy").await;

    assert_eq!(
        insert_rows.len(),
        ROWS as usize,
        "t_insert should have {ROWS} rows"
    );
    assert_eq!(
        copy_rows.len(),
        ROWS as usize,
        "t_copy should have {ROWS} rows"
    );

    // ---- Compare row-by-row -------------------------------------------------
    let mut mismatches = 0usize;
    for k in 0..ROWS {
        let ir = insert_rows.get(&k).expect("insert row missing for id={k}");
        let cr = copy_rows.get(&k).expect("copy row missing for id={k}");

        // f64 compare: allow tiny epsilon from text-float-text round-trip.
        let amount_ok = match (ir.amount, cr.amount) {
            (None, None) => true,
            (Some(a), Some(b)) => (a - b).abs() < 1e-9,
            _ => false,
        };
        let ratio_ok = match (ir.ratio, cr.ratio) {
            (None, None) => true,
            (Some(a), Some(b)) => (a - b).abs() < 1e-4,
            _ => false,
        };

        let row_ok = ir.id == cr.id
            && ir.user_id == cr.user_id
            && amount_ok
            && ir.label == cr.label
            && ir.score == cr.score
            && ir.active == cr.active
            && ratio_ok;

        if !row_ok {
            eprintln!("mismatch at id={k}:");
            eprintln!("  INSERT: {ir:?}");
            eprintln!("  COPY  : {cr:?}");
            mismatches += 1;
        }
    }

    assert_eq!(
        mismatches, 0,
        "{mismatches} row(s) differed between INSERT and COPY"
    );
}
