//! Stripe-merge compaction — integration tests over the shard-backed engine.
//!
//! Background: round-robin / multi-session striping spreads a table's PKs
//! across every write-stripe file. Each flushed file is PK-sorted, but its
//! zone map (catalog `column_stats` min/max) still spans nearly the whole PK
//! range — the ranges OVERLAP, so keyset pagination
//! (`WHERE id > $1 ORDER BY id LIMIT k`) can't prune and opens every live
//! file. The shard's stripe-merge sweep (`BASIN_STRIPE_MERGE_SECS`) merges
//! such overlapping cold files into fewer files with strictly DISJOINT PK
//! ranges.
//!
//! Covered here, end-to-end through pgwire (server boots exactly like
//! `insert_through_shard.rs`):
//!  1. multi-session + multi-round writes create >= 4 overlapping stripe
//!     files; the background merge tick reduces the live-file count, makes
//!     the per-file PK ranges disjoint (asserted from catalog
//!     `column_stats`), preserves row count/values, and keyset-pagination
//!     results are identical before/after the merge;
//!  2. a merge racing concurrent INSERT flushes loses nothing (OCC on
//!     `replace_data_files`: append-racers are retried, the data survives);
//!  3. `BASIN_STRIPE_MERGE_SECS=0` disables the sweep — config parses to a
//!     zero interval and overlapping files persist.
//!
//! All waits are deadline-polls against the catalog, never fixed sleeps.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, DataFileRef};
use basin_common::{ProjectId, TableName};
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _data_dir: TempDir,
    _wal_dir: TempDir,
    bg: Option<basin_shard::ShardBackgroundHandle>,
    wal: Arc<dyn basin_wal::Wal>,
    catalog: Arc<basin_catalog::InMemoryCatalog>,
    project: ProjectId,
    /// The shard config's effective stripe-merge cadence (zero = disabled).
    stripe_merge_interval: Duration,
}

/// Boot a shard-backed server (same wiring as `insert_through_shard.rs`).
/// `stripe_merge` overrides the merge cadence; `None` keeps whatever
/// `ShardConfig::new` derived from `BASIN_STRIPE_MERGE_SECS` (used by the
/// disable-via-env test).
async fn start_server_with_shard(stripe_merge: Option<Duration>) -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let data_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage_fs = LocalFileSystem::new_with_prefix(data_dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(storage_fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog = Arc::new(basin_catalog::InMemoryCatalog::new());
    let catalog_dyn: Arc<dyn basin_catalog::Catalog> = catalog.clone();

    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .expect("open WAL"),
    );

    let mut shard_cfg =
        basin_shard::ShardConfig::new(storage.clone(), catalog_dyn.clone(), wal.clone());
    if let Some(d) = stripe_merge {
        shard_cfg.stripe_merge_interval = d;
    }
    let stripe_merge_interval = shard_cfg.stripe_merge_interval;
    let shard = basin_shard::Shard::new(shard_cfg);
    let bg = shard.spawn_background();

    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog: catalog_dyn,
        shard: Some(shard),
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), project);
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
        _data_dir: data_dir,
        _wal_dir: wal_dir,
        bg: Some(bg),
        wal,
        catalog,
        project,
        stripe_merge_interval,
    }
}

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user={user} password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                Some(row)
            }
            _ => None,
        })
        .collect()
}

/// First cell of the first row as i64 (count(*)/sum(id) readback).
fn scalar_i64(msgs: &[SimpleQueryMessage]) -> i64 {
    let rows = rows_of(msgs);
    rows[0][0]
        .as_deref()
        .expect("non-null scalar")
        .parse::<f64>()
        .expect("numeric scalar") as i64
}

async fn live_files(server: &TestServer, table: &str) -> Vec<DataFileRef> {
    server
        .catalog
        .load_table(&server.project, &TableName::new(table).unwrap())
        .await
        .map(|m| m.live_data_files())
        .unwrap_or_default()
}

/// Decode each file's `id` zone map (8-byte LE i64, the writer's stat
/// contract for Int64). `None` when any live file lacks the stats.
fn id_ranges(files: &[DataFileRef]) -> Option<Vec<(i64, i64)>> {
    fn le_i64(b: &[u8]) -> Option<i64> {
        Some(i64::from_le_bytes(b.try_into().ok()?))
    }
    files
        .iter()
        .map(|f| {
            let cs = f.column_stats.get("id")?;
            Some((
                le_i64(cs.min_bytes.as_deref()?)?,
                le_i64(cs.max_bytes.as_deref()?)?,
            ))
        })
        .collect()
}

/// Strictly disjoint: sorted by min, every file's min is past the previous
/// file's max. This is exactly what lets `id > $1` prune to 1-2 files.
fn ranges_disjoint(ranges: &[(i64, i64)]) -> bool {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|r| r.0);
    sorted.windows(2).all(|w| w[1].0 > w[0].1)
}

fn ranges_overlap(ranges: &[(i64, i64)]) -> bool {
    !ranges_disjoint(ranges)
}

/// Insert `rounds * per_round` rows with ids interleaved ACROSS rounds
/// (round j gets `base + j, base + j + rounds, …`), forcing a flush
/// (`SELECT count(*)` trips the synchronous pre-SELECT compaction) after
/// every round so each round lands in its own cold file. Every round spans
/// nearly the same PK range, so the produced files are
/// deterministically-overlapping — no reliance on session-pid stripe
/// assignment. Returns the number of rows inserted.
async fn seed_interleaved_rounds(
    client: &tokio_postgres::Client,
    table: &str,
    base: i64,
    rounds: usize,
    per_round: usize,
) -> usize {
    for j in 0..rounds {
        let ids: Vec<i64> = (0..per_round)
            .map(|k| base + j as i64 + (k * rounds) as i64)
            .collect();
        for chunk in ids.chunks(10) {
            let values: Vec<String> = chunk.iter().map(|id| format!("({id}, 'b-{id}')")).collect();
            let sql = format!("INSERT INTO {table} VALUES {}", values.join(", "));
            client.simple_query(&sql).await.expect("seed insert");
        }
        // Flush this round's tail into its own cold file. A full scan is the
        // documented flush trigger (Option A tail visibility) — count(*) can
        // be answered from catalog metadata without draining the tail.
        client
            .simple_query(&format!("SELECT id FROM {table}"))
            .await
            .expect("flush select");
    }
    rounds * per_round
}

/// Multi-session seeding: `sessions` concurrent connections each insert the
/// id slice `{base + s + k*sessions}` in multi-row statements, then one
/// flush. With statement-affine striping the sessions land in distinct
/// stripe partitions, producing per-stripe files that all span
/// `[base, base+span)` — the historical overlapping layout this feature
/// exists to fix.
async fn seed_multi_session(
    addr: SocketAddr,
    table: &str,
    sessions: usize,
    per_session: usize,
    base: i64,
) -> usize {
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for s in 0..sessions {
        let table = table.to_string();
        set.spawn(async move {
            let client = connect(addr, "alice").await;
            let ids: Vec<i64> = (0..per_session)
                .map(|k| base + s as i64 + (k * sessions) as i64)
                .collect();
            for chunk in ids.chunks(10) {
                let values: Vec<String> = chunk
                    .iter()
                    .map(|id| format!("({id}, 's{s}-{id}')"))
                    .collect();
                let sql = format!("INSERT INTO {table} VALUES {}", values.join(", "));
                client
                    .simple_query(&sql)
                    .await
                    .unwrap_or_else(|e| panic!("session {s}: {e}"));
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("writer session panicked");
    }
    sessions * per_session
}

fn expected_keyset(total: i64, after: i64, limit: usize) -> Vec<String> {
    ((after + 1)..total)
        .take(limit)
        .map(|v| v.to_string())
        .collect()
}

async fn keyset_ids(
    client: &tokio_postgres::Client,
    table: &str,
    after: i64,
    limit: usize,
) -> Vec<String> {
    let res = client
        .simple_query(&format!(
            "SELECT id FROM {table} WHERE id > {after} ORDER BY id LIMIT {limit}"
        ))
        .await
        .expect("keyset query");
    rows_of(&res)
        .into_iter()
        .map(|r| r[0].clone().expect("non-null id"))
        .collect()
}

async fn shutdown(mut server: TestServer) {
    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();
}

/// 1. Multi-session + multi-round writes build >= 4 overlapping stripe
///    files; the background merge tick collapses them into fewer files with
///    strictly disjoint PK ranges; row count, sum, and keyset-pagination
///    results are identical before and after the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stripe_merge_reduces_files_to_disjoint_pk_ranges() {
    let server = start_server_with_shard(Some(Duration::from_millis(300))).await;
    let setup = connect(server.addr, "alice").await;

    setup
        .simple_query("CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL)")
        .await
        .expect("create table");

    // Phase A — the verified root cause: concurrent sessions interleave the
    // PK domain across stripe files. ids 0..600.
    let mut total = seed_multi_session(server.addr, "items", 6, 100, 0).await as i64;
    setup
        .simple_query("SELECT id FROM items")
        .await
        .expect("flush multi-session tails");

    // Phase B — deterministic overlapping rounds (ids 600..1000), so the
    // ">= 4 overlapping files" precondition never depends on stripe-pid
    // assignment.
    total += seed_interleaved_rounds(&setup, "items", total, 4, 100).await as i64;

    // Capture the pre-merge state. A merge tick may already have fired
    // mid-seed; top up with more overlapping rounds (bounded) until the
    // catalog shows >= 4 overlapping files, then snapshot.
    let deadline = Instant::now() + Duration::from_secs(30);
    let before_len = loop {
        let files = live_files(&server, "items").await;
        let ranges = id_ranges(&files).expect("flushed files carry id stats");
        if files.len() >= 4 && ranges_overlap(&ranges) {
            break files.len();
        }
        assert!(
            Instant::now() < deadline,
            "never reached >= 4 overlapping files: {} files, ranges {ranges:?}",
            files.len()
        );
        total += seed_interleaved_rounds(&setup, "items", total, 4, 100).await as i64;
    };

    // Keyset page before the merge (the query this feature accelerates).
    let before_page = keyset_ids(&setup, "items", 123, 7).await;
    assert_eq!(before_page, expected_keyset(total, 123, 7));

    // Wait for the merge tick: fewer live files, strictly disjoint PK
    // ranges, identical total row count in the catalog.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let files = live_files(&server, "items").await;
        let rows: u64 = files.iter().map(|f| f.row_count).sum();
        if let Some(ranges) = id_ranges(&files) {
            if files.len() < before_len && ranges_disjoint(&ranges) && rows == total as u64 {
                println!(
                    "[stripe_merge] merged {before_len} overlapping files -> {} disjoint: {ranges:?}",
                    files.len()
                );
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "stripe merge did not produce a reduced disjoint layout: \
             {} files (was {before_len}), rows {rows}/{total}, ranges {:?}",
            files.len(),
            id_ranges(&files),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Row count + values identical after the merge.
    let count = scalar_i64(
        &setup
            .simple_query("SELECT count(*) FROM items")
            .await
            .expect("count after merge"),
    );
    assert_eq!(count, total, "merge must not lose or duplicate rows");
    let sum = scalar_i64(
        &setup
            .simple_query("SELECT sum(id) FROM items")
            .await
            .expect("sum after merge"),
    );
    assert_eq!(
        sum,
        total * (total - 1) / 2,
        "id values must survive the merge"
    );

    // Keyset pagination returns the same page after the merge.
    let after_page = keyset_ids(&setup, "items", 123, 7).await;
    assert_eq!(after_page, before_page, "keyset page changed across merge");
    // And from deeper in the keyspace, spanning what used to be many files.
    let deep_page = keyset_ids(&setup, "items", total - 10, 20).await;
    assert_eq!(deep_page, expected_keyset(total, total - 10, 20));

    shutdown(server).await;
}

/// 2. A merge racing concurrent INSERT flushes loses nothing: tail flushes
///    APPEND files while the merge holds a snapshot of the old set;
///    optimistic concurrency on `replace_data_files` means the merge either
///    retries on top of the appended snapshot or abandons — either way every
///    row written during the race is still there, and once writes quiesce
///    the layout converges to disjoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stripe_merge_with_concurrent_inserts_loses_nothing() {
    // Merge any overlapping PAIR: with the default threshold (4) a tick that
    // fires mid-top-up could leave 2-3 trailing overlapping files that never
    // re-qualify, stalling the convergence poll below. Process-global, but
    // harmless to the sibling tests: the merge-enabled test tolerates any
    // threshold >= 2 (its preconditions only need ">= 4 overlapping files
    // exist"), and the disabled-via-env test never runs a sweep.
    std::env::set_var("BASIN_STRIPE_MERGE_MIN_FILES", "2");

    let server = start_server_with_shard(Some(Duration::from_millis(150))).await;
    let setup = connect(server.addr, "alice").await;

    setup
        .simple_query("CREATE TABLE busy (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL)")
        .await
        .expect("create table");

    // Seed an immediately-mergeable overlapping layout (ids 0..200) so merge
    // passes start firing while the writers below are still appending.
    let seeded = seed_interleaved_rounds(&setup, "busy", 0, 4, 50).await as i64;

    // Concurrent writers: 4 sessions × 5 iterations × 20 rows (ids 200..600,
    // interleaved across sessions so their flushed files also overlap). Each
    // iteration's SELECT forces a tail flush — new files APPEND to the
    // catalog mid-merge, exercising the CommitConflict path.
    const WRITERS: usize = 4;
    const ITERS: usize = 5;
    const ROWS_PER_ITER: usize = 20;
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for t in 0..WRITERS {
        let addr = server.addr;
        set.spawn(async move {
            let client = connect(addr, "alice").await;
            for i in 0..ITERS {
                let ids: Vec<i64> = (0..ROWS_PER_ITER)
                    .map(|k| 200 + t as i64 + ((i * ROWS_PER_ITER + k) * WRITERS) as i64)
                    .collect();
                for chunk in ids.chunks(10) {
                    let values: Vec<String> = chunk
                        .iter()
                        .map(|id| format!("({id}, 'w{t}-{id}')"))
                        .collect();
                    client
                        .simple_query(&format!("INSERT INTO busy VALUES {}", values.join(", ")))
                        .await
                        .unwrap_or_else(|e| panic!("writer {t} iter {i}: {e}"));
                }
                // Force a flush (full scan) so a fresh cold file lands
                // during the merges.
                client
                    .simple_query("SELECT id FROM busy")
                    .await
                    .unwrap_or_else(|e| panic!("writer {t} flush {i}: {e}"));
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("writer panicked");
    }
    let mut total = seeded + (WRITERS * ITERS * ROWS_PER_ITER) as i64; // ids 0..600 dense

    // Nothing lost during the concurrent phase.
    let count = scalar_i64(
        &setup
            .simple_query("SELECT count(*) FROM busy")
            .await
            .expect("count after concurrent phase"),
    );
    assert_eq!(
        count, total,
        "rows lost/duplicated while merges raced flushes"
    );

    // Quiesce + top up with one more guaranteed-overlapping batch (the
    // post-race file set might be < 4 files, under the merge threshold), then
    // the layout must converge to fully disjoint with every row accounted for.
    total += seed_interleaved_rounds(&setup, "busy", total, 4, 50).await as i64;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let files = live_files(&server, "busy").await;
        let rows: u64 = files.iter().map(|f| f.row_count).sum();
        if let Some(ranges) = id_ranges(&files) {
            if ranges_disjoint(&ranges) && rows == total as u64 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "post-quiesce layout never converged: {} files, rows {rows}/{total}, ranges {:?}",
            files.len(),
            id_ranges(&files),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let count = scalar_i64(
        &setup
            .simple_query("SELECT count(*) FROM busy")
            .await
            .expect("final count"),
    );
    assert_eq!(count, total);
    let sum = scalar_i64(
        &setup
            .simple_query("SELECT sum(id) FROM busy")
            .await
            .expect("final sum"),
    );
    assert_eq!(
        sum,
        total * (total - 1) / 2,
        "id values must survive merges"
    );
    let page = keyset_ids(&setup, "busy", 42, 9).await;
    assert_eq!(page, expected_keyset(total, 42, 9));

    shutdown(server).await;
}

/// 3. `BASIN_STRIPE_MERGE_SECS=0` disables the sweep: the config parses to a
///    zero interval and an overlapping file layout persists across an
///    observation window (polled, never reduced).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stripe_merge_disabled_via_env_zero() {
    // Process-global, but no other test in this binary reads the variable:
    // they override `ShardConfig::stripe_merge_interval` explicitly after
    // `ShardConfig::new`, so the value set here cannot leak into them.
    std::env::set_var("BASIN_STRIPE_MERGE_SECS", "0");

    let server = start_server_with_shard(None).await;
    assert!(
        server.stripe_merge_interval.is_zero(),
        "BASIN_STRIPE_MERGE_SECS=0 must parse to a zero (disabled) interval, got {:?}",
        server.stripe_merge_interval
    );

    let setup = connect(server.addr, "alice").await;
    setup
        .simple_query("CREATE TABLE frozen (id BIGINT NOT NULL PRIMARY KEY, body TEXT NOT NULL)")
        .await
        .expect("create table");

    let total = seed_interleaved_rounds(&setup, "frozen", 0, 4, 50).await as i64;

    let files = live_files(&server, "frozen").await;
    let ranges = id_ranges(&files).expect("flushed files carry id stats");
    assert!(
        files.len() >= 4,
        "expected >= 4 stripe files, got {}",
        files.len()
    );
    assert!(
        ranges_overlap(&ranges),
        "seed layout must overlap: {ranges:?}"
    );
    let baseline: std::collections::BTreeSet<String> =
        files.iter().map(|f| f.path.clone()).collect();

    // Observation window: poll the catalog; the live file set must never
    // shrink or change — the sweep is disabled.
    let deadline = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < deadline {
        let now: std::collections::BTreeSet<String> = live_files(&server, "frozen")
            .await
            .iter()
            .map(|f| f.path.clone())
            .collect();
        assert_eq!(
            now, baseline,
            "live file set changed while stripe merge is disabled"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let count = scalar_i64(
        &setup
            .simple_query("SELECT count(*) FROM frozen")
            .await
            .expect("count"),
    );
    assert_eq!(count, total);

    shutdown(server).await;
}
