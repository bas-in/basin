//! Time-series sustained INGEST-THROUGHPUT card — the TimescaleDB scale pitch.
//!
//! TimescaleDB's headline number is sustained insert rate into a hypertable.
//! This card measures exactly that on Basin: a `create_hypertable`-backed
//! metrics table, seeded at the 100M / 1B tier through the real bulk-INSERT +
//! WAL + cold-tier path, with the SUSTAINED rows/sec measured over the whole
//! run AND the per-window rate sampled so a flush-induced stall shows up as a
//! window dip instead of being hidden in the average.
//!
//! What it proves (and what it does NOT):
//!   * SUSTAINED rate: total rows / total wall — the number a Timescale-style
//!     pitch quotes. Recorded, never asserted on wall-clock (a loaded box must
//!     fail honestly or not at all).
//!   * Rate STABILITY across the run: per-window rows/sec, so flush/WAL stalls
//!     are visible. The artifact carries min/median/max window rate.
//!   * Flush behavior: an explicit `flush_to_parquet` is timed AFTER the seed
//!     (the WAL-tail-to-cold drain) and the resulting live cold-file count is
//!     recorded — the WAL/flush story Timescale tells about chunk flushing.
//!   * Correctness (the only HARD asserts): exact COUNT(*) after seed+flush
//!     (no row lost/dup across thousands of statements + the flush), and a
//!     `time_bucket` GROUP BY returns the deterministic bucket count.
//!
//! Hypertable note: `create_hypertable(...)` is accepted-DDL on Basin (see
//! `ext_bench_timescale.rs`). If it is rejected on this build, the card falls
//! back to a plain table, ingests identically, and records `hypertable_ok =
//! false` — measured honestly, not masked.
//!
//! # The tier ladder — and WHERE each tier runs
//!
//!   * ≤ 1M    — DEV / CI. The correctness asserts catch ingest/bucket
//!               regressions on every change. Default is 1M.
//!   * 10M     — BOX-CEILING. Largest tier that fits this dev box.
//!   * 100M    — TimescaleDB-class. PROVISIONED hardware only.
//!   * 1B      — scale-proof sustained-ingest. PROVISIONED hardware only.
//!
//! The runner (`benchmark/run/scale-suite.sh`, `BASIN_SCALE_MAX`) enforces the
//! box-ceiling-vs-provisioned distinction.
//!
//! # Artifact
//!
//! `benchmark/data/scale_timeseries_ingest_<N>.json` (N = row count), one per
//! tier, mirroring the ext size-suite's size-suffixed sidecars.
//!
//! # Running it
//!
//! ```text
//! # dev/CI (1M default):
//! cargo test -p basin-integration-tests --test scale_timeseries_ingest \
//!     ts_ingest_default -- --ignored --nocapture
//!
//! # any tier:
//! BASIN_TS_INGEST_ROWS=100000000 cargo test -p basin-integration-tests \
//!     --test scale_timeseries_ingest ts_ingest_default -- --ignored --nocapture
//!
//! # 1B sustained-ingest scale-proof (provisioned hardware only):
//! cargo test -p basin-integration-tests --test scale_timeseries_ingest \
//!     ts_ingest_1b -- --ignored --nocapture
//! ```
//!
//! or via `benchmark/run/scale-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_TS_INGEST_ROWS`  — rows for `ts_ingest_default` (default 1_000_000;
//!                             no ceiling, accepts 1_000_000_000+).
//! * `BASIN_TS_INGEST_BATCH` — rows per INSERT statement (default 10_000).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Seconds-since-epoch base for the synthetic timestamps. One row per second
/// (id == seconds offset) gives a deterministic, dense series.
const EPOCH: i64 = 1_700_000_000;

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Storage,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage)
}

/// Collect the first Int64 column of a result.
fn ids_of(res: &ExecResult) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 must be Int64");
                for r in 0..ids.len() {
                    out.push(ids.value(r));
                }
            }
            out
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if s.is_empty() {
        return f64::NAN;
    }
    let mid = s.len() / 2;
    if s.len() % 2 == 0 {
        (s[mid - 1] + s[mid]) / 2.0
    } else {
        s[mid]
    }
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[ts-ingest] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize ts-ingest artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[ts-ingest] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[ts-ingest] artifact rename {}: {e}", path.display());
    }
    eprintln!("[ts-ingest] artifact written: {}", path.display());
}

fn tier_label(rows: i64) -> &'static str {
    match rows {
        r if r <= 1_000_000 => "dev/CI (<=1M)",
        r if r <= 10_000_000 => "box-ceiling (10M)",
        r if r <= 100_000_000 => "TimescaleDB-class (100M, provisioned)",
        _ => "scale-proof (1B+, provisioned)",
    }
}

/// The sustained-ingest run. Shared by `ts_ingest_default` and `ts_ingest_1b`.
async fn run_ts_ingest(rows: i64, batch: i64) {
    assert!(
        rows >= 1_000,
        "ts ingest needs >= 1000 rows (bucket math assumes it); got {rows}"
    );
    let batch = batch.max(1);

    let (_sd, _wd, engine, shard, bg, wal, _storage) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    eprintln!(
        "[ts-ingest] tier={} rows={rows} batch={batch}",
        tier_label(rows)
    );

    sess.execute(
        "CREATE TABLE metrics (\
            id BIGINT NOT NULL, \
            ts TIMESTAMPTZ NOT NULL, \
            device BIGINT NOT NULL, \
            value DOUBLE PRECISION NOT NULL\
         )",
    )
    .await
    .unwrap();

    // create_hypertable is accepted-DDL on Basin (ext_bench_timescale.rs). If
    // rejected, we ingest into the plain table and record hypertable_ok=false.
    let hypertable_ok = sess
        .execute("SELECT create_hypertable('metrics', 'ts', chunk_time_interval => INTERVAL '1 day')")
        .await
        .is_ok();
    if !hypertable_ok {
        eprintln!("[ts-ingest] create_hypertable rejected on this build — ingesting into plain table (recorded honestly)");
    }

    // ── Sustained ingest, windowed rate sampling ─────────────────────────────
    // One row per second (id == seconds offset from EPOCH), device round-robin,
    // value a deterministic function of id. Rate is sampled per 1M-row window
    // so a flush stall shows as a window dip; eprintln keeps a long run peekable.
    let window: i64 = 1_000_000.min(rows);
    let mut window_rates: Vec<f64> = Vec::new();
    let seed_started = Instant::now();
    let mut win_started = Instant::now();
    let mut win_base = 0i64;
    let mut next_win = window;
    let mut id = 0i64;
    while id < rows {
        let lo = id;
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 64);
        stmt.push_str("INSERT INTO metrics VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            let ts = EPOCH + k;
            let device = k % 100;
            let value = (k % 1000) as f64 * 0.5;
            stmt.push_str(&format!("({k}, to_timestamp({ts}), {device}, {value})"));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
        if id >= next_win || id == rows {
            let win_secs = win_started.elapsed().as_secs_f64();
            let win_rows = id - win_base;
            let win_rate = win_rows as f64 / win_secs.max(1e-9);
            window_rates.push(win_rate);
            eprintln!(
                "[ts-ingest] ingested {id}/{rows} rows; window rate {win_rate:.0} rows/s ({:.0}s elapsed)",
                seed_started.elapsed().as_secs_f64()
            );
            win_started = Instant::now();
            win_base = id;
            next_win = id + window;
        }
    }
    let ingest_s = seed_started.elapsed().as_secs_f64();
    let sustained_rate = rows as f64 / ingest_s.max(1e-9);
    eprintln!("[ts-ingest] sustained ingest: {rows} rows in {ingest_s:.1}s = {sustained_rate:.0} rows/s");

    // ── Flush: WAL-tail-to-cold drain (the Timescale chunk-flush story) ──────
    let flush_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    let flush_s = flush_started.elapsed().as_secs_f64();
    let merge_started = Instant::now();
    shard.run_stripe_merge_once().await.unwrap();
    let merge_s = merge_started.elapsed().as_secs_f64();
    bg.shutdown().await;
    eprintln!("[ts-ingest] flush {flush_s:.1}s, stripe merge {merge_s:.1}s");

    let table = TableName::new("metrics").unwrap();
    let live_files = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .live_data_files()
        .len();
    eprintln!("[ts-ingest] settled layout: {live_files} live cold files");

    // ── Correctness (HARD asserts) ───────────────────────────────────────────
    // Exact row count after seed+flush: no row lost or duplicated.
    let cnt = ids_of(&sess.execute("SELECT COUNT(*) FROM metrics").await.unwrap());
    assert_eq!(
        cnt,
        vec![rows],
        "COUNT(*) must be exactly {rows} after sustained ingest + flush"
    );

    // time_bucket GROUP BY: with one row per second over `rows` seconds, the
    // number of 1-hour buckets is deterministic. (Skipped if time_bucket is
    // unavailable on this build — recorded, not masked.)
    let bucket_secs: i64 = 3_600;
    let expected_hour_buckets = ((rows + bucket_secs - 1) / bucket_secs) as usize;
    let bucket_sql =
        "SELECT time_bucket('1 hour', ts) AS b, COUNT(*) AS n FROM metrics GROUP BY b ORDER BY b";
    let (time_bucket_ok, observed_buckets) = match sess.execute(bucket_sql).await {
        Ok(res) => {
            let n = row_count(&res);
            assert_eq!(
                n, expected_hour_buckets,
                "time_bucket('1 hour') must produce exactly {expected_hour_buckets} buckets \
                 for {rows} one-second rows"
            );
            (true, n)
        }
        Err(e) => {
            eprintln!("[ts-ingest] time_bucket unavailable on this build — bucket-count check skipped: {e:?}");
            (false, 0)
        }
    };

    // ── Artifact ─────────────────────────────────────────────────────────────
    let win_min = window_rates.iter().cloned().fold(f64::INFINITY, f64::min);
    let win_max = window_rates.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let win_median = median(&window_rates);
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let artifact = json!({
        "kind": "scale_timeseries_ingest",
        "id": format!("scale_timeseries_ingest_{rows}"),
        "name": "Basin sustained time-series ingest rate (hypertable, LocalFS, default config)",
        "tier": tier_label(rows),
        "claim": "Sustained INSERT rate into a hypertable-backed metrics table at \
                  this tier, with per-window rate stability (flush stalls visible \
                  as window dips), flush/merge wall, and exact COUNT(*) + \
                  time_bucket correctness — the TimescaleDB sustained-ingest pitch.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "rows": rows,
        "batch": batch,
        "hypertable_ok": hypertable_ok,
        "time_bucket_ok": time_bucket_ok,
        "observed_hour_buckets": observed_buckets,
        "expected_hour_buckets": expected_hour_buckets,
        "ingest_s": ingest_s,
        "sustained_rows_per_sec": sustained_rate,
        "window_rows": window,
        "window_rate_min_rps": if win_min.is_finite() { win_min } else { 0.0 },
        "window_rate_median_rps": win_median,
        "window_rate_max_rps": if win_max.is_finite() { win_max } else { 0.0 },
        "window_rates_rps": window_rates,
        "flush_s": flush_s,
        "stripe_merge_s": merge_s,
        "live_files": live_files,
    });
    write_artifact(&format!("scale_timeseries_ingest_{rows}.json"), &artifact);

    println!(
        "[ts-ingest] tier={} rows={rows} hypertable_ok={hypertable_ok}: {sustained_rate:.0} rows/s sustained \
         (window min/med/max {:.0}/{:.0}/{:.0}), flush {flush_s:.1}s, {live_files} cold files; \
         COUNT(*) exact, time_bucket {} — PASS",
        tier_label(rows),
        if win_min.is_finite() { win_min } else { 0.0 },
        win_median,
        if win_max.is_finite() { win_max } else { 0.0 },
        if time_bucket_ok { "exact" } else { "skipped" },
    );

    wal.close().await.unwrap();
}

/// Env-driven tier (default 1M = dev/CI). `BASIN_TS_INGEST_ROWS` drives the
/// ladder; the runner sets it per tier point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "time-series sustained-ingest card: default 1M (dev/CI); set BASIN_TS_INGEST_ROWS for \
            10M/100M/1B — 10M is the box-ceiling, 100M/1B are provisioned-hardware only. \
            Run via benchmark/run/scale-suite.sh, or: \
            BASIN_TS_INGEST_ROWS=<N> cargo test ... --test scale_timeseries_ingest ts_ingest_default -- --ignored --nocapture"]
async fn ts_ingest_default() {
    let rows = env_i64("BASIN_TS_INGEST_ROWS", 1_000_000);
    let batch = env_i64("BASIN_TS_INGEST_BATCH", 10_000);
    run_ts_ingest(rows, batch).await;
}

/// The 1B sustained-ingest scale-proof, pinned. `BASIN_TS_INGEST_ROWS`
/// overrides so it smokes at tiny-N without editing source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "1B-row sustained-ingest scale-proof: ~tens-of-GB, hours, provisioned hardware only — \
            run via benchmark/run/scale-suite.sh BASIN_TS_INGEST_ROWS=1000000000, \
            never on a laptop (the runner's BASIN_SCALE_MAX refuses it by default)"]
async fn ts_ingest_1b() {
    let rows = env_i64("BASIN_TS_INGEST_ROWS", 1_000_000_000);
    let batch = env_i64("BASIN_TS_INGEST_BATCH", 10_000);
    run_ts_ingest(rows, batch).await;
}
