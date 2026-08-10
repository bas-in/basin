//! Noisy-tenant SATURATION card — adversarial single-tenant load vs a victim.
//!
//! The fleet ladder (`multi_project_fleet_scale.rs`) spreads noisy load across
//! a FRACTION of the fleet. This card is the opposite, adversarial shape: ONE
//! project drives PATHOLOGICAL load — huge GIN churn (high-cardinality JSONB
//! keys so posting lists explode), a deep overlay backlog (bulk JSONB UPDATEs
//! every iteration, never letting the reconciler drain), and a connection
//! flood (many concurrent sessions on the one noisy project) — while a single
//! latency-sensitive victim project is measured.
//!
//! This is the worst case for the per-project isolation P0 fixes: if the GIN
//! posting budget partition, the PkRowCache waterline, and the reconciler
//! round-robin work, ONE tenant cannot starve another no matter how
//! pathological its load. The victim p99 ratio (under-saturation / baseline) is
//! the headline; the only HARD asserts are correctness (victim reads keep
//! hitting, noisy ingest does not error) so the card is meaningful on any box.
//!
//! Contrast with the siblings:
//!   * `multi_project_fleet_scale.rs` — many noisy tenants, FLEET-SIZE axis.
//!   * `noisy_neighbor_harness.rs`     — primitive-level fairness gates.
//!   * THIS card                       — ONE adversarial tenant, SATURATION axis
//!                                       (load intensity, not fleet size).
//!
//! # Tiers — and WHERE each runs
//!
//!   * light  — DEV / CI. A few noisy connections, short. Correctness asserts
//!              catch regressions on every change. Default for `saturation_default`.
//!   * heavy  — BOX-STRESS / provisioned. A real connection flood + sustained
//!              overlay backlog; the honest saturation proof. Larger
//!              `BASIN_SAT_*` values, longer `BASIN_SAT_SECS`.
//!
//! # Artifact
//!
//! `benchmark/data/noisy_tenant_saturation_<conns>conns.json`.
//!
//! # Running it
//!
//! ```text
//! cargo test -p basin-integration-tests --test noisy_tenant_saturation \
//!     saturation_default -- --ignored --nocapture
//!
//! # heavier saturation:
//! BASIN_SAT_CONNS=64 BASIN_SAT_SECS=10 cargo test -p basin-integration-tests \
//!     --test noisy_tenant_saturation saturation_default -- --ignored --nocapture
//! ```
//!
//! or via `benchmark/run/scale-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_SAT_CONNS`      — concurrent noisy sessions on the one tenant (default 16).
//! * `BASIN_SAT_SECS`       — saturation duration in seconds (default 3).
//! * `BASIN_SAT_BATCH`      — rows per noisy INSERT statement (default 2_000).
//! * `BASIN_SAT_SEED_ROWS`  — seeded rows per project (default 50_000).
//! * `BASIN_SAT_QUERIES`    — victim query rounds per phase (default 300).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::task::JoinSet;

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
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage)
}

fn rows_in(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

fn percentile(samples: &mut [f64], p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64) * p).floor() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn env_usize(key: &str, default: usize) -> usize {
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
        eprintln!("[saturation] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize saturation artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[saturation] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[saturation] artifact rename {}: {e}", path.display());
    }
    eprintln!("[saturation] artifact written: {}", path.display());
}

/// Measure the victim: `queries` rounds of point read + keyset page on its one
/// session. Returns (point_ms, keyset_ms).
async fn measure_victim(
    sess: &basin_engine::ProjectSession,
    queries: usize,
    seed_rows: i64,
    assert_hits: bool,
) -> (Vec<f64>, Vec<f64>) {
    let mut point_ms = Vec::with_capacity(queries);
    let mut keyset_ms = Vec::with_capacity(queries);
    for i in 0..queries {
        let id = (i as i64 * 37) % seed_rows;
        let started = Instant::now();
        let res = sess
            .execute(&format!("SELECT v FROM events WHERE id = {id}"))
            .await
            .unwrap();
        point_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        if assert_hits {
            assert!(rows_in(&res) >= 1, "victim point read must hit: id={id}");
        }

        let cursor = (i as i64 * 53) % (seed_rows - 30).max(1);
        let started = Instant::now();
        let res = sess
            .execute(&format!(
                "SELECT id, v FROM events WHERE id > {cursor} ORDER BY id LIMIT 25"
            ))
            .await
            .unwrap();
        keyset_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        if assert_hits {
            assert!(
                rows_in(&res) >= 1,
                "victim keyset page must return rows: cursor={cursor}"
            );
        }
    }
    (point_ms, keyset_ms)
}

/// Provision one project (PK + value + GIN-indexed JSONB) and seed `seed_rows`.
/// Returns whether the GIN index applied.
async fn provision(engine: &Engine, p: ProjectId, seed_rows: i64) -> bool {
    let sess = engine.open_session(p).await.unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            v BIGINT NOT NULL, \
            payload JSONB NOT NULL\
         )",
    )
    .await
    .unwrap();
    let gin_ok = sess
        .execute("CREATE INDEX events_gin ON events USING gin (payload)")
        .await
        .is_ok();
    const SEED_BATCH: i64 = 5_000;
    let mut id = 0i64;
    while id < seed_rows {
        let lo = id;
        let hi = (id + SEED_BATCH).min(seed_rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 48);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k},'{{\"cat\":\"c{}\"}}')", k % 64));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    gin_ok
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "noisy-tenant saturation card: one adversarial tenant (GIN churn + overlay backlog + \
            connection flood) vs a victim; default is the light dev/CI tier, scale up with \
            BASIN_SAT_CONNS / BASIN_SAT_SECS for the box-stress saturation proof. \
            Run via benchmark/run/scale-suite.sh, or: \
            cargo test ... --test noisy_tenant_saturation saturation_default -- --ignored --nocapture"]
async fn saturation_default() {
    let conns = env_usize("BASIN_SAT_CONNS", 16).max(1);
    let secs = env_usize("BASIN_SAT_SECS", 3).max(1) as u64;
    let batch = env_usize("BASIN_SAT_BATCH", 2_000).max(10) as i64;
    let seed_rows = env_usize("BASIN_SAT_SEED_ROWS", 50_000).max(1_000) as i64;
    let queries = env_usize("BASIN_SAT_QUERIES", 300).max(10);

    eprintln!(
        "[saturation] config: noisy_conns={conns} duration={secs}s batch={batch} \
         seed_rows={seed_rows} queries={queries}"
    );

    let (_sd, _wd, engine, shard, bg, wal, _storage) = build().await;

    // Two projects: the victim (measured) and the noisy adversary (flooded).
    let victim_p = ProjectId::new();
    let noisy_p = ProjectId::new();
    let setup_started = Instant::now();
    let victim_gin = provision(&engine, victim_p, seed_rows).await;
    let noisy_gin = provision(&engine, noisy_p, seed_rows).await;
    let gin_ok = victim_gin && noisy_gin;
    if !gin_ok {
        eprintln!("[saturation] GIN index unavailable (victim={victim_gin}, noisy={noisy_gin}) — posting churn reduced, recorded");
    }

    // Settle; keep background loop ALIVE (reconciler is under test).
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    eprintln!(
        "[saturation] setup {:.1}s; background loop stays ALIVE",
        setup_started.elapsed().as_secs_f64()
    );

    let victim_sess = engine.open_session(victim_p).await.unwrap();

    // ── Phase A: victim baseline, no saturation ──────────────────────────────
    eprintln!("[saturation] phase A: victim baseline ({queries} rounds)");
    let (mut base_point, mut base_keyset) =
        measure_victim(&victim_sess, queries, seed_rows, true).await;

    // ── Phase B: saturate the noisy tenant, re-measure the victim ────────────
    // `conns` concurrent sessions ALL on the noisy project: each floods
    // high-cardinality JSONB inserts (GIN churn) plus an overlay-backlog
    // UPDATE every iteration (never lets the reconciler drain).
    eprintln!("[saturation] phase B: flooding noisy tenant with {conns} concurrent sessions for ~{secs}s + measuring victim");
    let stop = Arc::new(AtomicBool::new(false));
    let ins_ok = Arc::new(AtomicU64::new(0));
    let ins_rows = Arc::new(AtomicU64::new(0));
    let ins_err = Arc::new(AtomicU64::new(0));
    let upd_ok = Arc::new(AtomicU64::new(0));
    let upd_err = Arc::new(AtomicU64::new(0));

    let mut flood: JoinSet<()> = JoinSet::new();
    for c in 0..conns {
        let stop = stop.clone();
        let engine = engine.clone();
        let (ins_ok, ins_rows, ins_err) = (ins_ok.clone(), ins_rows.clone(), ins_err.clone());
        let (upd_ok, upd_err) = (upd_ok.clone(), upd_err.clone());
        flood.spawn(async move {
            // Connection flood: a fresh session per noisy worker on the one tenant.
            let sess = engine.open_session(noisy_p).await.unwrap();
            let mut next_id: i64 = 100_000_000 + (c as i64) * 1_000_000_000;
            let mut iter: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                iter += 1;
                let lo = next_id;
                let hi = lo + batch;
                let mut sql = String::with_capacity(batch as usize * 72);
                sql.push_str("INSERT INTO events VALUES ");
                for k in lo..hi {
                    if k > lo {
                        sql.push(',');
                    }
                    // Pathological GIN churn: every row carries a UNIQUE key so
                    // posting lists never stop growing.
                    sql.push_str(&format!(
                        "({k},{k},'{{\"cat\":\"c{}\",\"u{k}\":\"v{k}\"}}')",
                        k % 64
                    ));
                }
                match sess.execute(&sql).await {
                    Ok(_) => {
                        ins_ok.fetch_add(1, Ordering::Relaxed);
                        ins_rows.fetch_add(batch as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        ins_err.fetch_add(1, Ordering::Relaxed);
                    }
                }
                next_id = hi;
                // Overlay backlog: rewrite a seeded band EVERY iteration so the
                // sequential reconciler can never catch up.
                let a = (iter as i64 * 487) % (seed_rows - 500).max(1);
                let b = a + 499;
                let upd = format!(
                    "UPDATE events SET payload = '{{\"cat\":\"c{}\",\"rev\":\"r{}\"}}' \
                     WHERE id BETWEEN {a} AND {b}",
                    iter % 64,
                    iter
                );
                match sess.execute(&upd).await {
                    Ok(_) => {
                        upd_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        upd_err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // Let the flood ramp, then measure the victim while the saturation runs.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (mut load_point, mut load_keyset) =
        measure_victim(&victim_sess, queries, seed_rows, true).await;

    // Hold the saturation for at least the requested duration, then stop.
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    while flood.join_next().await.is_some() {}

    let n_ins_ok = ins_ok.load(Ordering::Relaxed);
    let n_ins_rows = ins_rows.load(Ordering::Relaxed);
    let n_ins_err = ins_err.load(Ordering::Relaxed);
    let n_upd_ok = upd_ok.load(Ordering::Relaxed);
    let n_upd_err = upd_err.load(Ordering::Relaxed);
    eprintln!(
        "[saturation] noisy flood: {n_ins_ok} inserts ({n_ins_rows} rows, {n_ins_err} errs), \
         {n_upd_ok} overlay updates ({n_upd_err} errs)"
    );
    assert_eq!(
        n_ins_err, 0,
        "noisy ingest must not error under saturation — {n_ins_err} INSERTs failed"
    );
    if n_upd_err > 0 {
        eprintln!(
            "[saturation] WARNING: {n_upd_err} overlay UPDATEs errored — recorded in artifact, investigate before publishing"
        );
    }

    // ── Stats ────────────────────────────────────────────────────────────────
    let bp50 = percentile(&mut base_point, 0.50);
    let bp99 = percentile(&mut base_point, 0.99);
    let bk50 = percentile(&mut base_keyset, 0.50);
    let bk99 = percentile(&mut base_keyset, 0.99);
    let lp50 = percentile(&mut load_point, 0.50);
    let lp99 = percentile(&mut load_point, 0.99);
    let lk50 = percentile(&mut load_keyset, 0.50);
    let lk99 = percentile(&mut load_keyset, 0.99);

    let point_p99_ratio = lp99 / bp99.max(1e-9);
    let keyset_p99_ratio = lk99 / bk99.max(1e-9);
    let worst_ratio = point_p99_ratio.max(keyset_p99_ratio);

    println!(
        "{:>16} {:>12} {:>12} {:>12} {:>12}",
        "scenario", "pt_p50_ms", "pt_p99_ms", "ks_p50_ms", "ks_p99_ms"
    );
    println!(
        "{:>16} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "baseline", bp50, bp99, bk50, bk99
    );
    println!(
        "{:>16} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "under_saturation", lp50, lp99, lk50, lk99
    );
    println!(
        "{:>16} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "ratio",
        lp50 / bp50.max(1e-9),
        point_p99_ratio,
        lk50 / bk50.max(1e-9),
        keyset_p99_ratio
    );

    const BAR_P99_RATIO: f64 = 10.0;
    let pass = worst_ratio < BAR_P99_RATIO;
    println!(
        "[saturation] victim p99 under one adversarial tenant: point {point_p99_ratio:.2}x, \
         keyset {keyset_p99_ratio:.2}x (informational bar <{BAR_P99_RATIO}x) {}",
        if pass {
            "PASS"
        } else {
            "FAIL (recorded — one tenant should not starve another if isolation holds)"
        }
    );

    // ── Artifact ─────────────────────────────────────────────────────────────
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let artifact = json!({
        "kind": "noisy_tenant_saturation",
        "id": format!("noisy_tenant_saturation_{conns}conns"),
        "name": "Single adversarial tenant saturation vs victim isolation",
        "claim": "Victim point-read and keyset-page p50/p99 with vs without ONE \
                  adversarial tenant under pathological load (high-cardinality GIN \
                  churn + per-iteration overlay backlog + connection flood). Proves \
                  the per-project isolation P0 fixes (GIN posting budget, PkRowCache \
                  waterline, reconciler round-robin) keep one tenant from starving \
                  another under adversarial saturation.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "config": {
            "noisy_conns": conns,
            "saturation_secs": secs,
            "noisy_batch_rows": batch,
            "seed_rows_per_project": seed_rows,
            "query_rounds": queries,
            "gin_indexed": gin_ok,
        },
        "noisy_load": {
            "insert_statements": n_ins_ok,
            "insert_rows": n_ins_rows,
            "insert_errors": n_ins_err,
            "overlay_updates": n_upd_ok,
            "overlay_update_errors": n_upd_err,
        },
        "rows": [
            { "scenario": "baseline",
              "point_p50_ms": bp50, "point_p99_ms": bp99,
              "keyset_p50_ms": bk50, "keyset_p99_ms": bk99 },
            { "scenario": "under_saturation",
              "point_p50_ms": lp50, "point_p99_ms": lp99,
              "keyset_p50_ms": lk50, "keyset_p99_ms": lk99 },
            { "scenario": "ratio",
              "point_p50_ms": lp50 / bp50.max(1e-9), "point_p99_ms": point_p99_ratio,
              "keyset_p50_ms": lk50 / bk50.max(1e-9), "keyset_p99_ms": keyset_p99_ratio },
        ],
        "primary": {
            "label": "worst victim p99 ratio (under_saturation / baseline)",
            "value": worst_ratio,
            "unit": "x",
            "informational_bar": BAR_P99_RATIO,
            "passed": pass,
        },
    });
    write_artifact(
        &format!("noisy_tenant_saturation_{conns}conns.json"),
        &artifact,
    );

    println!(
        "[saturation] conns={conns} secs={secs}: worst victim p99 ratio {worst_ratio:.2}x \
         ({n_ins_rows} adversarial rows, {n_upd_ok} overlay updates) — measurement recorded"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
