//! Multi-project FLEET-SCALE ladder — Basin's actual product thesis.
//!
//! `multi_project_fleet.rs` measures victim p50/p99 with vs without noisy load
//! at ONE project count (default 50). This card extends that to a PROJECT-COUNT
//! LADDER and asks the load-bearing product question: does per-project
//! isolation hold the victim's p99 FLAT as the fleet grows?
//!
//! The isolation machinery under test (all landed): the per-project GIN posting
//! budget partition, the PkRowCache waterline, and the reconciler round-robin.
//! The headline result is the victim p99 ratio (under-load / baseline) AS A
//! FUNCTION of project count — if isolation works, that ratio does not blow up
//! from 50 to 5000 projects.
//!
//! # The project-count ladder — and WHERE each tier runs
//!
//!   * 50    — DEV / CI. Seconds to a couple minutes. The correctness asserts
//!             (victim hits, zero noisy ingest errors) catch regressions on
//!             every change. Default for `fleet_scale_default`.
//!   * 500   — BOX-STRESS. The largest fleet this dev box runs in reasonable
//!             time (tens of minutes). The honest top of a laptop run.
//!   * 5000  — SCALE-PROOF. PROVISIONED hardware only — thousands of
//!             per-project tables + GIN indexes + the combined write/query
//!             axis is a real fleet's worth of state.
//!
//! The runner (`benchmark/run/scale-suite.sh`, `BASIN_FLEET_MAX`) enforces the
//! box-stress-vs-provisioned distinction so nobody runs 5000 on a laptop by
//! accident.
//!
//! # Combined axis: N projects × concurrent load
//!
//! Noisy projects scale WITH the fleet (a fixed FRACTION, `BASIN_FLEET_NOISY_FRAC`
//! percent, are noisy) so the write/query pressure grows with project count —
//! the realistic "more tenants ⇒ more simultaneous load" axis, not a fixed
//! handful of writers diluted across a growing fleet.
//!
//! # Artifact
//!
//! `benchmark/data/fleet_<N>projects.json` (N = project count), one per tier,
//! mirroring the ext size-suite's suffixed sidecars.
//!
//! # Running it
//!
//! ```text
//! # dev/CI (50 default):
//! cargo test -p basin-integration-tests --test multi_project_fleet_scale \
//!     fleet_scale_default -- --ignored --nocapture
//!
//! # any tier:
//! BASIN_FLEET_PROJECTS=500 cargo test -p basin-integration-tests \
//!     --test multi_project_fleet_scale fleet_scale_default -- --ignored --nocapture
//!
//! # 5000-project scale-proof (provisioned hardware only):
//! cargo test -p basin-integration-tests --test multi_project_fleet_scale \
//!     fleet_scale_5000 -- --ignored --nocapture
//! ```
//!
//! or via `benchmark/run/scale-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_FLEET_PROJECTS`   — project count for `fleet_scale_default` (default
//!                              50; no ceiling, accepts 5000+).
//! * `BASIN_FLEET_NOISY_FRAC` — percent of the fleet that is noisy (default 16,
//!                              i.e. 8/50 — scales noisy load WITH the fleet).
//! * `BASIN_FLEET_VICTIMS`    — measured victims, from the head (default 4).
//! * `BASIN_FLEET_SEED_ROWS`  — seeded rows per project (default 20_000).
//! * `BASIN_FLEET_QUERIES`    — victim query rounds per phase (default 200).
//! * `BASIN_FLEET_NOISY_BATCH`— rows per noisy INSERT statement (default 1_000).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::atomic::{AtomicBool, Ordering};
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
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
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
        eprintln!("[fleet-scale] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize fleet-scale artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[fleet-scale] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[fleet-scale] artifact rename {}: {e}", path.display());
    }
    eprintln!("[fleet-scale] artifact written: {}", path.display());
}

fn tier_label(projects: usize) -> &'static str {
    match projects {
        p if p <= 50 => "dev/CI (50)",
        p if p <= 500 => "box-stress (500)",
        _ => "scale-proof (5000+, provisioned)",
    }
}

/// One victim measurement pass: `queries` rounds, each issuing a point read +
/// keyset page on EVERY victim (round-robin so samples interleave). Returns
/// (point_ms, keyset_ms).
async fn measure_victims(
    engine: &Engine,
    victims: &[ProjectId],
    queries: usize,
    seed_rows: i64,
    assert_hits: bool,
) -> (Vec<f64>, Vec<f64>) {
    let mut sessions = Vec::with_capacity(victims.len());
    for v in victims {
        sessions.push(engine.open_session(*v).await.unwrap());
    }
    let mut point_ms = Vec::with_capacity(queries * victims.len());
    let mut keyset_ms = Vec::with_capacity(queries * victims.len());
    for i in 0..queries {
        for sess in &sessions {
            let id = (i as i64 * 37) % seed_rows;
            let started = Instant::now();
            let res = sess
                .execute(&format!("SELECT v FROM events WHERE id = {id}"))
                .await
                .unwrap();
            point_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            if assert_hits {
                assert!(
                    rows_in(&res) >= 1,
                    "victim point read must hit: id={id} returned no rows"
                );
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
    }
    (point_ms, keyset_ms)
}

/// The fleet-scale run at `n_projects`. Shared by all tier entrypoints. Emits
/// `fleet_<N>projects.json`.
async fn run_fleet(n_projects: usize) {
    let n_projects = n_projects.max(2);
    let noisy_frac = env_usize("BASIN_FLEET_NOISY_FRAC", 16).clamp(1, 90);
    // Noisy count scales WITH the fleet (combined N×load axis).
    let n_noisy = ((n_projects * noisy_frac) / 100).clamp(1, n_projects - 1);
    let n_victims = env_usize("BASIN_FLEET_VICTIMS", 4)
        .max(1)
        .min(n_projects - n_noisy);
    let seed_rows = env_usize("BASIN_FLEET_SEED_ROWS", 20_000).max(100) as i64;
    let queries = env_usize("BASIN_FLEET_QUERIES", 200).max(10);
    let noisy_batch = env_usize("BASIN_FLEET_NOISY_BATCH", 1_000).max(10);

    eprintln!(
        "[fleet-scale] tier={} projects={n_projects} noisy={n_noisy} (frac={noisy_frac}%) \
         victims={n_victims} seed_rows={seed_rows} queries={queries} noisy_batch={noisy_batch}",
        tier_label(n_projects)
    );

    let (_sd, _wd, engine, shard, bg, wal, _storage) = build().await;

    // Victims at the head, noisy at the tail — disjoint by construction.
    let projects: Vec<ProjectId> = (0..n_projects).map(|_| ProjectId::new()).collect();
    let victims: Vec<ProjectId> = projects[..n_victims].to_vec();
    let noisy: Vec<ProjectId> = projects[n_projects - n_noisy..].to_vec();

    let setup_started = Instant::now();
    let mut gin_failures = 0usize;
    for (idx, p) in projects.iter().enumerate() {
        let sess = engine.open_session(*p).await.unwrap();
        sess.execute(
            "CREATE TABLE events (\
                id BIGINT NOT NULL PRIMARY KEY, \
                v BIGINT NOT NULL, \
                payload JSONB NOT NULL\
             )",
        )
        .await
        .unwrap();
        if sess
            .execute("CREATE INDEX events_gin ON events USING gin (payload)")
            .await
            .is_err()
        {
            gin_failures += 1;
        }
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
        if (idx + 1) % 50 == 0 || idx + 1 == n_projects {
            eprintln!(
                "[fleet-scale] provisioned {}/{} projects ({:.1}s elapsed)",
                idx + 1,
                n_projects,
                setup_started.elapsed().as_secs_f64()
            );
        }
    }
    let gin_ok = gin_failures == 0;
    if !gin_ok {
        eprintln!("[fleet-scale] GIN index failed on {gin_failures}/{n_projects} projects — JSONB writes still run, posting churn reduced");
    }

    // Settle the seed so both phases start from the same layout. Background
    // loop stays ALIVE — the overlay reconciler is part of what's measured.
    let settle_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    let settle_s = settle_started.elapsed().as_secs_f64();
    let setup_s = setup_started.elapsed().as_secs_f64();
    eprintln!("[fleet-scale] setup {setup_s:.1}s (settle {settle_s:.1}s); background loop stays ALIVE");

    // ── Phase A: victim baseline, no noisy load ──────────────────────────────
    eprintln!("[fleet-scale] phase A: victim baseline ({queries} rounds x {n_victims} victims)");
    let (mut base_point, mut base_keyset) =
        measure_victims(&engine, &victims, queries, seed_rows, true).await;

    // ── Phase B: noisy load (scales with fleet) + victim measurement ─────────
    eprintln!("[fleet-scale] phase B: spawning {n_noisy} noisy writers, then re-measuring victims");
    let stop = Arc::new(AtomicBool::new(false));
    let mut noisy_set: JoinSet<(u64, u64, u64, u64, u64)> = JoinSet::new();
    for (t, p) in noisy.iter().enumerate() {
        let stop = stop.clone();
        let engine = engine.clone();
        let p = *p;
        let batch = noisy_batch as i64;
        noisy_set.spawn(async move {
            let sess = engine.open_session(p).await.unwrap();
            let (mut inserts, mut ins_rows, mut ins_errs) = (0u64, 0u64, 0u64);
            let (mut updates, mut upd_errs) = (0u64, 0u64);
            let mut next_id: i64 = 10_000_000 + (t as i64) * 1_000_000_000;
            let mut iter: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                iter += 1;
                let lo = next_id;
                let hi = lo + batch;
                let mut sql = String::with_capacity(batch as usize * 64);
                sql.push_str("INSERT INTO events VALUES ");
                for k in lo..hi {
                    if k > lo {
                        sql.push(',');
                    }
                    sql.push_str(&format!(
                        "({k},{k},'{{\"cat\":\"c{}\",\"k{}\":\"v{}\"}}')",
                        k % 64,
                        k % 32,
                        k % 97
                    ));
                }
                match sess.execute(&sql).await {
                    Ok(_) => {
                        inserts += 1;
                        ins_rows += batch as u64;
                    }
                    Err(_) => ins_errs += 1,
                }
                next_id = hi;
                if iter % 5 == 0 {
                    let a = (iter as i64 * 487) % (seed_rows - 500).max(1);
                    let b = a + 499;
                    let upd = format!(
                        "UPDATE events SET payload = '{{\"cat\":\"c{}\",\"rev\":\"r{}\"}}' \
                         WHERE id BETWEEN {a} AND {b}",
                        iter % 64,
                        iter
                    );
                    match sess.execute(&upd).await {
                        Ok(_) => updates += 1,
                        Err(_) => upd_errs += 1,
                    }
                }
            }
            (inserts, ins_rows, ins_errs, updates, upd_errs)
        });
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let (mut load_point, mut load_keyset) =
        measure_victims(&engine, &victims, queries, seed_rows, true).await;

    stop.store(true, Ordering::Relaxed);
    let (mut inserts, mut ins_rows, mut ins_errs, mut updates, mut upd_errs) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    while let Some(r) = noisy_set.join_next().await {
        let (i, ir, ie, u, ue) = r.unwrap();
        inserts += i;
        ins_rows += ir;
        ins_errs += ie;
        updates += u;
        upd_errs += ue;
    }
    eprintln!(
        "[fleet-scale] noisy totals: {inserts} insert stmts ({ins_rows} rows, {ins_errs} errors), \
         {updates} bulk JSONB updates ({upd_errs} errors)"
    );
    assert_eq!(
        ins_errs, 0,
        "noisy ingest must not error — {ins_errs} INSERT statements failed"
    );
    if upd_errs > 0 {
        eprintln!(
            "[fleet-scale] WARNING: {upd_errs} noisy JSONB UPDATEs errored — overlay churn was \
             partial; recorded in the artifact, investigate before publishing"
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
        "{:>14} {:>12} {:>12} {:>12} {:>12}",
        "scenario", "pt_p50_ms", "pt_p99_ms", "ks_p50_ms", "ks_p99_ms"
    );
    println!(
        "{:>14} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "baseline", bp50, bp99, bk50, bk99
    );
    println!(
        "{:>14} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "under_load", lp50, lp99, lk50, lk99
    );
    println!(
        "{:>14} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
        "ratio",
        lp50 / bp50.max(1e-9),
        point_p99_ratio,
        lk50 / bk50.max(1e-9),
        keyset_p99_ratio
    );

    // Informational bar (NOT a hard gate): the key product question is whether
    // this ratio stays FLAT as the fleet grows — a reader compares the ratio
    // across the 50/500/5000 artifacts. A per-tier ratio blow-up is the signal
    // that isolation broke at that fleet size.
    const BAR_P99_RATIO: f64 = 10.0;
    let pass = worst_ratio < BAR_P99_RATIO;
    println!(
        "[fleet-scale] projects={n_projects} victim p99 degradation: point {point_p99_ratio:.2}x, \
         keyset {keyset_p99_ratio:.2}x (informational bar <{BAR_P99_RATIO}x) {}",
        if pass { "PASS" } else { "FAIL (recorded — compare the ratio across tiers to see WHERE isolation broke)" }
    );

    // ── Artifact ─────────────────────────────────────────────────────────────
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let artifact = json!({
        "kind": "fleet_scale",
        "id": format!("fleet_{n_projects}projects"),
        "name": "Multi-project fleet-scale victim-isolation ladder",
        "tier": tier_label(n_projects),
        "claim": "Victim point-read and keyset-page p50/p99 with vs without noisy \
                  ingest + JSONB-overlay pressure, at this fleet size. The product \
                  result is whether the under-load/baseline p99 ratio stays FLAT \
                  as project count scales 50 -> 500 -> 5000 — i.e. whether the \
                  per-project GIN-budget / PkRowCache / reconciler-round-robin \
                  isolation holds the victim's p99 flat as the fleet grows.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "config": {
            "projects": n_projects,
            "noisy": n_noisy,
            "noisy_frac_pct": noisy_frac,
            "victims": n_victims,
            "seed_rows_per_project": seed_rows,
            "query_rounds": queries,
            "noisy_batch_rows": noisy_batch,
            "gin_indexed": gin_ok,
            "gin_index_failures": gin_failures,
        },
        "noisy_load": {
            "insert_statements": inserts,
            "insert_rows": ins_rows,
            "insert_errors": ins_errs,
            "jsonb_bulk_updates": updates,
            "jsonb_update_errors": upd_errs,
        },
        "rows": [
            { "scenario": "baseline",
              "point_p50_ms": bp50, "point_p99_ms": bp99,
              "keyset_p50_ms": bk50, "keyset_p99_ms": bk99 },
            { "scenario": "under_load",
              "point_p50_ms": lp50, "point_p99_ms": lp99,
              "keyset_p50_ms": lk50, "keyset_p99_ms": lk99 },
            { "scenario": "ratio",
              "point_p50_ms": lp50 / bp50.max(1e-9), "point_p99_ms": point_p99_ratio,
              "keyset_p50_ms": lk50 / bk50.max(1e-9), "keyset_p99_ms": keyset_p99_ratio },
        ],
        "primary": {
            "label": "worst victim p99 ratio (under_load / baseline) at this fleet size",
            "value": worst_ratio,
            "unit": "x",
            "informational_bar": BAR_P99_RATIO,
            "passed": pass,
        },
    });
    write_artifact(&format!("fleet_{n_projects}projects.json"), &artifact);

    println!(
        "[fleet-scale] tier={} projects={n_projects} (noisy={n_noisy}, victims={n_victims}): \
         worst victim p99 ratio {worst_ratio:.2}x — measurement recorded",
        tier_label(n_projects)
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Env-driven fleet size (default 50 = dev/CI). `BASIN_FLEET_PROJECTS` drives
/// the ladder; the runner sets it per tier point.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "fleet-scale ladder: default 50 (dev/CI); set BASIN_FLEET_PROJECTS for 500/5000 — \
            500 is box-stress, 5000 is provisioned-hardware only. \
            Run via benchmark/run/scale-suite.sh, or: \
            BASIN_FLEET_PROJECTS=<N> cargo test ... --test multi_project_fleet_scale fleet_scale_default -- --ignored --nocapture"]
async fn fleet_scale_default() {
    let n = env_usize("BASIN_FLEET_PROJECTS", 50);
    run_fleet(n).await;
}

/// The 5000-project scale-proof, pinned. `BASIN_FLEET_PROJECTS` overrides so it
/// smokes at tiny-N without editing source.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "5000-project fleet scale-proof: thousands of per-project tables + GIN indexes, \
            provisioned hardware only — run via benchmark/run/scale-suite.sh BASIN_FLEET_PROJECTS=5000, \
            never on a laptop (the runner's BASIN_FLEET_MAX refuses it by default)"]
async fn fleet_scale_5000() {
    let n = env_usize("BASIN_FLEET_PROJECTS", 5000);
    run_fleet(n).await;
}
