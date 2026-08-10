//! Multi-project fleet / noisy-neighbor benchmark.
//!
//! N projects (default 50) share ONE in-process engine (Shard + WAL + LocalFS
//! storage — the `scale_10m_smoke::build` stack), each with its own `events`
//! table. A subset of "noisy" projects drives combined write pressure —
//! heavy multi-row ingest with GIN-indexed JSONB payloads plus bulk JSONB
//! UPDATEs over the seeded band (overlay churn) — while "victim" projects run
//! latency-sensitive point reads and keyset pages.
//!
//! Victim p50/p99 is measured TWICE: Phase A baseline (no noisy load) and
//! Phase B under load. The degradation ratio is the headline number. This is
//! deliberately aimed at the known global-resource noisy-neighbor risks:
//!
//!   * the GLOBAL GIN posting budget (noisy JSONB writes churn postings);
//!   * the GLOBAL PkRowCache (noisy ingest evicts victims' hot PK rows);
//!   * the SEQUENTIAL overlay reconciler (noisy bulk JSONB UPDATEs queue
//!     overlay work ahead of victims' reads).
//!
//! Unlike `noisy_neighbor_harness.rs` (primitive-level fairness gates) and
//! `scaling_noisy_neighbor.rs` (2-project, scan-shaped, shard-less), this
//! card runs the FULL shard write path with the background loop ALIVE during
//! measurement — the reconciler is part of what's being measured.
//!
//! This is a MEASUREMENT card, not a regression gate: the per-project-budget
//! fixes for the risks above are not in yet, so the degradation ratio is
//! recorded (and printed PASS/FAIL against an informational 10x bar) but the
//! test only hard-asserts correctness invariants (victim hits, zero noisy
//! ingest errors). Once the budgets land, this same card is the before/after
//! evidence.
//!
//! Emits `benchmark/data/multi_project_fleet.json` with a unix-epoch
//! provenance timestamp.
//!
//! # Running it
//!
//! Sanctioned `#[ignore]`: an expensive fleet-scale probe, run explicitly and
//! ALONE on an idle box (repo cargo discipline):
//!
//! ```text
//! cargo test -p basin-integration-tests --test multi_project_fleet -- --ignored --nocapture
//! ```
//!
//! or via the suite runner `benchmark/run/scale-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_FLEET_PROJECTS`    — total projects (default 50)
//! * `BASIN_FLEET_NOISY`      — noisy projects, taken from the tail (default 8)
//! * `BASIN_FLEET_VICTIMS`    — measured victim projects, from the head (default 4)
//! * `BASIN_FLEET_SEED_ROWS`  — seeded rows per project (default 20_000)
//! * `BASIN_FLEET_QUERIES`    — victim query rounds per phase (default 200)
//! * `BASIN_FLEET_NOISY_BATCH`— rows per noisy INSERT statement (default 1_000)

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

/// Build a real in-process engine (Shard + WAL + LocalFileSystem storage).
/// Mirror of `scale_10m_smoke::build` — sibling test binaries each carry
/// their own copy of this helper by suite convention.
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

/// Count rows in a result; `Empty` counts as 0 (victim reads tolerate tags).
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

/// Atomic artifact write to `<repo_root>/benchmark/data/<file>` — minimal
/// copy of the `benchmark.rs` write contract (this card's filename is not
/// one of the `report_*` families).
fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[fleet] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize fleet artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[fleet] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[fleet] artifact rename {}: {e}", path.display());
    }
    eprintln!("[fleet] artifact written: {}", path.display());
}

/// Run one victim measurement pass: `queries` rounds, each round issuing a
/// point read and a keyset page on EVERY victim (round-robin, so the samples
/// interleave across projects the way real fleet traffic does). Returns
/// (point_ms, keyset_ms) sample vectors.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "multi-project fleet benchmark: N-project seed + noisy-load phases (tens of minutes); run via benchmark/run/scale-suite.sh on an idle box"]
async fn multi_project_fleet() {
    let n_projects = env_usize("BASIN_FLEET_PROJECTS", 50).max(2);
    let n_noisy = env_usize("BASIN_FLEET_NOISY", 8).max(1).min(n_projects - 1);
    let n_victims = env_usize("BASIN_FLEET_VICTIMS", 4)
        .max(1)
        .min(n_projects - n_noisy);
    let seed_rows = env_usize("BASIN_FLEET_SEED_ROWS", 20_000).max(100) as i64;
    let queries = env_usize("BASIN_FLEET_QUERIES", 200).max(10);
    let noisy_batch = env_usize("BASIN_FLEET_NOISY_BATCH", 1_000).max(10);

    eprintln!(
        "[fleet] config: projects={n_projects} noisy={n_noisy} victims={n_victims} \
         seed_rows={seed_rows} queries={queries} noisy_batch={noisy_batch}"
    );

    let (_sd, _wd, engine, shard, bg, wal, _storage) = build().await;

    // ── Provision + seed every project ───────────────────────────────────────
    // Victims are the head of the list, noisy projects the tail — disjoint
    // by construction. Every project has the same schema: PK + value + a
    // GIN-indexed JSONB payload (the global posting budget is one of the
    // contention surfaces under test).
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
        if (idx + 1) % 10 == 0 || idx + 1 == n_projects {
            eprintln!(
                "[fleet] provisioned {}/{} projects ({:.1}s elapsed)",
                idx + 1,
                n_projects,
                setup_started.elapsed().as_secs_f64()
            );
        }
    }
    let gin_ok = gin_failures == 0;
    if !gin_ok {
        eprintln!("[fleet] GIN index failed on {gin_failures}/{n_projects} projects — JSONB writes still run, posting churn reduced");
    }

    // Settle the seed into the cold tier so both phases start from the same
    // layout. The background loop is deliberately KEPT ALIVE afterwards —
    // the sequential overlay reconciler is one of the contention surfaces
    // this card exists to measure.
    let settle_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    let settle_s = settle_started.elapsed().as_secs_f64();
    let setup_s = setup_started.elapsed().as_secs_f64();
    eprintln!("[fleet] setup {setup_s:.1}s (settle {settle_s:.1}s); background loop stays ALIVE");

    // ── Phase A: victim baseline, no noisy load ──────────────────────────────
    eprintln!("[fleet] phase A: victim baseline ({queries} rounds x {n_victims} victims)");
    let (mut base_point, mut base_keyset) =
        measure_victims(&engine, &victims, queries, seed_rows, true).await;

    // ── Phase B: noisy load + victim measurement ─────────────────────────────
    // Each noisy project runs one task: heavy multi-row JSONB ingest into a
    // fresh id band, plus a bulk JSONB UPDATE over the seeded band every 5th
    // iteration (overlay-reconciler + GIN-overlay churn). Errors are counted,
    // never swallowed silently: ingest errors hard-fail the card; UPDATE
    // errors are reported in the artifact.
    eprintln!("[fleet] phase B: spawning {n_noisy} noisy writers, then re-measuring victims");
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
            // Fresh ids far above the seeded band; offset per task is
            // irrelevant (per-project tables) but keeps logs readable.
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
                    // Varied keys + values so the GIN posting lists keep
                    // growing (global posting budget pressure).
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
                    // Overlay churn: rewrite a 500-row JSONB band inside the
                    // seeded range (keeps the overlay reconciler busy).
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

    // Warm-up so the noisy writers are actually flowing before we measure.
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
        "[fleet] noisy totals: {inserts} insert stmts ({ins_rows} rows, {ins_errs} errors), \
         {updates} bulk JSONB updates ({upd_errs} errors)"
    );
    assert_eq!(
        ins_errs, 0,
        "noisy ingest must not error — {ins_errs} INSERT statements failed"
    );
    if upd_errs > 0 {
        eprintln!(
            "[fleet] WARNING: {upd_errs} noisy JSONB UPDATEs errored — overlay churn was \
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

    // Informational bar (NOT a hard gate — see header): the per-project
    // budget fixes for the GIN posting budget / PkRowCache / overlay
    // reconciler are the work this card baselines.
    const BAR_P99_RATIO: f64 = 10.0;
    let worst_ratio = point_p99_ratio.max(keyset_p99_ratio);
    let pass = worst_ratio < BAR_P99_RATIO;
    println!(
        "[fleet] victim p99 degradation: point {point_p99_ratio:.2}x, keyset {keyset_p99_ratio:.2}x \
         (informational bar <{BAR_P99_RATIO}x) {}",
        if pass { "PASS" } else { "FAIL (recorded, not gated — per-project budgets pending)" }
    );

    // ── Artifact ─────────────────────────────────────────────────────────────
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let artifact = json!({
        "kind": "fleet",
        "id": "multi_project_fleet",
        "name": "Multi-project fleet noisy-neighbor degradation",
        "claim": "Victim point-read and keyset-page p50/p99 with vs without noisy \
                  ingest + JSONB-overlay pressure across a shared-engine fleet — \
                  the before/after card for the per-project GIN-posting-budget, \
                  PkRowCache, and overlay-reconciler fixes.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "config": {
            "projects": n_projects,
            "noisy": n_noisy,
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
            "label": "worst victim p99 ratio (under_load / baseline)",
            "value": worst_ratio,
            "unit": "x",
            "informational_bar": BAR_P99_RATIO,
            "passed": pass,
        },
    });
    write_artifact("multi_project_fleet.json", &artifact);

    println!(
        "[fleet] projects={n_projects} (noisy={n_noisy}, victims={n_victims}): \
         worst victim p99 ratio {worst_ratio:.2}x — measurement recorded"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
