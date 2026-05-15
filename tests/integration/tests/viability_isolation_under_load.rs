//! Viability test 5: isolation under concurrent load.
//!
//! Claim: Concurrent multi-project traffic produces zero cross-project row
//! leakage. The brief is unambiguous: one leaked row across projects and the
//! project is dead. This test runs many tasks across many projects doing
//! mixed reads + writes simultaneously, and asserts every row a project
//! reads carries that project's sentinel.
//!
//! Setup: 50 projects, one shared Engine, 100 concurrent tasks each running
//! 20 ops (70% read, 30% write). Total 2000 mixed ops. Track leak count.

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const PROJECTS: usize = 50;
const TASKS: usize = 100;
const OPS_PER_TASK: usize = 20;

/// Cheap deterministic PRNG. We don't need cryptographic randomness; we just
/// need each task to make different choices without pulling in a `rand` dep.
fn next_u64(state: &mut u64) -> u64 {
    // SplitMix64. Bog-standard, fine for testing.
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_5_isolation_under_load() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    // Provision projects. Each gets its own sentinel marker (the project id
    // string itself — guaranteed unique). Open a session per project so the
    // hot loop doesn't pay the open-session cost on every op.
    //
    // Sessions are wrapped in `Mutex` because `ProjectSession::execute` takes
    // `&self` and SQL statements may mutate session state (table refresh
    // after INSERT). The mutex serializes ops within one project — but tasks
    // spread across all 50 projects, so contention is minimal in practice.
    let mut sessions: Vec<Arc<Mutex<ProjectSession>>> = Vec::with_capacity(PROJECTS);
    let mut markers: Vec<String> = Vec::with_capacity(PROJECTS);
    for _ in 0..PROJECTS {
        let t = ProjectId::new();
        let sess = engine.open_session(t).await.unwrap();
        sess.execute("CREATE TABLE events (id BIGINT NOT NULL, marker TEXT NOT NULL)")
            .await
            .unwrap();
        // Seed one row so the first read is non-empty.
        let marker = t.to_string();
        let seed_sql = format!("INSERT INTO events VALUES (0, '{}')", marker);
        sess.execute(&seed_sql).await.unwrap();
        markers.push(marker);
        sessions.push(Arc::new(Mutex::new(sess)));
    }

    let leaks = Arc::new(AtomicU64::new(0));
    let total_ops = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut tasks: JoinSet<()> = JoinSet::new();
    for task_id in 0..TASKS {
        let sessions = sessions.clone();
        let markers = markers.clone();
        let leaks = leaks.clone();
        let total_ops = total_ops.clone();
        tasks.spawn(async move {
            // Per-task PRNG seed. Mix in a static salt so identical task ids
            // across runs still differ.
            let mut rng_state: u64 = (task_id as u64).wrapping_mul(0xA5A5A5A5).wrapping_add(1);

            for op_idx in 0..OPS_PER_TASK {
                let project_idx = (next_u64(&mut rng_state) as usize) % PROJECTS;
                let expected_marker = markers[project_idx].as_str();
                let sess = sessions[project_idx].clone();
                let is_read = (next_u64(&mut rng_state) % 10) < 7; // 70% reads

                if is_read {
                    let s = sess.lock().await;
                    let res = s
                        .execute("SELECT marker FROM events ORDER BY id")
                        .await
                        .expect("read failed");
                    drop(s);
                    match res {
                        ExecResult::Rows { batches, .. } => {
                            for b in &batches {
                                let arr = b
                                    .column_by_name("marker")
                                    .expect("marker column")
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .expect("marker is utf8");
                                for i in 0..arr.len() {
                                    if arr.value(i) != expected_marker {
                                        leaks.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        ExecResult::Empty { .. } => panic!("expected rows for SELECT"),
                    }
                } else {
                    // 30% writes: insert a row stamped with this project's
                    // marker. id derived from task+op so collisions don't
                    // matter (no PK enforcement in PoC).
                    let id_val = 1 + (task_id as i64) * 1_000_000 + op_idx as i64;
                    let sql = format!("INSERT INTO events VALUES ({id_val}, '{expected_marker}')");
                    let s = sess.lock().await;
                    let _ = s.execute(&sql).await.expect("write failed");
                }
                total_ops.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }
    let elapsed = started.elapsed();

    let leaks_val = leaks.load(Ordering::Relaxed);
    let ops_val = total_ops.load(Ordering::Relaxed);
    let pass = leaks_val == 0;

    println!(
        "[VIABILITY 5] isolation under load: projects={}, ops={}, leaks={}, elapsed={:.2}s (bar leaks=0) {}",
        PROJECTS,
        ops_val,
        leaks_val,
        elapsed.as_secs_f64(),
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "isolation_under_load",
        "Project isolation under concurrent load",
        "Concurrent multi-project traffic produces zero cross-project row leakage.",
        pass,
        PrimaryMetric {
            label: "leaks".into(),
            value: leaks_val as f64,
            unit: "leaks".into(),
            bar: BarOp::eq(0.0),
        },
        json!({
            "projects": PROJECTS,
            "ops": ops_val,
            "elapsed_s": elapsed.as_secs_f64(),
        }),
    );

    assert_eq!(leaks_val, 0, "{leaks_val} cross-project row leaks observed");
}
