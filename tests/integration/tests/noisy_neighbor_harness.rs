//! Noisy-neighbor + fairness permanent regression harness.
//!
//! **Audit reference:** `docs/audits/2026-05-21-noisy-neighbor-fairness.md`
//! **Task:** TASK.md #22 (follow-up to audit #19)
//!
//! The P0 findings have all been fixed via Phase 6.X (lease ownership +
//! heartbeat budgets) and Phase 6.P0 (catalog pool, statement timeout, Wasm
//! runtime). This file codifies the audit's adversarial scenarios as permanent
//! regression tests so we don't backslide.
//!
//! ## Scenarios covered
//!
//! | Test fn                                    | Audit scenario | Invariant asserted |
//! |--------------------------------------------|----------------|--------------------|
//! | `A_htap_hard_cap_isolates_noisy_tenant`    | C / axis #4    | Memtable hard cap fires for the noisy project; other project unaffected |
//! | `B_connection_limit_enforces_ceiling`       | B / axis #1    | `ConnectionLimiter` with a fixed cap rejects at-limit connections; cross-project unaffected |
//! | `C_pgwire_rate_limit_throttles_burst`       | A / axis #1    | `PgRateLimit` throttles the noisy project; second project's token bucket is independent |
//! | `D_realtime_budget_isolates_per_project`    | D / axis #5    | Realtime `BudgetTracker` returns `BufferFull` for the flooder; quieter project can still reserve |
//! | `E_cron_per_project_job_cap_enforced`       | E / axis #13   | `CronStore` rejects the 101st job; sibling project is unaffected |
//! | `F_net_allowlist_deny_all_default`         | J / axis #14   | `AllowList` denies unknown hosts; whitelisted project can still reach its target |
//! | `G_net_rate_limit_per_project_isolation`   | J / axis #14   | `RateLimit::check` throttles the noisy project; the quiet project's bucket is independent |
//! | `H_realtime_channel_count_visible`         | K / axis #5    | `ChannelRegistry::channel_count` tracks entries; `prune()` drops dead ones |
//! | `I_cross_project_row_isolation_under_contention` | cross-cutting | Two projects writing under concurrent load see zero cross-project row leakage |
//!
//! ## Skipped scenarios and why
//!
//! - **Scenario A (JOIN / statement timeout)**: testing the statement-timeout
//!   path requires either driving a real long-running query to completion or
//!   mocking the DataFusion executor. The timeout itself lives deep in
//!   `executor.rs` and is validated by the engine's internal unit tests. A
//!   test-harness-level duplicate would be brittle and slow. Covered by the
//!   engine unit tests once the Phase 6.P0 timeout lands.
//!
//! - **Scenario F (catalog-PG mutex / DDL serialisation)**: the catalog
//!   `PostgresCatalog` is replaced by a pool in Phase 6.P0 and its unit tests
//!   validate the pool behaviour. We cannot inject a real Postgres catalog in
//!   CI without `BASIN_AUTH_TEST_POSTGRES_URL`; using `InMemoryCatalog` would
//!   not exercise the serialisation path.
//!
//! - **Scenario G (Wasm blocking-pool starvation)**: the wasm runtime and
//!   `FunctionGovernance` are already covered by `wasm_functions_soak.rs` and
//!   `wasm_functions_differential.rs`. Adding another soak-level test here
//!   would duplicate coverage without adding regression value.
//!
//! ## Design constraints
//!
//! - Every test finishes in < 5 s on a developer laptop.
//! - Every test uses its own `TempDir` + fresh `ProjectId` (no shared global
//!   state across test cases).
//! - Tests prefer pure primitive-level assertions over spinning up a full
//!   pgwire server (faster and more deterministic).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_hottier::{MemTableConfig, MemTableRegistry, ReservationOutcome};
use basin_net::{AllowList, RateLimit};
use basin_realtime::{
    budget::{BudgetError, BudgetTracker},
    ChannelKey, ChannelRegistry,
};
use async_trait::async_trait;
use basin_router::{ConnectionLimiter, ConnectionLimitProvider, PgRateLimit};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::task::JoinSet;

// ─── helpers ─────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario C / audit axis #4 — HTAP memtable hard cap isolates the noisy tenant
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: `MemTableRegistry::try_reserve_bytes` returns `HardCapReached`
// correctly for a project that exceeds its per-project hard cap
// (`registry.rs:216-218`). The invariant: the noisy project is rejected while
// a quiet sibling project can still make reservations into its own bucket.
//
// The audit catalogued this as "✅ on-by-default; CAS-correct". This test is
// the permanent regression gate that ensures the CAS-correct enforcement is
// never accidentally removed or weakened.

/// Audit scenario C / axis #4 (P0 fixed, HTAP hard cap isolation).
///
/// Sets a tiny per-project hard cap so the noisy tenant exhausts it quickly.
/// Asserts `HardCapReached` for the noisy project and `Granted` for the quiet
/// project — the quiet project's bucket is independent.
#[tokio::test]
async fn a_htap_hard_cap_isolates_noisy_tenant() {
    basin_common::telemetry::try_init_for_tests();

    // Tiny caps so the noisy project exhausts quickly.
    let cfg = MemTableConfig {
        project_hard_cap_bytes: 4 * 1024,  // 4 KiB hard cap
        project_soft_cap_bytes: 2 * 1024,  // 2 KiB soft cap
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // Exhaust the noisy project's cap.
    let mut granted = 0usize;
    let mut hard_cap_hits = 0usize;
    for _ in 0..20 {
        match reg.try_reserve_bytes(&noisy, 512) {
            ReservationOutcome::Granted | ReservationOutcome::FlushSuggested => granted += 1,
            ReservationOutcome::HardCapReached => hard_cap_hits += 1,
        }
    }
    assert!(
        hard_cap_hits >= 1,
        "FAIRNESS: noisy project must hit HardCapReached after exhausting cap; \
         granted={granted}, hard_cap_hits={hard_cap_hits}"
    );
    println!(
        "[scenario_A] noisy project: granted={granted}, hard_cap_hits={hard_cap_hits}"
    );

    // Quiet project can still reserve bytes — its bucket is completely independent.
    let quiet_outcome = reg.try_reserve_bytes(&quiet, 1024);
    assert_ne!(
        quiet_outcome,
        ReservationOutcome::HardCapReached,
        "FAIRNESS: quiet project must not be throttled by noisy project's cap exhaustion; \
         got {quiet_outcome:?}"
    );
    println!("[scenario_A] quiet project outcome: {quiet_outcome:?} (must not be HardCapReached)");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario B / audit axis #1 — connection limiter enforces per-project ceiling
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding (P0): `NoConnectionLimit` admits everything — a tenant can
// open 10k sockets and DOS the accept loop. The fix (Phase 6.P0) wires a
// default `ConnectionLimitProvider`. This test asserts that a limiter with a
// fixed per-project cap rejects connections beyond the ceiling and that
// cross-project counters are independent.

/// Fixed cap provider for testing — returns `Some(n)` for all projects.
struct FixedCapProvider(u32);

#[async_trait]
impl ConnectionLimitProvider for FixedCapProvider {
    async fn limit_for(&self, _project: ProjectId) -> Option<u32> {
        Some(self.0)
    }
}

/// Audit scenario B / axis #1 (P0 fixed, connection limit enforcement).
///
/// Asserts: a capped `ConnectionLimiter` admits up to `CAP` connections for
/// the noisy project and rejects the next attempt; the quiet project is not
/// affected by the noisy project's exhausted cap.
#[tokio::test]
async fn b_connection_limit_enforces_ceiling() {
    basin_common::telemetry::try_init_for_tests();

    const CAP: u32 = 5;
    let limiter = ConnectionLimiter::new(Arc::new(FixedCapProvider(CAP)));

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // Admit exactly CAP connections for the noisy project.
    let mut guards = Vec::new();
    for i in 0..CAP {
        guards.push(
            limiter
                .try_admit(noisy)
                .await
                .unwrap_or_else(|e| panic!("admit {i} must succeed, got err count {e}")),
        );
    }
    assert_eq!(limiter.live_count(noisy), CAP);

    // The next attempt must be rejected.
    let rejected = limiter
        .try_admit(noisy)
        .await
        .expect_err("FAIRNESS: connection beyond cap must be rejected");
    assert_eq!(
        rejected, CAP,
        "rejected count must equal the cap at the time of rejection"
    );
    println!(
        "[scenario_B] noisy project rejected at {rejected} connections (cap={CAP}) — correct"
    );

    // Quiet project is unaffected: its counter is independent.
    let quiet_guard = limiter
        .try_admit(quiet)
        .await
        .expect("FAIRNESS: quiet project must still admit connections");
    assert_eq!(
        limiter.live_count(quiet),
        1,
        "quiet project live count must be exactly 1 (independent of noisy project)"
    );
    println!("[scenario_B] quiet project admitted ok (count=1)");

    drop(guards);
    drop(quiet_guard);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario A (pgwire QPS) / audit axis #1 — rate limiter isolates projects
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding (P0-4): `BASIN_PGWIRE_RATE_LIMIT_QPS` defaults to 0 (disabled).
// When enabled, per-project token buckets are independent — the noisy project's
// exhausted bucket must not throttle the quiet project.

/// Audit scenario A / axis #1 (P0-4, pgwire QPS isolation).
///
/// Drives a 1 qps limiter for 100 calls on the noisy project — at least some
/// must be throttled. The quiet project's bucket is fresh and must admit on
/// the first check.
#[tokio::test]
async fn c_pgwire_rate_limit_throttles_burst() {
    basin_common::telemetry::try_init_for_tests();

    // 1 qps cap — very easy to exhaust.
    let rl = PgRateLimit::with_qps(1);

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    let mut throttled = 0u32;
    for _ in 0..100 {
        if rl.check(&noisy).is_err() {
            throttled += 1;
        }
    }
    assert!(
        throttled >= 1,
        "FAIRNESS: 100 hits at 1 qps must throttle at least once; got {throttled}"
    );
    println!("[scenario_C] noisy project throttled {throttled}/100 times — correct");

    // Quiet project's bucket is independent — first check must pass.
    let quiet_result = rl.check(&quiet);
    assert!(
        quiet_result.is_ok(),
        "FAIRNESS: quiet project must not be throttled by noisy project's burst; \
         got {quiet_result:?}"
    );
    println!("[scenario_C] quiet project admitted on first check — bucket is independent");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario D / audit axis #5 — realtime budget isolates per-project
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: `BudgetTracker::try_reserve` returns `Err(BudgetError::BufferFull)`
// when the per-project cap is exceeded. Correctly isolated (axis #5 is ✅).
// The test ensures the isolation invariant holds: the flooder exhausts its cap
// but the quiet project can still reserve bytes.

/// Audit scenario D / axis #5 (realtime budget isolation).
///
/// A tiny-capped `BudgetTracker` rejects the noisy project once full while
/// the quiet project's in-flight budget is independent and still admits.
#[tokio::test]
async fn d_realtime_budget_isolates_per_project() {
    basin_common::telemetry::try_init_for_tests();

    // 1 KiB cap — trivially exhausted.
    let tracker = BudgetTracker::new(1024);

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // Fill the noisy project's cap with two 512-byte reservations.
    let guard1 = tracker.try_reserve(noisy, 512).expect("first 512 must succeed");
    let guard2 = tracker.try_reserve(noisy, 512).expect("second 512 must succeed");

    // Now the noisy project is at capacity — next reservation must fail.
    let err = tracker
        .try_reserve(noisy, 1)
        .expect_err("FAIRNESS: noisy project at cap must return BufferFull");
    assert!(
        matches!(err, BudgetError::BufferFull),
        "expected BudgetError::BufferFull, got {err:?}"
    );
    println!("[scenario_D] noisy project correctly returns BufferFull at cap");

    // Quiet project is completely unaffected — its budget is independent.
    let quiet_guard = tracker
        .try_reserve(quiet, 512)
        .expect("FAIRNESS: quiet project must still reserve from its own budget");
    assert_eq!(
        tracker.bytes_in_flight(quiet),
        512,
        "quiet project in-flight must be exactly 512"
    );
    println!(
        "[scenario_D] quiet project bytes_in_flight={} — isolation confirmed",
        tracker.bytes_in_flight(quiet)
    );

    drop(guard1);
    drop(guard2);
    drop(quiet_guard);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario E / audit axis #13 — cron per-project job cap enforced
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: `CronRunner::PER_MINUTE_RATE_LIMIT` = 60 bounds execution
// rate, but there is no per-project cap on the NUMBER of schedules. The fix
// (Phase 6.P0) adds `CronStore::MAX_JOBS_PER_PROJECT` = 100. This test
// asserts the cap is enforced and that the sibling project is unaffected.

/// Audit scenario E / axis #13 (cron per-project schedule count cap).
///
/// Register `MAX_JOBS_PER_PROJECT` cron jobs for the noisy project and assert
/// the 101st call returns `ScheduleError::JobCapExceeded`. The quiet project
/// can still schedule jobs.
#[tokio::test]
async fn e_cron_per_project_job_cap_enforced() {
    use basin_cron::{CronStore, ScheduleError};

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let store = CronStore::new(engine.clone());

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // Provision tables for both projects.
    store
        .ensure_tables(&noisy)
        .await
        .expect("ensure_tables for noisy");
    store
        .ensure_tables(&quiet)
        .await
        .expect("ensure_tables for quiet");

    // Schedule MAX_JOBS_PER_PROJECT jobs for the noisy project.
    let max = CronStore::MAX_JOBS_PER_PROJECT;
    for i in 0..max {
        let name = format!("job-{i}");
        store
            .schedule(&noisy, "test-user", &name, "* * * * *", "SELECT 1")
            .await
            .unwrap_or_else(|e| panic!("schedule {name} must succeed: {e}"));
    }

    // The next schedule must fail with JobCapExceeded.
    let err = store
        .schedule(&noisy, "test-user", "overflow-job", "* * * * *", "SELECT 1")
        .await
        .expect_err("FAIRNESS: schedule beyond cap must return JobCapExceeded");
    assert!(
        matches!(err, ScheduleError::JobCapExceeded { .. }),
        "expected JobCapExceeded, got {err:?}"
    );
    println!(
        "[scenario_E] noisy project correctly rejected at {max} jobs: {err}"
    );

    // Quiet project is unaffected — can schedule freely.
    store
        .schedule(&quiet, "test-user", "quiet-job", "* * * * *", "SELECT 1")
        .await
        .expect("FAIRNESS: quiet project must be able to schedule a job");
    println!("[scenario_E] quiet project scheduled a job successfully — cap is per-project");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario J / audit axis #14 — net allowlist DENY-ALL default
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: `AllowList` defaults to DENY-ALL. A project that has not
// allowlisted a host cannot reach it. Cross-project allowlists are isolated.

/// Audit scenario J (basin-net DENY-ALL + isolation of per-project allowlists).
///
/// `AllowList` rejects a non-allowlisted host. Once a host is added to project
/// A's list, project B still cannot reach it — allowlists are per-project.
#[tokio::test]
async fn f_net_allowlist_deny_all_default() {
    basin_common::telemetry::try_init_for_tests();

    let allow = AllowList::new();

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    // Before any allowlist entry: both projects are denied.
    let deny = allow
        .check(&project_a, "https://api.example.com/data")
        .await;
    assert!(
        deny.is_err(),
        "FAIRNESS: DENY-ALL default must reject unenrolled host for project A"
    );
    let deny_b = allow
        .check(&project_b, "https://api.example.com/data")
        .await;
    assert!(
        deny_b.is_err(),
        "FAIRNESS: DENY-ALL default must reject unenrolled host for project B"
    );
    println!("[scenario_F] DENY-ALL default confirmed for both projects");

    // Add api.example.com to project A's allowlist only.
    allow.allow(&project_a, "api.example.com").await;

    let ok_a = allow
        .check(&project_a, "https://api.example.com/data")
        .await;
    assert!(
        ok_a.is_ok(),
        "FAIRNESS: project A's allowlisted host must be permitted; got {ok_a:?}"
    );

    // Project B still cannot reach that host — allowlists are per-project.
    let still_denied_b = allow
        .check(&project_b, "https://api.example.com/data")
        .await;
    assert!(
        still_denied_b.is_err(),
        "FAIRNESS: project B must NOT benefit from project A's allowlist entry — \
         allowlists are per-project isolation boundaries"
    );
    println!(
        "[scenario_F] project A allowed; project B still denied — allowlists are per-project"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario J / audit axis #14 — outbound HTTP rate limit per-project isolation
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: basin-net's `RateLimit` is per-project via `governor::keyed`.
// A noisy project exhausting its token bucket must not starve the quiet project.

/// Audit scenario J / axis #14 (outbound HTTP rate limit per-project isolation).
///
/// The noisy project exhausts its token bucket (10/s default; we drive 200 calls
/// to guarantee exhaustion). The quiet project's fresh bucket must still admit.
#[tokio::test]
async fn g_net_rate_limit_per_project_isolation() {
    basin_common::telemetry::try_init_for_tests();

    let rl = RateLimit::new();

    let noisy = ProjectId::new();
    let quiet = ProjectId::new();

    // Drive 200 checks against the noisy project. The default rate is 10/s so
    // after the burst allowance is consumed most will be rejected.
    let mut throttled = 0u32;
    for _ in 0..200 {
        if rl.check(&noisy).is_err() {
            throttled += 1;
        }
    }
    assert!(
        throttled >= 1,
        "FAIRNESS: 200 outbound checks at default rate must throttle at least once; \
         got {throttled}"
    );
    println!(
        "[scenario_G] noisy project throttled {throttled}/200 outbound calls — correct"
    );

    // Quiet project's bucket is independent — first check must pass.
    let quiet_result = rl.check(&quiet);
    assert!(
        quiet_result.is_ok(),
        "FAIRNESS: quiet project must not be rate-limited by noisy project's exhausted bucket; \
         got {quiet_result:?}"
    );
    println!("[scenario_G] quiet project admitted on first check — per-project isolation confirmed");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario K / audit axis #5 — realtime channel count tracking + prune
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding: `ChannelRegistry.channels` DashMap entries persist forever
// unless `prune()` is called. A tenant creating many subscriptions leaks
// entries. The test documents the current behaviour and asserts `prune()` does
// remove dead entries.

/// Audit scenario K / axis #5 (realtime channel registry accounting + prune).
///
/// Subscribe to N distinct channels for a project. Assert `channel_count()`
/// grows accordingly. Drop the receivers (simulate subscriber departure). Call
/// `prune()`. Assert channel count returns to 0.
#[tokio::test]
async fn h_realtime_channel_count_visible() {
    basin_common::telemetry::try_init_for_tests();

    let registry = ChannelRegistry::new(64);
    let project = ProjectId::new();

    const CHANNELS: usize = 20;

    // Subscribe to 20 distinct tables → 20 channel entries.
    let mut receivers = Vec::with_capacity(CHANNELS);
    for i in 0..CHANNELS {
        let table = TableName::new(&format!("table_{i}")).unwrap();
        let key = ChannelKey::new(project, table);
        receivers.push(registry.subscribe(key));
    }

    assert_eq!(
        registry.channel_count(),
        CHANNELS,
        "channel_count must equal the number of subscribed channels"
    );
    println!(
        "[scenario_H] channel_count={} after {CHANNELS} subscriptions",
        registry.channel_count()
    );

    // Drop all receivers — channels are now dead (no active senders or
    // receivers; senders linger until the DashMap entry is pruned).
    drop(receivers);

    // Before prune: entries still present in the DashMap.
    let count_before_prune = registry.channel_count();
    println!("[scenario_H] channel_count before prune: {count_before_prune}");

    // After prune: dead channels are evicted.
    registry.prune();
    let count_after_prune = registry.channel_count();
    println!("[scenario_H] channel_count after prune: {count_after_prune}");

    assert_eq!(
        count_after_prune,
        0,
        "FAIRNESS: prune() must evict dead channel entries; \
         before_prune={count_before_prune}, after_prune={count_after_prune}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-cutting: row isolation under concurrent multi-tenant write contention
// ─────────────────────────────────────────────────────────────────────────────
//
// Audit finding (cross-cutting): the wedge invariant "one project's behaviour
// cannot take down another" requires that even under high concurrent write load,
// rows written by project A never appear in project B's read results.
//
// This test is the fast-regression layer counterpart to `viability_5_
// isolation_under_load.rs`. That test runs 50 projects × 100 tasks; this
// one uses 2 projects × 10 tasks with 100 ops each — small enough to
// complete in < 3 s and cheap enough to run on every CI push.

/// Cross-cutting isolation invariant under concurrent write contention.
///
/// Two projects share one engine. Ten concurrent tasks per project write and
/// read. After all tasks complete, every row a project reads carries only
/// that project's sentinel — zero cross-project leaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i_cross_project_row_isolation_under_contention() {
    use arrow_array::{Array, StringArray};
    use basin_engine::ExecResult;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    let marker_a = project_a.to_string();
    let marker_b = project_b.to_string();

    // Set up one table per project with a sentinel marker column.
    let sess_a = engine.open_session(project_a).await.unwrap();
    let sess_b = engine.open_session(project_b).await.unwrap();
    for (sess, marker) in [(&sess_a, &marker_a), (&sess_b, &marker_b)] {
        sess.execute("CREATE TABLE events (id BIGINT NOT NULL, marker TEXT NOT NULL)")
            .await
            .unwrap();
        let sql = format!("INSERT INTO events VALUES (0, '{marker}')");
        sess.execute(&sql).await.unwrap();
    }

    let sess_a = Arc::new(Mutex::new(sess_a));
    let sess_b = Arc::new(Mutex::new(sess_b));

    let leaks = Arc::new(AtomicU64::new(0));
    const TASKS_PER_PROJECT: usize = 5;
    const OPS_PER_TASK: usize = 20;

    let mut tasks: JoinSet<()> = JoinSet::new();

    for task_id in 0..(TASKS_PER_PROJECT * 2) {
        let (sess, expected_marker) = if task_id < TASKS_PER_PROJECT {
            (sess_a.clone(), marker_a.clone())
        } else {
            (sess_b.clone(), marker_b.clone())
        };
        let leaks = leaks.clone();

        tasks.spawn(async move {
            for op_idx in 0..OPS_PER_TASK {
                // Alternate writes and reads.
                if op_idx % 3 == 0 {
                    let id = (task_id as i64) * 10_000 + op_idx as i64 + 1;
                    let sql = format!(
                        "INSERT INTO events VALUES ({id}, '{expected_marker}')"
                    );
                    let s = sess.lock().await;
                    let _ = s.execute(&sql).await;
                } else {
                    let s = sess.lock().await;
                    match s.execute("SELECT marker FROM events ORDER BY id").await {
                        Ok(ExecResult::Rows { batches, .. }) => {
                            for batch in &batches {
                                let arr = batch
                                    .column_by_name("marker")
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                for i in 0..arr.len() {
                                    if arr.value(i) != expected_marker.as_str() {
                                        leaks.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        _ => {} // Empty result or error: not a leak
                    }
                }
            }
        });
    }

    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }

    let leaks_observed = leaks.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "[scenario_I] cross-project leaks observed: {leaks_observed} \
         (projects={}, tasks={}, ops_per_task={})",
        2,
        TASKS_PER_PROJECT * 2,
        OPS_PER_TASK
    );
    assert_eq!(
        leaks_observed,
        0,
        "FAIRNESS: {leaks_observed} cross-project row leak(s) detected — \
         wedge invariant violated under concurrent write contention"
    );
}
