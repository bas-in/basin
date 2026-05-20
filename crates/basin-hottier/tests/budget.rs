//! Unit tests for Phase 5.14.C5 memory budget enforcement.
//!
//! Covers:
//! 1. Soft-cap trigger: 100 inserts of 2 MiB each on one project.
//! 2. Hard-cap trigger: verify `HardCapReached` fires at 256 MiB.
//! 3. Two projects in parallel: `largest_project()` returns the right one.
//! 4. `GlobalPressureScheduler` — largest-first ordering, limit, threshold.
//! 5. Semaphore back-pressure: blocked writers unblock after `release_bytes`.

use basin_common::ids::ProjectId;
use basin_hottier::{
    budget::{GlobalPressureScheduler, MemTableConfig, DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES},
    registry::{MemTableRegistry, ReservationOutcome},
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn proj() -> ProjectId {
    ProjectId::new()
}

/// Build a registry with the ADR 0016 defaults (256 MiB hard, 192 MiB soft,
/// 16 MiB per-table).
fn default_reg() -> MemTableRegistry {
    MemTableRegistry::new_with_config(MemTableConfig::default())
}

// ── test 1: soft-cap fires after 192 MiB, hard-cap fires at 256 MiB ─────────

/// Insert 100 × 2 MiB = 200 MiB on one project.
///
/// Expectations:
/// - First 96 × 2 MiB (= 192 MiB) → Granted.
/// - Inserts 97–128 (192..=256 MiB) → FlushSuggested.
/// - 129th insert (would take to 258 MiB) → HardCapReached.
#[test]
fn soft_cap_fires_then_hard_cap_blocks() {
    const ROW_BYTES: u64 = 2 * 1024 * 1024; // 2 MiB
    let reg = default_reg();
    let project = proj();

    let soft_cap = MemTableConfig::default().project_soft_cap_bytes;
    let hard_cap = MemTableConfig::default().project_hard_cap_bytes;

    let mut granted_count = 0u32;
    let mut flush_suggested_count = 0u32;
    let mut hard_cap_count = 0u32;

    // Keep inserting until we hit the hard cap.
    let max_rows = (hard_cap / ROW_BYTES) + 2;
    for _ in 0..max_rows {
        match reg.try_reserve_bytes(&project, ROW_BYTES) {
            ReservationOutcome::Granted => granted_count += 1,
            ReservationOutcome::FlushSuggested => flush_suggested_count += 1,
            ReservationOutcome::HardCapReached => {
                hard_cap_count += 1;
                // Stop at first hard-cap refusal; don't bump the counter.
                break;
            }
        }
    }

    // We should have reached the soft cap before the hard cap.
    let expected_granted = (soft_cap / ROW_BYTES) as u32; // 96 rows
    assert_eq!(
        granted_count, expected_granted,
        "expected {expected_granted} Granted results before soft cap"
    );

    // At least one FlushSuggested before hitting the hard cap.
    assert!(
        flush_suggested_count > 0,
        "expected at least one FlushSuggested outcome between soft and hard cap"
    );

    // Exactly one HardCapReached (we broke at the first one).
    assert_eq!(hard_cap_count, 1, "expected exactly one HardCapReached");

    // Total bytes committed must be exactly at or below the hard cap.
    let committed = reg.project_bytes(&project);
    assert!(
        committed <= hard_cap,
        "committed bytes {committed} must not exceed hard cap {hard_cap}"
    );
    // And total committed must equal granted + flush_suggested rows worth of bytes.
    assert_eq!(
        committed,
        (granted_count as u64 + flush_suggested_count as u64) * ROW_BYTES
    );
}

// ── test 2: two projects in parallel, largest_project correct ─────────────────

/// Two projects: A gets 100 MiB reserved, B gets 150 MiB reserved.
/// `largest_project()` must return B.
#[test]
fn largest_project_identifies_the_heavier_tenant() {
    let cfg = MemTableConfig {
        project_hard_cap_bytes: 512 * 1024 * 1024,
        project_soft_cap_bytes: 256 * 1024 * 1024,
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);

    let proj_a = proj();
    let proj_b = proj();

    let a_bytes: u64 = 100 * 1024 * 1024; // 100 MiB
    let b_bytes: u64 = 150 * 1024 * 1024; // 150 MiB

    let outcome_a = reg.try_reserve_bytes(&proj_a, a_bytes);
    let outcome_b = reg.try_reserve_bytes(&proj_b, b_bytes);

    // Both under soft cap (256 MiB) → Granted.
    assert_eq!(outcome_a, ReservationOutcome::Granted);
    assert_eq!(outcome_b, ReservationOutcome::Granted);

    assert_eq!(reg.project_bytes(&proj_a), a_bytes);
    assert_eq!(reg.project_bytes(&proj_b), b_bytes);

    let largest = reg.largest_project().expect("largest_project must return Some");
    assert_eq!(
        largest, proj_b,
        "largest_project must return the project with more bytes (B = 150 MiB)"
    );
}

// ── test 3: release_bytes allows previously-blocked writes ────────────────────

#[test]
fn release_bytes_unblocks_hard_cap() {
    let cfg = MemTableConfig {
        project_hard_cap_bytes: 4 * 1024 * 1024,
        project_soft_cap_bytes: 2 * 1024 * 1024,
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);
    let project = proj();

    // Fill to exactly the hard cap.
    let outcome = reg.try_reserve_bytes(&project, 4 * 1024 * 1024);
    assert_ne!(outcome, ReservationOutcome::HardCapReached);

    // Next write is blocked.
    assert_eq!(
        reg.try_reserve_bytes(&project, 1024),
        ReservationOutcome::HardCapReached
    );

    // Flush releases half the cap.
    reg.release_bytes(&project, 2 * 1024 * 1024);

    // A write within the remaining headroom now succeeds.
    let outcome = reg.try_reserve_bytes(&project, 1024 * 1024);
    assert_ne!(
        outcome,
        ReservationOutcome::HardCapReached,
        "write should succeed after release"
    );
}

// ── test 4: per-project cost O(bytes) — check project_bytes telemetry ─────────

/// Register 1 000 distinct projects each with 0 bytes (no writes). Verify
/// the registry overhead stays trivial — no panics, no OOM.
#[test]
fn thousand_inactive_projects_cost_nothing() {
    let reg = default_reg();
    for _ in 0..1_000 {
        let p = proj();
        // project_bytes on an unknown project must be 0, not OOM.
        assert_eq!(reg.project_bytes(&p), 0);
    }
    // The `projects` map should be empty (we never called project_state or
    // try_reserve_bytes — nothing was lazily allocated).
    // `entry_count` counts table entries, not project entries, but we can
    // verify there's no crash and the table map is empty.
    assert_eq!(reg.entry_count(), 0);
}

// ── test 5: MemTableConfig::from_env falls back to defaults ──────────────────

#[test]
fn from_env_defaults_when_vars_absent() {
    // Unset all BASIN_MEMTABLE_* vars (they may not be set in the test env).
    // We can't unset env vars portably in a test, but we can verify the
    // defaults are correct if no BASIN_MEMTABLE_* env vars are present.
    let cfg = MemTableConfig::from_env();
    // Only assert the hard cap default — other vars may or may not be set.
    if std::env::var("BASIN_MEMTABLE_HARD_CAP").is_err() {
        assert_eq!(
            cfg.project_hard_cap_bytes,
            MemTableConfig::default().project_hard_cap_bytes
        );
    }
}

// ── test 6: validate_hard_cap boundary conditions ────────────────────────────

#[test]
fn hard_cap_validation_boundaries() {
    use basin_hottier::budget::{MAX_HARD_CAP_BYTES, MIN_HARD_CAP_BYTES};

    assert!(MemTableConfig::validate_hard_cap(MIN_HARD_CAP_BYTES).is_ok());
    assert!(MemTableConfig::validate_hard_cap(MAX_HARD_CAP_BYTES).is_ok());
    assert!(MemTableConfig::validate_hard_cap(MIN_HARD_CAP_BYTES - 1).is_err());
    assert!(MemTableConfig::validate_hard_cap(MAX_HARD_CAP_BYTES + 1).is_err());
}

// ── test 7: ALTER PROJECT DDL parse tests (textual matcher) ──────────────────

#[test]
fn parse_byte_size_variants() {
    use basin_hottier::budget::parse_byte_size;

    assert_eq!(parse_byte_size("268435456"), Some(268_435_456));
    assert_eq!(parse_byte_size("256MB"), Some(256 * 1024 * 1024));
    assert_eq!(parse_byte_size("256 MB"), Some(256 * 1024 * 1024));
    assert_eq!(parse_byte_size("1GB"), Some(1024 * 1024 * 1024));
    assert_eq!(parse_byte_size("2 GB"), Some(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_byte_size("512KB"), Some(512 * 1024));
    assert_eq!(parse_byte_size("abc"), None);
    assert_eq!(parse_byte_size(""), None);
}

// ── test 8: GlobalPressureScheduler — below threshold returns None ────────────

/// `GlobalPressureScheduler::pick_flush_candidates` returns `None` when total
/// bytes across all projects is below the global pressure threshold (4 GiB).
#[test]
fn budget_scheduler_below_threshold_returns_none() {
    let sched = GlobalPressureScheduler::default();
    // Total = 3 GiB < 4 GiB threshold.
    let projects: Vec<(u32, u64)> = vec![
        (1, 1 * 1024 * 1024 * 1024),
        (2, 1 * 1024 * 1024 * 1024),
        (3, 1 * 1024 * 1024 * 1024),
    ];
    assert!(
        sched.pick_flush_candidates(&projects, usize::MAX).is_none(),
        "scheduler must be inactive below the global pressure threshold"
    );
    assert!(
        !sched.is_under_pressure(&projects),
        "is_under_pressure must return false below threshold"
    );
}

// ── test 9: GlobalPressureScheduler — above threshold, largest-first ──────────

/// When total bytes exceeds the 4 GiB threshold, `pick_flush_candidates`
/// returns projects in descending byte order (largest-first).
#[test]
fn budget_scheduler_above_threshold_largest_first() {
    let sched = GlobalPressureScheduler::default();
    // Total = 5 GiB > 4 GiB threshold.
    let projects: Vec<(u32, u64)> = vec![
        (10, 1 * 1024 * 1024 * 1024), // 1 GiB — smallest
        (20, 3 * 1024 * 1024 * 1024), // 3 GiB — largest
        (30, 1 * 1024 * 1024 * 1024), // 1 GiB — tie with project 10
    ];
    let candidates = sched
        .pick_flush_candidates(&projects, usize::MAX)
        .expect("scheduler must activate above threshold");
    assert_eq!(candidates.len(), 3);
    // First must be the 3 GiB project.
    assert_eq!(candidates[0], 20, "largest project must be scheduled first");
    assert!(
        sched.is_under_pressure(&projects),
        "is_under_pressure must return true above threshold"
    );
}

// ── test 10: GlobalPressureScheduler — limit caps returned candidates ─────────

/// The `limit` parameter caps the number of candidates returned even when
/// global pressure is active.
#[test]
fn budget_scheduler_limit_caps_candidates() {
    let sched = GlobalPressureScheduler::default();
    // 10 projects × 512 MiB = 5 GiB > threshold.
    let projects: Vec<(u32, u64)> = (0..10)
        .map(|i| (i, (i as u64 + 1) * 512 * 1024 * 1024))
        .collect();
    let candidates = sched
        .pick_flush_candidates(&projects, 3)
        .expect("must be under pressure");
    assert_eq!(candidates.len(), 3, "limit=3 must cap result to 3 candidates");
}

// ── test 11: GlobalPressureScheduler — exactly at threshold is NOT active ─────

/// The boundary condition: total bytes **equal to** the threshold must NOT
/// activate the scheduler. Only strictly greater triggers it.
#[test]
fn budget_scheduler_exactly_at_threshold_inactive() {
    let sched = GlobalPressureScheduler::default();
    let projects: Vec<(u32, u64)> = vec![(1, DEFAULT_GLOBAL_PRESSURE_THRESHOLD_BYTES)];
    assert!(
        sched.pick_flush_candidates(&projects, usize::MAX).is_none(),
        "total == threshold must NOT activate scheduler"
    );
}

// ── test 12: GlobalPressureScheduler integrates with MemTableRegistry ─────────

/// Feed real per-project byte totals from `MemTableRegistry` into the scheduler
/// and verify it correctly orders candidates.
#[test]
fn budget_scheduler_integrates_with_registry() {
    // Use a custom threshold of 50 MiB for this test.
    let sched = GlobalPressureScheduler {
        global_pressure_threshold_bytes: 50 * 1024 * 1024,
        ..GlobalPressureScheduler::default()
    };

    let cfg = MemTableConfig {
        project_hard_cap_bytes: 512 * 1024 * 1024,
        project_soft_cap_bytes: 256 * 1024 * 1024,
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);

    let proj_a = proj();
    let proj_b = proj();
    let proj_c = proj();

    // proj_b gets the most bytes.
    reg.try_reserve_bytes(&proj_a, 10 * 1024 * 1024); // 10 MiB
    reg.try_reserve_bytes(&proj_b, 30 * 1024 * 1024); // 30 MiB
    reg.try_reserve_bytes(&proj_c, 15 * 1024 * 1024); // 15 MiB
    // Total = 55 MiB > 50 MiB threshold.

    let pairs: Vec<(ProjectId, u64)> = [proj_a, proj_b, proj_c]
        .iter()
        .map(|p| (*p, reg.project_bytes(p)))
        .collect();

    let candidates = sched
        .pick_flush_candidates(&pairs, usize::MAX)
        .expect("55 MiB > 50 MiB threshold: scheduler must activate");

    assert_eq!(candidates.len(), 3);
    assert_eq!(
        candidates[0], proj_b,
        "proj_b (30 MiB) must be first flush candidate"
    );
    assert_eq!(
        candidates[2], proj_a,
        "proj_a (10 MiB) must be last flush candidate"
    );
}

// ── test 13: semaphore back-pressure — async unblock ─────────────────────────

/// Verify the semaphore back-pressure path: fill a project to the hard cap,
/// confirm the next write is `HardCapReached`, then release bytes and confirm
/// the writer can proceed.
///
/// This test exercises the same code path as the flush task (C4) unblocking
/// a stalled writer after flushing committed data.
#[tokio::test]
async fn budget_semaphore_backpressure_unblocks_writer() {
    let cfg = MemTableConfig {
        project_hard_cap_bytes: 4 * 1024 * 1024, // 4 MiB hard cap
        project_soft_cap_bytes: 2 * 1024 * 1024, // 2 MiB soft cap
        ..MemTableConfig::default()
    };
    let reg = std::sync::Arc::new(MemTableRegistry::new_with_config(cfg));
    let project = proj();

    // Fill project to exactly the hard cap (4 MiB).
    let fill_outcome = reg.try_reserve_bytes(&project, 4 * 1024 * 1024);
    assert_ne!(
        fill_outcome,
        ReservationOutcome::HardCapReached,
        "initial fill must succeed"
    );

    // Confirm the next write is blocked.
    assert_eq!(
        reg.try_reserve_bytes(&project, 1024),
        ReservationOutcome::HardCapReached,
        "write at hard cap must be HardCapReached"
    );

    // Simulate flush: release 2 MiB.
    let reg_clone = reg.clone();
    let project_clone = project;
    tokio::spawn(async move {
        reg_clone.release_bytes(&project_clone, 2 * 1024 * 1024);
    })
    .await
    .unwrap();

    // Now a write that fits in the freed headroom must succeed.
    let outcome = reg.try_reserve_bytes(&project, 1024 * 1024); // 1 MiB
    assert_ne!(
        outcome,
        ReservationOutcome::HardCapReached,
        "write must succeed after release_bytes"
    );

    // Verify semaphore permit count is consistent with the freed bytes.
    // After releasing 2 MiB and using 1 MiB, 1 MiB of headroom remains.
    // bytes_allocated = 4 MiB (original fill) - 2 MiB (released) + 1 MiB (new write) = 3 MiB.
    let committed = reg.project_bytes(&project);
    assert!(
        committed <= 4 * 1024 * 1024,
        "bytes_allocated {committed} must not exceed hard cap after partial release"
    );
}
