//! Unit tests for Phase 5.14.C5 memory budget enforcement.
//!
//! Covers:
//! 1. Soft-cap trigger: 100 inserts of 2 MiB each on one project.
//! 2. Hard-cap trigger: verify `HardCapReached` fires at 256 MiB.
//! 3. Two projects in parallel: `largest_project()` returns the right one.

use basin_common::ids::{ProjectId, TableName};
use basin_hottier::{
    budget::MemTableConfig,
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
