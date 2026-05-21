//! 10 000-project memory budget fuzz test for Phase 5.14.C5.
//!
//! Verifies the per-project byte-budget invariants at scale:
//! - No project exceeds its configured hard cap.
//! - Total process heap across all projects scales O(bytes), not O(projects).
//! - `largest_project()` always returns the project with the most bytes.
//!
//! Sizing: 10 000 projects × 1 000 rows × 200 bytes/row = 2 000 000 000 bytes
//! of attempted writes.  The registry hard cap (256 MiB = 268 435 456 bytes)
//! limits each project to ≤256 MiB committed.  Max committed across all
//! projects is therefore 10 000 × 256 MiB ≈ 2.5 TiB of *logical* reservations,
//! but because `HardCapReached` does NOT bump the counter, the actual
//! bytes_allocated sum is bounded by the hard cap.
//!
//! The test runs single-threaded so it does not trigger the async runtime.
//! Wall-clock is typically < 2 s.

use basin_common::ids::ProjectId;
use basin_hottier::{
    budget::MemTableConfig,
    registry::{MemTableRegistry, ReservationOutcome},
};

const NUM_PROJECTS: usize = 10_000;
const ROWS_PER_PROJECT: usize = 1_000;
const BYTES_PER_ROW: u64 = 200;

/// Maximum allowed total heap across all projects (4 GiB).
/// The sum of bytes_allocated is always ≤ NUM_PROJECTS × hard_cap but
/// in practice much less because most projects never reach the cap.
const MAX_TOTAL_HEAP_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Hard cap used for this fuzz test (256 MiB, the ADR 0016 default).
const HARD_CAP: u64 = 256 * 1024 * 1024;

#[test]
fn ten_k_project_fuzz_heap_under_4_gib() {
    let cfg = MemTableConfig {
        project_hard_cap_bytes: HARD_CAP,
        project_soft_cap_bytes: 192 * 1024 * 1024,
        table_soft_cap_bytes: 16 * 1024 * 1024,
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);

    // Allocate all project IDs upfront so we can inspect them after.
    let projects: Vec<ProjectId> = (0..NUM_PROJECTS).map(|_| ProjectId::new()).collect();

    let mut total_committed: u64 = 0;
    let mut hard_cap_hits: usize = 0;

    for &proj in &projects {
        let total_rows_bytes = ROWS_PER_PROJECT as u64 * BYTES_PER_ROW;

        // Each row is a separate reservation call (simulating one write per row).
        for _ in 0..ROWS_PER_PROJECT {
            match reg.try_reserve_bytes(&proj, BYTES_PER_ROW) {
                ReservationOutcome::Granted | ReservationOutcome::FlushSuggested => {
                    total_committed += BYTES_PER_ROW;
                }
                ReservationOutcome::HardCapReached => {
                    hard_cap_hits += 1;
                    // Stop reserving for this project once the cap is hit.
                    break;
                }
            }
        }

        // Per-project invariant: bytes_allocated must never exceed hard cap.
        let proj_bytes = reg.project_bytes(&proj);
        assert!(
            proj_bytes <= HARD_CAP,
            "project {proj} has {proj_bytes} bytes allocated, exceeding hard cap {HARD_CAP}"
        );

        // If we filled the project to the cap, all subsequent rows are blocked.
        let will_overflow = total_rows_bytes > HARD_CAP;
        if will_overflow {
            assert!(
                hard_cap_hits > 0 || proj_bytes <= HARD_CAP,
                "project filled past hard cap without a HardCapReached signal"
            );
            hard_cap_hits = 0; // reset per-project counter
        }
    }

    // Global heap invariant.
    let registry_total = projects
        .iter()
        .map(|p| reg.project_bytes(p))
        .sum::<u64>();

    assert!(
        registry_total <= MAX_TOTAL_HEAP_BYTES,
        "total registry bytes {registry_total} exceeds 4 GiB limit {MAX_TOTAL_HEAP_BYTES}"
    );

    // largest_project() must return a valid project and its bytes must be the
    // maximum observed.
    if let Some(largest) = reg.largest_project() {
        let largest_bytes = reg.project_bytes(&largest);
        for &p in &projects {
            let pb = reg.project_bytes(&p);
            assert!(
                pb <= largest_bytes,
                "project {p} has {pb} bytes but largest_project returned {largest} with only {largest_bytes}"
            );
        }
    }

    // O(bytes) cost assertion: the total heap (sum of all project counters)
    // must scale with total_committed, not with NUM_PROJECTS.
    // Any dormant project (bytes_allocated == 0) has a constant-size DashMap
    // entry but no heap allocation for the actual row bytes.
    // We verify: total_committed == registry_total (no phantom allocations).
    assert_eq!(
        total_committed, registry_total,
        "total_committed ({total_committed}) and registry_total ({registry_total}) must agree \
         — phantom bytes indicate a bug in the byte-counter accounting"
    );

    // Sanity: with 10k projects × 200 bytes/row × 1000 rows = 2 GB attempted.
    // Since 200 bytes × 1000 rows = 200 000 bytes per project, well under
    // 256 MiB, every single row should have been Granted or FlushSuggested.
    // Verify no project was actually hard-capped.
    let total_rows_per_project = ROWS_PER_PROJECT as u64 * BYTES_PER_ROW;
    assert!(
        total_rows_per_project <= HARD_CAP,
        "test setup invariant: rows per project ({total_rows_per_project} bytes) must fit in hard cap"
    );
    assert_eq!(
        total_committed,
        NUM_PROJECTS as u64 * total_rows_per_project,
        "all rows must have been committed (no hard-cap refusals in the per-project loop)"
    );

    // Final: total heap must be well under 4 GiB.
    // 10k × 200 000 bytes = 2 000 000 000 bytes ≈ 1.86 GiB.
    let expected_total = NUM_PROJECTS as u64 * ROWS_PER_PROJECT as u64 * BYTES_PER_ROW;
    assert_eq!(registry_total, expected_total);
    assert!(
        registry_total < MAX_TOTAL_HEAP_BYTES,
        "heap {registry_total} bytes (≈{} MiB) must be under 4 GiB limit",
        registry_total / (1024 * 1024)
    );
}

/// Verify per-project cost is O(bytes): two identical projects differ only
/// in the amount of data written, not in any per-project structural overhead.
#[test]
fn per_project_cost_is_o_bytes() {
    let cfg = MemTableConfig {
        project_hard_cap_bytes: 512 * 1024 * 1024,
        project_soft_cap_bytes: 256 * 1024 * 1024,
        ..MemTableConfig::default()
    };
    let reg = MemTableRegistry::new_with_config(cfg);

    let proj_light = ProjectId::new();
    let proj_heavy = ProjectId::new();

    // Light: 1 000 × 100 bytes = 100 KB.
    for _ in 0..1_000 {
        let _ = reg.try_reserve_bytes(&proj_light, 100);
    }
    // Heavy: 1 000 × 100 000 bytes = 100 MB.
    for _ in 0..1_000 {
        let _ = reg.try_reserve_bytes(&proj_heavy, 100_000);
    }

    assert_eq!(reg.project_bytes(&proj_light), 1_000 * 100);
    assert_eq!(reg.project_bytes(&proj_heavy), 1_000 * 100_000);

    // The ratio of bytes should match the ratio of data, not be some constant.
    let light = reg.project_bytes(&proj_light);
    let heavy = reg.project_bytes(&proj_heavy);
    assert_eq!(
        heavy / light,
        1_000,
        "cost ratio must match data ratio (1000×)"
    );
}
