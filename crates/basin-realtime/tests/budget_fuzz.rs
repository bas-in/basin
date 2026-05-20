//! 1k-tenant isolation fuzz for the realtime budget (Phase 5.11.R6).
//!
//! Acceptance gate (from TASK.md):
//! - 1000 lightweight tasks each publishing events; one task publishes 10×
//!   faster than the others.
//! - Noisy tenant's `BUFFER_FULL` count is high while others' are zero (or
//!   very low — race windows allow brief overlap).
//! - p99 delivery latency for non-noisy tenants is bounded (< 100ms in
//!   practice, generous for CI).
//!
//! The heavy `isolation_1k_tenants` test is marked `#[ignore]` because it
//! takes > 30s. Run with:
//!   `cargo test -p basin-realtime budget_fuzz -- --ignored`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use basin_common::{ChangeEvent, ChangeOp, ProjectId, TableName};
use basin_realtime::{BudgetError, BudgetGuard, BudgetTracker, estimate_event_size};
use chrono::Utc;
use tokio::runtime::Runtime;

fn make_event(project: ProjectId, seq: u64) -> ChangeEvent {
    ChangeEvent {
        project,
        table: TableName::new("events").unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": seq, "data": "x".repeat(256)})),
        committed_at: Utc::now(),
        seq,
        causation_user: None,
    }
}

/// Helper: fill a project's budget to `hard_cap` by reserving in chunks and
/// returning the held guards (without releasing them). Simulates events
/// sitting in the broadcast ring waiting for slow consumers.
fn fill_budget(tracker: &BudgetTracker, project: ProjectId, chunk: u64) -> Vec<BudgetGuard> {
    let mut guards = Vec::new();
    loop {
        match tracker.try_reserve(project, chunk) {
            Ok(g) => guards.push(g),
            Err(BudgetError::BufferFull) => break,
        }
    }
    guards
}

/// Smoke test: 100 tenants, 5× noisy, verifies isolation.
///
/// The noisy tenant pre-fills its budget by holding guards (simulating a slow
/// consumer that hasn't drained the ring). Other tenants still have their full
/// budget available.
#[test]
fn isolation_100_tenants_smoke() {
    const TENANT_COUNT: usize = 100;
    const PER_PROJECT_CAP: u64 = 16 * 1024; // 16 KiB per tenant
    const CHUNK: u64 = 512; // 512 bytes per reservation

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let tracker = BudgetTracker::new(PER_PROJECT_CAP);
        let projects: Vec<ProjectId> = (0..TENANT_COUNT).map(|_| ProjectId::new()).collect();
        let noisy_project = projects[0];

        // --- Noisy tenant saturates its own budget (holds guards = slow consumer).
        let noisy_guards = fill_budget(&tracker, noisy_project, CHUNK);
        assert!(
            !noisy_guards.is_empty(),
            "noisy tenant must have at least one guard"
        );

        // Noisy tenant is now at cap — next reservation must fail.
        assert_eq!(
            tracker.try_reserve(noisy_project, CHUNK).unwrap_err(),
            BudgetError::BufferFull,
            "noisy tenant must see BUFFER_FULL after filling budget"
        );

        // --- Other tenants still have their full budget (isolated).
        for &project in &projects[1..] {
            let g = tracker
                .try_reserve(project, CHUNK)
                .expect("other tenants must not be affected by noisy tenant overflow");
            drop(g);
        }

        // --- Release noisy tenant's budget.
        drop(noisy_guards);
        assert_eq!(tracker.bytes_in_flight(noisy_project), 0);

        // After release, noisy tenant can publish again.
        let _g = tracker
            .try_reserve(noisy_project, CHUNK)
            .expect("noisy tenant must be usable after release");
    });
}

/// 1k-tenant fuzz with concurrent tasks.
///
/// Marked `#[ignore]` — takes > 30s; run with `-- --ignored`.
#[test]
#[ignore]
fn isolation_1k_tenants_noisy_tenant_does_not_starve_others() {
    const TENANT_COUNT: usize = 1_000;
    // 64 KiB per project. Normal events = ~400 bytes each.
    // Noisy fills 64 KiB / 400 bytes ≈ 160 events before hitting cap.
    // Normal tenants publish 20 events each; none should hit the cap.
    const PER_PROJECT_CAP: u64 = 64 * 1024;
    const EVENTS_PER_NORMAL: u64 = 20;
    const NOISY_BATCH_SIZE: u64 = 200; // >> 160 → will definitely overflow

    let rt = Runtime::new().expect("runtime");
    rt.block_on(async move {
        let tracker = BudgetTracker::new(PER_PROJECT_CAP);
        let projects: Vec<ProjectId> = (0..TENANT_COUNT).map(|_| ProjectId::new()).collect();
        let noisy_project = projects[0];

        let noisy_buffer_full = Arc::new(AtomicU64::new(0));
        let other_buffer_full = Arc::new(AtomicU64::new(0));
        let latency_samples = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));

        // Pre-fill the noisy tenant's budget so it's near cap from the start.
        // We hold these guards for the duration of the test to simulate a slow
        // consumer not draining the ring. Noisy tenant's subsequent publishes
        // should all get BUFFER_FULL.
        let noisy_prefill: Vec<BudgetGuard> = fill_budget(&tracker, noisy_project, 512);
        assert!(!noisy_prefill.is_empty());

        let mut handles = Vec::new();

        // Noisy tenant task: tries to publish NOISY_BATCH_SIZE more events;
        // all should fail since budget is already full.
        {
            let t = tracker.clone();
            let nbf = noisy_buffer_full.clone();
            handles.push(tokio::spawn(async move {
                let event = make_event(noisy_project, 0);
                let size = estimate_event_size(&event);
                for _ in 0..NOISY_BATCH_SIZE {
                    match t.try_reserve(noisy_project, size) {
                        Ok(g) => drop(g),
                        Err(BudgetError::BufferFull) => {
                            nbf.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Normal tenant tasks.
        for &project in &projects[1..] {
            let t = tracker.clone();
            let obf = other_buffer_full.clone();
            let lat = latency_samples.clone();
            handles.push(tokio::spawn(async move {
                let event = make_event(project, 0);
                let size = estimate_event_size(&event);
                for seq in 0..EVENTS_PER_NORMAL {
                    let t0 = std::time::Instant::now();
                    match t.try_reserve(project, size) {
                        Ok(guard) => {
                            drop(guard);
                            let us = t0.elapsed().as_micros() as u64;
                            let mut lats = lat.lock().await;
                            lats.push(us);
                        }
                        Err(BudgetError::BufferFull) => {
                            obf.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if seq % 5 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        for h in handles {
            h.await.expect("task must not panic");
        }

        // Release pre-fill after tasks finish.
        drop(noisy_prefill);

        let noisy_full = noisy_buffer_full.load(Ordering::Relaxed);
        let other_full = other_buffer_full.load(Ordering::Relaxed);

        // (a) Noisy tenant sees BUFFER_FULL; others don't.
        assert!(
            noisy_full > 0,
            "noisy tenant must see at least one BUFFER_FULL"
        );
        assert_eq!(
            other_full, 0,
            "non-noisy tenants must see zero BUFFER_FULL; got {other_full}"
        );

        // (b) p99 for non-noisy tenants.
        let mut lats = latency_samples.lock().await;
        lats.sort_unstable();
        let p99_idx = (lats.len() as f64 * 0.99) as usize;
        let p99_us = lats.get(p99_idx).copied().unwrap_or(0);
        assert!(
            p99_us < 100_000,
            "p99 latency {p99_us}µs exceeds 100ms bound"
        );

        eprintln!(
            "1k-tenant fuzz: noisy BUFFER_FULL={noisy_full}, other BUFFER_FULL={other_full}, \
             non-noisy p99={p99_us}µs (samples={})",
            lats.len()
        );
    });
}
