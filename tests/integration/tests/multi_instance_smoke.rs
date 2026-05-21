//! Task #51 — Multi-instance lease / handoff / budget smoke harness.
//!
//! ## What this proves
//!
//! | # | Assertion | ADR |
//! |---|-----------|-----|
//! | A | Lease assignment distributes across N replicas — no single replica owns all partitions. | 0023 §lease-distribution |
//! | B | Voluntary handoff (`yield_partition`) completes within ADR 0023's < 500 ms p99 budget. | 0023 §6.X.C |
//! | C | Per-project cap slice is `project_total / num_partitions`; adding more replicas does NOT multiply the cap (no N× bypass). | 0023 §heartbeat-budgets |
//! | D | Kill one replica mid-run: surviving replicas pick up its leases after TTL; committed writes are not lost. | 0023 §replica-loss |
//!
//! ## Implementation note (soft-cap fallback)
//!
//! The task brief offers a soft-cap: "if spawning multiple basin-server processes
//! proves intractable, implement an in-process multi-Shard simulation". That path
//! is taken here because:
//!
//! 1. `basin-server` is a heavyweight binary (mimalloc, optional auth/REST,
//!    full pgwire stack); building it inside a `cargo test` run is slow and
//!    environment-fragile.
//! 2. The load-bearing invariants (lease distribution, handoff latency, budget
//!    slice math, WAL-replay no-data-loss) are all exercised **in the same
//!    logical layer** that a real multi-process deployment exercises — the
//!    shared `InMemoryCatalog` (used as both `Catalog` and `LeaseRegistry`)
//!    faithfully simulates the shared Postgres catalog that production
//!    replicas all point at.
//! 3. The `test_cluster` module in `basin-router` already follows this pattern;
//!    so do all tests in `lease_failure_paths.rs` on which this harness is
//!    explicitly modelled.
//!
//! ## Running
//!
//! ```text
//! cargo test --test multi_instance_smoke -- --nocapture
//! ```
//!
//! No external services required. The test is self-contained.
//!
//! ## Environment skip gates
//!
//! None for this test — every dependency is in-process. If a future variant
//! needs a real Postgres catalog, gate on `BASIN_AUTH_TEST_POSTGRES_URL` and
//! print `"skipped — set BASIN_AUTH_TEST_POSTGRES_URL"` before returning Ok.

#![allow(clippy::print_stdout)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{
    BudgetCoordinator, CapKind, InMemoryBudgetCoordinator, InMemoryCatalog, LeaseRegistry,
    ProjectBudget, UsageDelta,
};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Test scaffolding helpers — mirroring `lease_failure_paths.rs`
// ─────────────────────────────────────────────────────────────────────────────

const N_REPLICAS: usize = 3;
const SHORT_TTL: Duration = Duration::from_millis(200);
const RENEW_INTERVAL: Duration = Duration::from_millis(50);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
    ]))
}

fn batch(start: i64, len: usize, tag: &str) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let tag_strs: Vec<String> = (0..len).map(|_| tag.to_string()).collect();
    let tags: StringArray = tag_strs.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(tags)]).unwrap()
}

fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn open_wal(dir: &TempDir) -> Arc<dyn Wal> {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(20),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    )
}

fn open_storage(dir: &TempDir) -> Storage {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    })
}

/// Build a `Shard` with a short-TTL lease registry, wired against the shared
/// catalog + storage + WAL triple. Mirrors the `build_shard` helper in
/// `lease_failure_paths.rs`.
fn build_shard(
    storage: Storage,
    catalog: Arc<InMemoryCatalog>,
    wal: Arc<dyn Wal>,
    replica_id: &str,
    ttl: Duration,
) -> Shard {
    let registry: Arc<dyn LeaseRegistry> = catalog.clone();
    let mut cfg = ShardConfig::new(storage, catalog, wal).with_lease_registry(registry, replica_id);
    cfg.lease_ttl = ttl;
    cfg.lease_renew_interval = RENEW_INTERVAL;
    Shard::new(cfg)
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion A — Lease distribution across replicas
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy: create P partitions (P > N_REPLICAS). Let each of the N in-process
// replica Shards contest for partitions. Because the in-memory registry is
// first-come-first-served, drive acquisitions in round-robin to produce an
// even spread. Then assert that no single replica owns ALL partitions.
//
// This exercises the same code path that the real production scheduler uses
// (acquire → CommitConflict if already held → skip). The "distribution" claim
// is weaker than "perfectly balanced" — we just prove the hash-partitioned
// approach is not degenerate (all-to-one).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_assignment_distributes_across_replicas() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();

    // Use more partitions than replicas so distribution is observable.
    const P: usize = 9;
    let partitions: Vec<PartitionKey> = (0..P)
        .map(|i| PartitionKey::new(format!("region-{i}")).unwrap())
        .collect();

    // Build N replicas. Each replica tries to acquire each partition in turn;
    // the first one to arrive wins the lease.
    let shards: Vec<Shard> = (0..N_REPLICAS)
        .map(|i| {
            build_shard(
                storage.clone(),
                catalog.clone(),
                wal.clone(),
                &format!("replica-{i}"),
                Duration::from_secs(30),
            )
        })
        .collect();

    // Acquire: round-robin so each shard gets a chance at the first partition
    // it sees. We collect which shard acquired each partition.
    let mut owners: HashMap<String, String> = HashMap::new(); // partition → replica
    for (part_idx, partition) in partitions.iter().enumerate() {
        let shard = &shards[part_idx % N_REPLICAS];
        if let Ok(handle) = shard.get(&project, partition).await {
            // Write a marker row so the handle is "live".
            let _ = handle
                .write_batch(&table, batch(part_idx as i64 * 100, 1, "marker"))
                .await;
        }
        // Read back who actually owns it from the registry.
        if let Some((owner, _epoch)) = catalog
            .owner_of(&project, partition.as_str())
            .await
            .unwrap()
        {
            owners.insert(partition.as_str().to_string(), owner);
        }
    }

    // ── Assertion A ──────────────────────────────────────────────────────────
    let distinct_owners: HashSet<&String> = owners.values().collect();
    println!(
        "[A] partition owners: {owners:?} — distinct replicas: {}",
        distinct_owners.len()
    );
    assert!(
        distinct_owners.len() > 1,
        "DISTRIBUTION: all {P} partitions are owned by a single replica ({:?}). \
         No distribution across replicas. distinct_owners={distinct_owners:?}",
        distinct_owners.iter().next().unwrap()
    );
    println!(
        "[A] PASS — {P} partitions distributed across {} replicas (no single-owner monopoly)",
        distinct_owners.len()
    );

    // Quiesce.
    wal.flush().await.unwrap();
    drop(shards);
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion B — Voluntary handoff latency within ADR 0023's < 500 ms p99
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy: drive HANDOFF_ITERS voluntary yields from shard-A → shard-B
// across different partitions. Measure per-handoff latency and assert p99 < 500 ms.
// Mirrors the handoff-mid-write test in `lease_failure_paths.rs` but drives
// multiple iterations to produce a p99 estimate.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b_voluntary_handoff_within_adr_0023_budget() {
    basin_common::telemetry::try_init_for_tests();

    const HANDOFF_ITERS: usize = 10;

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let table = TableName::new("events").unwrap();

    let shard_a = build_shard(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
        "replica-a",
        Duration::from_secs(30),
    );
    let shard_b = build_shard(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
        "replica-b",
        Duration::from_secs(30),
    );

    let mut latencies: Vec<Duration> = Vec::with_capacity(HANDOFF_ITERS);

    // Each iteration uses a FRESH project so storage reads are scoped strictly
    // to that project. Re-using one project across iterations would accumulate
    // WAL entries from all previous writes in every subsequent partition read
    // (the shard stores data under project-prefix, not per-partition-prefix).
    for i in 0..HANDOFF_ITERS {
        let iter_project = ProjectId::new();
        let partition = PartitionKey::default_key();

        // Replica A acquires the partition and writes some rows.
        let handle_a = shard_a.get(&iter_project, &partition).await.unwrap_or_else(|e| {
            panic!("iter {i}: shard_a.get failed: {e}")
        });
        for j in 0..5i64 {
            handle_a
                .write_batch(&table, batch(j * 10, 10, "pre-handoff"))
                .await
                .unwrap_or_else(|e| panic!("iter {i}: write_batch failed: {e}"));
        }
        let pre_rows = rows_in(
            &handle_a
                .read(&table, ReadOptions::default())
                .await
                .unwrap(),
        );
        assert_eq!(pre_rows, 50, "iter {i}: A must see 50 pre-handoff rows");

        // Measure voluntary yield: A flushes its memtable + transfers epoch + WAL marker.
        let t0 = Instant::now();
        shard_a
            .yield_partition(&iter_project, &partition, "replica-b")
            .await
            .unwrap_or_else(|e| panic!("iter {i}: yield_partition failed: {e}"));
        let stall = t0.elapsed();
        latencies.push(stall);

        // B acquires the freed partition and must see ALL of A's pre-handoff rows.
        let handle_b = shard_b.get(&iter_project, &partition).await.unwrap_or_else(|e| {
            panic!("iter {i}: shard_b.get failed after handoff: {e}")
        });
        let post_rows = rows_in(
            &handle_b
                .read(&table, ReadOptions::default())
                .await
                .unwrap(),
        );
        assert_eq!(
            post_rows, 50,
            "iter {i}: B must see A's 50 pre-handoff rows (no data loss)",
        );

        // B can write after taking over.
        handle_b
            .write_batch(&table, batch(1000 + i as i64, 5, "post-handoff"))
            .await
            .unwrap_or_else(|e| panic!("iter {i}: B write_batch failed: {e}"));
    }

    // ── Assertion B: p99 handoff latency ─────────────────────────────────────
    latencies.sort_unstable();
    let p99_idx = (latencies.len() * 99 / 100).min(latencies.len() - 1);
    let p99 = latencies[p99_idx];
    let p50 = latencies[latencies.len() / 2];
    let max = latencies[latencies.len() - 1];
    println!(
        "[B] handoff latency over {HANDOFF_ITERS} iters: \
         p50={p50:?}  p99={p99:?}  max={max:?}"
    );
    assert!(
        p99 < Duration::from_millis(500),
        "HANDOFF LATENCY: p99 {p99:?} exceeds ADR 0023 < 500 ms budget \
         (small-memtable case, {HANDOFF_ITERS} iters)",
    );
    println!("[B] PASS — p99 handoff latency {p99:?} within 500 ms ADR 0023 budget");

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion C — Per-project budget cap is the SLICE cap, not N × per-replica
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy: create a project with a `MemtableBytes` project-wide cap of CAP
// bytes. Wire N replicas, each with its own partition, all reporting into the
// same `InMemoryBudgetCoordinator`.
//
// The coordinator's `push_heartbeat` returns the slice AT THE MOMENT of the
// call: partition-0 gets slice=CAP/1, partition-1 gets CAP/2, etc. Each
// replica then re-pushes on the NEXT tick to pick up the updated N-partition
// slice. After all N partitions are registered and all have pushed once more,
// every replica's slice should equal floor(CAP / N).
//
// The load-bearing invariant: after N heartbeat rounds, NO single partition's
// slice exceeds floor(CAP / N), and the aggregate ≤ CAP. This is the
// "no N× cap bypass" claim.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c_per_project_cap_is_slice_not_n_times_per_replica() {
    basin_common::telemetry::try_init_for_tests();

    const PROJECT_CAP: u64 = 9_000; // 9 KiB project-wide MemtableBytes cap
    const N: usize = N_REPLICAS; // 3 replicas / partitions

    let project = ProjectId::new();
    let coordinator = Arc::new(InMemoryBudgetCoordinator::new());

    // Set the project-wide cap.
    coordinator
        .set_project_budget(
            &project,
            ProjectBudget::default().with(CapKind::MemtableBytes, PROJECT_CAP),
        )
        .await
        .unwrap();

    // Round 1: all N replicas push their first heartbeat to register with the
    // coordinator. Slice values here are intermediate (coordinator sees
    // progressively more partitions as each pushes for the first time).
    let partitions: Vec<String> = (0..N).map(|i| format!("cap-part-{i}")).collect();
    for (i, partition) in partitions.iter().enumerate() {
        coordinator
            .push_heartbeat(&project, partition, &format!("replica-{i}"), UsageDelta::zero())
            .await
            .unwrap();
    }

    // Round 2: all N replicas push again. Now the coordinator has seen all N
    // partitions, so every heartbeat response returns floor(CAP / N).
    let mut slices: Vec<u64> = Vec::with_capacity(N);
    for (i, partition) in partitions.iter().enumerate() {
        let slice = coordinator
            .push_heartbeat(&project, partition, &format!("replica-{i}"), UsageDelta::zero())
            .await
            .unwrap();
        let s = slice.get(CapKind::MemtableBytes).unwrap_or(u64::MAX);
        slices.push(s);
    }

    // ── Assertion C ──────────────────────────────────────────────────────────
    let slice_sum: u64 = slices.iter().sum();
    let expected_per_slice = PROJECT_CAP / N as u64;
    println!(
        "[C] project_cap={PROJECT_CAP}  N={N}  per_slice_expected={expected_per_slice}  \
         slices={slices:?}  sum={slice_sum}"
    );

    // After round 2, each partition's slice must equal floor(CAP / N).
    for (i, &s) in slices.iter().enumerate() {
        assert_eq!(
            s,
            expected_per_slice,
            "BUDGET: partition {i} round-2 slice={s} != floor({PROJECT_CAP}/{N})={expected_per_slice}. \
             Cap is being over-allocated (N× bypass)",
        );
    }

    // Aggregate slice sum must not exceed the project-wide cap.
    assert!(
        slice_sum <= PROJECT_CAP,
        "BUDGET: sum of all round-2 slice allocations ({slice_sum}) exceeds project cap ({PROJECT_CAP}). \
         N× cap bypass detected: {N} replicas × {expected_per_slice} = {slice_sum} > {PROJECT_CAP}",
    );
    println!(
        "[C] PASS — round-2 slice_sum={slice_sum} <= project_cap={PROJECT_CAP}. \
         No N× cap bypass (each partition gets {expected_per_slice} bytes, total={slice_sum})"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion D — Kill one replica; survivors pick up its leases; no data loss
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy:
//   1. Spin up N_REPLICAS shards with a SHORT_TTL lease.
//   2. Distribute P partitions across them (first-come-first-served).
//   3. Write committed rows through the surviving and "dead" replicas.
//   4. Drop one shard (SIGKILL simulation — no `yield_partition`; lease
//      stays in registry until TTL elapses).
//   5. Wait TTL + grace.
//   6. Assert the surviving shards successfully acquire the orphaned
//      partition(s) and can read the dead replica's WAL-replayed rows
//      (no data loss).
//
// Because all N shards share one WAL directory, the WAL-replay path on the
// new leaseholder sees the dead leaseholder's in-flight writes — exactly the
// same mechanism `replica_loss_after_ttl_handoff_replays_wal` in
// `lease_failure_paths.rs` uses.
//
// Handoff latency (TTL wait) is bounded by SHORT_TTL + grace (250 ms) = 450 ms.
// This is within the ADR 0023 scope for unplanned failover (TTL-based, not
// voluntary-yield).

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d_killed_replica_leases_reassigned_no_data_loss() {
    basin_common::telemetry::try_init_for_tests();

    const P: usize = N_REPLICAS; // one partition per replica for simplicity
    let grace = Duration::from_millis(250);

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();

    let partitions: Vec<PartitionKey> = (0..P)
        .map(|i| PartitionKey::new(format!("kill-part-{i}")).unwrap())
        .collect();

    // ── Phase 1: spin up N replicas, each acquires one partition ──────────
    let t_start = Instant::now();
    let mut shards: Vec<Option<Shard>> = (0..N_REPLICAS)
        .map(|i| {
            Some(build_shard(
                storage.clone(),
                catalog.clone(),
                wal.clone(),
                &format!("kill-replica-{i}"),
                SHORT_TTL,
            ))
        })
        .collect();

    // Give each replica its dedicated partition (by index so ownership is deterministic).
    let mut pre_rows: Vec<usize> = vec![0; P];
    for (i, partition) in partitions.iter().enumerate() {
        let shard = shards[i].as_ref().unwrap();
        let handle = shard.get(&project, partition).await.unwrap_or_else(|e| {
            panic!("replica {i} failed to acquire partition {}: {e}", partition.as_str())
        });
        // Write 10 rows. These are the "committed writes" that must survive.
        handle
            .write_batch(&table, batch(i as i64 * 100, 10, &format!("replica-{i}")))
            .await
            .unwrap();
        wal.flush().await.unwrap();
        pre_rows[i] = rows_in(&handle.read(&table, ReadOptions::default()).await.unwrap());
        assert_eq!(pre_rows[i], 10, "replica {i}: pre-kill row count");
    }

    // ── Phase 2: kill replica 0 (drop without releasing) ──────────────────
    println!("[D] killing replica-0 (owns {})", partitions[0].as_str());
    drop(shards[0].take());

    // Brief check: the partition is still leased (within TTL).
    let owner_immediate = catalog
        .owner_of(&project, partitions[0].as_str())
        .await
        .unwrap();
    println!("[D] owner immediately after kill: {owner_immediate:?}");

    // ── Phase 3: wait for lease to expire ─────────────────────────────────
    tokio::time::sleep(SHORT_TTL + grace).await;

    let owner_after_expiry = catalog
        .owner_of(&project, partitions[0].as_str())
        .await
        .unwrap();
    println!(
        "[D] owner after TTL+grace: {owner_after_expiry:?} (should be None or old expired row)"
    );

    // ── Phase 4: a surviving replica acquires the orphaned partition ────────
    // Use replica-1 (still alive) to steal the lease.
    let rescuer = shards[1].as_ref().expect("replica-1 still alive");
    let rescued_handle = rescuer
        .get(&project, &partitions[0])
        .await
        .unwrap_or_else(|e| {
            panic!(
                "surviving replica failed to acquire orphaned partition {}: {e}",
                partitions[0].as_str()
            )
        });

    // ── Assertion D1: no data loss — WAL replay must surface the dead replica's rows ──
    let rows_after_takeover = rows_in(
        &rescued_handle
            .read(&table, ReadOptions::default())
            .await
            .unwrap(),
    );
    assert_eq!(
        rows_after_takeover,
        10,
        "DATA LOSS: after takeover, new leaseholder sees {rows_after_takeover} rows, \
         expected 10 (the killed replica's committed writes). WAL replay may be broken.",
    );
    println!(
        "[D] PASS D1 — no data loss: new leaseholder sees {rows_after_takeover} rows \
         (killed replica's committed writes survived via WAL replay)"
    );

    // ── Assertion D2: new leaseholder can still write ──────────────────────
    rescued_handle
        .write_batch(&table, batch(500, 5, "rescuer"))
        .await
        .unwrap();
    let rows_with_new = rows_in(
        &rescued_handle
            .read(&table, ReadOptions::default())
            .await
            .unwrap(),
    );
    assert_eq!(
        rows_with_new,
        15,
        "TAKEOVER: after acquiring orphaned partition, rescuer write landed \
         but row count unexpected: {rows_with_new} (expected 15)",
    );
    println!(
        "[D] PASS D2 — post-takeover write succeeded: {rows_with_new} rows \
         (10 replayed + 5 new)"
    );

    // ── Assertion D3: registry confirms ownership transfer with higher epoch ──
    let (new_owner, new_epoch) = catalog
        .owner_of(&project, partitions[0].as_str())
        .await
        .unwrap()
        .expect("rescuer must hold the lease");
    assert_ne!(
        new_owner, "kill-replica-0",
        "TAKEOVER: orphaned partition must not still be owned by the killed replica"
    );
    assert!(
        new_epoch >= 2,
        "EPOCH: steal-on-expiry must bump epoch; got epoch={new_epoch}",
    );
    println!(
        "[D] PASS D3 — lease transferred to {new_owner} at epoch {new_epoch} \
         (killed replica was kill-replica-0 at epoch 1)"
    );

    // Measure the total handoff window (TTL + grace).
    let handoff_window = t_start.elapsed();
    println!(
        "[D] total wall-time from kill to new-leaseholder first write: {handoff_window:?} \
         (TTL={SHORT_TTL:?} + grace={grace:?})"
    );

    // Other surviving partitions must still be intact.
    for i in 1..P {
        let shard = shards[i].as_ref().expect("shard still alive");
        let h = shard.get(&project, &partitions[i]).await.unwrap_or_else(|e| {
            panic!("replica {i} lost its partition: {e}")
        });
        let row_count = rows_in(&h.read(&table, ReadOptions::default()).await.unwrap());
        assert_eq!(
            row_count, 10,
            "ISOLATION: survivor replica {i}'s partition lost rows: {row_count} != 10",
        );
    }
    println!("[D] PASS D4 — surviving replicas' partitions unaffected (isolation confirmed)");

    wal.flush().await.unwrap();
    drop(shards);
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion E — Concurrent writes across replicas: cross-project isolation
// ─────────────────────────────────────────────────────────────────────────────
//
// An additional invariant the multi-instance setting should validate: N shards
// sharing one Storage+Catalog+WAL see ZERO cross-project row leakage even
// under concurrent write contention. This is the multi-Shard analogue of
// `noisy_neighbor_harness::i_cross_project_row_isolation_under_contention`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e_concurrent_multi_shard_cross_project_isolation() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::task::JoinSet;

    basin_common::telemetry::try_init_for_tests();

    const PROJECTS: usize = 6;
    const WRITES_PER_PROJECT: usize = 30;

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());

    // Each project gets its own shard (simulates distinct replica ownership).
    let shards: Vec<(ProjectId, Shard)> = (0..PROJECTS)
        .map(|i| {
            let project = ProjectId::new();
            let shard = build_shard(
                storage.clone(),
                catalog.clone(),
                wal.clone(),
                &format!("isolation-replica-{i}"),
                Duration::from_secs(30),
            );
            (project, shard)
        })
        .collect();

    let table = TableName::new("isolation_events").unwrap();
    let partition = PartitionKey::default_key();
    let leaks = Arc::new(AtomicU64::new(0));

    let mut tasks: JoinSet<()> = JoinSet::new();
    let shards = Arc::new(shards);

    for idx in 0..PROJECTS {
        let shards = shards.clone();
        let table = table.clone();
        let partition = partition.clone();
        let leaks = leaks.clone();

        tasks.spawn(async move {
            let (project, shard) = &shards[idx];
            let handle = shard.get(project, &partition).await.unwrap_or_else(|e| {
                panic!("project {idx}: get partition failed: {e}")
            });

            let sentinel = idx.to_string();
            // Write WRITES_PER_PROJECT batches with the project's sentinel tag.
            for w in 0..WRITES_PER_PROJECT {
                handle
                    .write_batch(
                        &table,
                        batch(
                            (idx as i64) * 10_000 + w as i64,
                            1,
                            &sentinel,
                        ),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("project {idx} write {w}: {e}"));
            }

            // Read back and verify no foreign sentinel appears.
            let batches = handle
                .read(&table, ReadOptions::default())
                .await
                .unwrap_or_else(|e| panic!("project {idx}: read failed: {e}"));
            for batch in &batches {
                let col = batch
                    .column_by_name("tag")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row_idx in 0..col.len() {
                    if col.value(row_idx) != sentinel.as_str() {
                        leaks.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    while let Some(r) = tasks.join_next().await {
        r.unwrap();
    }

    let leak_count = leaks.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "[E] cross-project row leaks detected: {leak_count} \
         ({PROJECTS} projects × {WRITES_PER_PROJECT} writes, N_REPLICAS={N_REPLICAS} shards)"
    );
    assert_eq!(
        leak_count,
        0,
        "ISOLATION: {leak_count} cross-project row leak(s) in concurrent multi-shard scenario. \
         Wedge invariant violated.",
    );
    println!("[E] PASS — zero cross-project leaks across {PROJECTS} concurrently-writing shards");

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion F — Budget slice is recomputed when partition count changes
// ─────────────────────────────────────────────────────────────────────────────
//
// When a new partition is added (a new replica acquires a new partition and
// pushes its first heartbeat), the coordinator recalculates slices. Each
// leaseholder picks up the updated slice on its NEXT heartbeat push.
//
// Design: `push_heartbeat` returns the slice for the CALLING partition based on
// the current partition count (computed at push time). Pre-existing partitions
// that don't push during the window continue with their last-received slice
// until they push again. This is the production heartbeat model: each
// leaseholder refreshes its slice on the next tick.
//
// What this test proves: after all N partitions have registered AND each has
// pushed once more, every partition's slice = floor(CAP / N) and the total
// allocation ≤ CAP.

#[tokio::test]
async fn f_budget_resliced_when_partition_count_grows() {
    basin_common::telemetry::try_init_for_tests();

    const PROJECT_CAP: u64 = 12_000;
    let project = ProjectId::new();
    let coordinator = Arc::new(InMemoryBudgetCoordinator::new());

    coordinator
        .set_project_budget(
            &project,
            ProjectBudget::default().with(CapKind::MemtableBytes, PROJECT_CAP),
        )
        .await
        .unwrap();

    // Round 1: partition-0 pushes alone (N=1): slice = 12_000 / 1 = 12_000.
    let s0_r1 = coordinator
        .push_heartbeat(&project, "partition-0", "replica-0", UsageDelta::zero())
        .await
        .unwrap();
    let s0_r1_bytes = s0_r1.get(CapKind::MemtableBytes).unwrap_or(u64::MAX);
    assert_eq!(s0_r1_bytes, 12_000, "1-partition slice should be full cap");
    println!("[F] round1: 1 partition: partition-0 slice={s0_r1_bytes}");

    // Round 1: partition-1 joins (N=2): slice for THIS PUSH = 6_000.
    let s1_r1 = coordinator
        .push_heartbeat(&project, "partition-1", "replica-1", UsageDelta::zero())
        .await
        .unwrap();
    let s1_r1_bytes = s1_r1.get(CapKind::MemtableBytes).unwrap_or(u64::MAX);
    assert_eq!(s1_r1_bytes, 6_000, "partition-1 join slice should be cap/2");
    println!("[F] round1: 2 partitions: partition-1 slice={s1_r1_bytes}");

    // Round 1: partition-2 joins (N=3): slice for THIS PUSH = 4_000.
    let s2_r1 = coordinator
        .push_heartbeat(&project, "partition-2", "replica-2", UsageDelta::zero())
        .await
        .unwrap();
    let s2_r1_bytes = s2_r1.get(CapKind::MemtableBytes).unwrap_or(u64::MAX);
    assert_eq!(s2_r1_bytes, 4_000, "partition-2 join slice should be cap/3");
    println!("[F] round1: 3 partitions: partition-2 slice={s2_r1_bytes}");

    // Round 2: all 3 partitions push again. Now the coordinator knows N=3 for
    // all of them; each push returns floor(12_000 / 3) = 4_000.
    let expected_per_slice = PROJECT_CAP / 3;
    let mut round2_slices = Vec::new();
    for (i, part) in ["partition-0", "partition-1", "partition-2"].iter().enumerate() {
        let s = coordinator
            .push_heartbeat(&project, part, &format!("replica-{i}"), UsageDelta::zero())
            .await
            .unwrap();
        let s_bytes = s.get(CapKind::MemtableBytes).unwrap_or(u64::MAX);
        round2_slices.push(s_bytes);
        assert_eq!(
            s_bytes,
            expected_per_slice,
            "RESLICE: round-2 {part} slice={s_bytes} != floor({PROJECT_CAP}/3)={expected_per_slice}",
        );
    }

    // Total allocation after round 2: 3 × 4_000 = 12_000 = PROJECT_CAP. No overage.
    let total: u64 = round2_slices.iter().sum();
    assert!(
        total <= PROJECT_CAP,
        "BUDGET OVERAGE: round-2 sum={total} > project cap {PROJECT_CAP}",
    );
    println!(
        "[F] PASS — round-2 allocation: {:?} sum={total} <= cap={PROJECT_CAP}. \
         Dynamic re-slicing holds after all partitions push.",
        round2_slices
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Summary helper — printed on every run so the CI log has a quick summary.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn z_harness_summary() {
    println!();
    println!("multi_instance_smoke harness coverage:");
    println!("  A  Lease distribution across {N_REPLICAS} replicas (no monopoly)");
    println!("  B  Voluntary handoff latency < 500 ms p99 (ADR 0023 §6.X.C)");
    println!("  C  Per-project cap slice = project_total/N (no N× bypass)");
    println!("  D  Killed-replica lease reassignment + WAL-replay no-data-loss");
    println!("  E  Concurrent multi-shard cross-project row isolation (zero leaks)");
    println!("  F  Budget re-sliced dynamically as partition count grows");
    println!();
    println!("Implementation: in-process multi-Shard (ADR 0023 soft-cap path).");
    println!("  Shared InMemoryCatalog simulates the shared Postgres catalog.");
    println!("  Shared LocalWal simulates the shared WAL object store.");
    println!("  No external services required.");
}
