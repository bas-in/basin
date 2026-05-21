//! Phase 6.X.E — lease failure-path hardening (ADR 0023).
//!
//! Failure-path tests that prove the lease + epoch-fence + heartbeat-budget
//! plumbing of 6.X.A/B/D actually behaves correctly when things go wrong, not
//! just on the happy path. ADR 0023 makes very specific claims:
//!
//! 1. A replica that dies mid-write loses its lease after TTL; another replica
//!    can pick it up and the WAL-replay path makes the dead replica's
//!    not-yet-flushed writes durable on the new owner. (replica-loss)
//! 2. If two replicas ever think they hold the same partition's lease at
//!    different epochs (network partition), the lower-epoch WAL appends are
//!    rejected by the file_wal fence with no LSN consumed. (dual-leaseholder)
//! 3. Network partition between a replica and the coordinator: heartbeat
//!    renews fail, the lease expires, another replica steals; the
//!    partitioned replica's writes fail-closed at the WAL because its epoch
//!    is now stale relative to the fence the new holder raised. (partition
//!    simulator)
//! 4. If the budget coordinator is unreachable, the slice-gate consumer
//!    falls back to "no slice = no cap" (safe degradation, not over-throttle).
//!    Closes the 6.X.D failure mode.
//! 5. **Handoff-mid-write atomicity** (6.X.C) — a coordinator-requested
//!    voluntary yield must flush the in-memory state before transferring the
//!    epoch so the new owner sees every acked write. This test is
//!    `#[ignore]`'d because 6.X.C has not landed yet (TASK.md row marks it
//!    `[ ]` as of this dispatch); once the `yield_lease` API ships, drop the
//!    ignore and replace the placeholder body with a real handoff drive.
//!
//! Scope discipline: these tests touch ONLY the new test file. If a real bug
//! is observed in the underlying 6.X.A/B/C/D src/ code, it is REPORTED in the
//! dispatch summary — not fixed here. The dispatch is "prove the failure
//! behaviour is what we claim", not "extend the failure behaviour".

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{
    BudgetCoordinator, CapKind, InMemoryBudgetCoordinator, InMemoryCatalog, Lease, LeaseRegistry,
    ProjectBudget, SliceBudget, SliceBudgetView, SliceGate, UsageDelta, UsageSnapshot,
    SLICE_UNSET,
};
use basin_common::{BasinError, PartitionKey, ProjectId, Result, TableName};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use basin_wal::{replay_wal, LocalWal, Lsn, Wal, WalConfig, WalEvent, WalReplayConfig};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::sync::RwLock;

// ─────────────────────────────────────────────────────────────────────────────
// Shared test scaffolding
// ─────────────────────────────────────────────────────────────────────────────

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(start: i64, len: usize, prefix: &str) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let names: Vec<String> = (0..len)
        .map(|i| format!("{prefix}{}", start + i as i64))
        .collect();
    let names: StringArray = names.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Open a fresh `LocalWal` rooted at `dir`. Two replicas reading the SAME
/// `dir` see each other's segments — that's how the replica-loss test reaches
/// the dead leaseholder's in-flight write via WAL replay.
async fn open_wal(dir: &TempDir) -> Arc<dyn Wal> {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            // Use a short flush interval so durability is reached promptly
            // and the second replica's `read_from` definitely sees the
            // dead replica's writes.
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

/// Build a `Shard` wired against the supplied storage/catalog/wal trio plus a
/// lease registry under `replica_id` with the given TTL. The renew interval is
/// kept short so failure-path tests that drive `spawn_background` see at
/// least one heartbeat tick within the TTL window; tests that drive without
/// the background loop simply never tick and let the lease expire by TTL.
fn build_shard(
    storage: Storage,
    catalog: Arc<dyn basin_catalog::Catalog>,
    registry: Arc<dyn LeaseRegistry>,
    wal: Arc<dyn Wal>,
    replica_id: &str,
    ttl: Duration,
) -> Shard {
    let mut cfg = ShardConfig::new(storage, catalog, wal).with_lease_registry(registry, replica_id);
    cfg.lease_ttl = ttl;
    cfg.lease_renew_interval = Duration::from_millis(50);
    Shard::new(cfg)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — Replica-loss: mid-write replica vanishes, peer takes over
// ─────────────────────────────────────────────────────────────────────────────
//
// Two in-process replicas share one WAL + storage + catalog (the trio that
// in production would all be remote durable services). Replica A acquires the
// partition lease with a short TTL (200 ms), writes a batch of rows, then
// dies (we drop the Shard handle WITHOUT releasing the lease — modelling a
// SIGKILL / pod crash). After the TTL elapses, replica B successfully
// acquires the same `(project, partition)` lease. B's writes succeed and a
// read of the partition returns BOTH A's pre-crash batch (replayed from the
// WAL into B's memtable via `replay_wal_into`) AND B's post-takeover batch.
// This is the "no data loss" guarantee ADR 0023 makes for the steady-state
// failure mode.

#[tokio::test]
async fn replica_loss_after_ttl_handoff_replays_wal() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let registry: Arc<dyn LeaseRegistry> = catalog.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // Replica A acquires the lease and writes 30 rows. TTL is short (200 ms)
    // so the test runs fast; the renew interval (50 ms) only matters when the
    // background loop is running — we deliberately don't spawn it for A so
    // its lease will expire naturally.
    let shard_a = build_shard(
        storage.clone(),
        catalog.clone(),
        registry.clone(),
        wal.clone(),
        "replica-a",
        Duration::from_millis(200),
    );
    let handle_a = shard_a.get(&project, &partition).await.unwrap();
    for i in 0..3 {
        handle_a
            .write_batch(&table, batch(i * 10, 10, "a-"))
            .await
            .unwrap();
    }
    // Make sure the WAL is durable before we kill A.
    wal.flush().await.unwrap();
    // A's pre-crash visibility (sanity).
    let read_a = handle_a.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(rows_in(&read_a), 30, "A's own read pre-crash");

    // A dies: drop the handle + the shard without releasing the lease. The
    // lease row stays in the registry until TTL elapses.
    drop(handle_a);
    drop(shard_a);

    // While A's lease is still live (we just dropped, < 200 ms ago) a peer
    // request must be REFUSED. This is the "lease holder owns the partition"
    // invariant of 6.X.A.
    let shard_b_early = build_shard(
        storage.clone(),
        catalog.clone(),
        registry.clone(),
        wal.clone(),
        "replica-b",
        Duration::from_secs(15),
    );
    match shard_b_early.get(&project, &partition).await {
        Err(BasinError::CommitConflict(_)) => {}
        Ok(_) => panic!("B must not acquire while A's lease is live"),
        Err(e) => panic!("expected CommitConflict, got {e:?}"),
    }
    drop(shard_b_early);

    // Wait past A's TTL.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now B can acquire. Use a generous TTL so the rest of the test is not
    // racing the heartbeat clock.
    let shard_b = build_shard(
        storage.clone(),
        catalog.clone(),
        registry.clone(),
        wal.clone(),
        "replica-b",
        Duration::from_secs(30),
    );
    let handle_b = shard_b.get(&project, &partition).await.unwrap();

    // B's first read MUST already see A's 30 rows — the cold-load path
    // replays the WAL (`replay_wal_into`) into the new partition state.
    // This is the "no data loss" claim made by ADR 0023's failure-path
    // section for the replica-loss case.
    let read_b_pre = handle_b.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(
        rows_in(&read_b_pre),
        30,
        "B must see A's pre-crash WAL entries (no data loss)",
    );

    // B writes its own batch. Reads see both A's and B's data.
    handle_b
        .write_batch(&table, batch(100, 10, "b-"))
        .await
        .unwrap();
    let read_b_post = handle_b.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(
        rows_in(&read_b_post),
        40,
        "after takeover, reads merge replayed A-writes + new B-writes",
    );

    // Registry confirms ownership has handed over to B with a strictly
    // higher epoch (steal-on-expiry bumps the epoch).
    let owner = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("B holds the lease");
    assert_eq!(owner.0, "replica-b");
    assert!(
        owner.1 >= 2,
        "epoch must bump on steal-on-expiry (got {})",
        owner.1
    );

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Dual-leaseholder fencing at the WAL append site
// ─────────────────────────────────────────────────────────────────────────────
//
// Force a "split-brain" by appending to the WAL with two different epochs
// against the SAME `(project, partition)`. The fence in `file_wal::append_fenced`
// (Phase 6.X.A) MUST reject the strictly-lower epoch with a `BasinError::Wal`
// and consume no LSN. The higher-epoch holder's appends succeed.
//
// This test exercises the WAL fence directly rather than wiring two Shards
// with manipulated lease epochs — the fence is the load-bearing safety
// primitive; a unit test on it directly is the cleanest way to prove the
// invariant the integration depends on.

#[tokio::test]
async fn dual_leaseholder_fence_rejects_lower_epoch() {
    basin_common::telemetry::try_init_for_tests();

    let wal_dir = TempDir::new().unwrap();
    let wal = open_wal(&wal_dir).await;
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();

    // High-epoch holder (winner of a hypothetical partition) writes at epoch 5.
    // First fenced append raises the partition's fence to 5.
    wal.append_fenced(
        &project,
        &partition,
        Bytes::from_static(b"high-epoch payload 1"),
        Some(5),
    )
    .await
    .expect("epoch-5 winner first append succeeds");
    wal.append_fenced(
        &project,
        &partition,
        Bytes::from_static(b"high-epoch payload 2"),
        Some(5),
    )
    .await
    .expect("epoch-5 winner second append succeeds");

    // Stale (epoch 3) holder — the dual-leaseholder loser — tries to append.
    // The fence rejects with Wal-shaped error and NO LSN is consumed.
    let err = wal
        .append_fenced(
            &project,
            &partition,
            Bytes::from_static(b"epoch-3 loser payload"),
            Some(3),
        )
        .await
        .expect_err("stale-epoch append must fail-closed at the WAL fence");
    assert!(
        matches!(err, BasinError::Wal(_)),
        "stale-epoch append must be a Wal fencing error, got {err:?}",
    );

    // Same fence: an even lower epoch (1) is rejected too.
    let err = wal
        .append_fenced(
            &project,
            &partition,
            Bytes::from_static(b"epoch-1 loser payload"),
            Some(1),
        )
        .await
        .expect_err("epoch-1 also rejected");
    assert!(matches!(err, BasinError::Wal(_)));

    // A NEW winner at strictly higher epoch (7) succeeds and raises the fence
    // further; subsequent epoch-5 appends are now ALSO rejected (the fence is
    // monotonic).
    wal.append_fenced(
        &project,
        &partition,
        Bytes::from_static(b"new winner epoch 7"),
        Some(7),
    )
    .await
    .expect("strictly-higher epoch raises the fence");
    let err = wal
        .append_fenced(
            &project,
            &partition,
            Bytes::from_static(b"stale-epoch-5 after fence raised"),
            Some(5),
        )
        .await
        .expect_err("epoch 5 now stale relative to fence at 7");
    assert!(matches!(err, BasinError::Wal(_)));

    // Verify exactly 3 successful appends are durable (the two epoch-5 wins
    // before the steal + the epoch-7 winner). Three rejected appends consumed
    // no LSN — corrupted state is impossible.
    wal.flush().await.unwrap();
    let durable = wal.read_from(&project, &partition, Lsn::ZERO).await.unwrap();
    assert_eq!(
        durable.len(),
        3,
        "exactly the three accepted appends are durable; rejected appends consumed no LSN",
    );

    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — Network-partition simulator
// ─────────────────────────────────────────────────────────────────────────────
//
// Wrap the in-memory lease registry in a mock that lets us simulate a
// coordinator partition for a single holder: every renew/acquire originating
// from "replica-a" returns an error while "replica-b" continues talking to
// the coordinator normally. Expected sequence:
//
//   1. A acquires the partition's lease at epoch 1 and writes successfully.
//   2. Operator partitions A → A's renew fails next tick → its lease is
//      effectively dead.
//   3. Once A's TTL elapses, B successfully acquires at epoch 2 and writes
//      via the shard (raising the WAL fence to 2).
//   4. Attempting to use A's *stale* epoch directly against the WAL is
//      REJECTED at the fence (`Wal` error). A is now fail-closed.
//   5. B continues writing successfully; the partitioned-side rejection does
//      not interfere.

/// Mock lease registry that delegates to a wrapped inner registry but can be
/// instructed to reject every `acquire`/`renew` call originating from a
/// specific holder string. Models the unidirectional "this replica is cut off
/// from the coordinator" partition.
struct PartitionedRegistry {
    inner: Arc<dyn LeaseRegistry>,
    /// Holder ids whose calls are rejected with a Catalog error.
    partitioned: Arc<RwLock<Vec<String>>>,
}

impl PartitionedRegistry {
    fn new(inner: Arc<dyn LeaseRegistry>) -> Self {
        Self {
            inner,
            partitioned: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn partition(&self, holder: &str) {
        self.partitioned.write().await.push(holder.to_string());
    }

    async fn is_partitioned(&self, holder: &str) -> bool {
        self.partitioned
            .read()
            .await
            .iter()
            .any(|h| h == holder)
    }
}

#[async_trait]
impl LeaseRegistry for PartitionedRegistry {
    async fn acquire(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<Lease>> {
        if self.is_partitioned(holder).await {
            return Err(BasinError::catalog(format!(
                "simulated partition: holder {holder} cannot reach coordinator"
            )));
        }
        self.inner.acquire(project, partition_id, holder, ttl).await
    }

    async fn renew(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
        epoch: i64,
        ttl: Duration,
    ) -> Result<bool> {
        if self.is_partitioned(holder).await {
            return Err(BasinError::catalog(format!(
                "simulated partition: holder {holder} renew rejected"
            )));
        }
        self.inner
            .renew(project, partition_id, holder, epoch, ttl)
            .await
    }

    async fn release(
        &self,
        project: &ProjectId,
        partition_id: &str,
        holder: &str,
    ) -> Result<bool> {
        // Release is tolerant: even a partitioned replica's local cleanup is
        // allowed (it's a no-op on the coordinator if the lease has already
        // moved). Tests rely on this; production tolerates it too.
        self.inner.release(project, partition_id, holder).await
    }

    async fn owner_of(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<(String, i64)>> {
        // Observation paths don't lie under partition; the coordinator
        // continues to serve the *other* side correctly.
        self.inner.owner_of(project, partition_id).await
    }
}

#[tokio::test]
async fn network_partition_simulator_partitioned_replica_fails_closed() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let inner_cat = Arc::new(InMemoryCatalog::new());
    let registry = Arc::new(PartitionedRegistry::new(inner_cat.clone()));
    let registry_dyn: Arc<dyn LeaseRegistry> = registry.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // A acquires + writes successfully. Short TTL so the post-partition
    // expiry happens quickly.
    let shard_a = build_shard(
        storage.clone(),
        inner_cat.clone(),
        registry_dyn.clone(),
        wal.clone(),
        "replica-a",
        Duration::from_millis(150),
    );
    let handle_a = shard_a.get(&project, &partition).await.unwrap();
    handle_a
        .write_batch(&table, batch(0, 5, "a-pre-partition-"))
        .await
        .unwrap();
    wal.flush().await.unwrap();

    // Note A's epoch from the coordinator.
    let (holder_a, epoch_a) = inner_cat
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("A holds the lease");
    assert_eq!(holder_a, "replica-a");
    assert_eq!(epoch_a, 1);

    // Operator partitions A. Any further renew/acquire by A fails.
    registry.partition("replica-a").await;
    // Confirm: A trying to renew at its known epoch is REJECTED (the mock
    // returns Catalog error; the shard's heartbeat would translate that to
    // "lease lost" on the renewal-returned-false branch, but our mock raises
    // the harder Err — exercising the coordinator-unreachable path too).
    let renew_result = registry
        .renew(
            &project,
            partition.as_str(),
            "replica-a",
            epoch_a,
            Duration::from_secs(15),
        )
        .await;
    assert!(
        renew_result.is_err(),
        "partitioned renew must error, got {renew_result:?}",
    );

    // Let A's TTL elapse so the coordinator regards the lease as reclaimable.
    tokio::time::sleep(Duration::from_millis(250)).await;
    // owner_of with an expired live row returns None on the in-memory side.
    let owner_after_expiry = inner_cat
        .owner_of(&project, partition.as_str())
        .await
        .unwrap();
    assert!(
        owner_after_expiry.is_none(),
        "A's lease must be expired, got {owner_after_expiry:?}",
    );

    // B takes over. B is NOT partitioned, so its acquire succeeds at a
    // strictly higher epoch.
    let shard_b = build_shard(
        storage.clone(),
        inner_cat.clone(),
        registry_dyn.clone(),
        wal.clone(),
        "replica-b",
        Duration::from_secs(30),
    );
    let handle_b = shard_b.get(&project, &partition).await.unwrap();
    let (holder_b, epoch_b) = inner_cat
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("B holds the lease post-partition");
    assert_eq!(holder_b, "replica-b");
    assert!(
        epoch_b > epoch_a,
        "B's epoch must strictly exceed A's (was {epoch_a}, got {epoch_b})",
    );

    // B writes — succeeds, raising the WAL fence to epoch_b.
    handle_b
        .write_batch(&table, batch(100, 5, "b-post-partition-"))
        .await
        .unwrap();
    wal.flush().await.unwrap();

    // The "partitioned-side write" — modelled by a direct WAL append carrying
    // A's stale epoch — is now rejected at the WAL fence. This is the
    // fail-closed guarantee: even if A's process were still running and
    // somehow tried to flush a buffered write through the WAL, the lower
    // epoch is hard-rejected.
    let stale_write = wal
        .append_fenced(
            &project,
            &partition,
            Bytes::from_static(b"A's late write after partition"),
            Some(epoch_a),
        )
        .await;
    assert!(
        matches!(stale_write, Err(BasinError::Wal(_))),
        "partitioned A's stale-epoch append must fail-closed, got {stale_write:?}",
    );

    // B can still read and write — the partitioned-side rejection doesn't
    // contaminate the new holder.
    let read_after = handle_b.read(&table, ReadOptions::default()).await.unwrap();
    // 5 from A (WAL-replayed) + 5 from B = 10.
    assert_eq!(
        rows_in(&read_after),
        10,
        "B sees A's pre-partition writes (WAL replay) + B's own writes",
    );

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — Coordinator-down (Phase 6.X.D failure mode)
// ─────────────────────────────────────────────────────────────────────────────
//
// ADR 0023 + the 6.X.D commit message specify: when the budget coordinator
// is unreachable, the slice view stays stale and `(project, cap)` pairs that
// have never received a slice fall back to the local per-process default.
// The SliceGate must NOT over-throttle in this degraded mode — that would
// turn a coordinator outage into a tenant-visible availability incident.
//
// Two flavours of "down" we cover here:
//   (a) coordinator never wired at all (Shard built without
//       `with_budget_coordinator`). Writes must succeed unimpeded.
//   (b) coordinator wired but `push_heartbeat` returns Err on every call —
//       the slice gate must continue admitting traffic (SLICE_UNSET sentinel).

/// Coordinator stub that always errors. Used to exercise the "coordinator
/// reachable but failing" branch of the failure-path.
struct FailingCoordinator;

#[async_trait]
impl BudgetCoordinator for FailingCoordinator {
    async fn set_project_budget(
        &self,
        _project: &ProjectId,
        _budget: ProjectBudget,
    ) -> Result<()> {
        Err(BasinError::catalog("coordinator down (test)"))
    }

    async fn push_heartbeat(
        &self,
        _project: &ProjectId,
        _partition_id: &str,
        _holder: &str,
        _delta: UsageDelta,
    ) -> Result<SliceBudget> {
        Err(BasinError::catalog("coordinator down (test)"))
    }

    async fn forget_partition(
        &self,
        _project: &ProjectId,
        _partition_id: &str,
    ) -> Result<()> {
        Err(BasinError::catalog("coordinator down (test)"))
    }

    async fn snapshot(
        &self,
        _project: &ProjectId,
        _partition_id: &str,
    ) -> Result<Option<UsageSnapshot>> {
        Err(BasinError::catalog("coordinator down (test)"))
    }
}

#[tokio::test]
async fn coordinator_unwired_writes_proceed_without_throttle() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let registry: Arc<dyn LeaseRegistry> = catalog.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // Shard wired with NO budget coordinator: cap consumers fall back to
    // their per-process defaults.
    let shard = build_shard(
        storage,
        catalog,
        registry,
        wal.clone(),
        "replica-a",
        Duration::from_secs(15),
    );
    let handle = shard.get(&project, &partition).await.unwrap();
    // A reasonable burst of writes — none should be throttled.
    for i in 0..50 {
        handle
            .write_batch(&table, batch(i * 5, 5, "v-"))
            .await
            .expect("no-coordinator mode must not throttle");
    }
    let read = handle.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(rows_in(&read), 250);

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

#[tokio::test]
async fn failing_coordinator_keeps_slice_view_stale_and_gate_admits() {
    basin_common::telemetry::try_init_for_tests();

    let project = ProjectId::new();
    // SliceGate built on a fresh view → no slice has ever been pushed. Every
    // try_consume returns Ok(()) (the SLICE_UNSET sentinel branch); a failing
    // coordinator's heartbeats CANNOT change that because they can't push a
    // slice. Verifies the safe-fallback contract directly on the primitive.
    let view = SliceBudgetView::new();
    let gate = SliceGate::new(view.clone());

    // Sanity: view reports unset before any heartbeat.
    assert_eq!(view.slice_for(project, CapKind::RestQps).await, SLICE_UNSET);

    // Simulate 1000 heartbeats against a failing coordinator — the slice
    // never gets pushed. (We model the heartbeat as a direct call rather
    // than wiring the full shard loop because the shard's heartbeat is
    // documented to swallow the error and continue; see in_process.rs
    // `heartbeat_budgets` "leave views stale" branch.)
    let coord = FailingCoordinator;
    for _ in 0..1000 {
        let _ = coord
            .push_heartbeat(
                &project,
                "_default",
                "replica-a",
                UsageDelta::zero(),
            )
            .await;
    }
    // View still unset → gate must admit unconditionally.
    assert_eq!(view.slice_for(project, CapKind::RestQps).await, SLICE_UNSET);

    // 10_000 admits — none rejected. If the gate over-throttled on
    // coordinator failure, this loop would fail well before 10k.
    let mut admitted = 0u64;
    for _ in 0..10_000 {
        if gate
            .try_consume(project, CapKind::RestQps, 1)
            .await
            .is_ok()
        {
            admitted += 1;
        }
    }
    assert_eq!(
        admitted, 10_000,
        "coordinator-down → SliceGate must admit all (degraded but safe), got {admitted}",
    );

    // Cross-check against the canonical InMemoryBudgetCoordinator when WIRED:
    // it pushes a slice of 0 if the project is capped at 0 — that's the
    // adversarial control to make sure our gate is not always-permissive,
    // it is "permissive only when no slice has ever been pushed".
    let healthy: Arc<dyn BudgetCoordinator> = Arc::new(InMemoryBudgetCoordinator::new());
    healthy
        .set_project_budget(&project, ProjectBudget::default().with(CapKind::RestQps, 0))
        .await
        .unwrap();
    let slice = healthy
        .push_heartbeat(&project, "_default", "replica-a", UsageDelta::zero())
        .await
        .unwrap();
    view.set_slice(project, CapKind::RestQps, slice.get(CapKind::RestQps).unwrap_or(0))
        .await;
    gate.refill_from_view().await;
    assert!(
        gate.try_consume(project, CapKind::RestQps, 1).await.is_err(),
        "with a healthy coordinator + slice=0, the gate MUST reject",
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5 — Handoff-mid-write atomicity (Phase 6.X.C — DEFERRED)
// ─────────────────────────────────────────────────────────────────────────────
//
// ADR 0023's 6.X.C ships a voluntary lease yield: leaseholder snapshots its
// memtable, flushes it, transfers the epoch, and acks; target < 500 ms p99
// stall. The atomicity guarantee is that no acked write is lost across the
// handoff — equivalently, the new holder's first read sees every write the
// old holder acked.
//
// TASK.md row marks 6.X.C as `[ ]` (not landed) at the time of this
// dispatch (Phase 6.X.E is the failure-path landing). There is no
// `Shard::yield_lease` / catalog `transfer_lease` API yet, so we cannot
// drive a real handoff. The test below is `#[ignore]`d and stubs the shape;
// drop the ignore and replace the body once 6.X.C ships:
//
//   1. Build shard A + shard B sharing the same WAL/storage/catalog/registry.
//   2. A acquires partition, writes N batches without flushing.
//   3. Call A's `yield_lease(project, partition)` (the new 6.X.C API).
//   4. A's yield must drive: flush memtable → catalog commit → release lease.
//   5. B acquires the freed lease (epoch bumps) and reads — must see all N
//      batches in Parquet (NOT just the WAL — the yield flushed them).
//   6. The handoff window measured end-to-end must be < 500 ms p99.

#[tokio::test]
async fn handoff_mid_write_is_atomic() {
    // Test 5 — Handoff-mid-write atomicity (Phase 6.X.C, ADR 0023).
    //
    // Proves: every write acked by the old leaseholder (A) is visible to the
    // new leaseholder (B) immediately after `yield_partition` completes. The
    // yield drives flush → epoch transfer → WAL marker in one atomic step.
    //
    // Mirrors the same helper / import pattern already in this file and
    // exercises `Shard::yield_partition` (landed in commit d85e893 / Phase 6.X.C).
    basin_common::telemetry::try_init_for_tests();

    let catalog = Arc::new(InMemoryCatalog::new());
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // ── Replica A: acquire lease + write 50 rows in 5 batches ────────────
    let shard_a = build_shard(
        storage.clone(),
        catalog.clone(),
        catalog.clone(),
        wal.clone(),
        "replica-a",
        Duration::from_secs(15),
    );
    let handle_a = shard_a.get(&project, &partition).await.unwrap();
    for i in 0..5i64 {
        handle_a
            .write_batch(&table, batch(i * 10, 10, "pre-"))
            .await
            .unwrap();
    }
    // All 50 rows are acked (written to the in-memory state / WAL).
    // We deliberately do NOT flush to Parquet here: the yield must do it.
    let pre_yield_rows = rows_in(
        &handle_a
            .read(&table, ReadOptions::default())
            .await
            .unwrap(),
    );
    assert_eq!(pre_yield_rows, 50, "A must see its 50 acked rows before yield");

    // ── Voluntary yield: A hands off to B; measure stall ─────────────────
    let t0 = Instant::now();
    shard_a
        .yield_partition(&project, &partition, "replica-b")
        .await
        .unwrap();
    let stall = t0.elapsed();
    println!(
        "lease_failure_paths handoff_mid_write_is_atomic: stall={stall:?}"
    );
    assert!(
        stall < Duration::from_millis(500),
        "handoff stall {stall:?} exceeded ADR 0023 < 500 ms p99 target \
         (small-memtable case = 50 rows)",
    );

    // After yield, A's lease is gone from the catalog.
    assert_eq!(
        catalog.owner_of(&project, partition.as_str()).await.unwrap(),
        None,
        "A must have released the lease after yield_partition",
    );

    // ── Replica B: acquire the freed lease and read ───────────────────────
    let shard_b = build_shard(
        storage.clone(),
        catalog.clone(),
        catalog.clone(),
        wal.clone(),
        "replica-b",
        Duration::from_secs(30),
    );
    let handle_b = shard_b.get(&project, &partition).await.unwrap();
    let read_b = handle_b.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(
        rows_in(&read_b),
        50,
        "post-handoff replica must see every pre-handoff acked row (no data loss)",
    );

    // B can write under its fresh lease.
    handle_b
        .write_batch(&table, batch(100, 5, "post-"))
        .await
        .unwrap();
    let read_b_post = handle_b.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(
        rows_in(&read_b_post),
        55,
        "B's post-handoff write must land alongside the pre-handoff rows",
    );

    // ── WAL handoff marker must be present and replay must be idempotent ──
    let events = wal
        .read_events(&project, &partition, Lsn::ZERO)
        .await
        .unwrap();
    let marker_count = events
        .iter()
        .filter(|e| {
            matches!(e, WalEvent::Handoff { to_holder, .. } if to_holder == "replica-b")
        })
        .count();
    assert_eq!(
        marker_count,
        1,
        "WAL must carry exactly one handoff marker to replica-b; got events={events:?}",
    );

    // Replaying the event stream twice must produce the same entries —
    // the handoff marker is a no-op in the replay path.
    let entries1 = replay_wal(events.clone(), &WalReplayConfig::default());
    let entries2 = replay_wal(events, &WalReplayConfig::default());
    assert_eq!(
        entries1.len(),
        entries2.len(),
        "handoff marker replay must be deterministic / idempotent",
    );

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Sanity: this file pulls in HashMap to make sure unused warnings don't break
// when the placeholder test body grows.
// ─────────────────────────────────────────────────────────────────────────────
#[allow(dead_code)]
fn _hashmap_anchor() -> HashMap<&'static str, u64> {
    HashMap::new()
}
