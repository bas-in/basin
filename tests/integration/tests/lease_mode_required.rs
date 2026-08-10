//! Multi-node phase 1 (ADR 0023) — `BASIN_LEASE_MODE=required` enforcement.
//!
//! Phase 6.X.A shipped the lease primitive (catalog CAS rows, shard
//! heartbeat, WAL epoch fence) but production `basin-server` never wired it:
//! single-writer was deployment-shape, not enforced. This suite pins the
//! enforcement semantics of `LeaseMode::Required` (the shard side of the
//! `BASIN_LEASE_MODE` knob):
//!
//! 1. **Happy path** — a required-mode shard acquires the writer lease on
//!    first touch, the background heartbeat renews it across several TTL
//!    windows (epoch stays put — renew is not a regrant), and writes flow.
//! 2. **Fencing on lost lease** — when the heartbeat loses the lease (the
//!    coordinator stops answering this replica, then a second registry
//!    handle steals the row via CAS), in-flight handles and fresh handles
//!    both refuse writes with the typed `BasinError::LeaseNotHeld`; reads
//!    continue throughout; and once the lease is reclaimable again the
//!    replica recovers by re-acquiring at a higher epoch.
//! 3. **Writes refused with no lease** — required mode + a live foreign
//!    lease means `get` still serves a handle (reads continue) but every
//!    write is refused before it reaches the WAL (no LSN consumed).
//! 4. **Off mode unchanged** — no registry: nothing fences; registry
//!    attached but mode Off: the pre-existing Phase 6.X.A behaviour
//!    (`CommitConflict` from `get` when a peer holds the lease) is pinned
//!    byte-for-byte so existing deployments see zero change.
//!
//! Scaffolding mirrors `lease_failure_paths.rs` / `multi_instance_smoke.rs`:
//! in-process shards over a shared `InMemoryCatalog` (used as both `Catalog`
//! and `LeaseRegistry` — the same dual role `basin-server` wires in
//! production, where the Postgres catalog plays both parts).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{InMemoryCatalog, Lease, LeaseRegistry};
use basin_common::{BasinError, PartitionKey, ProjectId, Result, TableName};
use basin_shard::{LeaseMode, Shard, ShardConfig};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::sync::RwLock;

// ─────────────────────────────────────────────────────────────────────────────
// Shared test scaffolding (mirrors lease_failure_paths.rs)
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

async fn open_wal(dir: &TempDir) -> Arc<dyn Wal> {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(20),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
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

/// Build a **required-mode** shard: registry + holder id + `LeaseMode::
/// Required`, with a short TTL / renew cadence so loss-and-takeover plays
/// out within test budgets. This is exactly the wiring `basin-server`
/// performs under `BASIN_LEASE_MODE=required`.
fn build_required_shard(
    storage: Storage,
    catalog: Arc<dyn basin_catalog::Catalog>,
    registry: Arc<dyn LeaseRegistry>,
    wal: Arc<dyn Wal>,
    replica_id: &str,
    ttl: Duration,
) -> Shard {
    let mut cfg = ShardConfig::new(storage, catalog, wal)
        .with_lease_registry(registry, replica_id)
        .with_lease_mode(LeaseMode::Required);
    cfg.lease_ttl = ttl;
    cfg.lease_renew_interval = Duration::from_millis(50);
    Shard::new(cfg)
}

/// Lease registry wrapper that can be told to cut a specific holder off from
/// the coordinator (every `acquire`/`renew` from that holder errors). Same
/// shape as `lease_failure_paths.rs`'s `PartitionedRegistry`, plus a `heal`
/// to model the partition ending. Models how a required-mode replica
/// *loses* its lease: renew fails, the TTL lapses, a peer steals the row.
struct PartitionedRegistry {
    inner: Arc<dyn LeaseRegistry>,
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

    async fn heal(&self, holder: &str) {
        self.partitioned.write().await.retain(|h| h != holder);
    }

    async fn is_partitioned(&self, holder: &str) -> bool {
        self.partitioned.read().await.iter().any(|h| h == holder)
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
                "simulated partition: {holder} cannot reach the coordinator"
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
                "simulated partition: {holder} cannot reach the coordinator"
            )));
        }
        self.inner
            .renew(project, partition_id, holder, epoch, ttl)
            .await
    }

    async fn release(&self, project: &ProjectId, partition_id: &str, holder: &str) -> Result<bool> {
        // Release is tolerant: even a partitioned replica's local cleanup is
        // allowed to fire (it is a best-effort row drop).
        self.inner.release(project, partition_id, holder).await
    }

    async fn owner_of(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<(String, i64)>> {
        self.inner.owner_of(project, partition_id).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — happy path: acquire on first touch, heartbeat renews, writes flow
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn required_mode_acquire_and_renew_happy_path() {
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

    // TTL 200 ms, renew every 50 ms (set by the helper): without the
    // background heartbeat the lease would lapse twice over during the
    // sleep below.
    let shard = build_required_shard(
        storage,
        catalog.clone(),
        registry,
        wal.clone(),
        "replica-a",
        Duration::from_millis(200),
    );
    let bg = shard.spawn_background();

    // First touch acquires the writer lease and the write proceeds.
    let handle = shard.get(&project, &partition).await.unwrap();
    handle.write_batch(&table, batch(0, 5, "a-")).await.unwrap();
    let (holder, epoch) = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("lease granted on first touch");
    assert_eq!(holder, "replica-a");
    assert_eq!(epoch, 1, "first grant starts at epoch 1");

    // Outlive the TTL several times over. The heartbeat must keep the lease
    // alive — and renew does NOT bump the epoch (it is not a regrant).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (holder, epoch) = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("lease still live after 3+ TTL windows of renewals");
    assert_eq!(holder, "replica-a");
    assert_eq!(epoch, 1, "renewals must not bump the epoch");

    // Writes still flow, and the read path sees everything.
    handle
        .write_batch(&table, batch(100, 5, "a-late-"))
        .await
        .unwrap();
    let rows = handle.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(rows_in(&rows), 10);

    bg.shutdown().await;
    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — fencing on lost lease (CAS stolen by a second registry handle)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lost_lease_fences_writes_reads_continue_then_recovers() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let registry = Arc::new(PartitionedRegistry::new(catalog.clone()));
    let registry_dyn: Arc<dyn LeaseRegistry> = registry.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    let shard = build_required_shard(
        storage,
        catalog.clone(),
        registry_dyn,
        wal.clone(),
        "replica-a",
        Duration::from_millis(150),
    );
    let bg = shard.spawn_background();

    // Steady state: A holds the lease and writes.
    let handle = shard.get(&project, &partition).await.unwrap();
    handle
        .write_batch(&table, batch(0, 5, "a-pre-loss-"))
        .await
        .unwrap();
    wal.flush().await.unwrap();
    let epoch_a = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("A holds the lease")
        .1;

    // Cut A off from the coordinator. The next heartbeat renew errors,
    // which the shard treats as lease loss: it drops the held epoch and the
    // partition state. From that point the IN-FLIGHT handle must refuse
    // writes with the typed error — poll until the heartbeat has fired.
    registry.partition("replica-a").await;
    let deadline = Instant::now() + Duration::from_secs(3);
    let fencing_err = loop {
        match handle
            .write_batch(&table, batch(1000, 1, "a-mid-loss-"))
            .await
        {
            Err(e) => break e,
            Ok(()) => {
                assert!(
                    Instant::now() < deadline,
                    "write through the in-flight handle never fenced after lease loss",
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    };
    assert!(
        matches!(fencing_err, BasinError::LeaseNotHeld(_)),
        "lost-lease write must fail with the typed LeaseNotHeld, got {fencing_err:?}",
    );

    // Let the TTL lapse, then a SECOND registry handle (a peer replica
    // talking straight to the coordinator) steals the row via CAS.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let stolen = catalog
        .acquire(
            &project,
            partition.as_str(),
            "replica-b",
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .expect("steal-on-expiry succeeds for the second handle");
    assert!(
        stolen.epoch > epoch_a,
        "stolen lease must bump the epoch (A had {epoch_a}, B got {})",
        stolen.epoch
    );

    // A FRESH handle on the fenced replica: `get` still serves (reads
    // continue — the state is rebuilt from the WAL), writes stay refused.
    let fresh = shard.get(&project, &partition).await.unwrap();
    let read = fresh.read(&table, ReadOptions::default()).await.unwrap();
    assert!(
        rows_in(&read) >= 5,
        "reads continue on the fenced replica (WAL replay serves A's rows)",
    );
    let err = fresh
        .write_batch(&table, batch(2000, 1, "a-post-steal-"))
        .await
        .expect_err("write on the fenced replica must be refused");
    assert!(
        matches!(err, BasinError::LeaseNotHeld(_)),
        "foreign-held lease must surface as LeaseNotHeld, got {err:?}",
    );

    // Recovery: B releases, A's partition heals; the next touch re-acquires
    // at a fresh epoch and writes flow again. Fail-closed is not fail-forever.
    assert!(catalog
        .release(&project, partition.as_str(), "replica-b")
        .await
        .unwrap());
    registry.heal("replica-a").await;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let h = shard.get(&project, &partition).await.unwrap();
        match h.write_batch(&table, batch(3000, 5, "a-recovered-")).await {
            Ok(()) => break,
            Err(BasinError::LeaseNotHeld(_)) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => panic!("recovery write failed with unexpected error: {e:?}"),
        }
    }
    let (holder, _) = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("A re-acquired after recovery");
    assert_eq!(holder, "replica-a");

    bg.shutdown().await;
    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — writes refused when required and no lease (foreign holder)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn writes_refused_when_required_and_no_lease() {
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

    // A peer already holds a live lease (acquired through a second registry
    // handle, exactly how a second basin-server process would appear).
    let lease = catalog
        .acquire(
            &project,
            partition.as_str(),
            "replica-b",
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .expect("peer pre-acquires");
    assert_eq!(lease.epoch, 1);

    let shard = build_required_shard(
        storage,
        catalog.clone(),
        registry,
        wal.clone(),
        "replica-a",
        Duration::from_millis(200),
    );

    // `get` serves a handle (reads must continue)…
    let handle = shard.get(&project, &partition).await.unwrap();
    let read = handle.read(&table, ReadOptions::default()).await;
    assert!(read.is_ok(), "reads continue without the lease: {read:?}");

    // …but the write is refused with the typed error before any WAL append.
    let err = handle
        .write_batch(&table, batch(0, 3, "a-"))
        .await
        .expect_err("write without the lease must be refused");
    assert!(
        matches!(err, BasinError::LeaseNotHeld(_)),
        "expected LeaseNotHeld, got {err:?}",
    );
    // No LSN was consumed: the WAL has nothing for this partition.
    assert_eq!(
        wal.high_water(&project, &partition).await.unwrap(),
        basin_wal::Lsn::ZERO,
        "refused writes must not reach the WAL",
    );

    // The peer's lease is untouched by the refusals.
    let (holder, epoch) = catalog
        .owner_of(&project, partition.as_str())
        .await
        .unwrap()
        .expect("peer still holds");
    assert_eq!((holder.as_str(), epoch), ("replica-b", 1));

    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — off mode unchanged (both shapes)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn off_mode_without_registry_is_unchanged() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();
    let table = TableName::new("events").unwrap();

    // Plain ShardConfig: no registry, default LeaseMode::Off — the
    // single-replica deployment every existing test and demo runs.
    let shard = Shard::new(ShardConfig::new(storage, catalog.clone(), wal.clone()));
    let handle = shard.get(&project, &partition).await.unwrap();
    handle
        .write_batch(&table, batch(0, 4, "solo-"))
        .await
        .unwrap();
    let rows = handle.read(&table, ReadOptions::default()).await.unwrap();
    assert_eq!(rows_in(&rows), 4);

    // No lease row was ever created.
    assert_eq!(
        catalog
            .owner_of(&project, partition.as_str())
            .await
            .unwrap(),
        None,
        "no-lease mode must not touch the lease table",
    );

    wal.flush().await.unwrap();
    wal.close().await.unwrap();
}

#[tokio::test]
async fn off_mode_with_registry_pins_phase_6xa_behaviour() {
    basin_common::telemetry::try_init_for_tests();

    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = open_storage(&storage_dir);
    let wal = open_wal(&wal_dir).await;
    let catalog = Arc::new(InMemoryCatalog::new());
    let registry: Arc<dyn LeaseRegistry> = catalog.clone();
    let project = ProjectId::new();
    let partition = PartitionKey::default_key();

    // A peer holds a live lease.
    catalog
        .acquire(
            &project,
            partition.as_str(),
            "replica-b",
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .expect("peer pre-acquires");

    // Registry attached but lease_mode left at the default Off: the
    // pre-existing Phase 6.X.A contract is that `get` itself errors with
    // CommitConflict when a peer owns the partition. Pinned here so the
    // required-mode changes provably did not alter off-mode behaviour.
    let mut cfg = ShardConfig::new(storage, catalog.clone(), wal.clone())
        .with_lease_registry(registry, "replica-a");
    cfg.lease_ttl = Duration::from_millis(200);
    assert_eq!(cfg.lease_mode, LeaseMode::Off, "default mode must be Off");
    let shard = Shard::new(cfg);

    // `ProjectHandle` is not `Debug`, so we cannot use `expect_err` (which
    // would format the Ok value); match the Result explicitly instead.
    let err = match shard.get(&project, &partition).await {
        Ok(_) => panic!("off-mode get must error while a peer holds the lease"),
        Err(e) => e,
    };
    assert!(
        matches!(err, BasinError::CommitConflict(_)),
        "off mode keeps the CommitConflict shape, got {err:?}",
    );

    wal.close().await.unwrap();
}
