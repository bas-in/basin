//! Two-engine integration test for #28 transparent per-partition write
//! forwarding.
//!
//! Two in-process engines/shards (A and B) share ONE `object_store::memory`
//! cold store + ONE `InMemoryCatalog` (which doubles as the shared
//! `LeaseRegistry`). `BASIN_SHARD_PEERS` is modelled by attaching a two-peer
//! [`crate::partition_router::PartitionRouter`] to each engine so the default
//! stripe partitions split deterministically across A and B.
//!
//! The forward transport is an in-process double ([`InProcDouble`]) rather than
//! a loopback HTTP server: it does EXACTLY what the REST receive handler does —
//! `B.shard().get(project, partition).write_batch_opts(table, batch, durable=
//! true)` — so the "rows physically landed on owner B + the lease is held by B
//! + a cross-node scan sees all rows" assertions are real. (The HTTP framing
//! itself is covered by the router/client unit tests and the receiver's own
//! tests; the engine-level integration we need is owner-resolution → land on
//! owner → cross-node visibility, which the double exercises faithfully.)
//!
//! The headline assertions:
//! * rows whose desired owner is B physically landed on B (B's partition tail
//!   holds them, A's does not);
//! * a table scan from EITHER engine returns ALL rows (read fan-in via the
//!   shared catalog + cold object store);
//! * each partition's lease is held by exactly its desired owner (no
//!   split-brain);
//! * total rowcount is exact (no dup / no loss);
//! * back-compat: with NO peer list, fan-out stays fully local on A.

#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use basin_catalog::InMemoryCatalog;
use basin_common::{PartitionKey, ProjectId, Result, TableName};
use object_store::memory::InMemory;
use object_store::ObjectStore;

use crate::partition_router::PartitionRouter;
use crate::write_forwarder::PartitionForwardClient;
use crate::{Engine, EngineConfig};

const PEER_A: &str = "http://a:5434";
const PEER_B: &str = "http://b:5434";

/// Serializes the two tests in this module: both pin the process-global
/// `BASIN_SHARD_PARTITIONS_PER_TABLE` to a fixed fan-out so the cursor visits a
/// known set of partitions deterministically. Holding this lock for the whole
/// test body keeps the env pin stable across the run.
static FANOUT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Build a shard wired against the shared cold store + shared catalog/lease
/// registry, with its own (in-memory) WAL, identified by `replica_id`.
async fn shard_for(
    cold: Arc<dyn ObjectStore>,
    catalog: Arc<InMemoryCatalog>,
    replica_id: &str,
) -> basin_shard::Shard {
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: cold,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    // Per-node WAL on its own in-memory store (WAL streams are per-node; cold
    // storage + catalog are the shared substrate, per ADR 0023).
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(InMemory::new()),
            root_prefix: None,
            flush_interval: std::time::Duration::from_millis(20),
            flush_max_bytes: 1024 * 1024,
            commit_delay: std::time::Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
    let cfg = basin_shard::ShardConfig::new(
        storage,
        catalog as Arc<dyn basin_catalog::Catalog>,
        wal,
    )
    .with_lease_registry(registry, replica_id)
    // Required mode is the multi-node enforcement shape: the lease HOLDER is
    // the single writer, and a non-owner `shard.get` serves a read-only handle
    // (reads continue) instead of hard-erroring. This is what lets engine B
    // scan an A-owned partition (and vice versa) without a `CommitConflict`,
    // while still guaranteeing single-writer via the lease CAS on write.
    .with_lease_mode(basin_shard::LeaseMode::Required);
    basin_shard::Shard::new(cfg)
}

/// Build an engine over a shard, sharing the supplied cold store + catalog.
async fn engine_for(
    cold: Arc<dyn ObjectStore>,
    catalog: Arc<InMemoryCatalog>,
    replica_id: &str,
) -> Engine {
    let shard = shard_for(cold.clone(), catalog.clone(), replica_id).await;
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: cold,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage,
        catalog: catalog as Arc<dyn basin_catalog::Catalog>,
        shard: Some(shard),
    })
}

/// In-process partition-forward transport double. Mirrors the REST receive
/// handler precisely: it RESOLVES the target engine by peer base URL and lands
/// the batch as a RAW partition write on that engine's shard
/// (`shard.get(project, partition).write_batch_opts(table, batch, true)`),
/// which acquires the writer lease on the owner. No constraint re-check (they
/// ran on the sender) — exactly what the handler does.
struct InProcDouble {
    /// peer base url → that peer's engine.
    peers: HashMap<String, Engine>,
}

#[async_trait]
impl PartitionForwardClient for InProcDouble {
    async fn forward_partition_write(
        &self,
        peer_base_url: &str,
        project: ProjectId,
        table: &str,
        partition_id: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        let engine = self
            .peers
            .get(peer_base_url)
            .unwrap_or_else(|| panic!("no in-proc peer for {peer_base_url:?}"));
        let rows = batch.num_rows() as u64;
        let shard = engine.shard().expect("peer engine has a shard");
        let part = PartitionKey::new(partition_id)?;
        let handle = shard.get(&project, &part).await?;
        // RAW write, durable — identical to the receiver handler.
        handle
            .write_batch_opts(&TableName::new(table.to_owned())?, batch, true)
            .await?;
        Ok(rows)
    }
}

fn ids_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).unwrap()
}

/// Count the rows currently in a shard's in-RAM tail for `(project, part,
/// table)` — used to assert which node a forwarded batch physically landed on.
async fn tail_rows(
    shard: &basin_shard::Shard,
    project: &ProjectId,
    part: &PartitionKey,
    table: &TableName,
) -> usize {
    let handle = shard.get(project, part).await.unwrap();
    let batches = handle
        .read(table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Drive enough fan-out ingest batches at engine A that the round-robin cursor
/// visits every default stripe partition, so some land locally on A and some
/// forward to B. Returns the partition→owner map the test asserts against.
#[tokio::test]
async fn two_engine_partition_forward_lands_on_owner_and_scans_cross_node() {
    // Force fanout=8 so the cursor visits s0..s7 deterministically and the
    // forward seam is exercised regardless of the compiled-in default. The
    // lock (held for the whole body) serializes against the sibling test, which
    // also pins this env var, so neither flips the other's fan-out mid-run.
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());

    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    // Two-peer router on each engine, self = its own peer url. (Models
    // BASIN_SHARD_PEERS="http://a:5434,http://b:5434" with BASIN_REPLICA_ID set
    // to each node's own url.)
    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));

    // A's forward transport delivers straight into B's engine (and, defensively,
    // back to A — never used, A-owned partitions take the local path).
    let mut map = HashMap::new();
    map.insert(PEER_A.to_string(), eng_a.clone());
    map.insert(PEER_B.to_string(), eng_b.clone());
    eng_a.attach_partition_forward_client(Arc::new(InProcDouble { peers: map }));

    // Create the table on A (DDL is catalog-level → visible to both engines via
    // the shared catalog).
    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a
        .execute("CREATE TABLE t (id BIGINT)")
        .await
        .unwrap();

    let table = TableName::new("t").unwrap();
    let router = PartitionRouter::new(peers.clone(), PEER_A);

    // Drive 8 multi-row ingest batches: the per-(project,table) cursor advances
    // once per batch, so batch i lands in stripe partition `s{i}` (i==0 →
    // _default). Each batch carries 10 distinct ids so num_rows()>1 (the
    // fan-out path requires >1 row).
    let per_batch = 10i64;
    let mut all_ids: Vec<i64> = Vec::new();
    // Track expected per-partition owners + landing.
    let mut b_owned_partitions: Vec<PartitionKey> = Vec::new();
    let mut a_owned_partitions: Vec<PartitionKey> = Vec::new();
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..per_batch).map(|k| base + k).collect();
        all_ids.extend_from_slice(&ids);
        crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids))
            .await
            .unwrap();
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        if router.desired_owner(&project, part.as_str()).is_self {
            a_owned_partitions.push(part);
        } else {
            b_owned_partitions.push(part);
        }
    }

    // The router must split the 8 partitions across BOTH nodes, or this test
    // proves nothing about forwarding.
    assert!(
        !b_owned_partitions.is_empty(),
        "router put no partitions on B — cannot exercise forwarding"
    );
    assert!(
        !a_owned_partitions.is_empty(),
        "router put no partitions on A — local path not exercised"
    );

    let shard_a = eng_a.shard().unwrap();
    let shard_b = eng_b.shard().unwrap();

    // (a) Rows whose desired owner is B physically landed on B (B's tail holds
    //     them) and NOT on A.
    for part in &b_owned_partitions {
        let on_b = tail_rows(&shard_b, &project, part, &table).await;
        let on_a = tail_rows(&shard_a, &project, part, &table).await;
        assert_eq!(
            on_b, per_batch as usize,
            "B-owned partition {} must hold its {per_batch} rows on B",
            part.as_str()
        );
        assert_eq!(
            on_a, 0,
            "B-owned partition {} must NOT have landed on A",
            part.as_str()
        );
    }
    // A-owned partitions landed locally on A, not on B.
    for part in &a_owned_partitions {
        let on_a = tail_rows(&shard_a, &project, part, &table).await;
        let on_b = tail_rows(&shard_b, &project, part, &table).await;
        assert_eq!(
            on_a, per_batch as usize,
            "A-owned partition {} must hold its rows locally on A",
            part.as_str()
        );
        assert_eq!(
            on_b, 0,
            "A-owned partition {} must NOT have landed on B",
            part.as_str()
        );
    }

    // (c) Each partition's lease is held by exactly its desired owner.
    let registry: Arc<dyn basin_catalog::LeaseRegistry> = catalog.clone();
    for part in &b_owned_partitions {
        let owner = registry
            .owner_of(&project, part.as_str())
            .await
            .unwrap()
            .map(|(h, _)| h);
        assert_eq!(
            owner.as_deref(),
            Some(PEER_B),
            "B-owned partition {} lease must be held by B",
            part.as_str()
        );
    }
    for part in &a_owned_partitions {
        let owner = registry
            .owner_of(&project, part.as_str())
            .await
            .unwrap()
            .map(|(h, _)| h);
        assert_eq!(
            owner.as_deref(),
            Some(PEER_A),
            "A-owned partition {} lease must be held by A",
            part.as_str()
        );
    }

    // (d) Total rowcount exact across both nodes' tails (no dup, no loss).
    let mut total = 0usize;
    for part in a_owned_partitions.iter().chain(b_owned_partitions.iter()) {
        let owner_shard = if router.desired_owner(&project, part.as_str()).is_self {
            &shard_a
        } else {
            &shard_b
        };
        total += tail_rows(owner_shard, &project, part, &table).await;
    }
    assert_eq!(total, all_ids.len(), "total rows must equal what we ingested");

    // (b) Cross-node scan: flush BOTH shards' tails to the shared cold store,
    //     then a SELECT from EITHER engine returns ALL rows (read fan-in via
    //     the shared catalog + cold object store).
    shard_a.flush_to_parquet().await.unwrap();
    shard_b.flush_to_parquet().await.unwrap();

    let expect = all_ids.len() as i64;
    for (label, sess) in [
        ("A", &sess_a),
        ("B", &eng_b.open_session(project).await.unwrap()),
    ] {
        let res = sess.execute("SELECT count(*) AS n FROM t").await.unwrap();
        let n = match res {
            crate::ExecResult::Rows { batches, .. } => batches
                .iter()
                .flat_map(|b| {
                    let col = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (0..b.num_rows()).map(|i| col.value(i)).collect::<Vec<_>>()
                })
                .next()
                .unwrap_or(0),
            other => panic!("expected Rows, got {other:?}"),
        };
        assert_eq!(
            n, expect,
            "cross-node scan from engine {label} must see ALL {expect} rows"
        );
    }

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}

/// Back-compat: with NO peer list (local-only router, the default), fan-out
/// stays fully local on A — nothing is forwarded, every partition lands on A,
/// even though a forward transport is installed (it must never be consulted).
#[tokio::test]
async fn back_compat_no_peers_keeps_fanout_local() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    // Local-only router (empty peer list → is_local_only). Default engine state
    // is already local-only; we set it explicitly to be unambiguous.
    eng_a.attach_partition_router(PartitionRouter::parse("", PEER_A));

    // Install a forward transport that PANICS if ever called — proving the
    // local-only seam never forwards.
    struct NeverForward;
    #[async_trait]
    impl PartitionForwardClient for NeverForward {
        async fn forward_partition_write(
            &self,
            _peer: &str,
            _project: ProjectId,
            _table: &str,
            _partition_id: &str,
            _batch: RecordBatch,
        ) -> Result<u64> {
            panic!("local-only fan-out must NEVER forward");
        }
    }
    eng_a.attach_partition_forward_client(Arc::new(NeverForward));

    let sess = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess.project();
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();

    let mut total_ingested = 0usize;
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..10).map(|k| base + k).collect();
        total_ingested += ids.len();
        crate::executor::exec_ingest_batch(&sess, &table, ids_batch(&ids))
            .await
            .unwrap();
    }

    // Every partition landed locally on A; B's shard is empty.
    let shard_a = eng_a.shard().unwrap();
    let shard_b = eng_b.shard().unwrap();
    let mut on_a = 0usize;
    for i in 0..8usize {
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        on_a += tail_rows(&shard_a, &project, &part, &table).await;
        assert_eq!(
            tail_rows(&shard_b, &project, &part, &table).await,
            0,
            "local-only: nothing should land on B"
        );
    }
    assert_eq!(on_a, total_ingested, "local-only: all rows land on A");

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}
