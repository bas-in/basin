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
        _idem_key: &str,
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
        // RAW write — identical to the receiver handler, including the Stage-3
        // durable-on-forward gate: with the barrier engaged (default) the owner
        // group-commits the batch to its WAL before acking, so a returned
        // forward implies owner-durability.
        let durable = crate::write_forwarder::forward_lands_durable();
        handle
            .write_batch_opts(&TableName::new(table.to_owned())?, batch, durable)
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
            _idem_key: &str,
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

// ── exactly-once forwarding: lost-ack retry reuses key & applies once ────────

/// A forward transport double that models the LOST-ACK failure: it writes the
/// batch into engine B (the owner) exactly like the real receiver, dedups on
/// the idempotency key (so a re-POST of the same key does NOT write twice), and
/// on the FIRST receipt of any key returns a transport error AFTER recording the
/// apply — simulating "the owner committed, then the HTTP ack was lost". The
/// sender's retry re-POSTs the SAME key; this time the double returns success.
/// It records every (idem_key) it saw so the test can assert the retry reused
/// the key and that exactly one physical apply happened.
struct LostAckDouble {
    target: Engine,
    /// idem_key → rows applied (dedup map, mirrors the real receiver window).
    applied: std::sync::Mutex<HashMap<String, u64>>,
    /// Every idem_key seen across all calls (incl. retries), in order.
    keys_seen: std::sync::Mutex<Vec<String>>,
    /// How many keys have we already failed-after-apply once (so the retry of
    /// that same key succeeds).
    failed_once: std::sync::Mutex<std::collections::HashSet<String>>,
}

#[async_trait]
impl PartitionForwardClient for LostAckDouble {
    async fn forward_partition_write(
        &self,
        _peer_base_url: &str,
        project: ProjectId,
        table: &str,
        partition_id: &str,
        idem_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.keys_seen.lock().unwrap().push(idem_key.to_string());
        let rows = batch.num_rows() as u64;

        // Dedup exactly like the receiver: an already-applied key SKIPS the
        // write and returns the original rowcount.
        if let Some(&n) = self.applied.lock().unwrap().get(idem_key) {
            return Ok(n);
        }

        // First sight of this key: physically write to B (the owner).
        let shard = self.target.shard().expect("target engine has a shard");
        let part = PartitionKey::new(partition_id)?;
        let handle = shard.get(&project, &part).await?;
        handle
            .write_batch_opts(&TableName::new(table.to_owned())?, batch, true)
            .await?;
        self.applied.lock().unwrap().insert(idem_key.to_string(), rows);

        // LOST ACK: the very first time we see a key we report a transport error
        // AFTER the write committed, forcing the sender to retry. The retry will
        // hit the dedup branch above and return the recorded rowcount.
        let mut failed = self.failed_once.lock().unwrap();
        if !failed.contains(idem_key) {
            failed.insert(idem_key.to_string());
            return Err(basin_common::BasinError::wal(
                "partition-write POST to \"http://owner\" failed: simulated lost ack".to_string(),
            ));
        }
        Ok(rows)
    }
}

/// End-to-end-ish: force ONE lost-ack retry on a forwarded batch and assert the
/// owner holds the batch's rows EXACTLY once (no dup), the retry reused the
/// SAME idempotency key, and the total rowcount across the table equals what was
/// sent (no dup, no loss).
#[tokio::test]
async fn lost_ack_retry_reuses_key_and_applies_exactly_once() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));

    let double = Arc::new(LostAckDouble {
        target: eng_b.clone(),
        applied: std::sync::Mutex::new(HashMap::new()),
        keys_seen: std::sync::Mutex::new(Vec::new()),
        failed_once: std::sync::Mutex::new(std::collections::HashSet::new()),
    });
    eng_a.attach_partition_forward_client(double.clone());

    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();
    let router = PartitionRouter::new(peers.clone(), PEER_A);

    // Drive batches until at least one lands on a B-owned partition (forwarded).
    let per_batch = 10i64;
    let mut all_ids: Vec<i64> = Vec::new();
    let mut forwarded_any = false;
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..per_batch).map(|k| base + k).collect();
        all_ids.extend_from_slice(&ids);
        crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids))
            .await
            .expect("ingest must succeed despite the lost-ack retry");
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        if !router.desired_owner(&project, part.as_str()).is_self {
            forwarded_any = true;
        }
    }
    assert!(forwarded_any, "router put no partitions on B — no forward exercised");

    // A retry happened (a key appears twice in keys_seen) and EVERY retry reused
    // the SAME key (each distinct key applied exactly once).
    let keys = double.keys_seen.lock().unwrap().clone();
    let applied = double.applied.lock().unwrap();
    // Each distinct forwarded batch was applied exactly once.
    for (k, &rows) in applied.iter() {
        let seen = keys.iter().filter(|x| *x == k).count();
        assert!(
            seen >= 2,
            "key {k} should have been seen at least twice (attempt + lost-ack retry)"
        );
        assert_eq!(rows, per_batch as u64, "each batch applies its full rowcount once");
    }

    // Owner B holds each forwarded partition's rows EXACTLY once (no dup from
    // the retry). Total across both nodes equals what we sent.
    let shard_a = eng_a.shard().unwrap();
    let shard_b = eng_b.shard().unwrap();
    let mut total = 0usize;
    for i in 0..8usize {
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        let owner_shard = if router.desired_owner(&project, part.as_str()).is_self {
            &shard_a
        } else {
            &shard_b
        };
        let n = tail_rows(owner_shard, &project, &part, &table).await;
        assert_eq!(
            n, per_batch as usize,
            "partition {} must hold its rows EXACTLY once (no dup from the lost-ack retry)",
            part.as_str()
        );
        total += n;
    }
    assert_eq!(total, all_ids.len(), "no dup, no loss across the cluster");

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}

/// STAGE 1: a single-node, multi-batch bulk COPY must populate the session's
/// `copy_touched` set with EVERY fan-out partition it wrote to, each carrying a
/// monotone (strictly increasing across that partition's batches) WAL LSN — the
/// per-partition last-LSN the end-of-COPY durable barrier awaits. Then
/// `await_copy_durable` must drive every touched partition durable and clear
/// the set.
#[tokio::test]
async fn copy_touched_set_covers_all_partitions_with_monotone_lsns() {
    let _guard = FANOUT_ENV_LOCK.lock().unwrap();
    // Pin a 4-way fan-out so the round-robin visits _default, s1, s2, s3.
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "4");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    // Local-only engine (no peer router): every fan-out write stays on-node, so
    // every partition surfaces a local LSN into copy_touched.
    let eng = engine_for(cold, catalog, "solo").await;
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT)").await.unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));

    // 8 batches of 10 rows each (>1 row → fan-out engages); round-robin over 4
    // partitions means each partition is written twice → its LSN must advance.
    let fanout = 4usize;
    let n_batches = 8usize;
    let per = 10usize;
    let mut prev_lsn_per_part: std::collections::HashMap<PartitionKey, basin_wal::Lsn> =
        std::collections::HashMap::new();

    for i in 0..n_batches {
        let base = (i * per) as i64;
        let rows: Vec<Vec<Option<String>>> = (0..per)
            .map(|k| vec![Some((base + k as i64).to_string())])
            .collect();
        sess.ingest_csv_batch("t", schema.clone(), None, rows)
            .await
            .unwrap();

        // After each batch, the just-written partition's recorded LSN must be
        // strictly greater than what it held before (monotone per partition).
        let part = if i % fanout == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{}", i % fanout)).unwrap()
        };
        let g = sess.copy_touched.lock().await;
        let cur = *g.get(&part).expect("touched set must include the written partition");
        if let Some(prev) = prev_lsn_per_part.get(&part) {
            assert!(
                cur > *prev,
                "partition {} LSN must advance across batches ({cur:?} > {prev:?})",
                part.as_str()
            );
        }
        prev_lsn_per_part.insert(part, cur);
        drop(g);
    }

    // Every one of the 4 fan-out partitions must be present in the touched set.
    {
        let g = sess.copy_touched.lock().await;
        assert_eq!(g.len(), fanout, "touched set must cover all {fanout} partitions");
        for i in 0..fanout {
            let part = if i == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("s{i}")).unwrap()
            };
            assert!(
                g.contains_key(&part),
                "touched set missing partition {}",
                part.as_str()
            );
        }
    }

    // The barrier drives every touched partition durable and clears the set.
    sess.await_copy_durable().await.unwrap();
    assert!(
        sess.copy_touched.lock().await.is_empty(),
        "await_copy_durable must drain the touched set"
    );

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}

/// A partition-forward transport double that mirrors the receive route's Stage-3
/// durable-on-forward contract AND records, per forwarded batch, the owner WAL
/// LSN it landed at — so a test can prove the batch is DURABLE on the owner the
/// moment the forward returns (Option A: durable-on-forward).
struct DurabilityProbingDouble {
    peers: HashMap<String, Engine>,
    /// (partition, lsn) the owner durably committed each forwarded batch at.
    landed: std::sync::Mutex<Vec<(PartitionKey, basin_wal::Lsn)>>,
}

#[async_trait]
impl PartitionForwardClient for DurabilityProbingDouble {
    async fn forward_partition_write(
        &self,
        peer_base_url: &str,
        project: ProjectId,
        table: &str,
        partition_id: &str,
        _idem_key: &str,
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
        // The receive route's exact gate: durable-on-forward when the barrier is
        // engaged. `write_batch_opts_lsn(..., true)` returns ONLY after the
        // owner's WAL has group-committed through this LSN, so the returned LSN
        // is durable on the owner by the time this method returns.
        let durable = crate::write_forwarder::forward_lands_durable();
        let lsn = handle
            .write_batch_opts_lsn(&TableName::new(table.to_owned())?, batch, durable)
            .await?;
        self.landed.lock().unwrap().push((part, lsn));
        Ok(rows)
    }
}

/// STAGE 3 (forwarded limb of the end-of-COPY durable barrier): a COPY batch
/// whose partition is owned by a REMOTE node must be DURABLE on that owner by
/// the time the forward returns — so when the originator later acks `COPY n`, a
/// crash of the OWNER node cannot lose the acked rows. We prove this by writing
/// a forwarded batch through the full engine A→B path and then asserting the
/// owner's WAL is ALREADY durable through the exact LSN it landed at (the
/// `await_durable` fast path resolves immediately — well within a tight
/// timeout). With the barrier OFF the contract relaxes to async, which this
/// test does not require; here we assert the default (barrier ON) guarantee.
#[tokio::test]
async fn forwarded_copy_batch_is_durable_on_owner_before_ack() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");
    // Barrier ON (default). Be explicit so the test is hermetic regardless of
    // ambient env; the flag is read once and cached, so this must match the
    // process default to stay deterministic across the suite.
    assert!(
        crate::write_forwarder::forward_lands_durable(),
        "default: forwarded writes land durable (BASIN_COPY_DURABLE_BARRIER on)"
    );

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));

    let mut map = HashMap::new();
    map.insert(PEER_A.to_string(), eng_a.clone());
    map.insert(PEER_B.to_string(), eng_b.clone());
    let double = Arc::new(DurabilityProbingDouble {
        peers: map,
        landed: std::sync::Mutex::new(Vec::new()),
    });
    eng_a.attach_partition_forward_client(double.clone());

    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();

    // Drive 8 multi-row batches so the cursor visits every stripe; some forward
    // to B. Each forward, per the double, records the owner WAL LSN it landed at.
    let router = PartitionRouter::new(peers.clone(), PEER_A);
    let mut b_owned: Vec<PartitionKey> = Vec::new();
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..10).map(|k| base + k).collect();
        crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids))
            .await
            .unwrap();
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        if !router.desired_owner(&project, part.as_str()).is_self {
            b_owned.push(part);
        }
    }
    assert!(
        !b_owned.is_empty(),
        "router put no partitions on B — cannot exercise forwarded durability"
    );

    // Every forwarded batch recorded a (partition, lsn) that landed on B. For
    // each, B's WAL must ALREADY be durable through that LSN: the forward only
    // returned after the owner's synchronous-commit append covered it, so
    // `await_durable` takes its fast path and resolves immediately. A tight
    // timeout makes "immediately durable" an assertion, not a hope — if the
    // owner had acked before durability, this LSN's segment would still be
    // buffered and the await would block past the budget.
    let shard_b = eng_b.shard().unwrap();
    let landed = double.landed.lock().unwrap().clone();
    assert!(!landed.is_empty(), "no batches were forwarded to B");
    for (part, lsn) in landed {
        let handle = shard_b.get(&project, &part).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(250), handle.await_durable(lsn))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "forwarded batch on B partition {} is NOT durable through {lsn:?} \
                     after the forward returned — durable-on-forward violated",
                    part.as_str()
                )
            })
            .unwrap();
    }

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}
