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

// ── MULTI-NODE restart-recovery of forwarded WAL-durable rows (#28) ──────────

/// Object store that FAILS every PUT while `wedged` is true, delegating every
/// other op to an inner store. Models the cloud incident where compaction /
/// flush PUTs to the shared DATA object store were wedged: a write's segment
/// never reaches a committed catalog file, but the rows are already WAL-durable
/// (each node's WAL has its own healthy store). Flip `wedged` to false to model
/// the wedge clearing after a node restart. Mirrors the single-node
/// `WedgeablePutStore` in `basin-shard`'s in_process tests.
#[derive(Debug)]
struct WedgeablePutStore {
    inner: Arc<dyn ObjectStore>,
    wedged: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Display for WedgeablePutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WedgeablePutStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for WedgeablePutStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.wedged.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "WedgeablePutStore",
                source: "data store wedged (compaction PUTs failing)".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOpts,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        if self.wedged.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "WedgeablePutStore",
                source: "data store wedged (compaction PUTs failing)".into(),
            });
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Build a no-lease-mode engine wired for multi-node forwarding over a SHARED
/// cold store + SHARED catalog, with its OWN per-node WAL. Mirrors the live
/// `BASIN_LEASE_MODE=off` deployment: forwarding is by the deterministic
/// `PartitionRouter`, NOT the lease registry, so the router has peers but no
/// `LeaseRegistry` is attached (writes on the owner need no lease, and
/// `recover_all` recovers every WAL-known partition rather than only
/// lease-held ones). The WAL store + cold store are supplied by the caller so a
/// "restart" can reopen fresh handles over the SAME backing dirs.
async fn lease_off_engine_for(
    cold: Arc<dyn ObjectStore>,
    catalog: Arc<InMemoryCatalog>,
    wal_store: Arc<dyn ObjectStore>,
    replica_id: &str,
) -> Engine {
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: wal_store,
            root_prefix: None,
            // Real flush cadence so synchronous (durable=true) appends ack only
            // after a real segment PUT to the WAL store.
            flush_interval: std::time::Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: std::time::Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: cold.clone(),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    // NO lease registry: lease-off mode. `replica_id` still identifies the node
    // for the WAL/shard, but ownership is the router's deterministic hash.
    let mut cfg = basin_shard::ShardConfig::new(
        storage,
        catalog.clone() as Arc<dyn basin_catalog::Catalog>,
        wal,
    );
    cfg.replica_id = replica_id.to_string();
    let shard = basin_shard::Shard::new(cfg);
    let storage2 = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: cold,
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    Engine::new(EngineConfig {
        storage: storage2,
        catalog: catalog as Arc<dyn basin_catalog::Catalog>,
        shard: Some(shard),
    })
}

/// Wire the two engines for deterministic A/B forwarding: a two-peer router on
/// each, and A's forward transport delivers straight into the peers' engines
/// (faithful to the REST receive handler — see [`InProcDouble`]).
fn wire_forwarding(eng_a: &Engine, eng_b: &Engine) {
    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));
    let mut map = HashMap::new();
    map.insert(PEER_A.to_string(), eng_a.clone());
    map.insert(PEER_B.to_string(), eng_b.clone());
    eng_a.attach_partition_forward_client(Arc::new(InProcDouble { peers: map }));
}

/// Count rows in a single WAL payload. The WAL payload layout is
/// `u32 LE table-name length | table name | Arrow-IPC stream of the batch`
/// (see `basin-shard`'s `encode_payload`). We only need the row count, so we
/// skip the table-name header and sum `num_rows()` over the IPC stream.
fn wal_payload_rows(payload: &[u8]) -> usize {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    assert!(payload.len() >= 4, "WAL payload shorter than 4-byte header");
    let tlen = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let ipc = &payload[4 + tlen..];
    let reader = StreamReader::try_new(Cursor::new(ipc), None).expect("ipc reader");
    reader
        .map(|b| b.expect("ipc batch").num_rows())
        .sum()
}

/// `SELECT count(*)` through a session — the global committed-segment row count
/// (the in-RAM tail is NOT counted; only committed catalog segments are).
async fn select_count(sess: &crate::ProjectSession, table: &str) -> i64 {
    let res = sess
        .execute(&format!("SELECT count(*) AS n FROM {table}"))
        .await
        .unwrap();
    match res {
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
    }
}

/// REPRODUCTION of the live MULTI-NODE restart-recovery incident
/// (object-store catalog, `BASIN_LEASE_MODE=off`): a durable fan-out COPY at
/// node A forwards the B-owned partitions' batches to node B, which group-
/// commits them to ITS OWN local WAL before acking (durable-on-forward). The
/// shared DATA object store is wedged, so NO partition tail on either node ever
/// compacts to a committed segment — `count(*)` (committed-segment-only)
/// under-counts even though every row is WAL-durable on its owner.
///
/// Then BOTH nodes restart: drop both engines/shards, close both WALs, reopen
/// fresh engines/shards over the SAME per-node WAL dirs + the SAME shared cold
/// store + catalog. The wedge clears. The headline question the live incident
/// posed: does `recover_all()` on BOTH nodes drain every WAL-durable partition
/// (including the FORWARDED ones that physically live on node B's WAL) so the
/// global `count(*)` returns N again?
///
/// Pins, in order:
///   1. The forwarded rows physically live on node B's reopened WAL (not lost).
///   2. WITHOUT recover_all, after reopen, count(*) < N (bug surface).
///   3. WITH recover_all on BOTH nodes, count(*) == N and a known id reads back
///      exactly once.
#[tokio::test]
async fn two_node_restart_recovers_forwarded_wal_durable_tail() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");
    basin_common::telemetry::try_init_for_tests();

    // The forwarded limb's durable-on-forward gate must be ON (process default),
    // or "forwarded rows are WAL-durable on B" would not hold.
    assert!(
        crate::write_forwarder::forward_lands_durable(),
        "default: forwarded writes land durable on the owner"
    );

    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    // SHARED cold store (wedgeable), persistent across the restart via a TempDir.
    let cold_dir = TempDir::new().unwrap();
    let wedged = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let cold_fs: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(cold_dir.path()).unwrap());
    let cold: Arc<dyn ObjectStore> = Arc::new(WedgeablePutStore {
        inner: cold_fs,
        wedged: wedged.clone(),
    });
    // SHARED catalog (the object-store catalog the peers both see). The Arc
    // outlives the shard drop, modelling the shared catalog surviving a restart.
    let catalog = Arc::new(InMemoryCatalog::new());

    // PER-NODE WAL dirs, persistent across the restart.
    let wal_dir_a = TempDir::new().unwrap();
    let wal_dir_b = TempDir::new().unwrap();
    let wal_store_a: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_a.path()).unwrap());
    let wal_store_b: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_b.path()).unwrap());

    let eng_a =
        lease_off_engine_for(cold.clone(), catalog.clone(), wal_store_a.clone(), PEER_A).await;
    let eng_b =
        lease_off_engine_for(cold.clone(), catalog.clone(), wal_store_b.clone(), PEER_B).await;
    wire_forwarding(&eng_a, &eng_b);

    // CREATE TABLE on A (catalog-level → visible to both via the shared catalog).
    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();
    let router = PartitionRouter::new(vec![PEER_A.to_string(), PEER_B.to_string()], PEER_A);

    // Durable fan-out COPY: 20k rows across 8 stripes, multi-row batches so the
    // fan-out path engages and the round-robin cursor visits every stripe —
    // some land locally on A, some forward to B. ids are 0..N-1, unique.
    let n: i64 = 20_000;
    let per: i64 = 200;
    let n_batches = (n / per) as usize; // 100 batches over 8 stripes
    let mut b_owned: Vec<PartitionKey> = Vec::new();
    let mut a_owned: Vec<PartitionKey> = Vec::new();
    for i in 0..n_batches {
        let base = (i as i64) * per;
        let ids: Vec<i64> = (0..per).map(|k| base + k).collect();
        crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids))
            .await
            .unwrap();
    }
    // Classify the 8 stripes by owner (for the cross-node landing assertion).
    for i in 0..8usize {
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        if router.desired_owner(&project, part.as_str()).is_self {
            a_owned.push(part);
        } else {
            b_owned.push(part);
        }
    }
    assert!(
        !b_owned.is_empty(),
        "router put no partitions on B — forwarding not exercised"
    );
    assert!(
        !a_owned.is_empty(),
        "router put no partitions on A — local limb not exercised"
    );

    // End-of-COPY durable barrier: drives every LOCALLY-touched (A-owned)
    // partition durable on A's WAL. The FORWARDED (B-owned) partitions are
    // already durable on B (durable-on-forward), so they never entered A's
    // local touched set — the barrier on A is sufficient.
    sess_a.await_copy_durable().await.unwrap();

    // The wedge means NOTHING compacted on either node: global count(*) < N.
    let committed_before = select_count(&sess_a, "t").await;
    assert!(
        committed_before < n,
        "precondition: wedged data store ⇒ count(*) must under-count \
         (committed {committed_before}, wrote {n})"
    );

    // === CRASH BOTH NODES: drop engines/shards and close BOTH WALs (flushes
    // buffered segments to the healthy WAL stores; the barrier + durable-on-
    // forward already made every row durable).
    let shard_a = eng_a.shard().unwrap();
    let shard_b = eng_b.shard().unwrap();
    shard_a.wal().close().await.unwrap();
    shard_b.wal().close().await.unwrap();
    drop(shard_a);
    drop(shard_b);
    drop(sess_a);
    drop(eng_a);
    drop(eng_b);

    // === RESTART: reopen fresh WAL stores over the SAME per-node dirs + the
    // SAME shared cold store + catalog.
    let wal_store_a2: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_a.path()).unwrap());
    let wal_store_b2: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_b.path()).unwrap());

    // (1) DEFINITIVE recoverability check: every acked row is still in SOME
    //     node's reopened WAL. Read each stripe from BOTH nodes' WALs (a stripe
    //     physically lives on exactly its owner's WAL); the union must be N, and
    //     node B's WAL must hold the forwarded rows.
    let probe_wal_a: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: wal_store_a2.clone(),
            root_prefix: None,
            flush_interval: std::time::Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: std::time::Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let probe_wal_b: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: wal_store_b2.clone(),
            root_prefix: None,
            flush_interval: std::time::Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: std::time::Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let mut wal_rows = 0usize;
    let mut b_wal_rows = 0usize;
    for i in 0..8usize {
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        for (wal, is_b) in [(&probe_wal_a, false), (&probe_wal_b, true)] {
            let entries = wal
                .read_from(&project, &part, basin_wal::Lsn::ZERO)
                .await
                .unwrap();
            for e in &entries {
                let r = wal_payload_rows(&e.payload);
                wal_rows += r;
                if is_b {
                    b_wal_rows += r;
                }
            }
        }
    }
    assert_eq!(
        wal_rows, n as usize,
        "RECOVERABILITY: every acked row must still be in SOME node's reopened \
         WAL (found {wal_rows}, expected {n}) — the rows are NOT lost"
    );
    assert!(
        b_wal_rows > 0,
        "FORWARDED rows must physically live on node B's WAL (found {b_wal_rows})"
    );
    probe_wal_a.close().await.unwrap();
    probe_wal_b.close().await.unwrap();
    drop(probe_wal_a);
    drop(probe_wal_b);

    // Reopen the two engines for real over the same dirs.
    let wal_store_a3: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_a.path()).unwrap());
    let wal_store_b3: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wal_dir_b.path()).unwrap());
    let eng_a2 = lease_off_engine_for(cold.clone(), catalog.clone(), wal_store_a3, PEER_A).await;
    let eng_b2 = lease_off_engine_for(cold.clone(), catalog.clone(), wal_store_b3, PEER_B).await;
    wire_forwarding(&eng_a2, &eng_b2);

    // Un-wedge: the incident's wedge has cleared, so compaction PUTs succeed.
    wedged.store(false, std::sync::atomic::Ordering::SeqCst);

    let sess_a2 = eng_a2.open_session(project).await.unwrap();

    // (2) BUG SURFACE: after reopen, with NO recover_all and no write, the
    //     resident map on each node is empty and count(*) is committed-only, so
    //     it still under-counts even though the wedge cleared.
    let after_reopen = select_count(&sess_a2, "t").await;
    assert!(
        after_reopen < n,
        "after reopen (no recover_all) count(*) must still under-count — \
         got {after_reopen}, wrote {n}"
    );

    // (3) THE FIX UNDER TEST: recover_all on BOTH nodes force-replays + drains
    //     every WAL-known partition each node owns (no-lease ⇒ all of its own).
    eng_a2.shard().unwrap().recover_all().await.unwrap();
    eng_b2.shard().unwrap().recover_all().await.unwrap();

    let after_recover = select_count(&sess_a2, "t").await;
    assert_eq!(
        after_recover, n,
        "FIX: recover_all on BOTH nodes must make global count(*) == N \
         (got {after_recover}, wrote {n}) — every WAL-durable row (incl. the \
         FORWARDED ones on node B) drained into committed segments"
    );

    // Point lookup of a known id (a high id in the last batch): after recovery
    // it must read back exactly once.
    let probe_id = n - 3;
    let res = sess_a2
        .execute(&format!("SELECT count(*) AS n FROM t WHERE id = {probe_id}"))
        .await
        .unwrap();
    let hits = match res {
        crate::ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..b.num_rows()).map(|i| col.value(i)).collect::<Vec<_>>()
            })
            .next()
            .unwrap_or(0),
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        hits, 1,
        "point lookup of a recovered id ({probe_id}) must return exactly one row \
         (got {hits})"
    );

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}

// ── DEGRADED forwarding: a failed forward must FAIL the COPY (fail-closed) ────

/// A partition-forward transport double that ALWAYS fails for B-owned
/// partitions, writing NOTHING anywhere — models the live incident's degraded
/// forwarding (peer unreachable / POST rejected / owner storage wedged so the
/// owner's durable append errors). The error string is a GENERIC one that does
/// NOT match the lease-transient or transport-transient retry predicates, so it
/// must surface on the FIRST attempt. The headline question: does a forwarded
/// batch that never got a durable home anywhere FAIL the COPY, or is it silently
/// dropped / counted as success (= acked rows with no durable home = data loss)?
struct AlwaysFailForward {
    /// Count of forward attempts, so the test can assert the transport was
    /// actually consulted (the partition really did route off-node).
    attempts: std::sync::Mutex<usize>,
}

#[async_trait]
impl PartitionForwardClient for AlwaysFailForward {
    async fn forward_partition_write(
        &self,
        _peer_base_url: &str,
        _project: ProjectId,
        _table: &str,
        _partition_id: &str,
        _idem_key: &str,
        _batch: RecordBatch,
    ) -> Result<u64> {
        *self.attempts.lock().unwrap() += 1;
        // A hard, NON-transient failure: the owner is gone / refused and nothing
        // was written. Deliberately not one of the retry-eligible markers
        // (`is_lease_transient` / `is_forward_transport_transient`).
        Err(basin_common::BasinError::wal(
            "owner unreachable: forward dropped, rows have no durable home".to_string(),
        ))
    }
}

/// FAIL-CLOSED REPRODUCTION: with forwarding wired (multi-peer router + a
/// transport that drops every B-owned batch), a fan-out COPY that touches a
/// B-owned partition must return an ERROR — the rows that could not be confirmed
/// durable on their owner are NOT acked. If the COPY instead succeeds (acks
/// `COPY n`) while the forwarded rows were dropped, that is the silent
/// ack-without-durability hole behind the live 14.19M loss.
#[tokio::test]
async fn forward_drop_fails_the_copy_fail_closed() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));

    let transport = Arc::new(AlwaysFailForward {
        attempts: std::sync::Mutex::new(0),
    });
    eng_a.attach_partition_forward_client(transport.clone());

    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();
    let router = PartitionRouter::new(peers.clone(), PEER_A);

    // Drive multi-row batches across the 8 stripes. The FIRST batch that routes
    // to a B-owned partition MUST surface the forward error; capture it.
    let per_batch = 10i64;
    let mut saw_b_owned = false;
    let mut forward_errored = false;
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..per_batch).map(|k| base + k).collect();
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        let routes_to_b = !router.desired_owner(&project, part.as_str()).is_self;
        let res = crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids)).await;
        if routes_to_b {
            saw_b_owned = true;
            // FAIL-CLOSED: a dropped forward must propagate as a statement error.
            // A silent `Ok` here is the data-loss bug.
            assert!(
                res.is_err(),
                "FAIL-CLOSED VIOLATED: a B-owned batch whose forward was dropped \
                 returned Ok (rows acked with no durable home) — silent multi-node \
                 data loss reproduced for partition {}",
                part.as_str()
            );
            let msg = res.unwrap_err().to_string();
            assert!(
                msg.contains("no durable home") || msg.contains("unreachable"),
                "the surfaced error must be the forward failure, got: {msg}"
            );
            forward_errored = true;
        } else {
            // A-owned local batches still succeed.
            res.unwrap();
        }
    }

    assert!(
        saw_b_owned,
        "router put no partitions on B — forwarding not exercised (test proves nothing)"
    );
    assert!(
        forward_errored,
        "expected at least one forwarded batch to fail the COPY"
    );
    assert!(
        *transport.attempts.lock().unwrap() >= 1,
        "the forward transport must have been consulted for the B-owned batch"
    );

    // NONE of the dropped rows landed on B (the transport wrote nothing).
    let shard_b = eng_b.shard().unwrap();
    for i in 0..8usize {
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        if !router.desired_owner(&project, part.as_str()).is_self {
            assert_eq!(
                tail_rows(&shard_b, &project, &part, &table).await,
                0,
                "dropped forward must have written nothing on B for {}",
                part.as_str()
            );
        }
    }

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}

/// MISCONFIG / owner-unknown limb: a multi-peer router resolves a B-owned
/// partition off-node, but NO forward transport is installed (the
/// `BASIN_FORWARD_SECRET` / transport was never wired). The fan-out path must
/// FAIL the write rather than silently writing to the wrong node (which would
/// split-brain the lease) or silently dropping the rows. Pins the
/// `write_batch_fanout` guard at executor.rs ~455.
#[tokio::test]
async fn forward_off_node_without_transport_fails_closed() {
    let _env = FANOUT_ENV_LOCK.lock().unwrap();
    std::env::set_var("BASIN_SHARD_PARTITIONS_PER_TABLE", "8");

    let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = Arc::new(InMemoryCatalog::new());
    let eng_a = engine_for(cold.clone(), catalog.clone(), PEER_A).await;
    let eng_b = engine_for(cold.clone(), catalog.clone(), PEER_B).await;

    let peers = vec![PEER_A.to_string(), PEER_B.to_string()];
    eng_a.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_A));
    eng_b.attach_partition_router(PartitionRouter::new(peers.clone(), PEER_B));
    // Deliberately DO NOT attach a partition-forward client to A.

    let sess_a = eng_a.open_session(ProjectId::new()).await.unwrap();
    let project = sess_a.project();
    sess_a.execute("CREATE TABLE t (id BIGINT)").await.unwrap();
    let table = TableName::new("t").unwrap();
    let router = PartitionRouter::new(peers.clone(), PEER_A);

    let mut saw_b_owned = false;
    for i in 0..8usize {
        let base = (i as i64) * 100;
        let ids: Vec<i64> = (0..10).map(|k| base + k).collect();
        let part = if i == 0 {
            PartitionKey::default_key()
        } else {
            PartitionKey::new(format!("s{i}")).unwrap()
        };
        let routes_to_b = !router.desired_owner(&project, part.as_str()).is_self;
        let res = crate::executor::exec_ingest_batch(&sess_a, &table, ids_batch(&ids)).await;
        if routes_to_b {
            saw_b_owned = true;
            assert!(
                res.is_err(),
                "FAIL-CLOSED VIOLATED: off-node partition {} with no transport \
                 returned Ok — rows were silently dropped or mis-written",
                part.as_str()
            );
            let msg = res.unwrap_err().to_string();
            assert!(
                msg.contains("partition-forward transport") && msg.contains("BASIN_FORWARD_SECRET"),
                "error must name the missing transport, got: {msg}"
            );
        } else {
            res.unwrap();
        }
    }
    assert!(saw_b_owned, "router put no partitions on B — guard not exercised");

    std::env::remove_var("BASIN_SHARD_PARTITIONS_PER_TABLE");
}
