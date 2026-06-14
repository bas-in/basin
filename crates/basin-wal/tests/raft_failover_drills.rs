//! Multi-node failover DRILLS for [`basin_wal::RaftWal`] (commit 7).
//!
//! These are real, un-`#[ignore]`d tests. Each spins up its OWN in-process
//! 3-node cluster (one shared [`SimCluster`] so the in-process `RaftNetwork`
//! dispatches RPCs by direct method call), drives a failure, and asserts the
//! cluster recovers with **zero acked-write loss**. They are gated only by the
//! runtime mode the test itself selects — no external services, no env, no
//! feature flags.
//!
//! Drill list (per the multi-node commit-7 prompt):
//!   1. `drill_kill_leader_elects_new_and_continues_writes`
//!   2. `drill_restart_killed_node_catches_up`
//!   3. `drill_partitioned_follower_rejected_cluster_healthy`
//!   4. `drill_non_leader_write_redirect_contract`
//!   5. zero-data-loss is asserted inside every drill via the `AckedSet`
//!      bookkeeping (the pattern from the wal_durability tests): every payload
//!      whose `append` returned `Ok` MUST be present after the drill.
//!
//! Storage is the disk-backed store: every node gets its own subdirectory
//! under one per-test `TempDir` returned to the test so it outlives the
//! cluster (concurrent tests / re-runs never see each other's files).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::{BasinError, PartitionKey, ProjectId};
use basin_wal::{ClusterRole, Lsn, RaftWal, RaftWalConfig, SimCluster, Wal};
use bytes::Bytes;
use openraft::BasicNode;
use tempfile::TempDir;

const ELECTION_MS: u32 = 300;
const HEARTBEAT_MS: u32 = 100;

/// Bound how many drills spin up a cluster at once. These are timing-sensitive
/// (real raft elections with a 300 ms timeout on disk-backed storage), and the
/// workspace shares one machine: letting all 8 multi-threaded drills race their
/// startup elections simultaneously starves the CPU and turns a tight election
/// window into a flake. A 2-permit gate keeps real parallelism (so the suite
/// stays fast) while preventing the all-at-once startup burst. This does NOT
/// soften any assertion — every drill still runs in full and asserts the same
/// zero-loss invariants; it only staggers when they start.
fn drill_gate() -> &'static tokio::sync::Semaphore {
    static GATE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    GATE.get_or_init(|| tokio::sync::Semaphore::new(2))
}

/// Acquire a drill slot. The returned permit must be held for the test's
/// duration (bind it to a `_gate` local).
async fn enter_drill() -> tokio::sync::SemaphorePermit<'static> {
    drill_gate().acquire().await.unwrap()
}

fn cfg(node_id: u64, cluster: Arc<SimCluster>, dir: &TempDir) -> RaftWalConfig {
    let mut cfg = RaftWalConfig::new(
        vec![],
        format!("node-{node_id}"),
        dir.path().join(format!("node-{node_id}")),
    )
    .with_node_id(node_id)
    .with_cluster(cluster);
    cfg.election_timeout_ms = ELECTION_MS;
    cfg.heartbeat_interval_ms = HEARTBEAT_MS;
    cfg
}

/// Build a cluster of `n` nodes, bootstrap node 1 as leader, add the rest as
/// learners, then promote to voters. The returned `TempDir` holds every
/// node's raft storage and must stay alive for the cluster's lifetime.
async fn spin_up_cluster(n: u64) -> (Arc<SimCluster>, Vec<Arc<RaftWal>>, TempDir) {
    let dir = TempDir::new().unwrap();
    let cluster = SimCluster::new();
    let mut nodes = Vec::new();
    for id in 1..=n {
        let wal = RaftWal::new(cfg(id, cluster.clone(), &dir)).await.unwrap();
        nodes.push(Arc::new(wal));
    }

    let mut initial = BTreeMap::new();
    initial.insert(1u64, BasicNode::new("node-1"));
    nodes[0].initialize(initial).await.unwrap();
    // Generous bootstrap-election bound: cluster setup is not the thing under
    // test, and when the whole suite runs in parallel the simultaneous startup
    // burst (8 clusters × 3 nodes electing at once) can push the first election
    // past a tight deadline on a busy box. The drills' own failure windows stay
    // tight; only this setup wait is relaxed.
    wait_for_leader_any(&nodes, Duration::from_secs(15)).await;

    if n > 1 {
        for id in 2..=n {
            nodes[0]
                .add_learner(id, BasicNode::new(format!("node-{id}")))
                .await
                .unwrap();
        }
        let voters: BTreeSet<u64> = (1..=n).collect();
        nodes[0].change_membership(voters).await.unwrap();
    }

    (cluster, nodes, dir)
}

/// Return the id of whichever live node currently reports a leader. Skips
/// nodes the caller has marked down.
async fn current_leader_id(nodes: &[Arc<RaftWal>], down: &[u64]) -> Option<u64> {
    for n in nodes {
        if down.contains(&n.node_id()) {
            continue;
        }
        if let Some(l) = n.current_leader().await {
            return Some(l);
        }
    }
    None
}

async fn wait_for_leader_any(nodes: &[Arc<RaftWal>], timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(l) = current_leader_id(nodes, &[]).await {
            return l;
        }
        if Instant::now() > deadline {
            panic!("no leader elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait for a NEW leader distinct from `old_leader`, ignoring `down` nodes.
async fn wait_for_new_leader(
    nodes: &[Arc<RaftWal>],
    old_leader: u64,
    down: &[u64],
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        for n in nodes {
            if down.contains(&n.node_id()) {
                continue;
            }
            if let Some(l) = n.current_leader().await {
                if l != old_leader && !down.contains(&l) {
                    return l;
                }
            }
        }
        if Instant::now() > deadline {
            panic!("no new leader (!= {old_leader}) elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn node_by_id(nodes: &[Arc<RaftWal>], id: u64) -> &Arc<RaftWal> {
    nodes
        .iter()
        .find(|n| n.node_id() == id)
        .expect("node id present")
}

async fn wait_for_high_water(
    wal: &dyn Wal,
    project: &ProjectId,
    partition: &PartitionKey,
    target: Lsn,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let hw = wal.high_water(project, partition).await.unwrap();
        if hw >= target {
            return;
        }
        if Instant::now() > deadline {
            panic!("high_water never reached {target}; last seen {hw}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Acked-write bookkeeping (zero-data-loss harness). Every payload whose
/// `append` returned `Ok` is recorded here; after a drill we assert every one
/// is still present in the surviving cluster's log, byte-for-byte.
#[derive(Default)]
struct AckedSet {
    payloads: Vec<Vec<u8>>,
}

impl AckedSet {
    fn record(&mut self, payload: &[u8]) {
        self.payloads.push(payload.to_vec());
    }

    /// Assert every acked payload is present in `wal`'s log for the partition.
    /// Reads from `Lsn::ZERO` so a node that recovered via snapshot or full
    /// log replay is checked identically.
    async fn assert_all_present(
        &self,
        wal: &dyn Wal,
        project: &ProjectId,
        partition: &PartitionKey,
    ) {
        let entries = wal.read_from(project, partition, Lsn::ZERO).await.unwrap();
        let have: BTreeSet<Vec<u8>> = entries.iter().map(|e| e.payload.to_vec()).collect();
        for p in &self.payloads {
            assert!(
                have.contains(p),
                "acked write lost across drill: {:?} (have {} entries)",
                String::from_utf8_lossy(p),
                entries.len()
            );
        }
    }
}

/// Append through the current leader, retrying on a transient not-leader /
/// no-quorum window. Returns the Lsn on success; records the payload as acked.
async fn append_acked(
    nodes: &[Arc<RaftWal>],
    down: &[u64],
    project: &ProjectId,
    partition: &PartitionKey,
    payload: &[u8],
    acked: &mut AckedSet,
    timeout: Duration,
) -> Lsn {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader_id) = current_leader_id(nodes, down).await {
            if !down.contains(&leader_id) {
                let leader = node_by_id(nodes, leader_id);
                match leader
                    .append(project, partition, Bytes::copy_from_slice(payload))
                    .await
                {
                    Ok(lsn) => {
                        acked.record(payload);
                        return lsn;
                    }
                    Err(_) => { /* leadership moved; retry */ }
                }
            }
        }
        if Instant::now() > deadline {
            panic!(
                "append never acked within {timeout:?} for {:?}",
                String::from_utf8_lossy(payload)
            );
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

async fn close_all(nodes: &[Arc<RaftWal>]) {
    for n in nodes {
        let _ = n.close().await;
    }
}

// ---------------------------------------------------------------------------
// Drill 1 — kill leader → new leader elected → writes continue, none lost.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_kill_leader_elects_new_and_continues_writes() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;

    // Steady-state writes before the failure.
    for i in 1..=5u64 {
        append_acked(
            &nodes,
            &[],
            &project,
            &part,
            format!("pre-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(3),
        )
        .await;
    }

    // Kill the leader.
    cluster.take_down(leader_id).await;
    let new_leader_id = wait_for_new_leader(
        &nodes,
        leader_id,
        &[leader_id],
        Duration::from_millis((ELECTION_MS as u64) * 40),
    )
    .await;
    assert_ne!(new_leader_id, leader_id, "a different node must lead");

    // Writes continue on the new leader.
    for i in 1..=5u64 {
        append_acked(
            &nodes,
            &[leader_id],
            &project,
            &part,
            format!("post-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(5),
        )
        .await;
    }

    // Zero-data-loss: every acked write present on a surviving node.
    let survivor = node_by_id(&nodes, new_leader_id);
    acked.assert_all_present(survivor.as_ref(), &project, &part).await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 2 — restart the killed node → it catches up → write set identical.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_restart_killed_node_catches_up() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;

    for i in 1..=4u64 {
        append_acked(
            &nodes,
            &[],
            &project,
            &part,
            format!("a-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(3),
        )
        .await;
    }

    // Pick a follower to "kill" (network-isolate). Keep the leader live so the
    // cluster stays available (3 nodes, one down still has quorum).
    let victim = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();
    cluster.take_down(victim).await;

    // Writes proceed while the victim is gone — it will need to catch these up.
    for i in 1..=6u64 {
        append_acked(
            &nodes,
            &[victim],
            &project,
            &part,
            format!("b-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(5),
        )
        .await;
    }

    // Bring the victim back. It must catch up to the full acked set (via log
    // replication or snapshot install — both go through read_from from ZERO).
    cluster.bring_up(victim).await;
    let total = acked.payloads.len() as u64;
    wait_for_high_water(
        node_by_id(&nodes, victim).as_ref(),
        &project,
        &part,
        Lsn(total),
        Duration::from_secs(10),
    )
    .await;

    // The recovered node's write set is identical to the acked set.
    acked
        .assert_all_present(node_by_id(&nodes, victim).as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 3 — partition one follower: cluster healthy, partitioned node rejects
// writes (it cannot become leader / commit without quorum).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_partitioned_follower_rejected_cluster_healthy() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;
    let follower = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();

    // Partition the follower off. The remaining two (incl. leader) keep quorum.
    cluster.take_down(follower).await;

    // Cluster is healthy: the leader still commits writes.
    for i in 1..=5u64 {
        append_acked(
            &nodes,
            &[follower],
            &project,
            &part,
            format!("healthy-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(5),
        )
        .await;
    }

    // The partitioned follower must NOT accept a write: it is not the leader
    // (it can't reach a quorum to win an election), so its append fails with
    // the typed not-leader / no-quorum error and never returns an Lsn.
    let partitioned = node_by_id(&nodes, follower);
    let res = tokio::time::timeout(
        Duration::from_millis(800),
        partitioned.append(&project, &part, Bytes::from("from-partitioned")),
    )
    .await;
    match res {
        // Timed out waiting (no quorum) — acceptable rejection shape.
        Err(_) => {}
        // Returned an error before the deadline — must be a typed rejection,
        // never a silent success.
        Ok(Err(e)) => {
            assert!(
                matches!(e, BasinError::LeaseNotHeld(_) | BasinError::Wal(_)),
                "partitioned write rejected with unexpected error: {e:?}"
            );
        }
        Ok(Ok(lsn)) => panic!("partitioned follower accepted a write at {lsn} (data-loss risk)"),
    }

    // Heal the partition; the follower catches the full acked set up.
    cluster.bring_up(follower).await;
    let total = acked.payloads.len() as u64;
    wait_for_high_water(
        partitioned.as_ref(),
        &project,
        &part,
        Lsn(total),
        Duration::from_secs(10),
    )
    .await;
    acked
        .assert_all_present(partitioned.as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 4 — non-leader write contract: a follower's append is refused with the
// typed retryable not-leader error (carrying a leader hint).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_non_leader_write_redirect_contract() {
    let _gate = enter_drill().await;
    let (_cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;

    // Sanity: the leader's status reports the leader role + itself as leader.
    let leader = node_by_id(&nodes, leader_id);
    assert!(leader.is_leader().await, "leader must report is_leader");
    let lstatus = leader.cluster_status().await;
    assert_eq!(lstatus.role, ClusterRole::Leader);
    assert_eq!(lstatus.leader_id, Some(leader_id));

    // A follower must refuse the write with the typed not-leader error and a
    // leader hint pointing at the current leader.
    let follower_id = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();
    let follower = node_by_id(&nodes, follower_id);
    assert!(
        !follower.is_leader().await,
        "follower must NOT report is_leader"
    );

    let err = follower
        .append(&project, &part, Bytes::from("via-follower"))
        .await
        .expect_err("follower write must be refused");
    match err {
        BasinError::LeaseNotHeld(msg) => {
            assert!(
                msg.contains("not raft leader"),
                "not-leader error message shape: {msg}"
            );
            // Leader hint present (the message embeds the leader addr/id).
            assert!(
                msg.contains("leader hint") || msg.contains("leader unknown"),
                "not-leader error should carry a leader hint: {msg}"
            );
        }
        other => panic!("expected typed not-leader (LeaseNotHeld), got {other:?}"),
    }

    // And the leader still accepts the same write (redirect target works).
    let lsn = leader
        .append(&project, &part, Bytes::from("via-leader"))
        .await
        .expect("leader accepts the write");
    assert!(lsn >= Lsn(1));

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 5 — CHAOS UNDER LOAD: kill the leader WHILE a sustained write stream is
// in flight (the cluster is NOT quiesced). Assert: every write that was acked
// (returned Ok) survives, and the stream resumes on the new leader. This is the
// strongest zero-loss proof — the kill races concurrent appends, so the harness
// must distinguish "acked and lost" (a bug) from "in-flight and rejected"
// (correct — the client retries).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_kill_leader_under_sustained_write_load() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let acked = Arc::new(tokio::sync::Mutex::new(AckedSet::default()));

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;

    // Warm up: a few committed writes so the cluster is in steady state.
    {
        let mut a = acked.lock().await;
        for i in 1..=3u64 {
            append_acked(
                &nodes,
                &[],
                &project,
                &part,
                format!("warm-{i}").as_bytes(),
                &mut a,
                Duration::from_secs(3),
            )
            .await;
        }
    }

    // Spawn a sustained writer that streams appends through whoever leads,
    // skipping the soon-to-be-killed node. It runs for the whole drill, so the
    // leader is killed mid-stream — appends are genuinely in flight.
    let writer_nodes = nodes.clone();
    let writer_acked = acked.clone();
    let writer_project = project;
    let writer_part = part.clone();
    let writer = tokio::spawn(async move {
        let mut seq = 0u64;
        let stop = Instant::now() + Duration::from_secs(6);
        while Instant::now() < stop {
            seq += 1;
            let payload = format!("stream-{seq}");
            // One append attempt against the current leader (excluding the
            // killed node). On success record it as acked; on failure (leader
            // moved / no quorum window) just retry the next loop — that write
            // was NEVER acked, so its loss is not a violation.
            if let Some(lid) = current_leader_id(&writer_nodes, &[leader_id]).await {
                if lid != leader_id {
                    let leader = node_by_id(&writer_nodes, lid);
                    if leader
                        .append(
                            &writer_project,
                            &writer_part,
                            Bytes::copy_from_slice(payload.as_bytes()),
                        )
                        .await
                        .is_ok()
                    {
                        writer_acked.lock().await.record(payload.as_bytes());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    });

    // Mid-stream: kill the leader. The writer is hammering appends as this
    // happens, so the election races live traffic.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.take_down(leader_id).await;

    let new_leader_id = wait_for_new_leader(
        &nodes,
        leader_id,
        &[leader_id],
        Duration::from_millis((ELECTION_MS as u64) * 40),
    )
    .await;
    assert_ne!(new_leader_id, leader_id, "a different node must lead");

    // Let the writer keep streaming on the new leader, then stop it.
    writer.await.unwrap();

    // The stream must have RESUMED on the new leader: there are acked writes
    // tagged after the failover (the post-kill `stream-*` entries). We prove
    // resumption by requiring strictly more acked writes than the warm-up set.
    let acked = Arc::into_inner(acked)
        .expect("writer task dropped its acked handle")
        .into_inner();
    assert!(
        acked.payloads.len() > 3,
        "write stream did not resume on the new leader (only {} acks)",
        acked.payloads.len()
    );

    // Zero-loss: EVERY acked write — including those acked during the chaos
    // window — is present on a surviving node.
    let survivor = node_by_id(&nodes, new_leader_id);
    acked
        .assert_all_present(survivor.as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 6 — SLOW (lagging) FOLLOWER must not stall commits. Take one follower
// down for a while WHILE writes stream (it lags far behind), then confirm the
// 2-node quorum kept committing the whole time (commits never blocked on the
// laggard), and the laggard catches the full acked set up once healed.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_slow_follower_does_not_stall_commits() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;
    let laggard = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();

    // The laggard goes dark. The remaining two (incl. leader) are a quorum.
    cluster.take_down(laggard).await;

    // Stream a burst of writes while the laggard is down. Each must commit
    // promptly on the 2-node quorum — a stalled commit (waiting on the laggard)
    // would blow the per-append deadline and fail the test.
    let burst = 12u64;
    for i in 1..=burst {
        append_acked(
            &nodes,
            &[laggard],
            &project,
            &part,
            format!("burst-{i}").as_bytes(),
            &mut acked,
            // Tight per-append budget: comfortably more than a heartbeat, far
            // less than any "wait for the dead laggard" would take. Proves the
            // quorum commit did not block on the lagging replica.
            Duration::from_secs(2),
        )
        .await;
    }
    assert_eq!(acked.payloads.len() as u64, burst, "all burst writes acked");

    // Heal the laggard; it catches up the full acked set (log replication, or
    // snapshot install if it fell past the purge floor — both via read_from 0).
    cluster.bring_up(laggard).await;
    wait_for_high_water(
        node_by_id(&nodes, laggard).as_ref(),
        &project,
        &part,
        Lsn(burst),
        Duration::from_secs(10),
    )
    .await;
    acked
        .assert_all_present(node_by_id(&nodes, laggard).as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 7 — PARTITION + HEAL UNDER LOAD: partition a follower off mid-stream,
// keep writing through the majority, then heal it — all while a writer streams.
// Asserts: the majority never stalled (writes kept being acked through the
// partition) and the healed node converges to the full acked set.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_partition_and_heal_under_load() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;
    let follower = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();

    // Pre-partition writes.
    for i in 1..=4u64 {
        append_acked(
            &nodes,
            &[],
            &project,
            &part,
            format!("p0-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(3),
        )
        .await;
    }

    // Partition the follower; majority keeps committing under load.
    cluster.take_down(follower).await;
    for i in 1..=8u64 {
        append_acked(
            &nodes,
            &[follower],
            &project,
            &part,
            format!("p1-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(3),
        )
        .await;
    }

    // Heal; keep writing across the heal so the convergence target keeps moving.
    cluster.bring_up(follower).await;
    for i in 1..=6u64 {
        append_acked(
            &nodes,
            &[],
            &project,
            &part,
            format!("p2-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(5),
        )
        .await;
    }

    let total = acked.payloads.len() as u64;
    wait_for_high_water(
        node_by_id(&nodes, follower).as_ref(),
        &project,
        &part,
        Lsn(total),
        Duration::from_secs(10),
    )
    .await;
    acked
        .assert_all_present(node_by_id(&nodes, follower).as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}

// ---------------------------------------------------------------------------
// Drill 8 — SNAPSHOT-STREAMING follower catch-up. A follower misses the
// log-purge window: while it is down, the leader commits writes AND runs a
// manifest-anchored snapshot + log purge (`record_flush_watermark`) that drops
// the entries below the watermark from the raft log. When the follower rejoins,
// it can no longer be caught up by log replication (the entries are gone) — it
// MUST be caught up by `install_snapshot`. The recovered follower ends with the
// identical committed write set.
//
// NOTE ON THE ENGINE-INTEGRATION BOUNDARY: the raft snapshot the WAL builds is
// SELF-CONTAINED for the WAL's own purpose — it serialises the full applied
// state machine (every `(project, partition)` entry the cluster committed) plus
// the `ManifestPointer`. So a WAL follower rebuilds the entire committed write
// set from the snapshot alone, with no object-store fetch. The `catalog_snapshot_id`
// in the pointer is the seam the ENGINE layer (not the WAL) uses to rebuild
// *flushed table state* from S3/the catalog; that fetch lives above the WAL and
// is out of scope here. This drill proves the WAL-side contract end to end.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drill_snapshot_streaming_follower_catch_up() {
    let _gate = enter_drill().await;
    let (cluster, nodes, _dir) = spin_up_cluster(3).await;
    let project = ProjectId::new();
    let part = PartitionKey::default_key();
    let mut acked = AckedSet::default();

    let leader_id = wait_for_leader_any(&nodes, Duration::from_secs(2)).await;
    let leader = node_by_id(&nodes, leader_id);

    // A handful of writes while all 3 are up.
    for i in 1..=3u64 {
        append_acked(
            &nodes,
            &[],
            &project,
            &part,
            format!("s0-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(3),
        )
        .await;
    }

    // Take a follower down. It will miss everything that follows, including the
    // purge — so log replication alone cannot recover it.
    let victim = nodes
        .iter()
        .map(|n| n.node_id())
        .find(|id| *id != leader_id)
        .unwrap();
    cluster.take_down(victim).await;

    // Commit a larger batch on the surviving quorum so there is a real log
    // suffix to purge below.
    for i in 1..=20u64 {
        append_acked(
            &nodes,
            &[victim],
            &project,
            &part,
            format!("s1-{i}").as_bytes(),
            &mut acked,
            Duration::from_secs(5),
        )
        .await;
    }
    let total = acked.payloads.len() as u64;

    // Record a flush watermark THROUGH the current high-water mark and purge the
    // raft log below it. This is the engine/compactor's post-flush call: it
    // snapshots (capturing the full applied state) then drops the purged log
    // entries. After this the only way a far-behind follower catches up is an
    // install_snapshot.
    let hw = leader.high_water(&project, &part).await.unwrap();
    let purged = leader
        .record_flush_watermark(&project, &part, hw, "catalog-snap-1")
        .await
        .expect("record_flush_watermark");
    assert!(
        purged.is_some(),
        "expected the log to purge below the watermark (got {purged:?})"
    );

    // Bring the victim back. The leader's log no longer holds the early entries,
    // so openraft must stream the snapshot to it. It ends with the identical
    // committed write set.
    cluster.bring_up(victim).await;
    wait_for_high_water(
        node_by_id(&nodes, victim).as_ref(),
        &project,
        &part,
        Lsn(total),
        Duration::from_secs(15),
    )
    .await;
    acked
        .assert_all_present(node_by_id(&nodes, victim).as_ref(), &project, &part)
        .await;

    close_all(&nodes).await;
}
