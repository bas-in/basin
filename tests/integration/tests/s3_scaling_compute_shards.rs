//! S3 port of `scaling_compute_shards.rs`.
//!
//! Same shape — N shard owners share one Storage + one in-memory catalog,
//! a front router fans project connections to them via stable hashing — but
//! the shared storage is real S3 instead of a LocalFS tempdir. Cluster
//! topology stays local (4 in-process shard listeners on ephemeral ports);
//! only the storage backend is remote.
//!
//! Card id: `compute_shards`. Bar: `max_shard_load_pct < 50`.
//!
//! Skips cleanly when `[s3]` is missing.
//!
//! ## Scale
//!
//! WAN latency to the S3 endpoint is ~500ms-1s per RPC. A 100-project /
//! 5-reconnect run like the LocalFS variant would take 5-10 minutes and
//! exhaust the test's budget. We scale to 25 projects × 1 connection each
//! (no reconnect pass) — enough samples to verify hash-uniformity (peak
//! load expected ~30%, well under the 50% bar) and consistent-routing
//! (every project lands on exactly one shard).

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;

use basin_common::ProjectId;
use basin_integration_tests::benchmark::{
    report_real_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::test_cluster::{start_n_shards_with_storage, Cluster};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

const TEST_NAME: &str = "s3_scaling_compute_shards";
const N_SHARDS: usize = 4;
const N_PROJECTS: usize = 25;
const BAR_MAX_SHARD_LOAD_PCT: f64 = 50.0;
const BAR_CONSISTENT_ROUTING_PCT: f64 = 100.0;

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user={user} password=ignored",
        addr.ip(),
        addr.port()
    );
    let user_owned = user.to_owned();
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user_owned}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            if !format!("{e}").contains("unexpected") {
                eprintln!("conn driver({user_owned}): {e}");
            }
        }
    });
    client
}

async fn shard_for_project(cluster: &Cluster, project: &ProjectId) -> Option<usize> {
    for (idx, shard) in cluster.shards.iter().enumerate() {
        let map = shard.stats.snapshot_per_project().await;
        if map.contains_key(project) {
            return Some(idx);
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live S3 / .basin-test.toml-gated; run with --ignored"]
async fn s3_scaling_compute_shards() {
    basin_common::telemetry::try_init_for_tests();

    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    // Lifetime-anchor tempdir for the cluster helper — empty, since storage
    // lives in S3 not on disk. The cluster keeps it alive for the duration
    // of the test; the bucket-side `CleanupOnDrop` handles real teardown.
    let anchor = TempDir::new().unwrap();
    let cluster = start_n_shards_with_storage(N_SHARDS, storage, anchor).await;
    let router_addr = cluster.router_addr();
    println!(
        "[S3 scaling_compute_shards] cluster: router {router_addr}, shards {:?}",
        cluster.shard_endpoints
    );

    let mut projects: Vec<(String, ProjectId)> = Vec::with_capacity(N_PROJECTS);
    for i in 0..N_PROJECTS {
        let user = format!("project_{i:03}");
        let t = ProjectId::new();
        cluster.resolver.insert(user.clone(), t).await;
        projects.push((user, t));
    }

    // First pass: every project runs CREATE / INSERT / SELECT through the
    // front router. With N_PROJECTS=25 and a 4-shard cluster the round-trip
    // cost per project is dominated by the S3 PUT for the INSERT (~500ms-1s
    // RTT on APAC R2). Total budget: ~30-60s.
    let mut placement: HashMap<ProjectId, usize> = HashMap::new();
    for (user, project) in &projects {
        let client = connect(router_addr, user).await;
        client
            .simple_query("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
            .await
            .unwrap_or_else(|e| panic!("CREATE for {user}: {e}"));
        client
            .simple_query("INSERT INTO events VALUES (1, 'hello')")
            .await
            .unwrap_or_else(|e| panic!("INSERT for {user}: {e}"));
        let res = client
            .simple_query("SELECT id, body FROM events")
            .await
            .unwrap_or_else(|e| panic!("SELECT for {user}: {e}"));
        let row_count = res
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count();
        assert!(
            row_count >= 1,
            "{user} expected >=1 row from SELECT, got {row_count}",
        );
        drop(client);

        let shard_idx = shard_for_project(&cluster, project)
            .await
            .unwrap_or_else(|| panic!("no shard saw project {project} after first round"));
        placement.insert(*project, shard_idx);
    }

    // Consistent routing: every project lands on exactly one shard. We don't
    // do a reconnect pass on real S3 (that would multiply the WAN budget by
    // RECONNECTS_PER_PROJECT for no extra signal). Stable hashing is a
    // function of (username, shard_count) — proven by the LocalFS variant —
    // so on real S3 we just verify each project has a single observed shard.
    let mut consistent_count: u64 = 0;
    for (_, project) in &projects {
        let mut hits = 0;
        for shard in cluster.shards.iter() {
            if shard
                .stats
                .snapshot_per_project()
                .await
                .contains_key(project)
            {
                hits += 1;
            }
        }
        if hits == 1 {
            consistent_count += 1;
        }
    }
    let consistent_routing_pct = 100.0 * consistent_count as f64 / N_PROJECTS as f64;

    let mut load_per_shard: Vec<usize> = vec![0; N_SHARDS];
    for idx in placement.values() {
        load_per_shard[*idx] += 1;
    }
    let max_load: usize = *load_per_shard.iter().max().unwrap();
    let max_shard_load_pct = 100.0 * max_load as f64 / N_PROJECTS as f64;

    println!(
        "[S3 scaling_compute_shards] projects={N_PROJECTS} shards={N_SHARDS} \
         load_per_shard={load_per_shard:?} \
         max_shard_load_pct={max_shard_load_pct:.1}% \
         consistent_routing_pct={consistent_routing_pct:.1}%",
    );

    let pass_consistency = consistent_routing_pct >= BAR_CONSISTENT_ROUTING_PCT;
    let pass_load = max_shard_load_pct < BAR_MAX_SHARD_LOAD_PCT;
    let passed = pass_consistency && pass_load;

    let json_rows: Vec<serde_json::Value> = load_per_shard
        .iter()
        .enumerate()
        .map(|(i, count)| {
            json!({
                "shard": i,
                "projects": *count,
                "load_pct": 100.0 * (*count as f64) / N_PROJECTS as f64,
            })
        })
        .collect();

    report_real_scaling(
        "compute_shards",
        "Compute sharding (router -> shard owners) on real S3",
        "Hash project_id -> shard_id; pgwire connections route to the owning shard. \
         Shards share one S3-backed Storage; load distributes evenly across shards. \
         Scaled to 25 projects on real S3 to fit the WAN test budget.",
        passed,
        AxisSpec {
            key: "shard".into(),
            label: "shard index".into(),
        },
        vec![
            SeriesSpec {
                key: "projects".into(),
                label: "projects on shard".into(),
                unit: Some("count".into()),
            },
            SeriesSpec {
                key: "load_pct".into(),
                label: "share of projects".into(),
                unit: Some("%".into()),
            },
        ],
        json_rows,
        Some(PrimaryMetric {
            label: "max shard load".into(),
            value: max_shard_load_pct,
            unit: "%".into(),
            bar: BarOp::lt(BAR_MAX_SHARD_LOAD_PCT),
        }),
    );

    // Drop cluster before the test ends so listeners shut down before
    // `_cleanup` walks the bucket.
    drop(cluster);

    assert!(
        pass_consistency,
        "FAIL: consistent_routing_pct={consistent_routing_pct:.1}% < {BAR_CONSISTENT_ROUTING_PCT}",
    );
    assert!(
        pass_load,
        "FAIL: max_shard_load_pct={max_shard_load_pct:.1}% >= {BAR_MAX_SHARD_LOAD_PCT}",
    );
}
