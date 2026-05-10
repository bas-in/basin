//! Scaling test: compute sharding (router -> shard owners).
//!
//! Claim: Basin's router can fan tenant connections across N shard-owner
//! processes via stable hashing. Reconnects from the same tenant always
//! land on the same shard (consistent routing); across many random
//! tenants no single shard hoards the load.
//!
//! Setup:
//!   - 4 in-process shard-owner pgwire listeners share one LocalFS-backed
//!     storage tempdir + one in-memory catalog.
//!   - 1 front router runs in `shard_endpoints` mode against those four.
//!   - 100 distinct tenants (one pgwire username each) connect through
//!     the front router; each runs CREATE TABLE / INSERT / SELECT.
//!   - We capture which shard handled each connection by reading the
//!     per-shard authentication counter wired into `test_cluster`.
//!   - Each tenant reconnects 5× to verify it lands on the same shard
//!     every time.
//!
//! Bars (dashboard card `scaling_compute_shards`):
//!   - `consistent_routing_pct == 100`: every reconnect for every tenant
//!     hit the same shard.
//!   - `max_shard_load_pct < 50`: across the 100 random tenants, no shard
//!     handled more than 50% of them. (Expected ~25% with hash uniformity
//!     across 4 buckets; 50% is wide enough to absorb ULID-derived noise
//!     on small samples without flaking.)

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{
    report_scaling, AxisSpec, BarOp, PrimaryMetric, SeriesSpec,
};
use basin_router::test_cluster::{start_n_shards, Cluster};
use serde_json::json;
use tokio_postgres::{NoTls, SimpleQueryMessage};

const N_SHARDS: usize = 4;
const N_TENANTS: usize = 100;
const RECONNECTS_PER_TENANT: usize = 5;
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
            // Test connections close eagerly; "unexpected EOF" here is normal.
            if !format!("{e}").contains("unexpected") {
                eprintln!("conn driver({user_owned}): {e}");
            }
        }
    });
    client
}

/// Read every shard's per-tenant authentication counters and return the
/// shard index a given `tenant` was last seen on. Returns `None` if no
/// shard has yet authenticated this tenant.
async fn shard_for_tenant(cluster: &Cluster, tenant: &TenantId) -> Option<usize> {
    for (idx, shard) in cluster.shards.iter().enumerate() {
        let map = shard.stats.snapshot_per_tenant().await;
        if map.contains_key(tenant) {
            return Some(idx);
        }
    }
    None
}

/// Snapshot the per-tenant counter on shard `idx` for `tenant`.
async fn shard_count_for_tenant(cluster: &Cluster, idx: usize, tenant: &TenantId) -> u64 {
    cluster.shards[idx]
        .stats
        .snapshot_per_tenant()
        .await
        .get(tenant)
        .copied()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn scaling_compute_shards() {
    basin_common::telemetry::try_init_for_tests();

    let cluster = start_n_shards(N_SHARDS).await;
    let router_addr = cluster.router_addr();
    println!(
        "spun up {N_SHARDS}-shard cluster: router {router_addr}, shards {:?}",
        cluster.shard_endpoints
    );

    // Provision tenants. One pgwire username per tenant. The shared
    // resolver lives in `cluster.resolver`; both the router and every
    // shard delegate to it so resolution is consistent across processes.
    let mut tenants: Vec<(String, TenantId)> = Vec::with_capacity(N_TENANTS);
    for i in 0..N_TENANTS {
        let user = format!("tenant_{i:03}");
        let t = TenantId::new();
        cluster.resolver.insert(user.clone(), t).await;
        tenants.push((user, t));
    }

    // First pass: every tenant runs CREATE / INSERT / SELECT through the
    // front router. Capture which shard ended up handling each.
    let mut placement: HashMap<TenantId, usize> = HashMap::new();
    for (user, tenant) in &tenants {
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
        // Drop the client so the upstream connection releases its grip
        // before we read counters; not strictly needed but keeps the
        // numbers crisp.
        drop(client);

        let shard_idx = shard_for_tenant(&cluster, tenant)
            .await
            .unwrap_or_else(|| panic!("no shard saw tenant {tenant} after first round"));
        placement.insert(*tenant, shard_idx);
    }

    // Second pass: reconnect each tenant N more times; assert each lands
    // on the same shard as the first round did.
    let mut consistent_hits: u64 = 0;
    let mut total_reconnects: u64 = 0;
    for (user, tenant) in &tenants {
        let expected_idx = *placement.get(tenant).unwrap();
        let baseline = shard_count_for_tenant(&cluster, expected_idx, tenant).await;
        for _ in 0..RECONNECTS_PER_TENANT {
            let client = connect(router_addr, user).await;
            // Lightweight probe so we know auth landed.
            let _ = client.simple_query("SELECT 1").await;
            drop(client);
            total_reconnects += 1;
        }
        let after = shard_count_for_tenant(&cluster, expected_idx, tenant).await;
        let delta = after.saturating_sub(baseline);
        // We expect every reconnect to land on the same shard, so the
        // counter on that shard should advance by exactly RECONNECTS_PER_TENANT.
        if delta == RECONNECTS_PER_TENANT as u64 {
            consistent_hits += RECONNECTS_PER_TENANT as u64;
        } else {
            // Fall through: count every reconnect that actually landed on
            // the expected shard, regardless of whether some leaked elsewhere.
            consistent_hits = consistent_hits.saturating_add(delta);
        }
    }

    let consistent_routing_pct = 100.0 * consistent_hits as f64 / total_reconnects.max(1) as f64;

    // Load distribution: how many tenants ended up on each shard?
    let mut load_per_shard: Vec<usize> = vec![0; N_SHARDS];
    for idx in placement.values() {
        load_per_shard[*idx] += 1;
    }
    let max_load: usize = *load_per_shard.iter().max().unwrap();
    let max_shard_load_pct = 100.0 * max_load as f64 / N_TENANTS as f64;

    println!(
        "[SCALING compute_shards] tenants={N_TENANTS} shards={N_SHARDS} \
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
                "tenants": *count,
                "load_pct": 100.0 * (*count as f64) / N_TENANTS as f64,
            })
        })
        .collect();

    report_scaling(
        "compute_shards",
        "Compute sharding (router -> shard owners)",
        "Hash tenant_id -> shard_id; pgwire connections route to the owning shard. \
         Reconnects from the same tenant always land on the same shard, and load \
         distributes evenly enough across shards.",
        passed,
        AxisSpec {
            key: "shard".into(),
            label: "shard index".into(),
        },
        vec![
            SeriesSpec {
                key: "tenants".into(),
                label: "tenants on shard".into(),
                unit: Some("count".into()),
            },
            SeriesSpec {
                key: "load_pct".into(),
                label: "share of tenants".into(),
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

    assert!(
        pass_consistency,
        "FAIL: consistent_routing_pct={consistent_routing_pct:.1}% < {BAR_CONSISTENT_ROUTING_PCT}",
    );
    assert!(
        pass_load,
        "FAIL: max_shard_load_pct={max_shard_load_pct:.1}% >= {BAR_MAX_SHARD_LOAD_PCT}",
    );
}
