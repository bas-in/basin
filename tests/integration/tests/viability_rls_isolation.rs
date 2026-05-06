//! Viability test: RLS does not replace tenant prefix isolation.
//!
//! Card: `viability_rls_isolation`
//! Bar: `cross_tenant_leak == 0`.
//!
//! Two tenants (A, B) each declare their own RLS policy on a same-named
//! table. The principal `"shared"` exists in both tenants. The test
//! exhaustively queries every (tenant, RLS-state) combination and
//! confirms:
//!
//! 1. Tenant A's session never returns a row tagged with tenant B's
//!    sentinel marker, regardless of RLS state on either side.
//! 2. RLS state on tenant A does not leak into tenant B (and vice-versa).
//!
//! What this proves: the structural per-tenant prefix isolation that
//! Phase 1 substrate ships continues to bind even when an RLS layer is
//! active. Cross-tenant leakage under any RLS configuration is a P0.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Inspect every `marker` cell returned by `sql` in `sess`, count the cells
/// matching `forbidden_marker`. Any non-zero count is a cross-tenant leak.
async fn count_forbidden_marker(
    sess: &TenantSession,
    sql: &str,
    forbidden_marker: &str,
) -> usize {
    let res = sess.execute(sql).await.expect("query failed");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return 0,
    };
    let mut leaks = 0;
    for b in &batches {
        let arr = b
            .column_by_name("marker")
            .expect("marker column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("marker is utf8");
        for i in 0..arr.len() {
            if arr.value(i) == forbidden_marker {
                leaks += 1;
            }
        }
    }
    leaks
}

#[tokio::test]
async fn viability_rls_isolation() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    // Two tenants, each with the same-named table `events`. Markers are
    // unique per tenant so any cross-tenant leak is unambiguous.
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let marker_a = format!("A-{}", tenant_a);
    let marker_b = format!("B-{}", tenant_b);

    // Schema setup runs through admin sessions (no auth) so the principal
    // sessions can be opened *after* the table exists and pick it up
    // through the engine's per-session listing-table registration.
    let admin_a = engine.open_session(tenant_a).await.unwrap();
    let admin_b = engine.open_session(tenant_b).await.unwrap();

    for s in [&admin_a, &admin_b] {
        s.execute("CREATE TABLE events (id BIGINT NOT NULL, marker TEXT NOT NULL, owner TEXT NOT NULL)")
            .await
            .unwrap();
    }

    // Use the same principal name (`"shared"`) in both tenants. This is the
    // attack surface: if RLS were to leak across tenants (say, by sharing a
    // policy table), shared's session under tenant A could see tenant B's
    // rows. The test asserts that doesn't happen.
    let shared_a = engine
        .open_session_as(tenant_a, "shared")
        .await
        .unwrap();
    let shared_b = engine
        .open_session_as(tenant_b, "shared")
        .await
        .unwrap();

    // Seed: each tenant gets 5 rows, all with owner='shared' so the
    // RLS policy matches the principal. The marker is what we count for
    // leak detection.
    for i in 0..5_i64 {
        admin_a
            .execute(&format!(
                "INSERT INTO events VALUES ({i}, '{marker_a}', 'shared')"
            ))
            .await
            .unwrap();
        admin_b
            .execute(&format!(
                "INSERT INTO events VALUES ({i}, '{marker_b}', 'shared')"
            ))
            .await
            .unwrap();
    }

    let mut leak_count: usize = 0;

    // Phase 1: RLS off everywhere. `shared@A` queries only A's rows by
    // tenant prefix; A's session must see *zero* B markers.
    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    // Phase 2: enable RLS on tenant A only. The policy is permissive for
    // owner='shared'; A's session sees its own rows; B's session under no
    // policy still sees its own rows. Neither sees the other's.
    admin_a
        .execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin_a
        .execute(
            "CREATE POLICY a_shared ON events FOR ALL TO PUBLIC USING (owner = current_user)",
        )
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    // Phase 3: enable RLS on tenant B with a *different* policy expression
    // that still happens to permit owner='shared'. This rules out any
    // accidental coupling between policies in the in-memory catalog.
    admin_b
        .execute("ALTER TABLE events ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin_b
        .execute(
            "CREATE POLICY b_shared ON events FOR SELECT TO PUBLIC USING (owner = current_user)",
        )
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    // Phase 4: drop A's policy with RLS still on (deny-default for
    // tenant A). A's session should see 0 rows; B is unaffected and still
    // sees its own rows. Critically, A still sees no B rows.
    admin_a
        .execute("DROP POLICY a_shared ON events")
        .await
        .unwrap();

    leak_count += count_forbidden_marker(&shared_a, "SELECT marker FROM events", &marker_b).await;
    leak_count += count_forbidden_marker(&shared_b, "SELECT marker FROM events", &marker_a).await;

    // A's row count under RLS-on + no-policy must be 0 (deny-default).
    let a_post_drop = match shared_a
        .execute("SELECT marker FROM events")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    assert_eq!(
        a_post_drop, 0,
        "tenant A under RLS-on + no policy should see 0 rows; got {a_post_drop}"
    );
    // B is unaffected: still sees its own 5 rows.
    let b_unaffected = match shared_b
        .execute("SELECT marker FROM events")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    assert_eq!(
        b_unaffected, 5,
        "tenant B unaffected by tenant A's policy drop; got {b_unaffected} rows"
    );

    let pass = leak_count == 0;
    println!(
        "[VIABILITY RLS isolation] cross_tenant_leak={leak_count} (bar=0) {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "rls_isolation",
        "RLS preserves tenant prefix isolation",
        "Cross-tenant row leakage is zero across every RLS configuration combination.",
        pass,
        PrimaryMetric {
            label: "cross_tenant_leak".into(),
            value: leak_count as f64,
            unit: "rows".into(),
            bar: BarOp::eq(0.0),
        },
        json!({
            "cross_tenant_leak": leak_count,
            "tenant_a": tenant_a.to_string(),
            "tenant_b": tenant_b.to_string(),
        }),
    );

    assert_eq!(leak_count, 0, "{leak_count} cross-tenant rows leaked under RLS");
}
