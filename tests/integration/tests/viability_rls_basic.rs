//! Viability test: row-level security predicate injection (Phase 5.6).
//!
//! Card: `viability_rls_basic`
//! Bar: `alice_rows_visible == 5 && bob_rows_visible == 5 && all_rows == 10`.
//!
//! Two principals (`alice`, `bob`) operate against the same tenant's `orders`
//! table under identical RLS policy `owner_id = current_user`. Each inserts
//! five rows tagged with their own owner. With RLS enabled:
//!
//! - Alice's `SELECT * FROM orders` returns exactly her five rows.
//! - Bob's `SELECT * FROM orders` returns exactly his five rows.
//! - An anonymous session (no policy match) sees zero rows under RLS.
//! - Disabling RLS lets every session see all ten rows.
//!
//! What this proves: predicate injection is principal-scoped (uses
//! `current_user`), tenant-local, and reversible — exactly what the spec
//! demands of an RLS layer that sits on top of tenant prefix isolation.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// Count rows in a result set. Panics on the wrong variant — every assertion
/// in this test expects `ExecResult::Rows`.
fn rows_seen(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
    }
}

#[tokio::test]
async fn viability_rls_basic() {
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

    let tenant = TenantId::new();

    // 1) Schema setup. We do this via an admin (no-auth) session — the
    //    schema-shaping DDL doesn't depend on the principal.
    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute("CREATE TABLE orders (id BIGINT NOT NULL, owner_id TEXT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    // 2) Two principals seed five rows each. Each session is opened with
    //    its own `current_user`. INSERTs do not yet enforce WITH CHECK in
    //    v0.1 (see TODO in `crates/basin-engine/src/rls.rs`), so this also
    //    tests that pre-existing data lands without restriction.
    let alice = engine
        .open_session_as(tenant, "alice")
        .await
        .unwrap();
    let bob = engine.open_session_as(tenant, "bob").await.unwrap();

    for i in 1..=5_i64 {
        alice
            .execute(&format!(
                "INSERT INTO orders VALUES ({i}, 'alice', 'a-payload-{i}')"
            ))
            .await
            .unwrap();
    }
    for i in 1..=5_i64 {
        bob.execute(&format!(
            "INSERT INTO orders VALUES ({}, 'bob', 'b-payload-{i}')",
            i + 100
        ))
        .await
        .unwrap();
    }

    // 3) Sanity: without RLS, every session sees all ten rows.
    let pre_rls = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(
        pre_rls, 10,
        "before ENABLE ROW LEVEL SECURITY, alice should see all 10 rows; got {pre_rls}"
    );

    // 4) Enable RLS + create the policy. Postgres-equivalent: the policy
    //    USING expression is `owner_id = current_user`; subsequent SELECTs
    //    transparently AND-merge the predicate into their plans.
    admin
        .execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY orders_owner_access ON orders FOR ALL TO PUBLIC \
             USING (owner_id = current_user)",
        )
        .await
        .unwrap();

    // 5) Each principal sees only their own rows.
    let alice_rows = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    let bob_rows = rows_seen(bob.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(alice_rows, 5, "alice should see her 5 rows under RLS, got {alice_rows}");
    assert_eq!(bob_rows, 5, "bob should see his 5 rows under RLS, got {bob_rows}");

    // 6) An anonymous session under the same tenant sees zero rows (no
    //    policy matches `current_user = 'anonymous'` for a row whose
    //    `owner_id` is `alice` or `bob`).
    let anon = engine.open_session(tenant).await.unwrap();
    let anon_rows = rows_seen(anon.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(
        anon_rows, 0,
        "anonymous session should see zero rows under RLS; got {anon_rows}"
    );

    // 7) Verify the predicate also filters projections + ORDER BY (i.e. the
    //    rewrite is plan-level, not a top-of-query string append). Pull out
    //    Alice's rows by id and confirm none of bob's ids leak in.
    let res = alice
        .execute("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();
    let alice_ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect(),
        ExecResult::Empty { .. } => panic!("expected rows"),
    };
    assert_eq!(alice_ids, vec![1, 2, 3, 4, 5]);

    // 8) Disable RLS — both principals see all rows again. This proves the
    //    rewrite is gated on the catalog flag.
    admin
        .execute("ALTER TABLE orders DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let alice_after = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    let bob_after = rows_seen(bob.execute("SELECT * FROM orders").await.unwrap());
    let anon_after = rows_seen(anon.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(alice_after, 10, "after DISABLE: alice sees all rows");
    assert_eq!(bob_after, 10, "after DISABLE: bob sees all rows");
    assert_eq!(anon_after, 10, "after DISABLE: anon sees all rows");

    // 9) Re-enable + DROP POLICY: with RLS on but no matching policy the
    //    deny-default kicks in. Alice should see zero rows even as the
    //    table owner.
    admin
        .execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute("DROP POLICY orders_owner_access ON orders")
        .await
        .unwrap();
    let alice_no_policy = rows_seen(alice.execute("SELECT * FROM orders").await.unwrap());
    assert_eq!(
        alice_no_policy, 0,
        "RLS on + no policy → deny-all (Postgres semantics); got {alice_no_policy}"
    );

    let pass =
        alice_rows == 5 && bob_rows == 5 && (alice_rows + bob_rows) == 10 && pre_rls == 10;
    println!(
        "[VIABILITY RLS basic] alice={alice_rows}, bob={bob_rows}, all={pre_rls}, anon={anon_rows} {}",
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "rls_basic",
        "Row-level security: per-principal predicate injection",
        "RLS isolates rows per `current_user` while preserving full visibility when disabled.",
        pass,
        PrimaryMetric {
            label: "alice_rows_visible".into(),
            value: alice_rows as f64,
            unit: "rows".into(),
            bar: BarOp::eq(5.0),
        },
        json!({
            "alice_rows_visible": alice_rows,
            "bob_rows_visible": bob_rows,
            "anonymous_rows_visible": anon_rows,
            "all_rows_without_rls": pre_rls,
            "all_rows_after_drop_policy_with_rls_on": alice_no_policy,
        }),
    );

    assert!(pass);
}
