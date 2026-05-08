//! Per-tenant telemetry counters viability test.
//!
//! Bar: write 10k rows for tenant A and 100 rows for tenant B; assert
//! `Engine::tenant_counters(A).bytes_written_total > 100 *
//! Engine::tenant_counters(B).bytes_written_total`. Confirms the registry
//! aggregates correctly per tenant and that storage-side bytes get bumped
//! through the engine's write path.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_tenant_counters_aggregate_isolated_bytes() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let a = TenantId::new();
    let b = TenantId::new();

    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();
    for s in [&sa, &sb] {
        s.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
    }

    // Tenant A: 10k rows split across many small inserts. Multiple inserts
    // mean multiple parquet files (each carries footer + schema overhead),
    // so per-tenant bytes_written reflects the workload volume even after
    // ZSTD squashes the row payloads. Distinct per-row payloads further
    // resist compression.
    const A_BATCHES: usize = 100;
    const A_ROWS_PER_BATCH: usize = 100;
    for batch in 0..A_BATCHES {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in 0..A_ROWS_PER_BATCH {
            if i > 0 {
                sql.push(',');
            }
            let id = batch * A_ROWS_PER_BATCH + i;
            sql.push_str(&format!("({id}, 'a-row-distinct-{id:08x}')"));
        }
        sa.execute(&sql).await.unwrap();
    }

    // Tenant B: 100 rows in one insert (one parquet file).
    let mut sql_b = String::from("INSERT INTO t VALUES ");
    for i in 0..100 {
        if i > 0 {
            sql_b.push(',');
        }
        sql_b.push_str(&format!("({i}, 'b-row')"));
    }
    sb.execute(&sql_b).await.unwrap();

    let snap_a = eng.tenant_counters(&a);
    let snap_b = eng.tenant_counters(&b);

    assert!(
        snap_a.bytes_written_total > 0,
        "tenant A bytes_written_total must be > 0, got {snap_a:?}"
    );
    assert!(
        snap_b.bytes_written_total > 0,
        "tenant B bytes_written_total must be > 0, got {snap_b:?}"
    );
    assert!(
        snap_a.bytes_written_total > 100 * snap_b.bytes_written_total,
        "tenant A's bytes_written_total ({}) must exceed 100x tenant B's ({})",
        snap_a.bytes_written_total,
        snap_b.bytes_written_total
    );

    // ops_total must be ≥ the number of execute() calls per tenant
    // (CREATE TABLE + INSERT = 2). Sanity that op counters wire through.
    assert!(snap_a.ops_total >= 2);
    assert!(snap_b.ops_total >= 2);

    // Read path bumps bytes_read_total. Drive a SELECT and check.
    let _ = sa.execute("SELECT id FROM t").await.unwrap();
    let after_read_a = eng.tenant_counters(&a);
    assert!(
        after_read_a.bytes_read_total > 0,
        "tenant A bytes_read_total must be > 0 after SELECT, got {after_read_a:?}"
    );

    // Tenant B's read total must remain at whatever it was pre-A's-SELECT —
    // the registry isolates per-tenant.
    let after_read_b = eng.tenant_counters(&b);
    assert_eq!(
        after_read_b.bytes_read_total, snap_b.bytes_read_total,
        "tenant B bytes_read_total leaked from tenant A's SELECT"
    );
}
