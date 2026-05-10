//! Cross-tenant isolation fuzz test.
//!
//! TASK.md "Cross-cutting": find an isolation bug -> file a P0. The single
//! load-bearing rule of the project is that no tenant ever observes another
//! tenant's bytes. This test bombards a fresh in-process Engine with random
//! query traffic from N tenants and asserts every row a session reads carries
//! that session's marker. A leak surfaces as `BasinError::IsolationViolation`
//! plus a `panic!` so CI flags it as a P0.
//!
//! Determinism: seeded `StdRng` keyed off `BASIN_FUZZ_SEED` (default 0xB451).
//! Failures reproduce by re-running with the same seed.

#![allow(clippy::print_stdout)]

use std::env;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult, TenantSession};
use object_store::local::LocalFileSystem;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

const TENANTS: usize = 8;
const ROWS_PER_TENANT: usize = 300;
const ITERATIONS: usize = 1_000;
const DEFAULT_SEED: u64 = 0xB451_FACE_DEAD_BEEF;

fn payload_for(tenant: &TenantId, row: usize) -> String {
    format!("t-{tenant}-{row}")
}

fn payload_prefix_for(tenant: &TenantId) -> String {
    format!("t-{tenant}-")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_cross_tenant_isolation() {
    // Path-injection vector: TableName must reject anything that could escape
    // the per-tenant prefix. Run this first so a regression here trips before
    // the engine even spins up.
    for bad in [
        "../tenant-other/data",
        "./../foo",
        "../../etc/passwd",
        "data/../other",
        "tenant/data",
        "a.b",
        "",
        "1leading",
        "with space",
    ] {
        let res = TableName::new(bad);
        assert!(
            res.is_err(),
            "TableName::new({bad:?}) accepted a path-traversal / malformed input; \
             this is a structural prefix-isolation breach"
        );
    }

    let seed = env::var("BASIN_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);
    println!("[fuzz_cross_tenant_isolation] seed={seed}");
    let mut rng = StdRng::seed_from_u64(seed);

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

    // Provision N tenants. Each gets the same `data(id BIGINT, payload TEXT)`
    // schema and a few hundred rows whose payload is recognisably its own.
    let mut sessions: Vec<TenantSession> = Vec::with_capacity(TENANTS);
    let mut tenant_ids: Vec<TenantId> = Vec::with_capacity(TENANTS);
    let mut known_ids: Vec<Vec<i64>> = Vec::with_capacity(TENANTS);
    for _ in 0..TENANTS {
        let t = TenantId::new();
        let sess = engine.open_session(t).await.unwrap();
        sess.execute("CREATE TABLE data (id BIGINT NOT NULL, payload TEXT NOT NULL)")
            .await
            .unwrap();

        // INSERT in a single multi-row VALUES so we don't pay one Parquet
        // file per row. The id space is offset per tenant so the predicate
        // shapes can pick a valid id-in-T even when running against S.
        let mut sql = String::with_capacity(ROWS_PER_TENANT * 48);
        sql.push_str("INSERT INTO data VALUES ");
        let mut ids = Vec::with_capacity(ROWS_PER_TENANT);
        for r in 0..ROWS_PER_TENANT {
            if r > 0 {
                sql.push(',');
            }
            let id = (tenant_ids.len() as i64) * 1_000_000 + r as i64;
            ids.push(id);
            // payload literal is single-quoted; the ULID and `r` are both
            // ASCII alphanumeric so SQL escaping is unnecessary.
            sql.push_str(&format!("({id}, '{}')", payload_for(&t, r)));
        }
        sess.execute(&sql).await.unwrap();

        tenant_ids.push(t);
        known_ids.push(ids);
        sessions.push(sess);
    }

    let started = Instant::now();
    let mut total_rows_scanned: u64 = 0;
    let mut shape_counts = [0u64; 4];

    for _ in 0..ITERATIONS {
        // Pick S, then T != S.
        let s_idx = rng.gen_range(0..TENANTS);
        let mut t_idx = rng.gen_range(0..TENANTS);
        while t_idx == s_idx {
            t_idx = rng.gen_range(0..TENANTS);
        }
        let s_tenant = &tenant_ids[s_idx];
        let t_tenant = &tenant_ids[t_idx];
        let sess = &sessions[s_idx];

        let shape = rng.gen_range(0..4u8);
        shape_counts[shape as usize] += 1;
        let sql = match shape {
            0 => "SELECT * FROM data".to_string(),
            1 => {
                // A "known id in T" — T's id range is disjoint from S's, so
                // a leak surfaces as a row in the result whose payload
                // doesn't start with `t-{S}-`.
                let ids = &known_ids[t_idx];
                let id = ids[rng.gen_range(0..ids.len())];
                format!("SELECT * FROM data WHERE id = {id}")
            }
            2 => "SELECT count(*) AS n FROM data".to_string(),
            _ => format!("SELECT * FROM data WHERE payload LIKE '%t-{t_tenant}%'"),
        };

        let res = match sess.execute(&sql).await {
            Ok(r) => r,
            Err(e) => panic!(
                "fuzz query failed (seed={seed}, S={s_tenant}, T={t_tenant}, sql={sql:?}): {e:?}"
            ),
        };

        // Shape 2 is a count query — it returns one row whose value isn't a
        // payload string. Skip the per-row marker check; the structural test
        // is that the count is bounded by S's row count (a leak would
        // inflate it).
        if shape == 2 {
            if let ExecResult::Rows { batches, .. } = res {
                for b in &batches {
                    total_rows_scanned += b.num_rows() as u64;
                    if b.num_rows() == 0 {
                        continue;
                    }
                    // n is BIGINT so the column is Int64Array.
                    let arr = b
                        .column_by_name("n")
                        .expect("count column 'n'")
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("count is bigint");
                    for i in 0..arr.len() {
                        let n = arr.value(i);
                        if n as usize > ROWS_PER_TENANT {
                            let msg = format!(
                                "count(*)={n} exceeds ROWS_PER_TENANT={ROWS_PER_TENANT} \
                                 for S={s_tenant} (seed={seed}, sql={sql:?}) — \
                                 isolation breach",
                            );
                            let _err = BasinError::isolation(msg.clone());
                            panic!("ISOLATION VIOLATION: {msg}");
                        }
                    }
                }
            }
            continue;
        }

        let expected_prefix = payload_prefix_for(s_tenant);
        match res {
            ExecResult::Rows { batches, .. } => {
                for b in &batches {
                    total_rows_scanned += b.num_rows() as u64;
                    if b.num_rows() == 0 {
                        continue;
                    }
                    let arr = b
                        .column_by_name("payload")
                        .expect("payload column missing")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("payload is utf8");
                    for i in 0..arr.len() {
                        let v = arr.value(i);
                        if !v.starts_with(&expected_prefix) {
                            // Construct the isolation error so the BasinError
                            // path is exercised, then panic so the test
                            // framework reports a hard failure (P0).
                            let msg = format!(
                                "tenant {s_tenant} observed payload {v:?} that does not \
                                 start with {expected_prefix:?} (seed={seed}, T={t_tenant}, \
                                 sql={sql:?})"
                            );
                            let _err = BasinError::isolation(msg.clone());
                            panic!("ISOLATION VIOLATION: {msg}");
                        }
                    }
                }
            }
            ExecResult::Empty { tag } => {
                panic!("expected rows from {sql:?}, got Empty({tag}) — seed={seed}")
            }
        }
    }

    let elapsed = started.elapsed();
    println!(
        "[fuzz_cross_tenant_isolation] iters={ITERATIONS}, tenants={TENANTS}, \
         rows_per_tenant={ROWS_PER_TENANT}, total_rows_scanned={total_rows_scanned}, \
         elapsed={:.2}s, shapes(scan/by_id/count/like)={:?}, seed={seed}",
        elapsed.as_secs_f64(),
        shape_counts
    );
}
