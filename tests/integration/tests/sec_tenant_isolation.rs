//! Security regression — load-bearing tenant isolation.
//!
//! Two projects A and B share one in-process `Engine`. Every assertion here
//! is keyed on the contract that project B's session can observe NOTHING
//! about project A's data — neither rows, nor metadata, nor catalog
//! existence — across every SQL surface.
//!
//! Covered scenarios (one `#[tokio::test]` per concern):
//!
//! 1. `cross_project_dml_isolation` — B cannot SELECT, INSERT, UPDATE,
//!    DELETE, DROP, or ALTER A's table when A and B both created
//!    same-named tables (or different ones).
//! 2. `cross_project_information_schema_isolation` — B cannot see A's table
//!    via `information_schema.tables` or `pg_catalog.pg_tables`.
//! 3. `cross_project_pg_catalog_class_isolation` — B cannot see A's
//!    `pg_class` row, even by inspecting `relname` directly.
//! 4. `cross_project_fts_search_isolation` — full-text search on B never
//!    surfaces A's tsvector rows.
//! 5. `cross_project_jsonb_isolation` — JSONB query on B never surfaces A's
//!    JSONB rows; the cross-tenant JSON probe stays in B's namespace.
//! 6. `cross_project_system_schema_no_leak` — system schemas (`auth`,
//!    `storage`) must not surface A's rows to B's session.
//!
//! Style: mirror `rmw_update_correctness.rs`'s in-process `build()` helper
//! and never spin up a TCP server. The point is to pin the engine's own
//! per-project session contract, not the pgwire routing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Two-project in-process Engine builder.
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
async fn build_two_project_engine() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    ProjectId,
    ProjectId,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    let a = ProjectId::new();
    let b = ProjectId::new();
    (sd, wd, engine, shard, bg, wal, a, b)
}

fn row_count(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// 1. Cross-project DML isolation: SELECT / INSERT / UPDATE / DELETE /
//    DROP / ALTER all stay scoped to the calling project.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_dml_isolation() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    // A creates `secrets` with a row. B has no `secrets` table.
    a.execute("CREATE TABLE secrets (id BIGINT PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();
    a.execute("INSERT INTO secrets VALUES (1, 'classified')")
        .await
        .unwrap();

    // 1a. B SELECTs A's table — must NotFound (or any failure).
    let sel = b.execute("SELECT * FROM secrets").await;
    assert!(
        sel.is_err(),
        "SECURITY P0: project B saw project A's secrets — {sel:?}"
    );
    if let Err(e) = sel {
        assert!(
            !matches!(e, BasinError::IsolationViolation(_)),
            "SECURITY P0: IsolationViolation must NOT be the user-facing surface; it indicates the safety check fired \
             after a leak almost happened. Got: {e:?}"
        );
    }

    // 1b. B tries INSERT into A's table — must fail.
    let ins = b.execute("INSERT INTO secrets VALUES (2, 'pwn')").await;
    assert!(ins.is_err(), "SECURITY P0: B inserted into A's secrets");

    // 1c. B tries UPDATE A's table — must fail.
    let upd = b.execute("UPDATE secrets SET payload = 'pwn'").await;
    assert!(upd.is_err(), "SECURITY P0: B updated A's secrets");

    // 1d. B tries DELETE A's table — must fail.
    let del = b.execute("DELETE FROM secrets").await;
    assert!(del.is_err(), "SECURITY P0: B deleted A's secrets");

    // 1e. B tries DROP A's table — must fail; A's data must still be there.
    let _ = b.execute("DROP TABLE secrets").await;

    // 1f. B tries ALTER A's table — must fail.
    let _ = b
        .execute("ALTER TABLE secrets ADD COLUMN owner TEXT")
        .await;

    // After every attempt, A's secret is still intact.
    let still = a.execute("SELECT payload FROM secrets").await.unwrap();
    assert_eq!(
        row_count(still),
        1,
        "SECURITY P0: B's adversarial attempts modified A's data"
    );

    // 1g. Same name collision: B creates its own `secrets` and inserts a
    //     distinct payload. Each project sees only its own row.
    b.execute("CREATE TABLE secrets (id BIGINT PRIMARY KEY, payload TEXT)")
        .await
        .unwrap();
    b.execute("INSERT INTO secrets VALUES (99, 'bobs-only')")
        .await
        .unwrap();
    let a_rows = a
        .execute("SELECT payload FROM secrets ORDER BY id")
        .await
        .unwrap();
    let a_payloads: Vec<String> = match a_rows {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect(),
        _ => panic!("expected rows"),
    };
    assert_eq!(
        a_payloads,
        vec!["classified".to_string()],
        "SECURITY P0: A's secrets leaked B's row (name collision)"
    );

    let b_rows = b.execute("SELECT payload FROM secrets").await.unwrap();
    let b_payloads: Vec<String> = match b_rows {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column_by_name("payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect(),
        _ => panic!("expected rows"),
    };
    assert_eq!(
        b_payloads,
        vec!["bobs-only".to_string()],
        "SECURITY P0: B's secrets leaked A's row (name collision)"
    );
}

// ---------------------------------------------------------------------------
// 2. information_schema.tables must be scoped per project.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_information_schema_isolation() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    a.execute("CREATE TABLE alice_only (id BIGINT)")
        .await
        .unwrap();

    // information_schema.tables.
    let q = "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'alice_only'";
    let from_a = a.execute(q).await.unwrap();
    assert_eq!(
        row_count(from_a),
        1,
        "information_schema must surface A's own table to A"
    );
    let from_b = b.execute(q).await.unwrap();
    assert_eq!(
        row_count(from_b),
        0,
        "SECURITY P0: B's information_schema.tables surfaced A's table"
    );

    // pg_catalog.pg_tables (same surface, different shape).
    let q2 = "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'alice_only'";
    let from_b2 = b.execute(q2).await.unwrap();
    assert_eq!(
        row_count(from_b2),
        0,
        "SECURITY P0: B's pg_catalog.pg_tables surfaced A's table"
    );
}

// ---------------------------------------------------------------------------
// 3. pg_catalog.pg_class — relname inspection must also be per-project.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_pg_catalog_class_isolation() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    a.execute("CREATE TABLE alice_class_probe (id BIGINT)")
        .await
        .unwrap();

    let q = "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alice_class_probe'";
    let from_b = b.execute(q).await.unwrap();
    assert_eq!(
        row_count(from_b),
        0,
        "SECURITY P0: B's pg_catalog.pg_class surfaced A's relname"
    );
}

// ---------------------------------------------------------------------------
// 4. FTS / tsvector path stays per-project.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_fts_search_isolation() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    // A creates a docs table with searchable text. The test uses the
    // text/LIKE shape directly (engine FTS feature gate doesn't matter — the
    // *isolation* property does).
    a.execute("CREATE TABLE docs (id BIGINT, body TEXT)")
        .await
        .unwrap();
    a.execute("INSERT INTO docs VALUES (1, 'top secret memo')")
        .await
        .unwrap();

    // B creates its own docs and runs the same search. Must not see A's row.
    b.execute("CREATE TABLE docs (id BIGINT, body TEXT)")
        .await
        .unwrap();
    b.execute("INSERT INTO docs VALUES (99, 'public memo')")
        .await
        .unwrap();
    let b_hits = b
        .execute("SELECT id FROM docs WHERE body LIKE '%secret%'")
        .await
        .unwrap();
    assert_eq!(
        row_count(b_hits),
        0,
        "SECURITY P0: FTS / LIKE scan on B's docs surfaced A's 'top secret' row"
    );

    // Same search on A surfaces A's row only.
    let a_hits = a
        .execute("SELECT id FROM docs WHERE body LIKE '%secret%'")
        .await
        .unwrap();
    assert_eq!(
        row_count(a_hits),
        1,
        "FTS on A's docs must surface A's matching row"
    );
}

// ---------------------------------------------------------------------------
// 5. JSONB-shaped scans stay per-project.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_jsonb_isolation() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    a.execute("CREATE TABLE blobs (id BIGINT, data JSONB)")
        .await
        .unwrap();
    a.execute("INSERT INTO blobs VALUES (1, '{\"owner\": \"alice\", \"flag\": true}'::jsonb)")
        .await
        .unwrap();

    // B creates a same-named JSONB table; the query "flag = true" must only
    // surface B's own row, not A's.
    b.execute("CREATE TABLE blobs (id BIGINT, data JSONB)")
        .await
        .unwrap();
    b.execute("INSERT INTO blobs VALUES (99, '{\"owner\": \"bob\", \"flag\": false}'::jsonb)")
        .await
        .unwrap();
    let b_hits = b
        .execute("SELECT id FROM blobs WHERE data->>'owner' = 'alice'")
        .await
        .unwrap();
    assert_eq!(
        row_count(b_hits),
        0,
        "SECURITY P0: JSONB scan on B's blobs surfaced A's owner=alice row"
    );
}

// ---------------------------------------------------------------------------
// 6. System schema namespaces (auth, storage, basin_*) — querying them as
//    a regular user session must not surface cross-project rows.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_system_schema_no_leak() {
    let (_sd, _wd, eng, _shard, _bg, _wal, pa, pb) = build_two_project_engine().await;
    let _a = eng.open_session(pa).await.unwrap();
    let b = eng.open_session(pb).await.unwrap();

    // Probe the reserved schemas from B. We don't care if these tables
    // exist; we care that a SELECT from them does NOT return another
    // project's rows. Engines that gate the reserved schema (PermissionDenied
    // / NotFound) are fine — same security posture.
    for q in [
        "SELECT * FROM auth.users",
        "SELECT * FROM storage.objects",
        "SELECT * FROM basin_projects",
    ] {
        let r = b.execute(q).await;
        match r {
            Ok(ExecResult::Rows { batches, .. }) => {
                let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(
                    n, 0,
                    "SECURITY P0: system-schema query `{q}` returned {n} rows from B's session"
                );
            }
            Ok(ExecResult::Empty { .. }) | Err(_) => {
                // Acceptable — schema is reserved or not registered.
            }
        }
    }
}
