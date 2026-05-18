//! Verification test for optimistic-locking row-version enforcement under
//! concurrent writers (#103).
//!
//! ## What this tests
//!
//! TypeORM's `@VersionColumn` pattern emits:
//!
//! ```sql
//! UPDATE events SET val = $2, version = version + 1
//! WHERE id = $1 AND version = $3
//! RETURNING version
//! ```
//!
//! If no row matches (version stale), the ORM detects 0 affected rows and
//! raises `OptimisticLockVersionMismatchError`. This is a purely client-side
//! pattern that should work via basin's UPDATE + RETURNING + parameterised
//! WHERE path.
//!
//! ## Race scenario
//!
//! Two concurrent tasks both:
//! 1. Read `version = 1` from the same row.
//! 2. Sleep 1–5 ms to maximise the race window.
//! 3. Execute `UPDATE … WHERE id = 1 AND version = 1 RETURNING version`.
//!
//! Under correct snapshot isolation / OCC semantics:
//! - **Exactly one** task should see 1 returned row (the winner).
//! - The **other** task should see 0 returned rows (loser — stale version).
//! - The final `version` value should be **2**, not 3 (only one committed).
//!
//! If both tasks see 1 returned row that is a row-version isolation bug.
//! If both tasks see 0 returned rows something else is broken.
//!
//! ## Expected outcome
//!
//! Basin's snapshot-based OCC already provides the correct semantics:
//! - Each transaction reads from a stable snapshot.
//! - The loser's commit conflicts with the winner's committed write, triggering
//!   the retry path (#87).
//! - The retry re-reads the row (version = 2 now) and re-executes the UPDATE
//!   with `WHERE version = 1` — no row matches — so 0 rows are returned.
//! - Final version = 2. Winner = 1, Loser = 0. Correct.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::NoTls;

// ---------------------------------------------------------------------------
// Harness (identical pattern to param_bind_smoke / concurrent_write_smoke)
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("occ_test".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=occ_test password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("optimistic_lock_verify conn driver: {e}");
        }
    });
    client
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Two concurrent tasks race to update the same row with a version guard.
///
/// Correct OCC semantics: exactly one task sees 1 returned row (wins), the
/// other sees 0 (loses — stale version). Final version must be 2, not 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optimistic_lock_row_version_concurrent_writers() {
    let server = start_server().await;
    let setup = connect(server.addr).await;

    // Create the table and seed initial row with version = 1.
    // No PRIMARY KEY: keeps the table schema simple and avoids pk-check
    // side-effects that are orthogonal to the OCC version-guard semantics.
    setup
        .execute(
            "CREATE TABLE events (id BIGINT NOT NULL, val TEXT NOT NULL, version BIGINT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE events");

    setup
        .execute(
            "INSERT INTO events (id, val, version) VALUES (1, 'initial', 1)",
            &[],
        )
        .await
        .expect("INSERT seed row");

    // Read the initial version once (outside the spawned tasks) so both tasks
    // start from the same known version=1 without racing against file-deletion
    // side-effects from concurrent writes.
    let version_rows = setup
        .query("SELECT version FROM events WHERE id = 1", &[])
        .await
        .expect("pre-read version");
    let initial_version: i64 = version_rows[0].get(0);
    assert_eq!(initial_version, 1, "seed version must be 1");

    // Spawn two concurrent tasks.  Both use the same stale version they read
    // above (simulating two concurrent ORM contexts that both read version=1
    // and then race to commit their version-guarded UPDATE).
    let addr = server.addr;

    let task_a = tokio::spawn(async move {
        let client = connect(addr).await;

        // Step (b): sleep to widen the race window between the two tasks.
        tokio::time::sleep(Duration::from_millis(3)).await;

        // Step (c): conditional UPDATE — TypeORM @VersionColumn pattern.
        let updated = client
            .query(
                "UPDATE events SET val = $1, version = version + 1 \
                 WHERE id = 1 AND version = $2 \
                 RETURNING version",
                &[&"task_a_value", &initial_version],
            )
            .await
            .expect("task_a: UPDATE … RETURNING");

        updated.len() as u32
    });

    let task_b = tokio::spawn(async move {
        let client = connect(addr).await;

        // Step (b): sleep slightly less so task_b is more likely to run first,
        // maximising the chance that exactly one sees a match.
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Step (c): same pattern as task_a — same stale version.
        let updated = client
            .query(
                "UPDATE events SET val = $1, version = version + 1 \
                 WHERE id = 1 AND version = $2 \
                 RETURNING version",
                &[&"task_b_value", &initial_version],
            )
            .await
            .expect("task_b: UPDATE … RETURNING");

        updated.len() as u32
    });

    let rows_a = task_a.await.expect("task_a panicked");
    let rows_b = task_b.await.expect("task_b panicked");

    println!("=== optimistic_lock_row_version_concurrent_writers ===");
    println!("task_a returned rows: {rows_a}  task_b returned rows: {rows_b}");

    // ── Invariant 1: combined affected rows == 1 ──────────────────────────────
    // Exactly one task should win.  Both winning (rows_a + rows_b == 2) is the
    // isolation bug.  Both losing (== 0) means something else is broken.
    let total_winners = rows_a + rows_b;
    assert_eq!(
        total_winners, 1,
        "expected exactly 1 winner out of 2 concurrent optimistic-lock writers; \
         got task_a={rows_a} task_b={rows_b}. \
         If total=2: row-version isolation bug (both committed). \
         If total=0: UPDATE WHERE or RETURNING is broken."
    );

    // ── Invariant 2: final version == 2 ──────────────────────────────────────
    // Only one writer should have incremented the version.
    let checker = connect(server.addr).await;
    let final_rows = checker
        .query("SELECT version, val FROM events WHERE id = 1", &[])
        .await
        .expect("final SELECT");

    assert_eq!(final_rows.len(), 1, "row must still exist");
    let final_version: i64 = final_rows[0].get(0);
    let final_val: &str = final_rows[0].get(1);

    println!("final version={final_version}  val={final_val}");

    assert_eq!(
        final_version, 2,
        "version must be exactly 2 after one winner incremented it; \
         got {final_version}. If 3: both tasks committed (isolation bug)."
    );

    // ── Invariant 3: winning task agrees with final val ───────────────────────
    // The val stored must correspond to whichever task won.
    assert!(
        final_val == "task_a_value" || final_val == "task_b_value",
        "final val '{final_val}' must be from one of the two tasks"
    );

    println!("PASS: optimistic-lock row-version enforcement is correct under concurrent writers.");
}

/// Baseline sanity: a single-writer version update must always succeed and
/// increment the version.  This ensures the UPDATE WHERE version=$N + RETURNING
/// path works in isolation before we exercise the concurrent race.
///
/// Uses the same table shape as the concurrent test (no PRIMARY KEY) to keep
/// the two tests comparable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optimistic_lock_single_writer_baseline() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE events2 (id BIGINT NOT NULL, val TEXT NOT NULL, version BIGINT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE events2");

    client
        .execute(
            "INSERT INTO events2 (id, val, version) VALUES (1, 'v0', 1)",
            &[],
        )
        .await
        .expect("INSERT seed");

    // Correct version: should update and return new version = 2.
    let rows = client
        .query(
            "UPDATE events2 SET val = $1, version = version + 1 \
             WHERE id = 1 AND version = $2 \
             RETURNING version",
            &[&"v1", &1_i64],
        )
        .await
        .expect("UPDATE with correct version");

    assert_eq!(rows.len(), 1, "correct version → 1 RETURNING row");
    let new_ver: i64 = rows[0].get(0);
    assert_eq!(new_ver, 2, "version must increment to 2");

    // Stale version: should touch 0 rows (TypeORM detects this as mismatch).
    let stale_rows = client
        .query(
            "UPDATE events2 SET val = $1, version = version + 1 \
             WHERE id = 1 AND version = $2 \
             RETURNING version",
            &[&"v_stale", &1_i64], // version is now 2, not 1
        )
        .await
        .expect("UPDATE with stale version must not error, just return 0 rows");

    assert_eq!(
        stale_rows.len(),
        0,
        "stale version → 0 RETURNING rows (TypeORM OptimisticLockVersionMismatchError signal)"
    );

    // Verify final state is unchanged from the first successful update.
    // Read row count and version together using aggregate to avoid any
    // per-column type-inference issues from the RETURNING path.
    let check = client
        .query(
            "SELECT COUNT(*) AS n, MAX(version) AS ver FROM events2 WHERE id = 1",
            &[],
        )
        .await
        .expect("final check");
    assert_eq!(check.len(), 1, "aggregate must return one row");
    let count: i64 = check[0].get(0);
    let max_ver: i64 = check[0].get(1);
    assert_eq!(count, 1, "exactly one row for id=1");
    assert_eq!(
        max_ver, 2,
        "version must be 2 after the single successful update"
    );

    // Also verify val directly.
    let val_rows = client
        .query("SELECT val FROM events2 WHERE id = 1", &[])
        .await
        .expect("val check");
    let val: &str = val_rows[0].get(0);
    assert_eq!(val, "v1", "val must reflect first (correct) update only");
}
