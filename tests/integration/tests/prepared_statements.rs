//! Acceptance-gate test for the PostgreSQL extended-query protocol
//! (Parse / Bind / Describe / Execute) wired through the engine's
//! `prepare` / `bind` / `execute_bound` path.
//!
//! These shapes are the keystone for ORM compatibility (Drizzle, Prisma, sqlx,
//! Diesel, TypeORM all drive them). Every test here uses `tokio_postgres`'s
//! parameterised API (`client.query("... $1 ...", &[&val])`) which goes
//! through the extended-query Parse → Bind → Execute path — the exact path
//! every ORM driver takes.
//!
//! Scope of the gate
//! -----------------
//!  1. `SELECT … WHERE id = $1` with an int param.
//!  2. `SELECT … WHERE email = $1` with a text param.
//!  3. `INSERT INTO t VALUES ($1, $2)` with mixed types + `RETURNING *`.
//!  4. `UPDATE t SET col = $1 WHERE id = $2`.
//!  5. `DELETE FROM t WHERE id = ANY($1::int[])` — array param with cast.
//!  6. Repeated `Execute` on the same `Portal` with different params
//!     (the whole point of prepared statements).
//!
//! The harness mirrors `param_bind_smoke.rs` (in-process Basin server +
//! `tokio_postgres` client) so any drift between the two is obvious.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// ---------------------------------------------------------------------------
// Test server helpers — copied from param_bind_smoke.rs / orm_compat.rs so
// this test stands on its own and is easy to read without grepping.
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
    map.insert("alice".to_owned(), project);
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

async fn connect(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("prepared_statements conn driver: {e}");
        }
    });
    client
}

/// Canonical users fixture: BIGINT id + TEXT email + TEXT name (nullable).
/// Mirrors the schema every ORM tutorial uses.
async fn seed_users(client: &Client) {
    for stmt in [
        "CREATE TABLE users (\
             id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             name TEXT\
         )",
        "INSERT INTO users (id, email, name) VALUES \
             (1, 'a@x.com', 'Alice'), \
             (2, 'b@x.com', 'Bob'), \
             (3, 'c@x.com', NULL)",
    ] {
        client
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("seed_users {stmt:?}: {e}"));
    }
}

// =============================================================================
// 1. SELECT … WHERE id = $1 — int param.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_where_id_eq_int_param() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    let rows = client
        .query("SELECT id, email FROM users WHERE id = $1", &[&2_i64])
        .await
        .expect("SELECT WHERE id = $1");
    assert_eq!(rows.len(), 1, "exactly one user with id=2");
    assert_eq!(rows[0].get::<_, i64>(0), 2);
    assert_eq!(rows[0].get::<_, &str>(1), "b@x.com");

    // No-match must succeed with zero rows, not error.
    let rows = client
        .query("SELECT id FROM users WHERE id = $1", &[&404_i64])
        .await
        .expect("no-match SELECT");
    assert_eq!(rows.len(), 0, "no row for id=404");
}

// =============================================================================
// 2. SELECT … WHERE email = $1 — text param.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_where_email_eq_text_param() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    let rows = client
        .query(
            "SELECT id, email FROM users WHERE email = $1 LIMIT 1",
            &[&"a@x.com"],
        )
        .await
        .expect("SELECT WHERE email = $1");
    assert_eq!(rows.len(), 1, "exactly one user with email=a@x.com");
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "a@x.com");
}

// =============================================================================
// 3. INSERT INTO t VALUES ($1, $2, ...) RETURNING *.
//    Mixed types: BIGINT + TEXT + nullable TEXT.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_values_returning_all() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    let row = client
        .query_one(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) RETURNING *",
            &[&42_i64, &"new@x.com", &"Newbie"],
        )
        .await
        .expect("INSERT … RETURNING *");
    assert_eq!(row.get::<_, i64>(0), 42);
    assert_eq!(row.get::<_, &str>(1), "new@x.com");
    assert_eq!(row.get::<_, Option<&str>>(2), Some("Newbie"));

    // Round-trip read back via a separate parameterised SELECT.
    let row = client
        .query_one("SELECT email, name FROM users WHERE id = $1", &[&42_i64])
        .await
        .expect("post-insert SELECT");
    assert_eq!(row.get::<_, &str>(0), "new@x.com");
    assert_eq!(row.get::<_, Option<&str>>(1), Some("Newbie"));
}

// =============================================================================
// 4. UPDATE t SET col = $1 WHERE id = $2.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_set_where_two_params() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    let n = client
        .execute(
            "UPDATE users SET email = $1 WHERE id = $2",
            &[&"alice2@x.com", &1_i64],
        )
        .await
        .expect("UPDATE SET email = $1 WHERE id = $2");
    assert_eq!(n, 1, "exactly one row updated");

    let row = client
        .query_one("SELECT email FROM users WHERE id = $1", &[&1_i64])
        .await
        .expect("post-update SELECT");
    assert_eq!(row.get::<_, &str>(0), "alice2@x.com");

    // No-op UPDATE on a non-existent id must succeed with 0 affected.
    let n = client
        .execute(
            "UPDATE users SET email = $1 WHERE id = $2",
            &[&"ghost@x.com", &404_i64],
        )
        .await
        .expect("UPDATE no-match must succeed with 0 rows");
    assert_eq!(n, 0);
}

// =============================================================================
// 5. DELETE FROM t WHERE id = ANY($1::int[]) — array param with explicit cast.
//
// This is the shape Drizzle (and many ORM array helpers) emit for
// `.where(inArray(t.id, ids))`. The placeholder is bound as a
// PostgreSQL text-format array literal (e.g. `{1,2}`) carried as TEXT on
// the wire; the engine substitutes it inside the SQL surrounded by the
// explicit `::int[]` cast.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_where_id_any_int_array_param() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    // tokio-postgres encodes &[i64] as `int8[]` (1016); the SQL casts it
    // explicitly to int[] (1007) inside the engine.  Both shapes are common
    // (Drizzle uses `int[]`, sqlx defaults to `int8[]`); we exercise both
    // here to keep the gate honest.
    let ids: Vec<i64> = vec![1, 3];
    let n = client
        .execute(
            "DELETE FROM users WHERE id = ANY($1)",
            &[&ids],
        )
        .await
        .expect("DELETE WHERE id = ANY($1) (bigint[])");
    assert_eq!(n, 2, "exactly ids {{1,3}} deleted");

    // Remaining user is id=2.
    let rows = client
        .query("SELECT id FROM users ORDER BY id", &[])
        .await
        .expect("post-delete SELECT");
    let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(ids, vec![2], "only id=2 remains after batch DELETE");
}

/// Drizzle's verbatim shape: `DELETE … WHERE id = ANY($1::int[])`.  The
/// explicit `::int[]` cast resolves through the projection-cast path so
/// ParameterDescription advertises `int4[]`; the engine substitutes the
/// bound `Vec<i32>` as `ARRAY[1, 3]` and `rewrite_any_array` folds it to
/// `IN (1, 3)` for the planner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_where_id_any_int4_array_cast_param() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    // Use INT (int4) so the cast actually narrows.
    client
        .simple_query(
            "CREATE TABLE small (id INT NOT NULL, label TEXT NOT NULL)",
        )
        .await
        .expect("CREATE TABLE small");
    client
        .simple_query("INSERT INTO small VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("seed small");

    let ids: Vec<i32> = vec![1, 3];
    let n = client
        .execute(
            "DELETE FROM small WHERE id = ANY($1::int[])",
            &[&ids],
        )
        .await
        .expect("DELETE WHERE id = ANY($1::int[]) (Drizzle's verbatim shape)");
    assert_eq!(n, 2, "two rows deleted");

    let rows = client
        .query("SELECT id FROM small ORDER BY id", &[])
        .await
        .expect("post-delete SELECT");
    let remaining: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(remaining, vec![2], "only id=2 survives");
}

// =============================================================================
// 6. Repeated Execute on the same Portal with different params.
//    This is the whole point of prepared statements: parse + plan once,
//    bind + execute many times.
// =============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_statement_repeated_execute_different_params() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_users(&client).await;

    // Prepare once.
    let stmt = client
        .prepare("SELECT email FROM users WHERE id = $1")
        .await
        .expect("prepare SELECT WHERE id = $1");

    // Re-execute three times with different binds; each must yield the
    // correct row independently (no Bind-state leaking between executes).
    for (id, want) in [(1_i64, "a@x.com"), (2, "b@x.com"), (3, "c@x.com")] {
        let rows = client
            .query(&stmt, &[&id])
            .await
            .unwrap_or_else(|e| panic!("re-execute id={id}: {e}"));
        assert_eq!(rows.len(), 1, "id={id} → one row on reuse");
        assert_eq!(rows[0].get::<_, &str>(0), want, "id={id} email mismatch");
    }

    // A no-match re-execute on the *same* cached statement must still work.
    let rows = client
        .query(&stmt, &[&404_i64])
        .await
        .expect("reused stmt, no-match bind");
    assert_eq!(rows.len(), 0, "no row for id=404 on reuse");

    // And a follow-up re-execute that does match must still work after a
    // no-match — proves the portal doesn't latch into a stale state.
    let rows = client
        .query(&stmt, &[&2_i64])
        .await
        .expect("post-no-match re-execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "b@x.com");
}
