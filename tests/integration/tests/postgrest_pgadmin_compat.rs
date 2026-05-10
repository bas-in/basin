//! Phase 5.11.M tooling-integration harness.
//!
//! PostgREST and pgAdmin run a known set of catalog-introspection queries on
//! connection startup. We don't bundle either binary; instead we issue the
//! same SQL through `tokio-postgres` against an in-process Basin server and
//! assert each query lands a `Vec<Row>` (no pgwire error) with a sane shape.
//!
//! With Phase 5.11.M Tier 3 complete, every PostgREST / pgAdmin probe
//! lands on a registered view. Earlier revisions of this file kept a
//! gap-marker test that asserted `pg_type` joins failed gracefully; now
//! that `pg_catalog.pg_type` ships, that test (`pgadmin_pg_type_join_succeeds`)
//! has been flipped to assert the JOIN returns the seeded table's columns
//! tagged with PG type names.
//!
//! Each test spins a fresh server, opens a single tenant connection, creates
//! a small fixture (2-3 tables + 1 function + 1 procedure), then runs one
//! tooling query.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

/// Lifetime guard for an in-process Basin pgwire server.
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

    let tenant = TenantId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), tenant);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
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
            eprintln!("conn driver: {e}");
        }
    });
    client
}

/// Create the 5.11.M fixture: three tables of varying shape, one user-defined
/// function, one procedure. Mirrors what a real PostgREST-fronted schema
/// would look like on first connect.
async fn seed_fixture(client: &Client) {
    // Basin's pgwire path requires one statement per protocol message. Send
    // each DDL as its own `simple_query`.
    for stmt in [
        "CREATE TABLE accounts (\
             id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             created_at TIMESTAMPTZ NOT NULL\
         )",
        "CREATE TABLE orders (\
             id BIGINT NOT NULL, \
             account_id BIGINT NOT NULL, \
             total DOUBLE PRECISION NOT NULL\
         )",
        "CREATE TABLE audit_log (\
             id BIGINT NOT NULL, \
             message TEXT NOT NULL\
         )",
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
             LANGUAGE sql AS $$ SELECT x + 1 $$",
        "CREATE PROCEDURE log_msg(g TEXT) LANGUAGE sql AS $$ \
             INSERT INTO audit_log VALUES (1, g) $$",
    ] {
        client
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("seed step failed: {stmt:?}: {e}"));
    }
}

// =============================================================================
// PostgREST startup queries
// =============================================================================

/// Query 1 — schema discovery. PostgREST asks for namespaces excluding the
/// system ones; in v0.1 Basin emits exactly `public` per tenant, so the
/// filtered result must contain `public`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_schema_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT n.nspname FROM pg_catalog.pg_namespace n \
             WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')",
            &[],
        )
        .await
        .expect("pg_namespace query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.iter().any(|n| n == "public"),
        "expected 'public' in pg_namespace, got {names:?}"
    );
}

/// Query 2 — relation discovery. PostgREST joins `pg_class` against
/// `pg_namespace` to enumerate user tables in `public`. Basin returns
/// `relkind='r'` for the seeded tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_relation_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT c.relname, c.relkind, n.nspname \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname = 'public'",
            &[],
        )
        .await
        .expect("pg_class JOIN pg_namespace");

    let mut found = std::collections::BTreeSet::new();
    for r in &rows {
        let name: String = r.get(0);
        let kind: String = r.get(1);
        let nspname: String = r.get(2);
        assert_eq!(nspname, "public");
        // The seeded tables are plain base tables.
        if matches!(name.as_str(), "accounts" | "orders" | "audit_log") {
            assert_eq!(kind, "r", "{name} should be a base table");
        }
        found.insert(name);
    }
    for expected in ["accounts", "orders", "audit_log"] {
        assert!(
            found.contains(expected),
            "missing table {expected:?} in pg_class: {found:?}"
        );
    }
}

/// Query 3 — column discovery per relation. PostgREST runs this once per
/// table to learn column ordering, types, and nullability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_column_discovery_per_table() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    // PostgREST's literal query joins pg_attribute → pg_class on attrelid =
    // c.oid and binds the table name as $1. We mirror that exactly.
    let rows = client
        .query(
            "SELECT a.attname, a.attnum, a.atttypid, a.attnotnull \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = $1 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&"accounts"],
        )
        .await
        .expect("pg_attribute JOIN pg_class");

    assert_eq!(rows.len(), 3, "accounts has 3 columns");
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["id", "email", "created_at"]);

    // attnum is 1-based and must come back contiguously.
    let nums: Vec<i16> = rows.iter().map(|r| r.get::<_, i16>(1)).collect();
    assert_eq!(nums, vec![1, 2, 3]);

    // atttypid must be a non-zero PG OID for every shipped column.
    for r in &rows {
        let oid: i64 = r.get(2);
        assert!(oid > 0, "atttypid must be a real PG OID, got {oid}");
    }

    // All seeded columns are NOT NULL.
    for r in &rows {
        let nn: bool = r.get(3);
        assert!(nn, "every seeded column is NOT NULL");
    }
}

/// Query 4 — constraint discovery. Basin v0.1 has no PK / FK / UNIQUE
/// surfaces so `pg_constraint` is empty for the contype filters PostgREST
/// uses. The test pins "empty result, not error".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_constraint_discovery_returns_empty() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT con.conname, con.contype, con.conrelid, con.confrelid \
             FROM pg_catalog.pg_constraint con \
             WHERE con.contype IN ('p', 'f', 'u')",
            &[],
        )
        .await
        .expect("pg_constraint query");

    assert_eq!(
        rows.len(),
        0,
        "v0.1 has no PK/FK/UNIQUE constraints; pg_constraint must be empty"
    );
}

/// Query 5 — routine discovery. PostgREST consults `information_schema.routines`
/// to learn which user-defined functions / procedures are callable as RPCs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_routine_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT r.routine_name, r.routine_type, r.data_type \
             FROM information_schema.routines r \
             WHERE r.specific_schema = 'public'",
            &[],
        )
        .await
        .expect("information_schema.routines query");

    // The fixture registers exactly one FUNCTION + one PROCEDURE.
    assert_eq!(
        rows.len(),
        2,
        "expected 1 function + 1 procedure, got {}",
        rows.len()
    );

    let mut by_name: HashMap<String, (String, Option<String>)> = HashMap::new();
    for r in &rows {
        let name: String = r.get(0);
        let rtype: String = r.get(1);
        let dt: Option<String> = r.get(2);
        by_name.insert(name, (rtype, dt));
    }

    let (rtype, dt) = by_name.get("add_one").expect("add_one missing");
    assert_eq!(rtype, "FUNCTION");
    assert!(dt.is_some(), "function must report a data_type");

    let (rtype, dt) = by_name.get("log_msg").expect("log_msg missing");
    assert_eq!(rtype, "PROCEDURE");
    assert!(
        dt.is_none(),
        "procedure data_type must be NULL (PG behaviour)"
    );
}

/// Query 6 — `information_schema.tables`. PostgREST reads this view to
/// double-check what `pg_class` advertised, and ORM auto-discovery probes
/// it as the SQL-standard surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_information_schema_tables() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT t.table_name, t.table_type \
             FROM information_schema.tables t \
             WHERE t.table_schema = 'public'",
            &[],
        )
        .await
        .expect("information_schema.tables query");

    let names: std::collections::BTreeSet<String> =
        rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for expected in ["accounts", "orders", "audit_log"] {
        assert!(
            names.contains(expected),
            "missing {expected:?} in info_schema.tables: {names:?}"
        );
    }
    for r in &rows {
        let table_type: String = r.get(1);
        // Every seeded relation is a base table.
        assert_eq!(
            table_type, "BASE TABLE",
            "non-MV seeded tables must surface as BASE TABLE"
        );
    }
}

/// Query 7 — `information_schema.columns`. Used by PostgREST + ORMs for
/// type inference. PG type names like `"integer"`, `"text"`, `"bigint"`,
/// `"double precision"` must come back verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgrest_information_schema_columns() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable \
             FROM information_schema.columns c \
             WHERE c.table_schema = 'public'",
            &[],
        )
        .await
        .expect("information_schema.columns query");

    // Build (table, column) → data_type for the assertions below.
    let mut by_col: HashMap<(String, String), (String, String)> = HashMap::new();
    for r in &rows {
        let table: String = r.get(0);
        let column: String = r.get(1);
        let dt: String = r.get(2);
        let nn: String = r.get(3);
        by_col.insert((table, column), (dt, nn));
    }

    // accounts.id BIGINT → "bigint"; orders.total DOUBLE PRECISION → "double precision";
    // accounts.email TEXT → "text". Type names must be the PG-style spellings.
    let (dt, nn) = by_col
        .get(&("accounts".to_owned(), "id".to_owned()))
        .expect("accounts.id row missing");
    assert_eq!(dt, "bigint", "BIGINT must surface as PG 'bigint'");
    assert_eq!(nn, "NO");

    let (dt, _) = by_col
        .get(&("accounts".to_owned(), "email".to_owned()))
        .expect("accounts.email row missing");
    assert_eq!(dt, "text", "TEXT must surface as PG 'text'");

    let (dt, _) = by_col
        .get(&("orders".to_owned(), "total".to_owned()))
        .expect("orders.total row missing");
    assert_eq!(
        dt, "double precision",
        "DOUBLE PRECISION must surface as PG 'double precision'"
    );

    // Every seeded column is NOT NULL — assertion mirrors what an ORM would
    // expect to read here.
    for ((table, column), (_, nn)) in &by_col {
        if matches!(table.as_str(), "accounts" | "orders" | "audit_log") {
            assert_eq!(nn, "NO", "{table}.{column} should be NOT NULL");
        }
    }
}

// =============================================================================
// pgAdmin browse-tree queries
// =============================================================================

/// pgAdmin's first browse query — same shape as PostgREST's relation
/// discovery but additionally projects `c.reltuples`. Must succeed and
/// return our seeded tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgadmin_relation_browse_succeeds() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    let rows = client
        .query(
            "SELECT n.nspname, c.relname, c.relkind, c.reltuples \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'v', 'm')",
            &[],
        )
        .await
        .expect("pgAdmin browse query");

    let mut found = std::collections::BTreeSet::new();
    for r in &rows {
        let nspname: String = r.get(0);
        let relname: String = r.get(1);
        let _relkind: String = r.get(2);
        // reltuples is FLOAT4 in PG; with our shipped pg_class schema the
        // wire-level type is real (Float32). Read it through f32 to pin
        // that the type advertisement is FLOAT4 rather than DOUBLE
        // PRECISION (a regression here would break pgAdmin's row-count
        // estimate parsing).
        let _reltuples: f32 = r.get(3);
        assert_eq!(nspname, "public");
        found.insert(relname);
    }
    for expected in ["accounts", "orders", "audit_log"] {
        assert!(found.contains(expected), "missing {expected:?}: {found:?}");
    }
}

/// pgAdmin's column-detail query joins `pg_attribute` against `pg_type`
/// to render PG type names alongside each column. This test was previously
/// a gap-marker (`pgadmin_pg_type_join_fails_gracefully`) that asserted a
/// "relation does not exist" error — flipped to a positive assertion now
/// that `pg_catalog.pg_type` ships (Phase 5.11.M Tier 3). The query must
/// succeed and return one row per seeded `accounts` column, with `typname`
/// populated from the static PG-type catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgadmin_pg_type_join_succeeds() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    seed_fixture(&client).await;

    // pgAdmin's column-detail query, verbatim shape: pg_attribute JOIN
    // pg_type on `t.oid = a.atttypid`, with `attrelid` resolved through
    // pg_class.
    let rows = client
        .query(
            "SELECT a.attnum, a.attname, t.typname, a.attnotnull \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = (SELECT c.oid FROM pg_catalog.pg_class c \
                                 WHERE c.relname = 'accounts') \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[],
        )
        .await
        .expect("pg_attribute JOIN pg_type must succeed now that pg_type ships");

    assert_eq!(rows.len(), 3, "accounts has 3 columns");
    let mut by_name: HashMap<String, (i16, String, bool)> = HashMap::new();
    for r in &rows {
        let attnum: i16 = r.get(0);
        let attname: String = r.get(1);
        let typname: String = r.get(2);
        let notnull: bool = r.get(3);
        by_name.insert(attname, (attnum, typname, notnull));
    }

    let (n, ty, nn) = by_name.get("id").expect("id column missing");
    assert_eq!(*n, 1);
    assert_eq!(ty, "int8", "BIGINT must surface as PG 'int8'");
    assert!(*nn);

    let (n, ty, _) = by_name.get("email").expect("email column missing");
    assert_eq!(*n, 2);
    assert_eq!(ty, "text", "TEXT must surface as PG 'text'");

    let (n, ty, _) = by_name
        .get("created_at")
        .expect("created_at column missing");
    assert_eq!(*n, 3);
    assert_eq!(
        ty, "timestamptz",
        "TIMESTAMPTZ must surface as PG 'timestamptz'"
    );
}
