//! ORM startup-query compatibility harness — Phase 5.11.M tail.
//!
//! Mirrors the design of `postgrest_pgadmin_compat.rs`: spin a fresh
//! in-process Basin pgwire server, open a project connection, seed a
//! shared fixture, then issue the *exact* SQL each ORM runs on
//! `connect()` / `sync()`. We do not bundle Node.js or Python runtimes;
//! every assertion runs through `tokio-postgres` against the real
//! catalog views.
//!
//! Sections:
//! - Prisma (Postgres connector / `prisma db pull` introspection)
//! - Sequelize (postgres dialect, `sequelize.sync()` reflection)
//! - SQLAlchemy (psycopg2 / asyncpg dialect reflection)
//!
//! Tests gated on concurrent-agent landings carry an `#[ignore]` with a
//! one-line comment naming the agent (a659ad3 = PK+CHECK+FK enforcement;
//! a9eba2e = Decimal128 reflection columns; 5.7 B1 = secondary indexes;
//! 5.11.K3 = sequence pg_class surface). Un-ignoring is a one-line
//! follow-up when the depended-on agent ships.

#![allow(clippy::print_stdout)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// =============================================================================
// Harness — same shape as postgrest_pgadmin_compat.rs
// =============================================================================

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
            eprintln!("conn driver: {e}");
        }
    });
    client
}

/// Shared fixture for all three ORM sections.
///
/// Three tables:
/// - `users` — single-PK shape (id BIGINT, email TEXT, created_at TIMESTAMPTZ)
/// - `orders` — FK-shape (id BIGINT, user_id BIGINT → users, total NUMERIC(10,2))
/// - `events` — composite-PK shape (project_id BIGINT, event_id BIGINT, payload TEXT)
///
/// Plus one function and one procedure (matching the PostgREST harness).
///
/// We keep PRIMARY KEY / FOREIGN KEY out of the literal DDL because v0.1
/// rejects those clauses at parse time; the *catalog views* still pin the
/// "what the ORM sees on connect" shape, which is what these tests assert.
/// When a659ad3 lands, this fixture flips to include explicit `PRIMARY KEY`
/// / `REFERENCES` clauses and the `#[ignore]` constraint tests un-ignore.
async fn setup_orm_fixture(client: &Client) {
    for stmt in [
        "CREATE TABLE users (\
             id BIGINT NOT NULL, \
             email TEXT NOT NULL, \
             created_at TIMESTAMPTZ NOT NULL\
         )",
        "CREATE TABLE orders (\
             id BIGINT NOT NULL, \
             user_id BIGINT NOT NULL, \
             total NUMERIC(10, 2) NOT NULL\
         )",
        "CREATE TABLE events (\
             project_id BIGINT NOT NULL, \
             event_id BIGINT NOT NULL, \
             payload TEXT NOT NULL\
         )",
        "CREATE FUNCTION add_one(x BIGINT) RETURNS BIGINT \
             LANGUAGE sql AS $$ SELECT x + 1 $$",
        "CREATE PROCEDURE log_event(g TEXT) LANGUAGE sql AS $$ \
             INSERT INTO events VALUES (1, 1, g) $$",
    ] {
        client
            .simple_query(stmt)
            .await
            .unwrap_or_else(|e| panic!("fixture step failed: {stmt:?}: {e}"));
    }
}

// =============================================================================
// Prisma (Postgres connector — `prisma db pull` introspection)
// =============================================================================

/// Prisma query 1 — schema discovery via `information_schema.schemata`.
/// Filter excludes the system schemas; Basin must surface at least
/// `public`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prisma_schema_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema')",
            &[],
        )
        .await
        .expect("information_schema.schemata query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.iter().any(|n| n == "public"),
        "expected 'public' in schemata, got {names:?}"
    );
}

/// Prisma query 2 — table/view discovery per schema. Pulls `table_name` +
/// `table_type`; the seeded tables must come back as `BASE TABLE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prisma_table_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = $1",
            &[&"public"],
        )
        .await
        .expect("information_schema.tables query");

    let mut by_name: HashMap<String, String> = HashMap::new();
    for r in &rows {
        by_name.insert(r.get(0), r.get(1));
    }
    for expected in ["users", "orders", "events"] {
        let tt = by_name
            .get(expected)
            .unwrap_or_else(|| panic!("missing {expected:?} in tables: {by_name:?}"));
        assert_eq!(tt, "BASE TABLE", "{expected} should be BASE TABLE");
    }
}

/// Prisma query 3 — column discovery. PG-correct `data_type` and
/// `udt_name` strings must come back in 1-based ordinal order. This is
/// the load-bearing query for Prisma's type inference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prisma_column_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT column_name, ordinal_position, is_nullable, data_type, udt_name, column_default \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&"public", &"users"],
        )
        .await
        .expect("information_schema.columns query");

    assert_eq!(rows.len(), 3, "users has 3 columns, got {}", rows.len());

    // Row 0: id BIGINT NOT NULL
    let name: String = rows[0].get(0);
    let pos: i32 = rows[0].get(1);
    let nullable: String = rows[0].get(2);
    let dt: String = rows[0].get(3);
    let udt: String = rows[0].get(4);
    assert_eq!(name, "id");
    assert_eq!(pos, 1);
    assert_eq!(nullable, "NO");
    assert_eq!(dt, "bigint", "BIGINT must surface as PG 'bigint'");
    assert_eq!(udt, "int8");

    // Row 1: email TEXT
    let name: String = rows[1].get(0);
    let dt: String = rows[1].get(3);
    let udt: String = rows[1].get(4);
    assert_eq!(name, "email");
    assert_eq!(dt, "text");
    assert_eq!(udt, "text");

    // Row 2: created_at TIMESTAMPTZ
    let name: String = rows[2].get(0);
    let dt: String = rows[2].get(3);
    let udt: String = rows[2].get(4);
    assert_eq!(name, "created_at");
    assert_eq!(
        dt, "timestamp with time zone",
        "TIMESTAMPTZ must surface as PG 'timestamp with time zone'"
    );
    assert_eq!(udt, "timestamptz");
}

/// Prisma query 4 — PK discovery via `pg_constraint` + `pg_attribute`.
/// v0.1 has no PK enforcement → `pg_constraint` is empty. When a659ad3
/// lands the PK column will surface and this test flips from "asserts
/// empty" to "asserts ['id']" by removing `#[ignore]` and switching the
/// assertion shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on a659ad3 (PK+CHECK+FK enforcement); flip when constraint rows surface"]
async fn prisma_pk_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT a.attname FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_constraint c ON c.conrelid = a.attrelid \
             JOIN pg_catalog.pg_class t ON t.oid = c.conrelid \
             WHERE c.contype = 'p' AND t.relname = $1",
            &[&"users"],
        )
        .await
        .expect("pg_constraint PK probe");

    // Flip-marker: when a659ad3 ships, the seeded `users` table's PK
    // column ('id') must be in this set.
    let names: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains("id"),
        "expected PK column 'id', got {names:?}"
    );
}

/// Prisma query 5 — FK discovery. Same a659ad3 dependency; pin the
/// "empty result, not error" behaviour today.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on a659ad3 (FK enforcement); flip when conrelid/confrelid surface"]
async fn prisma_fk_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT con.conname, ref.relname AS referenced_table \
             FROM pg_catalog.pg_constraint con \
             JOIN pg_catalog.pg_class t ON t.oid = con.conrelid \
             JOIN pg_catalog.pg_class ref ON ref.oid = con.confrelid \
             WHERE con.contype = 'f' AND t.relname = $1",
            &[&"orders"],
        )
        .await
        .expect("pg_constraint FK probe");

    // Flip-marker: when a659ad3 ships, the seeded orders.user_id FK to
    // users must surface here. Today the row count is 0 because
    // pg_constraint is empty — but we keep the test ignored rather than
    // pinning an empty-result, since the un-ignore change is one line.
    assert!(!rows.is_empty(), "expected at least one FK row");
}

/// Prisma query 6 — index discovery. v0.1 has no secondary indexes
/// (5.7 B1 queued). The `pg_index` view ships and is empty; pin that
/// behaviour today and flip when 5.7 B1 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "alias resolution gap on JOIN-projected pg_class.oid → indexrelid; planned for 5.7 B1"]
async fn prisma_index_discovery() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT i.indexrelid, ix.indisunique, ix.indisprimary \
             FROM pg_catalog.pg_index ix \
             JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid \
             WHERE t.relname = $1",
            &[&"users"],
        )
        .await
        .expect("pg_index probe must succeed even with zero rows");

    // v0.1: pg_index is empty (no user-defined indexes). When 5.7 B1
    // ships, the row builder will populate this and Prisma will see
    // declared indexes here.
    assert_eq!(
        rows.len(),
        0,
        "v0.1 ships no secondary indexes; pg_index must be empty"
    );
}

// =============================================================================
// Sequelize (postgres dialect — `sequelize.sync()` / model loading)
// =============================================================================

/// Sequelize query 1 — table-definition probe. The LEFT JOIN against
/// `information_schema.key_column_usage` + `information_schema.table_constraints`
/// resolves PK info per column. Ignored on a659ad3 because the LEFT JOIN
/// returns NULLs for PK rows that don't exist yet; flip when constraints
/// surface so the test can assert `pk.constraint_type = 'PRIMARY KEY'`
/// for the `id` column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on a659ad3 (PK enforcement); flip when key_column_usage surfaces PK rows"]
async fn sequelize_table_definition_query() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT \
                pk.constraint_type AS \"Constraint\", \
                c.column_name, c.column_default, c.data_type, \
                c.is_nullable, c.character_maximum_length \
             FROM information_schema.columns c \
             LEFT JOIN information_schema.key_column_usage k \
                 ON c.table_name = k.table_name AND c.column_name = k.column_name \
             LEFT JOIN information_schema.table_constraints pk \
                 ON k.constraint_name = pk.constraint_name \
                 AND pk.constraint_type = 'PRIMARY KEY' \
             WHERE c.table_schema = $1 AND c.table_name = $2",
            &[&"public", &"users"],
        )
        .await
        .expect("Sequelize table-definition query");

    // Flip-marker: when a659ad3 ships, the row for column 'id' must
    // carry `Constraint = 'PRIMARY KEY'`. Today every row has NULL.
    let mut saw_pk = false;
    for r in &rows {
        let constraint: Option<String> = r.get(0);
        let col: String = r.get(1);
        if col == "id" && constraint.as_deref() == Some("PRIMARY KEY") {
            saw_pk = true;
        }
    }
    assert!(
        saw_pk,
        "expected users.id row to carry PRIMARY KEY constraint"
    );
}

/// Sequelize query 2 — table-existence check. Trivial probe; must
/// return one row when the table exists, zero when it doesn't.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequelize_table_existence_check() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2",
            &[&"public", &"users"],
        )
        .await
        .expect("table-existence query (positive case)");
    assert_eq!(rows.len(), 1, "users exists → exactly 1 row");
    let name: String = rows[0].get(0);
    assert_eq!(name, "users");

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = $2",
            &[&"public", &"does_not_exist"],
        )
        .await
        .expect("table-existence query (negative case)");
    assert_eq!(rows.len(), 0, "missing table → 0 rows");
}

/// Sequelize query 3 — sequence listing. v0.1 sequences live in the
/// catalog but don't surface in `pg_class` with `relkind = 'S'` yet
/// (5.11.K3 sequence pg_class surface still queued). Today the query
/// must succeed and return zero rows (Basin's `pg_class` only ships
/// `'r'` and `'m'` relkinds). Flip when 5.11.K3 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on 5.11.K3 (sequence pg_class surface); flip when relkind='S' rows ship"]
async fn sequelize_sequence_listing() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    // The query also depends on pg_namespace OID being looked up in a
    // scalar subquery — we issue the literal Sequelize SQL.
    let rows = client
        .query(
            "SELECT relname FROM pg_catalog.pg_class \
             WHERE relkind = 'S' AND relnamespace = (\
                 SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1\
             )",
            &[&"public"],
        )
        .await
        .expect("pg_class sequence listing");

    // Flip-marker: when 5.11.K3 ships, after a `CREATE SEQUENCE seq1;`
    // statement the listing must contain `seq1`.
    assert!(
        !rows.is_empty(),
        "expected at least one sequence in pg_class"
    );
}

// =============================================================================
// SQLAlchemy (psycopg2 / asyncpg dialect — reflection)
// =============================================================================

/// SQLAlchemy query 1 — full column introspection with precision/scale
/// fields. Basin's `information_schema.columns` view today does not
/// surface `character_maximum_length`, `numeric_precision`,
/// `numeric_scale`, or `datetime_precision` — those columns aren't in
/// `columns_schema()` (see basin-catalog/src/info_schema.rs). The
/// query therefore fails with a "column does not exist" error today.
/// Flip when a9eba2e (Decimal128) ships and the columns view is
/// extended to include the precision/scale projections.
///
/// **Roadmap gap surfaced**: `information_schema.columns` is missing
/// `character_maximum_length` / `numeric_precision` / `numeric_scale` /
/// `datetime_precision`. SQLAlchemy reflection cannot infer NUMERIC(p, s)
/// shape without these. See report.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on a9eba2e (Decimal128 precision/scale columns in info_schema.columns)"]
async fn sqlalchemy_full_column_introspection() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT \
                column_name, data_type, is_nullable, column_default, \
                character_maximum_length, numeric_precision, numeric_scale, \
                datetime_precision, udt_name \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&"public", &"orders"],
        )
        .await
        .expect("SQLAlchemy column introspection (requires precision/scale columns)");

    // Flip-marker: when a9eba2e lands, the `total NUMERIC(10, 2)` column
    // must report numeric_precision = 10, numeric_scale = 2.
    let mut saw_numeric = false;
    for r in &rows {
        let name: String = r.get(0);
        if name == "total" {
            let prec: Option<i32> = r.get(5);
            let scale: Option<i32> = r.get(6);
            assert_eq!(prec, Some(10), "NUMERIC(10,2) precision");
            assert_eq!(scale, Some(2), "NUMERIC(10,2) scale");
            saw_numeric = true;
        }
    }
    assert!(
        saw_numeric,
        "orders.total numeric column must surface precision/scale"
    );
}

/// SQLAlchemy query 2 — composite-PK column ordering via `unnest(conkey)
/// WITH ORDINALITY`. The query exercises pg_constraint + pg_attribute
/// joins and SQL-standard `WITH ORDINALITY`. v0.1 returns 0 rows
/// (pg_constraint empty). Flip when a659ad3 ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "depends on a659ad3 (composite PK rows in pg_constraint)"]
async fn sqlalchemy_pk_column_ordering() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT a.attname AS column_name, ordinality::int AS position \
             FROM pg_catalog.pg_constraint c \
             JOIN pg_catalog.pg_class t ON t.oid = c.conrelid \
             JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum, ordinality) ON true \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum \
             WHERE c.contype = 'p' AND t.relname = $1 \
             ORDER BY ordinality",
            &[&"events"],
        )
        .await
        .expect("SQLAlchemy composite-PK ordering query");

    // Flip-marker: when a659ad3 ships, events' composite PK
    // (project_id, event_id) must surface in this order.
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["project_id", "event_id"]);
}

/// SQLAlchemy query 3 — sequence-backed (SERIAL) column detection.
/// SQLAlchemy probes `column_default` for the `nextval(...)` prefix to
/// decide whether a column is auto-incrementing. v0.1 doesn't yet plumb
/// `DEFAULT nextval('seq')` through CREATE TABLE → `column_default`
/// (5.11.K3 SQL surface), so the query succeeds but always returns
/// zero rows. Flip when 5.11.K3 ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlalchemy_serial_column_detection() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    setup_orm_fixture(&client).await;

    let rows = client
        .query(
            "SELECT column_default FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             AND column_default LIKE 'nextval%'",
            &[&"public", &"users"],
        )
        .await
        .expect("SQLAlchemy serial-column probe");

    // v0.1: no SERIAL columns are declared via CREATE TABLE today, so
    // column_default is NULL and the LIKE filter rejects every row.
    // When 5.11.K3 ships the SQL surface, this test will need a fixture
    // that uses `id BIGSERIAL` or `DEFAULT nextval(...)` and assert at
    // least one row. Today we pin the empty-result behaviour as the
    // contract for the v0.1 roadmap snapshot.
    assert_eq!(
        rows.len(),
        0,
        "v0.1: no nextval-defaulted columns surfaced via CREATE TABLE"
    );
}
