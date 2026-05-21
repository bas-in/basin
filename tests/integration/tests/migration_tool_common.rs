//! Shared scaffold for migration-tool integration tests (Phase 5.25.A).
//!
//! This module provides reusable helpers that every per-tool test (5.25.B–F)
//! can call without duplicating engine-launch or schema-introspection logic.
//!
//! ## Structure
//!
//! ```text
//! tests/integration/tests/
//!   migration_tool_common.rs      ← THIS FILE (scaffold + helpers)
//!   migration_tool_flyway.rs      ← 5.25.B  (calls helpers here)
//!   migration_tool_golang_migrate.rs ← 5.25.C
//!   migration_tool_diesel.rs      ← 5.25.D
//!   migration_tool_sqlx.rs        ← 5.25.E
//!   migration_tool_prisma.rs      ← 5.25.F
//! ```
//!
//! ## In-process server pattern
//!
//! `spawn_basin_server()` reuses the same infrastructure as `smoke_pgx` and
//! `compare_server_lifecycle`: it constructs a `LocalFileSystem`-backed
//! `Storage`, an `InMemoryCatalog`, an `Engine`, and then calls
//! `basin_router::run_until_bound` — the same one-liner every other
//! integration test uses. No new infrastructure is invented here.
//!
//! ## Docker-compose fixture
//!
//! `tests/integration/fixtures/migration-tool-scaffold/docker-compose.template.yml`
//! is a companion file for per-tool tests that need to invoke an external
//! migration CLI (Flyway, golang-migrate, etc.) against a persistent TCP
//! endpoint. Render the two placeholders (`{{PGWIRE_PORT}}` and
//! `{{PROJECT_NAME}}`), write it to a `TempDir`, and invoke docker-compose.
//! When docker is unavailable, the test should print `[skip]` and return
//! early — see `README.md` in the fixture directory for the full protocol.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

// ── In-process server ─────────────────────────────────────────────────────────

/// An ephemeral Basin server running in-process via `basin_router::run_until_bound`.
///
/// Holds the RAII handles that keep the server alive. Drop this value to shut
/// the server down and free the temporary storage directory.
pub struct EphemeralBasin {
    /// The `host:port` the server is bound to. Pass this to `tokio-postgres` or
    /// any other Postgres client to connect.
    pub addr: SocketAddr,
    /// The project slug used as the pgwire username.
    pub project_slug: String,
    /// Connection string suitable for `tokio_postgres::connect`.
    pub conn_str: String,
    // Keep these alive until drop:
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

/// Spawn an ephemeral Basin server bound to `127.0.0.1:0`.
///
/// Reuses the exact same infrastructure as `smoke_pgx::start_server` and
/// `compare_server_lifecycle`: `LocalFileSystem` storage + `InMemoryCatalog` +
/// `run_until_bound`. No new dependencies are introduced.
///
/// The returned `EphemeralBasin` keeps the server alive via its RAII handles.
pub async fn spawn_basin_server() -> EphemeralBasin {
    spawn_basin_server_with_slug("alice").await
}

/// Like `spawn_basin_server` but with a custom project slug / pgwire username.
pub async fn spawn_basin_server_with_slug(project_slug: &str) -> EphemeralBasin {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().expect("create storage tempdir");
    let fs = LocalFileSystem::new_with_prefix(dir.path()).expect("LocalFileSystem");
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert(project_slug.to_owned(), ProjectId::new());
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
    .expect("basin server failed to bind");

    let addr = running.local_addr;
    let conn_str = format!(
        "host=127.0.0.1 port={} user={} password=ignored dbname=basin",
        addr.port(),
        project_slug,
    );

    // Poll until the port is actually accepting connections (mirrors the
    // pattern in `compare_server_lifecycle::spawn_basin_server`).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "basin server did not become ready within 5 s"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    EphemeralBasin {
        addr,
        project_slug: project_slug.to_owned(),
        conn_str,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

// ── Schema introspection snapshot ─────────────────────────────────────────────

/// A point-in-time snapshot of tables, columns, and (lightly) constraints as
/// seen through `information_schema`. Per-tool tests build one of these as the
/// "expected" baseline and call `assert_schema_matches_snapshot` after running
/// the migration tool.
#[derive(Debug, Clone)]
pub struct SchemaSnapshot {
    /// Expected tables in the `public` schema, keyed by table name.
    pub tables: HashMap<String, TableSnapshot>,
}

/// Expected column layout for one table.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    /// Ordered list of (column_name, data_type_lower) pairs.
    /// `data_type_lower` should match `information_schema.columns.data_type`
    /// lowercased, e.g. `"integer"`, `"text"`, `"timestamp without time zone"`.
    pub columns: Vec<(String, String)>,
}

impl SchemaSnapshot {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(mut self, name: impl Into<String>, columns: Vec<(&str, &str)>) -> Self {
        self.tables.insert(
            name.into(),
            TableSnapshot {
                columns: columns
                    .into_iter()
                    .map(|(c, t)| (c.to_owned(), t.to_owned()))
                    .collect(),
            },
        );
        self
    }
}

impl Default for SchemaSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Introspect the live schema via `information_schema.columns` and assert that
/// every table and column in `expected` is present with the right type.
///
/// Extra tables/columns beyond what `expected` lists are silently ignored —
/// migration tools often create their own history tables (`flyway_schema_history`,
/// `schema_migrations`, etc.) that are not part of the application schema.
///
/// Panics with a clear diff message on mismatch.
pub async fn assert_schema_matches_snapshot(conn_str: &str, expected: &SchemaSnapshot) {
    let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .expect("connect for schema introspection");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT table_name, column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = 'public' \
             ORDER BY table_name, ordinal_position",
            &[],
        )
        .await
        .expect("information_schema.columns query");

    // Build an actual map: table → [(col, type)]
    let mut actual: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for row in &rows {
        let table: &str = row.get(0);
        let col: &str = row.get(1);
        let ty: &str = row.get(2);
        actual
            .entry(table.to_owned())
            .or_default()
            .push((col.to_owned(), ty.to_lowercase()));
    }

    for (table_name, expected_table) in &expected.tables {
        let actual_cols = actual.get(table_name).unwrap_or_else(|| {
            panic!(
                "schema introspection: expected table `{table_name}` not found in live schema.\n\
                 Actual tables: {:?}",
                actual.keys().collect::<Vec<_>>()
            )
        });

        for (exp_col, exp_type) in &expected_table.columns {
            let found = actual_cols.iter().find(|(c, _)| c == exp_col);
            match found {
                Some((_, actual_type)) if actual_type == exp_type => {}
                Some((_, actual_type)) => {
                    panic!(
                        "schema introspection: table `{table_name}` column `{exp_col}`: \
                         expected type `{exp_type}`, got `{actual_type}`"
                    );
                }
                None => {
                    panic!(
                        "schema introspection: table `{table_name}` missing column `{exp_col}` \
                         (expected type `{exp_type}`).\n\
                         Actual columns: {:?}",
                        actual_cols.iter().map(|(c, _)| c).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}

// ── ORM query battery ─────────────────────────────────────────────────────────

/// A single CRUD query in the battery. The `sql` is executed and the first
/// column of the first row (as text) is compared against `expected_first_col`.
/// Use `None` for `expected_first_col` when the result is "don't care" (e.g.
/// a DML statement that returns nothing meaningful).
#[derive(Debug, Clone)]
pub struct CrudQuery {
    pub label: &'static str,
    pub sql: &'static str,
    pub expected_first_col: Option<&'static str>,
}

/// Execute each query in `battery` against `conn_str` and assert the results.
/// Panics on any mismatch with a label-prefixed message.
pub async fn run_crud_battery(conn_str: &str, battery: &[CrudQuery]) {
    let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .expect("connect for CRUD battery");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    for q in battery {
        let rows = client
            .query(q.sql, &[])
            .await
            .unwrap_or_else(|e| panic!("[{}] query failed: {e}\nSQL: {}", q.label, q.sql));

        if let Some(expected) = q.expected_first_col {
            let first_value: String = rows
                .first()
                .unwrap_or_else(|| panic!("[{}] expected a row, got none", q.label))
                .try_get::<_, String>(0)
                .or_else(|_| {
                    rows.first()
                        .unwrap()
                        .try_get::<_, i64>(0)
                        .map(|v| v.to_string())
                })
                .unwrap_or_else(|_| {
                    // Fall back to text for any other type.
                    rows.first()
                        .unwrap()
                        .columns()
                        .first()
                        .map(|_| "<non-text>".to_owned())
                        .unwrap_or_default()
                });
            assert_eq!(
                first_value, expected,
                "[{}] first-column mismatch",
                q.label
            );
        }
    }
}

// ── Placeholder smoke test ─────────────────────────────────────────────────────
//
// This test is `#[ignore]`'d. It exists to prove the scaffold compiles and
// that `spawn_basin_server`, `assert_schema_matches_snapshot`, and
// `run_crud_battery` are all callable. It will be un-ignored (or superseded)
// when the real per-tool tests in 5.25.B–F land alongside their companion
// files: migration_tool_flyway.rs, migration_tool_golang_migrate.rs,
// migration_tool_diesel.rs, migration_tool_sqlx.rs, migration_tool_prisma.rs.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "placeholder: real test bodies land in 5.25.B-F per-tool files"]
async fn scaffold_smoke() {
    // Demonstrate that the launcher compiles and is callable.
    let server = spawn_basin_server().await;
    println!("[scaffold_smoke] server up at {}", server.addr);

    // Build a minimal expected snapshot for a trivial schema.
    let snapshot = SchemaSnapshot::new().add_table(
        "smoke_table",
        vec![("id", "integer"), ("name", "text")],
    );

    // Connect directly and create the table so the introspection helper has
    // something to assert against.
    let (client, conn) = tokio_postgres::connect(&server.conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .simple_query("CREATE TABLE smoke_table (id integer, name text)")
        .await
        .expect("CREATE TABLE");

    // Exercise the schema-diff helper.
    assert_schema_matches_snapshot(&server.conn_str, &snapshot).await;

    // Exercise the CRUD battery helper.
    run_crud_battery(
        &server.conn_str,
        &[
            CrudQuery {
                label: "insert",
                sql: "INSERT INTO smoke_table VALUES (1, 'hello')",
                expected_first_col: None,
            },
            CrudQuery {
                label: "select count",
                sql: "SELECT count(*) FROM smoke_table",
                expected_first_col: Some("1"),
            },
        ],
    )
    .await;

    println!("[scaffold_smoke] scaffold helpers verified — all callable");
}
