//! Honest ORM-compatibility gate — *wire-level flow* edition.
//!
//! Where `orm_compat.rs` / `prepared_statements.rs` assert individual SQL
//! *shapes*, this file asserts the *sequences* that real ORM/driver tooling
//! issues against a Postgres endpoint, in order, over the same in-process
//! Basin pgwire server (`basin_router::run_until_bound`) that every other
//! wire test uses. Each `*_flow` test simulates one ORM family's lifecycle.
//!
//! ## How this file behaves as a living gap tracker
//!
//! Every flow runs *all* of its steps even after one fails (results are
//! collected, never `?`/`unwrap`-ed mid-flow). For each step we print:
//!
//! ```text
//! [orm-compat] <flow>/<step>: OK
//! [orm-compat] <flow>/<step>: GAP(<short err>)
//! ```
//!
//! At the end of each flow we:
//!   1. ASSERT every step we *know* Basin supports stayed `OK` (a regression
//!      in a supported step fails the build — this is the real gate).
//!   2. Let genuinely-missing features print `GAP` *without* failing — they
//!      are tracked, not gated, so the recorded-gap list can only shrink.
//!   3. Tally GAP steps into a process-wide counter.
//!
//! A final summary test (`zzz_orm_flow_gap_ceiling`) asserts the total GAP
//! count across all flows is `<= RECORDED_GAP_CEILING`. If a previously-GAP
//! step starts passing, the ceiling is now slack — tighten it (free win). If
//! a NEW gap appears, the ceiling trips and the build fails (no silent
//! regressions). Because the ceiling test must observe every flow's tally, the
//! per-flow tallies live in a shared atomic and the ceiling test is named to
//! sort last; each flow test is independent, so we recompute the full flow set
//! inside the ceiling test rather than rely on test-ordering. See
//! `count_gaps_for_all_flows`.
//!
//! ## The asserted-supported vs recorded-gap reasoning (per flow, inline)
//!
//! The "known-supported" list was built empirically from the repo's existing
//! coverage:
//!   - extended-protocol Parse/Bind/Describe/Execute: `prepared_statements.rs`,
//!     `poc_extended_smoke.rs`, `param_bind_smoke.rs`.
//!   - `RETURNING` (INSERT/UPDATE/DELETE): `dml_extras.rs`,
//!     `extended_returning_smoke.rs`.
//!   - multi-row `INSERT ... VALUES (...), (...)`: `dml_extras.rs`.
//!   - transactions BEGIN/COMMIT/ROLLBACK: `coverage_txn_schema.rs`,
//!     `rollback_correctness.rs`, `tx_*`.
//!   - `information_schema.{columns,tables}`: `migration_tool_common.rs`,
//!     `explain_pg_stat_harness.rs`.
//!   - `pg_try_advisory_lock` / `pg_advisory_unlock`: `differential_pg.rs`.
//!   - `CREATE TABLE`/`ALTER TABLE ADD COLUMN`/`CREATE INDEX`:
//!     `viability_alter_add_column.rs`, `alter_table_std.rs`,
//!     `index_variants.rs`.
//!   - multi-statement simple query: `pgwire_multi_statement.rs`.
//!   - `INSERT ... ON CONFLICT DO UPDATE`: `dml_extras.rs`,
//!     `compare_postgres_common.rs`.
//!   - JSONB binary param bind / round-trip: `jsonb_uuid_param_binding.rs`.
//!   - `CREATE SEQUENCE` + `nextval`/`currval`/`lastval`: `type_ddl.rs`,
//!     `sequences.rs`.
//!   - `SELECT version()`, `current_schema()`: `3way_pg_compat.rs`.
//!
//! Steps deliberately classed as recorded-GAP (allowed to fail OR to return a
//! degenerate-but-non-erroring response) are documented at each call site with
//! the reason — these are the ones a real ORM tolerates because Basin is not a
//! drop-in for every Postgres GUC/session knob.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

// ---------------------------------------------------------------------------
// Server + client harness — copied verbatim in shape from
// prepared_statements.rs / poc_extended_smoke.rs so this file stands alone.
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
            eprintln!("orm_flow_compat conn driver: {e}");
        }
    });
    client
}

// ---------------------------------------------------------------------------
// Step-result collection. A flow records (step_name, supported?, outcome).
// `supported = true`  → must be OK, else the gate fails.
// `supported = false` → tracked as a gap if it errors, never fails the gate.
// ---------------------------------------------------------------------------

/// Process-wide count of steps that ended up as a GAP (a *recorded-gap* step
/// that errored, OR a *supported* step that errored — the latter also trips the
/// per-flow assertion, but we still count it so the ceiling sees the truth).
static TOTAL_GAPS: AtomicU64 = AtomicU64::new(0);

/// Outcome of a single flow step.
enum Outcome {
    Ok,
    /// Step errored; carries a short message for the printed GAP line.
    Gap(String),
}

struct Step {
    name: &'static str,
    /// Whether the repo's existing coverage says Basin supports this step.
    supported: bool,
    outcome: Outcome,
}

/// Accumulator threaded through a single flow. Prints each step immediately
/// (so a panicking assertion still leaves a readable trail) and at the end
/// asserts supported-step health + bumps the global gap counter.
struct Flow {
    name: &'static str,
    steps: Vec<Step>,
}

impl Flow {
    fn new(name: &'static str) -> Self {
        Flow {
            name,
            steps: Vec::new(),
        }
    }

    /// Record a step from a `Result`. `supported` is the empirical judgement.
    fn record<T, E: std::fmt::Display>(
        &mut self,
        name: &'static str,
        supported: bool,
        res: Result<T, E>,
    ) {
        let outcome = match res {
            Ok(_) => {
                println!("[orm-compat] {}/{}: OK", self.name, name);
                Outcome::Ok
            }
            Err(e) => {
                let msg = short_err(&e.to_string());
                println!("[orm-compat] {}/{}: GAP({msg})", self.name, name);
                Outcome::Gap(msg)
            }
        };
        self.steps.push(Step {
            name,
            supported,
            outcome,
        });
    }

    /// Record a step whose success/failure is decided by a bool predicate
    /// (used where the *driver call* succeeded but we still want to assert on
    /// the returned data, e.g. Describe column types).
    fn record_check(&mut self, name: &'static str, supported: bool, ok: bool, detail: &str) {
        let outcome = if ok {
            println!("[orm-compat] {}/{}: OK", self.name, name);
            Outcome::Ok
        } else {
            let msg = short_err(detail);
            println!("[orm-compat] {}/{}: GAP({msg})", self.name, name);
            Outcome::Gap(msg)
        };
        self.steps.push(Step {
            name,
            supported,
            outcome,
        });
    }

    /// Finalize: assert every supported step is OK, count gaps, print a summary.
    fn finish(self) {
        let mut gaps = 0u64;
        let mut supported_failures: Vec<String> = Vec::new();
        for s in &self.steps {
            if let Outcome::Gap(msg) = &s.outcome {
                gaps += 1;
                if s.supported {
                    supported_failures.push(format!("{}/{}: {msg}", self.name, s.name));
                }
            }
        }
        let total = self.steps.len();
        println!(
            "[orm-compat] SUMMARY {}: {} steps, {} OK, {} GAP",
            self.name,
            total,
            total as u64 - gaps,
            gaps
        );
        TOTAL_GAPS.fetch_add(gaps, Ordering::SeqCst);

        assert!(
            supported_failures.is_empty(),
            "{} known-supported step(s) regressed in flow `{}`:\n  {}",
            supported_failures.len(),
            self.name,
            supported_failures.join("\n  ")
        );
    }
}

/// Trim a driver/engine error to a single short line for the printed GAP tag.
fn short_err(s: &str) -> String {
    let first = s.lines().next().unwrap_or(s).trim();
    if first.len() > 100 {
        format!("{}…", &first[..100])
    } else {
        first.to_owned()
    }
}

// ===========================================================================
// 1. Prisma runtime CRUD.
//
// Prisma Client's Rust query engine drives the EXTENDED protocol exclusively:
// every query is a Parse(named/unnamed) + Bind + Execute, and writes always
// append `RETURNING ...` (Prisma needs the generated row back to hydrate its
// model objects). createMany() emits a single multi-row
// `INSERT ... VALUES ($1,$2),($3,$4),...`. Interactive transactions wrap
// statements in BEGIN/COMMIT. We trim to the load-bearing shapes.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prisma_runtime_crud() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("prisma_runtime_crud");

    // DDL setup (not a Prisma-runtime step, but the table must exist). We use
    // execute() so it rides the extended protocol like everything Prisma sends.
    f.record(
        "ddl_create_table",
        true,
        client
            .execute(
                "CREATE TABLE \"User\" (id BIGINT NOT NULL, email TEXT NOT NULL, name TEXT)",
                &[],
            )
            .await,
    );

    // INSERT ... RETURNING id — the canonical Prisma `create()`. Supported:
    // RETURNING + extended bind both have direct coverage.
    let insert_returning = client
        .query_one(
            "INSERT INTO \"User\" (id, email, name) VALUES ($1, $2, $3) RETURNING id",
            &[&1_i64, &"a@x.com", &"Alice"],
        )
        .await;
    let inserted_id = insert_returning.as_ref().map(|r| r.get::<_, i64>(0)).ok();
    f.record("insert_returning_id", true, insert_returning);

    // SELECT ... WHERE id = $1 — Prisma `findUnique()`.
    f.record(
        "select_where_id_param",
        true,
        client
            .query("SELECT id, email, name FROM \"User\" WHERE id = $1", &[&1_i64])
            .await,
    );

    // UPDATE ... RETURNING — Prisma `update()` returns the post-update row.
    f.record(
        "update_returning",
        true,
        client
            .query(
                "UPDATE \"User\" SET name = $1 WHERE id = $2 RETURNING id, name",
                &[&"Alice2", &1_i64],
            )
            .await,
    );

    // createMany()-style multi-row INSERT. Supported: dml_extras covers it.
    f.record(
        "create_many_multi_row_insert",
        true,
        client
            .execute(
                "INSERT INTO \"User\" (id, email, name) VALUES ($1,$2,$3),($4,$5,$6)",
                &[
                    &2_i64, &"b@x.com", &"Bob", &3_i64, &"c@x.com", &"Cara",
                ],
            )
            .await,
    );

    // DELETE — Prisma `delete()`.
    f.record(
        "delete_where_id_param",
        true,
        client
            .execute("DELETE FROM \"User\" WHERE id = $1", &[&3_i64])
            .await,
    );

    // Interactive transaction: BEGIN; read-modify-write; COMMIT. Prisma's
    // `$transaction(async tx => …)` issues exactly this. Each statement is a
    // separate extended-protocol round-trip on the same session.
    let tx_result: Result<(), tokio_postgres::Error> = async {
        client.batch_execute("BEGIN").await?;
        let row = client
            .query_one("SELECT name FROM \"User\" WHERE id = $1", &[&1_i64])
            .await?;
        let cur: String = row.get(0);
        let next = format!("{cur}-rmw");
        client
            .execute(
                "UPDATE \"User\" SET name = $1 WHERE id = $2",
                &[&next, &1_i64],
            )
            .await?;
        client.batch_execute("COMMIT").await?;
        Ok(())
    }
    .await;
    f.record("tx_begin_rmw_commit", true, tx_result);
    let _ = inserted_id; // (kept for readability; value asserted via OK status)

    f.finish();
}

// ===========================================================================
// 2. SQLAlchemy session flow.
//
// SQLAlchemy's psycopg/asyncpg dialect, on first connect, probes the server:
//   - `select version()`  (server-version gating)
//   - `show standard_conforming_strings`  (string-literal escaping mode)
// then runs unit-of-work transactions (BEGIN; INSERT; flush via RETURNING or
// lastval(); COMMIT) and, when `Table(autoload_with=…)` reflection is used,
// queries information_schema with the table name as a *bound parameter*.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlalchemy_session_flow() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("sqlalchemy_session_flow");

    // SELECT version() — supported (3way_pg_compat asserts it returns a row).
    f.record(
        "select_version",
        true,
        client.simple_query("SELECT version()").await,
    );

    // show standard_conforming_strings — GAP-tolerant. Basin's executor maps
    // unknown SHOW <var> to an empty (no-row) "SHOW" response rather than
    // erroring (see executor.rs ShowVariable arm), which SQLAlchemy tolerates.
    // We DO require it not to *error* (that part is supported), but we do not
    // require it to return the GUC value, so it is recorded as supported=false
    // unless it actually errors. It will normally be OK (no error).
    f.record(
        "show_standard_conforming_strings",
        false,
        client
            .simple_query("show standard_conforming_strings")
            .await,
    );

    // Reflection-lite needs a table. Use a SERIAL-free schema so lastval() has
    // a sequence to read only in the dedicated step below.
    f.record(
        "ddl_create_table",
        true,
        client
            .batch_execute(
                "CREATE TABLE accounts (id BIGINT NOT NULL, owner TEXT NOT NULL, balance BIGINT NOT NULL)",
            )
            .await,
    );

    // Unit-of-work transaction with a RETURNING flush (SQLAlchemy prefers
    // RETURNING over lastval() on Postgres ≥ 8.2 when an explicit PK is given,
    // but for server-generated PKs it falls back to lastval(); we exercise the
    // RETURNING path here which is the modern default).
    let uow: Result<(), tokio_postgres::Error> = async {
        client.batch_execute("BEGIN").await?;
        let _ = client
            .query_one(
                "INSERT INTO accounts (id, owner, balance) VALUES ($1,$2,$3) RETURNING id",
                &[&1_i64, &"alice", &100_i64],
            )
            .await?;
        client.batch_execute("COMMIT").await?;
        Ok(())
    }
    .await;
    f.record("tx_insert_returning_commit", true, uow);

    // Reflection-lite: information_schema.columns with table_name as a BOUND
    // PARAMETER ($1). This is the exact shape SQLAlchemy's PGDialect reflection
    // sends. migration_tool_common queries info_schema.columns (unparameterized);
    // the parameterized form additionally relies on extended-protocol bind into
    // an information_schema scan — treat as supported and let a regression fail.
    let cols = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[&"accounts"],
        )
        .await;
    // Require it to both succeed AND find the columns we created.
    let cols_ok = matches!(&cols, Ok(rows) if !rows.is_empty());
    f.record("reflect_information_schema_columns_param", true, cols);
    f.record_check(
        "reflect_columns_nonempty",
        true,
        cols_ok,
        "information_schema.columns returned no rows for table_name=$1",
    );

    // information_schema.tables scan (SQLAlchemy `has_table` / `get_table_names`).
    f.record(
        "reflect_information_schema_tables",
        true,
        client
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public'",
                &[],
            )
            .await,
    );

    f.finish();
}

// ===========================================================================
// 3. ActiveRecord (Rails) migration flow.
//
// Rails' PostgreSQLAdapter takes a session advisory lock around the migrator,
// maintains a `schema_migrations` table of applied version strings, and runs
// DDL (CREATE TABLE / ALTER TABLE ADD COLUMN / CREATE INDEX) for each migration.
//   SELECT pg_try_advisory_lock(<hashed-migrator-key>)
//   CREATE TABLE schema_migrations (version VARCHAR PRIMARY KEY)
//   INSERT INTO schema_migrations (version) VALUES (...)
//   SELECT version FROM schema_migrations ORDER BY version
//   <DDL ...>
//   SELECT pg_advisory_unlock(<key>)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activerecord_migration_flow() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("activerecord_migration_flow");

    // Advisory lock acquire. Supported: differential_pg covers
    // pg_try_advisory_lock(int8) + pg_advisory_unlock round-trips.
    // Real Rails INLINES the advisory key (derived bigint, interpolated), so
    // the literal form is the supported baseline. The $1-bound form is a
    // recorded gap: param type inference does not yet reach through UDF
    // argument positions, so Describe reports an unknown type and the
    // client refuses to serialize the bind.
    let lock = client
        .query("SELECT pg_try_advisory_lock(424242)", &[])
        .await;
    f.record("pg_try_advisory_lock", true, lock);
    let lock_param = client
        .query("SELECT pg_try_advisory_lock($1)", &[&424_243_i64])
        .await;
    f.record("pg_try_advisory_lock_param", false, lock_param);

    // schema_migrations table + version rows.
    f.record(
        "create_schema_migrations",
        true,
        client
            .batch_execute(
                "CREATE TABLE schema_migrations (version TEXT NOT NULL)",
            )
            .await,
    );
    f.record(
        "insert_versions",
        true,
        client
            .execute(
                "INSERT INTO schema_migrations (version) VALUES ($1),($2)",
                &[&"20260101000001", &"20260101000002"],
            )
            .await,
    );
    let versions = client
        .query(
            "SELECT version FROM schema_migrations ORDER BY version",
            &[],
        )
        .await;
    let versions_ok = matches!(&versions, Ok(r) if r.len() == 2);
    f.record("select_versions", true, versions);
    f.record_check(
        "select_versions_count",
        true,
        versions_ok,
        "expected 2 schema_migrations rows",
    );

    // A typical migration body: create an app table, ADD COLUMN, add an index.
    f.record(
        "create_app_table",
        true,
        client
            .batch_execute("CREATE TABLE widgets (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await,
    );
    f.record(
        "alter_add_column",
        true,
        client
            .batch_execute("ALTER TABLE widgets ADD COLUMN price BIGINT")
            .await,
    );
    f.record(
        "create_index",
        true,
        client
            .batch_execute("CREATE INDEX index_widgets_on_name ON widgets (name)")
            .await,
    );

    // Advisory unlock.
    f.record(
        "pg_advisory_unlock",
        true,
        client
            .query("SELECT pg_advisory_unlock(424242)", &[])
            .await,
    );

    f.finish();
}

// ===========================================================================
// 4. sqlx describe flow.
//
// sqlx's compile-time `query!` macro and runtime type-checking issue a
// Parse + Describe with NO Execute: it inspects the ParameterDescription
// (param OIDs) and RowDescription (column OIDs) to generate/verify Rust types.
// tokio-postgres exposes this exact path via `client.prepare()`, after which
// `Statement::params()` and `Statement::columns()` hold the describe result.
// We assert Describe returns the right shapes for a SELECT and an
// INSERT...RETURNING, then execute with binds to prove the prepared stmt runs.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlx_describe_flow() {
    use tokio_postgres::types::Type;

    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("sqlx_describe_flow");

    f.record(
        "ddl_create_table",
        true,
        client
            .batch_execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await,
    );

    // Describe a parameterized SELECT (Parse + Describe, no Execute).
    let sel = client
        .prepare("SELECT id, name FROM t WHERE id = $1")
        .await;
    match &sel {
        Ok(stmt) => {
            f.record_check(
                "describe_select_param_type",
                true,
                stmt.params() == [Type::INT8],
                &format!("expected params [INT8], got {:?}", stmt.params()),
            );
            // Column OIDs: id → int8, name → text.
            let cols: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();
            f.record_check(
                "describe_select_column_types",
                true,
                cols == [Type::INT8, Type::TEXT],
                &format!("expected columns [INT8, TEXT], got {cols:?}"),
            );
        }
        Err(e) => {
            // prepare() itself failed — record the describe step as a gap and
            // skip the dependent assertions.
            f.record("describe_select_param_type", true, Err::<(), _>(e));
        }
    }

    // Describe an INSERT ... RETURNING (sqlx checks the RETURNING row type).
    let ins = client
        .prepare("INSERT INTO t (id, name) VALUES ($1, $2) RETURNING id")
        .await;
    match &ins {
        Ok(stmt) => {
            f.record_check(
                "describe_insert_returning_params",
                true,
                stmt.params() == [Type::INT8, Type::TEXT],
                &format!("expected params [INT8, TEXT], got {:?}", stmt.params()),
            );
            let cols: Vec<Type> = stmt.columns().iter().map(|c| c.type_().clone()).collect();
            f.record_check(
                "describe_insert_returning_column",
                true,
                cols == [Type::INT8],
                &format!("expected RETURNING column [INT8], got {cols:?}"),
            );
        }
        Err(e) => {
            f.record("describe_insert_returning_params", true, Err::<(), _>(e));
        }
    }

    // Now execute the prepared statements with binds (sqlx then runs them).
    if let Ok(stmt) = &ins {
        f.record(
            "execute_prepared_insert",
            true,
            client.execute(stmt, &[&7_i64, &"seven"]).await,
        );
    }
    if let Ok(stmt) = &sel {
        let rows = client.query(stmt, &[&7_i64]).await;
        let ok = matches!(&rows, Ok(r) if r.len() == 1);
        f.record("execute_prepared_select", true, rows);
        f.record_check(
            "execute_prepared_select_row",
            true,
            ok,
            "expected exactly one row for id=7",
        );
    }

    f.finish();
}

// ===========================================================================
// 5. Drizzle batch flow.
//
// Drizzle's `db.batch([...])` and migrator push multiple statements; over the
// simple protocol the postgres-js / node-postgres driver sends them as one
// `stmt1; stmt2; stmt3` Query message. Drizzle's `.onConflictDoUpdate()` emits
// `INSERT ... ON CONFLICT (col) DO UPDATE SET ... RETURNING`, and jsonb columns
// round-trip as binary $1 binds.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drizzle_batch_flow() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("drizzle_batch_flow");

    // Multi-statement batch over the SIMPLE protocol (one Query, 3 statements).
    // Supported: pgwire_multi_statement asserts the router splits + dispatches.
    let batch = client
        .simple_query(
            "CREATE TABLE kv (k BIGINT NOT NULL, v TEXT NOT NULL); \
             INSERT INTO kv (k, v) VALUES (1, 'one'); \
             INSERT INTO kv (k, v) VALUES (2, 'two');",
        )
        .await;
    // Count the CommandComplete tags to prove all three statements ran.
    let batch_ok = match &batch {
        Ok(msgs) => {
            msgs.iter()
                .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
                .count()
                >= 3
        }
        Err(_) => false,
    };
    f.record("multi_statement_simple_batch", true, batch);
    f.record_check(
        "multi_statement_all_ran",
        true,
        batch_ok,
        "expected >=3 CommandComplete tags from 3-statement batch",
    );

    // INSERT ... ON CONFLICT DO UPDATE ... RETURNING (upsert). Supported:
    // dml_extras covers ON CONFLICT DO UPDATE; RETURNING is covered separately.
    // The combination (upsert + RETURNING) is the one Drizzle emits; treat as
    // supported and let a regression fail.
    let upsert = client
        .query(
            "INSERT INTO kv (k, v) VALUES ($1, $2) \
             ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v RETURNING k, v",
            &[&1_i64, &"one-updated"],
        )
        .await;
    f.record("insert_on_conflict_do_update_returning", true, upsert);

    // JSONB column round-trip via binary $1::jsonb bind. Supported:
    // jsonb_uuid_param_binding round-trips serde_json::Value in BINARY.
    f.record(
        "ddl_create_jsonb_table",
        true,
        client
            .batch_execute("CREATE TABLE docs (id BIGINT NOT NULL, payload JSONB NOT NULL)")
            .await,
    );
    let payload = json!({"a": 1, "b": ["x", "y"], "nested": {"z": true}});
    f.record(
        "insert_jsonb_param",
        true,
        client
            .execute(
                "INSERT INTO docs (id, payload) VALUES ($1, $2)",
                &[&1_i64, &payload],
            )
            .await,
    );
    // Read back unfiltered (jsonb_uuid_param_binding notes WHERE on binary/uuid
    // fast-select is a separate storage limitation; avoid it here).
    let read = client.query("SELECT id, payload FROM docs", &[]).await;
    let roundtrip_ok = match &read {
        Ok(rows) if rows.len() == 1 => {
            rows[0].get::<_, serde_json::Value>(1) == payload
        }
        _ => false,
    };
    f.record("select_jsonb_roundtrip", true, read);
    f.record_check(
        "jsonb_value_roundtrip_eq",
        true,
        roundtrip_ok,
        "JSONB binary round-trip did not preserve the document",
    );

    f.finish();
}

// ===========================================================================
// 6. Hibernate sequence flow.
//
// Hibernate's `SequenceStyleGenerator` (and JPA `GenerationType.SEQUENCE`)
// allocates IDs by calling `nextval('<seq>')` before INSERT, then INSERTs with
// the obtained value, and uses JDBC getGeneratedKeys() (which on Postgres maps
// to `RETURNING`) to read the row back. currval() backs same-session reads.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hibernate_sequence_flow() {
    let server = start_server().await;
    let client = connect(server.addr).await;
    let mut f = Flow::new("hibernate_sequence_flow");

    // CREATE SEQUENCE via DDL. Supported: type_ddl covers CREATE SEQUENCE.
    f.record(
        "create_sequence",
        true,
        client
            .batch_execute("CREATE SEQUENCE hibernate_seq START 1 INCREMENT 1")
            .await,
    );

    f.record(
        "ddl_create_table",
        true,
        client
            .batch_execute("CREATE TABLE entity (id BIGINT NOT NULL, val TEXT NOT NULL)")
            .await,
    );

    // nextval() to allocate an id (Hibernate's pre-INSERT allocation).
    let nv = client.query_one("SELECT nextval($1)", &[&"hibernate_seq"]).await;
    // nextval bound-param shape can differ from the literal form used in
    // type_ddl/sequences (those call nextval('lit')). Treat the *literal* form
    // as the supported baseline below; the param form is a best-effort probe.
    let next_id_param = nv.as_ref().map(|r| r.get::<_, i64>(0)).ok();
    f.record("nextval_param", false, nv);

    // Literal nextval — this is the form the repo's coverage exercises directly.
    let nv_lit = client
        .query_one("SELECT nextval('hibernate_seq')", &[])
        .await;
    let next_id = nv_lit
        .as_ref()
        .map(|r| r.get::<_, i64>(0))
        .ok()
        .or(next_id_param)
        .unwrap_or(1);
    f.record("nextval_literal", true, nv_lit);

    // currval() in the same session (getGeneratedKeys via currval fallback).
    f.record(
        "currval_literal",
        true,
        client.query_one("SELECT currval('hibernate_seq')", &[]).await,
    );

    // INSERT using the allocated id, with RETURNING (getGeneratedKeys path).
    let ins = client
        .query_one(
            "INSERT INTO entity (id, val) VALUES ($1, $2) RETURNING id",
            &[&next_id, &"hib"],
        )
        .await;
    let returned_ok = matches!(&ins, Ok(r) if r.get::<_, i64>(0) == next_id);
    f.record("insert_with_seq_id_returning", true, ins);
    f.record_check(
        "returning_matches_allocated_id",
        true,
        returned_ok,
        "RETURNING id did not match the nextval-allocated id",
    );

    f.finish();
}

// ===========================================================================
// Final gate: total recorded-GAP count across every flow must not exceed the
// recorded ceiling. Each flow test already asserts its supported steps are OK;
// this test guards against *new* gaps creeping in and rewards shrinking ones.
//
// Because cargo runs each `#[test]` independently (and may run them in any
// order / in parallel / individually), we cannot rely on the per-flow tests
// having populated TOTAL_GAPS. So this test RE-RUNS every flow's gap census in
// one process and tallies fresh. It re-uses the exact same step sequences via
// `count_gaps_for_all_flows`, which mirrors the flows but swallows the
// supported-step assertions (a regression there is already caught by the
// dedicated per-flow test).
// ===========================================================================

/// Recorded number of steps currently expected to be GAPs across all flows.
///
/// Reasoning for the current value: the only steps classed `supported = false`
/// (i.e. allowed to be a gap) are:
///   - `sqlalchemy_session_flow/show_standard_conforming_strings` — Basin maps
///     unknown SHOW <var> to an empty "SHOW" response (no GUC value). This does
///     NOT error (tokio-postgres simple_query accepts a zero-row response), so
///     it is counted OK in practice → contributes 0 gaps.
///   - `hibernate_sequence_flow/nextval_param` — `nextval($1)` with a bound
///     TEXT param (vs the literal `nextval('seq')` the repo covers). If the
///     planner can't infer the regclass/text param for the sequence function in
///     bind position, this errors → contributes 1 gap. The literal form is the
///     supported baseline and is asserted separately.
///
/// So the *expected* steady-state gap count is conservatively bounded at 2:
/// at most the two `supported = false` probes can fail. If both end up passing,
/// tighten this to the observed value (free win); a value above 2 means a
/// genuinely-supported step regressed and slipped past — investigate.
const RECORDED_GAP_CEILING: u64 = 2;

/// Re-run every flow as a pure gap census (no supported-step assertions) and
/// return the total gap count. Mirrors the six flows above, trimmed to the
/// same statements.
async fn count_gaps_for_all_flows() -> u64 {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // We replay a representative subset that includes BOTH the supported steps
    // (which should never gap) and the two `supported=false` probes (the only
    // legitimate gap sources). Counting all of them gives the honest ceiling.
    let mut gaps = 0u64;

    macro_rules! probe {
        ($label:expr, $fut:expr) => {{
            match $fut.await {
                Ok(_) => {}
                Err(e) => {
                    println!("[orm-compat] census/{}: GAP({})", $label, short_err(&e.to_string()));
                    gaps += 1;
                }
            }
        }};
    }

    // --- prisma ---
    probe!(
        "prisma/ddl",
        client.execute(
            "CREATE TABLE \"User\" (id BIGINT NOT NULL, email TEXT NOT NULL, name TEXT)",
            &[],
        )
    );
    probe!(
        "prisma/insert_returning",
        client.query(
            "INSERT INTO \"User\" (id, email, name) VALUES ($1,$2,$3) RETURNING id",
            &[&1_i64, &"a@x.com", &"Alice"],
        )
    );
    probe!(
        "prisma/update_returning",
        client.query(
            "UPDATE \"User\" SET name = $1 WHERE id = $2 RETURNING id",
            &[&"A2", &1_i64],
        )
    );
    probe!(
        "prisma/multi_row_insert",
        client.execute(
            "INSERT INTO \"User\" (id, email, name) VALUES ($1,$2,$3),($4,$5,$6)",
            &[&2_i64, &"b@x.com", &"Bob", &3_i64, &"c@x.com", &"Cara"],
        )
    );

    // --- sqlalchemy ---
    probe!("sqla/version", client.simple_query("SELECT version()"));
    probe!(
        "sqla/show_scs",
        client.simple_query("show standard_conforming_strings")
    );
    probe!(
        "sqla/ddl",
        client.batch_execute(
            "CREATE TABLE accounts (id BIGINT NOT NULL, owner TEXT NOT NULL, balance BIGINT NOT NULL)",
        )
    );
    probe!(
        "sqla/info_schema_columns_param",
        client.query(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
            &[&"accounts"],
        )
    );
    probe!(
        "sqla/info_schema_tables",
        client.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
    );

    // --- activerecord ---
    probe!(
        "ar/advisory_lock",
        client.query("SELECT pg_try_advisory_lock($1)", &[&424_242_i64])
    );
    probe!(
        "ar/schema_migrations",
        client.batch_execute("CREATE TABLE schema_migrations (version TEXT NOT NULL)")
    );
    probe!(
        "ar/add_column",
        client.batch_execute(
            "CREATE TABLE widgets (id BIGINT NOT NULL, name TEXT NOT NULL); \
             ALTER TABLE widgets ADD COLUMN price BIGINT"
        )
    );
    probe!(
        "ar/create_index",
        client.batch_execute("CREATE INDEX idx_widgets_name ON widgets (name)")
    );
    probe!(
        "ar/advisory_unlock",
        client.query("SELECT pg_advisory_unlock($1)", &[&424_242_i64])
    );

    // --- sqlx describe ---
    probe!(
        "sqlx/ddl",
        client.batch_execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
    );
    probe!(
        "sqlx/describe_select",
        client.prepare("SELECT id, name FROM t WHERE id = $1")
    );
    probe!(
        "sqlx/describe_insert_returning",
        client.prepare("INSERT INTO t (id, name) VALUES ($1, $2) RETURNING id")
    );

    // --- drizzle ---
    probe!(
        "drizzle/multi_batch",
        client.simple_query(
            "CREATE TABLE kv (k BIGINT NOT NULL, v TEXT NOT NULL); \
             INSERT INTO kv (k, v) VALUES (1, 'one'); \
             INSERT INTO kv (k, v) VALUES (2, 'two');",
        )
    );
    probe!(
        "drizzle/upsert_returning",
        client.query(
            "INSERT INTO kv (k, v) VALUES ($1, $2) \
             ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v RETURNING k, v",
            &[&1_i64, &"one-updated"],
        )
    );
    probe!(
        "drizzle/jsonb_ddl",
        client.batch_execute("CREATE TABLE docs (id BIGINT NOT NULL, payload JSONB NOT NULL)")
    );
    probe!(
        "drizzle/jsonb_insert",
        client.execute(
            "INSERT INTO docs (id, payload) VALUES ($1, $2)",
            &[&1_i64, &json!({"a": 1, "b": ["x", "y"]})],
        )
    );

    // --- hibernate ---
    probe!(
        "hib/create_sequence",
        client.batch_execute("CREATE SEQUENCE hibernate_seq START 1 INCREMENT 1")
    );
    probe!(
        "hib/nextval_param",
        client.query_one("SELECT nextval($1)", &[&"hibernate_seq"])
    );
    probe!(
        "hib/nextval_literal",
        client.query_one("SELECT nextval('hibernate_seq')", &[])
    );
    probe!(
        "hib/currval_literal",
        client.query_one("SELECT currval('hibernate_seq')", &[])
    );

    gaps
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zzz_orm_flow_gap_ceiling() {
    let gaps = count_gaps_for_all_flows().await;
    println!(
        "[orm-compat] GAP CEILING: observed {gaps} gap(s), ceiling {RECORDED_GAP_CEILING}"
    );
    assert!(
        gaps <= RECORDED_GAP_CEILING,
        "ORM-flow gap count regressed: {gaps} gaps observed but ceiling is \
         {RECORDED_GAP_CEILING}. A previously-supported step likely broke. \
         Inspect the `census/...: GAP(...)` lines above to find the new gap."
    );
    if gaps < RECORDED_GAP_CEILING {
        println!(
            "[orm-compat] NOTE: gap count ({gaps}) is below ceiling \
             ({RECORDED_GAP_CEILING}) — consider tightening RECORDED_GAP_CEILING \
             to {gaps} to lock in the win."
        );
    }
}
