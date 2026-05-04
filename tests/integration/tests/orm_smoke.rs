//! ORM-style compatibility smoke test.
//!
//! Goal: drive seven representative query patterns that real ORMs (Diesel,
//! SeaORM, Prisma, SQLAlchemy, Drizzle, ActiveRecord) emit through the
//! Postgres extended-query protocol, against a live `basin-server`. We run
//! them via raw `tokio-postgres` because every Rust/JS/Python ORM that
//! targets Postgres ultimately funnels through the same Parse/Bind/Execute
//! shape — if these patterns work here, the protocol surface is ORM-ready.
//!
//! The test is *survey-grade*. It does not panic on individual failures: it
//! collects them, prints a summary table, and writes a viability JSON for
//! the dashboard. The companion document is `docs/sql-compatibility.md`.
//!
//! Note: Diesel-async and SeaORM were considered as direct deps but pulling
//! them into `tests/integration` doubles compile time and adds nothing the
//! protocol-level patterns below don't already exercise. Each pattern is
//! annotated with the ORM idiom it mirrors so the gap report stays grounded.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::NoTls;

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
    });
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), TenantId::new());
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
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

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user={user} password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

#[derive(Debug)]
struct PatternResult {
    id: &'static str,
    description: &'static str,
    orm_analogue: &'static str,
    passed: bool,
    error: Option<String>,
}

impl PatternResult {
    fn pass(id: &'static str, description: &'static str, orm_analogue: &'static str) -> Self {
        Self {
            id,
            description,
            orm_analogue,
            passed: true,
            error: None,
        }
    }
    fn fail(
        id: &'static str,
        description: &'static str,
        orm_analogue: &'static str,
        err: impl ToString,
    ) -> Self {
        Self {
            id,
            description,
            orm_analogue,
            passed: false,
            error: Some(err.to_string()),
        }
    }
    /// Capture the full chain of `tokio_postgres::Error` so the verbatim
    /// server message lands in the report. `Display` on `Error` collapses
    /// to "db error" for `DbError` variants.
    fn fail_pg(
        id: &'static str,
        description: &'static str,
        orm_analogue: &'static str,
        err: tokio_postgres::Error,
    ) -> Self {
        Self::fail(id, description, orm_analogue, render_pg_error(&err))
    }
}

fn render_pg_error(err: &tokio_postgres::Error) -> String {
    if let Some(db) = err.as_db_error() {
        format!(
            "{} {}: {}",
            db.severity(),
            db.code().code(),
            db.message()
        )
    } else {
        // Walk the source chain — tokio-postgres wraps the underlying I/O
        // or codec failure here.
        let mut out = err.to_string();
        let mut src: Option<&dyn std::error::Error> = std::error::Error::source(err);
        while let Some(s) = src {
            out.push_str(": ");
            out.push_str(&s.to_string());
            src = s.source();
        }
        out
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orm_compat_smoke() {
    let server = start_server().await;
    let client = connect(server.addr, "alice").await;
    let mut results: Vec<PatternResult> = Vec::new();

    // Set up the canonical scratch table that the first few patterns use.
    // CREATE failure here is fatal: every pattern below needs the table.
    let create_setup = client
        .execute(
            "CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL, active BOOLEAN NOT NULL)",
            &[],
        )
        .await;
    if let Err(e) = create_setup {
        // Surface this verbatim and bail — without `t` nothing else is
        // meaningful. The dashboard JSON still lands so the failure is
        // visible in CI.
        eprintln!("setup CREATE TABLE t failed: {e}");
        emit_report(&[PatternResult::fail_pg(
            "setup",
            "CREATE TABLE t",
            "Schema migration",
            e,
        )]);
        return;
    }

    // 1) Multi-row INSERT with parameters. Diesel `insert_into(t).values(&vec)`
    //    and ActiveRecord `insert_all` collapse a list of records into one
    //    statement with N parameter slots. Tests that the placeholder scanner
    //    handles repeated `($1, $2), ($3, $4), ...` shapes.
    {
        let res = client
            .execute(
                "INSERT INTO t (id, name, active) VALUES \
                  ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
                &[
                    &1i64, &"alpha", &true,
                    &2i64, &"beta", &false,
                    &3i64, &"gamma", &true,
                ],
            )
            .await;
        results.push(match res {
            Ok(_) => PatternResult::pass(
                "multirow_insert",
                "INSERT … VALUES ($1,$2,$3), ($4,$5,$6), ($7,$8,$9)",
                "Diesel `insert_into(t).values(&vec)`; ActiveRecord `insert_all`",
            ),
            Err(e) => PatternResult::fail_pg(
                "multirow_insert",
                "INSERT … VALUES ($1,$2,$3), ($4,$5,$6), ($7,$8,$9)",
                "Diesel `insert_into(t).values(&vec)`; ActiveRecord `insert_all`",
                e,
            ),
        });
    }

    // 2) Prepared statement reuse. `client.prepare` once, `query` many times.
    //    This mirrors every connection-pool ORM (HikariCP, sqlx, asyncpg)
    //    that caches `Statement` handles keyed by SQL text. We do 100 binds
    //    against a pre-existing row to confirm the cached handle survives
    //    repeated Bind/Execute/Sync cycles.
    {
        let prep = client.prepare("SELECT id FROM t WHERE id = $1").await;
        match prep {
            Err(e) => results.push(PatternResult::fail_pg(
                "prepared_reuse",
                "prepare once + query 100x",
                "Connection-pool prepared-statement cache (sqlx, asyncpg, HikariCP)",
                e,
            )),
            Ok(stmt) => {
                let mut last_err: Option<String> = None;
                let mut got_rows = 0usize;
                for i in 0..100 {
                    // Cycle through the three rows we inserted in pattern 1.
                    let needle: i64 = (i % 3) + 1;
                    match client.query(&stmt, &[&needle]).await {
                        Ok(rs) => got_rows += rs.len(),
                        Err(e) => {
                            last_err = Some(render_pg_error(&e));
                            break;
                        }
                    }
                }
                let res = if let Some(e) = last_err {
                    PatternResult::fail(
                        "prepared_reuse",
                        "prepare once + query 100x",
                        "Connection-pool prepared-statement cache (sqlx, asyncpg, HikariCP)",
                        e,
                    )
                } else if got_rows == 100 {
                    PatternResult::pass(
                        "prepared_reuse",
                        "prepare once + query 100x",
                        "Connection-pool prepared-statement cache (sqlx, asyncpg, HikariCP)",
                    )
                } else {
                    PatternResult::fail(
                        "prepared_reuse",
                        "prepare once + query 100x",
                        "Connection-pool prepared-statement cache (sqlx, asyncpg, HikariCP)",
                        format!("expected 100 rows across 100 binds, got {got_rows}"),
                    )
                };
                results.push(res);
            }
        }
    }

    // 3) WHERE with multiple parameter types. Every ORM filter chain
    //    (`User.where(id: 1, name: "x", active: true)`) ends up here.
    //    We pin one of the rows from pattern 1 and require exactly one match.
    {
        let res = client
            .query(
                "SELECT id, name, active FROM t WHERE id = $1 AND name = $2 AND active = $3",
                &[&1i64, &"alpha", &true],
            )
            .await;
        results.push(match res {
            Ok(rows) if rows.len() == 1 => {
                let id: i64 = rows[0].get(0);
                let name: &str = rows[0].get(1);
                let active: bool = rows[0].get(2);
                if id == 1 && name == "alpha" && active {
                    PatternResult::pass(
                        "mixed_where",
                        "WHERE id = $1 AND name = $2 AND active = $3 (i64, &str, bool)",
                        "ActiveRecord `where(id:, name:, active:)`; Prisma `findMany({where:{...}})`",
                    )
                } else {
                    PatternResult::fail(
                        "mixed_where",
                        "WHERE id = $1 AND name = $2 AND active = $3 (i64, &str, bool)",
                        "ActiveRecord `where(id:, name:, active:)`; Prisma `findMany({where:{...}})`",
                        format!("row mismatch: id={id} name={name} active={active}"),
                    )
                }
            }
            Ok(rows) => PatternResult::fail(
                "mixed_where",
                "WHERE id = $1 AND name = $2 AND active = $3 (i64, &str, bool)",
                "ActiveRecord `where(id:, name:, active:)`; Prisma `findMany({where:{...}})`",
                format!("expected 1 row, got {}", rows.len()),
            ),
            Err(e) => PatternResult::fail_pg(
                "mixed_where",
                "WHERE id = $1 AND name = $2 AND active = $3 (i64, &str, bool)",
                "ActiveRecord `where(id:, name:, active:)`; Prisma `findMany({where:{...}})`",
                e,
            ),
        });
    }

    // 4) NULL parameters. ORM "optional column" support — `Option<T>` in
    //    Diesel, `Optional<T>` in jOOQ, `null` in JS — flows down to
    //    `Bind` with a -1 length param. We need a nullable column for this,
    //    so use a separate table where `note` is nullable.
    {
        let create = client
            .execute(
                "CREATE TABLE notes (id BIGINT NOT NULL, note TEXT)",
                &[],
            )
            .await;
        match create {
            Err(e) => results.push(PatternResult::fail(
                "null_param",
                "INSERT INTO notes VALUES ($1, $2) with Option::<&str>::None",
                "Diesel `Option<String>`; Prisma optional field",
                format!("create notes: {}", render_pg_error(&e)),
            )),
            Ok(_) => {
                let none: Option<&str> = None;
                let ins = client
                    .execute(
                        "INSERT INTO notes (id, note) VALUES ($1, $2)",
                        &[&42i64, &none],
                    )
                    .await;
                match ins {
                    Err(e) => results.push(PatternResult::fail_pg(
                        "null_param",
                        "INSERT INTO notes VALUES ($1, $2) with Option::<&str>::None",
                        "Diesel `Option<String>`; Prisma optional field",
                        e,
                    )),
                    Ok(_) => {
                        let sel = client
                            .query("SELECT id, note FROM notes WHERE id = $1", &[&42i64])
                            .await;
                        let outcome = match sel {
                            Ok(rs) if rs.len() == 1 => {
                                let got: Option<&str> = rs[0].get(1);
                                match got {
                                    None => Ok(()),
                                    Some(v) => {
                                        Err(format!("expected NULL, got Some({v:?})"))
                                    }
                                }
                            }
                            Ok(rs) => Err(format!("expected 1 row, got {}", rs.len())),
                            Err(e) => Err(render_pg_error(&e)),
                        };
                        results.push(match outcome {
                            Ok(()) => PatternResult::pass(
                                "null_param",
                                "INSERT INTO notes VALUES ($1, $2) with Option::<&str>::None",
                                "Diesel `Option<String>`; Prisma optional field",
                            ),
                            Err(e) => PatternResult::fail(
                                "null_param",
                                "INSERT INTO notes VALUES ($1, $2) with Option::<&str>::None",
                                "Diesel `Option<String>`; Prisma optional field",
                                e,
                            ),
                        });
                    }
                }
            }
        }
    }

    // 5) Parameter in LIMIT. Pagination — `.limit(?)` in Diesel, `take(n)`
    //    in Prisma, `LIMIT $1` in raw SeaQL. Postgres allows it; ours might
    //    not infer the int type yet. Document either outcome.
    {
        let res = client
            .query("SELECT id FROM t LIMIT $1", &[&2i64])
            .await;
        results.push(match res {
            Ok(rows) if rows.len() == 2 => PatternResult::pass(
                "limit_param",
                "SELECT … LIMIT $1",
                "Diesel `.limit(?)`; Prisma `take(n)`; SeaORM `limit(n)`",
            ),
            Ok(rows) => PatternResult::fail(
                "limit_param",
                "SELECT … LIMIT $1",
                "Diesel `.limit(?)`; Prisma `take(n)`; SeaORM `limit(n)`",
                format!("expected 2 rows, got {}", rows.len()),
            ),
            Err(e) => PatternResult::fail_pg(
                "limit_param",
                "SELECT … LIMIT $1",
                "Diesel `.limit(?)`; Prisma `take(n)`; SeaORM `limit(n)`",
                e,
            ),
        });
    }

    // 6) String containing a single quote. Tests parameter substitution
    //    quoting — if Basin substitutes `$1` textually with the literal,
    //    `O'Brien` would break the SQL parser. Real ORMs lean on the
    //    binary-text protocol to dodge this; we want to confirm Basin
    //    does too.
    {
        let value = "O'Brien";
        let ins = client
            .execute(
                "INSERT INTO t (id, name, active) VALUES ($1, $2, $3)",
                &[&999i64, &value, &true],
            )
            .await;
        let outcome = match ins {
            Err(e) => Err(format!("insert: {}", render_pg_error(&e))),
            Ok(_) => match client
                .query("SELECT name FROM t WHERE id = $1", &[&999i64])
                .await
            {
                Err(e) => Err(format!("select: {}", render_pg_error(&e))),
                Ok(rs) if rs.len() == 1 => {
                    let got: &str = rs[0].get(0);
                    if got == value {
                        Ok(())
                    } else {
                        Err(format!("expected {value:?}, got {got:?}"))
                    }
                }
                Ok(rs) => Err(format!("expected 1 row, got {}", rs.len())),
            },
        };
        results.push(match outcome {
            Ok(()) => PatternResult::pass(
                "single_quote",
                "INSERT … VALUES ($1) with \"O'Brien\"",
                "Any text-input form bound through ORM-style $1 placeholders",
            ),
            Err(e) => PatternResult::fail(
                "single_quote",
                "INSERT … VALUES ($1) with \"O'Brien\"",
                "Any text-input form bound through ORM-style $1 placeholders",
                e,
            ),
        });
    }

    // 7) Bytea round-trip. Required by JSONB-blob payloads, file uploads,
    //    cryptographic signatures stored alongside rows. Mirrors `Vec<u8>`
    //    in Diesel and `Buffer` in Prisma's `Bytes` type.
    {
        let create = client
            .execute("CREATE TABLE blobs (id BIGINT NOT NULL, data BYTEA NOT NULL)", &[])
            .await;
        match create {
            Err(e) => results.push(PatternResult::fail(
                "bytea_roundtrip",
                "INSERT/SELECT BYTEA via $1",
                "Diesel `Vec<u8>`; Prisma `Bytes`",
                format!("create blobs: {}", render_pg_error(&e)),
            )),
            Ok(_) => {
                let payload: Vec<u8> = vec![0u8, 1, 2, 255];
                let ins = client
                    .execute(
                        "INSERT INTO blobs (id, data) VALUES ($1, $2)",
                        &[&7i64, &payload],
                    )
                    .await;
                let outcome = match ins {
                    Err(e) => Err(format!("insert: {}", render_pg_error(&e))),
                    Ok(_) => match client
                        .query("SELECT data FROM blobs WHERE id = $1", &[&7i64])
                        .await
                    {
                        Err(e) => Err(format!("select: {}", render_pg_error(&e))),
                        Ok(rs) if rs.len() == 1 => {
                            let got: Vec<u8> = rs[0].get::<_, Vec<u8>>(0);
                            if got == payload {
                                Ok(())
                            } else {
                                Err(format!("byte mismatch: expected {payload:?}, got {got:?}"))
                            }
                        }
                        Ok(rs) => Err(format!("expected 1 row, got {}", rs.len())),
                    },
                };
                results.push(match outcome {
                    Ok(()) => PatternResult::pass(
                        "bytea_roundtrip",
                        "INSERT/SELECT BYTEA via $1",
                        "Diesel `Vec<u8>`; Prisma `Bytes`",
                    ),
                    Err(e) => PatternResult::fail(
                        "bytea_roundtrip",
                        "INSERT/SELECT BYTEA via $1",
                        "Diesel `Vec<u8>`; Prisma `Bytes`",
                        e,
                    ),
                });
            }
        }
    }

    // Summary table. Fixed-width-ish so it reads clean in `cargo test --
    // --nocapture`.
    eprintln!();
    eprintln!("=== ORM-compat survey ===");
    eprintln!("{:<20} {:<6} description", "id", "result");
    eprintln!("{}", "-".repeat(72));
    for r in &results {
        let tag = if r.passed { "PASS" } else { "FAIL" };
        eprintln!("{:<20} {:<6} {}", r.id, tag, r.description);
        if let Some(e) = &r.error {
            // Postgres error strings can contain newlines (multi-line server
            // notices). Indent the continuation lines so the table still
            // reads.
            for (i, line) in e.lines().enumerate() {
                let prefix = if i == 0 { "    error: " } else { "           " };
                eprintln!("{prefix}{line}");
            }
        }
    }
    eprintln!();

    emit_report(&results);
}

fn emit_report(results: &[PatternResult]) {
    let total = results.len() as u32;
    let passed = results.iter().filter(|r| r.passed).count() as u32;
    let ratio = if total == 0 {
        0.0
    } else {
        passed as f64 / total as f64
    };
    let bar = BarOp::ge(0.85);
    let pass = bar.evaluate(ratio);

    let detail_rows: Vec<serde_json::Value> = results
        .iter()
        .map(|r| {
            json!({
                "id": r.id,
                "description": r.description,
                "orm_analogue": r.orm_analogue,
                "passed": r.passed,
                "error": r.error,
            })
        })
        .collect();

    report_viability(
        "orm_compat",
        "ORM-style query patterns work end-to-end",
        "Seven representative query shapes that real ORMs (Diesel, SeaORM, Prisma, ActiveRecord, SQLAlchemy) emit through the Postgres extended-query protocol succeed against Basin. Failure mode is a clear protocol error, not a hang.",
        pass,
        PrimaryMetric {
            label: "Representative ORM patterns that succeed".into(),
            value: ratio,
            unit: "fraction".into(),
            bar,
        },
        json!({
            "passed": passed,
            "total": total,
            "driver": "tokio-postgres 0.7",
            "patterns": detail_rows,
        }),
    );
}
