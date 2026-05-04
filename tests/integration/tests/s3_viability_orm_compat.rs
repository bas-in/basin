//! S3 port of `orm_smoke.rs`.
//!
//! Same seven ORM-style query patterns, but the in-process Basin server is
//! backed by real-S3 `Storage`. Every CREATE / INSERT / SELECT touches an
//! S3-resident parquet file (or its catalog metadata), so this card surfaces
//! ORM compatibility against a cloud-resident table.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::{ServerConfig, StaticTenantResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio_postgres::NoTls;

const TEST_NAME: &str = "s3_viability_orm_compat";

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _cleanup: CleanupOnDrop,
}

async fn start_server(
    s3_cfg: &basin_integration_tests::test_config::S3Config,
) -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
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
        _cleanup: cleanup,
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
        format!("{} {}: {}", db.severity(), db.code().code(), db.message())
    } else {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_orm_compat() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let server = start_server(&s3_cfg).await;
    let client = connect(server.addr, "alice").await;
    let mut results: Vec<PatternResult> = Vec::new();

    let create_setup = client
        .execute(
            "CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL, active BOOLEAN NOT NULL)",
            &[],
        )
        .await;
    if let Err(e) = create_setup {
        eprintln!("setup CREATE TABLE t failed: {e}");
        emit_report(
            &[PatternResult::fail_pg(
                "setup",
                "CREATE TABLE t",
                "Schema migration",
                e,
            )],
            &s3_cfg,
        );
        return;
    }

    // 1) Multi-row INSERT
    {
        let res = client
            .execute(
                "INSERT INTO t (id, name, active) VALUES \
                  ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
                &[
                    &1i64, &"alpha", &true, &2i64, &"beta", &false, &3i64, &"gamma", &true,
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

    // 2) Prepared statement reuse (100x).
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

    // 3) WHERE with multiple parameter types.
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

    // 4) NULL parameters.
    {
        let create = client
            .execute("CREATE TABLE notes (id BIGINT NOT NULL, note TEXT)", &[])
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
                                    Some(v) => Err(format!("expected NULL, got Some({v:?})")),
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

    // 5) Parameter in LIMIT.
    {
        let res = client.query("SELECT id FROM t LIMIT $1", &[&2i64]).await;
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

    // 6) String containing single quote.
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

    // 7) Bytea round-trip.
    {
        let create = client
            .execute(
                "CREATE TABLE blobs (id BIGINT NOT NULL, data BYTEA NOT NULL)",
                &[],
            )
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
                                Err(format!(
                                    "byte mismatch: expected {payload:?}, got {got:?}"
                                ))
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

    eprintln!();
    eprintln!("=== ORM-compat survey (real S3) ===");
    eprintln!("{:<20} {:<6} description", "id", "result");
    eprintln!("{}", "-".repeat(72));
    for r in &results {
        let tag = if r.passed { "PASS" } else { "FAIL" };
        eprintln!("{:<20} {:<6} {}", r.id, tag, r.description);
        if let Some(e) = &r.error {
            for (i, line) in e.lines().enumerate() {
                let prefix = if i == 0 { "    error: " } else { "           " };
                eprintln!("{prefix}{line}");
            }
        }
    }
    eprintln!();

    emit_report(&results, &s3_cfg);
}

fn emit_report(
    results: &[PatternResult],
    s3_cfg: &basin_integration_tests::test_config::S3Config,
) {
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

    report_real_viability(
        "orm_compat",
        "ORM-style query patterns work end-to-end (real S3)",
        "Seven representative query shapes that real ORMs (Diesel, SeaORM, Prisma, ActiveRecord, SQLAlchemy) emit through the Postgres extended-query protocol succeed against Basin with real-S3 storage. Failure mode is a clear protocol error, not a hang.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket.clone(),
        }),
    );
}
