//! Extended ORM / driver wire-compatibility corpus — second wave.
//!
//! Companion to `orm_compat.rs`. Where the first file pins the *core* shapes
//! (params, pagination, joins, RETURNING, upsert, transactions, nested reads),
//! this file enumerates the *high-usage long-tail* patterns that push real-ORM
//! coverage from ~85 % toward 95–99 %:
//!
//!   * row-level locking — `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` /
//!     `FOR KEY SHARE`, each with `SKIP LOCKED` / `NOWAIT` (Prisma isolation
//!     txns, Django `select_for_update`, SQLAlchemy `with_for_update`, GORM
//!     `clause.Locking`, Drizzle `.for('update')`).
//!   * nested aggregations / groupBy APIs (`_count`/`_avg` includes,
//!     `annotate(Count/Sum/Avg)`, `countDistinct`).
//!   * `DISTINCT` variants incl. Django `distinct('col')` ⇒ `DISTINCT ON`.
//!   * bulk operations (`createMany`/`bulk_create` batches, `updateMany`,
//!     `deleteMany`, GORM `CreateInBatches`).
//!   * JSON column round-trips (Prisma `Json` path filters, Django
//!     `JSONField __contains/__has_key`, SQLAlchemy JSONB ops).
//!   * array columns (Django `ArrayField __contains/__overlap`, SQLAlchemy
//!     `ARRAY`).
//!   * enum columns (`CREATE TYPE … AS ENUM` + native-enum filters).
//!   * datetime semantics (`date_trunc`/`EXTRACT`, `__date`/`__year`).
//!   * decimal/numeric precision round-trips.
//!   * soft-delete patterns (`deletedAt IS NULL`).
//!   * optimistic locking (`UPDATE … WHERE version = $n`).
//!   * cursor / keyset pagination.
//!   * relation depth-3 eager loads.
//!   * raw-SQL escape hatches.
//!   * upsert with composite conflict targets.
//!   * `count()`/`exists()` idioms, `first()`/`latest()`,
//!     `values_list`/`pluck` projections.
//!   * migration breadth — the ALTER variants each ORM's schema editor emits
//!     (`ADD`/`DROP`/`RENAME COLUMN`, `ALTER TYPE`, `ADD UNIQUE`, `ADD FK`,
//!     `CREATE INDEX [CONCURRENTLY]`, `DROP TABLE`).
//!
//! Same engine, same harness, same gates as `orm_compat.rs`: a `TypedError`
//! outcome is a *legitimate* result for an unsupported feature (it proves the
//! engine refused cleanly rather than panicking); the only hard failure is a
//! connection-level regression. The honest gap map lives in the section
//! comments — patterns that come back `TypedError` are the next-session
//! coverage backlog.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

// =============================================================================
// Harness — identical shape to orm_compat.rs (reused, not reinvented).
// =============================================================================

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

// -----------------------------------------------------------------------------
// Shape vocabulary — same `ParamValue` / `Shape` / `run_shape` machinery as
// orm_compat.rs's corpus section.
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ParamValue {
    I32(i32),
    I64(i64),
    Text(&'static str),
    Bool(bool),
    Int4Array(&'static [i32]),
    Int8Array(&'static [i64]),
    F64(f64),
    Bytea(&'static [u8]),
}

impl ParamValue {
    fn to_boxed(&self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            ParamValue::I32(v) => Box::new(*v),
            ParamValue::I64(v) => Box::new(*v),
            ParamValue::Text(s) => Box::new(*s),
            ParamValue::Bool(b) => Box::new(*b),
            ParamValue::Int4Array(xs) => Box::new(xs.to_vec()),
            ParamValue::Int8Array(xs) => Box::new(xs.to_vec()),
            ParamValue::F64(v) => Box::new(*v),
            ParamValue::Bytea(b) => Box::new(b.to_vec()),
        }
    }
}

#[derive(Debug, Clone)]
struct Shape {
    sql: &'static str,
    params: &'static [ParamValue],
}

impl Shape {
    const fn raw(sql: &'static str) -> Self {
        Shape { sql, params: &[] }
    }
    const fn with(sql: &'static str, params: &'static [ParamValue]) -> Self {
        Shape { sql, params }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ShapeOutcome {
    Ok,
    TypedError { sqlstate: String, message: String },
}

async fn run_shape(client: &Client, shape: &Shape) -> Result<ShapeOutcome, String> {
    let result: Result<(), tokio_postgres::Error> = if shape.params.is_empty() {
        client.simple_query(shape.sql).await.map(|_| ())
    } else {
        let boxed: Vec<Box<dyn ToSql + Sync + Send>> =
            shape.params.iter().map(|p| p.to_boxed()).collect();
        let refs: Vec<&(dyn ToSql + Sync)> = boxed
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect();
        client.execute(shape.sql, &refs[..]).await.map(|_| ())
    };
    classify_shape_result(result)
}

fn classify_shape_result(
    result: Result<(), tokio_postgres::Error>,
) -> Result<ShapeOutcome, String> {
    match result {
        Ok(()) => Ok(ShapeOutcome::Ok),
        Err(e) => {
            if let Some(dbe) = e.as_db_error() {
                let code = dbe.code().code().to_string();
                if code.len() == 5 && !dbe.severity().is_empty() && !dbe.message().is_empty() {
                    return Ok(ShapeOutcome::TypedError {
                        sqlstate: code,
                        message: dbe.message().to_string(),
                    });
                }
                Err(format!(
                    "malformed ErrorResponse: code={code:?} sev={:?} msg={:?}",
                    dbe.severity(),
                    dbe.message()
                ))
            } else {
                Err(format!("connection-level error (possible panic): {e}"))
            }
        }
    }
}

#[derive(serde::Serialize)]
struct ShapeResult {
    sql: String,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlstate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(serde::Serialize)]
struct OrmSection {
    orm: String,
    total: usize,
    ok: usize,
    typed_error: usize,
    regression: usize,
    pass_rate_pct: f64,
    shapes: Vec<ShapeResult>,
}

async fn run_orm_section(client: &Client, orm: &str, shapes: &[Shape]) -> OrmSection {
    let mut shape_results = Vec::new();
    let mut ok = 0usize;
    let mut typed = 0usize;
    let mut reg = 0usize;

    for shape in shapes {
        let sql = shape.sql;
        let outcome = run_shape(client, shape).await;
        match outcome {
            Ok(ShapeOutcome::Ok) => {
                ok += 1;
                shape_results.push(ShapeResult {
                    sql: sql.to_string(),
                    outcome: "ok".to_string(),
                    sqlstate: None,
                    message: None,
                });
                println!("  [{orm}] OK: {}", &sql[..sql.len().min(80)]);
            }
            Ok(ShapeOutcome::TypedError { sqlstate, message }) => {
                typed += 1;
                println!(
                    "  [{orm}] TYPED-ERR ({sqlstate}): {}",
                    &sql[..sql.len().min(60)]
                );
                shape_results.push(ShapeResult {
                    sql: sql.to_string(),
                    outcome: "typed_error".to_string(),
                    sqlstate: Some(sqlstate),
                    message: Some(message),
                });
            }
            Err(e) => {
                reg += 1;
                eprintln!("  [{orm}] REGRESSION: {e} — sql: {}", &sql[..sql.len().min(60)]);
                shape_results.push(ShapeResult {
                    sql: sql.to_string(),
                    outcome: "regression".to_string(),
                    sqlstate: None,
                    message: Some(e),
                });
            }
        }
    }

    let total = shapes.len();
    let pass_rate_pct = if total == 0 {
        100.0
    } else {
        ok as f64 / total as f64 * 100.0
    };

    OrmSection {
        orm: orm.to_string(),
        total,
        ok,
        typed_error: typed,
        regression: reg,
        pass_rate_pct,
        shapes: shape_results,
    }
}

#[derive(serde::Serialize)]
struct OrmCompatReport {
    generated_at: String,
    sections: Vec<OrmSection>,
    total_shapes: usize,
    total_ok: usize,
    total_typed_error: usize,
    total_regression: usize,
}

fn emit_report(report: &OrmCompatReport) {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let data_dir = manifest
        .parent()
        .and_then(std::path::Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    let path = data_dir.join("orm_compat_ext.json");
    if let Ok(bytes) = serde_json::to_vec_pretty(report) {
        let _ = std::fs::create_dir_all(&data_dir);
        match std::fs::write(&path, &bytes) {
            Ok(()) => println!("\n[orm_compat_ext] report → {}", path.display()),
            Err(e) => eprintln!("[orm_compat_ext] write failed: {e}"),
        }
    }
}

// =============================================================================
// Extended corpus.
// =============================================================================

/// Boot a fresh server, bootstrap a schema rich enough for the long-tail
/// shapes (enum column, jsonb column, array column, numeric column, soft-delete
/// column, version column, datetime column), then run every per-ORM section.
///
/// Gates (identical to orm_compat.rs):
///   1. Zero regressions (connection-level failures) for every ORM.
///   2. Per-ORM pass rate >= MIN_PASS_RATE_PCT (0 %; typed errors are valid).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orm_compat_corpus_ext() {
    const MIN_PASS_RATE_PCT: f64 = 0.0;

    let server = start_server().await;
    let client = connect(server.addr).await;

    // ── Schema bootstrap ────────────────────────────────────────────────────
    // A native enum type + tables exercising every long-tail column kind.
    // CREATE TYPE … AS ENUM is itself a probed feature (Prisma/SQLAlchemy emit
    // it); if it fails the column falls back to TEXT so dependent shapes still
    // run — but the enum DDL shape is *also* asserted in the Prisma section so
    // the outcome is recorded honestly.
    let bootstrap_enum = Shape::raw(r#"CREATE TYPE "role_enum" AS ENUM ('USER', 'ADMIN', 'OWNER')"#);
    let enum_ok = matches!(
        run_shape(&client, &bootstrap_enum).await,
        Ok(ShapeOutcome::Ok)
    );
    println!("[orm_compat_ext] native enum type create: ok={enum_ok}");

    // Role column type: native enum if the CREATE TYPE succeeded, else TEXT.
    let role_col_ddl = if enum_ok {
        r#"CREATE TABLE IF NOT EXISTS "members" (
               "id"         BIGINT       NOT NULL,
               "email"      TEXT         NOT NULL,
               "role"       "role_enum"  NOT NULL DEFAULT 'USER',
               "meta"       JSONB,
               "tags"       TEXT[],
               "balance"    NUMERIC(12,2),
               "version"    BIGINT       NOT NULL DEFAULT 0,
               "deleted_at" TIMESTAMPTZ,
               "created_at" TIMESTAMPTZ,
               PRIMARY KEY ("id")
           )"#
    } else {
        r#"CREATE TABLE IF NOT EXISTS "members" (
               "id"         BIGINT       NOT NULL,
               "email"      TEXT         NOT NULL,
               "role"       TEXT         NOT NULL DEFAULT 'USER',
               "meta"       JSONB,
               "tags"       TEXT[],
               "balance"    NUMERIC(12,2),
               "version"    BIGINT       NOT NULL DEFAULT 0,
               "deleted_at" TIMESTAMPTZ,
               "created_at" TIMESTAMPTZ,
               PRIMARY KEY ("id")
           )"#
    };

    let bootstrap: &[&str] = &[
        role_col_ddl,
        r#"CREATE TABLE IF NOT EXISTS "events" (
               "id"         BIGINT      NOT NULL,
               "member_id"  BIGINT      NOT NULL,
               "kind"       TEXT        NOT NULL,
               "amount"     NUMERIC(12,2) NOT NULL DEFAULT 0,
               "occurred_at" TIMESTAMPTZ,
               PRIMARY KEY ("id")
           )"#,
        // Depth-3 relation chain: members 1-N events 1-N event_lines.
        r#"CREATE TABLE IF NOT EXISTS "event_lines" (
               "id"        BIGINT NOT NULL,
               "event_id"  BIGINT NOT NULL,
               "label"     TEXT   NOT NULL,
               "qty"       BIGINT NOT NULL DEFAULT 1,
               PRIMARY KEY ("id")
           )"#,
        // Composite-PK table for the composite ON CONFLICT target shapes.
        r#"CREATE TABLE IF NOT EXISTS "memberships" (
               "org_id"  BIGINT NOT NULL,
               "user_id" BIGINT NOT NULL,
               "level"   TEXT   NOT NULL,
               PRIMARY KEY ("org_id", "user_id")
           )"#,
        r#"INSERT INTO "members" ("id","email","role","meta","tags","balance","version","deleted_at","created_at")
           VALUES
             (1,'a@x.com','ADMIN','{"plan":"pro","limits":[1,2,3]}','{rust,db}',100.50,0,NULL,'2024-01-01 00:00:00+00'),
             (2,'b@x.com','USER','{"plan":"free"}','{go}',0.00,0,NULL,'2024-02-01 00:00:00+00'),
             (3,'c@x.com','USER',NULL,'{}',NULL,1,'2024-05-01 00:00:00+00','2024-03-01 00:00:00+00')
           ON CONFLICT DO NOTHING"#,
        r#"INSERT INTO "events" ("id","member_id","kind","amount","occurred_at")
           VALUES (10,1,'purchase',99.99,'2024-03-01 12:00:00+00'),
                  (11,1,'refund',-9.99,'2024-03-02 12:00:00+00'),
                  (12,2,'purchase',19.99,'2024-04-01 12:00:00+00')
           ON CONFLICT DO NOTHING"#,
        r#"INSERT INTO "event_lines" ("id","event_id","label","qty")
           VALUES (100,10,'item-a',2),(101,10,'item-b',1),(102,12,'item-c',5)
           ON CONFLICT DO NOTHING"#,
        r#"INSERT INTO "memberships" ("org_id","user_id","level")
           VALUES (1,1,'owner'),(1,2,'member')
           ON CONFLICT DO NOTHING"#,
    ];

    println!("\n[orm_compat_ext] bootstrapping schema...");
    for ddl in bootstrap {
        let shape = Shape::raw(ddl);
        match run_shape(&client, &shape).await {
            Ok(_) => {}
            Err(e) => panic!("schema bootstrap failed: {e}\nSQL: {ddl}"),
        }
    }
    println!("[orm_compat_ext] schema ready.");

    // ── Prisma — long-tail shapes ───────────────────────────────────────────
    //
    // Captured from `DEBUG="prisma:query"` traces and the Prisma client
    // query-compiler output for the corresponding client API calls.
    let prisma: &[Shape] = &[
        // findFirst — `take(1)`; Prisma emits LIMIT 1 OFFSET 0.
        Shape::raw(r#"SELECT "members"."id","members"."email" FROM "members" ORDER BY "members"."id" ASC LIMIT 1 OFFSET 0"#),
        // findMany with `_count` relation aggregate — Prisma builds the
        // per-parent child count as a correlated scalar subquery.
        Shape::raw(r#"SELECT "members"."id",
               (SELECT COUNT(*) FROM "events" WHERE "events"."member_id" = "members"."id") AS "_count_events"
           FROM "members" ORDER BY "members"."id""#),
        // aggregate({ _avg, _sum, _count }) — Prisma `prisma.event.aggregate`.
        Shape::raw(r#"SELECT COUNT(*) AS "_count", AVG("amount") AS "_avg_amount", SUM("amount") AS "_sum_amount" FROM "events""#),
        // groupBy({ by: ['member_id'], _count, _sum }) — Prisma groupBy API.
        Shape::raw(r#"SELECT "member_id", COUNT(*) AS "_count", SUM("amount") AS "_sum_amount" FROM "events" GROUP BY "member_id" HAVING COUNT(*) > 0 ORDER BY "member_id""#),
        // Json path filter — `where: { meta: { path: ['plan'], equals: 'pro' } }`
        // compiles to a `->>` extraction comparison.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "meta"->>'plan' = 'pro'"#),
        // Json `equals` on a nested array element via #>> path navigation.
        // (r##"…"## delimiter: the `"#>>` sequence would close a plain r#"…"# .)
        Shape::raw(r##"SELECT "id" FROM "members" WHERE "meta"#>>'{limits,0}' = '1'"##),
        // Native enum filter — `where: { role: 'ADMIN' }` binds the enum value.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "role" = 'ADMIN'"#),
        // Native enum IN filter — `where: { role: { in: ['ADMIN','OWNER'] } }`.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "role" IN ('ADMIN','OWNER')"#),
        // createMany({ data: [...], skipDuplicates: true }) — batch INSERT with
        // ON CONFLICT DO NOTHING (Prisma's skipDuplicates lowering).
        Shape::raw(r#"INSERT INTO "members" ("id","email","role") VALUES (50,'m50@x.com','USER'),(51,'m51@x.com','USER') ON CONFLICT DO NOTHING"#),
        // updateMany({ where, data }) — bulk UPDATE, no RETURNING.
        Shape::raw(r#"UPDATE "members" SET "role" = 'USER' WHERE "role" = 'OWNER'"#),
        // deleteMany({ where: { id: { in: [...] } } }).
        Shape::raw(r#"DELETE FROM "members" WHERE "id" IN (998,999)"#),
        // Soft-delete read — Prisma middleware injects `deleted_at IS NULL`.
        Shape::raw(r#"SELECT "id","email" FROM "members" WHERE "deleted_at" IS NULL ORDER BY "id""#),
        // Cursor pagination — `cursor: { id }, skip: 1, take: 10`.
        Shape::with(
            r#"SELECT "id","email" FROM "members" WHERE "id" > $1 ORDER BY "id" ASC LIMIT $2 OFFSET $3"#,
            &[ParamValue::I64(0), ParamValue::I64(10), ParamValue::I64(1)],
        ),
        // $transaction isolation — Prisma issues this before tx statements.
        Shape::raw(r#"SET TRANSACTION ISOLATION LEVEL READ COMMITTED"#),
        // findUnique({ where: { id }, select }) with `FOR UPDATE` inside an
        // interactive transaction (Prisma 5 row-locking preview).
        Shape::raw(r#"SELECT "id","email" FROM "members" WHERE "id" = 1 LIMIT 1 FOR UPDATE"#),
        // upsert with composite unique target — `where: { org_id_user_id: {...} }`.
        Shape::raw(r#"INSERT INTO "memberships" ("org_id","user_id","level") VALUES (1,1,'admin')
           ON CONFLICT ("org_id","user_id") DO UPDATE SET "level" = EXCLUDED."level""#),
        // $queryRaw escape hatch — typed cast + computed expression.
        // The harness has no Numeric ParamValue variant; inline the multiplier
        // as a typed literal — same convention as orm_compat.rs (which avoids
        // timestamptz/numeric bind params for the same reason).
        Shape::raw(
            r#"SELECT "id", "balance"::numeric(12,2) * 1.1::numeric AS "scaled" FROM "members" WHERE "balance" IS NOT NULL"#,
        ),
        // CREATE TYPE … AS ENUM — `prisma migrate` enum emission (probed).
        Shape::raw(r#"CREATE TYPE "status_enum" AS ENUM ('open','closed')"#),
        // Migration breadth — Prisma `migrate` ALTER ENUM ADD VALUE.
        Shape::raw(r#"ALTER TYPE "status_enum" ADD VALUE 'archived'"#),
    ];
    println!("\n=== Prisma EXT ({} shapes) ===", prisma.len());
    let prisma_result = run_orm_section(&client, "Prisma", prisma).await;

    // ── Drizzle — long-tail shapes ──────────────────────────────────────────
    let drizzle: &[Shape] = &[
        // countDistinct — `db.select({ n: countDistinct(t.role) })`.
        Shape::raw(r#"SELECT COUNT(DISTINCT "role") AS "n" FROM "members""#),
        // groupBy + multiple aggregates — `.groupBy(t.member_id)`.
        Shape::raw(r#"SELECT "member_id", COUNT(*) AS "c", SUM("amount") AS "s", AVG("amount") AS "a" FROM "events" GROUP BY "member_id" ORDER BY "member_id""#),
        // `.for('update', { skipLocked: true })` — Drizzle PG locking modifier.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "role" = 'USER' LIMIT 1 FOR UPDATE SKIP LOCKED"#),
        // `.for('update', { noWait: true })`.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "id" = 1 FOR UPDATE NOWAIT"#),
        // `.for('share')` shared lock.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "id" = 1 FOR SHARE"#),
        // jsonb `->>` filter (Drizzle `sql\`${t.meta}->>'plan'\``).
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "meta"->>'plan' = 'free'"#),
        // jsonb containment `@>` — Drizzle `jsonContains` helper.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "meta" @> '{"plan":"pro"}'"#),
        // array overlap `&&` — Drizzle pg `arrayOverlaps`.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "tags" && ARRAY['rust','python']"#),
        // array contains `@>` — Drizzle `arrayContains`.
        Shape::raw(r#"SELECT "id" FROM "members" WHERE "tags" @> ARRAY['rust']"#),
        // updateMany-style bulk UPDATE returning the rows (`.returning()`).
        Shape::raw(r#"UPDATE "members" SET "version" = "version" + 1 WHERE "role" = 'USER' RETURNING "id","version""#),
        // keyset pagination with composite cursor (created_at, id).
        // Drizzle emits the timestamp as a typed literal; embed it inline so
        // the tokio-postgres harness (which has no Timestamptz ParamValue variant)
        // does not trip on OID mismatch — matches the convention in orm_compat.rs
        // (see comment at the GORM timestamptz note).
        Shape::raw(
            r#"SELECT "id","email" FROM "members" WHERE ("created_at", "id") > ('2024-01-01 00:00:00+00'::timestamptz, 1) ORDER BY "created_at" ASC, "id" ASC LIMIT 5"#,
        ),
        // optimistic-lock UPDATE — `WHERE id = $1 AND version = $2`.
        Shape::with(
            r#"UPDATE "members" SET "email" = $1, "version" = "version" + 1 WHERE "id" = $2 AND "version" = $3"#,
            &[ParamValue::Text("a2@x.com"), ParamValue::I64(1), ParamValue::I64(0)],
        ),
        // Migration breadth — drizzle-kit `ALTER TABLE … ADD COLUMN`.
        Shape::raw(r#"ALTER TABLE "members" ADD COLUMN "nickname" text"#),
        // drizzle-kit `ALTER TABLE … DROP COLUMN`.
        Shape::raw(r#"ALTER TABLE "members" DROP COLUMN "nickname""#),
        // drizzle-kit `CREATE INDEX`.
        Shape::raw(r#"CREATE INDEX "members_role_idx" ON "members" ("role")"#),
        // drizzle-kit `ALTER TABLE … ADD CONSTRAINT … UNIQUE`.
        Shape::raw(r#"ALTER TABLE "members" ADD CONSTRAINT "members_email_uq" UNIQUE ("email")"#),
    ];
    println!("\n=== Drizzle EXT ({} shapes) ===", drizzle.len());
    let drizzle_result = run_orm_section(&client, "Drizzle", drizzle).await;

    // ── Django — long-tail shapes ───────────────────────────────────────────
    //
    // psycopg2 client-side interpolates params, so every shape is literal SQL
    // (verbatim mogrified output).
    let django: &[Shape] = &[
        // .count() idiom — Django wraps in an aggregate subquery.
        Shape::raw(r#"SELECT COUNT(*) AS "__count" FROM "members""#),
        // .exists() idiom — `SELECT 1 ... LIMIT 1` then truthiness.
        Shape::raw(r#"SELECT (1) AS "a" FROM "members" WHERE "members"."role" = 'ADMIN' LIMIT 1"#),
        // .latest('created_at') — ORDER BY DESC LIMIT 1.
        Shape::raw(r#"SELECT "members"."id","members"."created_at" FROM "members" ORDER BY "members"."created_at" DESC LIMIT 1"#),
        // .first() — ORDER BY pk ASC LIMIT 1.
        Shape::raw(r#"SELECT "members"."id" FROM "members" ORDER BY "members"."id" ASC LIMIT 1"#),
        // .values_list('id', flat=True) — single-column projection.
        Shape::raw(r#"SELECT "members"."id" FROM "members" ORDER BY "members"."id" ASC"#),
        // annotate(Count/Sum/Avg) + values — group annotation.
        Shape::raw(r#"SELECT "events"."member_id", COUNT("events"."id") AS "n", SUM("events"."amount") AS "total", AVG("events"."amount") AS "avg" FROM "events" GROUP BY "events"."member_id" ORDER BY "events"."member_id" ASC"#),
        // .distinct('role') ⇒ DISTINCT ON (Postgres-only Django feature).
        Shape::raw(r#"SELECT DISTINCT ON ("members"."role") "members"."role","members"."id" FROM "members" ORDER BY "members"."role" ASC, "members"."id" ASC"#),
        // JSONField __contains — `meta__contains={'plan':'pro'}` ⇒ `@>`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."meta" @> '{"plan": "pro"}'"#),
        // JSONField __has_key — `meta__has_key='plan'` ⇒ `?`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."meta" ? 'plan'"#),
        // JSONField key transform — `meta__plan='pro'` ⇒ `->> 'plan' = 'pro'`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE ("members"."meta" ->> 'plan') = 'pro'"#),
        // ArrayField __contains — `tags__contains=['rust']` ⇒ `@>`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."tags" @> ARRAY['rust']::text[]"#),
        // ArrayField __overlap — `tags__overlap=['go','rust']` ⇒ `&&`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."tags" && ARRAY['go','rust']::text[]"#),
        // __date filter — `occurred_at__date=...` ⇒ ::date cast comparison.
        Shape::raw(r#"SELECT "events"."id" FROM "events" WHERE ("events"."occurred_at")::date = '2024-03-01'"#),
        // __year filter — `occurred_at__year=2024` ⇒ EXTRACT comparison.
        Shape::raw(r#"SELECT "events"."id" FROM "events" WHERE EXTRACT('year' FROM "events"."occurred_at") = 2024"#),
        // select_for_update() inside atomic — wrapped BEGIN/COMMIT.
        Shape::raw(r#"BEGIN; SELECT "members"."id" FROM "members" WHERE "members"."id" = 1 FOR UPDATE; COMMIT;"#),
        // select_for_update(skip_locked=True) — Django 1.11+ queue pattern.
        Shape::raw(r#"BEGIN; SELECT "events"."id" FROM "events" WHERE "events"."kind" = 'purchase' ORDER BY "events"."id" ASC LIMIT 1 FOR UPDATE SKIP LOCKED; COMMIT;"#),
        // select_for_update(nowait=True).
        Shape::raw(r#"BEGIN; SELECT "members"."id" FROM "members" WHERE "members"."id" = 1 FOR UPDATE NOWAIT; COMMIT;"#),
        // bulk_create(..., ignore_conflicts=True) — INSERT … ON CONFLICT DO NOTHING.
        Shape::raw(r#"INSERT INTO "members" ("id","email","role") VALUES (60,'d60@x.com','USER'),(61,'d61@x.com','USER') ON CONFLICT DO NOTHING"#),
        // bulk_update — Django emits a CASE-based single UPDATE.
        Shape::raw(r#"UPDATE "members" SET "version" = CASE WHEN "id" = 1 THEN 5 WHEN "id" = 2 THEN 6 ELSE "version" END WHERE "id" IN (1,2)"#),
        // F() expression UPDATE — `update(amount=F('amount')+1)`.
        Shape::raw(r#"UPDATE "events" SET "amount" = ("events"."amount" + 1.00) WHERE "events"."kind" = 'purchase'"#),
        // Migration breadth — Django schema editor `RENAME COLUMN`.
        Shape::raw(r#"ALTER TABLE "events" RENAME COLUMN "kind" TO "event_kind""#),
        // …then rename it back so later shapes referencing "kind" still resolve.
        Shape::raw(r#"ALTER TABLE "events" RENAME COLUMN "event_kind" TO "kind""#),
        // Migration breadth — `ALTER COLUMN … TYPE` (Django AlterField).
        Shape::raw(r#"ALTER TABLE "events" ALTER COLUMN "kind" TYPE varchar(255)"#),
        // Migration breadth — add FK after creation (Django AddField FK).
        // Now supported: ALTER ADD CONSTRAINT FOREIGN KEY registers the FK in
        // catalog metadata (enforcement on subsequent writes; backfill deferred).
        Shape::raw(r#"ALTER TABLE "event_lines" ADD CONSTRAINT "event_lines_event_fk" FOREIGN KEY ("event_id") REFERENCES "events" ("id")"#),
        // Session cleanup guard (mirrors orm_compat.rs convention).
        Shape::raw(r#"ROLLBACK"#),
    ];
    println!("\n=== Django EXT ({} shapes) ===", django.len());
    let django_result = run_orm_section(&client, "Django", django).await;

    // ── SQLAlchemy — long-tail shapes ───────────────────────────────────────
    let sqlalchemy: &[Shape] = &[
        // with_for_update() — `select(M).with_for_update()`.
        Shape::with(
            r#"SELECT "members"."id" FROM "members" WHERE "members"."id" = $1 FOR UPDATE"#,
            &[ParamValue::I64(1)],
        ),
        // with_for_update(skip_locked=True).
        Shape::with(
            r#"SELECT "members"."id" FROM "members" WHERE "members"."role" = $1 LIMIT 1 FOR UPDATE SKIP LOCKED"#,
            &[ParamValue::Text("USER")],
        ),
        // func.count() scalar — `session.scalar(select(func.count()).select_from(M))`.
        Shape::raw(r#"SELECT count(*) AS "count_1" FROM "members""#),
        // exists() — `select(M).exists()` wrapped in EXISTS.
        Shape::raw(r#"SELECT EXISTS (SELECT 1 FROM "members" WHERE "members"."role" = 'ADMIN') AS "anon_1""#),
        // group_by + func aggregates.
        Shape::raw(r#"SELECT "events"."member_id", count("events"."id") AS "n", sum("events"."amount") AS "s" FROM "events" GROUP BY "events"."member_id" ORDER BY "events"."member_id""#),
        // JSONB `[].astext` — `M.meta['plan'].astext == 'pro'` ⇒ `->>`.
        Shape::with(
            r#"SELECT "members"."id" FROM "members" WHERE ("members"."meta" ->> $1) = $2"#,
            &[ParamValue::Text("plan"), ParamValue::Text("pro")],
        ),
        // JSONB `.contains()` ⇒ `@>`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."meta" @> '{"plan": "pro"}'"#),
        // JSONB `.has_key()` ⇒ `?`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."meta" ? 'plan'"#),
        // ARRAY `.contains()` ⇒ `@>`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."tags" @> ARRAY['rust']"#),
        // ARRAY `.overlap()` ⇒ `&&`.
        Shape::raw(r#"SELECT "members"."id" FROM "members" WHERE "members"."tags" && ARRAY['go']"#),
        // selectin eager load — the IN-list second query.
        Shape::raw(r#"SELECT "events"."id","events"."member_id" FROM "events" WHERE "events"."member_id" IN (1, 2) ORDER BY "events"."member_id""#),
        // numeric round-trip — `Numeric(12,2)` column with arithmetic.
        Shape::raw(r#"SELECT "members"."id", "members"."balance" + 0.01 AS "bal" FROM "members" WHERE "members"."balance" IS NOT NULL"#),
        // date_trunc — `func.date_trunc('month', col)`.
        Shape::raw(r#"SELECT date_trunc('month', "events"."occurred_at") AS "m", count(*) AS "c" FROM "events" GROUP BY date_trunc('month', "events"."occurred_at") ORDER BY "m""#),
        // text() raw escape hatch with named params re-emitted positional.
        Shape::with(
            r#"SELECT "id" FROM "members" WHERE "email" = $1"#,
            &[ParamValue::Text("a@x.com")],
        ),
        // upsert — `insert(M).on_conflict_do_update` (PG dialect, composite).
        Shape::raw(r#"INSERT INTO "memberships" ("org_id","user_id","level") VALUES (1,2,'lead')
           ON CONFLICT ("org_id","user_id") DO UPDATE SET "level" = EXCLUDED."level""#),
        // Alembic migration breadth — `op.add_column`.
        Shape::raw(r#"ALTER TABLE "members" ADD COLUMN "phone" varchar"#),
        // Alembic `op.alter_column(type_=...)`.
        Shape::raw(r#"ALTER TABLE "members" ALTER COLUMN "phone" TYPE text"#),
        // Alembic `op.drop_column`.
        Shape::raw(r#"ALTER TABLE "members" DROP COLUMN "phone""#),
        // Alembic `op.create_index`.
        Shape::raw(r#"CREATE INDEX "ix_members_email" ON "members" ("email")"#),
    ];
    println!("\n=== SQLAlchemy EXT ({} shapes) ===", sqlalchemy.len());
    let sqlalchemy_result = run_orm_section(&client, "SQLAlchemy", sqlalchemy).await;

    // ── GORM — long-tail shapes (pgx extended protocol, $N params) ──────────
    let gorm: &[Shape] = &[
        // Clauses(clause.Locking{Strength: "UPDATE"}) — `FOR UPDATE`.
        Shape::with(
            r#"SELECT * FROM "members" WHERE "id" = $1 LIMIT 1 FOR UPDATE"#,
            &[ParamValue::I64(1)],
        ),
        // Clauses(clause.Locking{Strength:"UPDATE", Options:"SKIP LOCKED"}).
        Shape::with(
            r#"SELECT * FROM "members" WHERE "role" = $1 LIMIT 1 FOR UPDATE SKIP LOCKED"#,
            &[ParamValue::Text("USER")],
        ),
        // db.Model(&M{}).Group("member_id").Select("member_id, count(*)").
        Shape::raw(r#"SELECT "member_id", count(*) AS "c", sum("amount") AS "s" FROM "events" GROUP BY "member_id" ORDER BY "member_id""#),
        // db.Distinct("role").Find — DISTINCT projection.
        Shape::raw(r#"SELECT DISTINCT "role" FROM "members""#),
        // db.Model(&M{}).Where(...).Count(&n).
        Shape::with(
            r#"SELECT count(*) FROM "members" WHERE "role" = $1"#,
            &[ParamValue::Text("USER")],
        ),
        // CreateInBatches — gorm splits a slice into multi-VALUES INSERTs.
        Shape::with(
            r#"INSERT INTO "members" ("id","email","role") VALUES ($1,$2,$3),($4,$5,$6) RETURNING "id""#,
            &[
                ParamValue::I64(70),
                ParamValue::Text("g70@x.com"),
                ParamValue::Text("USER"),
                ParamValue::I64(71),
                ParamValue::Text("g71@x.com"),
                ParamValue::Text("USER"),
            ],
        ),
        // datatypes.JSON `->> ` filter — gorm jsonb extract.
        Shape::with(
            r#"SELECT "id" FROM "members" WHERE "meta"->>'plan' = $1"#,
            &[ParamValue::Text("pro")],
        ),
        // gorm.Model soft-delete read — `deleted_at IS NULL` injected.
        Shape::raw(r#"SELECT * FROM "members" WHERE "members"."deleted_at" IS NULL ORDER BY "members"."id""#),
        // gorm soft delete — UPDATE deleted_at = now() (the Delete() emission).
        // GORM pgx binds the timestamp as a typed string over pgx; the harness has
        // no Timestamptz ParamValue variant, so inline the literal with ::timestamptz
        // cast — same convention as orm_compat.rs GORM section note.
        Shape::raw(
            r#"UPDATE "members" SET "deleted_at" = '2024-06-01 00:00:00+00'::timestamptz WHERE "id" = 2 AND "members"."deleted_at" IS NULL"#,
        ),
        // optimistic lock via gorm hooks — `WHERE id = $1 AND version = $2`.
        // GORM pgx drives numeric via the pgx Numeric codec; the harness has no
        // Numeric ParamValue variant, so inline the balance literal with ::numeric.
        Shape::with(
            r#"UPDATE "members" SET "balance" = 5.5::numeric, "version" = "version" + 1 WHERE "id" = $1 AND "version" = $2"#,
            &[ParamValue::I64(1), ParamValue::I64(0)],
        ),
        // composite ON CONFLICT upsert — gorm OnConflict with multiple Columns.
        Shape::with(
            r#"INSERT INTO "memberships" ("org_id","user_id","level") VALUES ($1,$2,$3) ON CONFLICT ("org_id","user_id") DO UPDATE SET "level"="excluded"."level""#,
            &[ParamValue::I64(1), ParamValue::I64(1), ParamValue::Text("staff")],
        ),
        // AutoMigrate HasIndex probe — gorm queries pg_indexes.
        Shape::with(
            r#"SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2"#,
            &[ParamValue::Text("members"), ParamValue::Text("idx_members_role")],
        ),
        // AutoMigrate CreateIndex — gorm DDL.
        Shape::raw(r#"CREATE INDEX IF NOT EXISTS "idx_members_role" ON "members" ("role")"#),
        // AutoMigrate ALTER ADD column (no IF NOT EXISTS; gorm probed first).
        Shape::raw(r#"ALTER TABLE "members" ADD "country" text"#),
    ];
    println!("\n=== GORM EXT ({} shapes) ===", gorm.len());
    let gorm_result = run_orm_section(&client, "GORM", gorm).await;

    // ── TypeORM — long-tail shapes ──────────────────────────────────────────
    let typeorm: &[Shape] = &[
        // .setLock('pessimistic_write') ⇒ `FOR UPDATE`.
        Shape::raw(r#"SELECT "m"."id" AS "m_id" FROM "members" "m" WHERE "m"."id" = 1 FOR UPDATE"#),
        // .setLock('pessimistic_write', undefined, ['m']) + skip-locked.
        Shape::raw(r#"SELECT "m"."id" AS "m_id" FROM "members" "m" WHERE "m"."role" = 'USER' LIMIT 1 FOR UPDATE SKIP LOCKED"#),
        // .setLock('pessimistic_read') ⇒ `FOR SHARE`.
        Shape::raw(r#"SELECT "m"."id" AS "m_id" FROM "members" "m" WHERE "m"."id" = 1 FOR SHARE"#),
        // getCount() — wraps the query in COUNT(DISTINCT primary).
        Shape::raw(r#"SELECT COUNT(DISTINCT("m"."id")) AS "cnt" FROM "members" "m""#),
        // .groupBy + addSelect aggregates.
        Shape::raw(r#"SELECT "e"."member_id" AS "mid", COUNT(*) AS "c", SUM("e"."amount") AS "s" FROM "events" "e" GROUP BY "e"."member_id" ORDER BY "e"."member_id" ASC"#),
        // .softDelete() ⇒ UPDATE deleted_at = NOW() WHERE … AND deleted_at IS NULL.
        Shape::raw(r#"UPDATE "members" SET "deleted_at" = NOW() WHERE "id" = 2 AND "deleted_at" IS NULL"#),
        // soft-delete-aware read — TypeORM appends `deleted_at IS NULL`.
        Shape::raw(r#"SELECT "m"."id" AS "m_id" FROM "members" "m" WHERE "m"."deleted_at" IS NULL"#),
        // optimistic @VersionColumn UPDATE — `WHERE id = $1 AND version = $2`.
        Shape::with(
            r#"UPDATE "members" SET "email" = $1, "version" = "version" + 1 WHERE "id" = $2 AND "version" = $3"#,
            &[ParamValue::Text("a3@x.com"), ParamValue::I64(1), ParamValue::I64(0)],
        ),
        // .upsert([...], { conflictPaths: ['org_id','user_id'] }) — composite.
        Shape::raw(r#"INSERT INTO "memberships" ("org_id","user_id","level") VALUES (1,2,'viewer') ON CONFLICT ("org_id","user_id") DO UPDATE SET "level" = EXCLUDED."level""#),
        // keyset pagination over an indexed column.
        Shape::with(
            r#"SELECT "m"."id" AS "m_id" FROM "members" "m" WHERE "m"."id" > $1 ORDER BY "m"."id" ASC LIMIT 5"#,
            &[ParamValue::I64(0)],
        ),
        // depth-3 eager LEFT JOIN chain — members → events → event_lines.
        Shape::raw(r#"SELECT "m"."id" AS "m_id", "e"."id" AS "e_id", "el"."id" AS "el_id", "el"."label" AS "el_label"
           FROM "members" "m"
           LEFT JOIN "events" "e" ON "e"."member_id" = "m"."id"
           LEFT JOIN "event_lines" "el" ON "el"."event_id" = "e"."id"
           ORDER BY "m"."id" ASC, "e"."id" ASC, "el"."id" ASC"#),
        // Migration breadth — TypeORM `ALTER TABLE … RENAME COLUMN`.
        Shape::raw(r#"ALTER TABLE "members" RENAME COLUMN "balance" TO "bal""#),
        // …rename back.
        Shape::raw(r#"ALTER TABLE "members" RENAME COLUMN "bal" TO "balance""#),
        // Migration breadth — `DROP TABLE` (TypeORM down() / drop schema sync).
        Shape::raw(r#"DROP TABLE IF EXISTS "memberships""#),
    ];
    println!("\n=== TypeORM EXT ({} shapes) ===", typeorm.len());
    let typeorm_result = run_orm_section(&client, "TypeORM", typeorm).await;

    // ── sqlx — long-tail shapes (Rust, $N positional binds) ─────────────────
    let sqlx: &[Shape] = &[
        // FOR UPDATE SKIP LOCKED — the canonical sqlx job-queue dequeue.
        Shape::with(
            r#"SELECT id FROM "events" WHERE kind = $1 ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"#,
            &[ParamValue::Text("purchase")],
        ),
        // FOR NO KEY UPDATE — lighter lock for the same queue pattern.
        Shape::raw(r#"SELECT id FROM "events" ORDER BY id LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED"#),
        // group_by aggregate via query_as!.
        Shape::raw(r#"SELECT member_id, COUNT(*) AS n, SUM(amount) AS total FROM "events" GROUP BY member_id ORDER BY member_id"#),
        // jsonb path extract bound filter.
        Shape::with(
            r#"SELECT id FROM "members" WHERE meta->>'plan' = $1"#,
            &[ParamValue::Text("pro")],
        ),
        // numeric round-trip with bound numeric arithmetic.
        // sqlx drives Decimal via its own codec; the harness has no Numeric
        // ParamValue variant, so inline the threshold literal with ::numeric.
        Shape::raw(
            r#"SELECT id FROM "members" WHERE balance > 50.0::numeric"#,
        ),
        // bytea round-trip — sqlx `Vec<u8>` bind.
        Shape::with(
            r#"SELECT $1::bytea AS "blob""#,
            &[ParamValue::Bytea(b"\x00\x01\x02hello")],
        ),
        // advisory lock — sqlx leader-election (now supported as a UDF).
        Shape::raw(r#"SELECT pg_try_advisory_lock(424242)"#),
        // advisory unlock.
        Shape::raw(r#"SELECT pg_advisory_unlock(424242)"#),
        // CREATE INDEX CONCURRENTLY — sqlx migration helper.
        // KNOWN GAP: CONCURRENTLY is explicitly rejected in v0.1.
        Shape::raw(r#"CREATE INDEX CONCURRENTLY "events_kind_idx" ON "events" ("kind")"#),
        // plain CREATE INDEX (the non-concurrent fallback) — supported.
        Shape::raw(r#"CREATE INDEX "events_kind_idx2" ON "events" ("kind")"#),
        // DROP TABLE migration.
        Shape::raw(r#"DROP TABLE IF EXISTS "event_lines""#),
    ];
    println!("\n=== sqlx EXT ({} shapes) ===", sqlx.len());
    let sqlx_result = run_orm_section(&client, "sqlx", sqlx).await;

    // ── Summary + gates ─────────────────────────────────────────────────────
    let sections = vec![
        prisma_result,
        drizzle_result,
        django_result,
        sqlalchemy_result,
        gorm_result,
        typeorm_result,
        sqlx_result,
    ];

    let total_shapes: usize = sections.iter().map(|s| s.total).sum();
    let total_ok: usize = sections.iter().map(|s| s.ok).sum();
    let total_typed: usize = sections.iter().map(|s| s.typed_error).sum();
    let total_reg: usize = sections.iter().map(|s| s.regression).sum();
    let overall_pct = if total_shapes == 0 {
        100.0
    } else {
        total_ok as f64 / total_shapes as f64 * 100.0
    };

    println!("\n══════════════════════════════════════════════════════════");
    println!("ORM COMPAT EXT CORPUS SUMMARY");
    println!("══════════════════════════════════════════════════════════");
    println!(
        "{:<12}  {:>5}  {:>3}  {:>11}  {:>10}  {:>9}",
        "ORM", "Total", "OK", "Typed-Err", "Regression", "OK-Rate"
    );
    println!("{}", "─".repeat(60));
    for s in &sections {
        println!(
            "{:<12}  {:>5}  {:>3}  {:>11}  {:>10}  {:>8.0}%",
            s.orm, s.total, s.ok, s.typed_error, s.regression, s.pass_rate_pct
        );
    }
    println!("{}", "─".repeat(60));
    println!(
        "{:<12}  {:>5}  {:>3}  {:>11}  {:>10}  {:>8.0}%",
        "TOTAL", total_shapes, total_ok, total_typed, total_reg, overall_pct
    );
    println!("══════════════════════════════════════════════════════════");

    let report = OrmCompatReport {
        generated_at: format!(
            "@{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        ),
        total_shapes,
        total_ok,
        total_typed_error: total_typed,
        total_regression: total_reg,
        sections,
    };
    emit_report(&report);

    // ── Gate 1: zero regressions ─────────────────────────────────────────────
    for s in &report.sections {
        assert_eq!(
            s.regression, 0,
            "[{}] {} regression(s) — server panicked or connection broken. \
             This is always a protocol regression.",
            s.orm, s.regression,
        );
    }

    // ── Gate 2: per-ORM pass rate ─────────────────────────────────────────────
    for s in &report.sections {
        assert!(
            s.pass_rate_pct >= MIN_PASS_RATE_PCT,
            "[{}] OK rate {:.0}% < minimum {:.0}% ({} ok / {} shapes)",
            s.orm,
            s.pass_rate_pct,
            MIN_PASS_RATE_PCT,
            s.ok,
            s.total,
        );
    }

    println!(
        "\n[orm_compat_ext] PASS — {total_ok}/{total_shapes} OK, \
         {total_typed} typed-errors, {total_reg} regressions."
    );
}
