//! Schema-namespace integration tests: CREATE/DROP SCHEMA, schema-qualified
//! DML, cross-schema isolation, search_path basics, `public` equivalence,
//! the verbatim drizzle-kit migrate statement sequence, and the Prisma shadow-
//! database path (typed 0A000 + recipe, option C).
//!
//! All tests use in-process engine sessions — no pgwire needed.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, Int16Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn open_engine() -> (TempDir, Engine) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (dir, engine)
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute [{sql}]: {e}"));
}

async fn exec_err(sess: &ProjectSession, sql: &str) -> BasinError {
    sess.execute(sql)
        .await
        .expect_err(&format!("expected error for [{sql}]"))
}

/// Collect a query's results into a flat list of string rows.
async fn rows(sess: &ProjectSession, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { schema, batches }) => {
            // Headers come from the result's top-level schema so an empty
            // result set (zero batches) still reports the projected columns.
            let mut headers: Vec<String> =
                schema.fields().iter().map(|f| f.name().clone()).collect();
            let mut out: Vec<Vec<String>> = Vec::new();
            for b in &batches {
                if headers.is_empty() {
                    headers = b
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                }
                for r in 0..b.num_rows() {
                    let mut row: Vec<String> = Vec::with_capacity(b.num_columns());
                    for c in 0..b.num_columns() {
                        row.push(render_cell(b.column(c), r));
                    }
                    out.push(row);
                }
            }
            (headers, out)
        }
        Ok(other) => panic!("non-rows result for [{sql}]: {other:?}"),
        Err(e) => panic!("execute [{sql}]: {e}"),
    }
}

fn render_cell(col: &dyn Array, r: usize) -> String {
    if col.is_null(r) {
        return "<null>".to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return a.value(r).to_string();
    }
    "<unknown>".to_string()
}

fn col(headers: &[String], name: &str) -> usize {
    headers
        .iter()
        .position(|h| h == name)
        .unwrap_or_else(|| panic!("column {name:?} not in {headers:?}"))
}

// ---------------------------------------------------------------------------
// 1. CREATE SCHEMA / DROP SCHEMA basics
// ---------------------------------------------------------------------------

/// CREATE SCHEMA creates a schema; DROP SCHEMA removes it.
#[tokio::test]
async fn create_and_drop_schema() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA myapp").await;
    // CREATE SCHEMA on an existing name without IF NOT EXISTS is an error.
    let e = exec_err(&sess, "CREATE SCHEMA myapp").await;
    assert!(
        matches!(e, BasinError::InvalidSchema(_)),
        "duplicate CREATE SCHEMA must error: {e}"
    );
    exec(&sess, "DROP SCHEMA myapp").await;
    // Second DROP without IF EXISTS is an error.
    let e = exec_err(&sess, "DROP SCHEMA myapp").await;
    assert!(
        matches!(e, BasinError::NotFound(_)),
        "DROP of non-existent schema must error: {e}"
    );
}

/// IF NOT EXISTS / IF EXISTS are idempotent.
#[tokio::test]
async fn schema_if_not_exists_and_if_exists() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Double CREATE IF NOT EXISTS — both succeed silently.
    exec(&sess, "CREATE SCHEMA IF NOT EXISTS billing").await;
    exec(&sess, "CREATE SCHEMA IF NOT EXISTS billing").await;

    // Double DROP IF EXISTS — both succeed silently.
    exec(&sess, "DROP SCHEMA IF EXISTS billing").await;
    exec(&sess, "DROP SCHEMA IF EXISTS billing").await;
}

/// public cannot be dropped.
#[tokio::test]
async fn cannot_drop_public_schema() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let e = exec_err(&sess, "DROP SCHEMA public").await;
    assert!(
        matches!(e, BasinError::InvalidSchema(_)),
        "dropping public must be an error: {e}"
    );
}

// ---------------------------------------------------------------------------
// 2. Schema-qualified CREATE TABLE + CRUD
// ---------------------------------------------------------------------------

/// A table created with a user-schema qualifier (myapp.events) is stored in
/// the public namespace (flat model) and is addressable by both qualified and
/// unqualified names within the same session.
#[tokio::test]
async fn qualified_create_table_and_crud() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA myapp").await;
    exec(
        &sess,
        "CREATE TABLE myapp.events (id BIGINT PRIMARY KEY, kind TEXT NOT NULL)",
    )
    .await;

    exec(&sess, "INSERT INTO myapp.events VALUES (1, 'click')").await;
    exec(&sess, "INSERT INTO myapp.events VALUES (2, 'view')").await;

    // SELECT through schema-qualified name.
    let (hdrs, rs) = rows(&sess, "SELECT id, kind FROM myapp.events ORDER BY id").await;
    assert_eq!(rs.len(), 2, "both rows visible");
    assert_eq!(rs[0][col(&hdrs, "id")], "1");
    assert_eq!(rs[1][col(&hdrs, "kind")], "view");

    // UPDATE through schema-qualified name.
    exec(&sess, "UPDATE myapp.events SET kind = 'tap' WHERE id = 1").await;
    let (hdrs2, rs2) = rows(&sess, "SELECT kind FROM myapp.events WHERE id = 1").await;
    assert_eq!(rs2[0][col(&hdrs2, "kind")], "tap");

    // DELETE through schema-qualified name.
    exec(&sess, "DELETE FROM myapp.events WHERE id = 2").await;
    let (_, rs3) = rows(&sess, "SELECT id FROM myapp.events").await;
    assert_eq!(rs3.len(), 1, "one row after delete");
}

// ---------------------------------------------------------------------------
// 3. DROP SCHEMA CASCADE
// ---------------------------------------------------------------------------

/// DROP SCHEMA CASCADE is accepted (same as RESTRICT in the flat model — no
/// catalog objects are owned per user schema). The schema is removed.
#[tokio::test]
async fn drop_schema_cascade_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA temp_ns").await;
    exec(&sess, "DROP SCHEMA temp_ns CASCADE").await;
    // Confirm it's gone: schema-qualified reference must fail.
    let e = exec_err(&sess, "CREATE TABLE temp_ns.t (id INT)").await;
    assert!(
        matches!(e, BasinError::NotFound(_)),
        "schema must be gone after DROP CASCADE: {e}"
    );
}

// ---------------------------------------------------------------------------
// 4. public-prefix equivalence (pinned back-compat)
// ---------------------------------------------------------------------------

/// public.foo and foo resolve to the same table. Creating through one name
/// makes the table visible through the other.
#[tokio::test]
async fn public_qualified_equals_bare_name() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE public.orders (id BIGINT PRIMARY KEY, total INT NOT NULL)",
    )
    .await;
    exec(&sess, "INSERT INTO orders VALUES (1, 100)").await;

    let (_, rs) = rows(&sess, "SELECT id FROM public.orders WHERE id = 1").await;
    assert_eq!(rs.len(), 1, "public.orders and bare 'orders' are the same");
}

/// Existing bare-name tables are addressable via public.name without any CREATE
/// SCHEMA statement — public is always present.
#[tokio::test]
async fn bare_table_addressable_via_public_dot() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(
        &sess,
        "CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec(&sess, "INSERT INTO widgets VALUES (1, 'bolt')").await;

    let (_, rs) = rows(&sess, "SELECT name FROM public.widgets WHERE id = 1").await;
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0][0], "bolt");
}

// ---------------------------------------------------------------------------
// 5. Cross-schema isolation: same table name in two user schemas
// ---------------------------------------------------------------------------

/// myapp.items and billing.items are physically the same table (flat model —
/// both alias to public.items). This is the documented flat-model behavior:
/// user schemas are namespace tags, not storage containers. Creating myapp.items
/// and billing.items resolves to the same underlying table.
#[tokio::test]
async fn user_schemas_alias_to_same_public_namespace() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA myapp").await;
    exec(&sess, "CREATE SCHEMA billing").await;

    exec(
        &sess,
        "CREATE TABLE myapp.items (id INT PRIMARY KEY, label TEXT)",
    )
    .await;
    // billing.items aliases to the same public.items — IF NOT EXISTS is needed
    // because the table already exists under public.items.
    exec(
        &sess,
        "CREATE TABLE IF NOT EXISTS billing.items (id INT PRIMARY KEY, label TEXT)",
    )
    .await;

    exec(&sess, "INSERT INTO myapp.items VALUES (1, 'alpha')").await;
    // Both qualified names see the same row (flat model).
    let (_, rs1) = rows(&sess, "SELECT id FROM myapp.items WHERE id = 1").await;
    let (_, rs2) = rows(&sess, "SELECT id FROM billing.items WHERE id = 1").await;
    assert_eq!(rs1.len(), 1, "myapp.items sees the row");
    assert_eq!(rs2.len(), 1, "billing.items aliases to the same table");
}

// ---------------------------------------------------------------------------
// 6. SET search_path basics
// ---------------------------------------------------------------------------

/// SET search_path updates the session's resolution order. After SET, an
/// unqualified name resolves to the new first entry (aliased to public for
/// user schemas per the flat model).
#[tokio::test]
async fn set_search_path_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA app").await;
    exec(&sess, "SET search_path = app, public").await;

    let (hdrs, rs) = rows(&sess, "SHOW search_path").await;
    let val = &rs[0][col(&hdrs, "search_path")];
    assert!(
        val.contains("app"),
        "search_path should contain 'app': {val}"
    );
}

// ---------------------------------------------------------------------------
// 7. Verbatim drizzle-kit migrate statement sequence
//    (from drizzle-orm/pg-core/dialect.js lines 45-67)
// ---------------------------------------------------------------------------

/// Execute the exact statement sequence drizzle-kit's migrate command issues:
///   1. CREATE SCHEMA IF NOT EXISTS "drizzle"
///   2. CREATE TABLE IF NOT EXISTS drizzle.__drizzle_migrations (
///        id SERIAL PRIMARY KEY, hash text NOT NULL, created_at bigint)
///   3. SELECT id, hash, created_at FROM drizzle.__drizzle_migrations
///        ORDER BY created_at DESC LIMIT 1
///   4. INSERT INTO drizzle.__drizzle_migrations ("hash","created_at")
///        VALUES ($1, $2)   [simulated here with literal values]
///   5. The user-schema migration itself (CREATE TABLE in public)
#[tokio::test]
async fn drizzle_migrate_sequence_end_to_end() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Step 1: drizzle-kit always issues CREATE SCHEMA IF NOT EXISTS.
    exec(&sess, r#"CREATE SCHEMA IF NOT EXISTS "drizzle""#).await;

    // Step 2: tracker table — two-part name, schema must be known.
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "drizzle"."__drizzle_migrations" (
            id SERIAL PRIMARY KEY,
            hash text NOT NULL,
            created_at bigint
        )"#,
    )
    .await;

    // Step 3: read last applied migration (empty on fresh DB).
    let (hdrs, rs) = rows(
        &sess,
        r#"SELECT id, hash, created_at
           FROM "drizzle"."__drizzle_migrations"
           ORDER BY created_at DESC
           LIMIT 1"#,
    )
    .await;
    assert_eq!(rs.len(), 0, "no migrations applied yet");
    // headers must match the expected column names
    assert!(hdrs.iter().any(|h| h == "hash"), "hash column present");

    // Step 4: record a migration.
    exec(
        &sess,
        r#"INSERT INTO "drizzle"."__drizzle_migrations" ("hash", "created_at")
           VALUES ('abc123def456', 1717000000000)"#,
    )
    .await;

    // Re-read: one row now.
    let (hdrs2, rs2) = rows(
        &sess,
        r#"SELECT id, hash, created_at
           FROM "drizzle"."__drizzle_migrations"
           ORDER BY created_at DESC
           LIMIT 1"#,
    )
    .await;
    assert_eq!(rs2.len(), 1, "one migration recorded");
    assert_eq!(
        rs2[0][col(&hdrs2, "hash")],
        "abc123def456",
        "hash round-trips"
    );

    // Step 5: user schema migration (CREATE TABLE in public, as drizzle-kit emits).
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "d_users" (
            "id" serial PRIMARY KEY NOT NULL,
            "email" text NOT NULL,
            "name" text,
            "created_at" timestamp with time zone DEFAULT now()
        )"#,
    )
    .await;
    exec(
        &sess,
        r#"INSERT INTO "d_users" ("email", "name") VALUES ('alice@drizzle.test', 'Alice')"#,
    )
    .await;
    let (_, ru) = rows(&sess, r#"SELECT email FROM "d_users""#).await;
    assert_eq!(ru.len(), 1, "user table DDL + DML via drizzle sequence");

    // Idempotency: running the migrate sequence again (IF NOT EXISTS) must not error.
    exec(&sess, r#"CREATE SCHEMA IF NOT EXISTS "drizzle""#).await;
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "drizzle"."__drizzle_migrations" (
            id SERIAL PRIMARY KEY,
            hash text NOT NULL,
            created_at bigint
        )"#,
    )
    .await;
}

/// drizzle-kit migrate also queries information_schema.columns for the table
/// schema after applying migrations. Verify the columns view returns rows for
/// the drizzle-migrated table (it must not error or return zero rows).
#[tokio::test]
async fn drizzle_info_schema_columns_after_migrate() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, r#"CREATE SCHEMA IF NOT EXISTS "drizzle""#).await;
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "d_posts" (
            "id" serial PRIMARY KEY NOT NULL,
            "title" text NOT NULL,
            "published" boolean DEFAULT false NOT NULL
        )"#,
    )
    .await;

    let (hdrs, rs) = rows(
        &sess,
        r#"SELECT column_name, data_type
           FROM information_schema.columns
           WHERE table_name = 'd_posts'
           ORDER BY column_name"#,
    )
    .await;
    assert!(
        rs.len() >= 3,
        "at least 3 columns for d_posts: got {}",
        rs.len()
    );
    let col_names: Vec<_> = rs
        .iter()
        .map(|r| r[col(&hdrs, "column_name")].clone())
        .collect();
    assert!(col_names.contains(&"id".to_string()), "id column present");
    assert!(
        col_names.contains(&"title".to_string()),
        "title column present"
    );
}

// ---------------------------------------------------------------------------
// 8. Prisma shadow-database: CREATE DATABASE / DROP DATABASE emit SQLSTATE 0A000
//    with a hint pointing to shadowDatabaseUrl (option C).
// ---------------------------------------------------------------------------

/// CREATE DATABASE must fail with FeatureNotSupported (SQLSTATE 0A000) and
/// the error message must name the `shadowDatabaseUrl` workaround.
#[tokio::test]
async fn create_database_returns_0a000_with_shadow_db_hint() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let e = exec_err(&sess, "CREATE DATABASE shadow_1234567890abcdef").await;
    match &e {
        BasinError::FeatureNotSupported(msg) => {
            // Must contain SQLSTATE 0A000.
            assert!(
                msg.contains("0A000"),
                "CREATE DATABASE error must cite 0A000: {msg}"
            );
            // Must hint at shadowDatabaseUrl so Prisma users know the recipe.
            assert!(
                msg.to_ascii_lowercase().contains("shadowdatabaseurl"),
                "error must name shadowDatabaseUrl recipe: {msg}"
            );
        }
        other => panic!("expected FeatureNotSupported, got: {other}"),
    }
}

/// DROP DATABASE is likewise rejected with SQLSTATE 0A000.
#[tokio::test]
async fn drop_database_returns_0a000() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    let e = exec_err(&sess, "DROP DATABASE IF EXISTS shadow_1234567890abcdef").await;
    assert!(
        matches!(e, BasinError::FeatureNotSupported(_)),
        "DROP DATABASE must be FeatureNotSupported: {e}"
    );
    if let BasinError::FeatureNotSupported(msg) = &e {
        assert!(msg.contains("0A000"), "must cite 0A000: {msg}");
    }
}

/// Prisma's `migrate deploy` flow does NOT issue CREATE DATABASE (it only does
/// that for `migrate dev`). The deploy flow must succeed: it issues only
/// standard DDL against the connection. This test exercises the full deploy
/// statement sequence — BEGIN / DDL / COMMIT — confirming the path that works
/// today keeps working.
#[tokio::test]
async fn prisma_migrate_deploy_flow_no_create_database() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Prisma migrate deploy issues statements from the committed migration.sql.
    // Simulate the relevant subset.
    exec(&sess, "BEGIN").await;
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "User" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "email" TEXT NOT NULL,
            "name" TEXT,
            "createdAt" TIMESTAMP WITH TIME ZONE DEFAULT now()
        )"#,
    )
    .await;
    exec(
        &sess,
        r#"CREATE TABLE IF NOT EXISTS "Post" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "title" TEXT NOT NULL,
            "published" BOOLEAN DEFAULT false NOT NULL,
            "authorId" INTEGER NOT NULL REFERENCES "User"("id")
        )"#,
    )
    .await;
    exec(&sess, "COMMIT").await;

    exec(
        &sess,
        r#"INSERT INTO "User" ("email","name") VALUES ('prisma@deploy.test','Alice')"#,
    )
    .await;
    let (_, rp) = rows(&sess, r#"SELECT id FROM "User""#).await;
    assert_eq!(rp.len(), 1, "Prisma deploy path produces a working table");
}

// ---------------------------------------------------------------------------
// 9. information_schema.schemata reflects user-created schemas
// ---------------------------------------------------------------------------

/// After CREATE SCHEMA myns, information_schema.schemata must include a row
/// with schema_name = 'myns'. The public schema is always present.
#[tokio::test]
async fn schemata_view_reflects_created_schemas() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA myns").await;

    let (hdrs, rs) = rows(
        &sess,
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
    )
    .await;
    let names: Vec<_> = rs
        .iter()
        .map(|r| r[col(&hdrs, "schema_name")].clone())
        .collect();

    assert!(
        names.contains(&"public".to_string()),
        "'public' must always appear in schemata: {names:?}"
    );
    assert!(
        names.contains(&"myns".to_string()),
        "'myns' must appear after CREATE SCHEMA: {names:?}"
    );

    // After DROP SCHEMA, the row must be gone.
    exec(&sess, "DROP SCHEMA myns").await;
    let (hdrs2, rs2) = rows(
        &sess,
        "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
    )
    .await;
    let names2: Vec<_> = rs2
        .iter()
        .map(|r| r[col(&hdrs2, "schema_name")].clone())
        .collect();
    assert!(
        !names2.contains(&"myns".to_string()),
        "'myns' must be absent after DROP SCHEMA: {names2:?}"
    );
}

/// pg_catalog.pg_namespace reflects user-created schemas.
#[tokio::test]
async fn pg_namespace_reflects_created_schemas() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE SCHEMA drizzle").await;

    let (hdrs, rs) = rows(
        &sess,
        "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname",
    )
    .await;
    let names: Vec<_> = rs
        .iter()
        .map(|r| r[col(&hdrs, "nspname")].clone())
        .collect();

    assert!(
        names.contains(&"public".to_string()),
        "'public' must always appear in pg_namespace: {names:?}"
    );
    assert!(
        names.contains(&"drizzle".to_string()),
        "'drizzle' must appear after CREATE SCHEMA: {names:?}"
    );
}
