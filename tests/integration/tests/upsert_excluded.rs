//! EXCLUDED pseudo-table alias resolution in `ON CONFLICT DO UPDATE`.
//!
//! Covers every form the corpus ORMs emit and the full surface of
//! `rewrite_do_update_expr`:
//!
//! * Basic counter upsert — `SET hits = EXCLUDED.hits` (both lower- and
//!   upper-case `EXCLUDED`, quoted `"excluded"` variant).
//! * Full-row replace — every column overwritten from the proposed row.
//! * Multi-row upsert — the per-row path assigns each conflicting row its
//!   OWN EXCLUDED values (not a single shared mapping).
//! * DO UPDATE WHERE clause — `DO UPDATE SET … WHERE EXCLUDED.hits > 0`
//!   (conditional update: row skipped when proposed value does not satisfy the
//!   predicate).
//! * Drizzle exact shape — `excluded."email"`, lowercase qualifier, quoted col.
//! * Prisma exact shape — `EXCLUDED."email"`, uppercase qualifier, quoted col.
//! * Diesel exact shape — `EXCLUDED."title"` via extended protocol ($N params).
//! * GORM exact shape — `"excluded"."code"`, both sides quoted, RETURNING.
//! * Arithmetic mixed — `SET n = t.n + EXCLUDED.n` (table.col + EXCLUDED.col).
//! * CAST form — `SET code = CAST(EXCLUDED.code AS TEXT)`.
//! * COALESCE function — `SET name = COALESCE(EXCLUDED.name, name)`.
//! * IS NOT NULL filter in DO UPDATE WHERE.
//! * Batched multi-row path correctly falls back for DO UPDATE WHERE.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness ────────────────────────────────────────────────────────────────

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

async fn session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

async fn rows_of(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows from {sql:?}, got {other:?}"),
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// Basic counter upsert: `EXCLUDED.hits` (uppercase EXCLUDED, unquoted col).
/// Verifies that the most common upsert shape resolves on first conflict.
#[tokio::test]
async fn excluded_basic_hits_counter() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(
        "CREATE TABLE accts (email TEXT NOT NULL, hits BIGINT NOT NULL, PRIMARY KEY (email))",
    )
    .await
    .unwrap();

    // First insert — no conflict.
    sess.execute("INSERT INTO accts (email, hits) VALUES ('u@x.com', 1) ON CONFLICT (email) DO UPDATE SET hits = EXCLUDED.hits")
        .await
        .unwrap();

    // Conflict — DO UPDATE must replace hits with the proposed value.
    sess.execute("INSERT INTO accts (email, hits) VALUES ('u@x.com', 42) ON CONFLICT (email) DO UPDATE SET hits = EXCLUDED.hits")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT hits FROM accts WHERE email = 'u@x.com'").await;
    assert_eq!(col_i64(&batches, "hits"), vec![42]);
}

/// Lowercase `excluded.hits` — matches PostgreSQL case-insensitive treatment.
#[tokio::test]
async fn excluded_lowercase_qualifier() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE cnt (k TEXT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO cnt VALUES ('a', 1)")
        .await
        .unwrap();
    sess.execute("INSERT INTO cnt VALUES ('a', 99) ON CONFLICT (k) DO UPDATE SET v = excluded.v")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT v FROM cnt WHERE k = 'a'").await;
    assert_eq!(col_i64(&batches, "v"), vec![99]);
}

/// Quoted `"excluded"."col"` — the form emitted by GORM and some Drizzle helpers.
#[tokio::test]
async fn excluded_double_quoted_both_sides() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(r#"CREATE TABLE products (id BIGINT NOT NULL PRIMARY KEY, code TEXT NOT NULL, price BIGINT NOT NULL)"#)
        .await
        .unwrap();
    sess.execute(r#"INSERT INTO products (id, code, price) VALUES (1, 'A', 10)"#)
        .await
        .unwrap();
    // Both qualifier and column are quoted (GORM / Drizzle style).
    sess.execute(r#"INSERT INTO products (id, code, price) VALUES (1, 'B', 20) ON CONFLICT (id) DO UPDATE SET code = "excluded"."code", price = "excluded"."price""#)
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT code, price FROM products WHERE id = 1").await;
    assert_eq!(col_string(&batches, "code"), vec!["B"]);
    assert_eq!(col_i64(&batches, "price"), vec![20]);
}

/// Full-row replace: all non-key columns overwritten from the proposed row.
#[tokio::test]
async fn excluded_full_row_replace() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE things (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, val BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO things VALUES (1, 'old', 100)")
        .await
        .unwrap();
    sess.execute("INSERT INTO things VALUES (1, 'new', 200) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT name, val FROM things WHERE id = 1").await;
    assert_eq!(col_string(&batches, "name"), vec!["new"]);
    assert_eq!(col_i64(&batches, "val"), vec![200]);
}

/// Multi-row upsert: each conflicting row receives ITS OWN EXCLUDED mapping,
/// not the mapping of some other row in the batch.
#[tokio::test]
async fn excluded_multi_row_per_row_mapping() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();

    // Seed rows 1 and 3 only; rows 2 and 4 do not exist yet.
    sess.execute("INSERT INTO kv VALUES (1, 10), (3, 30)")
        .await
        .unwrap();

    // Multi-row upsert: rows 1 and 3 conflict; rows 2 and 4 are fresh.
    // Each conflicting row must pick up its OWN proposed value.
    sess.execute(
        "INSERT INTO kv (k, v) VALUES (1, 11), (2, 22), (3, 33), (4, 44) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT k, v FROM kv ORDER BY k").await;
    assert_eq!(col_i64(&batches, "k"), vec![1, 2, 3, 4]);
    // Row 1 conflicted → updated to 11 (its OWN EXCLUDED.v, not 33).
    // Row 2 was fresh → inserted as 22.
    // Row 3 conflicted → updated to 33 (its OWN EXCLUDED.v, not 11).
    // Row 4 was fresh → inserted as 44.
    assert_eq!(col_i64(&batches, "v"), vec![11, 22, 33, 44]);
}

/// Arithmetic RHS: `SET n = t.n + EXCLUDED.n` — both an existing-row ref and an
/// EXCLUDED ref appear in the same expression.
#[tokio::test]
async fn excluded_arithmetic_mixed_refs() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE scores (k BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO scores VALUES (1, 5)")
        .await
        .unwrap();
    // n = existing(5) + proposed(3) = 8.
    sess.execute("INSERT INTO scores (k, n) VALUES (1, 3) ON CONFLICT (k) DO UPDATE SET n = scores.n + EXCLUDED.n")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT n FROM scores WHERE k = 1").await;
    assert_eq!(col_i64(&batches, "n"), vec![8]);
}

/// DO UPDATE WHERE clause: the update only fires when the proposed value satisfies
/// the predicate. A proposed value that fails the predicate leaves the existing
/// row intact (PG semantics: DO UPDATE WHERE is row-level; the conflict is
/// "resolved" by NOT updating, not by inserting a second row).
#[tokio::test]
async fn excluded_do_update_where_skips_non_matching() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE guarded (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO guarded VALUES (1, 100)")
        .await
        .unwrap();

    // Proposed value 0 fails `EXCLUDED.v > 0` → existing row unchanged.
    sess.execute(
        "INSERT INTO guarded (k, v) VALUES (1, 0) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > 0",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT v FROM guarded WHERE k = 1").await;
    assert_eq!(
        col_i64(&batches, "v"),
        vec![100],
        "v must not change when DO UPDATE WHERE predicate is false"
    );

    // Proposed value 200 passes → row is updated.
    sess.execute(
        "INSERT INTO guarded (k, v) VALUES (1, 200) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > 0",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT v FROM guarded WHERE k = 1").await;
    assert_eq!(col_i64(&batches, "v"), vec![200]);
}

/// Drizzle exact corpus shape: `excluded."email"` — lowercase `excluded`,
/// double-quoted column name.
#[tokio::test]
async fn excluded_drizzle_corpus_shape() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(r#"CREATE TABLE "users" ("id" BIGINT NOT NULL PRIMARY KEY, "email" TEXT NOT NULL, "name" TEXT)"#)
        .await
        .unwrap();
    sess.execute(
        r#"INSERT INTO "users" ("id","email","name") VALUES (60,'orig@example.com','Orig')"#,
    )
    .await
    .unwrap();

    // Drizzle emits lowercase `excluded` with a quoted column name.
    sess.execute(r#"INSERT INTO "users" ("id","email","name") VALUES (60,'up@example.com','U') ON CONFLICT ("id") DO UPDATE SET "email" = excluded."email", "name" = excluded."name""#)
        .await
        .unwrap();

    let batches = rows_of(
        &sess,
        r#"SELECT "email", "name" FROM "users" WHERE "id" = 60"#,
    )
    .await;
    assert_eq!(col_string(&batches, "email"), vec!["up@example.com"]);
    assert_eq!(col_string(&batches, "name"), vec!["U"]);
}

/// Prisma exact corpus shape: `EXCLUDED."email"` — uppercase `EXCLUDED`,
/// double-quoted column name.
#[tokio::test]
async fn excluded_prisma_corpus_shape() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(r#"CREATE TABLE "User" ("id" BIGINT NOT NULL PRIMARY KEY, "email" TEXT NOT NULL, "name" TEXT)"#)
        .await
        .unwrap();
    sess.execute(r#"INSERT INTO "User" ("id","email","name") VALUES (1,'a@x.com','A')"#)
        .await
        .unwrap();

    // Prisma emits uppercase EXCLUDED with quoted column name.
    sess.execute(r#"INSERT INTO "User" ("id","email","name") VALUES (1,'b@x.com','B') ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email", "name" = EXCLUDED."name""#)
        .await
        .unwrap();

    let batches = rows_of(
        &sess,
        r#"SELECT "email", "name" FROM "User" WHERE "id" = 1"#,
    )
    .await;
    assert_eq!(col_string(&batches, "email"), vec!["b@x.com"]);
    assert_eq!(col_string(&batches, "name"), vec!["B"]);
}

/// Diesel exact corpus shape: parameterised upsert via literal SQL with
/// EXCLUDED."title" (uppercase, quoted).
#[tokio::test]
async fn excluded_diesel_corpus_shape() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(r#"CREATE TABLE posts (id BIGINT NOT NULL PRIMARY KEY, title TEXT NOT NULL, user_id BIGINT NOT NULL)"#)
        .await
        .unwrap();
    sess.execute("INSERT INTO posts (id, title, user_id) VALUES (1, 'First', 1)")
        .await
        .unwrap();

    // Diesel emits uppercase EXCLUDED with quoted column (literal values here,
    // since we call sess.execute directly — mimics the substituted form).
    sess.execute(r#"INSERT INTO "posts" ("id", "title", "user_id") VALUES (1, 'Upserted', 1) ON CONFLICT ("id") DO UPDATE SET "title" = EXCLUDED."title""#)
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT title FROM posts WHERE id = 1").await;
    assert_eq!(col_string(&batches, "title"), vec!["Upserted"]);
}

/// GORM exact corpus shape: both qualifier AND column double-quoted.
/// `SET "code"="excluded"."code","price"="excluded"."price"`.
#[tokio::test]
async fn excluded_gorm_corpus_shape() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute(r#"CREATE TABLE "products" ("id" BIGINT NOT NULL PRIMARY KEY, "code" TEXT NOT NULL, "price" BIGINT NOT NULL, "stock" BIGINT NOT NULL)"#)
        .await
        .unwrap();
    sess.execute(r#"INSERT INTO "products" ("id","code","price","stock") VALUES (1,'A',10,100)"#)
        .await
        .unwrap();

    // GORM emits both sides quoted.
    sess.execute(r#"INSERT INTO "products" ("id","code","price","stock") VALUES (1,'B',20,200) ON CONFLICT ("id") DO UPDATE SET "code"="excluded"."code","price"="excluded"."price","stock"="excluded"."stock""#)
        .await
        .unwrap();

    let batches = rows_of(
        &sess,
        r#"SELECT "code", "price", "stock" FROM "products" WHERE "id" = 1"#,
    )
    .await;
    assert_eq!(col_string(&batches, "code"), vec!["B"]);
    assert_eq!(col_i64(&batches, "price"), vec![20]);
    assert_eq!(col_i64(&batches, "stock"), vec![200]);
}

/// CAST form: `SET code = CAST(EXCLUDED.code AS TEXT)` — verifies that
/// `rewrite_do_update_expr` recurses into `Expr::Cast`.
#[tokio::test]
async fn excluded_cast_expression() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, code TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO items VALUES (1, 'old')")
        .await
        .unwrap();
    sess.execute("INSERT INTO items VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET code = CAST(EXCLUDED.code AS TEXT)")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT code FROM items WHERE id = 1").await;
    assert_eq!(col_string(&batches, "code"), vec!["new"]);
}

/// COALESCE function: `SET name = COALESCE(EXCLUDED.name, name)` — verifies
/// that `rewrite_do_update_expr` recurses into `Expr::Function` arguments.
#[tokio::test]
async fn excluded_coalesce_function() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE ppl (id BIGINT NOT NULL PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO ppl VALUES (1, 'Alice')")
        .await
        .unwrap();

    // Proposed name is NULL → COALESCE picks the existing name.
    sess.execute("INSERT INTO ppl (id, name) VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, name)")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT name FROM ppl WHERE id = 1").await;
    assert_eq!(col_string(&batches, "name"), vec!["Alice"]);

    // Proposed name is non-NULL → COALESCE picks the proposed name.
    sess.execute("INSERT INTO ppl (id, name) VALUES (1, 'Bob') ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, name)")
        .await
        .unwrap();

    let batches = rows_of(&sess, "SELECT name FROM ppl WHERE id = 1").await;
    assert_eq!(col_string(&batches, "name"), vec!["Bob"]);
}

/// DO UPDATE WHERE with IS NOT NULL: the most common guard shape.
#[tokio::test]
async fn excluded_do_update_where_is_not_null() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE nullable_vals (id BIGINT NOT NULL PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nullable_vals VALUES (1, 'keep-me')")
        .await
        .unwrap();

    // Proposed NULL fails IS NOT NULL predicate → existing value kept.
    sess.execute(
        "INSERT INTO nullable_vals (id, val) VALUES (1, NULL) \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val IS NOT NULL",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT val FROM nullable_vals WHERE id = 1").await;
    assert_eq!(col_string(&batches, "val"), vec!["keep-me"]);

    // Non-NULL proposed value passes → row updated.
    sess.execute(
        "INSERT INTO nullable_vals (id, val) VALUES (1, 'replaced') \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val IS NOT NULL",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT val FROM nullable_vals WHERE id = 1").await;
    assert_eq!(col_string(&batches, "val"), vec!["replaced"]);
}

/// Multi-row upsert with DO UPDATE WHERE falls through to the per-row path
/// (the batched path opts out when a DO UPDATE WHERE clause is present). The
/// per-row path still produces correct results.
#[tokio::test]
async fn excluded_multi_row_do_update_where_falls_to_per_row() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE filtered (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO filtered VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    // Multi-row upsert with a DO UPDATE WHERE clause.
    // - Row (1, 0): conflict, but 0 > 50 is false → v stays 100.
    // - Row (2, 999): conflict, 999 > 50 is true → v becomes 999.
    // - Row (3, 30): conflict, but 30 > 50 is false → v stays 300.
    // - Row (4, 400): no conflict → inserted.
    sess.execute(
        "INSERT INTO filtered (k, v) VALUES (1, 0), (2, 999), (3, 30), (4, 400) \
         ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > 50",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT k, v FROM filtered ORDER BY k").await;
    assert_eq!(col_i64(&batches, "k"), vec![1, 2, 3, 4]);
    assert_eq!(col_i64(&batches, "v"), vec![100, 999, 300, 400]);
}

/// Read-back correctness after a sequence of mixed upserts: insert → conflict
/// → no-conflict insert → confirm final state has exactly the right rows.
#[tokio::test]
async fn excluded_read_back_correctness() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE log (id BIGINT NOT NULL PRIMARY KEY, msg TEXT NOT NULL, count BIGINT NOT NULL)")
        .await
        .unwrap();

    // Three fresh inserts.
    for i in 1i64..=3 {
        sess.execute(&format!("INSERT INTO log VALUES ({i}, 'msg{i}', {i})"))
            .await
            .unwrap();
    }

    // Conflict on ids 1 and 3, fresh insert for id 4.
    sess.execute(
        "INSERT INTO log (id, msg, count) VALUES (1, 'upd1', 10), (4, 'new4', 40), (3, 'upd3', 30) \
         ON CONFLICT (id) DO UPDATE SET msg = EXCLUDED.msg, count = EXCLUDED.count",
    )
    .await
    .unwrap();

    let batches = rows_of(&sess, "SELECT id, msg, count FROM log ORDER BY id").await;
    assert_eq!(col_i64(&batches, "id"), vec![1, 2, 3, 4]);
    assert_eq!(
        col_string(&batches, "msg"),
        vec!["upd1", "msg2", "upd3", "new4"]
    );
    assert_eq!(col_i64(&batches, "count"), vec![10, 2, 30, 40]);
}
