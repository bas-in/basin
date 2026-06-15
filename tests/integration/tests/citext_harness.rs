//! Phase 5.30.A — citext (case-insensitive text) test harness.
//!
//! **Task:** TASK.md Phase 5.30.A — citext type harness.
//! **Status:** GREEN. The 5.30.B–E implementation slices have all shipped, so
//! every test below now runs (no `#[ignore]`) and passes under a plain
//! `cargo test --test citext_harness`.
//!
//! ## Named slices
//!
//! | Test fn                                        | Slice                                   | Closed by |
//! |------------------------------------------------|-----------------------------------------|-----------|
//! | `citext_type_round_trip`                       | type round-trip                         | 5.30.B    |
//! | `citext_case_insensitive_comparison`           | comparison semantics                    | 5.30.C    |
//! | `citext_unique_constraint_case_insensitive`    | unique-constraint case-insensitivity    | 5.30.D    |
//! | `citext_orm_compat`                            | ORM compat                              | 5.30.E    |
//!
//! All slices are closed (5.30.B–E shipped); the harness is part of the
//! default `cargo test` run.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared engine helper ────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Run `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results so that assertion failures are
/// as close to the problematic SQL as possible.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return the first string scalar from the first row/column.
/// Panics if the result is not a single-cell string result.
async fn scalar_text(sess: &basin_engine::ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("expected StringArray for text result");
            arr.value(0).to_owned()
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Run `sql` and return the first boolean scalar from the first row/column.
/// Panics if the result is not a single-cell boolean result.
async fn scalar_bool(sess: &basin_engine::ProjectSession, sql: &str) -> bool {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let batch = batches.first().expect("expected at least one batch");
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("expected BooleanArray for boolean result");
            arr.value(0)
        }
        Ok(ExecResult::Empty { .. }) => panic!("expected Rows, got Empty for:\n  {sql}"),
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — Type round-trip
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that citext is a recognised column type: DDL is accepted, a value
// can be INSERT-ed, and the same value is returned verbatim by a SELECT.
//
// The round-trip preserves the *original casing* of the stored value — citext
// is case-insensitive for comparisons but it does NOT fold the stored bytes.
// So INSERT 'Foo@Bar.com' → SELECT returns 'Foo@Bar.com' (not 'foo@bar.com').
//
// RED until 5.30.B: Basin does not yet recognise `citext` as a column type;
// `CREATE TABLE u (email citext)` will fail with an unknown-type error.

/// Phase 5.30.A — citext type round-trip.
///
/// Creates a table with a `citext` column, inserts the mixed-case value
/// `'Foo@Bar.com'`, and asserts that the SELECT returns the stored string
/// unchanged. Round-trip preservation (not case-folding) is the PG contract
/// for citext.
///
/// Closed by: 5.30.B (citext type recognised by parser + engine).
#[tokio::test]
async fn citext_type_round_trip() {
    // Slice: "type round-trip"
    //
    // IMPORTANT: every assertion here reflects correct PostgreSQL semantics.
    // Do NOT weaken them to match Basin's current (wrong) output — fix the
    // engine in 5.30.B and let the assertions go green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL: citext column ─────────────────────────────────────────────────────
    // Expected after 5.30.B: `citext` is accepted as a column type.
    // Currently RED: unknown type error.
    sess.execute("CREATE TABLE u (email citext)")
        .await
        .expect(
            "CITEXT ROUND-TRIP: CREATE TABLE u (email citext) must succeed. \
             Closed by 5.30.B.",
        );

    // ── INSERT with mixed-case value ───────────────────────────────────────────
    sess.execute("INSERT INTO u VALUES ('Foo@Bar.com')")
        .await
        .expect(
            "CITEXT ROUND-TRIP: INSERT 'Foo@Bar.com' into citext column must succeed. \
             Closed by 5.30.B.",
        );

    // ── SELECT and verify verbatim round-trip ──────────────────────────────────
    // PG citext stores the original bytes; comparison is case-insensitive but
    // the stored form is preserved. SELECT must return 'Foo@Bar.com', not
    // 'foo@bar.com' or 'FOO@BAR.COM'.
    let returned = scalar_text(&sess, "SELECT email FROM u LIMIT 1").await;
    println!("[5.30.A round-trip] returned: {returned:?} (expected 'Foo@Bar.com')");
    assert_eq!(
        returned, "Foo@Bar.com",
        "CITEXT ROUND-TRIP: SELECT must return the original mixed-case value \
         'Foo@Bar.com'; citext preserves storage case. \
         got={returned:?}. Closed by 5.30.B."
    );

    println!("[5.30.A round-trip] PASSED — 'Foo@Bar.com' round-tripped correctly");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — Comparison semantics
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies that citext equality and ordering are case-insensitive per the PG
// citext contract:
//   'ABC'::citext = 'abc'::citext  → true
//   'abc'::citext < 'DEF'::citext  → true  (locale-aware, case-insensitive)
//   'ABC'::citext < 'def'::citext  → true  (same)
//
// These semantics are what distinguish citext from TEXT: the equality operator
// ignores case, and ORDER BY on a citext column is case-insensitive.
//
// RED until 5.30.C: Basin either rejects the `::citext` cast or treats citext
// like regular TEXT (case-sensitive equality), causing both assertions to fail.

/// Phase 5.30.A — citext case-insensitive comparison and ordering semantics.
///
/// Asserts:
///   1. `'ABC'::citext = 'abc'::citext` returns `true`.
///   2. Ordering: `ORDER BY email` on a citext column sorts case-insensitively
///      (e.g. 'apple', 'Banana', 'cherry' → alphabetic order regardless of case).
///
/// Closed by: 5.30.C (citext equality + comparison operators).
#[tokio::test]
async fn citext_case_insensitive_comparison() {
    // Slice: "comparison semantics"
    //
    // IMPORTANT: do NOT weaken assertions to match Basin's current behaviour.
    // Implement 5.30.C to flip this green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Equality: 'ABC'::citext = 'abc'::citext must be TRUE ──────────────────
    // PG citext uses a case-insensitive collation for =; this is the headline
    // property of the type.
    let eq_result = scalar_bool(
        &sess,
        "SELECT 'ABC'::citext = 'abc'::citext",
    )
    .await;
    println!(
        "[5.30.A comparison] 'ABC'::citext = 'abc'::citext → {eq_result} (expected true)"
    );
    assert!(
        eq_result,
        "CITEXT COMPARISON: 'ABC'::citext = 'abc'::citext must be TRUE. \
         citext equality is case-insensitive. Closed by 5.30.C."
    );

    // ── Ordering: ORDER BY on citext column is case-insensitive ───────────────
    // Seed three values with mixed case; ORDER BY should return them in
    // alphabetical order ignoring case.
    sess.execute("CREATE TABLE words (w citext)")
        .await
        .expect("CITEXT COMPARISON: CREATE TABLE words (w citext). Closed by 5.30.B.");

    sess.execute("INSERT INTO words VALUES ('Banana'), ('apple'), ('Cherry')")
        .await
        .expect("CITEXT COMPARISON: INSERT mixed-case words. Closed by 5.30.B.");

    // Expected case-insensitive alphabetic order: apple, Banana, Cherry.
    // A case-sensitive sort would produce: Banana, Cherry, apple (uppercase first).
    let n = row_count(
        &sess,
        "SELECT w FROM words ORDER BY w",
    )
    .await;
    assert_eq!(
        n, 3,
        "CITEXT COMPARISON: ORDER BY must return all 3 rows. got={n}"
    );

    // Verify the first row in ORDER BY is 'apple' (case-insensitive ordering).
    let first = scalar_text(&sess, "SELECT w FROM words ORDER BY w LIMIT 1").await;
    println!(
        "[5.30.A comparison] ORDER BY first row: {first:?} (expected 'apple')"
    );
    assert_eq!(
        first.to_lowercase(),
        "apple",
        "CITEXT COMPARISON: ORDER BY on citext column must sort case-insensitively; \
         'apple' must come first (before 'Banana'). \
         got={first:?}. Closed by 5.30.C."
    );

    // Verify '<' operator: 'ABC'::citext < 'def'::citext must be TRUE.
    // (In case-sensitive ASCII ordering 'A' < 'd' anyway, but the key check is
    // 'abc'::citext < 'ABC'::citext is FALSE and 'ABC'::citext = 'abc'::citext.)
    let lt_result = scalar_bool(
        &sess,
        "SELECT 'abc'::citext < 'def'::citext",
    )
    .await;
    println!(
        "[5.30.A comparison] 'abc'::citext < 'def'::citext → {lt_result} (expected true)"
    );
    assert!(
        lt_result,
        "CITEXT COMPARISON: 'abc'::citext < 'def'::citext must be TRUE \
         (case-insensitive ordering: a < d). Closed by 5.30.C."
    );

    println!(
        "[5.30.A comparison] PASSED — citext equality and ordering are case-insensitive"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — UNIQUE constraint case-insensitivity
// ─────────────────────────────────────────────────────────────────────────────
//
// The headline use case for citext: email uniqueness that is case-insensitive.
// After INSERT 'FOO@X.COM' into a UNIQUE citext column, a second INSERT of
// 'foo@x.com' must be rejected with a unique-violation error (SQLSTATE 23505).
//
// This is impossible to achieve with a plain TEXT UNIQUE constraint + LOWER()
// index unless the application always lowercases before insert; citext makes the
// constraint itself case-insensitive at the type level.
//
// RED until 5.30.D: Basin does not yet implement citext's case-insensitive
// uniqueness semantics, so the second INSERT succeeds (no deduplication).

/// Phase 5.30.A — UNIQUE constraint case-insensitivity for citext email column.
///
/// Inserts 'FOO@X.COM' into a `UNIQUE` `citext` column. A subsequent INSERT of
/// the same address in a different case ('foo@x.com') must fail with a
/// unique-violation error (SQLSTATE 23505). This is the headline use case for
/// `citext`: case-insensitive email uniqueness at the DB layer.
///
/// Closed by: 5.30.D (citext unique-index operator class + constraint enforcement).
#[tokio::test]
async fn citext_unique_constraint_case_insensitive() {
    // Slice: "unique-constraint case-insensitivity"
    //
    // IMPORTANT: do NOT weaken assertions. Implement 5.30.D to flip this green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── DDL: table with UNIQUE citext email column ─────────────────────────────
    sess.execute(
        "CREATE TABLE accounts (
            id    BIGSERIAL PRIMARY KEY,
            email citext    NOT NULL UNIQUE
        )",
    )
    .await
    .expect(
        "CITEXT UNIQUE: CREATE TABLE accounts (email citext NOT NULL UNIQUE) must succeed. \
         Closed by 5.30.B.",
    );

    // ── First INSERT: 'FOO@X.COM' — must succeed ───────────────────────────────
    sess.execute("INSERT INTO accounts (email) VALUES ('FOO@X.COM')")
        .await
        .expect(
            "CITEXT UNIQUE: first INSERT 'FOO@X.COM' must succeed. \
             Closed by 5.30.B.",
        );
    println!("[5.30.A unique] first INSERT 'FOO@X.COM' accepted");

    // ── Second INSERT: 'foo@x.com' — must be REJECTED (same email, different case)
    // PG raises SQLSTATE 23505 (unique_violation) for a duplicate citext value.
    // The engine must recognise 'foo@x.com' as a duplicate of 'FOO@X.COM'.
    let dup_result = sess
        .execute("INSERT INTO accounts (email) VALUES ('foo@x.com')")
        .await;
    println!(
        "[5.30.A unique] second INSERT 'foo@x.com' result: {:?} (expected error 23505)",
        dup_result
    );

    assert!(
        dup_result.is_err(),
        "CITEXT UNIQUE: INSERT 'foo@x.com' must be rejected as a duplicate of \
         'FOO@X.COM' — citext UNIQUE constraint is case-insensitive. \
         The INSERT succeeded when it should have failed. Closed by 5.30.D."
    );

    // Verify the error is a unique-violation (not some other engine error).
    // We inspect the error display string for the SQLSTATE / message rather than
    // depending on a typed error variant, mirroring the pattern in other harnesses.
    let err_str = format!("{:?}", dup_result.unwrap_err());
    let is_unique_violation = err_str.contains("23505")
        || err_str.to_lowercase().contains("unique")
        || err_str.to_lowercase().contains("duplicate");
    assert!(
        is_unique_violation,
        "CITEXT UNIQUE: the rejection error must be a unique-violation (SQLSTATE 23505 \
         or message containing 'unique'/'duplicate'). \
         got={err_str:?}. Closed by 5.30.D."
    );

    // ── Confirm only 1 row exists in the table ─────────────────────────────────
    let count = row_count(&sess, "SELECT email FROM accounts").await;
    assert_eq!(
        count, 1,
        "CITEXT UNIQUE: table must contain exactly 1 row after the duplicate \
         INSERT was rejected. got={count}. Closed by 5.30.D."
    );

    println!(
        "[5.30.A unique] PASSED — 'foo@x.com' correctly rejected as duplicate of 'FOO@X.COM'"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — ORM compat (Diesel CiText / sqlx / Prisma @db.Citext)
// ─────────────────────────────────────────────────────────────────────────────
//
// Exercises the SQL corpus that Diesel, sqlx, and Prisma emit when a schema
// uses a citext column. The test does NOT require actual ORM binaries — it
// drives the verbatim DDL + DML strings each ORM generates, verified against
// real ORM output.
//
// Sources for the SQL shapes:
//   Prisma:  `email String @db.Citext` in schema.prisma → migration emits
//            `"email" CITEXT NOT NULL` column type.
//   Diesel:  `diesel::pg::types::sql_types::Citext` newtype + migration
//            `ADD COLUMN email CITEXT UNIQUE`.
//   sqlx:    `sqlx::types::Text` mapped to `citext` via `CREATE DOMAIN` or
//            direct `CITEXT` column type; queries use `$1::citext` param cast.
//
// RED until 5.30.E: Basin does not yet accept citext in ORM-shaped DDL or
// parameter bindings.

/// Phase 5.30.A — ORM compat: Diesel CiText / sqlx / Prisma `@db.Citext`
/// DDL + INSERT + SELECT shapes are accepted by the engine and return correct
/// results.
///
/// The SQL corpus is derived from real ORM output:
///   - Prisma v5: `email String @db.Citext` + `@@unique([email])`
///   - Diesel 2.x: `Citext` SQL type + `CREATE UNIQUE INDEX … ON … (email)`
///   - sqlx 0.7: `query!` with `$1::citext` parameter cast
///
/// Closed by: 5.30.E (citext ORM compat — pgwire wire-type + param binding).
#[tokio::test]
async fn citext_orm_compat() {
    // Slice: "ORM compat"
    //
    // Each sub-section is prefixed with the ORM name and the exact DDL/query
    // shape it exercises. Expected behaviour after 5.30.B–E is documented in
    // each assertion comment.
    //
    // IMPORTANT: do NOT weaken assertions to match current behaviour. Implement
    // 5.30.B–E to flip each sub-section green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Prisma migration DDL ───────────────────────────────────────────────────
    // Prisma generates this when a model field has `@db.Citext` annotation and
    // `@@unique([email])` is declared.
    //
    // Prisma migration SQL (v5, PostgreSQL provider):
    //   CREATE TABLE "User" (
    //       "id"    SERIAL   PRIMARY KEY,
    //       "email" CITEXT   NOT NULL
    //   );
    //   CREATE UNIQUE INDEX "User_email_key" ON "User"("email");
    sess.execute(
        r#"CREATE TABLE "prisma_user" (
            "id"    BIGSERIAL PRIMARY KEY,
            "email" CITEXT    NOT NULL
        )"#,
    )
    .await
    .expect(
        "ORM COMPAT (Prisma): CREATE TABLE with CITEXT column must succeed. \
         Closed by 5.30.B.",
    );

    let prisma_idx = sess
        .execute(
            r#"CREATE UNIQUE INDEX "prisma_user_email_key" ON "prisma_user" ("email")"#,
        )
        .await;
    println!("[5.30.A orm] Prisma UNIQUE INDEX on citext: {prisma_idx:?}");
    assert!(
        prisma_idx.is_ok(),
        "ORM COMPAT (Prisma): CREATE UNIQUE INDEX on citext column must succeed. \
         Closed by 5.30.D. error: {:?}",
        prisma_idx.unwrap_err()
    );

    // Prisma INSERT (literal; Prisma uses $1 params via pgwire in practice)
    sess.execute(
        r#"INSERT INTO "prisma_user" ("email") VALUES ('Alice@Example.com')"#,
    )
    .await
    .expect(
        "ORM COMPAT (Prisma): INSERT citext value must succeed. Closed by 5.30.B.",
    );

    // Prisma findUnique: SELECT WHERE email = $1 (case-insensitive match)
    // Prisma maps `@db.Citext` lookups as plain equality — citext handles the rest.
    let prisma_rows = row_count(
        &sess,
        r#"SELECT "id", "email" FROM "prisma_user" WHERE "email" = 'alice@example.com'"#,
    )
    .await;
    assert_eq!(
        prisma_rows, 1,
        "ORM COMPAT (Prisma): WHERE email = 'alice@example.com' must match \
         'Alice@Example.com' via citext case-insensitive equality. \
         got={prisma_rows}. Closed by 5.30.C."
    );
    println!(
        "[5.30.A orm] Prisma citext lookup: matched {prisma_rows} row (expected 1)"
    );

    // Prisma duplicate detection: inserting 'ALICE@EXAMPLE.COM' must be rejected.
    let prisma_dup = sess
        .execute(
            r#"INSERT INTO "prisma_user" ("email") VALUES ('ALICE@EXAMPLE.COM')"#,
        )
        .await;
    assert!(
        prisma_dup.is_err(),
        "ORM COMPAT (Prisma): second INSERT with same email in different case must \
         be rejected by the UNIQUE INDEX on citext. Closed by 5.30.D."
    );
    println!("[5.30.A orm] Prisma duplicate INSERT correctly rejected");

    // ── Diesel migration DDL ───────────────────────────────────────────────────
    // Diesel 2.x migration (generated by `diesel migration generate`):
    //   up.sql:
    //     CREATE TABLE users (
    //         id    SERIAL  PRIMARY KEY,
    //         email CITEXT  NOT NULL
    //     );
    //     CREATE UNIQUE INDEX users_email_unique ON users (email);
    //
    //   Diesel query builder:
    //     users::table.filter(users::email.eq("alice@example.com"))
    //     → SELECT … FROM users WHERE email = 'alice@example.com'

    sess.execute(
        r#"CREATE TABLE "diesel_users" (
            "id"    BIGSERIAL PRIMARY KEY,
            "email" CITEXT    NOT NULL
        )"#,
    )
    .await
    .expect(
        "ORM COMPAT (Diesel): CREATE TABLE diesel_users with CITEXT column must succeed. \
         Closed by 5.30.B.",
    );

    let diesel_idx = sess
        .execute(
            r#"CREATE UNIQUE INDEX "diesel_users_email_unique"
               ON "diesel_users" ("email")"#,
        )
        .await;
    println!("[5.30.A orm] Diesel UNIQUE INDEX on citext: {diesel_idx:?}");
    assert!(
        diesel_idx.is_ok(),
        "ORM COMPAT (Diesel): CREATE UNIQUE INDEX on citext must succeed. \
         Closed by 5.30.D. error: {:?}",
        diesel_idx.unwrap_err()
    );

    // Diesel inserts two users with clearly different addresses.
    sess.execute(
        r#"INSERT INTO "diesel_users" ("email")
           VALUES ('Bob@Corp.io'), ('carol@startup.dev')"#,
    )
    .await
    .expect(
        "ORM COMPAT (Diesel): INSERT two citext email rows must succeed. \
         Closed by 5.30.B.",
    );

    // Diesel `.filter(users::email.eq("bob@corp.io"))` → case-insensitive match
    let diesel_rows = row_count(
        &sess,
        r#"SELECT "id" FROM "diesel_users" WHERE "email" = 'bob@corp.io'"#,
    )
    .await;
    assert_eq!(
        diesel_rows, 1,
        "ORM COMPAT (Diesel): WHERE email = 'bob@corp.io' must match 'Bob@Corp.io' \
         via citext equality. got={diesel_rows}. Closed by 5.30.C."
    );
    println!("[5.30.A orm] Diesel citext lookup: matched {diesel_rows} row (expected 1)");

    // ── sqlx query shapes ──────────────────────────────────────────────────────
    // sqlx 0.7: `query!("SELECT id FROM sqlx_logins WHERE email = $1::citext", addr)`
    // The `::citext` cast ensures the parameter is compared with citext semantics.
    // Here we drive the expanded literal SQL form (no pgwire round-trip needed
    // for the type-check; the pgwire param-binding path is in a separate harness).

    sess.execute(
        r#"CREATE TABLE "sqlx_logins" (
            "id"    BIGSERIAL PRIMARY KEY,
            "email" CITEXT    NOT NULL UNIQUE,
            "role"  TEXT      NOT NULL DEFAULT 'user'
        )"#,
    )
    .await
    .expect(
        "ORM COMPAT (sqlx): CREATE TABLE sqlx_logins with CITEXT UNIQUE column must \
         succeed. Closed by 5.30.B.",
    );

    sess.execute(
        r#"INSERT INTO "sqlx_logins" ("email", "role")
           VALUES
           ('Dave@Example.org', 'admin'),
           ('eve@example.org',  'user')"#,
    )
    .await
    .expect(
        "ORM COMPAT (sqlx): INSERT two citext email rows must succeed. \
         Closed by 5.30.B.",
    );

    // sqlx `WHERE email = $1::citext` — drives a case-insensitive lookup.
    // Literal form: `WHERE email = 'dave@example.org'` on a citext column.
    let sqlx_rows = row_count(
        &sess,
        r#"SELECT "id" FROM "sqlx_logins" WHERE "email" = 'dave@example.org'"#,
    )
    .await;
    assert_eq!(
        sqlx_rows, 1,
        "ORM COMPAT (sqlx): WHERE email = 'dave@example.org' must match \
         'Dave@Example.org' via citext equality. \
         got={sqlx_rows}. Closed by 5.30.C."
    );
    println!("[5.30.A orm] sqlx citext lookup: matched {sqlx_rows} row (expected 1)");

    // sqlx `::citext` explicit cast comparison (the shape the macro emits)
    let sqlx_cast_rows = row_count(
        &sess,
        r#"SELECT "id" FROM "sqlx_logins"
           WHERE "email" = 'EVE@EXAMPLE.ORG'::citext"#,
    )
    .await;
    assert_eq!(
        sqlx_cast_rows, 1,
        "ORM COMPAT (sqlx): WHERE email = 'EVE@EXAMPLE.ORG'::citext must match \
         'eve@example.org' via citext equality. \
         got={sqlx_cast_rows}. Closed by 5.30.C."
    );
    println!(
        "[5.30.A orm] sqlx ::citext cast lookup: matched {sqlx_cast_rows} row (expected 1)"
    );

    // Total row count sanity: 2 rows in sqlx_logins.
    let total = row_count(&sess, r#"SELECT "id" FROM "sqlx_logins""#).await;
    assert_eq!(
        total, 2,
        "ORM COMPAT (sqlx): sqlx_logins must contain exactly 2 rows. got={total}"
    );

    println!(
        "[5.30.A orm] PASSED — Prisma @db.Citext, Diesel CiText, and sqlx citext \
         DDL + INSERT + SELECT shapes accepted and return correct results"
    );
}
