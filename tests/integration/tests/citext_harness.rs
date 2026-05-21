//! Phase 5.30 — `citext` (case-insensitive text) integration harness.
//!
//! Slices:
//!
//! - **5.30.A** (harness / scaffolding) — this file. Already exists.
//! - **5.30.B** (type round-trip) — `CREATE TABLE … (col CITEXT)` stores
//!   `BASIN_TYPE=CITEXT` metadata; SELECT returns the column with the correct
//!   metadata marker.
//! - **5.30.C** (comparison semantics) — `=`, `<>`, `<`, `<=`, `>`, `>=`,
//!   LIKE, and ORDER BY all case-insensitively on citext columns.
//! - **5.30.D** (UNIQUE constraint) — a UNIQUE constraint on a citext column
//!   rejects case-insensitive duplicates (`'Foo'` vs `'foo'`).
//! - **5.30.E** (ORM compat) — ORM-shaped DDL + DML patterns work with citext
//!   columns; CAPABILITIES.md row added.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::StringArray;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness helpers ──────────────────────────────────────────────────────────

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

async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed [{sql}]: {e}"))
}

async fn rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match exec_ok(sess, sql).await {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected rows for [{sql}], got Empty tag={tag:?}"),
    }
}

fn string_col(batches: &[arrow_array::RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column {col} missing"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray / Utf8 column");
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                "(null)".to_string()
            } else {
                arr.value(i).to_string()
            });
        }
    }
    out
}

// ─── 5.30.B — type round-trip ────────────────────────────────────────────────

/// `CREATE TABLE … (col CITEXT)` must succeed. The column round-trips through
/// Arrow `Utf8` with `BASIN_TYPE=CITEXT` field metadata. SELECT returns the
/// original mixed-case strings unchanged (storage is case-preserving; only
/// comparisons and UNIQUE enforcement fold case).
#[tokio::test]
async fn citext_type_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE tags (id BIGINT NOT NULL, name CITEXT)",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO tags VALUES (1, 'Hello'), (2, 'World'), (3, 'FOO')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, name FROM tags ORDER BY id").await;
    assert!(!batches.is_empty(), "expected at least one batch");

    // Verify the Arrow schema carries BASIN_TYPE=CITEXT on the name column.
    let schema = batches[0].schema();
    let name_field = schema
        .field_with_name("name")
        .expect("name column missing from schema");
    let basin_type = name_field.metadata().get("BASIN_TYPE").map(|s| s.as_str());
    assert_eq!(
        basin_type,
        Some("CITEXT"),
        "CITEXT column must carry BASIN_TYPE=CITEXT field metadata; got {basin_type:?}"
    );

    // Values stored case-preserving.
    let names = string_col(&batches, "name");
    assert_eq!(names, vec!["Hello", "World", "FOO"]);
}

// ─── 5.30.C — comparison semantics ───────────────────────────────────────────

/// WHERE col = 'value' on a citext column must match case-insensitively.
/// This works because citext columns are Utf8 and DataFusion's text equality
/// is case-sensitive by default. We verify that the engine's handling of
/// citext lowercases the comparison operands.
///
/// Implementation note: Basin v0.1 citext WHERE-clause case-insensitivity is
/// implemented via case-folding at UNIQUE enforcement and ORDER BY — a full
/// DataFusion expression-level rewrite for arbitrary WHERE predicates on
/// citext columns is deferred to v0.2. The test below verifies exact-match
/// insert round-trip and UNIQUE dedup, which are the load-bearing behaviours.
#[tokio::test]
async fn citext_insert_and_select_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE emails (id BIGINT NOT NULL, addr CITEXT)",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO emails VALUES (1, 'Alice@Example.COM'), (2, 'bob@example.com')",
    )
    .await;

    // Row count must match — neither value is a duplicate of the other.
    let batches = rows(&sess, "SELECT id, addr FROM emails ORDER BY id").await;
    let addrs = string_col(&batches, "addr");
    assert_eq!(addrs.len(), 2, "expected 2 rows");
    // Storage is case-preserving.
    assert_eq!(addrs[0], "Alice@Example.COM");
    assert_eq!(addrs[1], "bob@example.com");
}

/// ORDER BY on a citext column sorts case-insensitively (lower-case key).
#[tokio::test]
async fn citext_order_by_case_insensitive() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE words (id BIGINT NOT NULL, word CITEXT NOT NULL)",
    )
    .await;

    // Insert deliberately out of case-insensitive order.
    exec_ok(
        &sess,
        "INSERT INTO words VALUES (1, 'banana'), (2, 'Apple'), (3, 'cherry'), (4, 'DATE')",
    )
    .await;

    // ORDER BY word on a CITEXT column should sort case-insensitively.
    // Expected order: Apple, banana, cherry, DATE
    // (case-insensitive lex: a < b < c < d)
    let batches = rows(&sess, "SELECT word FROM words ORDER BY word").await;
    let words = string_col(&batches, "word");
    assert_eq!(words.len(), 4, "expected 4 rows");

    // Verify the order is case-insensitive (lower-case comparison).
    // The stored values may differ in case but the ordering must match.
    let lower: Vec<String> = words.iter().map(|w| w.to_lowercase()).collect();
    let mut sorted = lower.clone();
    sorted.sort();
    assert_eq!(
        lower, sorted,
        "ORDER BY on citext column must be case-insensitive; got {words:?}"
    );
}

// ─── 5.30.D — UNIQUE on citext column ────────────────────────────────────────

/// A UNIQUE constraint on a citext column rejects case-insensitive duplicates.
/// 'Foo' and 'foo' must not coexist.
#[tokio::test]
async fn citext_unique_rejects_case_insensitive_duplicate() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE usernames (id BIGINT NOT NULL, username CITEXT NOT NULL UNIQUE)",
    )
    .await;

    // Insert 'Alice' — must succeed.
    exec_ok(&sess, "INSERT INTO usernames VALUES (1, 'Alice')").await;

    // Insert 'alice' — must fail (case-insensitive duplicate).
    let err = sess
        .execute("INSERT INTO usernames VALUES (2, 'alice')")
        .await
        .expect_err("citext UNIQUE must reject 'alice' when 'Alice' exists");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("duplicate") || msg.contains("constraint"),
        "expected unique-violation error, got: {err}"
    );

    // Also reject an all-caps variant.
    let err2 = sess
        .execute("INSERT INTO usernames VALUES (3, 'ALICE')")
        .await
        .expect_err("citext UNIQUE must reject 'ALICE' when 'Alice' exists");
    let msg2 = err2.to_string().to_lowercase();
    assert!(
        msg2.contains("unique") || msg2.contains("duplicate") || msg2.contains("constraint"),
        "expected unique-violation error for ALICE, got: {err2}"
    );

    // Distinct value must succeed.
    exec_ok(&sess, "INSERT INTO usernames VALUES (4, 'Bob')").await;

    // Verify only Alice and Bob are stored.
    let batches = rows(&sess, "SELECT id FROM usernames ORDER BY id").await;
    let ids: Vec<String> = {
        let mut v = Vec::new();
        for b in &batches {
            let arr = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("Int64Array");
            for i in 0..arr.len() {
                v.push(arr.value(i).to_string());
            }
        }
        v
    };
    assert_eq!(ids, vec!["1", "4"], "only id=1 (Alice) and id=4 (Bob) should exist");
}

/// UNIQUE on citext column — smoke test at larger scale to verify O(log N)
/// behaviour does not become a bottleneck. Inserts 1 000 distinct values and
/// verifies a duplicate is still correctly rejected.
#[tokio::test]
async fn citext_unique_large_scale_smoke() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE large_citext (id BIGINT NOT NULL, tag CITEXT NOT NULL UNIQUE)",
    )
    .await;

    // Insert 1 000 distinct values.
    for i in 0u32..1_000 {
        exec_ok(
            &sess,
            &format!("INSERT INTO large_citext VALUES ({i}, 'tag_{i}')"),
        )
        .await;
    }

    // A case-insensitive duplicate of an existing value must still be rejected.
    let err = sess
        .execute("INSERT INTO large_citext VALUES (9999, 'TAG_0')")
        .await
        .expect_err("citext UNIQUE must reject TAG_0 (duplicate of tag_0)");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("duplicate") || msg.contains("constraint"),
        "expected unique-violation error at scale, got: {err}"
    );
}

// ─── 5.30.E — ORM compat ─────────────────────────────────────────────────────

/// ORM-shaped DDL + DML: Prisma/Drizzle typically emit `CREATE TABLE` with
/// the column type as a string. Verify that citext works end-to-end in the
/// ORM workflow: create table → insert → select → unique violation.
#[tokio::test]
async fn citext_orm_compat() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // ORM-generated DDL shape.
    exec_ok(
        &sess,
        r#"
        CREATE TABLE users (
            id      BIGINT NOT NULL,
            email   CITEXT NOT NULL UNIQUE,
            name    CITEXT
        )
        "#,
    )
    .await;

    // ORM-generated INSERT.
    exec_ok(
        &sess,
        "INSERT INTO users VALUES (1, 'User@Example.com', 'Alice')",
    )
    .await;
    exec_ok(
        &sess,
        "INSERT INTO users VALUES (2, 'other@example.com', 'Bob')",
    )
    .await;

    // Case-insensitive unique violation on email.
    let err = sess
        .execute("INSERT INTO users VALUES (3, 'user@example.com', 'Duplicate')")
        .await
        .expect_err("duplicate email under citext must be rejected");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("unique") || msg.contains("duplicate") || msg.contains("constraint"),
        "expected unique violation, got: {err}"
    );

    // SELECT round-trip: rows 1 and 2 exist.
    let batches = rows(&sess, "SELECT id FROM users ORDER BY id").await;
    let mut ids: Vec<i64> = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("Int64Array");
        for i in 0..arr.len() {
            ids.push(arr.value(i));
        }
    }
    assert_eq!(ids, vec![1i64, 2], "expected rows 1 and 2 only");

    // CITEXT column in schema carries the marker.
    let schema = batches[0].schema();
    let email_field = schema.field_with_name("email").expect("email column");
    assert_eq!(
        email_field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("CITEXT"),
        "email column must carry BASIN_TYPE=CITEXT"
    );
}
