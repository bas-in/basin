//! `lower()` / `upper()` must dispatch on ARGUMENT TYPE, not on content.
//!
//! PostgreSQL has two unrelated families of functions under these names:
//! `lower(anyrange)` / `upper(anyrange)` (range bound accessors) and
//! `lower(text)` / `upper(text)` (case conversion). PG picks between them by
//! the argument's TYPE.
//!
//! Basin stores range values as `Utf8` JSON strings
//! (`{"l":…,"u":…,"li":…,"ui":…}`), so `range_udf.rs` originally picked the
//! range branch whenever the string started with `{` and parsed as that JSON.
//! That silently mis-read ordinary text: any `{`-leading value — a JSON
//! document, a brace-quoted string, an array literal — could be taken for a
//! range. The worst case round-tripped a value that *is* Basin's own range
//! encoding:
//!
//! ```text
//! SELECT lower('{"l":1,"u":5,"li":true,"ui":false}');
//!   postgres: {"l":1,"u":5,"li":true,"ui":false}
//!   basin:    1                                    ← wrong answer
//! ```
//!
//! The dispatch now reads the argument's Arrow `Field` metadata (`BASIN_TYPE`),
//! which DDL stamps on declared range columns and which the range-producing
//! UDFs stamp on their own output. Text has no such marker and always takes
//! the case-conversion branch.
//!
//! EVERY expected value below was taken from a live PostgreSQL 18.2 server
//! (`postgres://…/postgres`), not from memory. The pinning query is recorded
//! above each group.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        Ok(other) => panic!("expected rows from {sql:?}, got {other:?}"),
        Err(e) => panic!("SQL failed: {sql:?}\n  error: {e}"),
    }
}

/// Run a statement that returns no rows (DDL / DML), asserting it succeeded.
async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql:?}\n  error: {e}"));
}

/// First column of the first row as `Option<String>` (`None` == SQL NULL).
async fn scalar(sess: &ProjectSession, sql: &str) -> Option<String> {
    let batches = rows(sess, sql).await;
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from {sql:?}"));
    let arr = b
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("expected Utf8 first column from {sql:?}"));
    if arr.is_null(0) {
        None
    } else {
        Some(arr.value(0).to_string())
    }
}

async fn open(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Text that looks like Basin's own range encoding
//
// Live PG 18.2:
//   SELECT lower('{"l":1,"u":5,"li":true,"ui":false}');
//     → {"l":1,"u":5,"li":true,"ui":false}
//   SELECT upper('{"l":1,"u":5,"li":true,"ui":false}');
//     → {"L":1,"U":5,"LI":TRUE,"UI":FALSE}
// ─────────────────────────────────────────────────────────────────────────────

/// The exact reported bug. Before the type-driven dispatch this returned `1`.
#[tokio::test]
async fn lower_over_text_shaped_like_a_range_is_case_conversion_not_a_bound() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let got = scalar(
        &sess,
        r#"SELECT lower('{"l":1,"u":5,"li":true,"ui":false}')"#,
    )
    .await;

    assert_eq!(
        got.as_deref(),
        Some(r#"{"l":1,"u":5,"li":true,"ui":false}"#),
        "lower(text) is case conversion; a text literal that happens to be \
         shaped like Basin's range JSON is still TEXT (live PG 18.2)"
    );
}

/// The `upper` half of the same bug. Before the change this returned `5`.
#[tokio::test]
async fn upper_over_text_shaped_like_a_range_is_case_conversion_not_a_bound() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let got = scalar(
        &sess,
        r#"SELECT upper('{"l":1,"u":5,"li":true,"ui":false}')"#,
    )
    .await;

    assert_eq!(
        got.as_deref(),
        Some(r#"{"L":1,"U":5,"LI":TRUE,"UI":FALSE}"#),
        "upper(text) uppercases the whole string, keys and all (live PG 18.2)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Ordinary JSON documents
//
// Live PG 18.2:
//   SELECT lower('{"Name":"Ada","Tag":"X"}'); → {"name":"ada","tag":"x"}
//   SELECT upper('{"Name":"Ada","Tag":"X"}'); → {"NAME":"ADA","TAG":"X"}
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lower_and_upper_over_a_json_document_case_convert_it() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let lo = scalar(&sess, r#"SELECT lower('{"Name":"Ada","Tag":"X"}')"#).await;
    let up = scalar(&sess, r#"SELECT upper('{"Name":"Ada","Tag":"X"}')"#).await;

    assert_eq!(
        lo.as_deref(),
        Some(r#"{"name":"ada","tag":"x"}"#),
        "lower() over a JSON string (live PG 18.2)"
    );
    assert_eq!(
        up.as_deref(),
        Some(r#"{"NAME":"ADA","TAG":"X"}"#),
        "upper() over a JSON string (live PG 18.2)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Brace-quoted / array-literal text
//
// Live PG 18.2:
//   SELECT lower('{ABC,def}'); → {abc,def}
//   SELECT upper('{ABC,def}'); → {ABC,DEF}
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lower_and_upper_over_a_brace_leading_literal_case_convert_it() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let lo = scalar(&sess, "SELECT lower('{ABC,def}')").await;
    let up = scalar(&sess, "SELECT upper('{ABC,def}')").await;

    assert_eq!(
        lo.as_deref(),
        Some("{abc,def}"),
        "lower() over an array-literal-shaped text value (live PG 18.2)"
    );
    assert_eq!(
        up.as_deref(),
        Some("{ABC,DEF}"),
        "upper() over an array-literal-shaped text value (live PG 18.2)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Genuine range values — the accessor branch must be untouched
//
// Live PG 18.2:
//   SELECT lower(int4range(5, 20));  → 5
//   SELECT upper(int4range(5, 20));  → 20
//   SELECT lower(int4range(NULL,10));→ NULL   (unbounded below)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn lower_and_upper_over_a_range_constructor_return_the_bounds() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let lo = scalar(&sess, "SELECT lower(int4range(5, 20))").await;
    let up = scalar(&sess, "SELECT upper(int4range(5, 20))").await;

    assert_eq!(
        lo.as_deref(),
        Some("5"),
        "lower(anyrange) is the lower bound (live PG 18.2)"
    );
    assert_eq!(
        up.as_deref(),
        Some("20"),
        "upper(anyrange) is the upper bound (live PG 18.2)"
    );
}

#[tokio::test]
async fn lower_and_upper_over_a_declared_range_column_return_the_bounds() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    exec_ok(
        &sess,
        "CREATE TABLE booking (id BIGSERIAL PRIMARY KEY, r INT4RANGE)",
    )
    .await;
    exec_ok(&sess, "INSERT INTO booking (r) VALUES (int4range(7, 42))").await;

    let lo = scalar(&sess, "SELECT lower(r) FROM booking LIMIT 1").await;
    let up = scalar(&sess, "SELECT upper(r) FROM booking LIMIT 1").await;

    assert_eq!(
        lo.as_deref(),
        Some("7"),
        "a declared INT4RANGE column carries the BASIN_TYPE marker DDL stamped \
         on it, so lower() takes the accessor branch (live PG 18.2: 7)"
    );
    assert_eq!(
        up.as_deref(),
        Some("42"),
        "…and likewise upper() (live PG 18.2: 42)"
    );
}

/// A TEXT column holding `{`-leading rows is text, row by row — the old
/// content heuristic was per-value, so this is the column-shaped form of the
/// same bug.
#[tokio::test]
async fn a_text_column_of_brace_leading_rows_is_case_converted() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    exec_ok(
        &sess,
        "CREATE TABLE docs (id BIGSERIAL PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &sess,
        r#"INSERT INTO docs (body) VALUES ('{"l":1,"u":5,"li":true,"ui":false}')"#,
    )
    .await;

    let lo = scalar(&sess, "SELECT lower(body) FROM docs LIMIT 1").await;

    assert_eq!(
        lo.as_deref(),
        Some(r#"{"l":1,"u":5,"li":true,"ui":false}"#),
        "a TEXT column is text no matter what its rows contain (live PG 18.2)"
    );
}

/// An unbounded bound is SQL NULL, and it must stay NULL rather than falling
/// through to case conversion of the range's JSON encoding.
#[tokio::test]
async fn lower_of_an_unbounded_range_is_null() {
    let dir = TempDir::new().unwrap();
    let sess = open(&engine_in(&dir)).await;

    let lo = scalar(&sess, "SELECT lower(int4range(NULL, 10))").await;

    assert_eq!(
        lo, None,
        "live PG 18.2: SELECT lower(int4range(NULL,10)) is NULL"
    );
}
