//! End-to-end check that enum-typed columns sort and range-compare in
//! declaration order (PG semantics) rather than alphabetical (Arrow's
//! default Utf8 compare).
//!
//! The planner-side rewrite lives in `enum_ordinal::rewrite_enum_ordering`
//! — these tests register an enum, populate a table, and assert the
//! observed result row order / filter behaviour reflects the rewrite.

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
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

/// Pull the `status` text values out of a `SELECT status ...` result, in
/// row order, batch-concatenated.
fn collect_status_column(res: ExecResult) -> Vec<Option<String>> {
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        // Find the `status` column by name (case-insensitively).
        let idx = (0..b.num_columns())
            .find(|i| b.schema().field(*i).name().eq_ignore_ascii_case("status"))
            .expect("status column not present");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status must be Utf8");
        for r in 0..arr.len() {
            if arr.is_null(r) {
                out.push(None);
            } else {
                out.push(Some(arr.value(r).to_string()));
            }
        }
    }
    out
}

#[tokio::test]
async fn order_by_enum_uses_declaration_order() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('pending', 'paid', 'shipped', 'cancelled')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, status status NOT NULL)")
        .await
        .unwrap();
    // Insert in random (non-alphabetical, non-declaration) order so
    // neither natural insertion order nor lex order would accidentally
    // yield the right answer.
    sess.execute(
        "INSERT INTO t VALUES (3, 'shipped'), (1, 'pending'), (4, 'cancelled'), (2, 'paid')",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT status FROM t ORDER BY status")
        .await
        .unwrap();
    let got = collect_status_column(res);
    let expected: Vec<Option<String>> = ["pending", "paid", "shipped", "cancelled"]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect();
    assert_eq!(
        got, expected,
        "ORDER BY enum must follow declaration order, not alphabetical"
    );
}

#[tokio::test]
async fn order_by_enum_desc() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('pending', 'paid', 'shipped', 'cancelled')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, status status NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t VALUES (1, 'pending'), (2, 'paid'), (3, 'shipped'), (4, 'cancelled')",
    )
    .await
    .unwrap();

    let res = sess
        .execute("SELECT status FROM t ORDER BY status DESC")
        .await
        .unwrap();
    let got = collect_status_column(res);
    let expected: Vec<Option<String>> = ["cancelled", "shipped", "paid", "pending"]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect();
    assert_eq!(
        got, expected,
        "ORDER BY enum DESC must reverse declaration order"
    );
}

#[tokio::test]
async fn range_filter_uses_ordinal() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('pending', 'paid', 'shipped', 'cancelled')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, status status NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t VALUES (1, 'pending'), (2, 'paid'), (3, 'shipped'), (4, 'cancelled')",
    )
    .await
    .unwrap();

    // status > 'pending' → rows whose status appears AFTER 'pending'
    // in the declared list: paid, shipped, cancelled (ordinals 1, 2, 3).
    let res = sess
        .execute("SELECT status FROM t WHERE status > 'pending' ORDER BY status")
        .await
        .unwrap();
    let got = collect_status_column(res);
    let expected: Vec<Option<String>> = ["paid", "shipped", "cancelled"]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect();
    assert_eq!(
        got, expected,
        "WHERE status > 'pending' must yield ordinal-after-'pending' rows, not lexicographic"
    );
}

#[tokio::test]
async fn equality_filter_unaffected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('pending', 'paid', 'shipped', 'cancelled')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, status status NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'pending'), (2, 'paid'), (3, 'shipped'), (4, 'paid')")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT status FROM t WHERE status = 'paid'")
        .await
        .unwrap();
    let got = collect_status_column(res);
    assert_eq!(got.len(), 2, "expected 2 paid rows, got {got:?}");
    assert!(got.iter().all(|s| s.as_deref() == Some("paid")));
}

/// The same ordering guarantee over the EXTENDED (prepared) protocol.
///
/// `SELECT status FROM t ORDER BY status` carries no rewrite-pipeline marker
/// token, so the prepared path judged it "pipeline is a no-op" and dispatched
/// the parsed AST directly — bypassing the enum ordinal rewrite that the
/// simple-protocol path applies. Without that accounted for, the three tests
/// above pass while `prepare` + `bind` + `execute_bound` still sorts
/// alphabetically: the fix would hold on one protocol and silently not on the
/// other.
#[tokio::test]
async fn order_by_enum_declaration_order_over_prepared_statement() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TYPE status AS ENUM ('pending', 'paid', 'shipped', 'cancelled')")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, status status NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t VALUES (3, 'shipped'), (1, 'pending'), (4, 'cancelled'), (2, 'paid')",
    )
    .await
    .unwrap();

    let (handle, _schema) = sess
        .prepare("SELECT status FROM t ORDER BY status")
        .await
        .unwrap();
    let bound = sess.bind(&handle, Vec::new()).await.unwrap();
    let res = sess.execute_bound(bound).await.unwrap();

    let got = collect_status_column(res);
    let expected: Vec<Option<String>> = ["pending", "paid", "shipped", "cancelled"]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect();
    assert_eq!(
        got, expected,
        "prepared ORDER BY enum must follow declaration order, not alphabetical"
    );
}
