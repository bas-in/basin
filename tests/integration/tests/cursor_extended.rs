//! Extended cursor gap tests.
//!
//! Supplements the existing `cursor_lifecycle.rs` tests (declare/fetch/close/move)
//! with coverage for the gaps fixed in this patch:
//!
//!   1. FETCH past the end of a cursor → 0 rows (not an error)
//!   2. CLOSE ALL → clears every open cursor in the session
//!   3. CLOSE / FETCH on an unknown cursor → BasinError with SQLSTATE 34000
//!   4. Transaction-end semantics (v0.1: cursors are session-scoped)
//!   5. DECLARE … WITH HOLD → FeatureNotSupported (SQLSTATE 0A000)
//!   6. BASIN_CURSOR_MAX_ROWS row-cap enforcement (conditional on env var)
//!   7. Django / psycopg2 server-side cursor shape (BEGIN + DECLARE NO SCROLL
//!      + chunked FETCH + CLOSE + COMMIT)
//!
//! Place this file as `tests/integration/tests/cursor_extended.rs` in the
//! live tree.  The mirror path is
//! `/tmp/basin_composite_cursors/testing/tests/cursor_extended.rs`.

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{BasinError, ProjectId};
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
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn row_count(result: &ExecResult) -> usize {
    match result {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

fn assert_empty(result: &ExecResult, expected_tag: &str) {
    match result {
        ExecResult::Empty { tag } => {
            assert_eq!(
                tag.as_str(),
                expected_tag,
                "expected tag {expected_tag:?}, got {tag:?}"
            );
        }
        ExecResult::Rows { batches, .. } => {
            panic!(
                "expected Empty {{ tag: {expected_tag:?} }} but got {} rows",
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 1. FETCH past the end → 0 rows (not an error)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_past_end_returns_zero_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE nums (id BIGINT NOT NULL)").await.unwrap();
    sess.execute("INSERT INTO nums VALUES (1), (2), (3)").await.unwrap();

    sess.execute("DECLARE c CURSOR FOR SELECT id FROM nums ORDER BY id")
        .await
        .unwrap();

    // Drain all rows.
    let res = sess.execute("FETCH ALL FROM c").await.unwrap();
    assert_eq!(row_count(&res), 3, "should return all 3 rows");

    // A further FETCH must return 0 rows, not an error.
    let res = sess.execute("FETCH 5 FROM c").await.unwrap();
    assert_eq!(row_count(&res), 0, "FETCH past end must return 0 rows, not an error");

    sess.execute("CLOSE c").await.unwrap();
}

// ---------------------------------------------------------------------------
// 2. CLOSE ALL → clears every open cursor; subsequent FETCH must fail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_all_clears_all_cursors() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE vals (n BIGINT NOT NULL)").await.unwrap();
    sess.execute("INSERT INTO vals VALUES (1), (2)").await.unwrap();

    sess.execute("DECLARE c1 CURSOR FOR SELECT n FROM vals ORDER BY n")
        .await
        .unwrap();
    sess.execute("DECLARE c2 CURSOR FOR SELECT n FROM vals ORDER BY n")
        .await
        .unwrap();

    // CLOSE ALL must succeed.
    let res = sess.execute("CLOSE ALL").await.unwrap();
    assert_empty(&res, "CLOSE");

    // Both cursors must now be gone (SQLSTATE 34000 = CursorNotFound).
    let err1 = sess.execute("FETCH NEXT FROM c1").await;
    assert!(
        matches!(err1, Err(BasinError::CursorNotFound(_))),
        "c1 should be gone after CLOSE ALL, got {err1:?}"
    );
    let err2 = sess.execute("FETCH NEXT FROM c2").await;
    assert!(
        matches!(err2, Err(BasinError::CursorNotFound(_))),
        "c2 should be gone after CLOSE ALL, got {err2:?}"
    );
}

// ---------------------------------------------------------------------------
// 3a. CLOSE unknown cursor → BasinError::CursorNotFound (SQLSTATE 34000)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_unknown_cursor_is_34000() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess.execute("CLOSE no_such_cursor").await.unwrap_err();
    assert!(
        matches!(err, BasinError::CursorNotFound(_)),
        "expected CursorNotFound (SQLSTATE 34000), got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("no_such_cursor"),
        "error message must name the cursor; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// 3b. FETCH on unknown cursor → BasinError::CursorNotFound (SQLSTATE 34000)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_unknown_cursor_is_34000() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess.execute("FETCH NEXT FROM ghost_cursor").await.unwrap_err();
    assert!(
        matches!(err, BasinError::CursorNotFound(_)),
        "expected CursorNotFound (SQLSTATE 34000), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// 4. Transaction-end semantics (v0.1: session-scoped, survive ROLLBACK)
//
// In Basin v0.1 cursors are session-scoped, not transaction-scoped.  A cursor
// declared inside a transaction that is subsequently rolled back remains
// accessible.  This is documented as a known limitation; "automatically
// closed at transaction end" is deferred to v0.2.  This test pins the
// current (v0.1) behaviour so any future regression is caught immediately.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_is_session_scoped_survives_rollback() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE items (id BIGINT NOT NULL)").await.unwrap();
    sess.execute("INSERT INTO items VALUES (10), (20), (30)").await.unwrap();

    // Declare inside an explicit transaction, then roll back.
    sess.execute("BEGIN").await.unwrap();
    sess.execute("DECLARE cur CURSOR FOR SELECT id FROM items ORDER BY id")
        .await
        .unwrap();
    sess.execute("ROLLBACK").await.unwrap();

    // v0.1: cursor still accessible after ROLLBACK.
    let res = sess.execute("FETCH 2 FROM cur").await.unwrap();
    assert_eq!(
        row_count(&res),
        2,
        "cursor must still be accessible after ROLLBACK (v0.1 session-scoped behaviour)"
    );

    sess.execute("CLOSE cur").await.unwrap();
}

// ---------------------------------------------------------------------------
// 5. DECLARE … WITH HOLD → FeatureNotSupported (SQLSTATE 0A000)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn declare_with_hold_is_feature_not_supported() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (n BIGINT NOT NULL)").await.unwrap();
    sess.execute("INSERT INTO t VALUES (1)").await.unwrap();

    let err = sess
        .execute("DECLARE c CURSOR WITH HOLD FOR SELECT n FROM t")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::FeatureNotSupported(_)),
        "WITH HOLD must raise FeatureNotSupported (SQLSTATE 0A000), got {err:?}"
    );
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("with hold"),
        "error message must mention WITH HOLD; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// 6. BASIN_CURSOR_MAX_ROWS row-cap enforcement
//
// This test is conditional: it only runs when the caller sets
// BASIN_CURSOR_MAX_ROWS to a small integer (e.g. 3) so that inserting
// cap+1 rows and then declaring a cursor crosses the ceiling.
// Run isolated as:
//   BASIN_CURSOR_MAX_ROWS=3 cargo test -p basin-integration-tests cursor_row_cap_enforced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_row_cap_enforced_when_env_set() {
    let cap: usize = match std::env::var("BASIN_CURSOR_MAX_ROWS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
    {
        Some(0) | None => return, // cap disabled or env var absent — skip
        Some(n) => n,
    };

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE capped (n BIGINT NOT NULL)")
        .await
        .unwrap();

    // Insert cap+1 rows to cross the ceiling.
    let values: String = (0..=(cap as i64))
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(", ");
    sess.execute(&format!("INSERT INTO capped VALUES {values}"))
        .await
        .unwrap();

    let err = sess
        .execute("DECLARE c CURSOR FOR SELECT n FROM capped")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::QueryCostExceeded(_)),
        "DECLARE above the row cap must raise QueryCostExceeded (SQLSTATE 54000), got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("BASIN_CURSOR_MAX_ROWS"),
        "error message must mention BASIN_CURSOR_MAX_ROWS; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// 7. Django / psycopg2 server-side cursor shape
//
// Django's QuerySet.iterator(chunk_size=N) sends exactly this sequence:
//
//   BEGIN;
//   DECLARE <name> NO SCROLL CURSOR FOR <select>;
//   FETCH <N> FROM <name>;
//   [FETCH <N> FROM <name>; ...]   (repeated until 0 rows returned)
//   CLOSE <name>;
//   COMMIT;
//
// The cursor name includes the connection thread-id in real psycopg2 so
// we use an underscore-containing name to match that shape.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn django_server_side_cursor_shape() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE products (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO products VALUES \
         (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma'), (4, 'Delta'), (5, 'Epsilon')",
    )
    .await
    .unwrap();

    // Django emits BEGIN before the first DECLARE.
    sess.execute("BEGIN").await.unwrap();

    // Django uses NO SCROLL; Basin must accept it without error.
    let res = sess
        .execute(
            "DECLARE _django_curs_1234_5 NO SCROLL CURSOR \
             FOR SELECT id, name FROM products ORDER BY id",
        )
        .await
        .unwrap();
    assert_empty(&res, "DECLARE");

    // chunk_size=2 — first chunk.
    let res = sess.execute("FETCH 2 FROM _django_curs_1234_5").await.unwrap();
    assert_eq!(row_count(&res), 2, "first chunk: 2 rows");

    // Second chunk.
    let res = sess.execute("FETCH 2 FROM _django_curs_1234_5").await.unwrap();
    assert_eq!(row_count(&res), 2, "second chunk: 2 rows");

    // Third (partial) chunk: only 1 row remains.
    let res = sess.execute("FETCH 2 FROM _django_curs_1234_5").await.unwrap();
    assert_eq!(row_count(&res), 1, "third chunk: 1 remaining row");

    // Fourth chunk: exhausted — returns 0 rows.  Django stops here.
    let res = sess.execute("FETCH 2 FROM _django_curs_1234_5").await.unwrap();
    assert_eq!(row_count(&res), 0, "fourth chunk: 0 rows (exhausted, not an error)");

    let res = sess.execute("CLOSE _django_curs_1234_5").await.unwrap();
    assert_empty(&res, "CLOSE");

    sess.execute("COMMIT").await.unwrap();
}
