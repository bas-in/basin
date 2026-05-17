//! Integration tests for PRIMARY KEY / CHECK / FOREIGN KEY enforcement.

use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
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
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    }
}

fn col_string(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                out.push(arr.value(i).to_string());
            }
        }
    }
    out
}

fn col_i32(batches: &[RecordBatch], name: &str) -> Vec<i32> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

// ----- PRIMARY KEY -------------------------------------------------------

#[tokio::test]
async fn pk_simple_unique_enforced() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO users (id, name) VALUES (1, 'Ada2')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(msg.contains("users_pkey"), "msg = {msg}");
}

#[tokio::test]
async fn pk_composite_unique_enforced() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE order_items (order_id BIGINT NOT NULL, item_id BIGINT NOT NULL, \
         qty BIGINT NOT NULL, PRIMARY KEY (order_id, item_id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO order_items VALUES (1, 1, 5)")
        .await
        .unwrap();
    sess.execute("INSERT INTO order_items VALUES (1, 2, 5)")
        .await
        .unwrap();
    sess.execute("INSERT INTO order_items VALUES (2, 1, 5)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO order_items VALUES (1, 1, 9)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}

#[tokio::test]
async fn pk_column_must_be_not_null_at_create() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess
        .execute("CREATE TABLE t (id BIGINT NULL PRIMARY KEY)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(msg.to_ascii_lowercase().contains("not null"), "msg = {msg}");
}

#[tokio::test]
async fn pk_update_to_existing_pk_value_fails() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Bob')")
        .await
        .unwrap();
    let err = sess
        .execute("UPDATE users SET id = 1 WHERE id = 2")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
}

// ----- CHECK -------------------------------------------------------------

#[tokio::test]
async fn check_constraint_enforced_on_insert() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE products (id BIGINT NOT NULL, price BIGINT CHECK (price > 0))")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO products VALUES (1, -5)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::CheckViolation(_)), "got {err:?}");
}

#[tokio::test]
async fn check_constraint_passes_when_true() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE products (id BIGINT NOT NULL, price BIGINT CHECK (price > 0))")
        .await
        .unwrap();
    sess.execute("INSERT INTO products VALUES (1, 10)")
        .await
        .unwrap();
}

#[tokio::test]
async fn check_constraint_table_level_two_columns() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE shipments (start_day BIGINT NOT NULL, end_day BIGINT NOT NULL, \
         CHECK (end_day >= start_day))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO shipments VALUES (1, 5)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO shipments VALUES (10, 5)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::CheckViolation(_)), "got {err:?}");
}

#[tokio::test]
async fn check_constraint_in_pg_constraint_view() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE products (id BIGINT NOT NULL, price BIGINT CHECK (price > 0))")
        .await
        .unwrap();
    let batches = rows(
        &sess,
        "SELECT contype, conname FROM pg_catalog.pg_constraint",
    )
    .await;
    let types = col_string(&batches, "contype");
    assert!(types.contains(&"c".to_string()), "types = {types:?}");
}

#[tokio::test]
async fn check_constraint_named_per_pg_convention() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE products (id BIGINT NOT NULL, price BIGINT CHECK (price > 0))")
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT conname FROM pg_catalog.pg_constraint").await;
    let names = col_string(&batches, "conname");
    assert!(
        names.contains(&"products_price_check".to_string()),
        "names = {names:?}"
    );
}

// ----- FOREIGN KEY --------------------------------------------------------

#[tokio::test]
async fn fk_insert_with_existing_referenced_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'Ada')")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (10, 1)")
        .await
        .unwrap();
}

#[tokio::test]
async fn fk_insert_without_referenced_row_fails() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id))",
    )
    .await
    .unwrap();
    let err = sess
        .execute("INSERT INTO orders VALUES (10, 999)")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::ForeignKeyViolation(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn fk_delete_referenced_no_action_blocks() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id))",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'Ada')")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (10, 1)")
        .await
        .unwrap();
    let err = sess
        .execute("DELETE FROM users WHERE id = 1")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::ForeignKeyViolation(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn fk_delete_referenced_cascade() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE)")
        .await
        .unwrap();
    sess.execute("INSERT INTO users VALUES (1, 'Ada')")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (10, 1), (11, 1)")
        .await
        .unwrap();
    sess.execute("DELETE FROM users WHERE id = 1")
        .await
        .unwrap();

    // Both child rows should have cascaded.
    let batches = rows(&sess, "SELECT id FROM orders").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "expected cascade to clear orders");
}

#[tokio::test]
async fn fk_referenced_must_be_pk_or_unique() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .await
        .unwrap();
    let err = sess
        .execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, user_email TEXT NOT NULL REFERENCES users(email))")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
}

#[tokio::test]
async fn fk_cross_project_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess_a = eng.open_session(ProjectId::new()).await.unwrap();
    let sess_b = eng.open_session(ProjectId::new()).await.unwrap();

    sess_a
        .execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    // Project B has no `users` table; FK pointing at it must fail.
    let err = sess_b
        .execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id))")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
}

#[tokio::test]
async fn fk_in_information_schema_referential_constraints() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE)")
        .await
        .unwrap();
    let batches = rows(
        &sess,
        "SELECT update_rule, delete_rule FROM information_schema.referential_constraints",
    )
    .await;
    let updates = col_string(&batches, "update_rule");
    let deletes = col_string(&batches, "delete_rule");
    assert!(
        updates.contains(&"NO ACTION".to_string()),
        "updates={updates:?}"
    );
    assert!(
        deletes.contains(&"CASCADE".to_string()),
        "deletes={deletes:?}"
    );
}

// ----- pg_constraint / table_constraints / key_column_usage --------------

#[tokio::test]
async fn pg_constraint_lists_pk_check_fk_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), \
         qty BIGINT CHECK (qty > 0))",
    )
    .await
    .unwrap();

    let batches = rows(&sess, "SELECT contype FROM pg_catalog.pg_constraint").await;
    let contypes = col_string(&batches, "contype");
    let pk_count = contypes.iter().filter(|s| s == &"p").count();
    let fk_count = contypes.iter().filter(|s| s == &"f").count();
    let ck_count = contypes.iter().filter(|s| s == &"c").count();
    assert_eq!(
        pk_count, 2,
        "expected 2 PKs (users + orders), got {contypes:?}"
    );
    assert_eq!(fk_count, 1, "expected 1 FK, got {contypes:?}");
    assert_eq!(ck_count, 1, "expected 1 CHECK, got {contypes:?}");
}

#[tokio::test]
async fn information_schema_table_constraints_lists_all_kinds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), \
         qty BIGINT CHECK (qty > 0))",
    )
    .await
    .unwrap();
    let batches = rows(
        &sess,
        "SELECT constraint_type FROM information_schema.table_constraints",
    )
    .await;
    let types = col_string(&batches, "constraint_type");
    assert!(types.iter().any(|s| s == "PRIMARY KEY"), "types={types:?}");
    assert!(types.iter().any(|s| s == "FOREIGN KEY"), "types={types:?}");
    assert!(types.iter().any(|s| s == "CHECK"), "types={types:?}");
    assert!(types.iter().any(|s| s == "NOT NULL"), "types={types:?}");
}

#[tokio::test]
async fn key_column_usage_lists_pk_and_fk_columns() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE order_items (order_id BIGINT NOT NULL, item_id BIGINT NOT NULL, \
         qty BIGINT NOT NULL, PRIMARY KEY (order_id, item_id))",
    )
    .await
    .unwrap();
    let batches = rows(
        &sess,
        "SELECT column_name, ordinal_position FROM information_schema.key_column_usage",
    )
    .await;
    let cols = col_string(&batches, "column_name");
    let positions = col_i32(&batches, "ordinal_position");
    assert_eq!(cols, vec!["order_id".to_string(), "item_id".to_string()]);
    assert_eq!(positions, vec![1, 2]);
}

// ----- VARCHAR(n) / CHAR(n) declared-length enforcement (BUG #140) --------
// PG rejects an over-length value with SQLSTATE 22001
// (`value too long for type character varying(n)`). CHAR(n) is
// blank-padded to exactly n on store. Unbounded VARCHAR / TEXT are
// unaffected.

#[tokio::test]
async fn varchar_n_over_length_insert_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code VARCHAR(5))")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO t VALUES (1, 'toolong')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::StringTooLong(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("value too long for type character varying(5)"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn varchar_n_exactly_n_insert_ok() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code VARCHAR(5))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'abcde'), (2, 'ab')")
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT code FROM t ORDER BY id").await;
    assert_eq!(
        col_string(&batches, "code"),
        vec!["abcde".to_string(), "ab".to_string()]
    );
}

#[tokio::test]
async fn varchar_n_trailing_spaces_truncated_not_error() {
    // PG parity: over-length *trailing spaces* on a varchar input are
    // silently truncated to fit, not a 22001 error.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code VARCHAR(3))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'ab     ')")
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT code FROM t").await;
    // Kept value is exactly n chars: "ab " (the non-space prefix plus one
    // space, padded out of the original trailing run).
    assert_eq!(col_string(&batches, "code"), vec!["ab ".to_string()]);
}

#[tokio::test]
async fn varchar_n_update_grow_past_limit_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code VARCHAR(5))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'ab')")
        .await
        .unwrap();
    let err = sess
        .execute("UPDATE t SET code = 'abcdefg' WHERE id = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::StringTooLong(_)), "got {err:?}");
}

#[tokio::test]
async fn char_n_blank_padded_on_store() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code CHAR(5))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'ab')")
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT code FROM t").await;
    // CHAR(5) blank-pads "ab" → "ab   " (exactly 5 chars).
    assert_eq!(col_string(&batches, "code"), vec!["ab   ".to_string()]);
}

#[tokio::test]
async fn char_n_over_length_insert_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code CHAR(3))")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO t VALUES (1, 'abcd')")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::StringTooLong(_)), "got {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("value too long for type character(3)"),
        "msg = {msg}"
    );
}

#[tokio::test]
async fn unbounded_varchar_and_text_unaffected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, a VARCHAR, b TEXT)")
        .await
        .unwrap();
    let long = "x".repeat(10_000);
    sess.execute(&format!("INSERT INTO t VALUES (1, '{long}', '{long}')"))
        .await
        .unwrap();
    let batches = rows(&sess, "SELECT a, b FROM t").await;
    assert_eq!(col_string(&batches, "a"), vec![long.clone()]);
    assert_eq!(col_string(&batches, "b"), vec![long]);
}

#[tokio::test]
async fn char_n_update_grow_past_limit_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, code CHAR(4))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'ab')")
        .await
        .unwrap();
    let err = sess
        .execute("UPDATE t SET code = 'abcde' WHERE id = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::StringTooLong(_)), "got {err:?}");
    // The pre-existing row is still readable and remains blank-padded.
    let batches = rows(&sess, "SELECT code FROM t").await;
    assert_eq!(col_string(&batches, "code"), vec!["ab  ".to_string()]);
}
