//! What the shipping write path does when an `UPDATE` rewrites the key it is
//! matched on, and the cases immediately around it.
//!
//! Reference behaviour was taken from live PostgreSQL 18.2, statement for
//! statement, on identical DDL and fixture. Each test records the PG answer it
//! is asserting against in a comment so a future reader can re-run it.
//!
//! Every assertion here reads the table back rather than trusting the
//! statement's `Ok`/tag — the failure mode this file exists for is a statement
//! that succeeds and leaves the wrong rows behind.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
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

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                out.push(arr.value(i));
            }
        }
    }
    out
}

fn col_str(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
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

fn tag(r: ExecResult) -> String {
    match r {
        ExecResult::Empty { tag } => tag,
        other => panic!("expected a tag, got {other:?}"),
    }
}

/// Live PG 18.2:
/// ```text
/// UPDATE 1
///   id  | a | b
/// ------+---+---
///     2 | 2 | y
///  1001 | 1 | x
/// ```
/// One row in, one row out — the key moves, the row does not multiply.
#[tokio::test]
async fn update_shifting_own_pk_moves_the_row_it_does_not_duplicate_it() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w VALUES (1,1,'x'),(2,2,'y')")
        .await
        .unwrap();

    let t = tag(sess
        .execute("UPDATE w SET id = id + 1000 WHERE b = 'x'")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 1", "tag");

    let back = rows(&sess, "SELECT id, a, b FROM w ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![2, 1001], "ids after the shift");
    assert_eq!(col_i64(&back, "a"), vec![2, 1]);
    assert_eq!(col_str(&back, "b"), vec!["y", "x"]);
}

/// Live PG 18.2:
/// ```text
/// ERROR:  23505: duplicate key value violates unique constraint "w2_pkey"
/// DETAIL:  Key (id)=(1) already exists.
/// ```
/// and the table is untouched afterwards.
#[tokio::test]
async fn update_pk_onto_an_existing_key_raises_23505_and_changes_nothing() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w2 (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w2 VALUES (1,1,'p'),(2,2,'q')")
        .await
        .unwrap();

    let err = sess
        .execute("UPDATE w2 SET id = 1 WHERE id = 2")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");

    let back = rows(&sess, "SELECT id, a, b FROM w2 ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2], "table must be unchanged");
    assert_eq!(col_str(&back, "b"), vec!["p", "q"]);
}

/// Live PG 18.2, rows physically stored in ascending order:
/// ```text
/// ERROR:  23505: duplicate key value violates unique constraint "w2_pkey"
/// DETAIL:  Key (id)=(2) already exists.
/// ```
///
/// PG's uniqueness check here is per-row and physical-order dependent — see
/// [`update_shifting_all_pks_down_by_one_succeeds`] for the mirror case that
/// PG *allows*. What this test pins is only the direction the write path can
/// answer deterministically: the new key set `{2,3}` overlaps the surviving
/// old key `2` at the moment row 1 is rewritten, and PG rejects that.
#[tokio::test]
async fn update_shifting_all_pks_up_by_one_collides() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w3 (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w3 VALUES (1,1,'p'),(2,2,'q')")
        .await
        .unwrap();

    let err = sess
        .execute("UPDATE w3 SET id = id + 1 WHERE id IN (1,2)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");

    let back = rows(&sess, "SELECT id, a, b FROM w3 ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2], "table must be unchanged");
}

/// Live PG 18.2: `UPDATE 2`, table becomes `{0,1}`. The shift is downward, so
/// each row's new key is already free by the time it is written.
#[tokio::test]
async fn update_shifting_all_pks_down_by_one_succeeds() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w4 (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w4 VALUES (1,1,'p'),(2,2,'q')")
        .await
        .unwrap();

    let t = tag(sess
        .execute("UPDATE w4 SET id = id - 1 WHERE id IN (1,2)")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 2", "tag");

    let back = rows(&sess, "SELECT id, a, b FROM w4 ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![0, 1]);
    assert_eq!(col_str(&back, "b"), vec!["p", "q"]);
}

/// Live PG 18.2: `UPDATE 1`, nothing changes. Setting a key to its own value
/// must not self-collide.
#[tokio::test]
async fn update_pk_to_its_own_value_is_not_a_conflict() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w5 (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w5 VALUES (1,1,'p'),(2,2,'q')")
        .await
        .unwrap();

    let t = tag(sess
        .execute("UPDATE w5 SET id = id WHERE id = 1")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 1", "tag");

    let back = rows(&sess, "SELECT id, a, b FROM w5 ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2]);
    assert_eq!(col_str(&back, "b"), vec!["p", "q"]);
}

/// Live PG 18.2: `UPDATE 0`, table unchanged.
#[tokio::test]
async fn update_matching_zero_rows_is_a_no_op() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE w6 (id BIGINT PRIMARY KEY, a BIGINT, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO w6 VALUES (1,1,'p'),(2,2,'q')")
        .await
        .unwrap();

    let t = tag(sess
        .execute("UPDATE w6 SET id = 99 WHERE id = 12345")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 0", "tag");

    let back = rows(&sess, "SELECT id, a, b FROM w6 ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2]);
}

/// Multi-column PK, only one column moves. Live PG 18.2:
/// * `UPDATE m SET k2 = 9 WHERE k1=1 AND k2=1` → `UPDATE 1`, rows
///   `(1,2,b) (1,9,a) (2,1,c)`.
/// * `UPDATE m SET k2 = 2 WHERE k1=1 AND k2=9` → `23505`,
///   `DETAIL: Key (k1, k2)=(1, 2) already exists.`
/// * `UPDATE m SET k1 = 2 WHERE k1=1 AND k2=9` → `UPDATE 1` (`(2,9)` is free).
#[tokio::test]
async fn multi_column_pk_with_one_column_moving() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE m (k1 BIGINT, k2 BIGINT, v TEXT, PRIMARY KEY (k1, k2))")
        .await
        .unwrap();
    sess.execute("INSERT INTO m VALUES (1,1,'a'),(1,2,'b'),(2,1,'c')")
        .await
        .unwrap();

    let t = tag(sess
        .execute("UPDATE m SET k2 = 9 WHERE k1 = 1 AND k2 = 1")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 1");
    let back = rows(&sess, "SELECT k1, k2, v FROM m ORDER BY k1, k2").await;
    assert_eq!(col_i64(&back, "k1"), vec![1, 1, 2]);
    assert_eq!(col_i64(&back, "k2"), vec![2, 9, 1]);
    assert_eq!(col_str(&back, "v"), vec!["b", "a", "c"]);

    let err = sess
        .execute("UPDATE m SET k2 = 2 WHERE k1 = 1 AND k2 = 9")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let back = rows(&sess, "SELECT k1, k2, v FROM m ORDER BY k1, k2").await;
    assert_eq!(col_i64(&back, "k2"), vec![2, 9, 1], "unchanged after 23505");

    let t = tag(sess
        .execute("UPDATE m SET k1 = 2 WHERE k1 = 1 AND k2 = 9")
        .await
        .unwrap());
    assert_eq!(t, "UPDATE 1");
    let back = rows(&sess, "SELECT k1, k2, v FROM m ORDER BY k1, k2").await;
    assert_eq!(col_i64(&back, "k1"), vec![1, 2, 2]);
    assert_eq!(col_i64(&back, "k2"), vec![2, 1, 9]);
}

/// Two separate `INSERT`s, so the two rows live in two different data files
/// and the copy-on-write rewrite touches one file while the colliding key sits
/// in the other. Live PG 18.2, same statements in order:
/// * `SET id = 2 WHERE id = 1` → `23505`, `Key (id)=(2) already exists.`
/// * `SET id = 7 WHERE id = 1` → `UPDATE 1`, rows `(2,q) (7,p)`
/// * `SET id = 1 WHERE id = 2` → `UPDATE 1`, rows `(1,q) (7,p)`
/// * `SET id = 2 WHERE id = 7` → `UPDATE 1`, rows `(1,q) (2,p)`
///
/// The last two matter: a key is vacated and then reused by a later statement,
/// which is where a check that forgot to drop the old key would wrongly fire.
#[tokio::test]
async fn pk_move_collides_across_files_and_a_vacated_key_can_be_reused() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE xf (id BIGINT PRIMARY KEY, b TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO xf VALUES (1,'p')").await.unwrap();
    sess.execute("INSERT INTO xf VALUES (2,'q')").await.unwrap();

    let err = sess
        .execute("UPDATE xf SET id = 2 WHERE id = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    let back = rows(&sess, "SELECT id, b FROM xf ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2], "unchanged after 23505");

    assert_eq!(
        tag(sess
            .execute("UPDATE xf SET id = 7 WHERE id = 1")
            .await
            .unwrap()),
        "UPDATE 1"
    );
    let back = rows(&sess, "SELECT id, b FROM xf ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![2, 7]);
    assert_eq!(col_str(&back, "b"), vec!["q", "p"]);

    assert_eq!(
        tag(sess
            .execute("UPDATE xf SET id = 1 WHERE id = 2")
            .await
            .unwrap()),
        "UPDATE 1"
    );
    // Key 2 is now free — reusing it must not trip the check.
    assert_eq!(
        tag(sess
            .execute("UPDATE xf SET id = 2 WHERE id = 7")
            .await
            .unwrap()),
        "UPDATE 1"
    );
    let back = rows(&sess, "SELECT id, b FROM xf ORDER BY id").await;
    assert_eq!(col_i64(&back, "id"), vec![1, 2]);
    assert_eq!(col_str(&back, "b"), vec!["q", "p"]);
}

/// Live PG 18.2:
/// ```text
/// ERROR:  23502: null value in column "name" of relation "nn"
///         violates not-null constraint
/// ```
/// and `nn` still holds `(1, 'Ada')`.
#[tokio::test]
async fn update_setting_a_not_null_column_to_null_is_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE nn (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO nn VALUES (1, 'Ada')")
        .await
        .unwrap();

    let err = sess
        .execute("UPDATE nn SET name = NULL WHERE id = 1")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::NotNullViolation(_)),
        "got {err:?}"
    );

    let back = rows(&sess, "SELECT id, name FROM nn ORDER BY id").await;
    assert_eq!(col_str(&back, "name"), vec!["Ada"], "row must survive");
}

/// Live PG 18.2 returns exactly the `RETURNING` list, not the whole row:
/// `INSERT ... RETURNING id` yields one column named `id`.
#[tokio::test]
async fn insert_values_returning_projects_only_the_returning_list() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE r (id BIGINT PRIMARY KEY, a BIGINT, b TEXT, c BOOLEAN, \
         d DOUBLE PRECISION, e TEXT)",
    )
    .await
    .unwrap();

    let batches = rows(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id",
    )
    .await;
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string()], "RETURNING id → one column");
    assert_eq!(col_i64(&batches, "id"), vec![1]);

    let batches = rows(
        &sess,
        "INSERT INTO r VALUES (2,3,'y',false,2.5,'f') RETURNING id, b",
    )
    .await;
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string(), "b".to_string()]);

    let batches = rows(
        &sess,
        "INSERT INTO r VALUES (3,4,'z',true,3.5,'g') RETURNING *",
    )
    .await;
    assert_eq!(batches[0].schema().fields().len(), 6, "RETURNING * → all");
}
