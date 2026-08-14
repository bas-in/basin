//! `INSERT ... RETURNING` — what comes back, named how, in what order.
//!
//! Reference behaviour was taken from live PostgreSQL 18.2, statement for
//! statement, on equivalent DDL. Each test records the PG answer it asserts
//! against in a comment so a future reader can re-run it against a server.
//!
//! The bug this file was opened for: `exec_insert_prebuilt` took a
//! `has_returning: bool`, so by the time it produced a result it had lost the
//! RETURNING list and answered with the entire inserted row — `RETURNING id`
//! on a six-column table returned six columns. The fix routes INSERT through
//! `dml_mutate::project_returning`, the projector UPDATE and DELETE already
//! used, so all three DML verbs share one implementation of one semantic.
//!
//! KNOWN GAP, not covered here because it is not INSERT-specific. A RETURNING
//! list that names the same column twice (`RETURNING id, id`) is legal in PG
//! 18.2 and answers `id | id` with the value twice; Basin raises
//! `RETURNING expression failed to plan: ... Projections require unique
//! expression names`. That error comes from `project_returning`'s DataFusion
//! round-trip and so applies equally to UPDATE and DELETE — it predates this
//! file and needs a fix in the shared projector's output-name derivation, not
//! in the INSERT wiring.
//!
//! KNOWN GAP, INSERT-specific but out of reach of this wiring.
//! `ON CONFLICT (col) DO UPDATE ... RETURNING` answers with a bare command tag
//! whenever at least one row conflicts. PG 18.2 returns one row per affected
//! row, inserted or updated:
//!   INSERT INTO conf_probe (id,name) VALUES (1,'upd'),(4,'four')
//!     ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name RETURNING id, name
//!   → 1 | upd
//!     4 | four
//! `executor::try_on_conflict_do_update` applies each conflicting row as its
//! own re-executed UPDATE and re-executes the fresh rows as a plain INSERT
//! (`finish_upsert_fresh_inserts`), so no post-image batch survives to project
//! — collecting one needs that path restructured, not a RETURNING list
//! threaded through it. When NO row conflicts the statement falls through to
//! the plain INSERT path and RETURNING is projected correctly.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
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

/// Execute and require a row-set result (what RETURNING must produce), giving
/// back the declared schema's column names plus the batches. The schema is
/// read from `ExecResult::Rows.schema`, not from `batches[0]`, so a zero-row
/// RETURNING is still describable — which is exactly the `ON CONFLICT DO
/// NOTHING` case where every row conflicted.
async fn returning(sess: &ProjectSession, sql: &str) -> (Vec<String>, Vec<RecordBatch>) {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { schema, batches } => (
            schema.fields().iter().map(|f| f.name().clone()).collect(),
            batches,
        ),
        other => panic!("expected RETURNING rows from {sql:?}, got: {other:?}"),
    }
}

fn i64s(batches: &[RecordBatch], idx: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        for i in 0..arr.len() {
            assert!(!arr.is_null(i), "unexpected NULL");
            out.push(arr.value(i));
        }
    }
    out
}

fn i32s(batches: &[RecordBatch], idx: usize) -> Vec<i32> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 column");
        for i in 0..arr.len() {
            assert!(!arr.is_null(i), "unexpected NULL");
            out.push(arr.value(i));
        }
    }
    out
}

fn strs(batches: &[RecordBatch], idx: usize) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

const SIX_COL_DDL: &str = "CREATE TABLE r (id BIGINT PRIMARY KEY, a BIGINT, b TEXT, \
                           c BOOLEAN, d DOUBLE PRECISION, e TEXT)";

/// `RETURNING id` returns exactly one column, and `RETURNING *` returns every
/// column in table order.
///
/// PG 18.2, on `ret_probe(id serial pk, name text, qty int, note text,
/// created date, flag boolean)`:
///   INSERT ... RETURNING id  → one column `id`
///   INSERT ... RETURNING *   → id | name | qty | note | created | flag
#[tokio::test]
async fn returning_list_selects_columns_not_the_whole_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id",
    )
    .await;
    assert_eq!(names, vec!["id"], "PG returns the RETURNING list, not the row");
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(i64s(&batches, 0), vec![1]);

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (2,3,'y',false,2.5,'f') RETURNING *",
    )
    .await;
    assert_eq!(names, vec!["id", "a", "b", "c", "d", "e"], "* is table order");
    assert_eq!(i64s(&batches, 0), vec![2]);
    assert_eq!(strs(&batches, 2), vec!["y"]);
    let flag = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Boolean column");
    assert!(!flag.value(0));
}

/// A multi-column RETURNING list is emitted in the list's order, not the
/// table's.
///
/// PG 18.2: `INSERT INTO ret_probe (name) VALUES ('d') RETURNING name, id`
///   → name | id  (name first, though it is the second table column)
#[tokio::test]
async fn returning_list_order_is_the_lists_order_not_the_tables() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id, b",
    )
    .await;
    assert_eq!(names, vec!["id", "b"]);
    assert_eq!(i64s(&batches, 0), vec![1]);
    assert_eq!(strs(&batches, 1), vec!["x"]);

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (2,3,'y',true,1.5,'f') RETURNING b, id",
    )
    .await;
    assert_eq!(names, vec!["b", "id"], "reversed list stays reversed");
    assert_eq!(strs(&batches, 0), vec!["y"]);
    assert_eq!(i64s(&batches, 1), vec![2]);

}

/// `RETURNING col AS alias` names the output column with the alias.
///
/// PG 18.2: `INSERT INTO ret_probe (name) VALUES ('e') RETURNING id AS pk`
///   → column header `pk`
#[tokio::test]
async fn returning_alias_names_the_output_column() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id AS pk",
    )
    .await;
    assert_eq!(names, vec!["pk"]);
    assert_eq!(i64s(&batches, 0), vec![1]);

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (2,3,'y',true,1.5,'f') RETURNING id AS pk, b AS label",
    )
    .await;
    assert_eq!(names, vec!["pk", "label"]);
    assert_eq!(i64s(&batches, 0), vec![2]);
    assert_eq!(strs(&batches, 1), vec!["y"]);
}

/// RETURNING may hold arbitrary expressions over the inserted row, and their
/// VALUES are what PG computes.
///
/// PG 18.2:
///   RETURNING id + 1        → value 7 for the row whose id is 6
///   RETURNING upper(name)   → 'G' for name 'g'
/// PG names an un-aliased arithmetic expression `?column?` and an un-aliased
/// function call after the function (`upper`). Basin's shared projector
/// derives DataFusion's display names instead — measured on this branch:
/// `id + 1` → `__basin_returning_src.id + Int64(1)`, `upper(b)` →
/// `upper(__basin_returning_src.b)`. That divergence lives in
/// `dml_mutate::project_returning` and applies identically to UPDATE and
/// DELETE, so it is NOT pinned here (pinning it would enshrine it): this test
/// pins the VALUES for un-aliased expressions and pins the NAMES only where
/// the user supplied an alias — the shape every client that reads an
/// expression by name actually uses.
#[tokio::test]
async fn returning_expressions_compute_over_the_inserted_row() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (_, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (6,2,'x',true,1.5,'e') RETURNING id + 1",
    )
    .await;
    assert_eq!(i64s(&batches, 0), vec![7], "PG: id + 1 = 7");

    let (_, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (7,2,'g',true,1.5,'e') RETURNING upper(b)",
    )
    .await;
    assert_eq!(strs(&batches, 0), vec!["G"], "PG: upper('g') = 'G'");

    // Aliased expressions: name AND value are both pinned.
    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (8,2,'h',true,1.5,'e') RETURNING id + 1 AS nxt, upper(b) AS un",
    )
    .await;
    assert_eq!(names, vec!["nxt", "un"]);
    assert_eq!(i64s(&batches, 0), vec![9], "PG: 8 + 1 = 9");
    assert_eq!(strs(&batches, 1), vec!["H"], "PG: upper('h') = 'H'");

    // A mixed list keeps plain columns named after the column.
    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (9,2,'i',true,1.5,'e') RETURNING id, upper(b) AS un",
    )
    .await;
    assert_eq!(names, vec!["id", "un"]);
    assert_eq!(i64s(&batches, 0), vec![9]);
    assert_eq!(strs(&batches, 1), vec!["I"]);
}

/// A multi-row VALUES emits one RETURNING row per inserted row, in insertion
/// order.
///
/// PG 18.2: `INSERT INTO ret_probe (name) VALUES ('m1'),('m2'),('m3')
///           RETURNING id, name` → 3 rows, ids 9,10,11, names m1,m2,m3 in
/// that order.
#[tokio::test]
async fn returning_emits_one_row_per_inserted_row_in_insertion_order() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (30,1,'m1',true,1.0,'e'), (10,2,'m2',true,2.0,'f'), \
         (20,3,'m3',true,3.0,'g') RETURNING id, b",
    )
    .await;
    assert_eq!(names, vec!["id", "b"]);
    assert_eq!(total_rows(&batches), 3, "one output row per inserted row");
    // Insertion order, NOT sorted by id — PG does not sort RETURNING.
    assert_eq!(i64s(&batches, 0), vec![30, 10, 20]);
    assert_eq!(strs(&batches, 1), vec!["m1", "m2", "m3"]);
}

/// RETURNING shows the GENERATED value for a DEFAULT or sequence column —
/// the whole reason `RETURNING id` exists.
///
/// PG 18.2, `ret_probe(id serial pk, name text, qty int DEFAULT 7,
/// note text DEFAULT 'dflt')`:
///   INSERT INTO ret_probe (name) VALUES ('defs') RETURNING id, qty, note
///     → 12 | 7 | dflt         (defaults materialised, not NULL)
///   INSERT INTO ret_probe (name, qty) VALUES ('dk', DEFAULT) RETURNING id, qty
///     → 13 | 7                (explicit DEFAULT keyword resolves too)
#[tokio::test]
async fn returning_shows_generated_default_and_sequence_values() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE d (id SERIAL PRIMARY KEY, name TEXT, qty INTEGER DEFAULT 7, \
         note TEXT DEFAULT 'dflt')",
    )
    .await
    .unwrap();

    // The sequence value is assigned by the engine, never written by the user,
    // so a RETURNING that echoed the literal VALUES list could not produce it.
    let (names, batches) = returning(&sess, "INSERT INTO d (name) VALUES ('a') RETURNING id").await;
    assert_eq!(names, vec!["id"]);
    assert_eq!(i32s(&batches, 0), vec![1], "PG: first serial value is 1");

    let (names, batches) =
        returning(&sess, "INSERT INTO d (name) VALUES ('b') RETURNING id, qty, note").await;
    assert_eq!(names, vec!["id", "qty", "note"]);
    assert_eq!(i32s(&batches, 0), vec![2], "sequence advanced");
    assert_eq!(i32s(&batches, 1), vec![7], "DEFAULT 7 materialised");
    assert_eq!(strs(&batches, 2), vec!["dflt"], "DEFAULT 'dflt' materialised");

    // Multi-row: each row gets its own sequence value, in insertion order.
    let (_, batches) = returning(
        &sess,
        "INSERT INTO d (name) VALUES ('c'), ('e'), ('f') RETURNING id",
    )
    .await;
    assert_eq!(i32s(&batches, 0), vec![3, 4, 5]);
}

/// `ON CONFLICT DO NOTHING ... RETURNING` returns rows for the rows that were
/// actually inserted, and nothing for the rows that did nothing.
///
/// PG 18.2, `conf_probe(id int pk, name text, qty int DEFAULT 5)` holding
/// (1,'one'),(2,'two'):
///   INSERT ... VALUES (1,'dup') ON CONFLICT DO NOTHING RETURNING id, name
///     → 0 rows, tag `INSERT 0 0`
///   INSERT ... VALUES (1,'dup'),(3,'three') ON CONFLICT DO NOTHING
///           RETURNING id, name
///     → 1 row: 3 | three  (only the row that was inserted)
#[tokio::test]
async fn on_conflict_do_nothing_returns_only_the_rows_that_were_inserted() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute("CREATE TABLE c (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO c VALUES (1,'one'), (2,'two')")
        .await
        .unwrap();

    // Mixed: one conflicting row does nothing, one fresh row is inserted.
    let (names, batches) = returning(
        &sess,
        "INSERT INTO c VALUES (1,'dup'), (3,'three') ON CONFLICT (id) DO NOTHING \
         RETURNING id, name",
    )
    .await;
    assert_eq!(names, vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 1, "the conflicting row returns nothing");
    assert_eq!(i64s(&batches, 0), vec![3]);
    assert_eq!(strs(&batches, 1), vec!["three"]);

    // Every row conflicts: an empty row-set, still described by the
    // RETURNING list — not a bare command tag.
    let (names, batches) = returning(
        &sess,
        "INSERT INTO c VALUES (1,'dup'), (2,'dup') ON CONFLICT (id) DO NOTHING \
         RETURNING id, name",
    )
    .await;
    assert_eq!(names, vec!["id", "name"]);
    assert_eq!(total_rows(&batches), 0);

    // The conflicting rows were genuinely left alone.
    let (_, batches) = returning(&sess, "SELECT name FROM c WHERE id = 1").await;
    assert_eq!(strs(&batches, 0), vec!["one"]);
}

/// INSERT's RETURNING agrees with UPDATE's and DELETE's on the same list —
/// they share `dml_mutate::project_returning`, and this test is what keeps a
/// second INSERT-only implementation from growing back.
#[tokio::test]
async fn insert_update_delete_returning_agree_on_the_same_list() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    let (ins_names, ins_batches) = returning(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id AS k, b",
    )
    .await;
    let (upd_names, _) = returning(
        &sess,
        "UPDATE r SET a = 5 WHERE id = 1 RETURNING id AS k, b",
    )
    .await;
    let (del_names, _) = returning(&sess, "DELETE FROM r WHERE id = 1 RETURNING id AS k, b").await;

    assert_eq!(ins_names, upd_names, "INSERT vs UPDATE RETURNING names");
    assert_eq!(ins_names, del_names, "INSERT vs DELETE RETURNING names");
    assert_eq!(ins_names, vec!["k", "b"]);
    assert_eq!(i64s(&ins_batches, 0), vec![1]);
}

/// In an explicit transaction the INSERT is buffered rather than written
/// through, and that arm has its own RETURNING return point — it must project
/// the list too.
///
/// PG 18.2: RETURNING inside BEGIN/COMMIT is identical to auto-commit.
#[tokio::test]
async fn returning_projects_inside_an_explicit_transaction() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    sess.execute(SIX_COL_DDL).await.unwrap();

    sess.execute("BEGIN").await.unwrap();
    let (names, batches) = returning(
        &sess,
        "INSERT INTO r VALUES (1,2,'x',true,1.5,'e') RETURNING id, b",
    )
    .await;
    assert_eq!(names, vec!["id", "b"], "in-tx arm projects the list too");
    assert_eq!(i64s(&batches, 0), vec![1]);
    assert_eq!(strs(&batches, 1), vec!["x"]);
    sess.execute("COMMIT").await.unwrap();

    let (_, batches) = returning(&sess, "SELECT id FROM r").await;
    assert_eq!(i64s(&batches, 0), vec![1], "the row really committed");
}
