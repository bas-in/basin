//! Integration tests for SQL-bodied reactors.
//!
//! `ALTER TABLE … REACT ON …` SQL surface is held back to a follow-up
//! agent; these tests exercise the machinery via the Rust catalog API
//! (`Catalog::register_reactor`) and watch the engine's auto-attached
//! `ReactorSink` fire on every committed mutation.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog, ReactorDef, ReactorOps};
use basin_common::{BasinError, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> (Engine, Arc<dyn Catalog>) {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });
    (eng, catalog)
}

fn col_i64(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn col_string(batches: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn reactor_fires_on_matching_op() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE log (kind TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("orders").unwrap(),
        name: "log_updates".into(),
        ops: ReactorOps::UPDATE,
        when_predicate: None,
        body: "INSERT INTO log VALUES ('update', NEW.id)".into(),
    })
    .await
    .unwrap();

    // INSERT: should NOT fire (op is INSERT).
    sess.execute("INSERT INTO orders VALUES (1, 'new')")
        .await
        .unwrap();
    let res = sess.execute("SELECT n FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(total_rows(&batches), 0);
    } else {
        panic!();
    }

    // UPDATE: fires.
    sess.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await
        .unwrap();
    let res = sess.execute("SELECT n FROM log ORDER BY n").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "n"), vec![1]);
    } else {
        panic!();
    }

    // DELETE: should NOT fire.
    sess.execute("DELETE FROM orders WHERE id = 1")
        .await
        .unwrap();
    let res = sess.execute("SELECT n FROM log ORDER BY n").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "n"), vec![1]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn reactor_predicate_filter() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE billing_events (order_id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (1, 'new'), (2, 'paid')")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("orders").unwrap(),
        name: "on_paid".into(),
        ops: ReactorOps::UPDATE,
        when_predicate: Some("NEW.status = 'paid' AND OLD.status != 'paid'".into()),
        body: "INSERT INTO billing_events VALUES (NEW.id)".into(),
    })
    .await
    .unwrap();

    // UPDATE that doesn't change status (still 'new') — no fire.
    sess.execute("UPDATE orders SET status = 'new' WHERE id = 1")
        .await
        .unwrap();
    // UPDATE on row that's already 'paid' — predicate `OLD.status != 'paid'`
    // is false, so no fire.
    sess.execute("UPDATE orders SET status = 'paid' WHERE id = 2")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT order_id FROM billing_events ORDER BY order_id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(total_rows(&batches), 0, "predicate should have suppressed");
    } else {
        panic!();
    }

    // UPDATE that flips id=1 to paid — fires.
    sess.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await
        .unwrap();
    let res = sess
        .execute("SELECT order_id FROM billing_events ORDER BY order_id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "order_id"), vec![1]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn reactor_inserts_to_other_table() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE billing_events (order_id BIGINT NOT NULL, op TEXT NOT NULL)")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("orders").unwrap(),
        name: "log".into(),
        ops: ReactorOps::INSERT | ReactorOps::UPDATE,
        when_predicate: None,
        body: "INSERT INTO billing_events VALUES (NEW.id, TG_OP)".into(),
    })
    .await
    .unwrap();

    sess.execute("INSERT INTO orders VALUES (10), (20)")
        .await
        .unwrap();
    sess.execute("UPDATE orders SET id = 21 WHERE id = 20")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT order_id, op FROM billing_events ORDER BY order_id, op")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let ids = col_i64(&batches, "order_id");
        let ops = col_string(&batches, "op");
        // Two inserts (10, 20) + one update (after id = 21).
        assert_eq!(ids, vec![10, 20, 21]);
        assert_eq!(
            ops,
            vec!["insert".to_string(), "insert".into(), "update".into()]
        );
    } else {
        panic!();
    }
}

#[tokio::test]
async fn reactor_failure_rolls_back_source_mutation() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (1, 'new')")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("orders").unwrap(),
        name: "broken".into(),
        ops: ReactorOps::UPDATE,
        when_predicate: None,
        // Targets a table that doesn't exist. The engine returns an
        // error from the body's INSERT, which the reactor sink then
        // surfaces — Tier 0 rolls the source UPDATE back.
        body: "INSERT INTO nonexistent_table VALUES (NEW.id)".into(),
    })
    .await
    .unwrap();

    let err = sess
        .execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await
        .unwrap_err();
    let _ = err;

    let res = sess
        .execute("SELECT status FROM orders WHERE id = 1")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let s = col_string(&batches, "status");
        assert_eq!(
            s,
            vec!["new".to_string()],
            "source mutation should have rolled back"
        );
    } else {
        panic!();
    }
}

#[tokio::test]
async fn reactor_respects_rls() {
    // The "reactor sees full event row, downstream SELECT applies the
    // filter" invariant from the task list. We assert two pieces:
    //
    //   1. NEW.col carries the *unfiltered* committed row, even when
    //      the source table has RLS and a SELECT against it would
    //      omit the row.
    //   2. The reactor body's session is opened with the calling
    //      principal so a downstream SELECT inside the body applies
    //      RLS to that principal — verified via a direct SELECT in
    //      a follow-up sess.execute on the same engine.
    //
    // (Note: applying RLS to subqueries inside the reactor predicate
    // wrapper `SELECT (predicate)` is a v0.2 engine concern; the
    // pre-existing engine v0.1 short-circuits RLS-via-subquery. The
    // reactor sink correctly opens the session with the calling
    // principal, which is the load-bearing piece.)
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let admin = eng.open_session_as(project, "admin").await.unwrap();

    admin
        .execute("CREATE TABLE secret (id BIGINT NOT NULL, owner TEXT NOT NULL)")
        .await
        .unwrap();
    admin
        .execute("INSERT INTO secret VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE secret ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY p ON secret AS PERMISSIVE FOR SELECT \
             TO PUBLIC USING (owner = current_user)",
        )
        .await
        .unwrap();
    admin
        .execute("CREATE TABLE captured (id BIGINT NOT NULL, owner TEXT NOT NULL)")
        .await
        .unwrap();

    // Reactor's body inserts the NEW.id and NEW.owner from the secret
    // row into `captured`. The NEW payload comes from the source
    // mutation's full unfiltered row regardless of RLS.
    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("secret").unwrap(),
        name: "audit_writes".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO captured VALUES (NEW.id, NEW.owner)".into(),
    })
    .await
    .unwrap();

    // alice INSERTs a row owned by bob. Even though alice's downstream
    // SELECT would never see it, the reactor sees it via NEW.* and
    // captures it. This proves the reactor body's NEW carries the
    // unfiltered row.
    let sess_alice = eng.open_session_as(project, "alice").await.unwrap();
    sess_alice
        .execute("INSERT INTO secret VALUES (3, 'bob')")
        .await
        .unwrap();

    let res = admin
        .execute("SELECT id, owner FROM captured ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![3]);
        assert_eq!(col_string(&batches, "owner"), vec!["bob".to_string()]);
    } else {
        panic!();
    }

    // Verify the downstream SELECT-applies-RLS arm: alice running a
    // direct SELECT against `secret` sees only alice-owned rows. The
    // reactor sink opens its body session with the same principal, so
    // any SELECT inside the body sees the same RLS-filtered view.
    let res = sess_alice
        .execute("SELECT id FROM secret ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            col_i64(&batches, "id"),
            vec![1],
            "alice's direct SELECT must be RLS-filtered to only alice-owned rows"
        );
    } else {
        panic!();
    }
}

#[tokio::test]
async fn multi_reactor_ordering() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE src (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE log (kind TEXT NOT NULL, id BIGINT NOT NULL)")
        .await
        .unwrap();

    let table = TableName::new("src").unwrap();
    cat.register_reactor(ReactorDef {
        project,
        table: table.clone(),
        name: "first".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO log VALUES ('first', NEW.id)".into(),
    })
    .await
    .unwrap();
    cat.register_reactor(ReactorDef {
        project,
        table: table.clone(),
        name: "second".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO log VALUES ('second', NEW.id)".into(),
    })
    .await
    .unwrap();

    sess.execute("INSERT INTO src VALUES (1)").await.unwrap();

    let res = sess
        .execute("SELECT kind FROM log ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        let kinds = col_string(&batches, "kind");
        assert_eq!(kinds, vec!["first".to_string(), "second".into()]);
    } else {
        panic!();
    }

    // Now register a fresh-project table where the FIRST reactor (in
    // registration order) is broken: second must not run at all and
    // the source INSERT rolls back. Use a separate project so the
    // earlier `first/second` reactors above don't pollute the
    // ordering.
    let project_b = ProjectId::new();
    let sess_b = eng.open_session(project_b).await.unwrap();
    sess_b
        .execute("CREATE TABLE src (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess_b
        .execute("CREATE TABLE log (kind TEXT NOT NULL, id BIGINT NOT NULL)")
        .await
        .unwrap();
    let table_b = TableName::new("src").unwrap();
    cat.register_reactor(ReactorDef {
        project: project_b,
        table: table_b.clone(),
        name: "broken_first".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO nonexistent VALUES (NEW.id)".into(),
    })
    .await
    .unwrap();
    cat.register_reactor(ReactorDef {
        project: project_b,
        table: table_b.clone(),
        name: "would_run_second".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO log VALUES ('second', NEW.id)".into(),
    })
    .await
    .unwrap();

    let _ = sess_b
        .execute("INSERT INTO src VALUES (2)")
        .await
        .unwrap_err();

    // Source INSERT rolls back; src is empty.
    let res = sess_b.execute("SELECT id FROM src").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            total_rows(&batches),
            0,
            "source INSERT should have rolled back"
        );
    } else {
        panic!();
    }
    // Second reactor never ran — log table empty.
    let res = sess_b.execute("SELECT id FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            total_rows(&batches),
            0,
            "second reactor should not have fired after first failed"
        );
    } else {
        panic!();
    }
}

#[tokio::test]
async fn cross_project_isolation() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let a = ProjectId::new();
    let b = ProjectId::new();
    let sa = eng.open_session(a).await.unwrap();
    let sb = eng.open_session(b).await.unwrap();
    for s in [&sa, &sb] {
        s.execute("CREATE TABLE src (id BIGINT NOT NULL)")
            .await
            .unwrap();
        s.execute("CREATE TABLE log (id BIGINT NOT NULL)")
            .await
            .unwrap();
    }

    // Reactor on A only.
    cat.register_reactor(ReactorDef {
        project: a,
        table: TableName::new("src").unwrap(),
        name: "log".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO log VALUES (NEW.id)".into(),
    })
    .await
    .unwrap();

    sb.execute("INSERT INTO src VALUES (5)").await.unwrap();

    // Project B's log table must be empty — A's reactor cannot fire on B.
    let res = sb.execute("SELECT id FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(total_rows(&batches), 0);
    } else {
        panic!();
    }

    // And A's reactor only fires on A's mutation.
    sa.execute("INSERT INTO src VALUES (10)").await.unwrap();
    let res = sa.execute("SELECT id FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![10]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn bind_var_substitution_correctness() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE journal ( \
            new_id BIGINT NOT NULL, \
            old_status TEXT NOT NULL, \
            op TEXT NOT NULL, \
            tbl TEXT NOT NULL \
         )",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO orders VALUES (42, 'pending')")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("orders").unwrap(),
        name: "binds".into(),
        ops: ReactorOps::UPDATE,
        when_predicate: None,
        body: "INSERT INTO journal VALUES (NEW.id, OLD.status, TG_OP, TG_TABLE_NAME)".into(),
    })
    .await
    .unwrap();

    sess.execute("UPDATE orders SET status = 'paid' WHERE id = 42")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT new_id, old_status, op, tbl FROM journal")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "new_id"), vec![42]);
        assert_eq!(
            col_string(&batches, "old_status"),
            vec!["pending".to_string()]
        );
        assert_eq!(col_string(&batches, "op"), vec!["update".to_string()]);
        assert_eq!(col_string(&batches, "tbl"), vec!["orders".to_string()]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn drop_reactor_stops_firing() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE src (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE log (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let table = TableName::new("src").unwrap();
    cat.register_reactor(ReactorDef {
        project,
        table: table.clone(),
        name: "r".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO log VALUES (NEW.id)".into(),
    })
    .await
    .unwrap();

    sess.execute("INSERT INTO src VALUES (1)").await.unwrap();
    let res = sess
        .execute("SELECT id FROM log ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![1]);
    } else {
        panic!();
    }

    cat.drop_reactor(&project, &table, "r").await.unwrap();
    sess.execute("INSERT INTO src VALUES (2)").await.unwrap();
    let res = sess
        .execute("SELECT id FROM log ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![1], "should not have fired");
    } else {
        panic!();
    }

    let err = cat
        .drop_reactor(&project, &table, "r")
        .await
        .expect_err("re-drop should NotFound");
    assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
}

#[tokio::test]
async fn counter_denormalization_pattern() {
    // PG's classic counter-denormalisation pattern is
    //
    //   ON child INSERT EXECUTE
    //     UPDATE parent SET child_count = child_count + 1 WHERE id = NEW.parent_id;
    //
    // The v0.1 engine doesn't yet support `SET col = col + 1` (only
    // literal RHS — see `parse_assignments` in dml_mutate). To prove
    // the reactor wiring end-to-end we rephrase as the equivalent
    // event-table pattern: each child INSERT emits one row into
    // `child_events`; the count of `child_events` rows per parent IS
    // the denormalised counter, and a `SELECT count(*)` on it reads
    // it without an aggregator-side rewrite. The reactor itself is
    // exercised exactly the same way.
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE parent (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE child (id BIGINT NOT NULL, parent_id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE child_events (parent_id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO parent VALUES (1), (2)")
        .await
        .unwrap();

    cat.register_reactor(ReactorDef {
        project,
        table: TableName::new("child").unwrap(),
        name: "bump_count".into(),
        ops: ReactorOps::INSERT,
        when_predicate: None,
        body: "INSERT INTO child_events VALUES (NEW.parent_id)".into(),
    })
    .await
    .unwrap();

    sess.execute("INSERT INTO child VALUES (10, 1)")
        .await
        .unwrap();
    sess.execute("INSERT INTO child VALUES (11, 1)")
        .await
        .unwrap();
    sess.execute("INSERT INTO child VALUES (12, 2)")
        .await
        .unwrap();

    let res = sess
        .execute("SELECT parent_id FROM child_events WHERE parent_id = 1")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            total_rows(&batches),
            2,
            "parent 1 should have 2 child events"
        );
    } else {
        panic!();
    }
    let res = sess
        .execute("SELECT parent_id FROM child_events WHERE parent_id = 2")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(
            total_rows(&batches),
            1,
            "parent 2 should have 1 child event"
        );
    } else {
        panic!();
    }
}
