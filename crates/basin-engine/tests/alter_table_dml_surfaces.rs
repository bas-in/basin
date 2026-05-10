//! Integration tests for the three Tier 2 ALTER TABLE SQL surfaces:
//!
//! - 5.11.I: `ALTER TABLE ... SUBSCRIBE/UNSUBSCRIBE WEBHOOK ...`
//! - 5.11.C: `ALTER TABLE ... REACT ON ... EXECUTE ...` (SQL-bodied)
//! - 5.11.C2: `ALTER TABLE ... REACT ON ... CONSTRAINT (...)` (constraint-shaped)
//!
//! Each surface ships its `match_*` recogniser + `exec_*` applier in a
//! follow-up — these tests drive the helpers directly because the
//! `executor.rs` wiring is held back to a tiny follow-up agent (see
//! `webhook_ddl.rs` / `reactor_ddl.rs` module docs for the rationale).
//! The helpers' public API is intentionally callable in this shape so
//! the wiring patch can land independently.

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, TableName, TenantId};
use basin_engine::reactor_ddl::{
    exec_drop_reactor, exec_react_constraint, exec_react_on, match_alter_table_react_constraint,
    match_alter_table_react_on, match_drop_reactor,
};
use basin_engine::{
    exec_subscribe_webhook, exec_unsubscribe_webhook, match_alter_table_subscribe_webhook,
    match_alter_table_unsubscribe_webhook, Engine, EngineConfig, ExecResult, WebhookOps,
    WebhookRegistry,
};
use basin_webhooks::{RetryQueue, WebhookConfig, WebhookSink};
use chrono::Utc;
use object_store::local::LocalFileSystem;
use serde_json::json;
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

fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ============================================================================
// 5.11.I — SUBSCRIBE / UNSUBSCRIBE WEBHOOK
// ============================================================================

#[tokio::test]
async fn subscribe_webhook_round_trip() {
    let registry = WebhookRegistry::new();
    let tenant = TenantId::new();

    let intent = match_alter_table_subscribe_webhook(
        "ALTER TABLE orders SUBSCRIBE WEBHOOK my_hook \
         TO 'https://example.com/h' ON INSERT, UPDATE",
    )
    .unwrap()
    .unwrap();
    assert_eq!(intent.label.as_deref(), Some("my_hook"));
    assert_eq!(intent.ops, WebhookOps::INSERT | WebhookOps::UPDATE);

    let id = exec_subscribe_webhook(intent, &tenant, &registry)
        .await
        .unwrap();
    assert!(registry.get(id).await.is_some());

    let unsub =
        match_alter_table_unsubscribe_webhook("ALTER TABLE orders UNSUBSCRIBE WEBHOOK my_hook")
            .unwrap()
            .unwrap();
    exec_unsubscribe_webhook(unsub, &tenant, &registry)
        .await
        .unwrap();
    assert!(registry.get(id).await.is_none());
}

#[tokio::test]
async fn subscribe_webhook_validation_errors() {
    let registry = WebhookRegistry::new();
    let tenant = TenantId::new();

    // Empty URL (caught in match_*).
    let err =
        match_alter_table_subscribe_webhook("ALTER TABLE t SUBSCRIBE WEBHOOK TO '' ON INSERT")
            .unwrap_err();
    assert!(matches!(err, basin_common::BasinError::InvalidSchema(_)));

    // Empty OPS (caught in match_*).
    let err =
        match_alter_table_subscribe_webhook("ALTER TABLE t SUBSCRIBE WEBHOOK TO 'http://h' ON")
            .unwrap_err();
    assert!(matches!(err, basin_common::BasinError::InvalidSchema(_)));

    // Malformed predicate (caught in match_*).
    let err = match_alter_table_subscribe_webhook(
        "ALTER TABLE t SUBSCRIBE WEBHOOK TO 'http://h' ON INSERT WHERE (@@@)",
    )
    .unwrap_err();
    assert!(matches!(err, basin_common::BasinError::InvalidSchema(_)));

    // Invalid URL surfaces from registry::add via exec.
    let intent = match_alter_table_subscribe_webhook(
        "ALTER TABLE t SUBSCRIBE WEBHOOK TO 'not a url' ON INSERT",
    )
    .unwrap()
    .unwrap();
    let err = exec_subscribe_webhook(intent, &tenant, &registry)
        .await
        .unwrap_err();
    assert!(
        matches!(err, basin_common::BasinError::InvalidSchema(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn subscribe_webhook_filter_predicate_works() {
    // The sink evaluates the predicate against each event's JSON
    // payload at delivery time. Subscribe with a predicate, fire two
    // events (one matching, one not) through the sink directly, and
    // assert only the matching event lands in the queue.
    let dir = TempDir::new().unwrap();
    let registry = WebhookRegistry::new();
    let tenant = TenantId::new();

    let intent = match_alter_table_subscribe_webhook(
        "ALTER TABLE orders SUBSCRIBE WEBHOOK paid_hook \
         TO 'https://example.com/h' ON INSERT \
         WHERE (NEW.status = 'paid')",
    )
    .unwrap()
    .unwrap();
    assert_eq!(intent.predicate.as_deref(), Some("NEW.status = 'paid'"));
    exec_subscribe_webhook(intent, &tenant, &registry)
        .await
        .unwrap();

    // Build a sink against a TempDir-rooted queue so we can read the
    // queue depth as ground truth.
    let cfg = WebhookConfig::rooted_at(dir.path());
    let queue = Arc::new(
        RetryQueue::open(
            cfg.queue_path(),
            dir.path().join("webhooks").join("dead_letter"),
        )
        .await
        .unwrap(),
    );
    let sink = WebhookSink::new(registry.clone(), queue.clone());

    // Non-matching event — predicate is false, sink skips.
    let ev_skip = ChangeEvent {
        tenant,
        table: TableName::new("orders").unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(json!({"id": 1, "status": "pending"})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    sink.publish(&ev_skip).await.unwrap();
    assert_eq!(
        queue.depth().await,
        0,
        "non-matching event must not enqueue"
    );

    // Matching event — predicate is true, sink enqueues.
    let ev_fire = ChangeEvent {
        tenant,
        table: TableName::new("orders").unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(json!({"id": 2, "status": "paid"})),
        committed_at: Utc::now(),
        seq: 2,
        causation_user: None,
    };
    sink.publish(&ev_fire).await.unwrap();
    assert_eq!(queue.depth().await, 1, "matching event must enqueue");
}

// ============================================================================
// 5.11.C — REACT ON ... EXECUTE
// ============================================================================

#[tokio::test]
async fn react_on_insert_round_trip() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE log (id BIGINT NOT NULL)")
        .await
        .unwrap();

    // Match + exec via the SQL surface helpers.
    let intent = match_alter_table_react_on(
        "ALTER TABLE orders REACT ON INSERT EXECUTE INSERT INTO log VALUES (NEW.id)",
    )
    .unwrap()
    .unwrap();
    exec_react_on(intent, &tenant, &cat).await.unwrap();

    sess.execute("INSERT INTO orders VALUES (42)")
        .await
        .unwrap();
    let res = sess.execute("SELECT id FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![42]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn react_when_predicate() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL, status TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE billing (order_id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO orders VALUES (1, 'new')")
        .await
        .unwrap();

    let intent = match_alter_table_react_on(
        "ALTER TABLE orders REACT ON UPDATE WHEN (NEW.status = 'paid') \
         EXECUTE INSERT INTO billing VALUES (NEW.id)",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        intent.when_predicate.as_deref(),
        Some("NEW.status = 'paid'")
    );
    exec_react_on(intent, &tenant, &cat).await.unwrap();

    // UPDATE with non-matching status — predicate false, no fire.
    sess.execute("UPDATE orders SET status = 'pending' WHERE id = 1")
        .await
        .unwrap();
    let res = sess.execute("SELECT order_id FROM billing").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(total_rows(&batches), 0);
    } else {
        panic!();
    }

    // UPDATE that flips to paid — predicate true, fires.
    sess.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
        .await
        .unwrap();
    let res = sess.execute("SELECT order_id FROM billing").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "order_id"), vec![1]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn drop_reactor_works() {
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE src (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE log (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let intent = match_alter_table_react_on(
        "ALTER TABLE src REACT ON INSERT EXECUTE INSERT INTO log VALUES (NEW.id)",
    )
    .unwrap()
    .unwrap();
    let reactor_name = intent.name.clone();
    exec_react_on(intent, &tenant, &cat).await.unwrap();

    sess.execute("INSERT INTO src VALUES (1)").await.unwrap();
    let res = sess.execute("SELECT id FROM log").await.unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![1]);
    } else {
        panic!();
    }

    let drop_intent = match_drop_reactor(&format!("DROP REACTOR {reactor_name} ON src"))
        .unwrap()
        .unwrap();
    exec_drop_reactor(drop_intent, &tenant, &cat).await.unwrap();

    sess.execute("INSERT INTO src VALUES (2)").await.unwrap();
    let res = sess
        .execute("SELECT id FROM log ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        // Only the pre-DROP insert landed.
        assert_eq!(col_i64(&batches, "id"), vec![1]);
    } else {
        panic!();
    }
}

// ============================================================================
// 5.11.C2 — REACT ON ... CONSTRAINT
// ============================================================================

#[tokio::test]
async fn constraint_predicate_blocks_violating_insert() {
    // Cap the table at 2 rows. The 3rd INSERT must fail with a
    // SQLSTATE 23514 message; the source mutation rolls back so the
    // table stays at 2 rows.
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let intent = match_alter_table_react_constraint(
        "ALTER TABLE orders REACT ON INSERT CONSTRAINT ((SELECT count(*) FROM orders) <= 2) \
         WITH (name = 'cap')",
    )
    .unwrap()
    .unwrap();
    exec_react_constraint(intent, &tenant, &cat).await.unwrap();

    sess.execute("INSERT INTO orders VALUES (1)").await.unwrap();
    sess.execute("INSERT INTO orders VALUES (2)").await.unwrap();
    let err = sess
        .execute("INSERT INTO orders VALUES (3)")
        .await
        .expect_err("3rd insert must violate the cap");
    let msg = format!("{err}");
    assert!(
        msg.contains("23514") || msg.to_ascii_lowercase().contains("check"),
        "expected SQLSTATE 23514 / check_violation message, got {msg:?}"
    );

    // Source mutation rolled back — still 2 rows.
    let res = sess
        .execute("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![1, 2]);
    } else {
        panic!();
    }
}

#[tokio::test]
async fn constraint_with_subquery_against_sibling_table() {
    // Constraint references a sibling table in the same tenant. The
    // predicate is `(SELECT count(*) FROM siblings) <= 1`; once a
    // single row exists in siblings, further inserts into orders are
    // blocked.
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE siblings (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let intent = match_alter_table_react_constraint(
        "ALTER TABLE orders REACT ON INSERT CONSTRAINT ((SELECT count(*) FROM siblings) <= 1)",
    )
    .unwrap()
    .unwrap();
    exec_react_constraint(intent, &tenant, &cat).await.unwrap();

    sess.execute("INSERT INTO siblings VALUES (1)")
        .await
        .unwrap();
    // Allowed: siblings has 1 row, predicate `1 <= 1` true.
    sess.execute("INSERT INTO orders VALUES (10)")
        .await
        .unwrap();
    // Now add another row to siblings and try again — 2 > 1, blocked.
    sess.execute("INSERT INTO siblings VALUES (2)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO orders VALUES (20)")
        .await
        .expect_err("predicate should block the second insert");
    let msg = format!("{err}");
    assert!(
        msg.contains("23514") || msg.to_ascii_lowercase().contains("check"),
        "got {msg:?}"
    );
}

#[tokio::test]
async fn constraint_passes_under_cap() {
    // Negative control: an insert that satisfies the predicate must
    // not error (and the row commits).
    let dir = TempDir::new().unwrap();
    let (eng, cat) = engine_in(&dir);
    let tenant = TenantId::new();
    let sess = eng.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE orders (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let intent = match_alter_table_react_constraint(
        "ALTER TABLE orders REACT ON INSERT CONSTRAINT ((SELECT count(*) FROM orders) <= 100)",
    )
    .unwrap()
    .unwrap();
    exec_react_constraint(intent, &tenant, &cat).await.unwrap();

    for i in 0..5 {
        sess.execute(&format!("INSERT INTO orders VALUES ({i})"))
            .await
            .expect("should pass under cap");
    }
    let res = sess
        .execute("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = res {
        assert_eq!(col_i64(&batches, "id"), vec![0, 1, 2, 3, 4]);
    } else {
        panic!();
    }
}
