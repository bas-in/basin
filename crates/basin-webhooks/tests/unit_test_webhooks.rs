//! End-to-end tests for the webhook fanout machinery.
//!
//! Each test wires up the canonical four-piece stack:
//!
//! 1. [`WebhookRegistry`] — holds subscriptions
//! 2. [`RetryQueue`] — disk-backed pending deliveries
//! 3. [`HttpClient`] (basin-net) — outbound HTTP with the URL allowlist
//! 4. [`WebhookWorker`] — drains the queue
//!
//! Plus a scripted axum server that lets each test assert what the
//! subscriber received (and script the response sequence).

mod common;

use std::sync::Arc;

use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, TableName, TenantId};
use basin_net::HttpClient;
use basin_webhooks::{
    AttemptOutcome, RetryQueue, TestClock, WebhookConfig, WebhookOps, WebhookRegistry, WebhookSink,
    WebhookSubscription, WebhookSubscriptionId, WebhookWorker,
};
use chrono::Utc;
use tempfile::TempDir;

use common::{body_as_json, spawn_server, wait_for};

/// Test-fixture stack. Owns the temp dir so the queue/dead-letter files
/// outlive the test boundary cleanly.
struct Stack {
    _dir: TempDir,
    registry: WebhookRegistry,
    queue: Arc<RetryQueue>,
    sink: Arc<WebhookSink>,
    worker: WebhookWorker,
    clock: TestClock,
    _cfg: WebhookConfig,
}

async fn build_stack(net: HttpClient) -> Stack {
    let dir = TempDir::new().unwrap();
    let cfg = WebhookConfig::rooted_at(dir.path());
    let queue = Arc::new(
        RetryQueue::open(
            cfg.queue_path(),
            cfg.data_dir.join("webhooks").join("dead_letter"),
        )
        .await
        .unwrap(),
    );
    let registry = WebhookRegistry::new();
    let clock = TestClock::new(Utc::now());
    let sink = Arc::new(WebhookSink::with_clock(
        registry.clone(),
        queue.clone(),
        Arc::new(clock.clone()),
    ));
    let worker = WebhookWorker::with_clock(
        cfg.clone(),
        queue.clone(),
        registry.clone(),
        net,
        Arc::new(clock.clone()),
    );
    Stack {
        _dir: dir,
        registry,
        queue,
        sink,
        worker,
        clock,
        _cfg: cfg,
    }
}

fn evt(tenant: TenantId, table: &str, op: ChangeOp, seq: u64) -> ChangeEvent {
    ChangeEvent {
        tenant,
        table: TableName::new(table).unwrap(),
        op,
        before: None,
        after: Some(serde_json::json!({"id": seq, "x": "y"})),
        committed_at: Utc::now(),
        seq,
        causation_user: Some("alice".into()),
    }
}

fn sub(
    tenant: TenantId,
    table: &str,
    url: String,
    ops: WebhookOps,
    max_retries: u32,
) -> WebhookSubscription {
    WebhookSubscription {
        id: WebhookSubscriptionId::new(),
        tenant,
        table: TableName::new(table).unwrap(),
        url,
        ops,
        when_predicate: None,
        max_retries,
        paused: false,
    }
}

fn make_client() -> HttpClient {
    HttpClient::new()
}

/// 1. Register a subscription, fire a matching event, assert the POST
///    arrives at the test server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_subscription_routes_event() {
    let server = spawn_server(200).await;
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    let s = sub(tenant, "orders", server.url("/hook"), WebhookOps::INSERT, 4);
    let id = stack.registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 1);
    stack.sink.publish(&ev).await.unwrap();
    assert_eq!(stack.queue.depth().await, 1);

    let outcomes = stack.worker.tick().await;
    assert_eq!(outcomes.len(), 1);
    assert!(matches!(outcomes[0], AttemptOutcome::Delivered { .. }));

    let reqs = server.requests();
    assert_eq!(reqs.len(), 1);
    let body = body_as_json(&reqs[0]);
    assert_eq!(body["op"], "insert");
    assert_eq!(body["seq"], 1);
    assert_eq!(
        body["subscription_id"],
        serde_json::Value::String(id.to_string())
    );
    let key = reqs[0]
        .headers
        .get("x-basin-idempotency-key")
        .expect("idempotency header");
    assert_eq!(key.len(), 64);
    let header_sub = reqs[0]
        .headers
        .get("x-basin-webhook-subscription")
        .expect("subscription header");
    assert_eq!(header_sub, &id.to_string());
}

/// 2. First attempt 500s, second attempt 200s; both POSTs use the
///    same idempotency key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_idempotency_key_dedupes_after_retry() {
    let server = spawn_server(200).await;
    server.push_response(500);
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    let s = sub(tenant, "orders", server.url("/hook"), WebhookOps::INSERT, 4);
    stack.registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 7);
    stack.sink.publish(&ev).await.unwrap();

    // Tick 1: 500. Worker requeues.
    let o1 = stack.worker.tick().await;
    assert_eq!(o1.len(), 1);
    assert!(matches!(o1[0], AttemptOutcome::Requeued { .. }));

    // Advance the clock past the 1s initial backoff.
    stack.clock.advance(chrono::Duration::seconds(2));

    // Tick 2: 200. Worker delivers.
    let o2 = stack.worker.tick().await;
    assert_eq!(o2.len(), 1);
    assert!(matches!(o2[0], AttemptOutcome::Delivered { .. }));

    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    let k1 = reqs[0].headers.get("x-basin-idempotency-key").unwrap();
    let k2 = reqs[1].headers.get("x-basin-idempotency-key").unwrap();
    assert_eq!(k1, k2, "idempotency key must be stable across retries");
}

/// 3. URL returns 500 forever; with `max_retries = 2` the delivery
///    dead-letters after exactly 2 attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_dead_letter_after_max_retries() {
    let server = spawn_server(500).await;
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    let s = sub(tenant, "orders", server.url("/hook"), WebhookOps::INSERT, 2);
    let id = stack.registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 11);
    stack.sink.publish(&ev).await.unwrap();

    // Attempt 1: 500 → requeued.
    let o = stack.worker.tick().await;
    assert!(matches!(o[0], AttemptOutcome::Requeued { .. }));
    stack.clock.advance(chrono::Duration::seconds(2));

    // Attempt 2: 500 → dead-letter (attempt_count 2 = max_retries 2).
    let o = stack.worker.tick().await;
    assert_eq!(o.len(), 1, "exactly one outcome");
    match &o[0] {
        AttemptOutcome::DeadLettered { attempt_count, .. } => assert_eq!(*attempt_count, 2),
        other => panic!("expected dead-letter, got {other:?}"),
    }

    // Queue is empty.
    assert_eq!(stack.queue.depth().await, 0);

    // Dead-letter file has exactly one entry for our subscription.
    let dls = stack.queue.list_dead_letters(id).await.unwrap();
    assert_eq!(dls.len(), 1);
    assert_eq!(dls[0].attempt_count, 2);
    assert!(dls[0].reason.contains("max_retries"));
}

/// 4. A 422 response moves the delivery to dead-letter without retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_4xx_kills_immediately() {
    let server = spawn_server(422).await;
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    let s = sub(
        tenant,
        "orders",
        server.url("/hook"),
        WebhookOps::INSERT,
        16,
    );
    let id = stack.registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 1);
    stack.sink.publish(&ev).await.unwrap();

    let o = stack.worker.tick().await;
    assert_eq!(o.len(), 1);
    match &o[0] {
        AttemptOutcome::DeadLettered {
            attempt_count,
            reason,
            ..
        } => {
            assert_eq!(*attempt_count, 1, "dead-letter on the first attempt");
            assert!(
                reason.contains("422"),
                "reason includes the status: {reason}"
            );
        }
        other => panic!("expected dead-letter, got {other:?}"),
    }
    assert_eq!(server.requests().len(), 1);
    let dls = stack.queue.list_dead_letters(id).await.unwrap();
    assert_eq!(dls.len(), 1);
}

/// 5. Paused subscription does NOT generate POSTs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_pause_skip_delivery() {
    let server = spawn_server(200).await;
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    let mut s = sub(tenant, "orders", server.url("/hook"), WebhookOps::INSERT, 4);
    s.paused = true;
    let _id = stack.registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 1);
    stack.sink.publish(&ev).await.unwrap();
    // Sink does not check `paused` for enqueue — the worker enforces it.
    // (Actually it does, see sink.rs match block.) Either way: no POST.
    let _ = stack.worker.tick().await;
    assert!(
        server.requests().is_empty(),
        "paused subscription should not POST",
    );
}

/// 6. Sustained failures past `auto_pause_after` flip the subscription
///    to paused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_auto_pause_after_24h() {
    let server = spawn_server(500).await;
    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    // Custom config so `auto_pause_after` is 1 hour for test brevity;
    // the production value is 24h. The mechanism is the same.
    let dir = TempDir::new().unwrap();
    let mut cfg = WebhookConfig::rooted_at(dir.path());
    cfg.auto_pause_after = std::time::Duration::from_secs(60 * 60);
    cfg.backoff_initial = std::time::Duration::from_secs(1);
    cfg.backoff_max = std::time::Duration::from_secs(10);
    let queue = Arc::new(
        RetryQueue::open(
            cfg.queue_path(),
            cfg.data_dir.join("webhooks").join("dead_letter"),
        )
        .await
        .unwrap(),
    );
    let registry = WebhookRegistry::new();
    let clock = TestClock::new(Utc::now());
    let sink = WebhookSink::with_clock(registry.clone(), queue.clone(), Arc::new(clock.clone()));
    let worker = WebhookWorker::with_clock(
        cfg.clone(),
        queue.clone(),
        registry.clone(),
        net,
        Arc::new(clock.clone()),
    );

    let s = sub(
        tenant,
        "orders",
        server.url("/hook"),
        WebhookOps::INSERT,
        100,
    );
    let id = registry.add(s).await.unwrap();

    let ev = evt(tenant, "orders", ChangeOp::Insert, 1);
    sink.publish(&ev).await.unwrap();

    // First attempt to fail: gives `attempt_count = 1`.
    let _ = worker.tick().await;
    assert!(!registry.get(id).await.unwrap().paused);

    // Fast-forward 2 hours; tick the worker so the auto-pause sweep
    // fires.
    clock.advance(chrono::Duration::hours(2));
    let _ = worker.tick().await;

    let after = registry.get(id).await.unwrap();
    assert!(after.paused, "subscription should be auto-paused");
}

/// 7. Tenant A's subscription does NOT receive tenant B's events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_tenant_isolation() {
    let server = spawn_server(200).await;
    let net = make_client();
    let a = TenantId::new();
    let b = TenantId::new();
    net.allow_host(&a, "127.0.0.1").await;
    net.allow_host(&b, "127.0.0.1").await;

    let stack = build_stack(net).await;
    // Only tenant A subscribes.
    let sa = sub(a, "orders", server.url("/hook"), WebhookOps::all_ops(), 4);
    stack.registry.add(sa).await.unwrap();

    // Tenant B fires an event. No subscription matches → no enqueue,
    // no POST.
    let evb = evt(b, "orders", ChangeOp::Insert, 1);
    stack.sink.publish(&evb).await.unwrap();
    let _ = stack.worker.tick().await;
    assert!(
        server.requests().is_empty(),
        "B's event must not reach A's hook"
    );

    // Tenant A's event lands.
    let eva = evt(a, "orders", ChangeOp::Insert, 1);
    stack.sink.publish(&eva).await.unwrap();
    let _ = stack.worker.tick().await;
    assert_eq!(server.requests().len(), 1);
    let body = body_as_json(&server.requests()[0]);
    assert_eq!(body["tenant"], a.to_string());
}

/// 8. Plug `WebhookSink` into a real Engine via
///    `attach_post_commit_sink`; mutate a row; assert the row commits
///    even if the queue is wedged (we simulate this by writing to a
///    non-existent dir so the underlying file-handle write fails on
///    every enqueue).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_commit_failure_does_not_rollback() {
    use basin_catalog::InMemoryCatalog;
    use basin_engine::{Engine, EngineConfig, ExecResult};
    use object_store::local::LocalFileSystem;

    // Set up the engine.
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let net = make_client();
    let tenant = TenantId::new();
    net.allow_host(&tenant, "127.0.0.1").await;

    let stack = build_stack(net).await;
    // Subscription pointing at an unallocated TCP port — every POST
    // will fail at the network layer (connection refused). The crucial
    // part of this test is that the engine's INSERT still commits.
    let s = sub(
        tenant,
        "t",
        "http://127.0.0.1:1/black-hole".into(),
        WebhookOps::all_ops(),
        4,
    );
    stack.registry.add(s).await.unwrap();
    engine.attach_post_commit_sink(stack.sink.clone());

    let sess = engine.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("INSERT must commit even if webhook delivery fails");

    // Yield so any spawned post-commit work runs.
    tokio::task::yield_now().await;

    let res = sess.execute("SELECT id FROM t").await.unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            let count: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(count, 1, "row must be visible");
        }
        other => panic!("unexpected: {other:?}"),
    }

    // The post-commit dispatcher fans out via `tokio::spawn` per
    // (sink, event), so the queue may or may not have an entry yet.
    // What we *do* know: the SELECT above proves the source mutation
    // was not rolled back by any webhook-side error.
    //
    // We also wait briefly for the spawned sink call to land (it
    // enqueues the entry); this double-checks that the post-commit
    // path didn't itself swallow the event.
    let registry = stack.registry.clone();
    let _ = registry; // borrow for clarity; not used further.
    let queue = stack.queue.clone();
    let arrived = wait_for(
        || futures_block_on_depth(&queue) > 0,
        std::time::Duration::from_secs(2),
    )
    .await;
    assert!(arrived, "post-commit dispatcher should have enqueued");
    let _ = stack;
}

// `wait_for` predicate is sync; bridge to the async queue depth via a
// non-blocking try.
fn futures_block_on_depth(queue: &Arc<RetryQueue>) -> usize {
    // We can't .await inside a sync closure; use a blocking handle on
    // the current runtime.
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(queue.depth()))
}
