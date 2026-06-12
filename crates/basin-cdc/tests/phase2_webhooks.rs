//! Phase 2 webhook push-sink integration harness (ADR 0028 Phase 2).
//!
//! These tests drive the real delivery worker against a **local axum receiver**
//! over real HTTP (so HMAC signing headers, batching, retry/backoff, and the
//! resume cursor are all exercised end-to-end, not mocked). The catalog is the
//! real `InMemoryCatalog`, and the ring is a real `CdcRingWriter` over a
//! `LocalFileSystem` object store — the same wiring production uses.
//!
//! Coverage:
//! - happy-path batch delivery + `X-Basin-Signature` HMAC verification
//! - resume-from-cursor after a worker restart
//! - retry/backoff on 500s then eventual success
//! - ordering preserved under interleaved commits
//! - table/op filter correctness
//! - auto-disable after max delivery age
//! - cross-endpoint isolation (one 500ing endpoint never delays a healthy one)
//! - catalog round-trip for the subscription + cursor (both backends' contract)

use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use basin_catalog::{Catalog, CdcWebhookDef, InMemoryCatalog};
use basin_cdc::{
    sign_body, spawn_webhook_dispatcher, CdcConfig, CdcRingWriter, ReqwestHttp, WebhookConfig,
};
use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, ProjectId, TableName};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Local axum receiver — records every delivered batch + verifies signatures.
// ---------------------------------------------------------------------------

/// One received POST: the raw body, the signature header, and the seq-range
/// header, captured for assertions.
#[derive(Clone, Debug)]
struct Received {
    body: Vec<u8>,
    signature: Option<String>,
    seq_range: Option<String>,
}

#[derive(Clone)]
struct ReceiverState {
    received: Arc<Mutex<Vec<Received>>>,
    /// HTTP status to return. Lets a test make the endpoint 500 then recover.
    status: Arc<AtomicU16>,
    /// How many requests have arrived (for "fail the first N" logic).
    hits: Arc<AtomicUsize>,
    /// If > 0, the first `fail_first` requests return 500 regardless of `status`.
    fail_first: Arc<AtomicUsize>,
}

impl ReceiverState {
    fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            status: Arc::new(AtomicU16::new(200)),
            hits: Arc::new(AtomicUsize::new(0)),
            fail_first: Arc::new(AtomicUsize::new(0)),
        }
    }
}

async fn receiver_handler(
    State(state): State<ReceiverState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    let n = state.hits.fetch_add(1, Ordering::SeqCst);
    let signature = headers
        .get("x-basin-signature")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let seq_range = headers
        .get("x-basin-seq-range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let fail_first = state.fail_first.load(Ordering::SeqCst);
    let code = if n < fail_first {
        500
    } else {
        state.status.load(Ordering::SeqCst)
    };

    // Only record the batch when we are going to ack it (2xx) — a non-acked
    // batch will be re-delivered, and recording it would double-count.
    if (200..300).contains(&code) {
        state.received.lock().await.push(Received {
            body: body.to_vec(),
            signature,
            seq_range,
        });
    }
    StatusCode::from_u16(code).unwrap_or(StatusCode::OK)
}

/// Spawn the local receiver on an ephemeral port. Returns its base URL + state.
async fn spawn_receiver(state: ReceiverState) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/hook", post(receiver_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/hook"), handle)
}

// ---------------------------------------------------------------------------
// Ring + catalog helpers.
// ---------------------------------------------------------------------------

fn storage_in(dir: &TempDir) -> basin_storage::Storage {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    })
}

fn ev(project: ProjectId, table: &str, op: ChangeOp, seq: u64) -> ChangeEvent {
    ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op,
        before: None,
        after: Some(serde_json::json!({ "id": seq })),
        committed_at: chrono::Utc::now(),
        seq,
        causation_user: None,
    }
}

/// Write `events` into the durable ring and flush them.
async fn publish_all(writer: &CdcRingWriter, events: &[ChangeEvent]) {
    for e in events {
        writer.publish(e).await.unwrap();
    }
    writer.flush_all_for_test().await;
}

/// Read a single webhook's delivery state by id (None if absent).
async fn webhook_state(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    id: &str,
) -> Option<basin_catalog::CdcWebhookState> {
    catalog
        .list_cdc_webhooks(project)
        .await
        .into_iter()
        .find(|r| r.def.id == id)
        .map(|r| r.state)
}

fn def(project: ProjectId, id: &str, url: &str) -> CdcWebhookDef {
    CdcWebhookDef {
        id: id.to_string(),
        project,
        url: url.to_string(),
        tables: None,
        ops: None,
        secret_hex: "ab".repeat(32),
        active: true,
    }
}

/// Poll an async condition until it holds or we run out of patience (~5s).
async fn eventually<F, Fut>(mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..500 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within timeout");
}

/// Wait until a webhook's persisted cursor reaches `seq`.
async fn wait_last_seq(catalog: &Arc<dyn Catalog>, project: &ProjectId, id: &str, seq: u64) {
    eventually(|| async {
        webhook_state(catalog, project, id)
            .await
            .map(|s| s.last_seq == seq)
            .unwrap_or(false)
    })
    .await;
}

fn fast_cfg() -> WebhookConfig {
    WebhookConfig {
        max_batch: 100,
        backoff_base: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        max_delivery_age: Duration::from_secs(3600),
        poll_interval: Duration::from_millis(20),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[tokio::test]
async fn happy_path_batch_delivery_and_signature_verification() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();

    // Three committed events.
    publish_all(
        &writer,
        &[
            ev(project, "orders", ChangeOp::Insert, 1),
            ev(project, "orders", ChangeOp::Update, 2),
            ev(project, "orders", ChangeOp::Delete, 3),
        ],
    )
    .await;

    let rx = ReceiverState::new();
    let (url, _srv) = spawn_receiver(rx.clone()).await;

    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let d = def(project, "wh1", &url);
    let secret = d.secret_hex.clone();
    catalog.register_cdc_webhook(d).await.unwrap();

    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let dispatcher =
        spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), http);

    // The endpoint should receive exactly one batch carrying all 3 events.
    eventually(|| async { !rx.received.lock().await.is_empty() }).await;

    let received = rx.received.lock().await.clone();
    assert_eq!(received.len(), 1, "one batch (all 3 events fit one POST)");
    let batch = &received[0];

    // Body is a JSON array of 3 records, in seq order.
    let arr: serde_json::Value = serde_json::from_slice(&batch.body).unwrap();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let seqs: Vec<u64> = arr.iter().map(|r| r["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, vec![1, 2, 3]);

    // Signature verifies against the registered secret over the exact body.
    let expected_sig = sign_body(&secret, &batch.body);
    assert_eq!(
        batch.signature.as_deref(),
        Some(expected_sig.as_str()),
        "X-Basin-Signature is HMAC-SHA256(body, secret)",
    );
    // Seq-range header spans the batch.
    assert_eq!(batch.seq_range.as_deref(), Some("1-3"));

    // The cursor is persisted to the catalog at the batch high seq.
    eventually(|| async {
        webhook_state(&catalog, &project, "wh1")
            .await
            .map(|s| s.last_seq == 3 && s.last_status.as_deref() == Some("200"))
            .unwrap_or(false)
    })
    .await;

    dispatcher.abort();
}

#[tokio::test]
async fn resume_from_cursor_after_worker_restart() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let rx = ReceiverState::new();
    let (url, _srv) = spawn_receiver(rx.clone()).await;
    catalog.register_cdc_webhook(def(project, "wh1", &url)).await.unwrap();

    // First two events, delivered by the first worker instance.
    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 1),
            ev(project, "t", ChangeOp::Insert, 2),
        ],
    )
    .await;

    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let d1 = spawn_webhook_dispatcher(
        writer.clone(),
        catalog.clone(),
        project,
        fast_cfg(),
        Arc::clone(&http),
    );
    wait_last_seq(&catalog, &project, "wh1", 2).await;
    d1.abort(); // "restart": stop the worker.

    let delivered_before = rx.received.lock().await.len();

    // Two more events committed while/after the worker was down.
    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 3),
            ev(project, "t", ChangeOp::Insert, 4),
        ],
    )
    .await;

    // A fresh worker resumes from the persisted cursor (seq=2): it must deliver
    // ONLY 3 and 4, never re-deliver 1 and 2.
    let d2 = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), http);
    wait_last_seq(&catalog, &project, "wh1", 4).await;

    let received = rx.received.lock().await.clone();
    let new_batches = &received[delivered_before..];
    let mut new_seqs: Vec<u64> = Vec::new();
    for b in new_batches {
        let arr: serde_json::Value = serde_json::from_slice(&b.body).unwrap();
        for r in arr.as_array().unwrap() {
            new_seqs.push(r["seq"].as_u64().unwrap());
        }
    }
    assert_eq!(new_seqs, vec![3, 4], "resume delivered only the missed events");
    d2.abort();
}

#[tokio::test]
async fn retry_backoff_on_500_then_success() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let rx = ReceiverState::new();
    // Fail the first 3 attempts with 500, then ack.
    rx.fail_first.store(3, Ordering::SeqCst);
    let (url, _srv) = spawn_receiver(rx.clone()).await;
    catalog.register_cdc_webhook(def(project, "wh1", &url)).await.unwrap();

    publish_all(&writer, &[ev(project, "t", ChangeOp::Insert, 1)]).await;

    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let d = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), http);

    // Eventually the batch is acked (after the 500s), cursor advances to 1.
    wait_last_seq(&catalog, &project, "wh1", 1).await;

    // The endpoint saw at least 4 hits (3 failed + 1 success).
    assert!(rx.hits.load(Ordering::SeqCst) >= 4, "retried past the 500s");
    // Exactly one acked batch recorded (the successful one), carrying seq 1.
    let received = rx.received.lock().await.clone();
    assert_eq!(received.len(), 1);
    let arr: serde_json::Value = serde_json::from_slice(&received[0].body).unwrap();
    assert_eq!(arr.as_array().unwrap()[0]["seq"].as_u64(), Some(1));
    d.abort();
}

#[tokio::test]
async fn ordering_preserved_under_interleaved_commits() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let rx = ReceiverState::new();
    let (url, _srv) = spawn_receiver(rx.clone()).await;
    catalog.register_cdc_webhook(def(project, "wh1", &url)).await.unwrap();

    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let d = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), http);

    // Commit events in several interleaved flushes while the worker runs.
    for seq in 1..=20u64 {
        writer
            .publish(&ev(project, "t", ChangeOp::Insert, seq))
            .await
            .unwrap();
        if seq % 4 == 0 {
            writer.flush_all_for_test().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    writer.flush_all_for_test().await;

    wait_last_seq(&catalog, &project, "wh1", 20).await;

    // Concatenate every delivered seq across all batches → strictly increasing.
    let received = rx.received.lock().await.clone();
    let mut all: Vec<u64> = Vec::new();
    for b in &received {
        let arr: serde_json::Value = serde_json::from_slice(&b.body).unwrap();
        for r in arr.as_array().unwrap() {
            all.push(r["seq"].as_u64().unwrap());
        }
    }
    assert_eq!(all, (1..=20).collect::<Vec<u64>>(), "delivered in seq order");
    d.abort();
}

#[tokio::test]
async fn filter_correctness_table_and_op() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let rx = ReceiverState::new();
    let (url, _srv) = spawn_receiver(rx.clone()).await;
    // Filter: only `orders` table, only `insert` op.
    let mut d = def(project, "wh1", &url);
    d.tables = Some(vec!["orders".into()]);
    d.ops = Some(vec!["insert".into()]);
    catalog.register_cdc_webhook(d).await.unwrap();

    publish_all(
        &writer,
        &[
            ev(project, "orders", ChangeOp::Insert, 1), // match
            ev(project, "orders", ChangeOp::Update, 2), // op filtered out
            ev(project, "users", ChangeOp::Insert, 3),  // table filtered out
            ev(project, "orders", ChangeOp::Insert, 4), // match
        ],
    )
    .await;

    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let disp = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), http);

    // Cursor advances to 4 (the worker consumes the whole ring; non-matching
    // events are skipped but still advance the read position via the matched
    // high seq once 4 is delivered).
    wait_last_seq(&catalog, &project, "wh1", 4).await;

    let received = rx.received.lock().await.clone();
    let mut seqs: Vec<u64> = Vec::new();
    for b in &received {
        let arr: serde_json::Value = serde_json::from_slice(&b.body).unwrap();
        for r in arr.as_array().unwrap() {
            seqs.push(r["seq"].as_u64().unwrap());
        }
    }
    assert_eq!(seqs, vec![1, 4], "only orders/insert events delivered");
    disp.abort();
}

#[tokio::test]
async fn auto_disable_after_max_delivery_age() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let rx = ReceiverState::new();
    rx.status.store(500, Ordering::SeqCst); // permanently failing endpoint.
    let (url, _srv) = spawn_receiver(rx.clone()).await;
    catalog.register_cdc_webhook(def(project, "wh1", &url)).await.unwrap();

    publish_all(&writer, &[ev(project, "t", ChangeOp::Insert, 1)]).await;

    // Tiny max age so the endpoint disables quickly after it keeps failing.
    let cfg = WebhookConfig {
        max_delivery_age: Duration::from_millis(80),
        backoff_base: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
        poll_interval: Duration::from_millis(10),
        ..fast_cfg()
    };
    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let d = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, cfg, http);

    // The endpoint is auto-disabled (active=false, disabled_at stamped).
    eventually(|| async {
        catalog
            .list_cdc_webhooks(&project)
            .await
            .first()
            .map(|r| !r.def.active && r.state.disabled_at.is_some())
            .unwrap_or(false)
    })
    .await;

    // The un-acked batch never advanced the cursor.
    let rows = catalog.list_cdc_webhooks(&project).await;
    assert_eq!(rows[0].state.last_seq, 0, "failed batch never acked");
    d.abort();
}

#[tokio::test]
async fn cross_endpoint_isolation_500ing_endpoint_does_not_delay_healthy_one() {
    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let writer = CdcRingWriter::with_config(storage, CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    // Endpoint A: permanently 500ing. Endpoint B: healthy.
    let rx_bad = ReceiverState::new();
    rx_bad.status.store(500, Ordering::SeqCst);
    let (url_bad, _s1) = spawn_receiver(rx_bad.clone()).await;
    let rx_good = ReceiverState::new();
    let (url_good, _s2) = spawn_receiver(rx_good.clone()).await;

    catalog.register_cdc_webhook(def(project, "bad", &url_bad)).await.unwrap();
    catalog.register_cdc_webhook(def(project, "good", &url_good)).await.unwrap();

    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 1),
            ev(project, "t", ChangeOp::Insert, 2),
        ],
    )
    .await;

    // Long max age so `bad` stays retrying (not disabled) for the whole test —
    // it must NOT block `good`.
    let cfg = WebhookConfig {
        max_delivery_age: Duration::from_secs(3600),
        backoff_base: Duration::from_millis(15),
        max_backoff: Duration::from_millis(30),
        poll_interval: Duration::from_millis(15),
        ..fast_cfg()
    };
    let http: Arc<dyn basin_cdc::WebhookHttp> = Arc::new(ReqwestHttp::new());
    let d = spawn_webhook_dispatcher(writer.clone(), catalog.clone(), project, cfg, http);

    // The healthy endpoint delivers + advances its cursor to 2 regardless of
    // the failing endpoint's state.
    wait_last_seq(&catalog, &project, "good", 2).await;

    // The bad endpoint never acked (cursor still 0) but did keep retrying.
    let rows = catalog.list_cdc_webhooks(&project).await;
    let bad = rows.iter().find(|r| r.def.id == "bad").unwrap();
    assert_eq!(bad.state.last_seq, 0, "bad endpoint never advanced");
    assert!(bad.state.retry_count >= 1, "bad endpoint was retried");
    assert!(rx_bad.hits.load(Ordering::SeqCst) >= 1);
    d.abort();
}

#[tokio::test]
async fn catalog_round_trip_in_memory() {
    // Subscription + cursor round-trip on the in-memory backend (the postgres
    // backend shares the trait + GREATEST-cursor contract; it is exercised in
    // the basin-catalog postgres integration suite when a DSN is present).
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();

    // Empty before registration.
    assert!(catalog.list_cdc_webhooks(&project).await.is_empty());

    let mut d = def(project, "wh1", "https://example.com/hook");
    d.tables = Some(vec!["orders".into()]);
    d.ops = Some(vec!["insert".into(), "update".into()]);
    catalog.register_cdc_webhook(d.clone()).await.unwrap();

    // Duplicate id rejected.
    assert!(catalog.register_cdc_webhook(d.clone()).await.is_err());

    let rows = catalog.list_cdc_webhooks(&project).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].def.url, "https://example.com/hook");
    assert_eq!(rows[0].def.tables.as_deref(), Some(&["orders".to_string()][..]));
    assert_eq!(rows[0].state.last_seq, 0, "fresh cursor");

    // Ack advances the cursor monotonically; a lower ack never rewinds it.
    catalog.record_cdc_webhook_ack(&project, "wh1", 10, "200").await.unwrap();
    catalog.record_cdc_webhook_ack(&project, "wh1", 5, "200").await.unwrap();
    let rows = catalog.list_cdc_webhooks(&project).await;
    assert_eq!(rows[0].state.last_seq, 10, "GREATEST cursor — never rewinds");
    assert_eq!(rows[0].state.retry_count, 0, "ack resets retry_count");

    // Failure bumps retry_count + records status, does NOT advance the cursor.
    catalog.record_cdc_webhook_failure(&project, "wh1", 3, "500").await.unwrap();
    let rows = catalog.list_cdc_webhooks(&project).await;
    assert_eq!(rows[0].state.retry_count, 3);
    assert_eq!(rows[0].state.last_status.as_deref(), Some("500"));
    assert_eq!(rows[0].state.last_seq, 10);

    // Disable clears active + stamps disabled_at, retaining the row.
    catalog.disable_cdc_webhook(&project, "wh1").await.unwrap();
    let rows = catalog.list_cdc_webhooks(&project).await;
    assert!(!rows[0].def.active);
    assert!(rows[0].state.disabled_at.is_some());

    // Drop removes it; second drop is NotFound.
    catalog.drop_cdc_webhook(&project, "wh1").await.unwrap();
    assert!(catalog.list_cdc_webhooks(&project).await.is_empty());
    assert!(catalog.drop_cdc_webhook(&project, "wh1").await.is_err());
}
