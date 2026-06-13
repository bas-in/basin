//! Phase 3 Kafka push-sink integration harness (ADR 0028 Phase 3).
//!
//! These tests drive the real delivery worker against an **in-process mock
//! producer** (the [`basin_cdc::KafkaProducer`] trait, injected) — NOT a real
//! broker. What is under test is the delivery-worker logic that is identical in
//! shape to the webhook worker but transport-agnostic: cursor resume, ordered
//! delivery, retry/backoff, table/op filters, partition-key determinism,
//! tx-group contiguity, cross-project isolation, and the catalog round-trip on
//! both backends. A real-broker end-to-end test is an env-gated optional test
//! (`BASIN_CDC_KAFKA_TEST_BROKERS`) so CI does not require a Kafka cluster.
//!
//! The catalog is the real `InMemoryCatalog`; the ring is a real
//! `CdcRingWriter` over `LocalFileSystem` — the same wiring production uses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, CdcKafkaSinkDef, InMemoryCatalog, PartitionKey};
use basin_cdc::{
    spawn_kafka_dispatcher, CdcConfig, CdcRingWriter, KafkaConfig, KafkaProducer, KafkaRecord,
};
use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, ProjectId, TableName};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// In-process mock producer.
// ---------------------------------------------------------------------------

/// Records every produced batch (when acked) so tests can assert ordering,
/// keys, topics, and filtering. Can be configured to fail the first N produce
/// calls, or to fail permanently.
#[derive(Clone)]
struct MockProducer {
    /// Acked batches, in produce order.
    produced: Arc<Mutex<Vec<Vec<KafkaRecord>>>>,
    /// Total produce() calls (incl. failed).
    calls: Arc<AtomicUsize>,
    /// Fail the first N calls (transient error), then ack.
    fail_first: Arc<AtomicUsize>,
    /// If true, every call fails (permanent error).
    always_fail: Arc<std::sync::atomic::AtomicBool>,
}

impl MockProducer {
    fn new() -> Self {
        Self {
            produced: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
            fail_first: Arc::new(AtomicUsize::new(0)),
            always_fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Flatten every acked record's seq across all batches, in produce order.
    async fn delivered_seqs(&self) -> Vec<u64> {
        self.produced
            .lock()
            .await
            .iter()
            .flat_map(|b| b.iter().map(|r| r.seq))
            .collect()
    }
}

#[async_trait::async_trait]
impl KafkaProducer for MockProducer {
    async fn produce(&self, _brokers: &str, batch: &[KafkaRecord]) -> Result<(), String> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.always_fail.load(Ordering::SeqCst) {
            return Err("mock: permanent broker failure".into());
        }
        if n < self.fail_first.load(Ordering::SeqCst) {
            return Err("mock: transient broker failure".into());
        }
        self.produced.lock().await.push(batch.to_vec());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers.
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

fn ev(project: ProjectId, table: &str, op: ChangeOp, seq: u64, id: i64) -> ChangeEvent {
    let (before, after) = match op {
        ChangeOp::Insert => (None, Some(serde_json::json!({ "id": id }))),
        ChangeOp::Update => (
            Some(serde_json::json!({ "id": id, "v": 0 })),
            Some(serde_json::json!({ "id": id, "v": 1 })),
        ),
        ChangeOp::Delete => (Some(serde_json::json!({ "id": id })), None),
    };
    ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op,
        before,
        after,
        committed_at: chrono::Utc::now(),
        seq,
        causation_user: None,
    }
}

async fn publish_all(writer: &CdcRingWriter, events: &[ChangeEvent]) {
    for e in events {
        writer.publish(e).await.unwrap();
    }
    writer.flush_all_for_test().await;
}

fn def(project: ProjectId, id: &str) -> CdcKafkaSinkDef {
    CdcKafkaSinkDef {
        id: id.to_string(),
        project,
        brokers: "localhost:9092".to_string(),
        topic: "basin.{project}.{table}".to_string(),
        tables: None,
        ops: None,
        partition_key: PartitionKey::RowPk,
        active: true,
    }
}

async fn sink_state(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    id: &str,
) -> Option<basin_catalog::CdcKafkaSinkState> {
    catalog
        .list_cdc_kafka_sinks(project)
        .await
        .into_iter()
        .find(|r| r.def.id == id)
        .map(|r| r.state)
}

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

async fn wait_last_seq(catalog: &Arc<dyn Catalog>, project: &ProjectId, id: &str, seq: u64) {
    eventually(|| async {
        sink_state(catalog, project, id)
            .await
            .map(|s| s.last_seq == seq)
            .unwrap_or(false)
    })
    .await;
}

fn fast_cfg() -> KafkaConfig {
    KafkaConfig {
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
async fn ordered_delivery_with_topic_and_value() {
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    publish_all(
        &writer,
        &[
            ev(project, "orders", ChangeOp::Insert, 1, 100),
            ev(project, "orders", ChangeOp::Update, 2, 100),
            ev(project, "orders", ChangeOp::Delete, 3, 100),
        ],
    )
    .await;

    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();
    let producer = MockProducer::new();
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let d = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p);

    wait_last_seq(&catalog, &project, "s1", 3).await;

    let seqs = producer.delivered_seqs().await;
    assert_eq!(seqs, vec![1, 2, 3], "delivered in seq order");

    // Topic rendered with project + table; value carries seq for consumer dedup.
    let batches = producer.produced.lock().await.clone();
    let first = &batches[0][0];
    assert!(first.topic.contains(&project.to_string()));
    assert!(first.topic.ends_with(".orders"));
    let v: serde_json::Value = serde_json::from_slice(&first.value).unwrap();
    assert_eq!(v["seq"].as_u64(), Some(1));

    // Status is "ok" after the ack.
    assert_eq!(
        sink_state(&catalog, &project, "s1").await.unwrap().last_status.as_deref(),
        Some("ok")
    );
    d.abort();
}

#[tokio::test]
async fn resume_after_restart_from_cursor() {
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();

    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 1, 1),
            ev(project, "t", ChangeOp::Insert, 2, 2),
        ],
    )
    .await;

    let producer = MockProducer::new();
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let d1 = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), Arc::clone(&p));
    wait_last_seq(&catalog, &project, "s1", 2).await;
    d1.abort();

    let delivered_before = producer.produced.lock().await.len();

    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 3, 3),
            ev(project, "t", ChangeOp::Insert, 4, 4),
        ],
    )
    .await;

    // Fresh producer + worker: must produce ONLY 3 and 4, never re-deliver 1,2.
    let producer2 = MockProducer::new();
    let p2: Arc<dyn KafkaProducer> = Arc::new(producer2.clone());
    let d2 = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p2);
    wait_last_seq(&catalog, &project, "s1", 4).await;

    assert!(delivered_before >= 1, "first worker delivered 1,2");
    let resumed = producer2.delivered_seqs().await;
    assert_eq!(resumed, vec![3, 4], "resume delivered only the missed events");
    d2.abort();
}

#[tokio::test]
async fn retry_backoff_on_producer_error_then_success() {
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();

    publish_all(&writer, &[ev(project, "t", ChangeOp::Insert, 1, 1)]).await;

    let producer = MockProducer::new();
    producer.fail_first.store(3, Ordering::SeqCst); // 3 transient failures, then ack.
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let d = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p);

    wait_last_seq(&catalog, &project, "s1", 1).await;
    assert!(producer.calls.load(Ordering::SeqCst) >= 4, "retried past the failures");
    assert_eq!(producer.delivered_seqs().await, vec![1], "exactly one acked batch");
    d.abort();
}

#[tokio::test]
async fn table_and_op_filter() {
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());

    let mut d = def(project, "s1");
    d.tables = Some(vec!["orders".into()]);
    d.ops = Some(vec!["insert".into()]);
    catalog.register_cdc_kafka_sink(d).await.unwrap();

    publish_all(
        &writer,
        &[
            ev(project, "orders", ChangeOp::Insert, 1, 1), // match
            ev(project, "orders", ChangeOp::Update, 2, 1), // op filtered
            ev(project, "users", ChangeOp::Insert, 3, 1),  // table filtered
            ev(project, "orders", ChangeOp::Insert, 4, 2), // match
        ],
    )
    .await;

    let producer = MockProducer::new();
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let disp = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p);

    wait_last_seq(&catalog, &project, "s1", 4).await;
    assert_eq!(
        producer.delivered_seqs().await,
        vec![1, 4],
        "only orders/insert events produced"
    );
    disp.abort();
}

#[tokio::test]
async fn partition_key_determinism_across_ops_for_a_row() {
    // The insert/update/delete of one logical row (same `id`) must share a
    // partition key so a keyed consumer preserves per-row order.
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();

    // Row id=7 gets insert, update; row id=8 gets insert.
    publish_all(
        &writer,
        &[
            ev(project, "orders", ChangeOp::Insert, 1, 7),
            ev(project, "orders", ChangeOp::Insert, 2, 8),
            ev(project, "orders", ChangeOp::Update, 3, 7),
        ],
    )
    .await;

    let producer = MockProducer::new();
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let d = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p);
    wait_last_seq(&catalog, &project, "s1", 3).await;

    let batches = producer.produced.lock().await.clone();
    let mut by_seq: HashMap<u64, Vec<u8>> = HashMap::new();
    for b in &batches {
        for r in b {
            by_seq.insert(r.seq, r.key.clone());
        }
    }
    // Insert(seq1) and Update(seq3) of row 7 share a key.
    assert_eq!(by_seq[&1], by_seq[&3], "same row → same partition key");
    // Row 8 has a different key.
    assert_ne!(by_seq[&1], by_seq[&2], "different row → different key");
    d.abort();
}

#[tokio::test]
async fn tx_group_contiguity_preserved() {
    // Events of one commit have contiguous seq; the worker must produce them
    // contiguously and in order (never interleaving another commit's rows).
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();

    // Two "commits": seqs 1-3 (txA) and 4-6 (txB), interleaved into separate
    // ring flushes to exercise the cross-segment ordering.
    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 1, 1),
            ev(project, "t", ChangeOp::Insert, 2, 2),
            ev(project, "t", ChangeOp::Insert, 3, 3),
        ],
    )
    .await;
    publish_all(
        &writer,
        &[
            ev(project, "t", ChangeOp::Insert, 4, 4),
            ev(project, "t", ChangeOp::Insert, 5, 5),
            ev(project, "t", ChangeOp::Insert, 6, 6),
        ],
    )
    .await;

    let producer = MockProducer::new();
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let d = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, fast_cfg(), p);
    wait_last_seq(&catalog, &project, "s1", 6).await;

    let seqs = producer.delivered_seqs().await;
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6], "commits stay contiguous + ordered");
    d.abort();
}

#[tokio::test]
async fn cross_project_isolation() {
    // Two projects, separate rings + sinks. Each sink only ever sees its own
    // project's events.
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let a = ProjectId::new();
    let b = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(a, "sa")).await.unwrap();
    catalog.register_cdc_kafka_sink(def(b, "sb")).await.unwrap();

    publish_all(
        &writer,
        &[
            ev(a, "t", ChangeOp::Insert, 1, 1),
            ev(a, "t", ChangeOp::Insert, 2, 2),
        ],
    )
    .await;
    publish_all(&writer, &[ev(b, "t", ChangeOp::Insert, 1, 1)]).await;

    let prod_a = MockProducer::new();
    let prod_b = MockProducer::new();
    let pa: Arc<dyn KafkaProducer> = Arc::new(prod_a.clone());
    let pb: Arc<dyn KafkaProducer> = Arc::new(prod_b.clone());
    let da = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), a, fast_cfg(), pa);
    let db = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), b, fast_cfg(), pb);

    wait_last_seq(&catalog, &a, "sa", 2).await;
    wait_last_seq(&catalog, &b, "sb", 1).await;

    assert_eq!(prod_a.delivered_seqs().await, vec![1, 2]);
    assert_eq!(prod_b.delivered_seqs().await, vec![1]);
    // Sink A's records all carry topics for project A only.
    let a_batches = prod_a.produced.lock().await.clone();
    for batch in &a_batches {
        for r in batch {
            assert!(r.topic.contains(&a.to_string()), "A sink only sees A topics");
            assert!(!r.topic.contains(&b.to_string()));
        }
    }
    da.abort();
    db.abort();
}

#[tokio::test]
async fn auto_disable_after_max_delivery_age() {
    let dir = TempDir::new().unwrap();
    let writer = CdcRingWriter::with_config(storage_in(&dir), CdcConfig::default());
    let project = ProjectId::new();
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    catalog.register_cdc_kafka_sink(def(project, "s1")).await.unwrap();

    publish_all(&writer, &[ev(project, "t", ChangeOp::Insert, 1, 1)]).await;

    let producer = MockProducer::new();
    producer.always_fail.store(true, Ordering::SeqCst);
    let p: Arc<dyn KafkaProducer> = Arc::new(producer.clone());
    let cfg = KafkaConfig {
        max_delivery_age: Duration::from_millis(80),
        backoff_base: Duration::from_millis(10),
        max_backoff: Duration::from_millis(20),
        poll_interval: Duration::from_millis(10),
        ..fast_cfg()
    };
    let d = spawn_kafka_dispatcher(writer.clone(), catalog.clone(), project, cfg, p);

    eventually(|| async {
        catalog
            .list_cdc_kafka_sinks(&project)
            .await
            .first()
            .map(|r| !r.def.active && r.state.disabled_at.is_some())
            .unwrap_or(false)
    })
    .await;

    let rows = catalog.list_cdc_kafka_sinks(&project).await;
    assert_eq!(rows[0].state.last_seq, 0, "failed batch never acked");
    d.abort();
}

#[tokio::test]
async fn catalog_round_trip_in_memory() {
    // Subscription + cursor round-trip on the in-memory backend. The postgres
    // backend shares the trait + GREATEST-cursor contract; it is exercised in
    // the basin-catalog postgres integration suite when a DSN is present.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();

    assert!(catalog.list_cdc_kafka_sinks(&project).await.is_empty());

    let mut d = def(project, "s1");
    d.tables = Some(vec!["orders".into()]);
    d.ops = Some(vec!["insert".into(), "update".into()]);
    d.partition_key = PartitionKey::Project;
    catalog.register_cdc_kafka_sink(d.clone()).await.unwrap();

    // Duplicate id rejected.
    assert!(catalog.register_cdc_kafka_sink(d.clone()).await.is_err());

    let rows = catalog.list_cdc_kafka_sinks(&project).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].def.topic, "basin.{project}.{table}");
    assert_eq!(rows[0].def.partition_key, PartitionKey::Project);
    assert_eq!(rows[0].def.tables.as_deref(), Some(&["orders".to_string()][..]));
    assert_eq!(rows[0].state.last_seq, 0, "fresh cursor");

    // Ack advances monotonically; a lower ack never rewinds.
    catalog.record_cdc_kafka_ack(&project, "s1", 10, "ok").await.unwrap();
    catalog.record_cdc_kafka_ack(&project, "s1", 5, "ok").await.unwrap();
    let rows = catalog.list_cdc_kafka_sinks(&project).await;
    assert_eq!(rows[0].state.last_seq, 10, "GREATEST cursor — never rewinds");
    assert_eq!(rows[0].state.retry_count, 0, "ack resets retry_count");

    // Failure bumps retry_count + records status, does NOT advance the cursor.
    catalog.record_cdc_kafka_failure(&project, "s1", 3, "broker down").await.unwrap();
    let rows = catalog.list_cdc_kafka_sinks(&project).await;
    assert_eq!(rows[0].state.retry_count, 3);
    assert_eq!(rows[0].state.last_status.as_deref(), Some("broker down"));
    assert_eq!(rows[0].state.last_seq, 10);

    // Disable clears active + stamps disabled_at, retaining the row.
    catalog.disable_cdc_kafka_sink(&project, "s1").await.unwrap();
    let rows = catalog.list_cdc_kafka_sinks(&project).await;
    assert!(!rows[0].def.active);
    assert!(rows[0].state.disabled_at.is_some());

    // Drop removes it; second drop is NotFound.
    catalog.drop_cdc_kafka_sink(&project, "s1").await.unwrap();
    assert!(catalog.list_cdc_kafka_sinks(&project).await.is_empty());
    assert!(catalog.drop_cdc_kafka_sink(&project, "s1").await.is_err());
}
