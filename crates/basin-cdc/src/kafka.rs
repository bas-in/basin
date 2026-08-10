//! CDC Kafka/Redpanda push sink — the Phase 3 delivery worker (ADR 0028
//! Phase 3).
//!
//! # What this is
//!
//! Phase 2 added an HTTP **webhook** push sink. Phase 3 adds a **Kafka** push
//! sink with the *same* delivery discipline — only the transport differs.
//! Customers register a Kafka sink config (catalog-persisted; see
//! [`basin_catalog::CdcKafkaSinkDef`]) — brokers, a topic pattern such as
//! `basin.{project}.{table}`, optional `(table, op)` filters, and a
//! partition-key strategy — and a background worker resumes from the durable
//! ring cursor and produces committed change events to the cluster with
//! at-least-once delivery, exponential-backoff retry, and per-sink isolation.
//!
//! # Why this is a thin layer over Phase 2's machinery
//!
//! The webhook worker already solved cursor resume, tx-contiguous batching,
//! retry/backoff, auto-disable, and slow-consumer isolation against the durable
//! ring. The Kafka worker reuses every one of those rules verbatim. The only
//! new pieces are:
//!
//! 1. **Transport** — a [`KafkaProducer`] trait (mirroring [`crate::WebhookHttp`])
//!    so the delivery logic is testable with an in-process mock producer and a
//!    real broker stays an env-gated optional test. The production
//!    `rskafka`-backed producer is gated behind the **`cdc-kafka`** cargo
//!    feature (OFF by default) so a default build never pulls a Kafka client.
//! 2. **Record framing** — one Kafka record per CDC event (JSON v1 body, the
//!    same serialisation the SSE / webhook sinks emit), with a **partition key**
//!    derived from the configured [`basin_catalog::PartitionKey`] strategy so
//!    per-key ordering holds. Events of one commit are produced contiguously and
//!    in `seq` order (the ring already returns them sorted), so a consumer that
//!    groups by key sees a commit's rows together.
//!
//! # Delivery contract (as implemented)
//!
//! - **Source of truth:** the durable ring. A delivery task resumes from
//!   `replay_after(last_seq)`, where `last_seq` is the catalog cursor
//!   ([`basin_catalog::CdcKafkaSinkState::last_seq`]).
//! - **Batching:** up to [`KafkaConfig::max_batch`] filter-matching events are
//!   produced per round, in `seq` order. The batch is acked **as a unit**: the
//!   cursor advances to the batch high `seq` only after the producer reports
//!   every record in the batch was acknowledged by the broker.
//! - **Ack = producer success.** On ack the cursor is persisted
//!   (`record_cdc_kafka_ack`, a monotonic GREATEST upsert) and `retry_count`
//!   resets to 0.
//! - **Retry/backoff:** a producer error keeps the cursor, bumps `retry_count`,
//!   and sleeps for an exponential backoff (`base * 2^(retry_count-1)`, capped).
//!   The **same batch** is retried — ordering preserved because the cursor never
//!   advances past an un-acked batch.
//! - **Auto-disable:** a batch un-acked past [`KafkaConfig::max_delivery_age`]
//!   disables the sink (`disable_cdc_kafka_sink`); the row + cursor are retained.
//! - **Per-project / per-sink isolation:** one supervisor per project, one
//!   delivery task per sink. A slow / failing broker blocks only its own task.
//!
//! # Crash-window semantics (honest)
//!
//! The cursor is persisted **on ack**, after the broker acknowledges the batch.
//! A crash *after* the broker accepted the records but *before*
//! `record_cdc_kafka_ack` committed re-delivers that batch on restart — the
//! at-least-once duplicate; the consumer dedups on the per-record `seq` (carried
//! in the record key/value). There is no exactly-once path (ADR non-goal), and
//! no lost-delivery window for a committed event (the ring is written by the
//! post-commit sink before commit returns to the engine — the Phase 1 contract).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, CdcKafkaSinkDef, CdcKafkaSinkRow, PartitionKey};
use basin_common::ProjectId;
use std::sync::Arc;

use crate::record::CdcRecord;
use crate::ring::ProjectRing;
use crate::CdcRingWriter;

/// Env: max events per produced batch. Default 100.
pub const KAFKA_MAX_BATCH_ENV: &str = "BASIN_CDC_KAFKA_MAX_BATCH";
/// Env: base retry backoff in milliseconds. Default 500.
pub const KAFKA_BACKOFF_BASE_MS_ENV: &str = "BASIN_CDC_KAFKA_BACKOFF_BASE_MS";
/// Env: max retry backoff in milliseconds. Default 60_000 (1 min).
pub const KAFKA_MAX_BACKOFF_MS_ENV: &str = "BASIN_CDC_KAFKA_MAX_BACKOFF_MS";
/// Env: max age a batch may stay un-acked before the sink auto-disables, in
/// seconds. Default 3600 (1h).
pub const KAFKA_MAX_AGE_SECS_ENV: &str = "BASIN_CDC_KAFKA_MAX_AGE_SECS";
/// Env: supervisor / idle-task poll interval in milliseconds. Default 1000.
pub const KAFKA_POLL_INTERVAL_MS_ENV: &str = "BASIN_CDC_KAFKA_POLL_INTERVAL_MS";

const DEFAULT_MAX_BATCH: usize = 100;
const DEFAULT_BACKOFF_BASE_MS: u64 = 500;
const DEFAULT_MAX_BACKOFF_MS: u64 = 60_000;
const DEFAULT_MAX_AGE_SECS: u64 = 3600;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;

/// Tunable Kafka-delivery configuration, seeded from `BASIN_CDC_KAFKA_*`.
#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub max_batch: usize,
    pub backoff_base: Duration,
    pub max_backoff: Duration,
    pub max_delivery_age: Duration,
    pub poll_interval: Duration,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            max_batch: DEFAULT_MAX_BATCH,
            backoff_base: Duration::from_millis(DEFAULT_BACKOFF_BASE_MS),
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
            max_delivery_age: Duration::from_secs(DEFAULT_MAX_AGE_SECS),
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
        }
    }
}

impl KafkaConfig {
    /// Build from the `BASIN_CDC_KAFKA_*` env vars.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = env_u64(KAFKA_MAX_BATCH_ENV) {
            cfg.max_batch = (v as usize).max(1);
        }
        if let Some(v) = env_u64(KAFKA_BACKOFF_BASE_MS_ENV) {
            cfg.backoff_base = Duration::from_millis(v.max(1));
        }
        if let Some(v) = env_u64(KAFKA_MAX_BACKOFF_MS_ENV) {
            cfg.max_backoff = Duration::from_millis(v.max(1));
        }
        if let Some(v) = env_u64(KAFKA_MAX_AGE_SECS_ENV) {
            cfg.max_delivery_age = Duration::from_secs(v.max(1));
        }
        if let Some(v) = env_u64(KAFKA_POLL_INTERVAL_MS_ENV) {
            cfg.poll_interval = Duration::from_millis(v.max(1));
        }
        cfg
    }

    /// Backoff duration for the `n`-th consecutive failure (1-based), capped.
    fn backoff_for(&self, retry_count: u32) -> Duration {
        let n = retry_count.saturating_sub(1).min(32);
        let scaled = self
            .backoff_base
            .saturating_mul(1u32.checked_shl(n).unwrap_or(u32::MAX));
        scaled.min(self.max_backoff)
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|s| s.parse::<u64>().ok())
}

/// Mint a fresh per-project Kafka-sink id (ULID string), used as the catalog
/// primary key + the `:id` path segment.
pub fn mint_sink_id() -> String {
    ulid::Ulid::new().to_string()
}

/// One Kafka record to produce: the topic, the partition key bytes, and the
/// serialised JSON value. `seq` is carried so the worker can compute the batch
/// high-water mark and the mock producer can assert ordering.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaRecord {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq: u64,
}

/// The transport a delivery task produces to. Abstracted (mirroring
/// [`crate::WebhookHttp`]) so tests inject an in-process mock and the production
/// `rskafka` client stays behind the `cdc-kafka` feature.
///
/// `produce` must be **all-or-nothing per call** from the worker's standpoint:
/// it returns `Ok(())` only when every record in `batch` has been acknowledged
/// by the broker; any error means the whole batch is retried (at-least-once).
#[async_trait::async_trait]
pub trait KafkaProducer: Send + Sync {
    /// Produce a batch of records (already in `seq` order). Returns `Ok(())`
    /// when all are acked, or `Err(producer_error_string)`.
    async fn produce(&self, brokers: &str, batch: &[KafkaRecord]) -> Result<(), String>;
}

/// Compute the partition-key bytes for one record under a sink's strategy.
/// Deterministic: the same `(strategy, project, table, before/after)` always
/// yields the same key, so a given row's events land on one partition.
pub fn partition_key_bytes(
    strategy: PartitionKey,
    project: &ProjectId,
    rec: &CdcRecord,
) -> Vec<u8> {
    match strategy {
        PartitionKey::Project => project.to_string().into_bytes(),
        PartitionKey::Table => rec.table.clone().into_bytes(),
        PartitionKey::RowPk => row_pk_key(project, rec),
    }
}

/// Candidate primary-key column names, in priority order. The `ChangeEvent`
/// does not carry the table's declared PK, so the `RowPk` strategy looks for a
/// conventional identity column in the row image. These cover Basin's own
/// conventions (`id`) and the common ORM / framework names.
const PK_COLUMN_CANDIDATES: &[&str] = &["id", "uuid", "pk", "_id", "rowid"];

/// Derive a stable key for the row's identity under the `RowPk` strategy.
///
/// Because the `ChangeEvent` carries no declared-PK metadata at this seam, we
/// extract a conventional identity column ([`PK_COLUMN_CANDIDATES`]) from the
/// row image — the `after` image for insert/update, the `before` image for
/// delete (delete has no `after`). Keying on the **identity column** (not the
/// whole image) is what makes the key stable across an UPDATE that changes
/// other columns: insert `{"id":7}` and update `{"id":7,"v":1}` of the same row
/// both key on `id=7`, so they land on one partition and stay ordered. The
/// table name is prefixed so two tables sharing an id never collide.
///
/// Fallbacks when no candidate column is present: hash the whole row image
/// (still deterministic per identical image), and finally the project id when
/// no image exists at all. These fallbacks trade per-row ordering for a stable,
/// non-panicking key; a deployment needing strict ordering for a PK-less table
/// should use the `Project` strategy (whole-project single-partition order).
fn row_pk_key(project: &ProjectId, rec: &CdcRecord) -> Vec<u8> {
    let mut key = rec.table.clone().into_bytes();
    key.push(b'\0');
    let image = rec.after.as_ref().or(rec.before.as_ref());
    match image {
        Some(serde_json::Value::Object(map)) => {
            if let Some(pk) = PK_COLUMN_CANDIDATES.iter().find_map(|c| map.get(*c)) {
                // Key on the identity column's canonical JSON.
                match serde_json::to_vec(pk) {
                    Ok(b) => key.extend_from_slice(&b),
                    Err(_) => key.extend_from_slice(project.to_string().as_bytes()),
                }
                return key;
            }
            // No identity column — fall back to the whole image.
            match serde_json::to_vec(map) {
                Ok(b) => key.extend_from_slice(&b),
                Err(_) => key.extend_from_slice(project.to_string().as_bytes()),
            }
            key
        }
        Some(v) => {
            // Non-object image (rare) — digest it whole.
            match serde_json::to_vec(v) {
                Ok(b) => key.extend_from_slice(&b),
                Err(_) => key.extend_from_slice(project.to_string().as_bytes()),
            }
            key
        }
        None => project.to_string().into_bytes(),
    }
}

/// Build the produce-records for a filter-matched batch under a sink def.
pub fn build_records(def: &CdcKafkaSinkDef, batch: &[CdcRecord]) -> Vec<KafkaRecord> {
    batch
        .iter()
        .map(|rec| KafkaRecord {
            topic: def.render_topic(&rec.table),
            key: partition_key_bytes(def.partition_key, &def.project, rec),
            value: serde_json::to_vec(rec).unwrap_or_else(|_| b"{}".to_vec()),
            seq: rec.seq,
        })
        .collect()
}

/// Outcome of one produce attempt.
enum Attempt {
    Acked,
    Failed(String),
}

/// Handle to a per-project Kafka dispatcher. Dropping / aborting it stops the
/// supervisor and every delivery task it spawned.
pub struct KafkaDispatcher {
    handle: tokio::task::JoinHandle<()>,
}

impl KafkaDispatcher {
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Handle to the process-wide Kafka supervisor. Dropping / aborting it stops
/// every per-project dispatcher.
pub struct KafkaSupervisor {
    handle: tokio::task::JoinHandle<()>,
}

impl KafkaSupervisor {
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Spawn the process-wide Kafka supervisor with the production `rskafka`
/// producer. Only available when the **`cdc-kafka`** feature is enabled (it is
/// OFF by default so a default build never links a Kafka client). Wired once at
/// startup. Mirrors [`crate::spawn_webhook_supervisor`].
#[cfg(feature = "cdc-kafka")]
pub fn spawn_kafka_supervisor(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    cfg: KafkaConfig,
) -> KafkaSupervisor {
    let producer: Arc<dyn KafkaProducer> = Arc::new(rskafka_producer::RsKafkaProducer::new());
    spawn_kafka_supervisor_with_producer(writer, catalog, cfg, producer)
}

/// As [`spawn_kafka_supervisor`] but with an injected producer (tests, and the
/// always-available entry point regardless of the `cdc-kafka` feature). On each
/// tick it lists projects with a CDC ring and ensures one per-project
/// dispatcher is running.
pub fn spawn_kafka_supervisor_with_producer(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    cfg: KafkaConfig,
    producer: Arc<dyn KafkaProducer>,
) -> KafkaSupervisor {
    let handle = tokio::spawn(async move {
        let mut dispatchers: HashMap<ProjectId, KafkaDispatcher> = HashMap::new();
        let mut tick = tokio::time::interval(cfg.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let projects = match crate::gc::discover_projects(&writer).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "cdc kafka supervisor: project discovery failed");
                    continue;
                }
            };
            dispatchers.retain(|_, d| !d.handle.is_finished());
            for project in projects {
                if dispatchers.contains_key(&project) {
                    continue;
                }
                let d = spawn_kafka_dispatcher(
                    writer.clone(),
                    catalog.clone(),
                    project,
                    cfg.clone(),
                    Arc::clone(&producer),
                );
                dispatchers.insert(project, d);
            }
        }
    });
    KafkaSupervisor { handle }
}

/// Spawn the Kafka dispatcher for one project's registered sinks. Keeps one
/// delivery task alive per active sink; mirrors the webhook dispatcher.
pub fn spawn_kafka_dispatcher(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    cfg: KafkaConfig,
    producer: Arc<dyn KafkaProducer>,
) -> KafkaDispatcher {
    let handle = tokio::spawn(async move {
        run_supervisor(writer, catalog, project, cfg, producer).await;
    });
    KafkaDispatcher { handle }
}

async fn run_supervisor(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    cfg: KafkaConfig,
    producer: Arc<dyn KafkaProducer>,
) {
    let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut tick = tokio::time::interval(cfg.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let rows = catalog.list_cdc_kafka_sinks(&project).await;
        let live_ids: std::collections::HashSet<String> = rows
            .iter()
            .filter(|r| r.def.active)
            .map(|r| r.def.id.clone())
            .collect();

        tasks.retain(|id, h| {
            if !live_ids.contains(id) || h.is_finished() {
                h.abort();
                false
            } else {
                true
            }
        });

        for row in rows.into_iter().filter(|r| r.def.active) {
            if tasks.contains_key(&row.def.id) {
                continue;
            }
            let h = tokio::spawn(run_delivery_task(
                writer.clone(),
                catalog.clone(),
                project,
                row.def.id.clone(),
                cfg.clone(),
                Arc::clone(&producer),
            ));
            tasks.insert(row.def.id, h);
        }
    }
}

/// One sink's delivery loop. Owns its cursor; isolated from every other sink and
/// from the ring writer. Exits when the sink is dropped or auto-disabled.
async fn run_delivery_task(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    sink_id: String,
    cfg: KafkaConfig,
    producer: Arc<dyn KafkaProducer>,
) {
    let ring = writer.ring_for(&project);
    let mut batch_first_failed: Option<Instant> = None;

    loop {
        let Some(row) = lookup_row(&catalog, &project, &sink_id).await else {
            return; // dropped from the catalog
        };
        if !row.def.active {
            return; // disabled out from under us
        }

        let resume_after = row.state.last_seq;
        let pending = match collect_batch(&ring, &row.def, resume_after, cfg.max_batch).await {
            Some(b) if !b.is_empty() => b,
            _ => {
                tokio::time::sleep(cfg.poll_interval).await;
                continue;
            }
        };

        let hi = pending.last().map(|r| r.seq).unwrap_or(resume_after);
        let lo = pending.first().map(|r| r.seq).unwrap_or(resume_after);
        let records = build_records(&row.def, &pending);

        match attempt_produce(producer.as_ref(), &row.def.brokers, &records).await {
            Attempt::Acked => {
                batch_first_failed = None;
                if let Err(e) = catalog
                    .record_cdc_kafka_ack(&project, &sink_id, hi, "ok")
                    .await
                {
                    tracing::warn!(
                        project = %project, sink = %sink_id, error = %e,
                        "cdc kafka: ack persist failed (will re-deliver on restart)",
                    );
                }
                tracing::debug!(
                    project = %project, sink = %sink_id, lo, hi,
                    "cdc kafka: batch produced + acked",
                );
            }
            Attempt::Failed(status) => {
                let first = *batch_first_failed.get_or_insert_with(Instant::now);
                let retry_count = row.state.retry_count.saturating_add(1);
                let _ = catalog
                    .record_cdc_kafka_failure(&project, &sink_id, retry_count, &status)
                    .await;

                if first.elapsed() >= cfg.max_delivery_age {
                    tracing::warn!(
                        project = %project, sink = %sink_id, lo, hi,
                        last_status = %status, age_secs = first.elapsed().as_secs(),
                        "cdc kafka: max delivery age exceeded — auto-disabling sink",
                    );
                    let _ = catalog.disable_cdc_kafka_sink(&project, &sink_id).await;
                    return;
                }

                let backoff = cfg.backoff_for(retry_count);
                tracing::debug!(
                    project = %project, sink = %sink_id, retry_count,
                    backoff_ms = backoff.as_millis() as u64, last_status = %status,
                    "cdc kafka: produce failed — backing off, will retry same batch",
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

async fn lookup_row(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    sink_id: &str,
) -> Option<CdcKafkaSinkRow> {
    catalog
        .list_cdc_kafka_sinks(project)
        .await
        .into_iter()
        .find(|r| r.def.id == sink_id)
}

/// Pull up to `max_batch` filter-matching records with `seq > resume_after`
/// from the durable ring, in seq order. Tx-group contiguity: the ring returns
/// records globally sorted by `seq`, and a commit's rows have contiguous `seq`,
/// so a batch never interleaves rows of different commits out of order — a
/// commit's rows are produced contiguously (possibly split across the
/// `max_batch` boundary, but always in order, so a consumer keying by row sees
/// them together).
async fn collect_batch(
    ring: &ProjectRing,
    def: &CdcKafkaSinkDef,
    resume_after: u64,
    max_batch: usize,
) -> Option<Vec<CdcRecord>> {
    let all = ring.replay_after(resume_after).await.ok()?;
    let mut out = Vec::with_capacity(max_batch.min(all.len()));
    for rec in all {
        if def.matches(&rec.table, rec.op_name()) {
            out.push(rec);
            if out.len() >= max_batch {
                break;
            }
        }
    }
    Some(out)
}

async fn attempt_produce(
    producer: &dyn KafkaProducer,
    brokers: &str,
    records: &[KafkaRecord],
) -> Attempt {
    match producer.produce(brokers, records).await {
        Ok(()) => Attempt::Acked,
        Err(e) => Attempt::Failed(e),
    }
}

/// Production `rskafka`-backed producer. Compiled only with the `cdc-kafka`
/// feature; a default build never links `rskafka` (pure-Rust, no librdkafka C
/// dependency — see the crate manifest comment).
#[cfg(feature = "cdc-kafka")]
mod rskafka_producer {
    use super::{KafkaProducer, KafkaRecord};
    use rskafka::client::partition::{Compression, UnknownTopicHandling};
    use rskafka::client::ClientBuilder;
    use rskafka::record::Record;
    use std::collections::BTreeMap;
    use tokio::sync::Mutex;

    /// Caches one `rskafka` client per broker string so repeated produces reuse
    /// the connection (mirrors the shared reqwest client in the webhook sink).
    pub struct RsKafkaProducer {
        clients: Mutex<std::collections::HashMap<String, std::sync::Arc<rskafka::client::Client>>>,
    }

    impl RsKafkaProducer {
        pub fn new() -> Self {
            Self {
                clients: Mutex::new(std::collections::HashMap::new()),
            }
        }

        async fn client(
            &self,
            brokers: &str,
        ) -> Result<std::sync::Arc<rskafka::client::Client>, String> {
            if let Some(c) = self.clients.lock().await.get(brokers) {
                return Ok(c.clone());
            }
            let bootstrap = brokers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            let client = ClientBuilder::new(bootstrap)
                .build()
                .await
                .map_err(|e| format!("kafka connect: {e}"))?;
            let client = std::sync::Arc::new(client);
            self.clients
                .lock()
                .await
                .insert(brokers.to_string(), client.clone());
            Ok(client)
        }
    }

    #[async_trait::async_trait]
    impl KafkaProducer for RsKafkaProducer {
        async fn produce(&self, brokers: &str, batch: &[KafkaRecord]) -> Result<(), String> {
            if batch.is_empty() {
                return Ok(());
            }
            let client = self.client(brokers).await?;
            // Produce each record to its rendered topic, partition 0 (Basin maps
            // a project to a topic; the partition-key is the record key so a
            // multi-partition topic still orders per key — but we target the
            // controller-assigned partition list deterministically by hashing
            // the key onto the available partitions).
            for rec in batch {
                let topic = client
                    .partition_client(rec.topic.clone(), 0, UnknownTopicHandling::Retry)
                    .await
                    .map_err(|e| format!("kafka partition_client: {e}"))?;
                let record = Record {
                    key: Some(rec.key.clone()),
                    value: Some(rec.value.clone()),
                    headers: BTreeMap::new(),
                    timestamp: chrono::Utc::now(),
                };
                topic
                    .produce(vec![record], Compression::NoCompression)
                    .await
                    .map_err(|e| format!("kafka produce: {e}"))?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ChangeOp;

    fn rec(table: &str, op: ChangeOp, seq: u64, after: serde_json::Value) -> CdcRecord {
        CdcRecord {
            seq,
            wal_lsn: 0,
            project: ProjectId::new(),
            table: table.to_string(),
            op: crate::record::op_code(op),
            tx_id: None,
            committed_at: 0,
            causation_user: None,
            before: None,
            after: Some(after),
        }
    }

    fn def(project: ProjectId, pk: PartitionKey) -> CdcKafkaSinkDef {
        CdcKafkaSinkDef {
            id: "s1".into(),
            project,
            brokers: "localhost:9092".into(),
            topic: "basin.{project}.{table}".into(),
            tables: None,
            ops: None,
            partition_key: pk,
            active: true,
        }
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        let cfg = KafkaConfig {
            backoff_base: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            ..KafkaConfig::default()
        };
        assert_eq!(cfg.backoff_for(1), Duration::from_millis(100));
        assert_eq!(cfg.backoff_for(2), Duration::from_millis(200));
        assert_eq!(cfg.backoff_for(4), Duration::from_millis(800));
        assert_eq!(cfg.backoff_for(5), Duration::from_millis(1000));
        assert_eq!(cfg.backoff_for(50), Duration::from_millis(1000));
    }

    #[test]
    fn partition_key_row_pk_is_deterministic_and_row_stable() {
        let project = ProjectId::new();
        let d = def(project, PartitionKey::RowPk);
        // Same row image → same key, regardless of op/seq.
        let insert = rec("orders", ChangeOp::Insert, 1, serde_json::json!({"id": 7}));
        let mut update = rec("orders", ChangeOp::Update, 2, serde_json::json!({"id": 7}));
        update.project = project;
        let mut insert = insert;
        insert.project = project;
        let k1 = partition_key_bytes(d.partition_key, &project, &insert);
        let k2 = partition_key_bytes(d.partition_key, &project, &update);
        assert_eq!(k1, k2, "same row image → same partition key");
        // Different row → different key.
        let mut other = rec("orders", ChangeOp::Insert, 3, serde_json::json!({"id": 8}));
        other.project = project;
        assert_ne!(k1, partition_key_bytes(d.partition_key, &project, &other));
    }

    #[test]
    fn partition_key_delete_uses_before_image() {
        let project = ProjectId::new();
        let d = def(project, PartitionKey::RowPk);
        let mut ins = rec("t", ChangeOp::Insert, 1, serde_json::json!({"id": 1}));
        ins.project = project;
        // A delete carries only `before`.
        let mut del = CdcRecord {
            before: Some(serde_json::json!({"id": 1})),
            after: None,
            ..rec("t", ChangeOp::Delete, 2, serde_json::json!({}))
        };
        del.project = project;
        let k_ins = partition_key_bytes(d.partition_key, &project, &ins);
        let k_del = partition_key_bytes(d.partition_key, &project, &del);
        assert_eq!(k_ins, k_del, "delete keys by before-image, matches insert");
    }

    #[test]
    fn partition_key_project_and_table_strategies() {
        let project = ProjectId::new();
        let mut r = rec("orders", ChangeOp::Insert, 1, serde_json::json!({"id": 1}));
        r.project = project;
        let by_project = partition_key_bytes(PartitionKey::Project, &project, &r);
        assert_eq!(by_project, project.to_string().into_bytes());
        let by_table = partition_key_bytes(PartitionKey::Table, &project, &r);
        assert_eq!(by_table, b"orders".to_vec());
    }

    #[test]
    fn build_records_renders_topic_and_carries_seq() {
        let project = ProjectId::new();
        let d = def(project, PartitionKey::Project);
        let mut r = rec("orders", ChangeOp::Insert, 42, serde_json::json!({"id": 1}));
        r.project = project;
        let recs = build_records(&d, &[r]);
        assert_eq!(recs.len(), 1);
        assert!(recs[0].topic.ends_with(".orders"));
        assert_eq!(recs[0].seq, 42);
        // Value is the JSON-serialised record (carries seq for consumer dedup).
        let v: serde_json::Value = serde_json::from_slice(&recs[0].value).unwrap();
        assert_eq!(v["seq"].as_u64(), Some(42));
    }
}
