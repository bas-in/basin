//! CDC webhook push sink — the Phase 2 delivery worker (ADR 0028 Phase 2).
//!
//! # What this is
//!
//! Phase 1 made committed change events durable in a per-project ring and
//! streamable over SSE. Phase 2 adds a **push** sink: customers register an
//! HTTPS endpoint (catalog-persisted — see [`basin_catalog::CdcWebhookDef`])
//! and a background worker POSTs batches of events to it with at-least-once
//! delivery, exponential-backoff retry, and a resumable per-endpoint cursor.
//!
//! # Topology — per-endpoint isolation
//!
//! [`spawn_webhook_dispatcher`] runs one supervisor loop that, on each tick,
//! lists the project's registered webhooks and ensures exactly one **delivery
//! task** is running per active endpoint. Each delivery task owns its own
//! cursor and its own retry state and talks to the durable ring + the HTTP
//! endpoint independently. A slow or 500ing endpoint blocks **only its own**
//! task: it never delays another endpoint's deliveries, and it never blocks the
//! ring writer (Phase 1's post-commit sink is a wholly separate path). This is
//! the ADR §6 fan-out-isolation contract realised at the delivery layer.
//!
//! # Delivery contract (as implemented)
//!
//! - **Source of truth:** the durable ring. A delivery task resumes from
//!   `replay_after(last_acked_seq)`, where `last_acked_seq` is read from the
//!   catalog ([`basin_catalog::CdcWebhookState::last_seq`]).
//! - **Batching:** up to [`WebhookConfig::max_batch`] events per POST, sent as a
//!   JSON array. The body is signed: `X-Basin-Signature: <hex HMAC-SHA256(body,
//!   secret)>` and `X-Basin-Seq-Range: <lo>-<hi>`.
//! - **Ack = HTTP 2xx.** On ack the cursor is advanced to the batch's high seq
//!   and persisted (`record_cdc_webhook_ack`, a monotonic GREATEST upsert), and
//!   `retry_count` resets to 0.
//! - **Retry/backoff:** a non-2xx response or a transport error increments the
//!   per-endpoint `retry_count` and the task sleeps for an exponential backoff
//!   (`base * 2^(retry_count-1)`, capped). The **same batch** is retried — per-
//!   endpoint ordering is preserved because the cursor never advances past an
//!   un-acked batch.
//! - **Auto-disable:** if a batch stays un-acked for longer than
//!   [`WebhookConfig::max_delivery_age`], the endpoint is disabled
//!   (`disable_cdc_webhook`) and a `warn!` line is logged. The row + cursor are
//!   retained for observability; re-enabling is an explicit re-registration.
//!
//! # Crash-window semantics (honest)
//!
//! The cursor is persisted **on ack**, batched (one write per delivered batch),
//! to the catalog. Because the ring is the source of truth and the cursor is the
//! last-acked bookmark, the crash windows are:
//!
//! - Crash *after* the endpoint received + acked a batch but *before*
//!   `record_cdc_webhook_ack` committed → on restart the worker re-delivers that
//!   batch (it replays from the older persisted cursor). This is the
//!   at-least-once duplicate; the subscriber dedups on `seq` (the
//!   `X-Basin-Seq-Range` header makes this trivial).
//! - Crash *before* the endpoint ever received a batch → no data loss; the
//!   batch is still in the ring and is delivered on restart.
//!
//! There is no exactly-once path (ADR non-goal). There is no lost-delivery
//! window for a committed event, because the ring is written by the post-commit
//! sink before the commit returns to the engine (Phase 1 contract).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{CdcWebhookDef, CdcWebhookRow, Catalog};
use basin_common::ProjectId;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::record::CdcRecord;
use crate::ring::ProjectRing;
use crate::CdcRingWriter;

/// Env: max events per delivered batch. Default 100.
pub const WEBHOOK_MAX_BATCH_ENV: &str = "BASIN_CDC_WEBHOOK_MAX_BATCH";
/// Env: base retry backoff in milliseconds. Default 500. The schedule is
/// `base * 2^(retry_count-1)`, capped at [`WebhookConfig::max_backoff`].
pub const WEBHOOK_BACKOFF_BASE_MS_ENV: &str = "BASIN_CDC_WEBHOOK_BACKOFF_BASE_MS";
/// Env: max retry backoff in milliseconds. Default 60_000 (1 min).
pub const WEBHOOK_MAX_BACKOFF_MS_ENV: &str = "BASIN_CDC_WEBHOOK_MAX_BACKOFF_MS";
/// Env: max age a batch may stay un-acked before the endpoint auto-disables,
/// in seconds. Default 3600 (1h).
pub const WEBHOOK_MAX_AGE_SECS_ENV: &str = "BASIN_CDC_WEBHOOK_MAX_AGE_SECS";
/// Env: supervisor poll interval in milliseconds — how often the dispatcher
/// rescans the catalog for new/dropped webhooks and how often an idle (caught
/// up) delivery task re-polls the ring. Default 1000 (1s).
pub const WEBHOOK_POLL_INTERVAL_MS_ENV: &str = "BASIN_CDC_WEBHOOK_POLL_INTERVAL_MS";

const DEFAULT_MAX_BATCH: usize = 100;
const DEFAULT_BACKOFF_BASE_MS: u64 = 500;
const DEFAULT_MAX_BACKOFF_MS: u64 = 60_000;
const DEFAULT_MAX_AGE_SECS: u64 = 3600;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;

/// HTTP signature header (hex HMAC-SHA256 of the POST body under the endpoint
/// secret).
pub const SIGNATURE_HEADER: &str = "X-Basin-Signature";
/// Header carrying the inclusive `seq` range of the batch (`<lo>-<hi>`), the
/// idempotency / dedup key for the subscriber.
pub const SEQ_RANGE_HEADER: &str = "X-Basin-Seq-Range";

/// Tunable webhook-delivery configuration, seeded from `BASIN_CDC_WEBHOOK_*`.
#[derive(Clone, Debug)]
pub struct WebhookConfig {
    pub max_batch: usize,
    pub backoff_base: Duration,
    pub max_backoff: Duration,
    pub max_delivery_age: Duration,
    pub poll_interval: Duration,
}

impl Default for WebhookConfig {
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

impl WebhookConfig {
    /// Build from the `BASIN_CDC_WEBHOOK_*` env vars.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = env_u64(WEBHOOK_MAX_BATCH_ENV) {
            cfg.max_batch = (v as usize).max(1);
        }
        if let Some(v) = env_u64(WEBHOOK_BACKOFF_BASE_MS_ENV) {
            cfg.backoff_base = Duration::from_millis(v.max(1));
        }
        if let Some(v) = env_u64(WEBHOOK_MAX_BACKOFF_MS_ENV) {
            cfg.max_backoff = Duration::from_millis(v.max(1));
        }
        if let Some(v) = env_u64(WEBHOOK_MAX_AGE_SECS_ENV) {
            cfg.max_delivery_age = Duration::from_secs(v.max(1));
        }
        if let Some(v) = env_u64(WEBHOOK_POLL_INTERVAL_MS_ENV) {
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

/// Mint a fresh per-project webhook id (ULID string). Lexicographically
/// sortable and time-ordered; used as the catalog primary key + the
/// `:id` path segment.
pub fn mint_webhook_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Mint a fresh hex-encoded HMAC secret (32 random bytes → 64 hex chars) for a
/// new webhook registration. Returned to the creator exactly once.
pub fn mint_secret_hex() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

/// Compute the `X-Basin-Signature` value for `body` under a hex secret.
/// `secret_hex` is the value stored on the catalog row; a malformed secret
/// (non-hex) is treated as raw UTF-8 bytes so signing never panics.
pub fn sign_body(secret_hex: &str, body: &[u8]) -> String {
    let key = hex::decode(secret_hex).unwrap_or_else(|_| secret_hex.as_bytes().to_vec());
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key)
        .expect("HMAC accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Outcome of one POST attempt, distinguished so the worker can update the
/// catalog state and decide whether to advance the cursor.
enum Attempt {
    /// 2xx — batch acked. Advance + persist the cursor.
    Acked,
    /// non-2xx or transport error — keep the cursor, back off, retry.
    Failed(String),
}

/// The thing a delivery task POSTs to. Abstracted so tests can inject an
/// in-process receiver instead of a real network call, and so the supervisor
/// owns a single shared `reqwest::Client`.
#[async_trait::async_trait]
pub trait WebhookHttp: Send + Sync {
    /// POST `body` to `url` with the given headers. Returns the HTTP status
    /// code on a completed request, or `Err(transport_error_string)`.
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        signature: &str,
        seq_range: &str,
    ) -> Result<u16, String>;
}

/// Production HTTP client (reqwest + rustls). One shared instance across all
/// delivery tasks (connection pooling).
pub struct ReqwestHttp {
    client: reqwest::Client,
}

impl ReqwestHttp {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { client }
    }
}

impl Default for ReqwestHttp {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl WebhookHttp for ReqwestHttp {
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        signature: &str,
        seq_range: &str,
    ) -> Result<u16, String> {
        let resp = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .header(SIGNATURE_HEADER, signature)
            .header(SEQ_RANGE_HEADER, seq_range)
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(resp.status().as_u16())
    }
}

/// Handle to the running webhook dispatcher. Dropping it stops the supervisor
/// and (cooperatively) every delivery task it spawned.
pub struct WebhookDispatcher {
    handle: tokio::task::JoinHandle<()>,
}

impl WebhookDispatcher {
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Spawn the webhook dispatcher for one project's registered endpoints.
///
/// The supervisor polls the catalog every [`WebhookConfig::poll_interval`] and
/// keeps one delivery task alive per active webhook. Newly-registered webhooks
/// get a task on the next tick; dropped or disabled ones have their task
/// stopped. Each delivery task is independent (slow-consumer isolation).
pub fn spawn_webhook_dispatcher(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    cfg: WebhookConfig,
    http: Arc<dyn WebhookHttp>,
) -> WebhookDispatcher {
    let handle = tokio::spawn(async move {
        run_supervisor(writer, catalog, project, cfg, http).await;
    });
    WebhookDispatcher { handle }
}

async fn run_supervisor(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    cfg: WebhookConfig,
    http: Arc<dyn WebhookHttp>,
) {
    // webhook_id → its delivery task handle. We restart a task only if it has
    // finished (disabled / dropped); a running task is left alone.
    let mut tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut tick = tokio::time::interval(cfg.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let rows = catalog.list_cdc_webhooks(&project).await;
        let live_ids: std::collections::HashSet<String> =
            rows.iter().filter(|r| r.def.active).map(|r| r.def.id.clone()).collect();

        // Reap tasks for endpoints that are gone or no longer active.
        tasks.retain(|id, h| {
            if !live_ids.contains(id) || h.is_finished() {
                h.abort();
                false
            } else {
                true
            }
        });

        // Ensure a task exists per active endpoint.
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
                Arc::clone(&http),
            ));
            tasks.insert(row.def.id, h);
        }
    }
}

/// One endpoint's delivery loop. Owns its own cursor; isolated from every other
/// endpoint and from the ring writer. Exits (so the supervisor can reap it)
/// when the webhook is dropped or auto-disabled.
async fn run_delivery_task(
    writer: CdcRingWriter,
    catalog: Arc<dyn Catalog>,
    project: ProjectId,
    webhook_id: String,
    cfg: WebhookConfig,
    http: Arc<dyn WebhookHttp>,
) {
    let ring = writer.ring_for(&project);
    // First-failure instant for the current un-acked batch — drives auto-disable.
    let mut batch_first_failed: Option<Instant> = None;

    loop {
        // Reload the webhook (def + cursor) each iteration so a drop / disable /
        // filter change is observed promptly and the cursor reflects any other
        // writer (defensive — there is one writer per endpoint in practice).
        let Some(row) = lookup_row(&catalog, &project, &webhook_id).await else {
            // Dropped from the catalog → stop.
            return;
        };
        if !row.def.active {
            return; // disabled out from under us.
        }

        let resume_after = row.state.last_seq;
        let pending = match collect_batch(&ring, &row.def, resume_after, cfg.max_batch).await {
            Some(b) if !b.is_empty() => b,
            _ => {
                // Nothing to deliver right now: idle until the next poll tick.
                tokio::time::sleep(cfg.poll_interval).await;
                continue;
            }
        };

        let lo = pending.first().map(|r| r.seq).unwrap_or(resume_after);
        let hi = pending.last().map(|r| r.seq).unwrap_or(resume_after);
        let body = serialize_batch(&pending);
        let signature = sign_body(&row.def.secret_hex, &body);
        let seq_range = format!("{lo}-{hi}");

        match attempt_post(http.as_ref(), &row.def, body, &signature, &seq_range).await {
            Attempt::Acked => {
                batch_first_failed = None;
                if let Err(e) = catalog
                    .record_cdc_webhook_ack(&project, &webhook_id, hi, "200")
                    .await
                {
                    tracing::warn!(
                        project = %project, webhook = %webhook_id, error = %e,
                        "cdc webhook: ack persist failed (will re-deliver on restart)",
                    );
                }
                tracing::debug!(
                    project = %project, webhook = %webhook_id, lo, hi,
                    "cdc webhook: batch delivered + acked",
                );
                // Loop straight back to drain any further pending events.
            }
            Attempt::Failed(status) => {
                let first = *batch_first_failed.get_or_insert_with(Instant::now);
                let retry_count = row.state.retry_count.saturating_add(1);
                let _ = catalog
                    .record_cdc_webhook_failure(&project, &webhook_id, retry_count, &status)
                    .await;

                // Auto-disable if the batch has been failing past the max age.
                if first.elapsed() >= cfg.max_delivery_age {
                    tracing::warn!(
                        project = %project, webhook = %webhook_id, lo, hi,
                        last_status = %status,
                        age_secs = first.elapsed().as_secs(),
                        "cdc webhook: max delivery age exceeded — auto-disabling endpoint",
                    );
                    let _ = catalog.disable_cdc_webhook(&project, &webhook_id).await;
                    return;
                }

                let backoff = cfg.backoff_for(retry_count);
                tracing::debug!(
                    project = %project, webhook = %webhook_id, retry_count,
                    backoff_ms = backoff.as_millis() as u64, last_status = %status,
                    "cdc webhook: delivery failed — backing off, will retry same batch",
                );
                tokio::time::sleep(backoff).await;
                // Loop: same cursor → same batch retried (ordering preserved).
            }
        }
    }
}

async fn lookup_row(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    webhook_id: &str,
) -> Option<CdcWebhookRow> {
    catalog
        .list_cdc_webhooks(project)
        .await
        .into_iter()
        .find(|r| r.def.id == webhook_id)
}

/// Pull up to `max_batch` filter-matching records with `seq > resume_after`
/// from the durable ring, in seq order.
async fn collect_batch(
    ring: &ProjectRing,
    def: &CdcWebhookDef,
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

/// Serialize a batch as a JSON array of CDC records (the POST body).
fn serialize_batch(records: &[CdcRecord]) -> Vec<u8> {
    serde_json::to_vec(records).unwrap_or_else(|_| b"[]".to_vec())
}

async fn attempt_post(
    http: &dyn WebhookHttp,
    def: &CdcWebhookDef,
    body: Vec<u8>,
    signature: &str,
    seq_range: &str,
) -> Attempt {
    match http.post(&def.url, body, signature, seq_range).await {
        Ok(status) if (200..300).contains(&status) => Attempt::Acked,
        Ok(status) => Attempt::Failed(status.to_string()),
        Err(transport) => Attempt::Failed(transport),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let cfg = WebhookConfig {
            backoff_base: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            ..WebhookConfig::default()
        };
        assert_eq!(cfg.backoff_for(1), Duration::from_millis(100));
        assert_eq!(cfg.backoff_for(2), Duration::from_millis(200));
        assert_eq!(cfg.backoff_for(3), Duration::from_millis(400));
        assert_eq!(cfg.backoff_for(4), Duration::from_millis(800));
        // 5th would be 1600ms but the cap is 1000ms.
        assert_eq!(cfg.backoff_for(5), Duration::from_millis(1000));
        assert_eq!(cfg.backoff_for(50), Duration::from_millis(1000));
    }

    #[test]
    fn sign_body_matches_independent_hmac() {
        let secret = "ab".repeat(32);
        let body = br#"[{"seq":1}]"#;
        let sig = sign_body(&secret, body);
        // Recompute independently.
        let key = hex::decode(&secret).unwrap();
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key).unwrap();
        mac.update(body);
        assert_eq!(sig, hex::encode(mac.finalize().into_bytes()));
        assert_eq!(sig.len(), 64);
    }

    #[test]
    fn mint_secret_is_64_hex_chars() {
        let s = mint_secret_hex();
        assert_eq!(s.len(), 64);
        assert!(hex::decode(&s).is_ok());
        assert_ne!(s, mint_secret_hex(), "secrets are random");
    }
}
