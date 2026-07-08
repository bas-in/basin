//! `POST /internal/v1/partition-write` — the partition-owner receive endpoint
//! for the multi-node bulk-ingest write path (#28).
//!
//! N Basin engine nodes share one project's object-store catalog + lease
//! registry + Tigris data. A bulk COPY lands on whichever node the client
//! connected to, but the write fan-out (`executor::write_batch_fanout`) spreads
//! a table's chunks across P partitions, each with ONE deterministic owner node
//! (see `basin_engine::partition_router`). When the sender node does not own a
//! partition it POSTs the already-parsed, already-constraint-checked Arrow
//! RecordBatch to THIS handler on the owner node, which writes it locally.
//!
//! # Wire format (must match `basin_engine::write_forwarder::HttpPartitionForwardClient`)
//!
//! * Body: a raw Arrow IPC stream (schema + exactly one RecordBatch), encoded by
//!   [`basin_engine::write_forwarder::encode_partition_batch`] and decoded here
//!   by [`basin_engine::write_forwarder::decode_partition_batch`]. No JSON / no
//!   base64 envelope — bulk batches are large.
//! * Headers carry the destination:
//!   - `x-basin-partition-project` — project ULID
//!   - `x-basin-partition-table`   — table name
//!   - `x-basin-partition-id`      — partition key
//!   - `x-basin-partition-hop`     — forward hop count; a forwarded write is
//!     always `1`. The receiver rejects `hop != 1` (loop guard: it must never
//!     re-forward, which would let A→B→A spin).
//!   - `x-basin-forward-secret`    — the shared secret (constant-time checked).
//! * Response: a JSON object `{"rows":N,"durable_lsn":M}` (#46 durability
//!   audit) — the rows committed AND the owner's confirmed durable WAL
//!   high-water LSN for the partition after the write. The sender
//!   ([`basin_engine::write_forwarder::HttpPartitionForwardClient`]) decodes
//!   either this object or a bare number (back-compat with a pre-audit peer).
//!
//! # Why no constraint re-check
//!
//! PK / UNIQUE / NOT NULL constraints already ran on the SENDER across ALL
//! partitions of the statement, before fan-out (see `write_batch_fanout`). The
//! receiver is by construction the partition's lease owner, so it does a RAW
//! partition write (`write_batch_opts(table, batch, durable=true)`) and does NOT
//! re-run constraints — re-checking here would be redundant work and, for a
//! cross-partition UNIQUE, would falsely fail (this node only sees its own
//! partition). Single-writer-per-partition is preserved by the lease CAS that
//! `shard.get` performs (`ensure_lease`): the owner holds the writer lease, so a
//! second node forwarding to the same partition cannot also write it.
//!
//! # Security
//!
//! Identical posture to [`crate::routes::internal_forward`]:
//!
//! * **Fail-closed mounting.** Mounted only when `BASIN_FORWARD_SECRET` is set
//!   (see [`crate::server::router`]). With no secret the path 404s.
//! * **Shared-secret check.** Every request must carry the secret in the
//!   `x-basin-forward-secret` header (constant-time compare). On Fly the
//!   endpoint is additionally reachable only over the private 6PN network.

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::write_forwarder::{
    decode_partition_batch, FORWARD_SECRET_HEADER, PARTITION_FORWARD_HOP_HEADER,
    PARTITION_FORWARD_IDEM_HEADER, PARTITION_FORWARD_PARTITION_HEADER,
    PARTITION_FORWARD_PROJECT_HEADER, PARTITION_FORWARD_TABLE_HEADER,
};

use crate::errors::ApiError;
use crate::server::Inner;

/// How many recently-applied idempotency keys the per-process dedup window
/// retains. A lost-ack retry fires near-immediately (the sender backs off only
/// ~50ms before retrying — see `executor::forward_partition_to_owner`), so the
/// retry's key is still in the window even under heavy concurrent ingest: with
/// 8 fan-out partitions and many in-flight batches, several thousand keys covers
/// far more than the handful of batches that can be in flight within a 50ms
/// retry window. The bound caps memory (32 bytes/key + map overhead → well
/// under a few MB) and evicts FIFO. A retry that somehow arrived AFTER eviction
/// would re-apply (back to the original at-least-once bug for that one batch),
/// but the immediate retry timing makes that practically impossible; see the
/// note in `forward_partition_to_owner`.
const IDEM_WINDOW: usize = 8192;

/// State recorded for an idempotency key the moment a batch begins applying.
#[derive(Clone, Copy)]
enum IdemEntry {
    /// A receipt with this key is currently applying (in-flight). Carries no
    /// rowcount yet; a concurrent duplicate must wait/skip rather than re-apply.
    InFlight,
    /// The batch with this key was applied; carries the rowcount the original
    /// apply returned, so a dedup'd retry returns the IDENTICAL rowcount.
    Applied(u64),
}

/// A bounded, concurrency-safe set of recently-seen idempotency keys with their
/// apply state. FIFO eviction at [`IDEM_WINDOW`]. The single `Mutex` makes
/// check-and-record atomic, so two near-simultaneous retries of the same key
/// can't both apply: the first claims `InFlight` and applies; the second sees
/// the key present and skips.
struct IdemWindow {
    inner: Mutex<IdemWindowInner>,
}

struct IdemWindowInner {
    map: HashMap<String, IdemEntry>,
    order: VecDeque<String>,
}

/// Outcome of claiming an idempotency key for application.
enum IdemClaim {
    /// First sight of this key — the caller must apply the write, then call
    /// [`IdemWindow::record_applied`] with the rowcount.
    Apply,
    /// Already applied — skip the write and return this rowcount (matches the
    /// original apply, so the sender's retry sees the expected total).
    AlreadyApplied(u64),
    /// A concurrent receipt of the same key is mid-apply. Treat as a duplicate:
    /// skip and return this rowcount (known up front from the batch size, which
    /// equals what the in-flight apply will return — partition writes never drop
    /// rows on the receiver, so batch size IS the applied rowcount).
    InFlightDuplicate(u64),
}

impl IdemWindow {
    fn global() -> &'static IdemWindow {
        static W: OnceLock<IdemWindow> = OnceLock::new();
        W.get_or_init(|| IdemWindow {
            inner: Mutex::new(IdemWindowInner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        })
    }

    /// Atomically claim `key` for application. On the first sight of `key`
    /// records it `InFlight` and returns [`IdemClaim::Apply`]; a repeat returns
    /// `AlreadyApplied`/`InFlightDuplicate` so the caller skips the write.
    /// `rows` is the batch size, used as the dedup rowcount for the in-flight
    /// case (the receiver never drops rows, so batch size == applied rowcount).
    fn claim(&self, key: &str, rows: u64) -> IdemClaim {
        let mut g = self.inner.lock().expect("idem window poisoned");
        match g.map.get(key) {
            Some(IdemEntry::Applied(n)) => IdemClaim::AlreadyApplied(*n),
            Some(IdemEntry::InFlight) => IdemClaim::InFlightDuplicate(rows),
            None => {
                g.insert(key.to_string(), IdemEntry::InFlight);
                IdemClaim::Apply
            }
        }
    }

    /// Promote an in-flight key to `Applied(rows)` after the write committed.
    fn record_applied(&self, key: &str, rows: u64) {
        let mut g = self.inner.lock().expect("idem window poisoned");
        // Key is already present (claimed InFlight); just update its state.
        if let Some(slot) = g.map.get_mut(key) {
            *slot = IdemEntry::Applied(rows);
        }
    }

    /// Drop an in-flight key when the apply FAILED, so a later retry of the same
    /// batch is allowed to apply (the failed attempt wrote nothing to dedup
    /// against). Without this a transient owner-side write error would wedge the
    /// key as permanently-skipped and silently lose the batch.
    fn forget_inflight(&self, key: &str) {
        let mut g = self.inner.lock().expect("idem window poisoned");
        if let Some(IdemEntry::InFlight) = g.map.get(key) {
            g.map.remove(key);
            g.order.retain(|k| k != key);
        }
    }
}

impl IdemWindowInner {
    /// Insert a new key, evicting the oldest when at capacity (FIFO ring).
    fn insert(&mut self, key: String, entry: IdemEntry) {
        if self.map.len() >= IDEM_WINDOW {
            while let Some(old) = self.order.pop_front() {
                if self.map.remove(&old).is_some() {
                    break;
                }
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, entry);
    }
}

/// Constant-time secret comparison so a timing side-channel can't reveal the
/// configured secret byte-by-byte. Mirrors `internal_forward::secrets_match`.
fn secrets_match(provided: &[u8], expected: &[u8]) -> bool {
    if provided.len() != expected.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (a, b) in provided.iter().zip(expected.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Verify the `x-basin-forward-secret` header against the configured secret.
/// Rejects (401) when the header is absent, non-ASCII, or mismatched. The
/// caller guarantees `expected` is `Some` (the route is only mounted with a
/// secret), but we re-check defensively and fail closed if it is ever `None`.
/// `pub(crate)`: shared with the tail-drain receive route
/// ([`crate::routes::internal_tail_drain`]), which sits behind the identical
/// fail-closed secret gate.
pub(crate) fn check_forward_secret(
    headers: &HeaderMap,
    expected: Option<&str>,
) -> Result<(), ApiError> {
    let expected = expected.ok_or_else(|| {
        ApiError::unauthenticated("partition-write endpoint is not configured (no shared secret)")
    })?;
    let provided = headers
        .get(FORWARD_SECRET_HEADER)
        .ok_or_else(|| ApiError::unauthenticated("missing forward secret header"))?
        .to_str()
        .map_err(|_| ApiError::unauthenticated("forward secret header is not ASCII"))?;
    if !secrets_match(provided.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::unauthenticated("forward secret mismatch"));
    }
    Ok(())
}

/// Read a required ASCII header, mapping absent / non-ASCII to a 400.
/// `pub(crate)`: shared with the tail-drain receive route.
pub(crate) fn required_header<'h>(headers: &'h HeaderMap, name: &str) -> Result<&'h str, ApiError> {
    headers
        .get(name)
        .ok_or_else(|| ApiError::invalid(format!("missing {name} header")))?
        .to_str()
        .map_err(|_| ApiError::invalid(format!("{name} header is not ASCII")))
}

/// `POST /internal/v1/partition-write` — land a forwarded Arrow batch into the
/// partition this node owns. RAW write (no constraint re-check; see module
/// docs). Returns the rowcount as a bare JSON number.
pub(crate) async fn post_partition_write(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    check_forward_secret(&headers, state.forward_secret.as_deref())?;

    // Loop guard: a forwarded write carries hop=1. The receiver MUST land it
    // locally and never re-forward, so anything other than 1 is rejected
    // (prevents an A→B→A forward loop if owner resolution ever disagreed).
    let hop = required_header(&headers, PARTITION_FORWARD_HOP_HEADER)?;
    let hop: u32 = hop
        .trim()
        .parse()
        .map_err(|_| ApiError::invalid(format!("{PARTITION_FORWARD_HOP_HEADER} is not a number")))?;
    if hop != 1 {
        return Err(ApiError::invalid(format!(
            "partition-write hop must be 1 (got {hop}); the receiver never re-forwards"
        )));
    }

    let project = required_header(&headers, PARTITION_FORWARD_PROJECT_HEADER)?;
    let project = ProjectId::from_str(project)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_PROJECT_HEADER}: {e}")))?;
    let table = required_header(&headers, PARTITION_FORWARD_TABLE_HEADER)?;
    let table = TableName::new(table)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_TABLE_HEADER}: {e}")))?;
    let partition = required_header(&headers, PARTITION_FORWARD_PARTITION_HEADER)?;
    let partition = PartitionKey::new(partition)
        .map_err(|e| ApiError::invalid(format!("bad {PARTITION_FORWARD_PARTITION_HEADER}: {e}")))?;

    let batch = decode_partition_batch(&body).map_err(ApiError::from)?;
    let rows = batch.num_rows() as u64;

    // EXACTLY-ONCE: forwarding is at-least-once over the network — a lost ack
    // makes the sender re-POST the SAME batch with the SAME idempotency key.
    // Dedup on that key so an already-applied batch is NOT written twice;
    // instead return the original rowcount so the sender's retry sees success
    // and totals stay correct. Back-compat: if the header is absent (older
    // sender), fall back to apply-unconditionally — the matching sender in this
    // build always sends it.
    let idem_key = headers
        .get(PARTITION_FORWARD_IDEM_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // The shard handle. `shard.get` performs the lease CAS (ensure_lease): this
    // node, the partition's deterministic owner, holds the writer lease, so the
    // RAW write below is the single writer for the partition.
    let shard = match state.cfg.engine.shard() {
        Some(s) => s,
        None => {
            if let Some(key) = idem_key.as_deref() {
                IdemWindow::global().forget_inflight(key);
            }
            return Err(ApiError::internal(
                "partition-write received but this node has no shard configured",
            ));
        }
    };

    if let Some(key) = idem_key.as_deref() {
        match IdemWindow::global().claim(key, rows) {
            IdemClaim::AlreadyApplied(n) | IdemClaim::InFlightDuplicate(n) => {
                // Duplicate receipt — skip the write entirely, return the same
                // rowcount the original apply did (== batch size; the receiver
                // never drops rows). Atomic check-and-claim above guarantees two
                // simultaneous retries can't both reach the apply path. For the
                // #46 durability audit, report the partition's CURRENT durable
                // WAL high-water: the original apply already landed durable, so
                // the partition is durable at least through it.
                let durable_lsn = shard
                    .wal()
                    .high_water(&project, &partition)
                    .await
                    .ok()
                    .map(|l| l.0);
                return Ok((
                    StatusCode::OK,
                    Json(basin_engine::write_forwarder::PartitionWriteResponse {
                        rows: n,
                        durable_lsn,
                    }),
                )
                    .into_response());
            }
            IdemClaim::Apply => { /* fall through to apply, then record */ }
        }
    }
    // RAW partition write. Constraints already ran on the SENDER across all
    // partitions before fan-out (see module docs); do NOT re-run. On ANY
    // failure, forget the in-flight key so a legitimate retry can re-apply (the
    // failed attempt wrote nothing to dedup against).
    //
    // DURABLE-ON-FORWARD (Stage 3, the forwarded limb of the end-of-COPY
    // durable barrier): when the barrier is engaged (the default), this is an
    // ack-after-durable write — the owner group-commits the batch to its WAL
    // and only then acks this POST, so a returned forward already means the
    // rows are durable HERE on the owner. The originating node therefore needs
    // no remote barrier: by the time it emits `COPY n`, every forwarded batch
    // is durable on its owner, so a crash of THIS owner node cannot lose an
    // acked row. With the barrier disabled (`BASIN_COPY_DURABLE_BARRIER=0`) the
    // write is async (ack-before-durable), symmetric with the local async path.
    let durable = basin_engine::write_forwarder::forward_lands_durable();
    let apply = async {
        let handle = shard.get(&project, &partition).await.map_err(ApiError::from)?;
        // #46 durability audit: capture the WAL LSN the batch landed at. With
        // durable-on-forward engaged (the default), `write_batch_opts_lsn(...,
        // true)` returns ONLY after the owner's WAL group-committed through this
        // LSN — so the LSN we return is a CONFIRMED durable high-water for the
        // partition, tying every acked forwarded row to owner durability.
        handle
            .write_batch_opts_lsn(&table, batch, durable)
            .await
            .map_err(ApiError::from)
    }
    .await;
    let durable_lsn = match apply {
        Ok(lsn) => lsn,
        Err(e) => {
            if let Some(key) = idem_key.as_deref() {
                IdemWindow::global().forget_inflight(key);
            }
            return Err(e);
        }
    };

    // Commit succeeded: promote the in-flight key to Applied(rows) so a
    // lost-ack retry dedups to exactly this rowcount.
    if let Some(key) = idem_key.as_deref() {
        IdemWindow::global().record_applied(key, rows);
    }

    // #46: report rows + the owner's confirmed durable high-water LSN. When the
    // barrier is OFF (async forward) the append is ack-before-durable, so the
    // returned LSN is the appended position, not yet a durability confirmation;
    // the originator's audit treats a confirmed LSN as durable only because the
    // default (barrier ON) path is ack-after-durable. We still report the LSN so
    // the audit has a per-partition high-water either way.
    Ok((
        StatusCode::OK,
        Json(basin_engine::write_forwarder::PartitionWriteResponse {
            rows,
            durable_lsn: Some(durable_lsn.0),
        }),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn secret_headers(secret: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            FORWARD_SECRET_HEADER,
            HeaderValue::from_str(secret).unwrap(),
        );
        h
    }

    #[test]
    fn secret_match_is_constant_time_correct() {
        assert!(secrets_match(b"hunter2", b"hunter2"));
        assert!(!secrets_match(b"hunter2", b"hunter3"));
        assert!(!secrets_match(b"hunter2", b"hunter22"));
        assert!(!secrets_match(b"", b"x"));
        assert!(secrets_match(b"", b""));
    }

    #[test]
    fn missing_secret_header_rejected() {
        let headers = HeaderMap::new();
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn wrong_secret_rejected() {
        let headers = secret_headers("nope");
        let err = check_forward_secret(&headers, Some("s3cret")).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn matching_secret_accepted() {
        let headers = secret_headers("s3cret");
        assert!(check_forward_secret(&headers, Some("s3cret")).is_ok());
    }

    #[test]
    fn no_configured_secret_fails_closed() {
        let headers = secret_headers("anything");
        let err = check_forward_secret(&headers, None).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::UNAUTHORIZED);
    }

    // ── idempotency window: dedup correctness + bound + atomicity ────────────

    /// A fresh window (not the process-global one) so tests don't alias state.
    fn fresh_window() -> IdemWindow {
        IdemWindow {
            inner: Mutex::new(IdemWindowInner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    #[test]
    fn same_key_twice_applies_once_with_same_rowcount() {
        let w = fresh_window();
        // First receipt of key "k": claim → Apply, then we "write" 100 rows.
        match w.claim("k", 100) {
            IdemClaim::Apply => w.record_applied("k", 100),
            _ => panic!("first claim must be Apply, got a duplicate"),
        }
        // Second receipt of the SAME key: dedup to AlreadyApplied with the SAME
        // rowcount — the receiver must SKIP the write and return 100 again.
        match w.claim("k", 100) {
            IdemClaim::AlreadyApplied(n) => assert_eq!(n, 100, "dedup returns original rowcount"),
            _ => panic!("second claim of same key must dedup to AlreadyApplied"),
        }
    }

    #[test]
    fn different_key_same_content_applies_again() {
        let w = fresh_window();
        match w.claim("k1", 100) {
            IdemClaim::Apply => w.record_applied("k1", 100),
            _ => panic!("k1 first claim must Apply"),
        }
        // A DIFFERENT key (even with identical content/rowcount) is a distinct
        // batch → must apply again, never dedup.
        assert!(
            matches!(w.claim("k2", 100), IdemClaim::Apply),
            "a distinct idempotency key must apply (no false dedup on content)"
        );
    }

    #[test]
    fn concurrent_double_receipt_of_one_key_applies_once() {
        // The first claim takes InFlight; a SECOND concurrent claim of the same
        // key (before record_applied) must NOT also Apply — it sees the in-flight
        // entry and dedups, so two simultaneous retries can't both write.
        let w = fresh_window();
        assert!(matches!(w.claim("k", 100), IdemClaim::Apply), "first claims");
        match w.claim("k", 100) {
            IdemClaim::InFlightDuplicate(n) => assert_eq!(n, 100),
            _ => panic!("concurrent second claim must dedup as InFlightDuplicate"),
        }
        // Only after the first finishes does it become Applied.
        w.record_applied("k", 100);
        assert!(matches!(w.claim("k", 100), IdemClaim::AlreadyApplied(100)));
    }

    #[test]
    fn failed_apply_forgets_inflight_so_retry_can_apply() {
        let w = fresh_window();
        assert!(matches!(w.claim("k", 100), IdemClaim::Apply));
        // Apply failed → forget the in-flight key; a later retry must be allowed
        // to apply (the failed attempt wrote nothing to dedup against).
        w.forget_inflight("k");
        assert!(
            matches!(w.claim("k", 100), IdemClaim::Apply),
            "after a failed apply, the same key must be allowed to apply again"
        );
    }

    #[test]
    fn window_evicts_fifo_at_bound() {
        let w = fresh_window();
        // Fill past the bound; the earliest key must be evicted (and so re-apply
        // if it ever returns — accepted trade-off, see IDEM_WINDOW doc).
        for i in 0..(IDEM_WINDOW + 10) {
            let k = format!("k{i}");
            match w.claim(&k, 1) {
                IdemClaim::Apply => w.record_applied(&k, 1),
                _ => panic!("each distinct key applies"),
            }
        }
        let g = w.inner.lock().unwrap();
        assert!(g.map.len() <= IDEM_WINDOW, "window stays bounded");
        assert!(!g.map.contains_key("k0"), "oldest key evicted FIFO");
        assert!(g.map.contains_key(&format!("k{}", IDEM_WINDOW + 9)), "newest retained");
    }

    #[test]
    fn required_header_present_and_absent() {
        let mut h = HeaderMap::new();
        h.insert(PARTITION_FORWARD_TABLE_HEADER, HeaderValue::from_static("t"));
        assert_eq!(
            required_header(&h, PARTITION_FORWARD_TABLE_HEADER).unwrap(),
            "t"
        );
        let err = required_header(&h, PARTITION_FORWARD_PARTITION_HEADER).unwrap_err();
        assert_eq!(err.code.status(), StatusCode::BAD_REQUEST);
    }
}
