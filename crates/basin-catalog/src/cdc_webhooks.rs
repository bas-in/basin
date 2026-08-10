//! Per-project CDC webhook subscriptions (ADR 0028 Phase 2).
//!
//! A CDC webhook is a registered HTTPS endpoint that receives committed change
//! events over POST with at-least-once delivery (see `basin-cdc`'s delivery
//! worker). The catalog stores two things per webhook:
//!
//! - the **subscription** ([`CdcWebhookDef`]): immutable-ish registration —
//!   destination URL, optional `(table, op)` filters, the HMAC signing secret,
//!   and the `active` flag. Written by `POST /v1/cdc/:project/webhooks`.
//! - the **delivery cursor / state** ([`CdcWebhookState`]): mutable per-endpoint
//!   progress — `last_seq` (the highest acked event), `last_status`,
//!   `retry_count`, and `disabled_at`. The delivery worker persists `last_seq`
//!   on every successful batch ack, and the auto-disable path stamps
//!   `disabled_at`. This is the crash-window contract: the durable ring is the
//!   source of truth for *what* to deliver; this cursor is the (batched,
//!   last-ack) bookmark for *how far* an endpoint has consumed. A crash between
//!   "endpoint acked batch" and "cursor persisted" re-delivers the batch on
//!   restart — at-least-once, the worker's idempotency contract.
//!
//! Like every other per-project catalog entity (reactors, inbound webhooks,
//! compaction watermarks), the trait methods carry **legacy-tolerant default
//! impls** so a catalog backend (or an old schema) that predates this table
//! behaves as "no webhooks registered" rather than erroring.

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};

/// One registered CDC webhook subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcWebhookDef {
    /// Opaque per-project webhook id (ULID string). Minted at registration.
    pub id: String,
    pub project: ProjectId,
    /// Destination URL. Events are POSTed here as a JSON array.
    pub url: String,
    /// Optional table allow-list. `None` ⇒ all tables. An empty `Vec` is
    /// treated the same as `None` (no filter) by [`Self::matches`].
    #[serde(default)]
    pub tables: Option<Vec<String>>,
    /// Optional op allow-list (`insert` / `update` / `delete`, lowercase).
    /// `None` ⇒ all ops.
    #[serde(default)]
    pub ops: Option<Vec<String>>,
    /// Hex-encoded HMAC-SHA256 secret (32 raw bytes → 64 hex chars). Minted at
    /// registration, returned to the creator exactly once. The delivery worker
    /// signs each POST body with this secret (`X-Basin-Signature` header).
    pub secret_hex: String,
    /// Whether the worker should deliver to this endpoint. Set `false` by the
    /// auto-disable path (max delivery age exceeded) or an explicit operator
    /// action; the row is retained so its cursor / last error survive.
    pub active: bool,
}

impl CdcWebhookDef {
    /// Does an event for `table` / `op` (lowercase op name) pass this
    /// subscription's filters? `O(filter_len)` — filters are tiny.
    pub fn matches(&self, table: &str, op: &str) -> bool {
        if let Some(tables) = self.tables.as_ref() {
            if !tables.is_empty() && !tables.iter().any(|t| t == table) {
                return false;
            }
        }
        if let Some(ops) = self.ops.as_ref() {
            if !ops.is_empty() && !ops.iter().any(|o| o.eq_ignore_ascii_case(op)) {
                return false;
            }
        }
        true
    }
}

/// Mutable per-endpoint delivery state (the resumable cursor + observability).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcWebhookState {
    /// Highest event `seq` this endpoint has acked (200-ranged). The delivery
    /// worker resumes from `replay_after(last_seq)`. `0` ⇒ never delivered.
    pub last_seq: u64,
    /// Last delivery outcome, for observability via the GET route. Either an
    /// HTTP status (`"200"`, `"500"`) or a transport error string.
    #[serde(default)]
    pub last_status: Option<String>,
    /// Consecutive failed attempts on the current (un-acked) batch. Reset to 0
    /// on a successful ack. Drives the exponential backoff schedule.
    #[serde(default)]
    pub retry_count: u32,
    /// RFC3339 timestamp at which the endpoint was auto-disabled (max delivery
    /// age exceeded). `None` while healthy.
    #[serde(default)]
    pub disabled_at: Option<String>,
}

/// A webhook plus its current delivery state, returned by the GET route and
/// consumed by the delivery worker. Bundling avoids a second round-trip.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcWebhookRow {
    #[serde(flatten)]
    pub def: CdcWebhookDef,
    pub state: CdcWebhookState,
}

/// Errors specific to CDC webhook registration.
#[derive(Debug)]
pub enum CdcWebhookError {
    /// `url` is not a parseable `http`/`https` URL.
    InvalidUrl(String),
    /// An op filter token is not one of `insert` / `update` / `delete`.
    InvalidOp(String),
    /// No webhook with the given `(project, id)` exists.
    NotFound,
}

/// Validate a registration request. Checks the URL scheme and that every op
/// filter token is a known op.
pub fn validate_registration(url: &str, ops: Option<&[String]>) -> Result<(), CdcWebhookError> {
    let lower = url.trim().to_ascii_lowercase();
    if !(lower.starts_with("http://") || lower.starts_with("https://")) {
        return Err(CdcWebhookError::InvalidUrl(format!(
            "url must be http(s): {url:?}"
        )));
    }
    if let Some(ops) = ops {
        for op in ops {
            match op.trim().to_ascii_lowercase().as_str() {
                "insert" | "update" | "delete" => {}
                other => return Err(CdcWebhookError::InvalidOp(other.to_string())),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(tables: Option<Vec<String>>, ops: Option<Vec<String>>) -> CdcWebhookDef {
        CdcWebhookDef {
            id: "01J".into(),
            project: ProjectId::new(),
            url: "https://example.com/hook".into(),
            tables,
            ops,
            secret_hex: "ab".repeat(32),
            active: true,
        }
    }

    #[test]
    fn no_filters_matches_everything() {
        let d = def(None, None);
        assert!(d.matches("orders", "insert"));
        assert!(d.matches("anything", "delete"));
    }

    #[test]
    fn table_filter_restricts() {
        let d = def(Some(vec!["orders".into()]), None);
        assert!(d.matches("orders", "insert"));
        assert!(!d.matches("users", "insert"));
    }

    #[test]
    fn op_filter_is_case_insensitive() {
        let d = def(None, Some(vec!["update".into(), "delete".into()]));
        assert!(!d.matches("t", "insert"));
        assert!(d.matches("t", "UPDATE"));
        assert!(d.matches("t", "delete"));
    }

    #[test]
    fn empty_filter_vec_means_no_filter() {
        let d = def(Some(vec![]), Some(vec![]));
        assert!(d.matches("anything", "insert"));
    }

    #[test]
    fn validate_rejects_non_http_url() {
        assert!(matches!(
            validate_registration("ftp://x", None),
            Err(CdcWebhookError::InvalidUrl(_))
        ));
        assert!(validate_registration("https://x.example/h", None).is_ok());
    }

    #[test]
    fn validate_rejects_bad_op() {
        assert!(matches!(
            validate_registration("https://x", Some(&["frobnicate".into()])),
            Err(CdcWebhookError::InvalidOp(_))
        ));
        assert!(
            validate_registration("https://x", Some(&["insert".into(), "UPDATE".into()])).is_ok()
        );
    }
}
