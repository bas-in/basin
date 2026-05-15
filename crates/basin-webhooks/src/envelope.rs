//! JSON envelope POSTed to the subscriber.
//!
//! Same shape as [`ChangeEvent`](basin_common::ChangeEvent), plus the
//! `subscription_id` so the subscriber can route by subscription. Defined
//! here as a standalone serde struct rather than via `Serialize` on
//! `ChangeEvent`: keeping the wire shape in this crate lets us evolve it
//! independently of `basin-common::events` (which is a stable Tier 0
//! contract).

use basin_common::{ChangeEvent, ChangeOp, TableName, ProjectId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::WebhookSubscriptionId;

/// String form of [`ChangeOp`]. Lower-case so Postgres-flavoured
/// downstream consumers (`'insert'` / `'update'` / `'delete'`) parse it
/// directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookOpStr {
    Insert,
    Update,
    Delete,
}

impl From<ChangeOp> for WebhookOpStr {
    fn from(op: ChangeOp) -> Self {
        match op {
            ChangeOp::Insert => Self::Insert,
            ChangeOp::Update => Self::Update,
            ChangeOp::Delete => Self::Delete,
        }
    }
}

/// JSON body POSTed to the subscriber.
///
/// Stable wire shape; downstream consumers parse against this. Field
/// order is the natural envelope-then-payload-then-metadata ordering;
/// `serde_json` preserves it on serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEnvelope {
    pub subscription_id: WebhookSubscriptionId,
    pub project: ProjectId,
    pub table: String,
    pub op: WebhookOpStr,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub committed_at: DateTime<Utc>,
    pub seq: u64,
    pub causation_user: Option<String>,
}

impl WebhookEnvelope {
    /// Build an envelope from a change-event + the subscription receiving
    /// it. `table` is rendered as a string at this boundary because
    /// [`TableName`] does not expose a stable serde representation that
    /// carries through the cross-language POST boundary.
    pub fn from_event(subscription_id: WebhookSubscriptionId, ev: &ChangeEvent) -> Self {
        Self {
            subscription_id,
            project: ev.project,
            table: ev.table.to_string(),
            op: ev.op.into(),
            before: ev.before.clone(),
            after: ev.after.clone(),
            committed_at: ev.committed_at,
            seq: ev.seq,
            causation_user: ev.causation_user.clone(),
        }
    }
}

// Defensive: keep `TableName` referenced so the compiler errors if its
// import is dropped during a future refactor that breaks `to_string()`.
#[allow(dead_code)]
fn _enforce_tablename_in_scope(_t: &TableName) {}
