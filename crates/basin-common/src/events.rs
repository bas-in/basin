//! Change-event primitive — the forward-compat substrate for triggers,
//! reactors, webhooks, and (future) realtime subscriptions.
//!
//! See `docs/decisions/0012-change-event-primitive.md` for the design
//! rationale. The summary is: every committed mutation can emit a
//! [`ChangeEvent`]; pluggable [`ChangeEventSink`]s consume them.
//! Pre-commit sinks abort on `Err`; post-commit sinks are fire-and-forget.
//!
//! Tier 0 (this file) ships only the trait + registry. No sinks are
//! attached by default; with an empty registry the engine path is
//! byte-identical to the no-sink baseline.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::Result;
use crate::ids::{TableName, ProjectId};

/// What kind of mutation produced this event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One committed-row change. `before` is `None` for INSERT; `after` is
/// `None` for DELETE; UPDATE carries both.
///
/// `seq` is a monotonic counter scoped per-project. It increments by 1
/// for each emitted event and is the caller's ordering signal — sinks
/// that fan out concurrently can use it to reconstruct the commit order
/// for a project.
#[derive(Clone, Debug)]
pub struct ChangeEvent {
    pub project: ProjectId,
    pub table: TableName,
    pub op: ChangeOp,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub committed_at: DateTime<Utc>,
    pub seq: u64,
    pub causation_user: Option<String>,
}

/// Sink that consumes [`ChangeEvent`]s. Implementations must be cheap
/// when their internal "is this event relevant?" filter rejects — the
/// engine calls `publish` for every committed row.
#[async_trait]
pub trait ChangeEventSink: Send + Sync {
    async fn publish(&self, event: &ChangeEvent) -> Result<()>;
}

/// Per-engine registry of attached sinks. Pre-commit sinks run
/// synchronously in the mutation path; an `Err` from any of them
/// aborts the mutation. Post-commit sinks run after the catalog
/// commit succeeds and their errors do NOT roll back.
#[derive(Default)]
pub struct EventSinkRegistry {
    pre_commit: Vec<Arc<dyn ChangeEventSink>>,
    post_commit: Vec<Arc<dyn ChangeEventSink>>,
}

impl EventSinkRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_pre_commit(&mut self, sink: Arc<dyn ChangeEventSink>) {
        self.pre_commit.push(sink);
    }

    pub fn register_post_commit(&mut self, sink: Arc<dyn ChangeEventSink>) {
        self.post_commit.push(sink);
    }

    pub fn pre_commit_is_empty(&self) -> bool {
        self.pre_commit.is_empty()
    }

    pub fn post_commit_is_empty(&self) -> bool {
        self.post_commit.is_empty()
    }

    pub fn pre_commit(&self) -> &[Arc<dyn ChangeEventSink>] {
        &self.pre_commit
    }

    pub fn post_commit(&self) -> &[Arc<dyn ChangeEventSink>] {
        &self.post_commit
    }
}
