//! In-memory webhook subscription registry.
//!
//! Holds a single `RwLock<HashMap<WebhookSubscriptionId, WebhookSubscription>>`
//! shared across the whole process. Per-project cost is **O(bytes per active
//! subscription)** — one map entry, one URL string, one per-row enum, no
//! per-project pooled resource (file, worker, channel). See crate docs for
//! the per-project cost discipline this honours.

use std::collections::HashMap;
use std::sync::Arc;

use basin_common::{ProjectId, TableName};
use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Stable surrogate id for one subscription. Used as the idempotency-key
/// salt and as the dead-letter filename.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WebhookSubscriptionId(pub Uuid);

impl WebhookSubscriptionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WebhookSubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WebhookSubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

bitflags! {
    /// Mutation ops a subscription matches. Bitset so the SQL surface's
    /// `ON INSERT OR UPDATE` form maps to a single field.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct WebhookOps: u8 {
        const INSERT = 0b001;
        const UPDATE = 0b010;
        const DELETE = 0b100;
    }
}

impl WebhookOps {
    /// Convenience: every op.
    pub fn all_ops() -> Self {
        Self::INSERT | Self::UPDATE | Self::DELETE
    }
}

/// One registered subscription. Lives in [`WebhookRegistry`] for v0.1; the
/// catalog-persistence row is defined when the SQL surface ships.
#[derive(Clone, Debug)]
pub struct WebhookSubscription {
    pub id: WebhookSubscriptionId,
    pub project: ProjectId,
    pub table: TableName,
    pub url: String,
    pub ops: WebhookOps,
    /// `WHERE` predicate text. v0.1 rejects non-`None` values from
    /// [`WebhookRegistry::add`] — predicate evaluation requires the engine
    /// and lands with the SQL surface.
    pub when_predicate: Option<String>,
    /// Hard ceiling on attempt count. Default
    /// [`WebhookRegistry::DEFAULT_MAX_RETRIES`] (16).
    pub max_retries: u32,
    /// Manual pause flag. Auto-pause flips this on after sustained
    /// failures (see `basin-webhooks`'s `WebhookConfig::auto_pause_after`).
    pub paused: bool,
}

/// Errors surfaced from [`WebhookRegistry::add`] and friends.
#[derive(Debug, Error)]
pub enum SubscriptionError {
    /// `when_predicate` was set. v0.1 only supports match-all.
    #[error("WHERE predicate not supported in v0.1; subscription must be match-all")]
    PredicateNotSupported,
    /// `url` failed structural validation (no scheme, etc.).
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    /// `ops` was empty — no events would ever match.
    #[error("ops bitset is empty")]
    NoOps,
    /// `max_retries` was zero — nothing would ever be retried; deliveries
    /// would dead-letter on first failure. Almost certainly a misuse.
    #[error("max_retries must be >= 1")]
    InvalidMaxRetries,
}

/// Process-wide registry. Cheap to clone (`Arc` inside).
#[derive(Clone, Default, Debug)]
pub struct WebhookRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Default, Debug)]
struct RegistryInner {
    subs: RwLock<HashMap<WebhookSubscriptionId, WebhookSubscription>>,
}

impl WebhookRegistry {
    /// Default cap on delivery attempts.
    pub const DEFAULT_MAX_RETRIES: u32 = 16;

    pub fn new() -> Self {
        Self::default()
    }

    /// Register a subscription. Returns the assigned id (the caller-
    /// supplied `id` is preserved unless `Uuid::nil()`, in which case a
    /// fresh one is minted).
    ///
    /// Validates the subscription up-front: rejects `when_predicate.is_some()`
    /// (v0.1 limitation on this entry point — see
    /// [`Self::add_with_predicate`] for the SQL-surface entry point that
    /// accepts predicates), empty `ops`, malformed `url`, zero
    /// `max_retries`.
    pub async fn add(
        &self,
        sub: WebhookSubscription,
    ) -> Result<WebhookSubscriptionId, SubscriptionError> {
        if sub.when_predicate.is_some() {
            return Err(SubscriptionError::PredicateNotSupported);
        }
        self.insert_validated(sub).await
    }

    /// Register a subscription, accepting a non-`None` `when_predicate`.
    /// The SQL surface (`ALTER TABLE ... SUBSCRIBE WEBHOOK ... WHERE (...)`)
    /// uses this entry point; `basin-webhooks`'s `WebhookSink` evaluates the
    /// predicate against each [`basin_common::ChangeEvent`]'s `before` /
    /// `after` JSON payloads at delivery time. Predicate text is
    /// validated by the caller (sqlparser at registration time);
    /// untrusted text reaching this method may produce a sink-side
    /// `tracing::warn!` and skip delivery rather than crash.
    pub async fn add_with_predicate(
        &self,
        sub: WebhookSubscription,
    ) -> Result<WebhookSubscriptionId, SubscriptionError> {
        self.insert_validated(sub).await
    }

    /// Common validation + insertion path shared by [`Self::add`] and
    /// [`Self::add_with_predicate`]. Predicate-presence policy is
    /// applied by the caller; everything else (ops, URL, retries) is
    /// uniform.
    async fn insert_validated(
        &self,
        mut sub: WebhookSubscription,
    ) -> Result<WebhookSubscriptionId, SubscriptionError> {
        if sub.ops.is_empty() {
            return Err(SubscriptionError::NoOps);
        }
        if sub.max_retries == 0 {
            return Err(SubscriptionError::InvalidMaxRetries);
        }
        // Cheap structural URL check. The full per-project allowlist
        // gate runs in basin-net at delivery time; here we only catch
        // garbage that would never even parse.
        if url::Url::parse(&sub.url).is_err() {
            return Err(SubscriptionError::InvalidUrl(sub.url.clone()));
        }
        if sub.id.0.is_nil() {
            sub.id = WebhookSubscriptionId::new();
        }
        let id = sub.id;
        let mut g = self.inner.subs.write().await;
        g.insert(id, sub);
        Ok(id)
    }

    /// Find a subscription by `(project, table, name)` where the
    /// "name" is the user-supplied subscription label (the
    /// `<subscription_name>` from `ALTER TABLE ... SUBSCRIBE WEBHOOK
    /// <name> ...`). v0.1 stores the label inside [`WebhookSubscription`]
    /// only as part of the URL — but the SQL surface needs a stable
    /// way to unsubscribe by name. Until subscriptions carry an
    /// explicit `label` field (catalog-persistence work), the engine
    /// treats `(project, table, url)` as the unique key and asks the
    /// SQL-surface caller to plumb through the URL recorded at
    /// SUBSCRIBE time. This helper returns the first matching
    /// subscription's id.
    pub async fn find_by_url(
        &self,
        project: &ProjectId,
        table: &TableName,
        url: &str,
    ) -> Option<WebhookSubscriptionId> {
        let g = self.inner.subs.read().await;
        for (id, s) in g.iter() {
            if s.project == *project && &s.table == table && s.url == url {
                return Some(*id);
            }
        }
        None
    }

    /// Remove a subscription. Returns `true` if it was present. In-flight
    /// deliveries already in the queue are *not* drained; they will surface
    /// `Err(_: NotFound)` from [`Self::get`] when the worker picks them up
    /// and skip cleanly.
    pub async fn remove(&self, id: WebhookSubscriptionId) -> bool {
        self.inner.subs.write().await.remove(&id).is_some()
    }

    /// Look up by id. `None` if removed (or never registered).
    pub async fn get(&self, id: WebhookSubscriptionId) -> Option<WebhookSubscription> {
        self.inner.subs.read().await.get(&id).cloned()
    }

    /// All subscriptions for `project`, in id order. Used by the sink's
    /// match path on every `publish`.
    pub async fn list(&self, project: &ProjectId) -> Vec<WebhookSubscription> {
        let g = self.inner.subs.read().await;
        let mut out: Vec<WebhookSubscription> = g
            .values()
            .filter(|s| s.project == *project)
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        out
    }

    /// Manually pause a subscription. Idempotent. Returns `true` if the
    /// subscription existed.
    pub async fn pause(&self, id: WebhookSubscriptionId) -> bool {
        let mut g = self.inner.subs.write().await;
        match g.get_mut(&id) {
            Some(s) => {
                s.paused = true;
                true
            }
            None => false,
        }
    }

    /// Resume a paused subscription. The matching call to "unpause" after
    /// auto-pause has flipped a subscription off; manual paths use this
    /// too. Idempotent.
    pub async fn resume(&self, id: WebhookSubscriptionId) -> bool {
        let mut g = self.inner.subs.write().await;
        match g.get_mut(&id) {
            Some(s) => {
                s.paused = false;
                true
            }
            None => false,
        }
    }
}
