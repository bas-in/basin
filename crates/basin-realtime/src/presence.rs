//! Presence registry — Phase 5.11.R4.
//!
//! Tracks which clients are currently active on which `(project, channel)` pair.
//! Implements the Phoenix Channels presence protocol:
//!
//! - `track`   — client announces itself (or updates metadata).
//! - `untrack` — client leaves a channel.
//! - `presence_state` — server snapshot sent to a newly joining client.
//! - `presence_diff`  — server diff (`{joins, leaves}`) broadcast to the
//!                       remaining clients when the set changes.
//!
//! # Wire shapes (client to server)
//!
//! ```json
//! {"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{}}
//! {"type":"presence_untrack","channel":"room:1","client_id":"c1"}
//! {"type":"heartbeat","channel":"room:1","client_id":"c1"}
//! ```
//!
//! # Wire shapes (server to client)
//!
//! ```json
//! {"type":"presence_state","channel":"room:1","presences":[]}
//! {"type":"presence_diff","channel":"room:1","joins":[],"leaves":[]}
//! ```
//!
//! # Liveness
//!
//! Clients must send a heartbeat for each `(channel, client_id)` they own
//! within `PresenceConfig::heartbeat_ttl` (default 90 s). A background eviction
//! task spawned by [`PresenceRegistry::start_eviction_task`] sweeps stale
//! entries at `PresenceConfig::eviction_interval` (default 30 s).
//!
//! # Channel isolation
//!
//! The registry is keyed by `(ProjectId, channel_name)`. A client subscribing
//! to `room:1` never sees presence events for `room:2`.
//!
//! # Memory model
//!
//! Ephemeral only — no persistence across restarts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info};

use basin_common::ProjectId;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// A presence channel name, e.g. `"room:1"` or `"lobby"`.
pub type ChannelName = String;

/// An opaque client identifier chosen by the client.
pub type ClientId = String;

// ---------------------------------------------------------------------------
// Trust-boundary limits (6.SEC.P1)
// ---------------------------------------------------------------------------

/// Maximum serialized size (in bytes) of presence `metadata` accepted from a
/// client. Anything larger is rejected before being stored or broadcast, to
/// bound per-`track` fanout cost. 4 KiB is generous for status payloads
/// (cursor position, typing flag, display name, avatar URL).
pub const MAX_METADATA_BYTES: usize = 4 * 1024;

/// Why a presence `track`/`untrack`/`heartbeat` request was rejected.
///
/// The presence identity (`client_id`) is bound to the connection's
/// authenticated session: a client must not be able to act as another
/// `client_id`. See [`authorize_client_id`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PresenceRejection {
    /// The wire-supplied `client_id` does not match the connection's
    /// authenticated identity (impersonation attempt).
    ClientIdMismatch {
        /// Identity proven by the connection's JWT.
        authenticated: ClientId,
        /// Identity the client tried to claim.
        requested: ClientId,
    },
    /// The serialized `metadata` exceeds [`MAX_METADATA_BYTES`].
    MetadataTooLarge {
        /// Actual serialized size in bytes.
        size: usize,
        /// The enforced limit.
        limit: usize,
    },
}

impl PresenceRejection {
    /// Stable machine-readable error code for the WS `error` frame.
    pub fn code(&self) -> &'static str {
        match self {
            PresenceRejection::ClientIdMismatch { .. } => "presence_client_id_forbidden",
            PresenceRejection::MetadataTooLarge { .. } => "presence_metadata_too_large",
        }
    }

    /// Human-readable description for the WS `error` frame.
    pub fn message(&self) -> String {
        match self {
            PresenceRejection::ClientIdMismatch { authenticated, .. } => format!(
                "presence client_id must match the authenticated session identity ({authenticated})"
            ),
            PresenceRejection::MetadataTooLarge { size, limit } => {
                format!("presence metadata of {size} bytes exceeds the {limit}-byte limit")
            }
        }
    }
}

/// Bind a wire-supplied `client_id` to the connection's authenticated identity.
///
/// The presence identity is derived from the connection's auth context (the
/// JWT-proven `user_id`), never trusted from the wire. A client may either:
/// - omit / supply its own authenticated identity, or
/// - supply nothing meaningful — but it must NOT claim a different identity.
///
/// Returns the identity to use on success, or [`PresenceRejection::ClientIdMismatch`]
/// when the client tries to act as someone else.
pub fn authorize_client_id<'a>(
    authenticated: &'a str,
    requested: &str,
) -> Result<&'a str, PresenceRejection> {
    if requested.is_empty() || requested == authenticated {
        Ok(authenticated)
    } else {
        Err(PresenceRejection::ClientIdMismatch {
            authenticated: authenticated.to_owned(),
            requested: requested.to_owned(),
        })
    }
}

/// Reject presence `metadata` whose serialized form exceeds
/// [`MAX_METADATA_BYTES`]. Returns `Ok(())` for in-bounds payloads.
pub fn validate_metadata_size(metadata: &serde_json::Value) -> Result<(), PresenceRejection> {
    // Use the compact serialized length as the canonical size measure; this is
    // what would actually be broadcast to subscribers.
    let size = serde_json::to_vec(metadata).map(|v| v.len()).unwrap_or(0);
    if size > MAX_METADATA_BYTES {
        Err(PresenceRejection::MetadataTooLarge {
            size,
            limit: MAX_METADATA_BYTES,
        })
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tunable knobs for the presence system. Pass `PresenceConfig::default()` in
/// production; use shorter durations in tests to avoid waiting 90 seconds.
#[derive(Clone, Debug)]
pub struct PresenceConfig {
    /// How long a client can go without sending a heartbeat before being evicted.
    /// Default: 90 s.
    pub heartbeat_ttl: Duration,
    /// How often the background eviction sweep runs.
    /// Default: 30 s.
    pub eviction_interval: Duration,
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            heartbeat_ttl: Duration::from_secs(90),
            eviction_interval: Duration::from_secs(30),
        }
    }
}

impl PresenceConfig {
    /// Shortened timeouts for integration tests. Clients are evicted after
    /// `heartbeat_ttl` and the sweep runs every `eviction_interval`.
    pub fn for_test(heartbeat_ttl: Duration, eviction_interval: Duration) -> Self {
        Self {
            heartbeat_ttl,
            eviction_interval,
        }
    }
}

// ---------------------------------------------------------------------------
// Presence record
// ---------------------------------------------------------------------------

/// Metadata stored per `(channel, client_id)`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PresenceMeta {
    /// Arbitrary JSON payload supplied by the client.
    #[serde(default)]
    pub metadata: serde_json::Value,
    /// Monotonic timestamp of the last heartbeat (not serialized — server-only).
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,
}

impl PresenceMeta {
    pub fn new(metadata: serde_json::Value) -> Self {
        Self {
            metadata,
            last_heartbeat: Instant::now(),
        }
    }

    pub fn touch(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// Presence events (published on per-channel broadcast channel)
// ---------------------------------------------------------------------------

/// A presence change event delivered to all WS connections subscribed to the
/// same `(project, channel)`.
#[derive(Clone, Debug)]
pub enum PresenceEvent {
    /// Full snapshot: sent once to the newly joining client.
    State {
        channel: ChannelName,
        presences: Vec<PresenceEntry>,
    },
    /// Incremental diff: broadcast to all other clients when the set changes.
    Diff {
        channel: ChannelName,
        joins: Vec<PresenceEntry>,
        leaves: Vec<PresenceEntry>,
    },
}

/// A single presence entry in a `presence_state` or `presence_diff` message.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PresenceEntry {
    pub client_id: ClientId,
    pub metadata: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Registry internals
// ---------------------------------------------------------------------------

/// Inner per-channel state: client map + broadcast channel.
struct ChannelPresence {
    /// `client_id` to metadata (with heartbeat timestamp).
    clients: DashMap<ClientId, PresenceMeta>,
    /// Broadcast sender; every WS connection on this channel has a receiver.
    tx: broadcast::Sender<PresenceEvent>,
}

impl ChannelPresence {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            clients: DashMap::new(),
            tx,
        }
    }

    fn snapshot(&self) -> Vec<PresenceEntry> {
        self.clients
            .iter()
            .map(|kv| PresenceEntry {
                client_id: kv.key().clone(),
                metadata: kv.value().metadata.clone(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// PresenceRegistry
// ---------------------------------------------------------------------------

/// Shared presence registry. Cheap to clone — all state is `Arc`-backed.
#[derive(Clone)]
pub struct PresenceRegistry {
    /// `(ProjectId, ChannelName)` to channel state.
    channels: Arc<DashMap<(ProjectId, ChannelName), Arc<ChannelPresence>>>,
    config: PresenceConfig,
}

impl PresenceRegistry {
    /// Create a new registry with the given configuration.
    pub fn new(config: PresenceConfig) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            config,
        }
    }

    fn channel_entry(&self, project: ProjectId, channel: &str) -> Arc<ChannelPresence> {
        self.channels
            .entry((project, channel.to_owned()))
            .or_insert_with(|| Arc::new(ChannelPresence::new()))
            .clone()
    }

    /// Subscribe to presence events for `(project, channel)`.
    ///
    /// The caller receives a broadcast receiver. The first message sent to
    /// the client should be a `presence_state` snapshot, obtained via
    /// [`PresenceRegistry::snapshot`] after calling `subscribe` to avoid
    /// a race between the snapshot and future diffs.
    pub fn subscribe(
        &self,
        project: ProjectId,
        channel: &str,
    ) -> broadcast::Receiver<PresenceEvent> {
        self.channel_entry(project, channel).tx.subscribe()
    }

    /// Return the current presence snapshot for `(project, channel)`.
    pub fn snapshot(&self, project: ProjectId, channel: &str) -> Vec<PresenceEntry> {
        match self.channels.get(&(project, channel.to_owned())) {
            Some(ch) => ch.snapshot(),
            None => vec![],
        }
    }

    /// Register (or refresh) a client in `(project, channel)`.
    ///
    /// - If the client is **new**: inserts and publishes
    ///   `presence_diff { joins: [client], leaves: [] }`.
    /// - If the client **already exists**: updates metadata + heartbeat;
    ///   publishes diff only when metadata changed.
    pub fn track(
        &self,
        project: ProjectId,
        channel: &str,
        client_id: &str,
        metadata: serde_json::Value,
    ) {
        let ch = self.channel_entry(project, channel);
        let is_new = !ch.clients.contains_key(client_id);
        let entry = PresenceEntry {
            client_id: client_id.to_owned(),
            metadata: metadata.clone(),
        };

        if is_new {
            ch.clients
                .insert(client_id.to_owned(), PresenceMeta::new(metadata));
            debug!(project = %project, channel = %channel, client = %client_id, "presence: tracked");
            let _ = ch.tx.send(PresenceEvent::Diff {
                channel: channel.to_owned(),
                joins: vec![entry],
                leaves: vec![],
            });
        } else if let Some(mut meta) = ch.clients.get_mut(client_id) {
            let changed = meta.metadata != metadata;
            meta.metadata = metadata;
            meta.touch();
            if changed {
                let _ = ch.tx.send(PresenceEvent::Diff {
                    channel: channel.to_owned(),
                    joins: vec![entry],
                    leaves: vec![],
                });
            }
        }
    }

    /// Remove a client from `(project, channel)`.
    ///
    /// Publishes `presence_diff { joins: [], leaves: [client] }` when present.
    /// No-op if the client was not tracked.
    pub fn untrack(&self, project: ProjectId, channel: &str, client_id: &str) {
        let key = (project, channel.to_owned());
        if let Some(ch) = self.channels.get(&key) {
            if let Some((_, meta)) = ch.clients.remove(client_id) {
                debug!(project = %project, channel = %channel, client = %client_id, "presence: untracked");
                let entry = PresenceEntry {
                    client_id: client_id.to_owned(),
                    metadata: meta.metadata,
                };
                let _ = ch.tx.send(PresenceEvent::Diff {
                    channel: channel.to_owned(),
                    joins: vec![],
                    leaves: vec![entry],
                });
            }
        }
    }

    /// Refresh the heartbeat timestamp for `(project, channel, client_id)`.
    ///
    /// Called when the client sends a `heartbeat` message. No-op if the client
    /// is not tracked.
    pub fn heartbeat(&self, project: ProjectId, channel: &str, client_id: &str) {
        let key = (project, channel.to_owned());
        if let Some(ch) = self.channels.get(&key) {
            if let Some(mut meta) = ch.clients.get_mut(client_id) {
                meta.touch();
            }
        }
    }

    /// Untrack a client from every channel it is registered on within `project`.
    ///
    /// Call on WS disconnect.
    pub fn untrack_all(&self, project: ProjectId, client_id: &str) {
        let channel_names: Vec<ChannelName> = self
            .channels
            .iter()
            .filter(|e| e.key().0 == project && e.value().clients.contains_key(client_id))
            .map(|e| e.key().1.clone())
            .collect();
        for ch in channel_names {
            self.untrack(project, &ch, client_id);
        }
    }

    /// Return the number of distinct clients tracked across all channels for
    /// `project`.
    #[allow(dead_code)]
    pub fn client_count(&self, project: ProjectId) -> usize {
        self.channels
            .iter()
            .filter(|e| e.key().0 == project)
            .map(|e| e.value().clients.len())
            .sum()
    }

    /// Sweep all channels, evicting clients whose last heartbeat is older than
    /// `heartbeat_ttl`. Returns `channel_name -> evicted_client_ids` for
    /// observability / logging.
    pub fn evict_stale(&self) -> HashMap<ChannelName, Vec<ClientId>> {
        let ttl = self.config.heartbeat_ttl;
        let mut evicted: HashMap<ChannelName, Vec<ClientId>> = HashMap::new();

        for entry in self.channels.iter() {
            let channel_name = &entry.key().1;
            let ch = entry.value();

            let stale: Vec<ClientId> = ch
                .clients
                .iter()
                .filter(|kv| kv.value().last_heartbeat.elapsed() > ttl)
                .map(|kv| kv.key().clone())
                .collect();

            for cid in &stale {
                if let Some((_, meta)) = ch.clients.remove(cid) {
                    info!(
                        channel = %channel_name,
                        client = %cid,
                        "presence: evicted stale client"
                    );
                    let ev_entry = PresenceEntry {
                        client_id: cid.clone(),
                        metadata: meta.metadata,
                    };
                    let _ = ch.tx.send(PresenceEvent::Diff {
                        channel: channel_name.clone(),
                        joins: vec![],
                        leaves: vec![ev_entry],
                    });
                    evicted
                        .entry(channel_name.clone())
                        .or_default()
                        .push(cid.clone());
                }
            }
        }

        evicted
    }

    /// Spawn a background Tokio task that calls [`evict_stale`] at
    /// `config.eviction_interval`. Drop the returned handle to stop the task.
    pub fn start_eviction_task(&self) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        let interval = self.config.eviction_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                registry.evict_stale();
            }
        })
    }
}

impl Default for PresenceRegistry {
    fn default() -> Self {
        Self::new(PresenceConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Wire-format helpers
// ---------------------------------------------------------------------------

/// Serialize a `presence_state` JSON frame for sending over WS.
pub fn serialize_presence_state(channel: &str, presences: &[PresenceEntry]) -> String {
    let v = serde_json::json!({
        "type": "presence_state",
        "channel": channel,
        "presences": presences,
    });
    serde_json::to_string(&v)
        .unwrap_or_else(|_| r#"{"type":"error","code":"serialize_error"}"#.to_owned())
}

/// Serialize a `presence_diff` JSON frame for sending over WS.
pub fn serialize_presence_diff(
    channel: &str,
    joins: &[PresenceEntry],
    leaves: &[PresenceEntry],
) -> String {
    let v = serde_json::json!({
        "type": "presence_diff",
        "channel": channel,
        "joins": joins,
        "leaves": leaves,
    });
    serde_json::to_string(&v)
        .unwrap_or_else(|_| r#"{"type":"error","code":"serialize_error"}"#.to_owned())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    fn project() -> ProjectId {
        ProjectId::new()
    }

    // --- track / untrack ---------------------------------------------------

    #[test]
    fn track_adds_client_to_snapshot() {
        let reg = PresenceRegistry::default();
        let p = project();
        reg.track(p, "room:1", "c1", serde_json::json!({"name": "Alice"}));
        let snap = reg.snapshot(p, "room:1");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].client_id, "c1");
        assert_eq!(snap[0].metadata["name"], "Alice");
    }

    #[test]
    fn untrack_removes_client_from_snapshot() {
        let reg = PresenceRegistry::default();
        let p = project();
        reg.track(p, "room:1", "c1", serde_json::json!({}));
        reg.untrack(p, "room:1", "c1");
        assert!(reg.snapshot(p, "room:1").is_empty());
    }

    #[test]
    fn untrack_noop_when_not_tracked() {
        let reg = PresenceRegistry::default();
        let p = project();
        // Must not panic.
        reg.untrack(p, "room:1", "not-there");
    }

    #[test]
    fn channel_isolation_room1_vs_room2() {
        let reg = PresenceRegistry::default();
        let p = project();
        reg.track(p, "room:1", "c1", serde_json::json!({}));
        reg.track(p, "room:2", "c2", serde_json::json!({}));

        let snap1 = reg.snapshot(p, "room:1");
        let snap2 = reg.snapshot(p, "room:2");

        assert_eq!(snap1.len(), 1);
        assert_eq!(snap1[0].client_id, "c1");
        assert_eq!(snap2.len(), 1);
        assert_eq!(snap2[0].client_id, "c2");

        assert!(snap1.iter().all(|e| e.client_id != "c2"));
        assert!(snap2.iter().all(|e| e.client_id != "c1"));
    }

    #[test]
    fn untrack_all_clears_all_channels() {
        let reg = PresenceRegistry::default();
        let p = project();
        reg.track(p, "room:1", "c1", serde_json::json!({}));
        reg.track(p, "room:2", "c1", serde_json::json!({}));
        reg.untrack_all(p, "c1");
        assert!(reg.snapshot(p, "room:1").is_empty());
        assert!(reg.snapshot(p, "room:2").is_empty());
    }

    // --- broadcast events --------------------------------------------------

    #[tokio::test]
    async fn track_publishes_diff_joins() {
        let reg = PresenceRegistry::default();
        let p = project();
        let mut rx = reg.subscribe(p, "room:1");

        reg.track(p, "room:1", "c1", serde_json::json!({"x": 1}));

        let ev = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("channel open");

        match ev {
            PresenceEvent::Diff { joins, leaves, .. } => {
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].client_id, "c1");
                assert!(leaves.is_empty());
            }
            other => panic!("expected Diff, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn untrack_publishes_diff_leaves() {
        let reg = PresenceRegistry::default();
        let p = project();
        reg.track(p, "room:1", "c1", serde_json::json!({}));

        let mut rx = reg.subscribe(p, "room:1");
        reg.untrack(p, "room:1", "c1");

        let ev = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout")
            .expect("channel open");

        match ev {
            PresenceEvent::Diff { joins, leaves, .. } => {
                assert!(joins.is_empty());
                assert_eq!(leaves.len(), 1);
                assert_eq!(leaves[0].client_id, "c1");
            }
            other => panic!("expected Diff, got {other:?}"),
        }
    }

    // --- heartbeat / eviction ---------------------------------------------

    #[tokio::test]
    async fn evict_stale_removes_idle_clients() {
        let cfg = PresenceConfig::for_test(Duration::from_millis(50), Duration::from_millis(200));
        let reg = PresenceRegistry::new(cfg);
        let p = project();
        let mut rx = reg.subscribe(p, "room:1");

        reg.track(p, "room:1", "idle", serde_json::json!({}));

        // Wait longer than the TTL.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let evicted = reg.evict_stale();
        assert!(evicted.contains_key("room:1"));
        assert!(evicted["room:1"].contains(&"idle".to_owned()));

        // Consume events until we see the eviction diff (first event is the
        // join diff from track; second is the eviction diff from evict_stale).
        let mut saw_eviction = false;
        for _ in 0..3 {
            let ev = match timeout(Duration::from_secs(2), rx.recv()).await {
                Ok(Ok(e)) => e,
                _ => break,
            };
            if let PresenceEvent::Diff { leaves, .. } = ev {
                if leaves.iter().any(|e| e.client_id == "idle") {
                    saw_eviction = true;
                    break;
                }
            }
        }
        assert!(
            saw_eviction,
            "should have received eviction diff with 'idle' in leaves"
        );
    }

    #[tokio::test]
    async fn heartbeat_prevents_eviction() {
        let cfg = PresenceConfig::for_test(Duration::from_millis(80), Duration::from_millis(200));
        let reg = PresenceRegistry::new(cfg);
        let p = project();
        reg.track(p, "room:1", "active", serde_json::json!({}));

        // Heartbeat every 30 ms for 120 ms total.
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            reg.heartbeat(p, "room:1", "active");
        }

        let evicted = reg.evict_stale();
        let room_evicted = evicted.get("room:1").map(|v| v.as_slice()).unwrap_or(&[]);
        assert!(
            !room_evicted.contains(&"active".to_owned()),
            "active client should not be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_task_runs_automatically() {
        let cfg = PresenceConfig::for_test(Duration::from_millis(50), Duration::from_millis(60));
        let reg = PresenceRegistry::new(cfg);
        let p = project();
        let mut rx = reg.subscribe(p, "room:1");

        reg.track(p, "room:1", "stale", serde_json::json!({}));

        let _handle = reg.start_eviction_task();

        // Consume events until we see the eviction diff (first event is the
        // join diff; subsequent events include the eviction once TTL+sweep fires).
        // Budget: TTL 50 ms + sweep 60 ms + margin 300 ms = 410 ms.
        let deadline = std::time::Instant::now() + Duration::from_millis(600);
        let mut saw_eviction = false;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let ev = match timeout(remaining, rx.recv()).await {
                Ok(Ok(e)) => e,
                _ => break,
            };
            if let PresenceEvent::Diff { leaves, .. } = ev {
                if leaves.iter().any(|e| e.client_id == "stale") {
                    saw_eviction = true;
                    break;
                }
            }
        }
        assert!(
            saw_eviction,
            "background eviction task should have evicted 'stale' within deadline"
        );
    }

    // --- wire format -------------------------------------------------------

    #[test]
    fn serialize_state_valid_json() {
        let entries = vec![PresenceEntry {
            client_id: "c1".to_owned(),
            metadata: serde_json::json!({"role": "admin"}),
        }];
        let json = serialize_presence_state("room:1", &entries);
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(v["type"], "presence_state");
        assert_eq!(v["channel"], "room:1");
        assert_eq!(v["presences"][0]["client_id"], "c1");
    }

    // --- 6.SEC.P1: client_id binding + metadata cap -----------------------

    #[test]
    fn forged_client_id_is_rejected() {
        // Alice's connection (authenticated as "alice") tries to act as "bob".
        let err = authorize_client_id("alice", "bob").unwrap_err();
        assert_eq!(
            err,
            PresenceRejection::ClientIdMismatch {
                authenticated: "alice".to_owned(),
                requested: "bob".to_owned(),
            }
        );
        assert_eq!(err.code(), "presence_client_id_forbidden");
    }

    #[test]
    fn self_track_is_authorized() {
        // Supplying your own identity is fine and returns it unchanged.
        assert_eq!(authorize_client_id("alice", "alice").unwrap(), "alice");
    }

    #[test]
    fn empty_client_id_normalizes_to_authenticated_identity() {
        // An omitted/empty wire client_id falls back to the authenticated id,
        // never to an attacker-chosen value.
        assert_eq!(authorize_client_id("alice", "").unwrap(), "alice");
    }

    #[test]
    fn small_metadata_is_accepted() {
        let meta = serde_json::json!({"name": "Alice", "typing": true});
        assert!(validate_metadata_size(&meta).is_ok());
    }

    #[test]
    fn oversized_metadata_is_rejected() {
        // ~8 KiB string payload — comfortably over the 4 KiB cap.
        let big = "x".repeat(8 * 1024);
        let meta = serde_json::json!({ "blob": big });
        let err = validate_metadata_size(&meta).unwrap_err();
        match err {
            PresenceRejection::MetadataTooLarge { size, limit } => {
                assert!(size > MAX_METADATA_BYTES);
                assert_eq!(limit, MAX_METADATA_BYTES);
            }
            other => panic!("expected MetadataTooLarge, got {other:?}"),
        }
        assert_eq!(
            validate_metadata_size(&meta).unwrap_err().code(),
            "presence_metadata_too_large"
        );
    }

    #[test]
    fn metadata_exactly_at_limit_is_accepted() {
        // Build a payload whose serialized form is exactly at the boundary.
        // {"blob":"<N x>"} — overhead is 11 bytes for the wrapper.
        let overhead = r#"{"blob":""}"#.len();
        let fill = MAX_METADATA_BYTES - overhead;
        let meta = serde_json::json!({ "blob": "x".repeat(fill) });
        assert_eq!(serde_json::to_vec(&meta).unwrap().len(), MAX_METADATA_BYTES);
        assert!(validate_metadata_size(&meta).is_ok());
    }

    #[test]
    fn serialize_diff_valid_json() {
        let joins = vec![PresenceEntry {
            client_id: "c2".to_owned(),
            metadata: serde_json::json!({}),
        }];
        let leaves = vec![PresenceEntry {
            client_id: "c1".to_owned(),
            metadata: serde_json::json!({}),
        }];
        let json = serialize_presence_diff("room:1", &joins, &leaves);
        let v: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(v["type"], "presence_diff");
        assert_eq!(v["joins"][0]["client_id"], "c2");
        assert_eq!(v["leaves"][0]["client_id"], "c1");
    }
}
