//! Concrete cross-region [`WriteForwarder`] impls (multi-region v0.2, ADR 0009).
//!
//! The OSS engine ships the [`crate::region::WriteForwarder`] *trait* plus the
//! fail-loud `WrongRegion` default and NO network code. This module is the
//! cloud-private layer that is compiled INTO the engine binary (it cannot live
//! in the basin-cloud control-plane process, which does not depend on
//! `basin-engine` and is off the data path — see `basin-cloud/HANDOFF.md` and
//! `docs/scaling-multi-region.md`). It is registered once at engine startup via
//! [`crate::Engine::attach_write_forwarder`], gated on `BASIN_WRITE_FORWARD_MODE`.
//!
//! # The two forwarding strategies
//!
//! A non-home auto-commit write reaches a region that is NOT the project's
//! `home_region`. Instead of raising `WrongRegion`, [`RegionWriteForwarder`]
//! decides a route based on [`ForwardMode`]:
//!
//! * **`fly-replay`** ([`ForwardMode::FlyReplay`]) — the forwarder does NOT
//!   itself perform any network I/O. It returns the typed control signal
//!   [`BasinError::ForwardReplay`] carrying the target `home_region` plus an
//!   opaque replay *token* (a loop-guard / correlation id). The engine's
//!   pgwire/REST **edge** intercepts that signal and emits a Fly
//!   `fly-replay: region=<home_region>;state=<token>` response header (via
//!   `basin_router::fly_replay_directive`), so Fly re-fires the SAME request
//!   against the home region. One extra RTT for a write; zero for reads (reads
//!   never hit the write gate). This is the cheapest path on Fly and is the
//!   default when forwarding is enabled.
//!
//! * **`http-forward`** ([`ForwardMode::HttpForward`]) — used when fly-replay
//!   can't carry the request (no HTTP response to attach a header to — e.g. a
//!   mid-session pgwire statement). The forwarder resolves the home region to a
//!   peer base URL from [`PeerRegistry`] (env `BASIN_REGION_PEERS`, a
//!   raft-peers-style list `region@host:port,...`), then forwards the verbatim
//!   statement to that peer's engine over Fly 6PN HTTP through a
//!   [`ForwardClient`] and returns the home region's [`ExecResult`] verbatim.
//!
//! # What is unit-tested here vs deployment-bound
//!
//! The **routing decision** ([`RegionWriteForwarder::route`]), **peer
//! resolution** ([`PeerRegistry`]), **mode parsing** ([`ForwardMode::from_env`]
//! / [`ForwardMode::parse`]), and the **fly-replay-vs-http-forward-vs-reject
//! branch** are pure and fully unit-tested. The *live cross-region round trip*
//! (an actual `fly-replay` honoured by Fly, or a real 6PN HTTP forward landing
//! in a second region's engine) needs ≥2 deployed regions and is
//! deployment-validated, exactly like the multi-machine Raft path: the engine
//! cannot fabricate a second region on one box without faking it. The
//! [`ForwardClient`] seam is the boundary — the routing decision that selects
//! the client and target is tested; the wire round trip is not.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{BasinError, Result};

use crate::region::{ForwardContext, WriteForwarder};
use crate::ExecResult;

/// Environment variable selecting the forwarding strategy. Default (unset /
/// empty / `off`) keeps the fail-loud `WrongRegion` behaviour byte-for-byte.
pub const FORWARD_MODE_ENV: &str = "BASIN_WRITE_FORWARD_MODE";

/// Environment variable holding the region→peer map for `http-forward` mode.
/// Format mirrors the Raft peers list: `region@host:port,region@host:port`,
/// e.g. `fra@basin-fra.internal:5432,sin@basin-sin.internal:5432`.
pub const REGION_PEERS_ENV: &str = "BASIN_REGION_PEERS";

// ─── forwarding mode ───────────────────────────────────────────────────────

/// Which forwarding strategy a [`RegionWriteForwarder`] applies to a non-home
/// write. Parsed from [`FORWARD_MODE_ENV`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardMode {
    /// No forwarding: a non-home write fails loud with `WrongRegion`. This is
    /// the default and is byte-identical to a deployment with no forwarder
    /// registered at all — the engine never installs a forwarder in this mode.
    Off,
    /// Emit a Fly `fly-replay` directive at the edge (no engine network I/O).
    FlyReplay,
    /// Forward the statement to the home region's engine over 6PN HTTP.
    HttpForward,
}

impl ForwardMode {
    /// Parse a raw mode string (case-insensitive, trimmed). Unknown / empty
    /// values resolve to [`ForwardMode::Off`] — fail-safe: a typo never silently
    /// enables cross-region forwarding.
    pub fn parse(raw: &str) -> ForwardMode {
        match raw.trim().to_ascii_lowercase().as_str() {
            "fly-replay" | "fly_replay" | "flyreplay" => ForwardMode::FlyReplay,
            "http-forward" | "http_forward" | "http" | "6pn" => ForwardMode::HttpForward,
            // "off", "", and anything unrecognised → fail-safe Off.
            _ => ForwardMode::Off,
        }
    }

    /// Read [`FORWARD_MODE_ENV`] from the process environment.
    pub fn from_env() -> ForwardMode {
        match std::env::var(FORWARD_MODE_ENV) {
            Ok(v) => ForwardMode::parse(&v),
            Err(_) => ForwardMode::Off,
        }
    }
}

// ─── peer registry ─────────────────────────────────────────────────────────

/// Maps a region code to the base URL (`host:port`) of that region's engine,
/// for `http-forward` mode. Parsed from [`REGION_PEERS_ENV`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerRegistry {
    peers: HashMap<String, String>,
}

impl PeerRegistry {
    /// Build from a raw `region@host:port,region@host:port` string. Empty
    /// entries are skipped; a malformed entry (missing `@`, empty region, or
    /// empty host) returns an error naming the offending fragment so a
    /// misconfiguration fails loud at startup rather than at first forward.
    pub fn parse(raw: &str) -> Result<PeerRegistry> {
        let mut peers = HashMap::new();
        for frag in raw.split(',') {
            let frag = frag.trim();
            if frag.is_empty() {
                continue;
            }
            let (region, host) = frag.split_once('@').ok_or_else(|| {
                BasinError::Internal(format!(
                    "{REGION_PEERS_ENV} entry {frag:?} is not in region@host:port form"
                ))
            })?;
            let region = region.trim();
            let host = host.trim();
            if region.is_empty() || host.is_empty() {
                return Err(BasinError::Internal(format!(
                    "{REGION_PEERS_ENV} entry {frag:?} has an empty region or host"
                )));
            }
            peers.insert(region.to_string(), host.to_string());
        }
        Ok(PeerRegistry { peers })
    }

    /// Read [`REGION_PEERS_ENV`]; unset / empty yields an empty registry.
    pub fn from_env() -> Result<PeerRegistry> {
        match std::env::var(REGION_PEERS_ENV) {
            Ok(v) => PeerRegistry::parse(&v),
            Err(_) => Ok(PeerRegistry::default()),
        }
    }

    /// The base URL (`host:port`) for `region`, if known.
    pub fn base_url(&self, region: &str) -> Option<&str> {
        self.peers.get(region).map(String::as_str)
    }

    /// Number of registered peers (mostly for tests / startup logging).
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// True when no peers are registered.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

// ─── routing decision (pure, unit-tested) ──────────────────────────────────

/// The decision the forwarder reaches for one non-home write, before any I/O.
/// Pure and fully unit-tested; the actual fly-replay header emission and HTTP
/// round trip happen *after* this and are deployment-validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForwardRoute {
    /// `fly-replay` mode: surface the typed control signal; the edge emits the
    /// `fly-replay: region=<home_region>` header.
    Replay { home_region: String, token: String },
    /// `http-forward` mode: forward the statement to this peer base URL
    /// (`host:port`) over 6PN HTTP.
    HttpPeer { home_region: String, base_url: String },
}

// ─── HTTP forward client seam ──────────────────────────────────────────────

/// The boundary between the (tested) routing decision and the (deployment-
/// validated) wire round trip for `http-forward` mode. A real impl POSTs the
/// statement to the home-region engine's internal SQL endpoint over Fly 6PN
/// and decodes the home region's [`ExecResult`]; tests inject a fake.
#[async_trait]
pub trait ForwardClient: Send + Sync + 'static {
    /// Forward one auto-commit statement to `base_url` (the home region's
    /// engine), executing as `current_user` against `project`, and return the
    /// home region's result verbatim.
    async fn forward(
        &self,
        base_url: &str,
        project: basin_common::ProjectId,
        current_user: &str,
        sql: &str,
    ) -> Result<ExecResult>;
}

/// The default `http-forward` transport: a placeholder that fails loud.
///
/// The wire protocol for the internal 6PN forward endpoint is not yet defined
/// in this repo (it needs a matching server-side route on the home region's
/// engine — the edge-integration follow-up). Until that route lands, this
/// client returns a typed error rather than silently dropping the write, so an
/// operator who sets `http-forward` without wiring the endpoint sees a loud
/// failure rather than data loss. The routing decision that *selects* this
/// client is fully tested; only the byte-level transport is pending.
#[derive(Clone, Debug, Default)]
pub struct UnconfiguredForwardClient;

#[async_trait]
impl ForwardClient for UnconfiguredForwardClient {
    async fn forward(
        &self,
        base_url: &str,
        _project: basin_common::ProjectId,
        _current_user: &str,
        _sql: &str,
    ) -> Result<ExecResult> {
        Err(BasinError::wal(format!(
            "http-forward transport to peer {base_url:?} is not yet wired \
             (the home-region internal SQL endpoint is the edge-integration \
             follow-up); set BASIN_WRITE_FORWARD_MODE=fly-replay on Fly"
        )))
    }
}

// ─── region-aware connection-ceiling accounting model ──────────────────────

/// Where a forwarded write is counted against the per-project
/// `max_connections` ceiling (issue #28 follow-up).
///
/// The cloud pushes ONE ceiling per project; the authoritative live-connection
/// count for a project lives in **its home region's** `ConnectionLimiter`
/// (that is the only region that ever accepts a write for the project). The
/// model below guarantees a forwarded write is counted exactly where it lands —
/// at home — and never bypasses the cap, regardless of mode:
///
/// * **`fly-replay`** — the forwarder emits NO connection of its own. Fly
///   re-fires the entire client request against the home region, which opens a
///   real pgwire/REST connection THERE and is admitted (or refused with
///   `53300`) by the home region's `ConnectionLimiter` like any direct client.
///   The originating (non-home) region only ever served the pre-replay edge
///   handshake, which is released the instant the `fly-replay` header is sent
///   (the local write never executes). So the write is counted once, at home.
///
/// * **`http-forward`** — the [`ForwardClient`] opens an HTTP connection to the
///   home region's engine. That connection MUST traverse the home region's
///   connection-admission path (the same `ConnectionLimiter::try_admit` the
///   pgwire/REST edge uses) so the forwarded write counts at home. The
///   forwarded connection is counted at the home region; the originating region
///   does NOT also count it against the project (the originating connection is
///   a transient relay, not a project session executing the write locally).
///
/// In BOTH modes the rule is identical and is the answer to "does a forwarded
/// write bypass `max_connections`?": **no** — it is admitted and counted by the
/// home region's limiter, exactly once, at the region that executes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CeilingAccounting {
    /// Counted by the home region's `ConnectionLimiter` when Fly re-fires the
    /// request there. No local count in the originating region.
    AtHomeViaReplay,
    /// Counted by the home region's `ConnectionLimiter` when the forward client
    /// connects to the home engine's admission path. No local count for the
    /// project in the originating region.
    AtHomeViaForwardConnection,
}

impl ForwardMode {
    /// The connection-ceiling accounting model this mode uses for a forwarded
    /// write. `Off` has no model (a non-home write never executes), so this is
    /// only meaningful for the forwarding modes; it returns `None` for `Off`.
    pub fn ceiling_accounting(self) -> Option<CeilingAccounting> {
        match self {
            ForwardMode::Off => None,
            ForwardMode::FlyReplay => Some(CeilingAccounting::AtHomeViaReplay),
            ForwardMode::HttpForward => Some(CeilingAccounting::AtHomeViaForwardConnection),
        }
    }
}

// ─── the forwarder ─────────────────────────────────────────────────────────

/// Concrete [`WriteForwarder`] implementing both strategies behind
/// [`ForwardMode`]. Constructed once at startup and registered via
/// [`crate::Engine::attach_write_forwarder`].
pub struct RegionWriteForwarder {
    mode: ForwardMode,
    peers: PeerRegistry,
    client: Arc<dyn ForwardClient>,
}

impl RegionWriteForwarder {
    /// Build with an explicit mode / peers / client (used by tests and by
    /// [`Self::from_env`]).
    pub fn new(mode: ForwardMode, peers: PeerRegistry, client: Arc<dyn ForwardClient>) -> Self {
        Self {
            mode,
            peers,
            client,
        }
    }

    /// Build from the process environment, or `None` when forwarding is
    /// disabled ([`ForwardMode::Off`]) — in which case the caller should NOT
    /// register a forwarder, preserving the byte-identical fail-loud default.
    /// Returns an error only for a *malformed* peer list, so a typo fails loud
    /// at startup.
    pub fn from_env() -> Result<Option<RegionWriteForwarder>> {
        let mode = ForwardMode::from_env();
        if mode == ForwardMode::Off {
            return Ok(None);
        }
        let peers = PeerRegistry::from_env()?;
        let client: Arc<dyn ForwardClient> = Arc::new(UnconfiguredForwardClient);
        Ok(Some(Self::new(mode, peers, client)))
    }

    /// The configured mode (for startup logging / tests).
    pub fn mode(&self) -> ForwardMode {
        self.mode
    }

    /// PURE routing decision for a non-home write: given the home region and a
    /// replay token, decide the route. Off-mode is unreachable here (the
    /// forwarder is never registered in Off mode), but is mapped to a
    /// `WrongRegion` error defensively. An `http-forward` to a region absent
    /// from the peer registry is a typed `WrongRegion` error (unknown peer →
    /// cannot route), naming the missing region.
    ///
    /// `token` is supplied by the caller so the decision stays pure and
    /// deterministic for tests; production passes a fresh per-call token.
    pub fn route(
        &self,
        project: basin_common::ProjectId,
        home_region: &str,
        local_region: &str,
        token: &str,
    ) -> Result<ForwardRoute> {
        match self.mode {
            ForwardMode::Off => Err(BasinError::wrong_region_for(
                project,
                "write",
                home_region,
                local_region,
            )),
            ForwardMode::FlyReplay => Ok(ForwardRoute::Replay {
                home_region: home_region.to_string(),
                token: token.to_string(),
            }),
            ForwardMode::HttpForward => match self.peers.base_url(home_region) {
                Some(base_url) => Ok(ForwardRoute::HttpPeer {
                    home_region: home_region.to_string(),
                    base_url: base_url.to_string(),
                }),
                None => Err(BasinError::wrong_region_for(
                    project,
                    &format!("write (no {REGION_PEERS_ENV} entry for home region)"),
                    home_region,
                    local_region,
                )),
            },
        }
    }
}

/// Generate a fresh replay token: an opaque correlation / loop-guard id the
/// edge echoes into the `fly-replay` directive's `state=` field. Includes the
/// originating (local) region so a replay loop (region A → B → A) is visible in
/// logs. Uniqueness comes from a monotonic process counter; this is a
/// correlation id, not a security token.
fn fresh_replay_token(local_region: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{local_region}-{n}")
}

#[async_trait]
impl WriteForwarder for RegionWriteForwarder {
    async fn forward_write(&self, ctx: ForwardContext<'_>) -> Result<ExecResult> {
        // NOTE: the engine only ever calls this for an auto-commit single
        // statement against a non-home project (see `region::region_write_gate`
        // and the trait contract: in-transaction writes are rejected upstream
        // and never reach a forwarder). So the route decision below does not
        // re-check transaction state.
        let token = fresh_replay_token(ctx.local_region);
        match self.route(ctx.project, ctx.home_region, ctx.local_region, &token)? {
            // fly-replay: hand the typed control signal back. The edge turns it
            // into a `fly-replay: region=<home>` header; this call does NO I/O.
            ForwardRoute::Replay { home_region, token } => {
                Err(BasinError::forward_replay(home_region, token))
            }
            // http-forward: forward over 6PN and return the home result verbatim.
            ForwardRoute::HttpPeer { base_url, .. } => {
                self.client
                    .forward(&base_url, ctx.project, ctx.current_user, ctx.sql)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    fn proj() -> ProjectId {
        ProjectId::new()
    }

    // ── mode parsing ───────────────────────────────────────────────────────

    #[test]
    fn mode_parse_defaults_off() {
        assert_eq!(ForwardMode::parse(""), ForwardMode::Off);
        assert_eq!(ForwardMode::parse("off"), ForwardMode::Off);
        assert_eq!(ForwardMode::parse("nonsense"), ForwardMode::Off);
        // Fail-safe: a typo of a real mode does NOT enable forwarding.
        assert_eq!(ForwardMode::parse("fly-relpay"), ForwardMode::Off);
    }

    #[test]
    fn mode_parse_recognises_strategies() {
        assert_eq!(ForwardMode::parse("fly-replay"), ForwardMode::FlyReplay);
        assert_eq!(ForwardMode::parse(" FLY_REPLAY "), ForwardMode::FlyReplay);
        assert_eq!(ForwardMode::parse("http-forward"), ForwardMode::HttpForward);
        assert_eq!(ForwardMode::parse("6pn"), ForwardMode::HttpForward);
    }

    // ── peer registry parsing / resolution ──────────────────────────────────

    #[test]
    fn peers_parse_and_resolve() {
        let r = PeerRegistry::parse(
            "fra@basin-fra.internal:5432, sin@basin-sin.internal:5432 ,",
        )
        .expect("parse");
        assert_eq!(r.len(), 2);
        assert_eq!(r.base_url("fra"), Some("basin-fra.internal:5432"));
        assert_eq!(r.base_url("sin"), Some("basin-sin.internal:5432"));
        assert_eq!(r.base_url("jnb"), None);
    }

    #[test]
    fn peers_empty_is_ok_and_empty() {
        let r = PeerRegistry::parse("").expect("parse empty");
        assert!(r.is_empty());
        assert_eq!(r.base_url("fra"), None);
    }

    #[test]
    fn peers_malformed_fails_loud() {
        assert!(PeerRegistry::parse("frabasin-fra:5432").is_err()); // no '@'
        assert!(PeerRegistry::parse("@host:5432").is_err()); // empty region
        assert!(PeerRegistry::parse("fra@").is_err()); // empty host
    }

    // ── routing decision per mode ───────────────────────────────────────────

    fn fwd(mode: ForwardMode, peers: &str) -> RegionWriteForwarder {
        RegionWriteForwarder::new(
            mode,
            PeerRegistry::parse(peers).expect("peers"),
            Arc::new(UnconfiguredForwardClient),
        )
    }

    #[test]
    fn route_fly_replay_targets_home() {
        let f = fwd(ForwardMode::FlyReplay, "");
        let r = f
            .route(proj(), "fra", "jnb", "jnb-7")
            .expect("replay route");
        assert_eq!(
            r,
            ForwardRoute::Replay {
                home_region: "fra".into(),
                token: "jnb-7".into(),
            }
        );
    }

    #[test]
    fn route_http_forward_picks_peer() {
        let f = fwd(ForwardMode::HttpForward, "fra@host-fra:5432,sin@host-sin:5432");
        let r = f.route(proj(), "sin", "jnb", "tok").expect("http route");
        assert_eq!(
            r,
            ForwardRoute::HttpPeer {
                home_region: "sin".into(),
                base_url: "host-sin:5432".into(),
            }
        );
    }

    #[test]
    fn route_http_forward_unknown_region_is_typed_error() {
        let f = fwd(ForwardMode::HttpForward, "fra@host-fra:5432");
        let err = f
            .route(proj(), "sin", "jnb", "tok")
            .expect_err("unknown peer must error");
        match err {
            BasinError::WrongRegion(msg) => {
                assert!(msg.contains("sin"), "names the unrouteable home region");
                assert!(msg.contains(REGION_PEERS_ENV), "names the missing config");
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    #[test]
    fn route_off_mode_is_wrong_region() {
        // Defensive: Off is never registered, but route() still fails loud.
        let f = fwd(ForwardMode::Off, "");
        assert!(matches!(
            f.route(proj(), "fra", "jnb", "tok"),
            Err(BasinError::WrongRegion(_))
        ));
    }

    // ── forward_write end-to-end (decision only; no live region) ─────────────

    #[tokio::test]
    async fn forward_write_fly_replay_yields_replay_signal() {
        let f = fwd(ForwardMode::FlyReplay, "");
        let ctx = ForwardContext {
            project: proj(),
            home_region: "fra",
            local_region: "jnb",
            current_user: "app",
            sql: "INSERT INTO t VALUES (1)",
        };
        match f.forward_write(ctx).await {
            Err(BasinError::ForwardReplay { home_region, token }) => {
                assert_eq!(home_region, "fra");
                assert!(token.starts_with("jnb-"), "token carries origin region");
            }
            other => panic!("expected ForwardReplay, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forward_write_http_uses_client() {
        // A fake client that records its inputs and returns an Empty result,
        // standing in for the home-region engine.
        struct RecordingClient {
            seen: std::sync::Mutex<Option<(String, String, String)>>,
        }
        #[async_trait]
        impl ForwardClient for RecordingClient {
            async fn forward(
                &self,
                base_url: &str,
                _project: ProjectId,
                current_user: &str,
                sql: &str,
            ) -> Result<ExecResult> {
                *self.seen.lock().unwrap() =
                    Some((base_url.to_string(), current_user.to_string(), sql.to_string()));
                Ok(ExecResult::Empty {
                    tag: "INSERT 0 1".into(),
                })
            }
        }
        let client = Arc::new(RecordingClient {
            seen: std::sync::Mutex::new(None),
        });
        let f = RegionWriteForwarder::new(
            ForwardMode::HttpForward,
            PeerRegistry::parse("fra@host-fra:5432").unwrap(),
            client.clone(),
        );
        let ctx = ForwardContext {
            project: proj(),
            home_region: "fra",
            local_region: "jnb",
            current_user: "app",
            sql: "INSERT INTO t VALUES (1)",
        };
        let res = f.forward_write(ctx).await.expect("forward ok");
        assert!(matches!(res, ExecResult::Empty { .. }));
        let seen = client.seen.lock().unwrap().clone().expect("client called");
        assert_eq!(seen.0, "host-fra:5432", "routed to the home peer base url");
        assert_eq!(seen.1, "app");
        assert_eq!(seen.2, "INSERT INTO t VALUES (1)");
    }

    #[tokio::test]
    async fn forward_write_http_unknown_region_errors_without_calling_client() {
        let f = fwd(ForwardMode::HttpForward, "fra@host-fra:5432");
        let ctx = ForwardContext {
            project: proj(),
            home_region: "sin", // not in peers
            local_region: "jnb",
            current_user: "app",
            sql: "UPDATE t SET a=1",
        };
        assert!(matches!(
            f.forward_write(ctx).await,
            Err(BasinError::WrongRegion(_))
        ));
    }

    // ── ceiling-accounting model ────────────────────────────────────────────

    #[test]
    fn ceiling_accounting_is_always_at_home() {
        // A forwarded write is counted at the HOME region in both modes, never
        // double-counted, never bypassing the per-project max_connections cap.
        assert_eq!(ForwardMode::Off.ceiling_accounting(), None);
        assert_eq!(
            ForwardMode::FlyReplay.ceiling_accounting(),
            Some(CeilingAccounting::AtHomeViaReplay)
        );
        assert_eq!(
            ForwardMode::HttpForward.ceiling_accounting(),
            Some(CeilingAccounting::AtHomeViaForwardConnection)
        );
    }

    #[test]
    fn replay_token_is_unique_and_carries_region() {
        let a = fresh_replay_token("jnb");
        let b = fresh_replay_token("jnb");
        assert_ne!(a, b);
        assert!(a.starts_with("jnb-"));
    }
}
