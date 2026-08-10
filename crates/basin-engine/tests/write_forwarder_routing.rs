//! Cross-region [`WriteForwarder`] contract tests (ADR 0009 v0.2).
//!
//! These exercise the PUBLIC forwarder surface — mode gating, the
//! replay-vs-http-forward-vs-reject branch, peer resolution, and the
//! ceiling-accounting model — through `forward_write` / `route`, the same way
//! the engine's write gate calls them. They are the unit-testable half of the
//! feature.
//!
//! What is NOT tested here (deployment-bound, like the multi-machine Raft path,
//! and explicitly documented as such):
//!
//! * A real `fly-replay` honoured by Fly, or a live 6PN HTTP forward landing in
//!   a second region's engine — needs ≥2 deployed regions on real hardware.
//! * The gate-level wiring that (a) lets a HOME-region write pass through
//!   untouched and (b) refuses an in-transaction non-home write WITHOUT calling
//!   the forwarder. That logic lives in `basin_engine::region::region_write_gate`
//!   / `forward_or_reject`, runs against a live `ProjectSession`, and is covered
//!   by the engine's session-level tests. Here we assert the forwarder-side
//!   invariants those gate paths rely on: the forwarder is only ever handed a
//!   non-home auto-commit write, and for such a write each mode produces the
//!   documented decision. The home-passthrough / in-transaction contracts are
//!   verified at the gate (which never calls the forwarder in those cases), so
//!   the forwarder has no code path to forward them.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, Result};
use basin_engine::region::{ForwardContext, WriteForwarder};
use basin_engine::write_forwarder::{
    CeilingAccounting, ForwardClient, ForwardMode, ForwardRoute, PeerRegistry, RegionWriteForwarder,
};
use basin_engine::ExecResult;

fn ctx<'a>(home: &'a str, local: &'a str, sql: &'a str) -> ForwardContext<'a> {
    ForwardContext {
        project: ProjectId::new(),
        home_region: home,
        local_region: local,
        current_user: "app",
        sql,
    }
}

fn fwd(mode: ForwardMode, peers: &str) -> RegionWriteForwarder {
    RegionWriteForwarder::new(
        mode,
        PeerRegistry::parse(peers).expect("peer list parses"),
        Arc::new(NoopClient),
    )
}

/// A forward client that must never be called in the cases that error before
/// transport. Panics if used.
struct NoopClient;
#[async_trait]
impl ForwardClient for NoopClient {
    async fn forward(
        &self,
        _base_url: &str,
        _project: ProjectId,
        _current_user: &str,
        _sql: &str,
    ) -> Result<ExecResult> {
        panic!("ForwardClient must not be called on this path");
    }
}

// ── mode = off ───────────────────────────────────────────────────────────────

/// off-mode: a non-home write yields WrongRegion (08006), UNCHANGED from the
/// no-forwarder default — never a replay directive.
#[tokio::test]
async fn off_mode_non_home_write_is_wrong_region() {
    let f = fwd(ForwardMode::Off, "");
    match f
        .forward_write(ctx("fra", "jnb", "INSERT INTO t VALUES (1)"))
        .await
    {
        Err(BasinError::WrongRegion(msg)) => {
            assert!(
                msg.contains("fra") && msg.contains("jnb"),
                "names both regions"
            );
        }
        other => panic!("off mode must be WrongRegion, got {other:?}"),
    }
    // And the env-constructed forwarder is None in off mode (nothing attached).
    // (route() defensively still rejects.)
    assert!(matches!(
        f.route(ProjectId::new(), "fra", "jnb", "tok"),
        Err(BasinError::WrongRegion(_))
    ));
}

// ── mode = fly-replay ────────────────────────────────────────────────────────

/// fly-replay: a non-home write yields the replay control signal naming the
/// correct target home region — NOT WrongRegion.
#[tokio::test]
async fn fly_replay_non_home_write_yields_replay_directive() {
    let f = fwd(ForwardMode::FlyReplay, "");
    match f.forward_write(ctx("fra", "jnb", "UPDATE t SET a=1")).await {
        Err(BasinError::ForwardReplay { home_region, token }) => {
            assert_eq!(home_region, "fra", "replay targets the home region");
            assert!(
                token.starts_with("jnb-"),
                "token carries the originating region"
            );
        }
        other => panic!("fly-replay must be ForwardReplay, got {other:?}"),
    }
}

#[test]
fn fly_replay_route_picks_home_regardless_of_peers() {
    // fly-replay needs no peer list — Fly resolves the region itself.
    let f = fwd(ForwardMode::FlyReplay, "");
    assert_eq!(
        f.route(ProjectId::new(), "sin", "jnb", "tk").unwrap(),
        ForwardRoute::Replay {
            home_region: "sin".into(),
            token: "tk".into()
        }
    );
}

// ── mode = http-forward ──────────────────────────────────────────────────────

/// http-forward: the routing decision picks the right peer from
/// BASIN_REGION_PEERS and the statement reaches that peer verbatim.
#[tokio::test]
async fn http_forward_routes_to_correct_peer() {
    struct Recorder(Mutex<Option<String>>);
    #[async_trait]
    impl ForwardClient for Recorder {
        async fn forward(
            &self,
            base_url: &str,
            _project: ProjectId,
            _current_user: &str,
            sql: &str,
        ) -> Result<ExecResult> {
            *self.0.lock().unwrap() = Some(format!("{base_url}|{sql}"));
            Ok(ExecResult::Empty {
                tag: "UPDATE 1".into(),
            })
        }
    }
    let rec = Arc::new(Recorder(Mutex::new(None)));
    let f = RegionWriteForwarder::new(
        ForwardMode::HttpForward,
        PeerRegistry::parse("fra@h-fra:5432,sin@h-sin:5432").unwrap(),
        rec.clone(),
    );
    let res = f
        .forward_write(ctx("sin", "jnb", "UPDATE t SET a=1"))
        .await
        .expect("forward ok");
    assert!(matches!(res, ExecResult::Empty { .. }));
    assert_eq!(
        rec.0.lock().unwrap().clone().unwrap(),
        "h-sin:5432|UPDATE t SET a=1",
        "routed to sin's peer with verbatim SQL"
    );
}

/// http-forward: an unknown home region (no peer entry) is a typed error and
/// the client is never invoked.
#[tokio::test]
async fn http_forward_unknown_region_is_typed_error() {
    let f = fwd(ForwardMode::HttpForward, "fra@h-fra:5432"); // NoopClient panics if called
    match f
        .forward_write(ctx("sin", "jnb", "INSERT INTO t VALUES (1)"))
        .await
    {
        Err(BasinError::WrongRegion(msg)) => {
            assert!(msg.contains("sin"), "names the unrouteable region");
            assert!(
                msg.contains("BASIN_REGION_PEERS"),
                "points at the missing config"
            );
        }
        other => panic!("unknown peer must be a typed WrongRegion, got {other:?}"),
    }
}

// ── home-region write never reaches a forward decision ───────────────────────

/// The forwarder is only ever called for a NON-home write (the gate returns
/// early for home/unpinned projects — see module docs). We assert the
/// forwarder-side invariant that backs that: when local == home, both
/// forwarding modes still produce a self-targeted decision that would be a
/// no-op replay/loop — which is exactly why the gate must (and does) short-
/// circuit before calling the forwarder. This documents the contract boundary.
#[test]
fn home_equals_local_is_a_gate_responsibility_not_the_forwarders() {
    let f = fwd(ForwardMode::FlyReplay, "");
    // If the gate ever (incorrectly) called the forwarder with home==local,
    // the replay would target the SAME region — a detectable self-loop, not a
    // silent local write. The gate prevents this; the token-carries-origin
    // design makes such a loop observable in logs if it ever regressed.
    let r = f.route(ProjectId::new(), "jnb", "jnb", "jnb-0").unwrap();
    assert_eq!(
        r,
        ForwardRoute::Replay {
            home_region: "jnb".into(),
            token: "jnb-0".into()
        },
        "self-target is detectable (gate short-circuits before this in practice)"
    );
}

// ── ceiling-accounting model ─────────────────────────────────────────────────

/// A forwarded write is accounted at the HOME region in both modes, exactly
/// once, never bypassing per-project max_connections.
#[test]
fn forwarded_write_is_counted_at_home() {
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
