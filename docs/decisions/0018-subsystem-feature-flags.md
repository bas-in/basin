---
title: "ADR 0018 — Subsystem feature flags + minimal-build target"
nav_section: decisions
sidebar_position: 18
summary: "Gate optional subsystems behind Cargo features so OSS users can ship a minimal pgwire-only binary; basin-cloud and the default OSS build keep the full feature set."
tags: [build, packaging, oss]
---

# 0018 — Subsystem feature flags + minimal-build target

- **Status:** Proposed, 2026-05-19.
- **Tags:** build, packaging, oss
- **Supersedes:** none
- **Cross-references:**
  [ADR 0006 (REST API layer)](./0006-rest-api-layer.md),
  [ADR 0012 (change-event primitive)](./0012-change-event-primitive.md)

## Context

`services/basin-server/Cargo.toml` today pulls in every subsystem
unconditionally: `basin-auth`, `basin-rest`, `basin-webhooks`,
`basin-engine`, `basin-storage`, `basin-catalog`, `basin-shard`,
`basin-router`, `basin-wal`, `basin-pool`. Two more subsystems are
on the roadmap as deferred crates: `basin-realtime` (WebSocket /
SSE realtime, per ADR 0012's "Trigger to revisit" clause),
`basin-wasm-udf` (5.11.J), and possibly others as customer signal
lands.

Without a feature-flag story, every deferred crate makes the binary
fatter and the compile slower for OSS users who don't need it. ADR
0006 already mentions in passing (line 181) that REST should "gate on
a Cargo feature so deploys can opt out at link time" — but no ADR has
committed the rule, and only two crates (`basin-net`, `basin-webhooks`)
have any `[features]` section today, both for internal toggles, not
user-facing opt-out.

The trait-shaped design committed in ADR 0012 (`ChangeEventSink`
registry + capture point at the commit boundary) makes opt-out clean
for sinks: not constructing and attaching a sink at startup is
identical to having compiled without it. The same shape applies to
HTTP layers — not mounting a route is identical to not compiling it.
This ADR commits to that as the project-wide rule.

## Decision

**Add Cargo features to `services/basin-server` that gate optional
subsystems at the registration boundary. Default build ships the
new-SaaS happy path; `--no-default-features` ships the
just-a-database minimal build. Three CI build configurations are
tested; everything else is the user's problem.**

### Feature definitions

```toml
# services/basin-server/Cargo.toml
[features]
default   = ["auth", "rest", "webhooks"]
auth      = ["dep:basin-auth"]
rest      = ["dep:basin-rest"]
webhooks  = ["dep:basin-webhooks"]
realtime  = ["dep:basin-realtime"]    # placeholder until ADR 0012 trigger fires
wasm-udf  = ["basin-engine/wasm"]     # placeholder until 5.11.J ships
```

`realtime` and `wasm-udf` are scaffolded now but the underlying crate
/ feature does not exist yet — they're declared so the contract is
visible and so adding them later is a no-op for the build matrix.

### What's core (non-gateable)

These crates form the database itself. There is no minimal build
without them:

- `basin-common`
- `basin-engine`
- `basin-catalog`
- `basin-storage`
- `basin-wal`
- `basin-shard`
- `basin-router`
- `basin-pool`
- `basin-iceberg-rest` (spec-compliant Iceberg catalog interop)
- pgwire (the primary client protocol; lives in `basin-engine`'s
  session layer)

### What's gateable

- `basin-auth` (`auth` feature) — JWT verification, per-project
  schemas (ADR 0013), bcrypt/argon2 password ops.
- `basin-rest` (`rest` feature) — PostgREST-shaped HTTP surface
  (ADR 0006).
- `basin-webhooks` (`webhooks` feature) — `WebhookSink` post-commit
  ChangeEventSink + disk-backed retry queue (Phase 5.11.I).
- `basin-realtime` (`realtime` feature, placeholder) — WebSocket /
  SSE ChangeEventSink, gated on ADR 0012's revisit clause.
- `basin-engine/wasm` (`wasm-udf` feature, placeholder) — `LANGUAGE
  wasm` UDFs via wasmtime (5.11.J).

Other internal crates (`basin-vector`, `basin-cv`, `basin-trgm`,
`basin-geo`, `basin-cron`, `basin-net`, `basin-sketch`, `basin-hottier`)
are consumed by `basin-engine` rather than by `basin-server`
directly. Gating them is a deeper change touching `basin-engine`'s
feature surface and is **out of scope for this ADR** — revisit if a
real demand signal appears.

### cfg-gate placement rule

All `#[cfg(feature = "…")]` gates live at the **registration
boundary** in `services/basin-server/src/`:

- sink attach: `Engine::attach_post_commit_sink(WebhookSink::new(…))`
- route mount: `app.merge(basin_rest::router())`
- auth attach: `Server::with_auth_provider(…)`

Gates **must not** appear inside `basin-engine`, `basin-catalog`, or
any other library crate. The library crates stay feature-clean; the
binary crate decides what to compose. This keeps test matrices
manageable and prevents `#[cfg]` from spreading into hot paths.

### CI matrix

Tested in CI:

1. `default` — `auth + rest + webhooks`
2. `--no-default-features` — minimal pgwire-only build
3. `--all-features` — kitchen sink (including placeholder features
   once their deps exist)

Combinations beyond those three are not tested. Users compiling
arbitrary subsets are on their own.

## What this does NOT commit us to

- **Splitting `basin-server` into multiple binaries** — not part of
  this ADR. One binary, multiple features. Multi-process deployment
  is a separate decision (see "Trigger to revisit" below).
- **Gating crates inside `basin-engine`** — out of scope. Vector
  search, continuous views, etc. compile in unconditionally.
- **A binary-size budget** — this ADR adds the mechanism, not a
  hard size SLO. A budget can be added later once real measurements
  exist.
- **Replacing the default build with the minimal build** — default
  stays `auth + rest + webhooks` because that's the new-SaaS happy
  path. Minimal is an opt-in for embedded / on-prem / "I'll bring my
  own HTTP layer" use cases.

## Trigger to revisit (split into separate binaries)

**Add a second binary (`basin-core`, pgwire+engine only) or split
`basin-server` into multiple processes** when ANY of:

1. An embedded use case demonstrates real pull — someone wants to
   link `basin-engine` as a library, not run it as a server. The
   minimal-features build doesn't fully solve this (server still
   has a tokio runtime + listen sockets); a true library would
   need a separate target.
2. basin-cloud benchmarks show a meaningful latency or throughput
   win from running `basin-rest` or `basin-realtime` as a separate
   proxy process. The trait-shaped design (ADR 0012) means this is
   a refactor of the deployment topology, not the engine.
3. A common distribution channel (Homebrew tap, official Docker
   image variants, package managers) demands a "lite" build that
   the feature-flagged single binary doesn't cleanly express.

Until then: one binary, multiple features. Premature
multi-process-ization adds ops surface without buying anything.

## References

- [ADR 0006 — REST API layer](./0006-rest-api-layer.md) — the
  in-passing mention of feature-gating REST that this ADR formalises.
- [ADR 0012 — Change-event primitive](./0012-change-event-primitive.md)
  — the trait-shaped design that makes opt-out clean for sinks.
- 2026-05-19 conversation log — the build-speed / multi-binary
  discussion that motivated writing this ADR.
