---
title: "ADR 0029 — Distributed-systems primitives Basin deliberately does not carry"
nav_section: decisions
sidebar_position: 29
summary: "Basin adopts verifiable release distribution and nothing else. No hybrid logical clock, no CRDT, no cryptographic node identity, no capability tokens — each absence a design consequence with the grep evidence behind it, not a gap."
tags: [architecture, meta, release, security]
---

# 0029 — Distributed-systems primitives Basin deliberately does not carry

- **Status:** Accepted (2026-07-30). Reframed 2026-08-12 — see note below.
- **Tags:** architecture, meta, release, security
- **Cross-references:** [ADR 0010 — catalog replication](./0010-catalog-replication.md),
  [ADR 0023 — leases and partition routing](./0023-leases-and-partition-routing.md),
  [ADR 0005 — auth system](./0005-auth-system.md),
  [ADR 0020 — Auth v2: OAuth + MFA](./0020-auth-v2-oauth-mfa.md)

> **Note on this revision.** This ADR was originally written as an answer to an
> external audit and framed its decisions by comparison to a specific set of
> third-party components. Basin is an independent project, so that framing was
> misleading about what Basin is. The revision keeps every technical decision and
> its evidence intact, and restates each one against the *general class* of
> solution rather than a named implementation. Nothing was withdrawn or reversed.
> This is an exception to the repo's don't-edit-accepted-ADRs rule, made
> deliberately and recorded here rather than silently.

## Context

Five recurring "shouldn't a distributed database have X?" questions deserve
written answers rather than silence: does Basin need a hybrid logical clock, a
CRDT, cryptographic node identity, capability tokens, or verifiable release
distribution?

The default failure mode is a second implementation nobody remembers choosing.
It is equally worth answering *honestly*: "we should adopt the standard
primitive" is wrong when the primitive solves a different problem, and adopting
it anyway is cargo-culting.

## Decision

**Basin adopts one of the five — verifiable release distribution — and
deliberately carries none of the other four.**

### Adopted: signed, verifiable release artifacts

Before this ADR, `release.yml` published per-target tarballs each with its own
`.sha256` sidecar, and nothing else: no manifest over the release as a whole, no
signature, no attestation, and no script a user could run. A per-asset digest
fetched from the same origin as the asset proves only that the origin is
self-consistent with itself.

Basin's release job now emits `SHA256SUMS` over the whole staged directory,
checks it with the same `scripts/verify.sh` that users run, proves the verifier
still refuses 24 kinds of broken release, and attaches a sigstore
build-provenance attestation minted from the workflow's OIDC identity. The
verifier's selftest exercises the "every `.` in a filename is a regex wildcard"
trap against the punctuation Basin actually publishes.

No long-lived key, no new secret, and no service the user does not already
depend on to have fetched the release at all.

### Not adopted: hybrid logical clocks

Basin has no HLC and needs none. `grep -ril 'hybrid.logical\|HybridLogical' crates services tests` returns nothing.

Ordering inside a Basin deployment is a **log position**, not a timestamp:
`basin-wal` maintains an LSN and the Raft log gives a single total order per
partition. Where wall-clock time appears it is either user data
(`TIMESTAMPTZ` columns) or a lease expiry (ADR 0023), and a lease is
deliberately *not* an ordering primitive — it is a fencing token whose safety
argument is "the previous owner's writes are rejected", not "clocks agree".

An HLC buys causal ordering across nodes that cannot coordinate. Basin's nodes
*do* coordinate, through Raft, on purpose. Introducing an HLC would add a second
notion of order alongside the log, which is how a system acquires two answers to
"what happened first".

### Not adopted: a CRDT

ADR 0010 already rejected this on its own merits, and that reasoning is
reaffirmed here: the catalog's contract is that a commit either wins or
conflicts, which is a consensus problem, not a merge problem. A CRDT that
preserves that contract reduces to single-writer anyway; one that does not
preserve it is a different product.

Convergent replicated types are the right tool for eventually-consistent,
offline-first state — a mail client's local store — and the wrong tool for a
database's schema catalog, where "both DDL statements merged" is not an
acceptable outcome. **Do not introduce a CRDT dependency into Basin.**

The two `last-write-wins` mentions in the tree
(`basin-hottier/src/memtable.rs`, `basin-engine/src/lib.rs`) are local
descriptions of overlay-version selection *within one node's memtable* — not a
distributed merge function.

### Not adopted: cryptographic node identity

Basin's `NodeId` is openraft's: a small integer naming a member of one
operator's Raft cluster, resolved to an address through configuration
(`basin-wal/src/raft_net/peers.rs`). Peer authentication is TLS
(`raft_net_tls.rs`), i.e. the operator's own PKI.

Self-sovereign identity systems — device certificates, recovery policy, key
transparency, out-of-band safety numbers — solve a genuinely different problem:
zero-authority identity for parties who have no shared administrator. Basin's
replicas share an administrator by construction. Adopting such a system to let
three processes an operator started recognise each other would add a
key-management story where a config file suffices.

### Not adopted: capability tokens

`grep -ril 'macaroon\|biscuit\|ucan\|capability token' crates services` returns
nothing, and that is correct. (Scoped to code: over `docs` it now matches this
ADR and the two generated indexes that carry its summary, which says nothing
about the implementation.) Basin's authorization surface is
Postgres-shaped on purpose: a JWT identifies the caller, `auth.uid()` exposes
its `sub` claim to SQL, and **row-level security policies are the authorization
primitive** (ADR 0005, ADR 0020). That is the compatibility promise — an
existing Postgres application's policies keep meaning what they meant.

Capability-token schemes (UCAN, macaroons, biscuits) delegate authority between
mutually-distrusting parties across a network. Basin's equivalent question —
"may this caller see this row?" — is answered by a policy the schema owner
wrote, evaluated in the engine. Bolting delegated capability tokens on top would
create a second authorization path that RLS does not see, which is the shape of
an isolation bug rather than a feature.

## Consequences

- Verifiable release distribution is now present in Basin, and
  `verify.sh --selftest` runs on every push, so it cannot quietly stop refusing.
- Four "should Basin adopt X?" questions have a written answer with the grep
  evidence behind it, so the next reviewer can check the reasoning instead of
  re-deriving it.
- Basin remains free of external product dependencies. *Not importing something
  you do not need* is cheaper than importing it and documenting why it is unused.

## Trigger to reconsider

- **CRDT:** if Basin ever grows a genuinely offline-writable client-side
  replica — an embedded Basin on a device that accepts writes while partitioned
  and must merge on reconnect — then the merge problem becomes real and
  convergent types are the first thing to evaluate. Today's read replicas
  (ADR 0004) do not accept writes, so it is not that.
- **HLC:** if partitions ever need a cross-partition causal order *without* a
  shared Raft group — for example a cross-shard change-stream that must be
  ordered globally without 2PC (ADR 0011 rejected 2PC) — an HLC becomes the
  standard answer and this decision should be revisited rather than worked
  around with wall-clock comparisons.
- **Node identity:** if Basin nodes are ever federated across administrative
  domains (one org's replica following another's), the "operator's own PKI"
  assumption breaks and device-certificate identity becomes the right prior art.
- **Capability tokens:** if a caller must delegate a *subset* of its own access
  to a third party without that party holding the caller's JWT, RLS alone cannot
  express it, and a capability profile becomes the honest answer.
