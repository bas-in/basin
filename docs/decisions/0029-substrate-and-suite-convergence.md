---
title: "ADR 0029 — What Basin shares with the VulOS substrate, and what it deliberately does not"
nav_section: decisions
sidebar_position: 29
summary: "Basin adopts the suite's release-verification pattern and nothing else. No HLC, no CRDT, no cryptographic node identity, no capability tokens — each absence a design consequence, not a gap."
tags: [architecture, meta, substrate, release]
---

# 0029 — What Basin shares with the VulOS substrate, and what it deliberately does not

- **Status:** Accepted (2026-07-30)
- **Tags:** architecture, meta, substrate, release
- **Cross-references:** [ADR 0010 — catalog replication](./0010-catalog-replication.md),
  [ADR 0023 — leases and partition routing](./0023-leases-and-partition-routing.md),
  [ADR 0005 — auth system](./0005-auth-system.md),
  [ADR 0020 — Auth v2: OAuth + MFA](./0020-auth-v2-oauth-mfa.md)

## Context

A suite-wide audit asked every product the same five questions: does it carry
its own hybrid logical clock, node identity, signed-artifact distribution,
capability tokens, or a CRDT — and if so, does it agree with the published
substrate (`kotva-core`/`kotva-mail`/`kotva-sync`, and `ephor` for
reachability) or has it grown a private fork of a solved problem?

The question is worth answering explicitly rather than by silence, because the
default failure mode is a second implementation nobody remembers choosing. It
is equally worth answering *honestly*: "we should converge on the substrate" is
wrong when the substrate solves a different problem, and adopting it anyway
would be cargo-culting.

The written answer matters more than the code change, because the audit found
that in this suite the thing that actually propagated between repos was **copied
files**, not shared design documents.

## Decision

**Basin adopts one thing from the suite — the release-verification pattern —
and deliberately shares none of the other four primitives.**

### Adopted: signed-artifact distribution

Before this ADR, `release.yml` published per-target tarballs each with its own
`.sha256` sidecar, and nothing else: no manifest over the release as a whole, no
signature, no attestation, and no script a user could run. A per-asset digest
fetched from the same origin as the asset proves only that the origin is
self-consistent with itself.

`ephor`'s `RELEASE-TEMPLATE.md` is explicit that it is a **template, not a
shared dependency** — copy the file, change one line. Basin does exactly that:
`scripts/verify.sh` is a copy with `DEFAULT_REPO` repointed (and the selftest's
synthetic asset renamed to Basin's real naming shape, so the "every `.` in a
filename is a regex wildcard" trap is exercised against the punctuation Basin
actually publishes). The release job emits `SHA256SUMS` over the whole staged
directory, checks it with the same script users run, proves the verifier still
refuses 24 kinds of broken release, and attaches a sigstore build-provenance
attestation minted from the workflow's OIDC identity.

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

`kotva-sync` exists for eventually-consistent, offline-first replicated state in
the DMTAP algebra. That is the right tool for a mail client's local store and
the wrong tool for a database's schema catalog, where "both DDL statements
merged" is not an acceptable outcome. **Do not introduce a `kotva-sync`
dependency into Basin.** It would also be a cross-product import, which is
independently forbidden.

The two `last-write-wins` mentions in the tree
(`basin-hottier/src/memtable.rs`, `basin-engine/src/lib.rs`) are local
descriptions of overlay-version selection *within one node's memtable* — not a
distributed merge function.

### Not adopted: cryptographic node identity

Basin's `NodeId` is openraft's: a small integer naming a member of one
operator's Raft cluster, resolved to an address through configuration
(`basin-wal/src/raft_net/peers.rs`). Peer authentication is TLS
(`raft_net_tls.rs`), i.e. the operator's own PKI.

`kotva-core`'s `identity` module solves a genuinely different problem —
zero-authority, user-facing identity with device certificates, recovery policy,
key transparency and out-of-band safety numbers, for parties who have no shared
administrator. Basin's replicas share an administrator by construction. Adopting
a self-sovereign identity system to let three processes an operator started
recognise each other would add a key-management story where a config file
suffices.

### Not adopted: capability tokens

`grep -ril 'macaroon\|biscuit\|ucan\|capability token' crates services` returns
nothing, and that is correct. (Scoped to code: over `docs` it now matches this
ADR and the two generated indexes that carry its summary, which says nothing
about the implementation.) Basin's authorization surface is
Postgres-shaped on purpose: a JWT identifies the caller, `auth.uid()` exposes
its `sub` claim to SQL, and **row-level security policies are the authorization
primitive** (ADR 0005, ADR 0020). That is the compatibility promise — an
existing Postgres application's policies keep meaning what they meant.

`kotva-core`'s `capability` module is a UCAN v1.0 profile for delegating
authority between mutually-distrusting parties across a network. Basin's
equivalent question — "may this caller see this row?" — is answered by a policy
the schema owner wrote, evaluated in the engine. Bolting delegated capability
tokens on top would create a second authorization path that RLS does not see,
which is the shape of an isolation bug rather than a feature.

## Consequences

- The suite's release-verification pattern is now present in Basin, and
  `verify.sh --selftest` runs on every push, so it cannot quietly stop refusing.
- Four "should Basin use the substrate for X?" questions have a written answer
  with the grep evidence behind it, so the next audit can check the reasoning
  instead of re-deriving it.
- Basin remains free of any cross-product import. The substrate is not a
  product, but *not importing something you do not need* is cheaper than
  importing it and documenting why it is unused.

## Trigger to reconsider

- **CRDT / `kotva-sync`:** if Basin ever grows a genuinely offline-writable
  client-side replica — an embedded Basin on a device that accepts writes while
  partitioned and must merge on reconnect — then the merge problem becomes real
  and `kotva-sync`'s algebra is the first thing to evaluate. Today's read
  replicas (ADR 0004) do not accept writes, so it is not that.
- **HLC:** if partitions ever need a cross-partition causal order *without* a
  shared Raft group — for example a cross-shard change-stream that must be
  ordered globally without 2PC (ADR 0011 rejected 2PC) — an HLC becomes the
  standard answer and this decision should be revisited rather than worked
  around with wall-clock comparisons.
- **Node identity:** if Basin nodes are ever federated across administrative
  domains (one org's replica following another's), the "operator's own PKI"
  assumption breaks and `kotva-core::identity` becomes the right prior art.
- **Capability tokens:** if a caller must delegate a *subset* of its own access
  to a third party without that party holding the caller's JWT, RLS alone cannot
  express it, and a capability profile becomes the honest answer.
