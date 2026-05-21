# Autonomous-loop decisions log

Entries are appended in reverse-chronological order (newest at top). Each
entry records a decision made by the autonomous-loop dev sweep that
isn't already captured in TASK.md, an ADR, or a commit message.

---

## 2026-05-21 — Multi-replica scale-out architecture: leases, not coordinator-or-counters (ADR 0023)

The noisy-neighbor audit (`docs/audits/2026-05-21-noisy-neighbor-fairness.md`,
4 P0) surfaced two coupled architectural P0s: **hot-tenant pinning** (router
hashes ProjectId→one replica; memtable + WAL live only there) and
**multi-instance cap bypass** (per-project caps are per-process DashMaps; N
replicas = N× the cap). The audit offered two repair options — (a) central
coordinator/budget service, (b) replicated counters. **Rejected both:** (a)
fixes caps but leaves whales pinned; (b) pays a distributed-systems cost on
the hot path for a weaker (a). Neither addresses the root cause: per-project
state lives in process-local memory keyed by a hash.

**Decision (ADR 0023):** lease-based ownership + partition-level routing +
heartbeat budgets. Key insight — almost every subsystem is ALREADY keyed by
`(project,partition)` / `(project,table)` (WAL, memtable, catalog, blooms,
sketches, indexes); only the router still hashes whole-project. Fix that
mismatch. Ownership becomes a lease in the catalog Postgres (CAS + TTL +
epoch fencing token; no Raft). Replicas go stateless; the router consults
leases; budgets reconcile via 5s heartbeats (leaseholder is the per-partition
arbiter — no per-request coordination; project totals sum across leaseholders
async). This fixes pinning AND cap-correctness in one shape, composes with
everything already built, and needs no new infra service. Cost: ~6–10 wk.
Phase 6.X, TOP PRIORITY. Three single-instance mechanical fixes (statement
timeout, catalog pool, Wasm dedicated runtime) ship first as Phase 6.P0 —
independent, ~1 day each, immediately good.

**Also (2026-05-21 security audit, 3 P0):** Phase 6.SEC beta-blocker — MFA
TOTP replay is a no-op (`mfa.rs:852,931` passes empty seen-set), WebAuthn does
zero crypto (only echoes the challenge nonce), inbound webhooks are
unauthenticated (the ADR-0019 plaintext gate doesn't exist). aal2 is currently
security theater. Must fix before any beta; ~4–5 eng-days for all P0 + 4/5 P1.

---

## 2026-05-20 — Schemas: user-defined WON'T-DO, system schemas made real (ADR 0022)

Decided after an architecture discussion. Full rationale in
[ADR 0022](./docs/decisions/0022-system-schema-namespacing.md).

**SQL schemas' original purpose** (SQL-92): a namespace of objects *owned by a
single authorization identifier* — i.e. a per-user private namespace inside one
shared database (Postgres's `"$user", public` default search_path is the
fossil). Modern uses (schema-per-tenant, logical grouping, extension
namespacing) are drift from that intent.

**Decision:**
- **User-defined schemas → WON'T-DO.** Basin's **project** already IS the
  owner-scoped isolated namespace schemas were invented for, promoted to a hard
  storage-prefix boundary. A second user-facing namespace axis is redundant and
  adds a second place to leak (violates the "one leaked row kills the project"
  invariant) for ~zero wedge benefit. `CREATE SCHEMA x` stays accepted but
  **aliased to `public`** (flat). Note: schemas are *more complex* but **not
  meaningfully less efficient** — they're a metadata concern; the objection is
  complexity/correctness-surface, not CPU/storage. We skip them because
  projects make them redundant, not because they're slow.
- **System schemas → MAKE REAL.** `auth/storage/cron/net/realtime/public/
  pg_catalog/information_schema` become first-class **reserved** schemas with
  honest `(schema, table)` keying + real `search_path` + honest
  introspection. This replaces today's prefix hacks (`basin_auth_*`,
  `_net_http_response`, etc.), makes `auth.users`/`storage.objects` genuinely
  schema-qualified for Supabase-shaped SDK + PG tooling, and leaves the
  per-project isolation boundary untouched.

**Implementation:** Phase 5.18.A–E (catalog keying → search_path → migrate
system namespaces → honest introspection → differential/tooling test). Reserved
schema list is a closed enum; user schemas are NOT user-extensible containers.

---

## 2026-05-20 — Big session-limit cutoff (8:50pm reset) + recovery

7 agents killed mid-flight at the account session limit. Recovery done by
the loop owner (Opus, not limit-blocked):

**Recovered + committed (green):**
- basin-cli: byo / oauth_apps / saml / scim ports (`67bb419`, `0285e66`,
  `fd78b60`, `55befa8`) — **basin-cli rust-port now has ZERO unported
  commands; 950 tests green.**
- 5.17.E bytes counter (`<committed>`) — basin-blob 31 tests.
- 5.10.M MFA **core** (`<committed>`) — basin-auth 123 tests. Fixed a
  missing `use base64::Engine;` in mfa.rs.

**Re-dispatch queue (after 8:50pm reset) — all verified-needed:**
1. **5.10.M MFA integration tests** — `tests/integration/tests/mfa_totp.rs.wip`
   + `mfa_webauthn.rs.wip` (renamed `.wip` to keep the workspace compiling).
   63 errors: the test's `MfaCache` mock doesn't impl `MfaStore`. Fix the
   mock (or use the real store), rename back to `.rs`.
2. **5.17.D signed URLs** — `crates/basin-rest/src/routes/storage_sign.rs`
   exists (orphan, untracked) but was NEVER wired (no `mod storage_sign`,
   no route mount). Wire it into routes/mod.rs + server.rs + storage.rs,
   add the HMAC verify path + tests.
3. **GROUPS/EXCLUDE window frames** — agent died with nothing in tree; redo
   from scratch (window_extras.rs + window_fns.rs test).
4. **5.17.C storage RLS** — never started; storage.objects honours RLS
   policies (Phase 5.6) via auth.uid/role/aal; public buckets short-circuit.
5. **matview test reconciliation** — `crates/basin-engine/tests/cv_sql_round_trip.rs:143`
   `create_materialized_view_without_basin_continuous_is_rejected` fails:
   a sibling added snapshot-matview support so non-continuous CREATE MV now
   succeeds. Update the test to assert snapshot behaviour (feature is intended).
6. **CLI clippy cleanup** — pre-existing clippy errors in basin-cli (flagged
   by the WS-test agent); non-blocking polish.

**Inert untracked files left in tree (safe — do not break build):**
`storage_sign.rs` (not in any `mod`), `*.rs.wip` (cargo doesn't compile `.wip`).

---

## 2026-05-20 — Roadmap reload + new auth/storage work chain

User hand-updated TASK.md (OSS), TASKS.md+ROADMAP.md (js, cli) and the
cloud roadmap. Reloaded. Material changes:

- **New OSS engine work** that unblocks basin-js's 🔒 stubs:
  - **5.10.O** OAuth providers (presets + generic OIDC), ADR 0020 basin-auth v2
  - **5.10.M** MFA: TOTP + WebAuthn/passkeys, ADR 0020
  - **5.17.A–F** Object storage (Supabase-style blobs), ADR 0021 — new crate +
    basin-rest HTTP API + RLS + signed URLs + bytes counter + TUS resumable
- **basin-js** is fully ✅ except T-020–024 (OAuth/MFA/storage), which stay
  🔒 until the OSS routes above ship. Route shapes are specced in the js
  ROADMAP (OAuth `/auth/v1/authorize`, MFA `/auth/v1/factors/*`, storage
  `/storage/v1/object/*`).

**Dispatch chain:** OSS 5.10.O/M + 5.17.* → then basin-js T-020–024 wiring.

**Dispatcher discipline reaffirmed:** before sending agents for these,
re-read the full 5.10.O / 5.10.M / 5.17.* specs from TASK.md at dispatch
time (don't trust context — it goes stale as the user edits). Hold the
new wave until the in-flight OSS wave lands: 6.X is editing the workspace
`Cargo.toml` and 5.17.A also adds a crate there — guaranteed conflict if
run concurrently.

**Open issues to triage when the OSS wave lands:**
1. CLI Go-deletion (agent a49382a) was **reverted** on agent death — 104
   .go files are back; HEAD still fe80e9c. Re-dispatch the delete once the
   Rust port is confirmed parity-complete + green.
2. `create_materialized_view_without_basin_continuous_is_rejected` test now
   fails — a sibling agent added snapshot-matview support without updating
   the test. Reconcile (update test or revert the behaviour change).

---

## 2026-05-20 — 1M-context credit cutoff + tangled-orphan recovery

Three agents (advanced LATERAL, advanced window frames, 5.13.B prescreen
continuation) all died with **"Usage credits required for 1M context"** —
a different failure from the earlier session-limit cutoffs. The pattern:
a long-running agent accumulates >200k tokens of context, flips into 1M
mode, and that mode requires usage credits that aren't enabled. The work
was done; the agents just couldn't commit the final result.

**Critical recovery finding:** HEAD (`7967a42`) did **not compile on its
own** — earlier committed work referenced helpers that lived only in the
uncommitted tree. The three agents' orphaned work was intertwined across
shared files (executor.rs, lib.rs), so it could not be split per-feature
without leaving a non-compiling intermediate commit. Recovered as one
bundled commit `dfff647` after verifying every affected suite green
(LATERAL 7/7, window_fns 21/21, sql_support_matrix pass).

Separately fixed the lone pre-existing red — `nullif_rewrite::tests::
bare_nullif_is_left_alone` (`7dfd4c5`): a stale Debug-string assertion,
not a behaviour bug. Newer DataFusion renders a ScalarUDF Debug as the
struct name (`NullIfFunc`) instead of call-style `nullif(...)`.

**Mitigation for the dispatcher.** The 1M-context trip happens at the
*end* of long agent runs. Keep agent prompts tight and single-purpose so
they finish + commit before crossing ~200k context. Prefer many small
focused agents over few sprawling ones. Cap concurrent agents at 5 per
the user's instruction. After ANY agent reports a credit/limit error,
assume its work is uncommitted-but-possibly-complete: verify compile +
tests, recover by committing explicitly-staged files.

---

## 2026-05-20 — Account session-limit cutoff + orphan recovery

Three agents reported "completed" but two were actually **killed by the
Anthropic account session limit** (reset 6:30am Africa/Johannesburg), not
finished:

- **5.11.R5** (filter pushdown) — genuinely done; landed in `94db513`
  (co-committed from a shared working tree with 5.16.D).
- **5.11.R3** (WebSocket) — code-complete but uncommitted when killed.
  Recovery: verified `cargo test realtime_ws` → 6/6 green, committed as
  `10858d8`. No rework needed.
- **5.11.C2** (constraint reactors) — uncommitted AND buggy: a stack
  overflow in `drop_reactor_removes_constraint` (DROP-path recursion).
  Re-dispatched a fresh agent to root-cause + fix rather than commit
  broken work.

**Lesson for the loop dispatcher.** An agent "completed" notification is
NOT proof of success. Always verify: (1) did it commit (`git log`), and
(2) does its work pass (`cargo test`). Recover clean orphans directly;
re-dispatch buggy ones. Stage recovered files EXPLICITLY by path — never
`git add -A` while sibling agents have uncommitted work in the tree
(this session already had R6's work accidentally swept into a docs commit
that way, commit `d1acaa0`).

---

## 2026-05-20 — ADR 0020 reconciliation (WAL Commit marker vs implicit-commit-at-EOF)

**Option A chosen (impl wins).** The C2 WAL impl (`5551761`) shipped `TxBegin` + `TxRollback` only — no `TxCommit` variant. ADR 0020 §6 required an explicit `Commit` marker. Reconciliation: updated ADR 0020 with a "Reconciliation (2026-05-20)" section documenting the shipped implicit-commit-at-EOF semantics, the correctness gap, and the deferred path to add explicit `TxCommit` in Phase 5.14.C4+. No code changes needed; `cargo check --workspace` is clean. Option B (add `TxCommit` variant + emit from executor) was deferred due to high conflict risk with the C3 agent editing `executor.rs` and because the correctness impact is bounded to the hot tier's in-memory rebuild path (no object-store durability affected).

---

## 2026-05-20 — Wave 3 reality check + recovery

**TASK.md was MORE out of sync than the kickoff entry knew.** Three Phase
5.14/5.16 items shipped in prior sessions but TASK.md still showed `[ ]`:

- `57dae11` 5.14.C1 — `MemTable`/`MemTableRegistry` crate skeleton
- `d7b96c4` 5.14.B1 — basin-sketch crate hoist (already shipped before my B1 dispatch)
- `12b2fc2` 5.14.C5 — first-class budget module + ALTER PROJECT DDL + 10k-tenant fuzz
- `33ae73f` 5.16.A — `QueryShapeHash` with xxh3_64 cross-process stability
- `f5bb2e6` — follow-up fix adding sketch fields to DataFile in storage reader/writer
- `5551761` 5.14.C2 prep — WAL BEGIN/ROLLBACK markers

The `git log -5` at session start truncated below `82e7b2f`, hiding these.

**Recovery actions taken.**

- Stopped duplicate B1 + C5 agents (`aad212340ed014b97`, `a9920bb6fcb30b5d3`).
  Note: B1 agent had already committed a clean validation tick (`1c2387d`)
  before being stopped — net effect is just the TASK.md checkbox update.
- Committed the orphaned B5 sketch differential (`tests/integration/tests/sketches.rs`,
  538 lines) that agent `a583d6d17fea33e01` wrote but didn't commit before
  its 1M-row × 7-file-counts test run exhausted its lifetime. Marked
  `#[ignore]` with run instructions; recovered as commit `6d8aafc`.
- Synced TASK.md ticks for C1 / C5 / 5.16.A.

**Lesson for the loop dispatcher.** Before dispatching the next wave,
ALWAYS run `git log --oneline -20` (not -5) and grep for the candidate
phase markers in commit messages — TASK.md cannot be trusted as the
canonical state.

**Still-running agents (genuine work).** C3 (`a3ef293caa7524c1e`) reads-merge
path; D4 (`ada18687e5a690d55`) D2/D3 differential. C3's `merge.rs` (385
lines) and D4's `catalog_window.rs` (701 lines) are present in the working
tree. If D4 hits the same test-execution timeout B5 did, recover its file
the same way (commit with `#[ignore]`).

---

## 2026-05-20 — Wave 3 dispatch + autonomous loop kickoff

**Loop config.** `/loop 10m autonomous …` → cron `3,13,23,33,43,53 * * * *`
(offset off :00 to avoid the API fleet thundering herd at every-N-minutes
boundaries). Session-only; auto-expires after 7 days. Job id `3dec0191`.

**Wave 3 selection rule.** Pick the largest set of TASK.md items whose
file scopes are pairwise disjoint (or contention-free in practice) and
whose declared deps are met. Sonnet agents, 1 item each, commit on green
gates. The loop heartbeat checks for landed waves and dispatches the
next file-disjoint set.

**Wave 3 items.**

1. **5.14.B1** — Hoist `Hll` + `TDigest` types out of `basin-engine` into
   a new `basin-sketch` crate, add `hll_sketches` / `tdigest_sketches`
   fields to `DataFileRef`. Refactor: B2 already shipped the sketch
   write path inline; B1 makes the dependency direction
   storage→sketch + engine→sketch instead of storage→engine.
2. **5.14.B5** — 1M-row differential test that asserts sketch-merge
   results are within bounds across every file count 1..100. Tests-only
   scope, isolated.
3. **5.14.C3** — Read-merge path. `basin-hottier/src/merge.rs` (new) +
   `fast_select.rs` (point-probe memtable first) + `executor.rs` (full-scan
   merge). Also wire INSERT-SELECT and DEFAULT VALUES into the memtable
   (deferred from C2 per its post-merge note).
4. **5.14.C5** — Per-project memory budget (`basin-hottier/src/budget.rs`
   new). Semaphore back-pressure on hard cap; largest-first global flush
   scheduler. Lifts the per-project hard cap from C2's existing
   project-level enforcement into a first-class budget module.
5. **5.14.D4** — Multi-sort + catalog-aware WindowExec differential
   (`tests/integration/tests/catalog_window.rs` new). Tests-only.

**Why not 5.16.A in Wave 3.** Touches `executor.rs` for the plan-shape
hash hook — would race C3's executor.rs edits. Deferred to Wave 4.

**Why not 5.14.B5 differential before B1.** B5 only needs the
end-to-end sketch path, which B3+B4 already shipped inline. Independent
of B1's hoist.

**File-conflict accepted.** C3 + C5 both touch `crates/basin-hottier/`
but separate files (`merge.rs` vs `budget.rs`); `cargo` builds tolerate
this. C5 also touches `basin-engine/src/lib.rs` for config plumbing —
small, easy to rebase if conflict.

**Deviation to flag on next pass.** ADR 0020 §6 (WAL transaction
markers) currently specifies an explicit `Commit` variant; the C2 agent
followed the actual WAL impl (implicit-commit-on-clean-shutdown, no
explicit `Commit` variant). Reconciliation = either update ADR 0020 to
match impl, or add the variant + emit it. Queueing for Wave 4 alongside
5.16.A.

**Artifacts policy.** Agents must not write to `.claude/`. The autonomous
loop persists nothing to disk except commits + this decisions.md.
