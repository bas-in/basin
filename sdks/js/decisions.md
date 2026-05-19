# basin-js — Decisions

Append-only log of non-obvious choices. Newest at top. Each entry has the
date the decision was made, what the alternatives were, and the reasoning
that would let someone re-litigate it later.

---

## 2026-05-19 — Autonomous v0.2 execution: 2 sonnet agents in worktrees, 15-min loop, 4-hour window

**Choice:** Run forward-task execution autonomously: launch up to 2 Sonnet
worker agents in parallel, each isolated in a git worktree, picking tasks
off TASKS.md by priority. A 15-min wake-up timer fires until 4 hours have
elapsed, with each tick checking on running agents and dispatching new ones
as slots free up.

**Alternatives considered:**
- Opus workers — rejected. Throughput cost is too high for tasks this
  well-scoped. Sonnet handles 30–60 min agent-sized work cleanly.
- 4 parallel agents — rejected. File conflicts in `src/postgrest/builder.ts`
  alone would force serialisation; 2 is the realistic concurrency.
- Single-agent serial execution — rejected. Too slow given the task list
  has many independent surface areas (admin scaffold, postgrest streaming,
  openapi codegen all touch different files).
- No isolation (shared working tree) — rejected. Two agents writing to
  overlapping files would clobber each other. Worktrees give each a clean
  branch.

**Why this matters:** Agents are not aware of each other. Conflict
avoidance lives in (a) the task-to-file mapping in TASKS.md, (b) worktree
isolation, and (c) the loop ticker picking pairs that don't share files.

---

## 2026-05-19 — Overwrite TASKS.md rather than append

**Choice:** Replace TASKS.md entirely with a forward-looking v0.2+ task
list. The v0.1 completion log (54 entries with detailed completion notes)
is preserved in git history (see `git log -- TASKS.md` around 2026-05-11).

**Alternatives considered:**
- Rename existing to `TASKS-v0.1.md`, new TASKS.md for forward work —
  rejected on user direction (explicit "overwrite").
- Append a new section to existing TASKS.md — rejected. The file would
  grow long and mix "what we did" with "what we plan to do," which slows
  down agents that scan the file to pick their next task.

**Why this matters:** Sonnet agents reading TASKS.md should be able to
pick a task and start immediately, without reading 100+ lines of
completion history to find the next pending item.

---

## 2026-05-19 — NDJSON auto-streaming is a bug fix, prioritised above everything else in 0.2

**Choice:** T-001 (detect `application/x-ndjson` in `then()` and parse
line-by-line) is the highest-priority v0.2 task. It runs before
`.stream()`, `.paginate()`, OpenAPI codegen, admin namespace, all of it.

**Alternatives considered:**
- Treat NDJSON support as a new feature behind `.stream()` and document
  large-result quirks — rejected. The basin engine auto-promotes any
  response > 1 MiB or > 10,000 rows to NDJSON, even when the caller never
  asked for streaming. That means today's `.select()` silently breaks on
  large queries: the SDK tries to parse the NDJSON body as JSON and
  surfaces a confusing parse error.
- Cap response size at the SDK layer — rejected. The engine threshold
  exists for memory reasons; subverting it client-side risks OOM in the
  engine.

**Why this matters:** Anyone running a basin app with >10K rows in a
table is already hitting this. Fix-first ordering protects the
production users we don't have yet.

**Reference:** `basin/crates/basin-rest/src/lib.rs:50-55` for the engine's
auto-promotion documentation.

---

## 2026-05-19 — Three pillars of v0.2: streaming, OpenAPI types, admin

**Choice:** v0.2's hero features are the three things this SDK can do that
no PostgREST-style client offers natively:

1. Cursor pagination + NDJSON streaming (T-001, T-002, T-003, T-004)
2. OpenAPI introspection → generated types (T-005, T-006)
3. `basin.admin.*` namespace for project provisioning (T-007–T-010)

**Alternatives considered:**
- Lead with OAuth + storage + realtime — these are the visibly-missing
  features compared to other BaaS SDKs, so leading with them is the
  "obvious" play. Rejected because they're all blocked on engine v0.2
  routes. Doing them first means the SDK ships nothing new while it
  waits for the engine. The three pillars above need zero engine
  changes.
- Lead with retry/backoff / SSR / DX polish — rejected. These are
  refinements; they don't change what someone can build with the SDK.

**Why this matters:** Streaming + OpenAPI types + admin are what makes
basin-js feel like a basin client rather than a generic REST wrapper.
They're what someone evaluating basin would notice first.

---

## 2026-05-19 — SDK calls basin-engine directly, not basin-cloud

**Choice:** All SDK methods hit basin-engine's REST surface directly
(`/auth/v1/*`, `/rest/v1/*`, `/admin/v1/*`). basin-cloud (the Go SaaS
layer) is not in the request path for SDK calls.

**Alternatives considered:**
- Route through basin-cloud for auth / sessions / tenancy — rejected.
  Cloud is a thin proxy to basin-auth (Rust) post-migration; the extra
  hop adds latency for no functional gain. The engine itself is the
  source of truth for auth state.
- Mixed model (some calls direct, some via cloud) — rejected as
  complexity sink.

**Why this matters:** Anyone running self-hosted basin (no cloud at
all) gets the full SDK. The cloud SaaS adds dashboard / billing /
provisioning, not SDK functionality.

---

## (Historical entries below are placeholders for past v0.1 decisions)

Future decisions should append to the top of this file. Format:
`## YYYY-MM-DD — One-line summary` followed by Choice / Alternatives /
Why-it-matters.
