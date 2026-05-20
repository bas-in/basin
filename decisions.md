# Autonomous-loop decisions log

Entries are appended in reverse-chronological order (newest at top). Each
entry records a decision made by the autonomous-loop dev sweep that
isn't already captured in TASK.md, an ADR, or a commit message.

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
