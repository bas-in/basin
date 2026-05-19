# Autonomous-loop decisions log

Entries are appended in reverse-chronological order (newest at top). Each
entry records a decision made by the autonomous-loop dev sweep that
isn't already captured in TASK.md, an ADR, or a commit message.

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
