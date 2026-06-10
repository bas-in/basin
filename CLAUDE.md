# CLAUDE.md — working conventions for this repo

Conventions every Claude Code session in `basin/` must follow. These are
hard rules distilled from how this project is actually run, not suggestions.
When in doubt, prefer the conservative choice and ask.

## Attribution

- **No AI attribution anywhere.** Not in commit messages, not in code
  comments, not in PR bodies, not in docs. No "Generated with", no
  "Co-Authored-By: Claude", no "🤖". Commits and code read as if written by
  the human author.

## Git

- **New commits only.** Never `git commit --amend`, never force-push, never
  rewrite published history. Fix-ups are follow-up commits.
- **Never `--no-verify`.** Pre-commit / pre-push hooks run; do not bypass them.
- **Stage specific files.** Use `git add <path> <path>`; never `git add -A`,
  `git add .`, or `git commit -a`. You must know exactly what each commit
  contains.
- **Benchmark artifacts stay unstaged.** `benchmark/data/*.json`,
  `benchmark/data_seaweedfs/*.json`, the generated `benchmark/RESULTS_*.md`,
  `benchmark/index_*.html`, and `benchmark/<dir>/results.js` are regenerated
  outputs — leave them out of commits unless a human explicitly asks to
  refresh published numbers.
- Don't commit, push, or open PRs unless the user asks. If you must commit and
  you're on the default branch, branch first.
- Don't force-push over a reviewer's inline edits (see `CONTRIBUTING.md`).

## Generated files — never hand-edit

`benchmark/bundle.py` generates, and the next run overwrites:
- `benchmark/RESULTS_<slug>.md`
- `benchmark/index_<slug>.html`
- `benchmark/<data_dir>/results.js`

Hand-editing any of these is a no-op (clobbered on the next `bundle.py` run).
The hand-written benchmark docs are `benchmark/BENCHMARKS.md` and
`benchmark/README.md` — those you may edit. `docs/sql-support.md` is generated
from `sql_support_matrix.rs`; do not hand-edit it either.

## Source-of-truth files you must not touch

- **`sql_support_matrix.rs`** — never edit it to make the SQL matrix look
  better. It is a test-derived ground truth; changing it to mask a gap is
  forbidden.
- **`.env` and any secrets files** — do not read them. Don't echo credentials
  into the transcript or into commands.

## Hiding failures is forbidden

- **No `#[ignore]` to silence a failing test.** If a test fails, fix the code
  or the test honestly, or leave it failing and report it. `#[ignore]` is only
  legitimate for harness slices that are genuinely pending wiring and are
  documented as such — never to make a red run look green.
- Don't soften an assertion or delete a gate to get to green.

## Cargo / build discipline (shared `target/`, single-box)

- **Do not run cargo without being asked.** (Documentation/agent sessions in
  particular: do not run `cargo build` / `cargo test`.)
- **One `cargo build` / `cargo test` at a time.** The workspace shares a single
  `target/`; concurrent cargo invocations corrupt or serialize on it.
- **Worktrees are banned** — a second worktree means a second `target/`, which
  blows the disk budget on this box. Work in the main checkout.
- **1M-row timing benchmarks must run alone on an idle box.** They are
  wall-clock-sensitive; a noisy neighbour (another cargo, a build, a browser)
  invalidates the numbers. The 1M LocalFS card is the most timing-fragile —
  treat single-run 1M results as indicative, not authoritative.

## Benchmark methodology

- **Published benchmark config == default config.** The Postgres-compare cards
  run with Basin's default configuration — HTAP fastpaths and fast bulk-INSERT
  are always-on, no non-default flags. Never publish a number obtained with a
  non-default flag as if it were the default.
- Cite number provenance the way the existing docs do ("measured locally").

## Docs are part of the change (review blocker)

**Every feature/perf commit that changes user-visible behavior or published
numbers must update `README.md`, `CAPABILITIES.md`, and `CHANGELOG.md` in the
same commit — or in an immediately following docs commit.** Docs drift is a
review blocker. If you ship a capability, a SQL surface, a perf win, or a
changed benchmark number and the docs still say the old thing, the change is
not done.

Ground doc numbers in `benchmark/data/*.json` /
`benchmark/data_seaweedfs/*.json`; ground feature claims in code + tests. The
repo's tone is honest-comparative — publish wins **and** losses.
