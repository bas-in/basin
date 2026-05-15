# Contributing to Basin

Thanks for considering a contribution. Basin is **pre-alpha** — the
public API still moves between minor versions and we're optimising for
correctness first, ergonomics second.

## Before you open a PR

1. **Read [`CAPABILITIES.md`](./CAPABILITIES.md)** — the public-facing
   description of what Basin does today, what's planned, and what's
   explicitly out of scope. If your PR conflicts with a row in there,
   open an issue first.

2. **Read [`docs/decisions/`](./docs/decisions/)** — every "no" we've
   committed to is documented as an ADR with the trigger that would
   change our mind. Especially [ADR 0002](./docs/decisions/0002-no-postgres-extensions.md)
   (no upstream PG extensions) and [ADR 0012](./docs/decisions/0012-change-event-primitive.md)
   (no PL/pgSQL).

3. **Look for an existing issue.** If you're fixing a bug or adding a
   small feature, link the issue. For larger work, open an issue first
   so we can align on the approach.

## Local development

```sh
# Build + test the workspace
cargo build --workspace --all-targets
cargo test --workspace --no-fail-fast

# Format + lint
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings

# Postgres-backed catalog tests skip if PG is unreachable; for full
# coverage point at a local PG via:
DATABASE_URL=postgres://basin:basin@localhost:5432/basin cargo test -p basin-catalog
```

CI runs the same on push and PR — see [`.github/workflows/ci.yml`](./.github/workflows/ci.yml).

## Code style

- **Per-project cost discipline is load-bearing.** Anything that scales
  with the number of projects in memory (`HashMap<ProjectId, _>` at the
  top level, per-project background tasks, per-project connection pools)
  needs an explicit justification. The standard pattern is a single
  shared resource keyed on project ID, with cheap per-project primitives
  (atomic counters, sequence numbers, semaphores).

- **Scope discipline.** Bug fixes don't need surrounding cleanup; feature
  PRs don't need design for hypothetical future requirements. Three
  similar lines is better than a premature abstraction.

- **No comments that reference the current task / PR / issue number.**
  Those belong in the commit message and PR description; comments rot
  as the code evolves.

- **No backwards-compat shims for unreleased code.** If you can change
  the code, change it.

- **rustfmt + clippy clean** before pushing.

## Pull request process

1. Fork the repo, branch from `main`.
2. Make your changes. Keep commits focused; squash trivial fix-up
   commits before submitting.
3. Add tests. New features need at least one integration test that
   exercises the public surface.
4. Update `CAPABILITIES.md` if you're adding a user-visible feature or
   changing one. Update `CHANGELOG.md` under the `[Unreleased]` section.
5. Open a PR with a clear description of:
   - What changed
   - Why (link the issue or design doc)
   - How to test it
   - Any user-visible behaviour changes
6. Address review feedback. Don't force-push over reviewers' inline
   comments without rebasing.

## Commit messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/)
shape for the subject line:

```
<type>(<scope>): <description>

<body>
```

Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `ci`, `chore`.

Scopes are usually crate names (`engine`, `catalog`, `router`, etc) or
the area touched. Body explains the *why*, not the *what* — the diff
shows the what.

Example:

```
feat(engine): rewrite ORDER BY <-> LIMIT to vector_search fast path

Previously every vector ORDER BY went through the brute-force scan even
when an HNSW sidecar existed. The new planner detects the shape and
routes to Storage::vector_search; falls back to brute-force on JOINs,
unbounded LIMIT, etc.

5.6× speedup on the 1K-row debug-build benchmark.
```

## Reporting security issues

**Do NOT open a public issue for security vulnerabilities.** Email
security@basin.dev (or whatever the repo's security contact is) with:

- A description of the issue
- Steps to reproduce
- Affected versions
- Your suggested fix if you have one

We aim to triage within 48 hours and ship a patch within 14 days.

## Releasing

See [`RELEASING.md`](./RELEASING.md).

## Code of Conduct

Be excellent to each other. Disagreements about technical decisions are
welcome; ad hominem attacks are not. The project lead reserves the right
to remove contributors who consistently violate this norm.
