# Contributing to basin-cli

Thanks for considering a contribution. `basin` is a Rust binary (clap +
reqwest, one file per subcommand under `src/commands/`); the bar for
landing changes is high but the surface is tiny, so most patches read in
a few minutes.

## Filing issues

Use the GitHub issue templates under `.github/ISSUE_TEMPLATE/` when they
exist — bug reports, feature requests, and CLI-↔-cloud compatibility
problems each have their own. If none fit, open a blank issue and
include:

- `basin --version` output (version + build date + os/arch)
- the exact command that failed + the rendered error
- the cloud version (`curl https://api.basin.run/v1/version`) when the
  bug looks like an API-shape mismatch

## Pull request review

- One topic per PR. Small + focused beats sprawling.
- Cover behaviour changes with a test. The existing `*_test.rs` files
  are the template: spin an `mockito` or `httptest`-style server and
  drive the command handler directly.
- Match the existing style — terse, no narrative comments, no emojis.
- `cargo build --locked && cargo test --locked` MUST pass before
  requesting review. CI gates the same matrix (Ubuntu + macOS).

## Dependencies

The CLI uses a small, intentional set of crates (clap, reqwest, serde,
serde_json, chrono — see `Cargo.toml`). Patches that would add a new
dependency require a clear justification; cosmetic or convenience crates
get bounced. Check `cargo audit` before submitting — the CI will.

## Commit signing

Commits to `main` must be signed (GPG or SSH; either is fine — see
GitHub's `git config commit.gpgsign true` docs). The release pipeline
also signs tag artefacts; that's automated, not something contributors
need to set up.

## Support window

The CLI follows a two-minor support window against Basin Cloud: a CLI
on minor `N` is supported against cloud `N-1`, `N`, or `N+1`. Anything
outside the window emits a stderr warning but never blocks. Breaking
`/v1/*` changes on the cloud side require a one-minor `Sunset:` header
deprecation. Patches that would silently break older CLIs get bounced
back asking for a versioned shim.
