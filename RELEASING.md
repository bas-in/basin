# Releasing Basin

Basin uses a tag-driven release process. Pushing a `vX.Y.Z` tag triggers
`.github/workflows/release.yml`, which builds prebuilt binaries for each
supported target and creates a GitHub Release with notes pulled from
`CHANGELOG.md`.

## Versioning

Pre-1.0:

- **0.x.0** (minor) — new features, possible breaking API changes
- **0.x.y** (patch) — bug fixes, no breaking changes

Post-1.0 we move to standard SemVer.

## Cut a release

### 1. Land all PRs

CI on `main` must be green.

### 2. Update `CHANGELOG.md`

Move everything in `[Unreleased]` to a new `[X.Y.Z] - YYYY-MM-DD`
section. Keep the `[Unreleased]` heading with `_Nothing yet._`. Update
the link refs at the bottom.

Keep entries scannable — short bullets, not paragraphs. The whole
section gets rendered verbatim on the GitHub Releases page.

### 3. Bump the workspace version

In `Cargo.toml`:

```toml
[workspace.package]
version = "0.X.Y"
```

All workspace members use `version.workspace = true`, so this single
edit propagates.

### 4. Refresh `Cargo.lock`

```sh
cargo update --workspace
```

### 5. Sanity-check locally

These commands mirror what CI runs, so a green local run predicts a
green CI run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace --exclude basin-integration-tests --no-fail-fast
cargo build --release -p basin-server -p basinctl
```

Notes:

- Clippy runs in advisory mode (no `-D warnings`); only deny-by-default
  lints (e.g. `clippy::approx_constant`) fail the build.
- `basin-integration-tests` is excluded from the CI test surface —
  those `viability_*` / `s3_scaling_*` cards each link a near-full
  workspace and OOM free CI runners. Run them locally on demand:
  ```sh
  cargo test -p basin-integration-tests
  ```

### 6. Commit + tag + push

```sh
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "release: vX.Y.Z"
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

### 7. Watch the release workflow

It builds binaries for:

- `x86_64-unknown-linux-gnu` (ubuntu-latest)
- `aarch64-unknown-linux-gnu` (ubuntu-24.04-arm — native ARM runner)
- `aarch64-apple-darwin` (macos-latest)

Each archive contains `basin-server`, `basinctl`, README, LICENSE, and
CHANGELOG.

The release job then, in order:

1. **Emits `SHA256SUMS`** over every staged asset in `dist/`. It refuses to
   publish if `dist/` is empty, or if the manifest's line count does not equal
   the asset count — an under-covering manifest looks exactly like a complete
   one, so coverage is asserted rather than assumed.
2. **Verifies that manifest with `scripts/verify.sh --dir dist`** — the same
   script users run. A producer/consumer format disagreement surfaces here, in a
   red release job, rather than in a user's terminal after the release is public.
3. **Runs `scripts/verify.sh --selftest`**, proving the verifier's 24 refusal
   paths still fire on this runner before anything is published.
4. **Attaches a sigstore build-provenance attestation** over `dist/*` (including
   `SHA256SUMS`, so the attestation transitively covers every asset it names),
   signed with a short-lived certificate minted from the workflow's OIDC token.
   **No long-lived signing key exists and none should be created.**
5. **Prepends the verification snippet** to the release notes.

If any of those steps fails, nothing is published. Do not "unblock" a release by
removing a step — see `RELEASE-TEMPLATE.md` for what the contract is and why.

Per-asset `.tar.gz.sha256` sidecars are still produced by the build job and
published, but `SHA256SUMS` is the manifest verification uses. A per-asset digest
served from the same origin as the asset only proves the origin agrees with
itself.

### 8. Polish the GitHub Release

The workflow seeds release notes from the matching `[X.Y.Z]` section
in `CHANGELOG.md`. Polish on GitHub before announcing if needed —
edits there don't loop back into `CHANGELOG.md`.

## Pre-release tags

Tags ending in `-alpha`, `-beta`, or `-rc` (e.g. `v0.2.0-alpha.1`) are
auto-marked as pre-release on GitHub. Use them for design-partner
testing before a stable cut.

## Hotfix release

For an urgent fix off an existing release tag:

1. Branch from the release tag: `git checkout -b hotfix/0.X.Y vX.Y.Z-1`
2. Cherry-pick or apply the fix
3. Bump to the next patch (e.g. `v0.1.2 → v0.1.3`)
4. Tag and push as above

## Supported targets

Current matrix (3 targets):

| Target                       | Runner             | Notes                              |
| ---------------------------- | ------------------ | ---------------------------------- |
| `x86_64-unknown-linux-gnu`   | `ubuntu-latest`    | Default server target.             |
| `aarch64-unknown-linux-gnu`  | `ubuntu-24.04-arm` | Native ARM runner; no Docker.      |
| `aarch64-apple-darwin`       | `macos-latest`     | Apple Silicon. Runs under Rosetta on Intel Macs. |

`x86_64-apple-darwin` (Intel Mac) was dropped — runner-hour cost vs.
shrinking install base. Re-add if user demand surfaces.

## crates.io publish (optional)

The `release.yml` workflow has a commented-out `publish-crates` job. To
enable:

1. Generate a crates.io API token at https://crates.io/me
2. Add it as `CARGO_REGISTRY_TOKEN` GitHub repo secret
3. Uncomment the `publish-crates` job
4. Verify the publish order matches the dep graph (basin-common first,
   basin-storage / basin-catalog next, then basin-engine, then the
   downstream crates)

Library crates (basin-engine, basin-storage, basin-catalog, basin-trgm,
basin-geo, basin-cv, basin-cron, basin-net, basin-vector, basin-router,
basin-rest, basin-auth, basin-pool, basin-shard, basin-wal,
basin-webhooks, basin-iceberg-rest, basin-common) are crates.io
candidates. Service binaries (`basin-server`, `basinctl`) are
GitHub-Releases-only.
