# Releasing Basin

Basin uses a tag-driven release process. Pushing a `vX.Y.Z` tag triggers
`.github/workflows/release.yml`, which builds prebuilt binaries for four
targets and creates a GitHub Release with notes pulled from
`CHANGELOG.md`.

## Versioning

Pre-1.0:

- **0.x.0** (minor) — new features, possible breaking API changes
- **0.x.y** (patch) — bug fixes, no breaking changes

Post-1.0 we move to standard SemVer.

## Cut a release

1. **Land all PRs** for the target version. CI on `main` should be green.

2. **Update CHANGELOG.md.** Move everything in `[Unreleased]` to a new
   `[X.Y.Z] - YYYY-MM-DD` section. Keep the `[Unreleased]` heading
   empty for the next cycle. Update the link refs at the bottom.

3. **Bump the workspace version** in `Cargo.toml`:

   ```toml
   [workspace.package]
   version = "0.X.Y"
   ```

   All workspace member crates use `version.workspace = true`, so this
   single edit propagates.

4. **Refresh `Cargo.lock`** with the new version:

   ```sh
   cargo update --workspace
   ```

5. **Sanity-check the build locally:**

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace --no-fail-fast
   cargo build --release -p basin-server -p basinctl
   ```

6. **Commit the version bump:**

   ```sh
   git add Cargo.toml Cargo.lock CHANGELOG.md
   git commit -m "release: vX.Y.Z"
   ```

7. **Tag and push:**

   ```sh
   git tag vX.Y.Z
   git push origin main
   git push origin vX.Y.Z
   ```

8. **Watch the release workflow.** It builds binaries for:
   - `x86_64-unknown-linux-gnu`
   - `aarch64-unknown-linux-gnu`
   - `x86_64-apple-darwin`
   - `aarch64-apple-darwin`

   Each archive includes `basin-server`, `basinctl`, README, LICENSE,
   CHANGELOG. `.tar.gz.sha256` companion files for verification.

9. **Edit the GitHub Release notes** if needed (the workflow seeds them
   from CHANGELOG.md but you can polish before announcing).

## Pre-release tags

Tags ending in `-alpha`, `-beta`, or `-rc` (e.g. `v0.2.0-alpha.1`) are
auto-marked as pre-release on GitHub. Use them for design-partner
testing before a stable cut.

## Hotfix release

For urgent bug fixes off an existing release tag:

1. Branch from the release tag: `git checkout -b hotfix/0.X.Y vX.Y-1.Z`
2. Cherry-pick or apply the fix
3. Bump to `vX.Y-1.Z+1` (e.g. `v0.1.2 → v0.1.3`)
4. Tag and push as above

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
