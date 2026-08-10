---
title: "Release signing template"
nav_section: meta
sidebar_position: 12
summary: "The release-integrity contract implemented by the release workflow and scripts/verify.sh. A template to copy into a sibling repo, not a shared dependency."
---

# Release signing — the contract Basin's release job implements

> **Provenance:** this file and `scripts/verify.sh` were copied from
> [`vul-os/ephor`](https://github.com/vul-os/ephor) per its own
> `RELEASE-TEMPLATE.md`. They are a **matched pair** and a **template, not a
> shared dependency** — nothing imports them, and nothing should. Kept here so
> the contract travels with the code that implements it.

Two files. Copy both or neither; they are a matched pair, not a library.

| File | Role |
|---|---|
| `.github/workflows/release.yml` → the `release` job | Emits `SHA256SUMS` over every published asset and attests it |
| `scripts/verify.sh` | What a **user** runs before executing your bytes. Self-contained: `curl` + `sha256sum`/`shasum` |

This is deliberately a template, not a shared dependency. The suite's one written
design contract is the one nobody followed; the things that actually propagated
were copied files. Copy it.

## What to change

1. **`scripts/verify.sh`** — one line:
   ```sh
   DEFAULT_REPO="vul-os/basin"     # already set for this repo
   ```
   Nothing else is Ephor-specific. `MANIFEST="SHA256SUMS"` only changes if you
   already publish a differently-named manifest.

2. **The `release` job** — three things (all already done here):
   - `path: dist` on the `download-artifact` step, and any other steps that
     stage assets, must all write into `dist/`. **The manifest covers that
     directory**, so "published" and "covered" are the same set by construction
     rather than two lists someone keeps in sync by hand. (Basin stages into
     `dist/`, not `release/`; the emit step's `find` excludes `*.sha256`, the
     per-asset sidecars the build job also produces.)
   - `files: dist/*` on the release step — publish the directory, not a
     hand-listed set of names.
   - The release-notes verify snippet — the asset filename is built from the
     tag in the `Extract release notes` step.

3. **`.github/workflows/ci.yml`** — the `verify-script` job runs
   `verify.sh --selftest` on every push. A checksum guard that has quietly
   stopped failing is indistinguishable, from a green CI run, from one that
   works. Do not delete it to make a build green.

Required job permissions: `contents: write`, `id-token: write`,
`attestations: write`. **No secret is needed and none should be added.**

## Why attestation, not a detached signature

A detached GPG/minisign signature needs a private key: somewhere to live, an
owner, a rotation story, and a published public half users must obtain over some
channel they already trust — and if that channel is the same GitHub repo, the
signature proves nothing the TLS fetch didn't. An unrotated key nobody owns is
the normal end state.

`actions/attest-build-provenance` signs with a short-lived sigstore certificate
minted from the workflow's OIDC token. No long-lived key, no new secret, no new
hosted service. The identity it binds is *"this repo's release workflow at this
commit"* rather than *"whoever holds the key"*, which is the property you
actually wanted. Users check it with `gh attestation verify`.

It is **not load-bearing**: `verify.sh`'s digest path needs only curl and
sha256sum, and `--attest` is opt-in. If the action is removed or broken,
`SHA256SUMS` still verifies and nobody's verification silently becomes a no-op.
Correspondingly, a run *without* `--attest` prints that provenance was **not**
checked — a pass never implies more than it checked.

## The contract you are copying

`verify.sh` has two outcomes: verified, or non-zero with a distinct diagnostic.
There is no `--skip-verify` and no path where a missing `SHA256SUMS` means
"nothing to check". That case is the entire point: a verifier that shrugs at a
404 prints a line that looks like verification while checking nothing, which is
worse than no verifier because it converts *"I don't know"* into *"it's fine"*.

| Exit | Failure mode |
|---|---|
| 0 | verified |
| 2 | usage (no artifact, no `--tag`/`--base-url`, empty name, unknown flag) |
| 3 | `SHA256SUMS` unfetchable / absent |
| 4 | HTML page served where the manifest was expected |
| 5 | manifest empty or has no well-formed digest line |
| 6 | manifest has no entry for the requested artifact |
| 7 | artifact unfetchable, or HTML served where bytes were expected |
| 8 | truncated download (origin closed before its declared `Content-Length`) |
| 9 | digest mismatch |
| 10 | `curl` or a digest tool missing |
| 11 | `--attest` requested and provenance did not verify |
| 12 | plaintext non-loopback origin refused |

Prove it still holds: `bash scripts/verify.sh --selftest` (24 synthetic-origin
cases, asserts exit code **and** that a diagnostic was printed).

## Four defects not to reintroduce

All four were live in a sibling repo's installer until an adversarial audit found
them (that is the history this template exists to carry forward, not something in
this repo). If you edit `verify.sh`, re-read these; each is a one-line regression.

1. **Fall-open.** Never degrade to an unverified path when the network, CDN or
   origin misbehaves. Every fetch failure aborts.
2. **Silent death at a pipeline.** Under `set -e` + `pipefail`,
   `x="$(grep … | awk … | head -1)"` *kills the script with no message* when the
   grep matches nothing — so the "no entry found" guard written below it never
   runs. Every lookup pipeline here ends in `|| true` and its result is then
   explicitly tested for emptiness. **A guard has to be reachable to be a guard.**
3. **`\n` inside a `%s`.** `die` takes one argument per line and prints each with
   its own `printf`, message as an *argument* and never the format string. No
   filename or digest echoed into a diagnostic can be read as a format or an
   escape.
4. **Substring / regex name matching.** The artifact name is compared by `awk`
   against **field 2**, as a string. A substring grep treats the name as a regex
   — every `.` in `basin-0.1.9-x86_64-unknown-linux-gnu.tar.gz` is a wildcard — and will return
   the digest of `…tar.gz.sig`. The selftest plants that trap in a shape where a
   naive grep reports **exit 0 on an artifact nobody vouched for**.
