---
title: "SDK toolchain coverage and local validation status"
nav_section: meta
sidebar_position: 40
summary: "Which SDK toolchains are installed locally versus validated only in CI, and what that means for how each SDK change is reviewed."
---

# SDK toolchain coverage & local validation status

Basin ships ten client SDKs (`sdks/`). They are validated in two places: a
developer's local box and CI. **Not every SDK's toolchain is installed on every
box**, so some SDK changes are validated by *building and running their tests*
locally while others are only *reviewed by inspection* locally and must rely on
CI (or a developer with that toolchain) to actually compile and test.

This file records, honestly, which toolchains were present on the box used for
the most recent SDK work and therefore which SDK changes were *executed* vs.
*written-but-not-locally-run*. When you touch an SDK whose toolchain is listed
as **absent**, treat the change as unverified-locally and make sure CI for that
SDK is green before relying on it.

## Toolchain availability on the last SDK-work box (2026-06-14)

| SDK | Toolchain | Present locally? | Build/test runnable locally? |
|---|---|---|---|
| basin-go | Go | ✅ present | ✅ `go build` / `go test` |
| basin-js | Node + npm | ✅ present | ✅ `npm test` |
| basin-python | Python 3 | ✅ present | ✅ `pytest` |
| basin-ruby | Ruby ≥ 3.4 (+ bundler) | ✅ present (4.0.5) | ✅ `bundle exec rspec` |
| basin-rust | Cargo | ✅ present | ⚠️ shares the workspace `target/` — builds **serially** (one cargo at a time), never in parallel with other Rust work |
| basin-swift | Swift (SwiftPM) | ✅ present (6.1) | ✅ `swift build`; `swift test` available |
| basin-java | Java runtime ✅ but **Maven/Gradle absent** | ⚠️ partial | ❌ **cannot build** — no build tool installed |
| basin-dotnet | .NET SDK | ❌ absent | ❌ cannot build/test |
| basin-dart | Dart | ❌ absent | ❌ cannot build/test |
| basin-php | PHP (+ composer) | ❌ absent | ❌ cannot build/test |

## Implications

- **Validated by execution locally:** changes to **basin-go, basin-js,
  basin-python, basin-ruby, basin-swift** (and basin-rust, serially) are built
  and tested on-box.
- **Not executable locally (review-only; rely on CI):** changes to
  **basin-java, basin-dotnet, basin-dart, basin-php** are written to match the
  SDK's existing conventions but are **not compiled or run** on this box. CI (or
  a contributor with the toolchain) is the source of truth for them.
- **Feature-library gaps** are separate from toolchain gaps: e.g. Arrow IPC
  needs a viable per-language Arrow library — mature for Java/Ruby/JS/Python,
  thin-to-nonexistent for PHP/Dart/Swift. Where no usable library exists, the
  SDK documents Arrow as not-yet-available rather than shipping a broken decoder.

## CI requirement

For full confidence the CI matrix must include the **absent** toolchains above
(Java via Maven/Gradle, .NET, Dart, PHP). Until then, treat changes to those
four SDKs as needing an explicit CI pass before release.
