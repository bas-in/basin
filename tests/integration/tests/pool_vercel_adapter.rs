//! Phase 5.27.A — `@vercel/postgres` dockerised integration placeholder.
//!
//! ## What this tests (when green)
//!
//! A Node.js application shaped like a Vercel Edge / Serverless Function
//! that uses the `@vercel/postgres` package is spun up via docker-compose.
//! 50 concurrent "invocations" each run 10 queries through the Basin pgwire
//! endpoint in transaction-mode pooling.  Assertions:
//!
//!   - 0 connection failures across all 500 queries.
//!   - p99 query latency ≤ 100 ms (measured inside the Node app).
//!   - No `cursor`/`session-state` bleed detected (the app checks for it).
//!
//! ## Red / green status
//!
//! **ALL TESTS IN THIS FILE ARE `#[ignore]`** — they require Docker and a
//! Node.js runtime available in the test environment, plus a built
//! `basin-server` binary on PATH.  These pre-conditions are never met in the
//! in-process unit-test tier.
//!
//! **Flips green at Phase 5.27.C** when:
//!   1. `TxnModePool` is wired into `basin-router` (5.27.B).
//!   2. The docker-compose service + Node app described below is added under
//!      `tests/integration/docker/vercel-postgres-app/`.
//!   3. CI gains a Docker-enabled job that runs this file explicitly.
//!
//! ## Intended `tests/integration/docker/vercel-postgres-app/` layout
//!
//! ```
//! tests/integration/docker/vercel-postgres-app/
//! ├── docker-compose.yml          # Two services: basin-server + node-app
//! │                               # basin-server: image built from workspace,
//! │                               #   BASIN_POOL_MODE=transaction,
//! │                               #   BASIN_BIND=0.0.0.0:5432
//! │                               # node-app: builds from Dockerfile below,
//! │                               #   POSTGRES_URL=postgres://alice@basin:5432/db
//! ├── Dockerfile                  # FROM node:20-alpine; COPY app/; npm ci; CMD node app.js
//! └── app/
//!     ├── package.json            # { "dependencies": { "@vercel/postgres": "^0.10" } }
//!     └── app.js                  # HTTP server: GET /bench runs 50 concurrent
//!                                 #   async invocations × 10 queries each.
//!                                 #   Returns JSON: { ok, failures, p99_ms }
//! ```
//!
//! The Rust test below launches docker-compose, waits for the node-app HTTP
//! server to become healthy, GETs `/bench`, and asserts the JSON response.
//!
//! ## How to run manually (once Docker infra exists)
//!
//! ```sh
//! cd tests/integration
//! cargo test --test pool_vercel_adapter -- --include-ignored
//! ```

#![allow(clippy::print_stdout, dead_code)]

use std::time::Duration;

// ---------------------------------------------------------------------------
// The actual test is `#[ignore]` — infrastructure not available in CI yet.
// Phase 5.27.C wires this up.
// ---------------------------------------------------------------------------

/// Spin up a docker-compose stack (basin-server + vercel-postgres Node app),
/// drive 50 concurrent invocations × 10 queries, and assert 0 failures and
/// p99 ≤ 100 ms.
///
/// **`#[ignore]`** — requires Docker, Node.js, and a built `basin-server`
/// binary.  Flips green at Phase 5.27.C once the
/// `tests/integration/docker/vercel-postgres-app/` stack is added.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker + Node.js infra; flips green at Phase 5.27.C"]
async fn vercel_postgres_50_concurrent_invocations_0_failures() {
    // Phase 5.27.C implementation sketch:
    //
    // 1. Locate docker-compose.yml at
    //    tests/integration/docker/vercel-postgres-app/docker-compose.yml
    //    (relative to CARGO_MANIFEST_DIR).
    //
    // 2. Run `docker-compose up -d` and capture the mapped host port for the
    //    node-app HTTP server (port 3000 by default).
    //
    // 3. Poll GET http://localhost:<port>/health until 200 OK or 30s timeout.
    //
    // 4. GET http://localhost:<port>/bench — the Node app runs the 50×10
    //    benchmark internally and returns:
    //      { "ok": true, "failures": 0, "p99_ms": <number> }
    //
    // 5. Assert failures == 0 and p99_ms <= 100.
    //
    // 6. Run `docker-compose down` in a Drop guard.
    //
    // Stub body so the test compiles right now:
    let _ = Duration::from_secs(30); // keep import live
    println!("vercel_postgres test: skipped — Docker infra not present (Phase 5.27.C)");
}

/// Verify that the docker-compose file and Node app source tree exist before
/// attempting the full integration run.
///
/// **`#[ignore]`** — same infra gate as the parent test; flips green at
/// Phase 5.27.C.
#[test]
#[ignore = "needs Docker + Node.js infra; flips green at Phase 5.27.C"]
fn docker_compose_stack_files_exist() {
    // Phase 5.27.C: enable after docker/ tree is committed.
    //
    // let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // let compose = manifest
    //     .join("docker")
    //     .join("vercel-postgres-app")
    //     .join("docker-compose.yml");
    // assert!(compose.exists(), "docker-compose.yml not found at {compose:?}");
    // let app_js = manifest
    //     .join("docker")
    //     .join("vercel-postgres-app")
    //     .join("app")
    //     .join("app.js");
    // assert!(app_js.exists(), "app.js not found at {app_js:?}");
    println!("docker_compose_stack_files_exist: skipped (Phase 5.27.C)");
}
