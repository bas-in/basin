//! Phase 5.27.A / 5.27.C — `@vercel/postgres` dockerised integration test.
//!
//! ## What this tests (when green)
//!
//! A Node.js application shaped like a Vercel Edge / Serverless Function
//! that uses the `pg` (node-postgres) driver in Vercel-function form is spun
//! up via docker-compose.  50 concurrent "invocations" each run 10 queries
//! through the Basin pgwire endpoint in transaction-mode pooling.
//!
//! **Driver substitution:** `@vercel/postgres` was the original target but is
//! not usable against a raw pgwire endpoint — it wraps
//! `@neondatabase/serverless` which requires Neon's hosted WebSocket proxy.
//! The `pg` (node-postgres) driver is substituted; the function _shape_
//! (stateless, short-lived pool checkout) is identical.  See
//! `tests/integration/docker/vercel-postgres-app/README.md` §Driver
//! Substitution for full rationale.
//!
//! Assertions:
//!   - 0 connection failures across all 500 queries.
//!   - p99 query latency ≤ 100 ms (measured inside the Node app).
//!   - No `cursor`/`session-state` bleed (the app checks for it).
//!
//! ## Red / green status
//!
//! **ALL TESTS IN THIS FILE ARE `#[ignore]`** — they require Docker and a
//! Node.js runtime available in the test environment, plus a built
//! `basin-server` image on the local Docker daemon.  These pre-conditions are
//! never met in the in-process unit-test tier.
//!
//! To run, set `BASIN_VERCEL_E2E=1` and pass `--include-ignored`:
//!
//! ```sh
//! cd tests/integration
//! BASIN_VERCEL_E2E=1 cargo test --test pool_vercel_adapter -- --include-ignored
//! ```
//!
//! The test body is a no-op (prints a skip message) when `BASIN_VERCEL_E2E`
//! is not set, so the test still compiles and passes in normal CI even when
//! `--include-ignored` is accidentally passed.
//!
//! ## Docker infra (Phase 5.27.C)
//!
//! The docker-compose stack lives at:
//!   `tests/integration/docker/vercel-postgres-app/`
//!
//! Layout:
//! ```
//! tests/integration/docker/vercel-postgres-app/
//! ├── docker-compose.yml          Two services: basin-server + node-app
//! │                               basin-server: pre-built basin-server image
//! │                                 BASIN_POOL_MODE=transaction
//! │                                 BASIN_BIND=0.0.0.0:5432
//! │                               node-app: built from Dockerfile
//! │                                 POSTGRES_URL=postgres://basin@basin-server:5432/basin?pool_mode=transaction
//! ├── Dockerfile                  FROM node:20-alpine; npm install; CMD node app.js
//! ├── README.md                   Full rationale, driver substitution note, run guide
//! └── app/
//!     ├── package.json            { "dependencies": { "pg": "^8.13.3" } }
//!     └── app.js                  GET /health → liveness
//!                                 GET /bench  → 50×10 benchmark; returns
//!                                   { ok, failures, p99_ms }
//! ```
//!
//! ## Manual run (once Docker infra exists)
//!
//! ```sh
//! # 1. Build the Basin server image (repo root, ~5 min first time):
//! docker build -t basin-server .
//!
//! # 2. Run the e2e stack directly:
//! cd tests/integration/docker/vercel-postgres-app
//! docker compose up --build --abort-on-container-exit
//!
//! # 3. Or drive it through the Rust test:
//! cd tests/integration
//! BASIN_VERCEL_E2E=1 cargo test --test pool_vercel_adapter -- --include-ignored
//! ```

#![allow(clippy::print_stdout, dead_code)]

use std::time::Duration;

// ---------------------------------------------------------------------------
// Helper: find the docker-compose.yml relative to this crate's CARGO_MANIFEST_DIR.
// ---------------------------------------------------------------------------

fn compose_file() -> std::path::PathBuf {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .join("docker")
        .join("vercel-postgres-app")
        .join("docker-compose.yml")
}

// ---------------------------------------------------------------------------
// The actual test is `#[ignore]` — infrastructure not available in CI yet.
// Set BASIN_VERCEL_E2E=1 + pass --include-ignored to flip it green.
// ---------------------------------------------------------------------------

/// Spin up a docker-compose stack (basin-server + vercel-postgres Node app),
/// drive 50 concurrent invocations × 10 queries, and assert 0 failures and
/// p99 ≤ 100 ms.
///
/// **`#[ignore]`** — requires Docker + a pre-built `basin-server` image.
///
/// Activate with:
/// ```sh
/// BASIN_VERCEL_E2E=1 cargo test --test pool_vercel_adapter -- --include-ignored
/// ```
///
/// Without the env var the body is a no-op (test still compiles and passes).
/// With the env var it drives the full docker-compose workflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker + basin-server image; set BASIN_VERCEL_E2E=1 and pass --include-ignored"]
async fn vercel_postgres_50_concurrent_invocations_0_failures() {
    let _ = Duration::from_secs(30); // keep import live

    // ── Early-exit when the env gate is not set ────────────────────────────
    if std::env::var("BASIN_VERCEL_E2E").as_deref() != Ok("1") {
        println!(
            "vercel_postgres test: skipped — set BASIN_VERCEL_E2E=1 to run \
             (needs Docker + pre-built basin-server image)"
        );
        return;
    }

    // ── Locate compose file ────────────────────────────────────────────────
    let compose = compose_file();
    assert!(
        compose.exists(),
        "docker-compose.yml not found at {compose:?}; \
         run `docker build -t basin-server .` from the repo root first"
    );

    let compose_dir = compose.parent().unwrap();

    // ── docker compose up -d ───────────────────────────────────────────────
    let up = std::process::Command::new("docker")
        .args(["compose", "up", "--build", "-d"])
        .current_dir(compose_dir)
        .status()
        .expect("failed to launch `docker compose up`");
    assert!(up.success(), "`docker compose up` exited non-zero: {up}");

    // Ensure we always tear down, even on panic.
    struct ComposeDown(std::path::PathBuf);
    impl Drop for ComposeDown {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["compose", "down", "-v"])
                .current_dir(&self.0)
                .status();
        }
    }
    let _guard = ComposeDown(compose_dir.to_path_buf());

    // ── Poll /health until the node-app is ready ───────────────────────────
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let health_url = "http://localhost:3000/health";
    loop {
        if std::time::Instant::now() > deadline {
            panic!("node-app /health did not become 200 OK within 60 s");
        }
        let resp = std::process::Command::new("curl")
            .args(["-sf", "-o", "/dev/null", "-w", "%{http_code}", health_url])
            .output();
        if let Ok(out) = resp {
            if out.stdout == b"200" {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // ── GET /bench ─────────────────────────────────────────────────────────
    let bench_out = std::process::Command::new("curl")
        .args(["-sf", "http://localhost:3000/bench"])
        .output()
        .expect("failed to run curl against /bench");
    assert!(bench_out.status.success(), "curl /bench failed: {:?}", bench_out.status);

    let body = String::from_utf8_lossy(&bench_out.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("could not parse /bench JSON response");

    let failures = parsed["failures"].as_u64().unwrap_or(u64::MAX);
    let p99_ms = parsed["p99_ms"].as_f64().unwrap_or(f64::MAX);

    println!(
        "vercel_postgres bench result: failures={failures} p99_ms={p99_ms:.1} ok={ok}",
        ok = parsed["ok"]
    );

    assert_eq!(failures, 0, "expected 0 connection failures, got {failures}");
    assert!(
        p99_ms <= 100.0,
        "p99 latency {p99_ms:.1} ms exceeds 100 ms budget"
    );
}

/// Verify that the docker-compose file and Node app source tree exist before
/// attempting the full integration run.
///
/// **`#[ignore]`** — same infra gate as the parent test.
///
/// Activate with:
/// ```sh
/// BASIN_VERCEL_E2E=1 cargo test --test pool_vercel_adapter -- --include-ignored
/// ```
#[test]
#[ignore = "needs Docker + basin-server image; set BASIN_VERCEL_E2E=1 and pass --include-ignored"]
fn docker_compose_stack_files_exist() {
    if std::env::var("BASIN_VERCEL_E2E").as_deref() != Ok("1") {
        println!("docker_compose_stack_files_exist: skipped (set BASIN_VERCEL_E2E=1 to enable)");
        return;
    }

    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let base = manifest.join("docker").join("vercel-postgres-app");

    let compose = base.join("docker-compose.yml");
    assert!(compose.exists(), "docker-compose.yml not found at {compose:?}");

    let app_js = base.join("app").join("app.js");
    assert!(app_js.exists(), "app.js not found at {app_js:?}");

    let pkg = base.join("app").join("package.json");
    assert!(pkg.exists(), "package.json not found at {pkg:?}");

    let dockerfile = base.join("Dockerfile");
    assert!(dockerfile.exists(), "Dockerfile not found at {dockerfile:?}");

    println!("docker_compose_stack_files_exist: all files present at {base:?}");
}
