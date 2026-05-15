//! Loopback auth smoke test. **No external Postgres required.**
//!
//! This is the safety net that catches "engine boot stuck / basin-auth refused
//! pgwire / REST didn't listen / schema bootstrap broken" failures in
//! `cargo test` BEFORE the dashboard team finds them by clicking buttons.
//!
//! The flow (one boot, one assertion sweep):
//!
//! 1. Allocate a random free pgwire port and a random free REST port via
//!    `TcpListener::bind("127.0.0.1:0")`, then drop the listeners so
//!    basin-server can rebind. Tiny race window; we retry once on failure.
//! 2. Spawn `basin-server` as a subprocess with `BASIN_CATALOG=memory` and
//!    `BASIN_AUTH_CATALOG_DSN` pointed at our random pgwire port — that's
//!    the loopback path: basin-auth's catalog lives inside engine's own
//!    pgwire as the reserved `basin_auth` user.
//! 3. Wait until both ports accept TCP connections (30s ceiling, 100ms
//!    backoff).
//! 4. `POST /auth/v1/signup` → 201 + `user_id`.
//! 5. Flip the user's `email_verified_at` directly via a `basin_auth`
//!    loopback pgwire connection (the SMTP relay is a stub on
//!    `localhost:2525`).
//! 6. `POST /auth/v1/signin` → 200 + `access_token` JWT.
//! 7. Connect to pgwire as the static `test` user (from `BASIN_PROJECTS=
//!    test=...`), run `SELECT 1`, assert one row with the int value `1`.
//! 8. Connect via `basin_auth` again and read `SELECT email FROM
//!    basin_auth_users` to verify the signup row landed in the auth catalog.
//! 9. Tear down: `kill_on_drop` on the child handles cleanup; TempDir drops
//!    automatically.
//!
//! The shape mirrors `poc_pgwire_jwt_smoke.rs` (binary subprocess boot, raw
//! TCP HTTP client to avoid pulling reqwest into the integration crate's
//! dep set) but exercises the *loopback* catalog path — no external
//! Postgres precondition, and the test is the first thing that fails
//! cleanly if engine boot breaks.

#![allow(clippy::print_stdout)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

/// Crockford-base32 ULID, valid characters only (no I/L/O/U). Stable across
/// runs so a debugging operator can grep logs deterministically.
const STATIC_PROJECT_ULID: &str = "01JABBASEZTESTPROJECT0000ZZ";
const STATIC_PROJECT_USER: &str = "test";

/// Hex-encoded 32-byte JWT secret (≥32 bytes after `hex::decode`).
const JWT_SECRET_HEX: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

/// Locate the basin-server binary that `cargo build -p basin-server` (or the
/// integration test's own build-deps) produced. Mirrors the lookup pattern
/// in `poc_pgwire_jwt_smoke.rs`.
fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let mut p = exe.clone();
    p.pop(); // strip executable name
    p.pop(); // strip `deps/`
    let candidate = p.join("basin-server");
    if candidate.exists() {
        return Some(candidate);
    }
    let parent = p.parent()?;
    for prof in ["debug", "release"] {
        let c = parent.join(prof).join("basin-server");
        if c.exists() {
            return Some(c);
        }
    }
    None
}

/// Reserve a free TCP port by binding `127.0.0.1:0` then dropping the
/// listener. There is a tiny race window between `drop` and basin-server's
/// rebind; callers tolerate one retry.
fn pick_free_port() -> Option<SocketAddr> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let addr = l.local_addr().ok()?;
    drop(l);
    Some(addr)
}

struct ChildGuard {
    child: Option<tokio::process::Child>,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
    }
}

/// Spawn basin-server in loopback mode (no external PG). Returns the child
/// guard plus the bound pgwire / REST addresses, plus the TempDirs we own
/// for the duration of the test.
async fn boot() -> Option<(ChildGuard, SocketAddr, SocketAddr, TempDir, TempDir)> {
    let bin = find_basin_server_binary()?;

    // Two attempts — `127.0.0.1:0` then drop-and-rebind has a vanishing
    // probability of port collision under heavy parallel test runs. One
    // retry covers it without hiding real failures.
    for attempt in 0..2 {
        let Some(pg_addr) = pick_free_port() else {
            continue;
        };
        let Some(rest_addr) = pick_free_port() else {
            continue;
        };
        if pg_addr.port() == rest_addr.port() {
            // Same port handed out twice by the kernel after our drop. Vanishingly
            // rare but possible — retry.
            continue;
        }
        let data_dir = TempDir::new().ok()?;
        let wal_dir = TempDir::new().ok()?;
        let loopback_dsn = format!(
            "postgres://basin_auth:basin_auth@127.0.0.1:{}/basin?sslmode=disable",
            pg_addr.port()
        );

        let mut cmd = tokio::process::Command::new(&bin);
        cmd.env("BASIN_BIND", pg_addr.to_string())
            .env("BASIN_REST_BIND", rest_addr.to_string())
            .env("BASIN_DATA_DIR", data_dir.path())
            .env("BASIN_WAL_DIR", wal_dir.path())
            .env(
                "BASIN_PROJECTS",
                format!("{STATIC_PROJECT_USER}={STATIC_PROJECT_ULID}"),
            )
            .env("BASIN_CATALOG", "memory")
            .env("BASIN_AUTH_ENABLED", "1")
            .env("BASIN_REST_ENABLED", "1")
            .env("BASIN_AUTH_JWT_SECRET", JWT_SECRET_HEX)
            .env("BASIN_AUTH_CATALOG_DSN", &loopback_dsn)
            // SMTP stub. STARTTLS is fine here even though no relay is
            // listening — we don't drive the email-sending paths in this
            // test (we mark users verified directly via pgwire).
            .env("BASIN_AUTH_SMTP_HOST", "localhost")
            .env("BASIN_AUTH_SMTP_PORT", "2525")
            .env("BASIN_AUTH_SMTP_USERNAME", "test")
            .env("BASIN_AUTH_SMTP_PASSWORD", "test")
            .env("BASIN_AUTH_SMTP_FROM", "noreply@test.local")
            .env("BASIN_AUTH_SMTP_TLS", "none")
            // Bcrypt cost 4 keeps signup/signin fast in tests.
            .env("BASIN_AUTH_BCRYPT_COST", "4")
            // Pre-set admin envs for parity with the spec, even though the
            // current basin-auth does not bootstrap an admin from them — they
            // are harmless and document the future expectation.
            .env("BASIN_AUTH_ADMIN_EMAIL", "admin@test.local")
            .env("BASIN_AUTH_ADMIN_PASSWORD", "test-admin-password-12c")
            .env("RUST_LOG", "warn,basin_auth=debug,basin_router=info")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("spawn attempt {attempt} failed: {e}");
                continue;
            }
        };
        let stderr = child.stderr.take().expect("piped stderr");
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                eprintln!("basin-server: {line}");
            }
        });

        // Both listeners must accept TCP. REST is brought up *after* the
        // pgwire router in the basin-server boot sequence, so the natural
        // gate is "REST is reachable".
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut pg_up = false;
        let mut rest_up = false;
        while Instant::now() < deadline {
            if !pg_up {
                pg_up = TcpStream::connect(pg_addr).await.is_ok();
            }
            if !rest_up {
                rest_up = TcpStream::connect(rest_addr).await.is_ok();
            }
            if pg_up && rest_up {
                return Some((
                    ChildGuard { child: Some(child) },
                    pg_addr,
                    rest_addr,
                    data_dir,
                    wal_dir,
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Bring-up timed out — clean up and either retry or fail.
        let _ = child.start_kill();
        if attempt == 0 {
            eprintln!("attempt {attempt}: bring-up timed out, retrying");
            continue;
        }
        eprintln!("auth_loopback_smoke: basin-server failed to bind both ports within 30s");
        return None;
    }
    None
}

// --- minimal HTTP/1.1 client (mirrors poc_pgwire_jwt_smoke.rs) ---------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

impl HttpResp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body is not JSON ({e}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> HttpResp {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    sock.write_all(req.as_bytes()).await.expect("write head");
    if let Some(b) = body {
        sock.write_all(b).await.expect("write body");
    }
    sock.flush().await.expect("flush");

    let mut buf = Vec::with_capacity(4096);
    sock.read_to_end(&mut buf).await.expect("read");
    parse_response(&buf)
}

fn parse_response(buf: &[u8]) -> HttpResp {
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response missing header terminator");
    let head = &buf[..split];
    let body = &buf[split + 4..];

    let head_str = std::str::from_utf8(head).expect("headers utf8");
    let mut lines = head_str.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status u16");

    let mut chunked = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.trim().eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            }
        }
    }

    let body = if chunked {
        decode_chunked(body)
    } else {
        body.to_vec()
    };
    HttpResp { status, body }
}

fn decode_chunked(mut buf: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let nl = match buf.windows(2).position(|w| w == b"\r\n") {
            Some(p) => p,
            None => return out,
        };
        let size_str = std::str::from_utf8(&buf[..nl]).expect("chunk size utf8");
        let size_str = size_str.split(';').next().unwrap().trim();
        let size = usize::from_str_radix(size_str, 16).expect("chunk size hex");
        buf = &buf[nl + 2..];
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&buf[..size]);
        buf = &buf[size + 2..];
    }
}

/// Open a pgwire connection as the reserved `basin_auth` user. This goes
/// through the auto-injected static-resolver entry (basin-server registers
/// `basin_auth -> INTERNAL_AUTH_PROJECT_ID` at startup) and lands in the
/// system project's namespace — exactly where basin-auth's `users` /
/// `refresh_tokens` / etc. tables live.
async fn connect_as_basin_auth(
    pg_addr: SocketAddr,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=basin_auth password=basin_auth dbname=basin",
        pg_addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("pgwire connect as basin_auth");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    (client, driver)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auth_loopback_signup_signin_pgwire_smoke() {
    let started = Instant::now();
    let Some((_guard, pg_addr, rest_addr, _data_dir, _wal_dir)) = boot().await else {
        // Skip cleanly so a missing basin-server binary doesn't make CI red.
        // The VERIFY block in the task description builds basin-server first
        // for exactly this reason.
        eprintln!("[skip] auth_loopback_smoke: basin-server binary missing or failed to boot");
        return;
    };

    // 1. SIGNUP --------------------------------------------------------------
    // The basin-rest signup endpoint requires `project_id` in the body —
    // basin-auth's user table is per-project. We use the same ULID we baked
    // into BASIN_PROJECTS so the static-resolver path can find it later from
    // the pgwire side.
    let email = "alice@example.com";
    let password = "hunter2!hunter2!";
    let signup_body = serde_json::json!({
        "project_id": STATIC_PROJECT_ULID,
        "email": email,
        "password": password,
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signup",
        &[("Content-Type", "application/json")],
        Some(signup_body.as_bytes()),
    )
    .await;
    if !(r.status == 200 || r.status == 201) {
        // Drain server logs before panicking so the diagnosis isn't blank.
        tokio::time::sleep(Duration::from_millis(300)).await;
        panic!(
            "signup status: {} body: {}",
            r.status,
            String::from_utf8_lossy(&r.body)
        );
    }
    let v = r.json();
    assert_eq!(v["ok"], true, "signup must return ok=true: {v}");
    let user_id = v["user_id"]
        .as_str()
        .expect("signup must return user_id (UUID)")
        .to_owned();
    // Sanity: looks like a UUID (8-4-4-4-12 hex with dashes).
    assert_eq!(
        user_id.len(),
        36,
        "user_id should be UUID-shaped: {user_id}"
    );

    // 2. MARK EMAIL VERIFIED -----------------------------------------------
    // Signin in basin-auth requires a verified email. The SMTP relay is a
    // stub; rather than spin up a fake mailbox, flip the column directly
    // through the *same* loopback pgwire path the engine handed basin-auth.
    // This doubles as proof that the auth catalog is reachable.
    let (client, driver) = connect_as_basin_auth(pg_addr).await;
    // Table is `basin_auth_users` (the schema string is used as a TABLE
    // PREFIX, not a Postgres schema namespace — basin engine's pgwire does
    // not currently accept schema-qualified identifiers like
    // `basin_auth.users`).
    let n = client
        .execute(
            "UPDATE basin_auth_users SET email_verified_at = now() WHERE email = $1",
            &[&email],
        )
        .await
        .expect("update email_verified_at via loopback pgwire");
    assert_eq!(
        n, 1,
        "expected exactly one user row to mark verified, got {n}"
    );
    drop(client);
    let _ = driver.await;

    // 3. SIGNIN -------------------------------------------------------------
    let signin_body = serde_json::json!({
        "project_id": STATIC_PROJECT_ULID,
        "email": email,
        "password": password,
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signin",
        &[("Content-Type", "application/json")],
        Some(signin_body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "signin status: {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let access = v["access_token"]
        .as_str()
        .expect("signin must return access_token")
        .to_owned();
    let refresh = v["refresh_token"]
        .as_str()
        .expect("signin must return refresh_token")
        .to_owned();
    assert!(!access.is_empty(), "access_token must be non-empty");
    assert!(!refresh.is_empty(), "refresh_token must be non-empty");
    // JWTs are three base64url segments split by `.` — cheap shape check.
    assert_eq!(
        access.matches('.').count(),
        2,
        "access_token should be a JWT (header.payload.signature): {access}"
    );

    // 4. STATIC PASSWORDLESS PGWIRE IS BLOCKED ------------------------------
    // Once auth is enabled, BASIN_PROJECTS is only a bootstrap/internal
    // resolver. Customer pgwire connections must use provisioned per-project
    // credentials; accepting any password here would be the old bug.
    let static_conn = format!(
        "host=127.0.0.1 port={} user={} password=ignored dbname=basin",
        pg_addr.port(),
        STATIC_PROJECT_USER
    );
    assert!(
        tokio_postgres::connect(&static_conn, NoTls).await.is_err(),
        "static BASIN_PROJECTS user must not accept an arbitrary password when auth is enabled"
    );

    // 5. PGWIRE SELECT 1 AS PROVISIONED PROJECT ------------------------------
    // Provision a project credential row directly through the auth catalog,
    // then prove pgwire startup bcrypt-validates that password.
    let pgwire_user = "project_deadbeef";
    let pgwire_password = "project-password-12345";
    let pgwire_hash = bcrypt::hash(pgwire_password, 4).expect("bcrypt project pgwire password");
    let (client, driver) = connect_as_basin_auth(pg_addr).await;
    let n = client
        .execute(
            "INSERT INTO basin_auth_auth_project_credentials \
               (project_id, pgwire_user, password_hash, dbname) \
             VALUES ($1, $2, $3, 'basin')",
            &[&STATIC_PROJECT_ULID, &pgwire_user, &pgwire_hash],
        )
        .await
        .expect("insert provisioned pgwire credential");
    assert_eq!(n, 1, "expected one project credential row");
    drop(client);
    let _ = driver.await;

    let project_conn = format!(
        "host=127.0.0.1 port={} user={} password={} dbname=basin",
        pg_addr.port(),
        pgwire_user,
        pgwire_password
    );
    let (client, driver) = tokio_postgres::connect(&project_conn, NoTls)
        .await
        .expect("pgwire connect with provisioned project credentials");
    let driver = tokio::spawn(async move {
        let _ = driver.await;
    });
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 via static pgwire");
    let mut got: Vec<String> = Vec::new();
    for m in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            // simple_query exposes column 0 as a string irrespective of type.
            if let Some(v) = r.get(0) {
                got.push(v.to_owned());
            }
        }
    }
    assert_eq!(
        got,
        vec!["1".to_string()],
        "SELECT 1 should return exactly one row with value 1, got {got:?}"
    );
    drop(client);
    let _ = driver.await;

    // 6. AUTH CATALOG INTROSPECTION ----------------------------------------
    // Read the signup row back through loopback pgwire as `basin_auth`.
    // Catches "schema bootstrap broken" / "auth catalog landed in the wrong
    // project namespace" regressions that would otherwise only surface
    // mid-dashboard.
    let (client, driver) = connect_as_basin_auth(pg_addr).await;
    let rows = client
        .query(
            "SELECT email FROM basin_auth_users WHERE email = $1",
            &[&email],
        )
        .await
        .expect("select from basin_auth_users via loopback pgwire");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one users row for {email}, got {}",
        rows.len()
    );
    let got_email: &str = rows[0].get(0);
    assert_eq!(got_email, email);
    drop(client);
    let _ = driver.await;

    let elapsed = started.elapsed();
    eprintln!(
        "auth_loopback_smoke completed in {:.2}s",
        elapsed.as_secs_f64()
    );
    assert!(
        elapsed < Duration::from_secs(60),
        "test ran too long: {elapsed:?} (should be <60s; we cap fast-feedback at 30s after binary is built)"
    );
}
