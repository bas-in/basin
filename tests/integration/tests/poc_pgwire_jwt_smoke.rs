//! End-to-end smoke test for `BASIN_AUTH_ENABLED=1` on the *pgwire* side.
//!
//! Spawns the basin-server binary with both auth and REST enabled, then drives
//! signup + signin via REST to mint a JWT and connects to the pgwire listener
//! using that JWT as the `user` parameter. Asserts:
//!
//! 1. A pgwire connection authenticated as `user=<jwt>` can `CREATE TABLE`,
//!    `INSERT`, and `SELECT` against the JWT's tenant — proving the binary
//!    auto-mounts `JwtTenantResolver` when auth is enabled.
//! 2. A second pgwire connection as `user=alice` (the static fallback from
//!    `BASIN_TENANTS=alice=*`) also succeeds — proving the stacked resolver
//!    keeps the static map as a real fallback rather than dropping it.
//! 3. A third pgwire connection as `user=garbage_unknown` is rejected — neither
//!    JWT verification nor the static map should accept random strings.
//!
//! Skip-cleanly: every external precondition (PG reachable, basin-server
//! binary built) silently exits Ok with a one-line note. Mirrors the pattern
//! in `poc_rest_smoke.rs`.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let mut p = exe.clone();
    p.pop();
    p.pop();
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

struct ChildGuard {
    child: Option<tokio::process::Child>,
    auth_schema: String,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
        let schema = self.auth_schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(pair)) => pair,
                    _ => return,
                };
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
                drop(client);
                let _ = tokio::time::timeout(Duration::from_millis(200), driver).await;
            });
        })
        .join();
    }
}

async fn pg_alive() -> bool {
    let connect = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_postgres::connect(PG_URL, NoTls),
    )
    .await;
    match connect {
        Ok(Ok((_c, conn))) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            true
        }
        _ => false,
    }
}

async fn spawn_with_auth(
) -> Option<(ChildGuard, std::net::SocketAddr, std::net::SocketAddr, TempDir)> {
    if !pg_alive().await {
        eprintln!("postgres unreachable, skipping poc_pgwire_jwt_smoke");
        return None;
    }
    let bin = find_basin_server_binary()?;
    let data_dir = TempDir::new().ok()?;

    let pg_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let pg_addr = pg_probe.local_addr().ok()?;
    drop(pg_probe);
    let rest_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let rest_addr = rest_probe.local_addr().ok()?;
    drop(rest_probe);

    let auth_schema = format!(
        "basin_pgwire_jwt_smoke_{}",
        Ulid::new().to_string().to_lowercase()
    );

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.env("BASIN_BIND", pg_addr.to_string())
        .env("BASIN_DATA_DIR", data_dir.path())
        .env("BASIN_TENANTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("BASIN_AUTH_ENABLED", "1")
        .env("BASIN_REST_ENABLED", "1")
        .env("BASIN_REST_BIND", rest_addr.to_string())
        .env(
            "BASIN_AUTH_JWT_SECRET",
            "0011223344556677889900112233445566778899001122334455667788990011",
        )
        .env("BASIN_AUTH_SMTP_HOST", "smtp.invalid")
        .env("BASIN_AUTH_SMTP_PORT", "587")
        .env("BASIN_AUTH_SMTP_USERNAME", "u")
        .env("BASIN_AUTH_SMTP_PASSWORD", "p")
        .env("BASIN_AUTH_SMTP_FROM", "noreply@example.com")
        .env("BASIN_AUTH_SMTP_TLS", "starttls")
        .env("BASIN_AUTH_CATALOG_DSN", PG_URL)
        .env("BASIN_AUTH_CATALOG_SCHEMA", &auth_schema)
        .env("BASIN_AUTH_BCRYPT_COST", "4")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().ok()?;
    let stderr = child.stderr.take().expect("piped stderr");
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            eprintln!("basin-server: {line}");
        }
    });

    // Wait for both REST and pgwire to accept TCP connections before handing
    // back. REST is brought up before the router task spawns, so polling for
    // the router port last is the safest readiness gate.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut rest_up = false;
    while Instant::now() < deadline {
        if !rest_up {
            rest_up = TcpStream::connect(rest_addr).await.is_ok();
        }
        if rest_up && TcpStream::connect(pg_addr).await.is_ok() {
            return Some((
                ChildGuard {
                    child: Some(child),
                    auth_schema,
                },
                pg_addr,
                rest_addr,
                data_dir,
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = child.start_kill();
    None
}

// --- minimal HTTP/1.1 client over TCP (mirrors poc_rest_smoke) -------------

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
    addr: std::net::SocketAddr,
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

/// Mark every user in the auth schema as verified so signin can proceed
/// without going through the SMTP-backed verification flow. Same shortcut
/// `poc_rest_smoke.rs` takes.
async fn mark_users_verified(schema: &str) {
    let (client, conn) = tokio_postgres::connect(PG_URL, NoTls)
        .await
        .expect("pg connect");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .batch_execute(&format!(
            "UPDATE {schema}.users SET email_verified_at = now()"
        ))
        .await
        .expect("mark verified");
    drop(client);
    let _ = driver.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_pgwire_jwt_resolver_round_trip() {
    let Some((guard, pg_addr, rest_addr, _data_dir)) = spawn_with_auth().await else {
        return;
    };

    let tenant = basin_common::TenantId::new().to_string();

    // 1. signup via REST
    let body = serde_json::json!({
        "tenant_id": tenant,
        "email": "alice@example.com",
        "password": "longenoughpassword",
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signup",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        201,
        "signup status: {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    // 2. flip email_verified_at directly so signin proceeds
    mark_users_verified(&guard.auth_schema).await;

    // 3. signin to mint a JWT
    let body = serde_json::json!({
        "tenant_id": tenant,
        "email": "alice@example.com",
        "password": "longenoughpassword",
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signin",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
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
    let access = v["access_token"].as_str().expect("access_token").to_owned();

    // 4. pgwire connection with `user=<jwt>` — primary path through the
    //    stacked resolver. CREATE / INSERT / SELECT must all succeed against
    //    the JWT's tenant.
    let conn_str = format!(
        "host={} port={} user={} password=ignored",
        pg_addr.ip(),
        pg_addr.port(),
        access,
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("pgwire connect with jwt user");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .simple_query("CREATE TABLE jwt_smoke (id BIGINT NOT NULL)")
        .await
        .expect("create table via jwt-authenticated pgwire");
    client
        .simple_query("INSERT INTO jwt_smoke VALUES (1)")
        .await
        .expect("insert via jwt-authenticated pgwire");
    let rows = client
        .simple_query("SELECT * FROM jwt_smoke")
        .await
        .expect("select via jwt-authenticated pgwire");
    // simple_query returns a `Vec<SimpleQueryMessage>`; one Row + one
    // CommandComplete is the success shape we expect.
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1, "expected 1 row, got {row_count} ({rows:?})");
    drop(client);
    let _ = driver.await;

    // 5. pgwire connection with `user=alice` — static fallback. Must also
    //    succeed, proving the stacked resolver keeps `BASIN_TENANTS` as a
    //    real fallback rather than discarding it when JWT is enabled.
    let conn_str_static = format!(
        "host={} port={} user=alice password=ignored",
        pg_addr.ip(),
        pg_addr.port(),
    );
    let (client, conn) = tokio_postgres::connect(&conn_str_static, NoTls)
        .await
        .expect("pgwire connect with static user=alice");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("select 1 via static-resolved pgwire");
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1);
    drop(client);
    let _ = driver.await;

    // 6. pgwire connection with garbage username — neither resolver should
    //    accept it, and the connection must fail to authenticate.
    let conn_str_bad = format!(
        "host={} port={} user=garbage_unknown_username password=ignored",
        pg_addr.ip(),
        pg_addr.port(),
    );
    // tokio_postgres' success type isn't `Debug`, so we destructure manually
    // and panic with the success path's effect (a working query) if we ever
    // get that far.
    match tokio_postgres::connect(&conn_str_bad, NoTls).await {
        Ok((client, conn)) => {
            let driver = tokio::spawn(async move {
                let _ = conn.await;
            });
            let q = client.simple_query("SELECT 1").await;
            drop(client);
            let _ = driver.await;
            panic!(
                "connection with unknown username should be rejected; got handshake success and SELECT result {q:?}"
            );
        }
        Err(_) => { /* expected: handshake fails */ }
    }
}
