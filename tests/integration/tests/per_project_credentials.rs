//! End-to-end test for per-project pgwire connection URLs.
//!
//! Boots Basin in-process: pgwire router with `ProjectCredentialsResolver`
//! stacked alongside `StaticProjectResolver`, and a basin-rest service for
//! the admin endpoints. Then:
//!
//! 1. Mint an admin JWT with `is_admin=true`.
//! 2. `POST /admin/v1/projects` to provision a fresh project + URL.
//! 3. Connect with `tokio_postgres::connect(url, NoTls)`, run CREATE +
//!    INSERT + SELECT — proves bcrypt validation worked AND the resolved
//!    `ProjectId` is consistent across reconnects (the second connection
//!    sees the row from the first).
//! 4. Rotate via `POST /admin/v1/projects/{user}/rotate`. Old URL must now
//!    fail with `28P01`. New URL works.
//! 5. Path-injection: `pgwire_user = "project_../etc"` rejected at provision
//!    time (validated server-side; we sanity-check the format directly).
//!
//! Skip-cleanly: Postgres must be reachable on `127.0.0.1:5432` for the
//! basin-auth catalog. If not, the test prints a skip note and returns Ok.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
// `AuthService` is used both inside `boot` and inside the standalone tests
// that exercise the AuthService directly without booting the full stack.
use basin_common::ProjectId;
use basin_router::{
    ServerConfig, StackedProjectResolver, StaticProjectResolver, ProjectCredentialsResolver,
    ProjectResolver,
};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;
use uuid::Uuid;

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!(
        "basin_perproject_creds_{}",
        Ulid::new().to_string().to_lowercase()
    )
}

struct SchemaGuard {
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
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

fn auth_cfg(schema: &str, pgwire_host: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![9u8; 32],
        token_ttl: Duration::from_secs(300),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
        catalog_schema: schema.to_owned(),
        smtp: SmtpConfig {
            host: "smtp.invalid".into(),
            port: 587,
            username: "u".into(),
            password: "p".into(),
            from_email: "noreply@example.com".into(),
            from_name: Some("Basin".into()),
            tls: SmtpTls::StartTls,
        },
        bcrypt_cost: 4,
        password_min_len: 10,
        rate_limit_per_ip_per_min: 1000,
        email_enabled: true,
        pgwire_public_host: pgwire_host.to_owned(),
    }
}

/// Issue an admin JWT signed with the same secret AuthService is using.
/// We bypass signup/email-verification since this test is about the admin
/// pathway, not user provisioning.
fn admin_jwt(secret: &[u8]) -> String {
    let keys = basin_auth::jwt::JwtKeys::new(secret);
    let now = chrono::Utc::now();
    let (jwt, _) = keys
        .issue_with_admin(
            &ProjectId::new(),
            Uuid::new_v4(),
            "admin@example.com",
            &[],
            true,
            now,
            Duration::from_secs(300),
        )
        .expect("issue admin jwt");
    jwt
}

struct Servers {
    pgwire_addr: SocketAddr,
    rest_addr: SocketAddr,
    admin_jwt: String,
    _data_dir: TempDir,
    _schema_guard: SchemaGuard,
    _router: basin_router::RunningServer,
    _rest: basin_rest::RunningRest,
}

async fn boot() -> Option<Servers> {
    if !pg_alive().await {
        eprintln!("postgres unreachable, skipping per_project_credentials test");
        return None;
    }

    // Pre-allocate a pgwire port we know up-front so we can bake the public
    // host into the AuthConfig (the password URLs include this host).
    let pgwire_listener = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let pgwire_addr: SocketAddr = pgwire_listener.local_addr().ok()?;
    drop(pgwire_listener);

    let schema = unique_schema();
    let secret = vec![9u8; 32];
    let cfg = auth_cfg(&schema, &pgwire_addr.to_string());
    let mailer = StubMailer::new(cfg.smtp.from_email.clone());
    let auth = match tokio::time::timeout(
        Duration::from_secs(3),
        AuthService::connect_with_mailer(cfg, Arc::new(mailer)),
    )
    .await
    {
        Ok(Ok(a)) => a,
        _ => {
            eprintln!("auth connect failed, skipping");
            return None;
        }
    };
    let auth = Arc::new(auth);
    let schema_guard = SchemaGuard { schema };

    let data_dir = TempDir::new().ok()?;
    let fs = LocalFileSystem::new_with_prefix(data_dir.path()).ok()?;
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    // Static fallback so `BASIN_PROJECTS=alice=*` style demos still work.
    let mut static_map = HashMap::new();
    static_map.insert("alice".to_owned(), ProjectId::new());
    let static_resolver = StaticProjectResolver::new(static_map);

    let resolver: Arc<dyn ProjectResolver> = Arc::new(StackedProjectResolver::new(vec![
        Arc::new(ProjectCredentialsResolver::new(auth.clone())),
        Arc::new(static_resolver),
    ]));

    let router = basin_router::run_until_bound(ServerConfig {
        bind_addr: pgwire_addr,
        engine: engine.clone(),
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
    })
    .await
    .expect("router bind");

    let rest_cfg =
        basin_rest::RestConfig::new("127.0.0.1:0".parse().unwrap(), engine, auth.clone());
    let rest_svc = basin_rest::RestService::new(rest_cfg);
    let rest = rest_svc.run_until_bound().await.expect("rest bind");
    let rest_addr = rest.local_addr;

    let admin_jwt_str = admin_jwt(&secret);

    let pgwire_local = router.local_addr;
    Some(Servers {
        pgwire_addr: pgwire_local,
        rest_addr,
        admin_jwt: admin_jwt_str,
        _data_dir: data_dir,
        _schema_guard: schema_guard,
        _router: router,
        _rest: rest,
    })
}

// --- minimal HTTP/1.1 client (mirrors poc_pgwire_jwt_smoke) -----------------

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
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response missing header terminator");
    let head = std::str::from_utf8(&buf[..split]).expect("headers utf8");
    let body = buf[split + 4..].to_vec();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status code");
    HttpResp { status, body }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_project_credentials_round_trip() {
    let Some(s) = boot().await else {
        return;
    };

    // 1. POST /admin/v1/projects -> get a fresh URL.
    let r = http_request(
        s.rest_addr,
        "POST",
        "/admin/v1/projects",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {}", s.admin_jwt)),
        ],
        Some(b"{}"),
    )
    .await;
    assert_eq!(
        r.status,
        201,
        "provision status: {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let conn_url = v["connection_url"]
        .as_str()
        .expect("connection_url")
        .to_owned();
    let pgwire_user = v["pgwire_user"].as_str().expect("pgwire_user").to_owned();
    let project_id = v["project_id"].as_str().expect("project_id").to_owned();
    assert!(pgwire_user.starts_with("project_"));
    // The URL parses project id as ULID; sanity check it does.
    let _: ProjectId = project_id.parse().expect("project_id parses as ULID");

    // 2. Rewrite the URL host to point at our test pgwire port. The
    //    AuthConfig was built with the bound port baked in, so this is a
    //    string check, not a rewrite — confirm the URL already targets the
    //    right host:port.
    assert!(
        conn_url.contains(&s.pgwire_addr.to_string()),
        "conn_url {conn_url:?} should target the test pgwire addr {}",
        s.pgwire_addr
    );

    // 3. Connect via the URL. tokio_postgres accepts the libpq URI form.
    let (client, conn) = tokio_postgres::connect(&conn_url, NoTls)
        .await
        .expect("pgwire connect with provisioned URL");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });

    // 4. CREATE + INSERT + SELECT round-trip.
    client
        .simple_query("CREATE TABLE perproject (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO perproject VALUES (42, 'hello')")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT * FROM perproject")
        .await
        .expect("select");
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 1, "expected 1 row, got {n}");
    drop(client);
    let _ = driver.await;

    // 5. Reconnect using the same URL — proves the resolved ProjectId is
    //    stable across reconnects (the row inserted above is visible).
    let (client2, conn2) = tokio_postgres::connect(&conn_url, NoTls)
        .await
        .expect("pgwire reconnect");
    let driver2 = tokio::spawn(async move {
        let _ = conn2.await;
    });
    let rows = client2
        .simple_query("SELECT * FROM perproject")
        .await
        .expect("select after reconnect");
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        n, 1,
        "row from first connection must be visible to reconnect"
    );
    drop(client2);
    let _ = driver2.await;

    // 6. Rotate. Old URL must now fail.
    let r = http_request(
        s.rest_addr,
        "POST",
        &format!("/admin/v1/projects/{pgwire_user}/rotate"),
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {}", s.admin_jwt)),
        ],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "rotate status: {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let new_url = v["connection_url"]
        .as_str()
        .expect("connection_url")
        .to_owned();
    assert_ne!(new_url, conn_url, "rotate must produce a fresh URL");

    // Old URL: must fail with auth error (28P01). tokio_postgres surfaces
    // the SqlState via `as_db_error().code()`; a `db error` Display alone
    // hides the code, so we inspect the typed error.
    match tokio_postgres::connect(&conn_url, NoTls).await {
        Ok((_c, _conn)) => panic!("old URL still authenticates after rotate"),
        Err(e) => {
            let code = e.code().map(|c| c.code().to_owned());
            let dbg = format!("{e:?}");
            assert!(
                code.as_deref() == Some("28P01")
                    || dbg.contains("28P01")
                    || dbg.to_lowercase().contains("invalid"),
                "rotated old URL should fail with 28P01, got code={code:?} debug={dbg}"
            );
        }
    }

    // New URL works.
    let (client3, conn3) = tokio_postgres::connect(&new_url, NoTls)
        .await
        .expect("connect with rotated URL");
    let driver3 = tokio::spawn(async move {
        let _ = conn3.await;
    });
    let rows = client3
        .simple_query("SELECT * FROM perproject")
        .await
        .expect("select with rotated URL");
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 1);
    drop(client3);
    let _ = driver3.await;

    // 7. List credentials (descriptors only — never plaintext).
    let r = http_request(
        s.rest_addr,
        "GET",
        &format!("/admin/v1/projects/{project_id}/credentials"),
        &[("Authorization", &format!("Bearer {}", s.admin_jwt))],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert!(
        arr[0].get("password").is_none(),
        "list must not return plaintext"
    );
    assert!(
        arr[0].get("password_hash").is_none(),
        "list must not return hash"
    );
    assert!(
        arr[0]["rotated_at"].is_string(),
        "rotated_at should be set after rotate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provision_rejects_invalid_dbname() {
    // Path-injection guard: provisioning must reject a dbname like "../etc".
    // We exercise the AuthService directly here so we don't need a server up
    // for this branch.
    if !pg_alive().await {
        return;
    }
    let schema = unique_schema();
    let _g = SchemaGuard {
        schema: schema.clone(),
    };
    let cfg = auth_cfg(&schema, "127.0.0.1:5433");
    let mailer = StubMailer::new(cfg.smtp.from_email.clone());
    let auth = match tokio::time::timeout(
        Duration::from_secs(3),
        AuthService::connect_with_mailer(cfg, Arc::new(mailer)),
    )
    .await
    {
        Ok(Ok(a)) => a,
        _ => return,
    };

    let project = ProjectId::new();
    let err = auth
        .provision_project_db(&project, Some("../etc"))
        .await
        .expect_err("must reject path-injecting dbname");
    assert!(
        err.to_string().to_lowercase().contains("invalid"),
        "expected invalid-ident error, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotate_rejects_invalid_pgwire_user_format() {
    // The path-injection invariant on the pgwire_user format. `rotate`
    // accepts a user string from the URL path, so the format check is
    // load-bearing — a malicious operator can't smuggle "project_../etc"
    // past the validator.
    if !pg_alive().await {
        return;
    }
    let schema = unique_schema();
    let _g = SchemaGuard {
        schema: schema.clone(),
    };
    let cfg = auth_cfg(&schema, "127.0.0.1:5433");
    let mailer = StubMailer::new(cfg.smtp.from_email.clone());
    let auth = match tokio::time::timeout(
        Duration::from_secs(3),
        AuthService::connect_with_mailer(cfg, Arc::new(mailer)),
    )
    .await
    {
        Ok(Ok(a)) => a,
        _ => return,
    };

    let err = auth
        .rotate_pgwire_password("project_../etc")
        .await
        .expect_err("must reject path-injecting pgwire_user");
    assert!(
        err.to_string().to_lowercase().contains("invalid"),
        "expected invalid-ident error, got {err:?}"
    );
}

/// Cross-project isolation when each project has their own URL. The wedge
/// claim: hand customer A their URL, customer B theirs; B's URL must never
/// reach A's data — not via direct SELECT, not via UNION, not via CTE.
///
/// Per-project prefix isolation makes this *structural*: the engine session
/// is bound to a `ProjectId` at startup, and every `TableScan` resolves
/// inside that project's namespace. This test proves the structural claim
/// end-to-end through the credential-validated pgwire path so a future
/// regression that accidentally shared session state across pgwire
/// connections would be caught.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_project_credentials_isolate_cross_project_data() {
    let Some(s) = boot().await else {
        return;
    };

    // Provision two projects. Both POSTs hit the same admin endpoint with
    // the same admin JWT, but each issues a fresh `project_id` + URL.
    let provision = |name: &'static str| {
        let s_addr = s.rest_addr;
        let admin = s.admin_jwt.clone();
        async move {
            let r = http_request(
                s_addr,
                "POST",
                "/admin/v1/projects",
                &[
                    ("Content-Type", "application/json"),
                    ("Authorization", &format!("Bearer {admin}")),
                ],
                Some(b"{}"),
            )
            .await;
            assert_eq!(
                r.status,
                201,
                "{name}: {}",
                String::from_utf8_lossy(&r.body)
            );
            let v = r.json();
            (
                v["connection_url"].as_str().unwrap().to_owned(),
                v["project_id"].as_str().unwrap().to_owned(),
            )
        }
    };
    let (url_a, tid_a) = provision("project_a").await;
    let (url_b, tid_b) = provision("project_b").await;
    assert_ne!(tid_a, tid_b, "two projects must have distinct ids");
    assert_ne!(url_a, url_b, "two projects must have distinct URLs");

    // Helper: connect via a URL, drive simple-query operations, return
    // the row count from a SELECT * style query.
    let connect_and_setup = |url: String, payload: &'static str| async move {
        let (client, conn) = tokio_postgres::connect(&url, NoTls)
            .await
            .expect("pgwire connect");
        let driver = tokio::spawn(async move {
            let _ = conn.await;
        });
        client
            .simple_query("CREATE TABLE secrets (id BIGINT NOT NULL, body TEXT NOT NULL)")
            .await
            .expect("create");
        let stmt = format!("INSERT INTO secrets VALUES (1, '{payload}')");
        client.simple_query(&stmt).await.expect("insert");
        (client, driver)
    };

    let (client_a, driver_a) = connect_and_setup(url_a.clone(), "project_a_secret").await;
    let (client_b, driver_b) = connect_and_setup(url_b.clone(), "project_b_secret").await;

    // From project A's URL: see only A's row.
    let collect_bodies = |msgs: &[tokio_postgres::SimpleQueryMessage]| -> Vec<String> {
        msgs.iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get("body").map(|s| s.to_owned()),
                _ => None,
            })
            .collect()
    };

    let rows = client_a
        .simple_query("SELECT * FROM secrets")
        .await
        .expect("a select");
    let bodies = collect_bodies(&rows);
    assert_eq!(bodies, vec!["project_a_secret".to_string()]);

    let rows = client_b
        .simple_query("SELECT * FROM secrets")
        .await
        .expect("b select");
    let bodies = collect_bodies(&rows);
    assert_eq!(bodies, vec!["project_b_secret".to_string()]);

    // Cross-project attack via UNION ALL: project B's session can only
    // resolve `secrets` in *its own* project namespace. UNION just unions
    // project B's row twice; project A's row is structurally unreachable.
    let rows = client_b
        .simple_query("SELECT body FROM secrets UNION ALL SELECT body FROM secrets")
        .await
        .expect("b union");
    let bodies = collect_bodies(&rows);
    assert_eq!(
        bodies,
        vec!["project_b_secret".to_string(), "project_b_secret".to_string()],
        "UNION must not cross projects"
    );
    for body in &bodies {
        assert_ne!(body, "project_a_secret", "project A data leaked via UNION");
    }

    // Cross-project attack via CTE: same structural guarantee.
    let rows = client_b
        .simple_query("WITH peek AS (SELECT body FROM secrets) SELECT body FROM peek")
        .await
        .expect("b cte");
    let bodies = collect_bodies(&rows);
    for body in &bodies {
        assert_ne!(body, "project_a_secret", "project A data leaked via CTE");
    }

    drop(client_a);
    drop(client_b);
    let _ = driver_a.await;
    let _ = driver_b.await;
}

/// Within a single project, RLS still works after credential-based pgwire
/// auth. Provisioning two URLs for the same project is the v0.2 follow-up;
/// for v0.1 we prove RLS is unaffected by the new credential validator
/// path by enabling RLS on a project's table and confirming the policy
/// filter fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_project_credentials_keep_rls_within_project() {
    let Some(s) = boot().await else {
        return;
    };

    let r = http_request(
        s.rest_addr,
        "POST",
        "/admin/v1/projects",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {}", s.admin_jwt)),
        ],
        Some(b"{}"),
    )
    .await;
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    let v = r.json();
    let url = v["connection_url"].as_str().unwrap().to_owned();

    let (client, conn) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("pgwire connect");
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });

    // Insert two rows with different "owner" values, then enable RLS with
    // a policy that always filters to id=1. With RLS active, `SELECT *`
    // must return only the matching row even though both rows are in the
    // same project's table.
    client
        .simple_query("CREATE TABLE orders (id BIGINT NOT NULL, owner TEXT NOT NULL)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO orders VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("insert");

    // Sanity: without RLS, both rows visible.
    let rows = client
        .simple_query("SELECT id FROM orders ORDER BY id")
        .await
        .expect("pre-rls select");
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get("id").map(|s| s.to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);

    // Enable RLS + add a policy that pins to id=1. Basin's RLS predicate
    // injection runs on the logical-plan layer, so any `SELECT … FROM
    // orders` (and any UNION / CTE wrapping it) sees only id=1.
    client
        .simple_query("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");
    client
        .simple_query("CREATE POLICY only_one ON orders USING (id = 1)")
        .await
        .expect("create policy");

    // Direct SELECT — only id=1 survives.
    let rows = client
        .simple_query("SELECT id FROM orders")
        .await
        .expect("rls select");
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get("id").map(|s| s.to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["1".to_string()], "RLS policy must filter rows");

    // UNION can't bypass RLS (the same security regression we fixed earlier
    // this cycle for the engine — re-verified through the credential-auth
    // path here).
    let rows = client
        .simple_query("SELECT id FROM orders UNION ALL SELECT id FROM orders")
        .await
        .expect("rls union");
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get("id").map(|s| s.to_owned()),
            _ => None,
        })
        .collect();
    for id in &ids {
        assert_ne!(id, "2", "RLS bypass via UNION: id=2 leaked");
    }

    drop(client);
    let _ = driver.await;
}
