//! Integration tests: `storage.objects` RLS — Phase 5.17.C.
//!
//! Acceptance criteria (from TASK.md §5.17.C):
//!
//! 1. Two authenticated users each upload objects to the same private bucket.
//!    With `owner = auth.uid()` policy enabled, each user sees only their own
//!    objects on list and can only download their own objects.
//! 2. Cross-project isolation: user from project-A cannot access project-B's
//!    objects even if they share a bucket name.
//! 3. Public-bucket reads bypass RLS — any authenticated user can list/download
//!    from a public bucket without an owner-match policy.
//! 4. RLS off (no `set_objects_rls_enabled(true)`) → no filtering; all objects
//!    visible to any authenticated user.
//!
//! These tests spin up a live `RestService` backed by the in-memory catalog and
//! object store.  When Postgres is unavailable we skip cleanly (same pattern as
//! `object_storage.rs`).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_blob::{ObjectPolicy, ObjectPolicyCommand, ObjectUsing};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_rest::{RestConfig, RestService, RunningRest};
use object_store::local::LocalFileSystem;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Helpers — mirrors object_storage.rs helpers exactly
// ---------------------------------------------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!("basin_rls_test_{}", Ulid::new().to_string().to_lowercase())
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
                Err(e) => {
                    eprintln!("storage_rls schema cleanup runtime: {e}");
                    return;
                }
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
                let _driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
            });
        })
        .join();
    }
}

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn auth_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![77u8; 32],
        token_ttl: Duration::from_secs(60),
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
        pgwire_public_host: "127.0.0.1:5433".into(),
    }
}

/// Spin up a REST server. Returns None if Postgres is unreachable.
async fn try_serve() -> Option<(RunningRest, RestService, AuthService, StubMailer, SchemaGuard)> {
    let schema = unique_schema();
    let cfg = auth_cfg(&schema);
    let mailer = StubMailer::new(cfg.smtp.from_email.clone());
    let auth = match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, Arc::new(mailer.clone())),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => {
            eprintln!("postgres unreachable, skipping storage_rls test: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("postgres connect timed out, skipping storage_rls test");
            return None;
        }
    };
    let dir = TempDir::new().expect("tempdir");
    let dir_path: &'static TempDir = Box::leak(Box::new(dir));
    let engine = engine_in(dir_path);
    let svc = RestService::new(RestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        auth: Arc::new(auth.clone()),
        max_body_bytes: 1 << 20,
        default_page_size: 100,
        max_page_size: 1000,
        cors_origins: Vec::new(),
        rate_limit_per_sec: 1000,
    });
    let running = svc.clone().run_until_bound().await.expect("bind");
    Some((running, svc, auth, mailer, SchemaGuard { schema }))
}

fn last_token(mailer: &StubMailer) -> String {
    let log = mailer.sent();
    let body = &log.last().expect("at least one email sent").body;
    let needle = "token=";
    let start = body.find(needle).expect("body has token=") + needle.len();
    let tail = &body[start..];
    let end = tail
        .find(|c: char| !c.is_ascii_hexdigit())
        .unwrap_or(tail.len());
    tail[..end].to_owned()
}

async fn make_user(
    auth: &AuthService,
    mailer: &StubMailer,
    project: &ProjectId,
    email: &str,
) -> basin_auth::Tokens {
    let user = auth
        .signup(project, email, "longenoughpassword")
        .await
        .expect("signup");
    auth.request_email_verification(project, user)
        .await
        .expect("request verify");
    let tok = last_token(mailer);
    auth.verify_email(project, &tok).await.expect("verify");
    auth.signin(project, email, "longenoughpassword")
        .await
        .expect("signin")
}

// ---------------------------------------------------------------------------
// Minimal raw HTTP/1.1 client
// ---------------------------------------------------------------------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

async fn raw(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> HttpResp {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    for (k, v) in extra_headers {
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
    parse_http(&buf)
}

fn parse_http(buf: &[u8]) -> HttpResp {
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("missing header terminator");
    let head = std::str::from_utf8(&buf[..split]).expect("header utf8");
    let body = buf[split + 4..].to_vec();
    let status: u16 = head
        .split("\r\n")
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("parse status");
    HttpResp { status, body }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Owner-scoped policy: each user sees only their own objects on list +
/// download; cross-user access denied.
#[tokio::test]
async fn rls_owner_eq_auth_uid_filters_list_and_download() {
    let Some((running, svc, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let project = ProjectId::new();

    // Two users in the same project.
    let alice_tokens = make_user(&auth, &mailer, &project, "alice_rls@example.com").await;
    let bob_tokens = make_user(&auth, &mailer, &project, "bob_rls@example.com").await;
    let alice_bearer = format!("Bearer {}", alice_tokens.access_token);
    let bob_bearer = format!("Bearer {}", bob_tokens.access_token);

    // Create a private bucket (as alice).
    let bucket_body =
        serde_json::to_vec(&json!({"name": "private-rls", "public": false})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/bucket",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&bucket_body),
    )
    .await;
    assert_eq!(r.status, 201, "create bucket: {}", String::from_utf8_lossy(&r.body));

    // Enable RLS + owner policy via blob_store directly.
    svc.blob_store().set_objects_rls_enabled(&project, true);
    svc.blob_store().add_object_policy(
        &project,
        ObjectPolicy {
            name: "owner_access".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        },
    );

    // Alice uploads alice.txt.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/private-rls/alice.txt",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"alice content"),
    )
    .await;
    assert_eq!(r.status, 200, "alice upload: {}", String::from_utf8_lossy(&r.body));

    // Bob uploads bob.txt.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/private-rls/bob.txt",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"bob content"),
    )
    .await;
    assert_eq!(r.status, 200, "bob upload: {}", String::from_utf8_lossy(&r.body));

    // --- List ---

    // Alice lists → should see only alice.txt.
    let list_body = serde_json::to_vec(&json!({})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/private-rls",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200, "alice list: {}", String::from_utf8_lossy(&r.body));
    let alice_list: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(alice_list.len(), 1, "alice should see exactly 1 object; got {}", alice_list.len());
    assert_eq!(alice_list[0]["path"], "alice.txt", "alice should see alice.txt");

    // Bob lists → should see only bob.txt.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/private-rls",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200, "bob list: {}", String::from_utf8_lossy(&r.body));
    let bob_list: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(bob_list.len(), 1, "bob should see exactly 1 object; got {}", bob_list.len());
    assert_eq!(bob_list[0]["path"], "bob.txt", "bob should see bob.txt");

    // --- Download ---

    // Alice downloads alice.txt → 200.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/private-rls/alice.txt",
        &[("Authorization", alice_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(r.status, 200, "alice download own object");
    assert_eq!(r.body, b"alice content");

    // Alice tries to download bob.txt → 403.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/private-rls/bob.txt",
        &[("Authorization", alice_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status, 403,
        "alice must not download bob's object; got {}",
        r.status
    );

    // Bob downloads bob.txt → 200.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/private-rls/bob.txt",
        &[("Authorization", bob_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(r.status, 200, "bob download own object");
    assert_eq!(r.body, b"bob content");

    // Bob tries to download alice.txt → 403.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/private-rls/alice.txt",
        &[("Authorization", bob_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status, 403,
        "bob must not download alice's object; got {}",
        r.status
    );

    println!("[storage_rls] owner=auth.uid() filter PASS: alice sees 1, bob sees 1, cross-access denied");
}

/// Public bucket: authenticated reads bypass the owner policy; anonymous
/// reads via the `/storage/v1/object/public/...` alias path are gated by
/// object-level RLS like any other read (since the 6.SEC.P1 fix in
/// `f1ed678`) and require an explicit `anon`-role policy to permit them.
#[tokio::test]
async fn rls_public_bucket_bypasses_rls_for_reads() {
    let Some((running, svc, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let project = ProjectId::new();
    let alice_tokens = make_user(&auth, &mailer, &project, "alice_pub@example.com").await;
    let bob_tokens = make_user(&auth, &mailer, &project, "bob_pub@example.com").await;
    let alice_bearer = format!("Bearer {}", alice_tokens.access_token);
    let bob_bearer = format!("Bearer {}", bob_tokens.access_token);

    // Create a PUBLIC bucket (alice).
    let bucket_body = serde_json::to_vec(&json!({"name": "public-rls", "public": true})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/bucket",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&bucket_body),
    )
    .await;
    assert_eq!(r.status, 201);

    // Enable RLS with owner-only policy.
    svc.blob_store().set_objects_rls_enabled(&project, true);
    svc.blob_store().add_object_policy(
        &project,
        ObjectPolicy {
            name: "owner_only".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        },
    );

    // Alice uploads.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/public-rls/img.png",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "image/png"),
        ],
        Some(b"\x89PNG"),
    )
    .await;
    assert_eq!(r.status, 200);

    // Bob lists the public bucket — should see alice's object (RLS bypassed).
    let list_body = serde_json::to_vec(&json!({})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/public-rls",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200);
    let items: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        items.len(),
        1,
        "public bucket: bob should see all objects (RLS bypassed); got {}",
        items.len()
    );

    // Anonymous (no JWT) download of the public-bucket alias is RLS-gated
    // since 6.SEC.P1 (`f1ed678`). With only an `OwnerEqAuthUid` policy on
    // the bucket and no `anon`-role permit, the anonymous request is denied.
    let r = raw(
        addr,
        "GET",
        &format!("/storage/v1/object/public/{project}/public-rls/img.png"),
        &[],
        None,
    )
    .await;
    assert!(
        r.status == 401 || r.status == 403,
        "anon download of RLS-on public bucket without anon policy should be 401/403; got {}",
        r.status
    );

    // Operator opt-in: add an `anon`-role permit policy → anon download works.
    svc.blob_store().add_object_policy(
        &project,
        ObjectPolicy {
            name: "anon_read_ok".into(),
            command: ObjectPolicyCommand::Select,
            applies_to_roles: vec!["anon".into()],
            using: ObjectUsing::True,
        },
    );
    let r = raw(
        addr,
        "GET",
        &format!("/storage/v1/object/public/{project}/public-rls/img.png"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        r.status, 200,
        "with explicit anon-permit policy, public download should be 200; got {}",
        r.status
    );
    assert_eq!(r.body, b"\x89PNG");

    println!("[storage_rls] public-bucket auth-bypass + anon-RLS-gated PASS");
}

/// Cross-project isolation: objects uploaded in project-A are invisible from
/// project-B even with the same bucket name.
#[tokio::test]
async fn rls_cross_project_isolation() {
    let Some((running, svc, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    let alice_tokens = make_user(&auth, &mailer, &project_a, "alice_iso@example.com").await;
    let bob_tokens = make_user(&auth, &mailer, &project_b, "bob_iso@example.com").await;
    let alice_bearer = format!("Bearer {}", alice_tokens.access_token);
    let bob_bearer = format!("Bearer {}", bob_tokens.access_token);

    // Each project has a bucket with the same name.
    for (bearer, _project) in [
        (alice_bearer.as_str(), &project_a),
        (bob_bearer.as_str(), &project_b),
    ] {
        let bucket_body =
            serde_json::to_vec(&json!({"name": "shared-name", "public": false})).unwrap();
        let r = raw(
            addr,
            "POST",
            "/storage/v1/bucket",
            &[
                ("Authorization", bearer),
                ("Content-Type", "application/json"),
            ],
            Some(&bucket_body),
        )
        .await;
        assert_eq!(r.status, 201);
    }

    // Enable RLS (open policy — TRUE) in project-A.
    svc.blob_store().set_objects_rls_enabled(&project_a, true);
    svc.blob_store().add_object_policy(
        &project_a,
        ObjectPolicy {
            name: "open".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::True,
        },
    );

    // Alice uploads to project-A's bucket.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/shared-name/secret.txt",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"project-a-secret"),
    )
    .await;
    assert_eq!(r.status, 200, "alice upload: {}", String::from_utf8_lossy(&r.body));

    // Bob's JWT binds him to project-B — listing /storage/v1/object/list/shared-name
    // uses his JWT's project_id, so he sees project-B's (empty) bucket.
    let list_body = serde_json::to_vec(&json!({})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/shared-name",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200, "bob list: {}", String::from_utf8_lossy(&r.body));
    let bob_items: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        bob_items.len(),
        0,
        "bob (project-B) must see 0 objects from project-B's empty bucket; got {}",
        bob_items.len()
    );

    // Bob tries to download the object alice uploaded in project-A.
    // His JWT is scoped to project-B so the bucket lookup returns NotFound.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/shared-name/secret.txt",
        &[("Authorization", bob_bearer.as_str())],
        None,
    )
    .await;
    assert_ne!(
        r.status, 200,
        "bob (project-B) must not access project-A's object; got {}",
        r.status
    );

    println!("[storage_rls] cross-project isolation PASS: project-B sees only its own (empty) bucket");
}

/// Delete is also RLS-gated: user can delete only their own objects.
#[tokio::test]
async fn rls_delete_gated_by_policy() {
    let Some((running, svc, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let project = ProjectId::new();
    let alice_tokens = make_user(&auth, &mailer, &project, "alice_del@example.com").await;
    let bob_tokens = make_user(&auth, &mailer, &project, "bob_del@example.com").await;
    let alice_bearer = format!("Bearer {}", alice_tokens.access_token);
    let bob_bearer = format!("Bearer {}", bob_tokens.access_token);

    let bucket_body =
        serde_json::to_vec(&json!({"name": "del-bucket", "public": false})).unwrap();
    raw(
        addr,
        "POST",
        "/storage/v1/bucket",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&bucket_body),
    )
    .await;

    // Enable RLS with owner policy.
    svc.blob_store().set_objects_rls_enabled(&project, true);
    svc.blob_store().add_object_policy(
        &project,
        ObjectPolicy {
            name: "owner_del".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        },
    );

    // Alice uploads a file.
    raw(
        addr,
        "POST",
        "/storage/v1/object/del-bucket/alice_file.txt",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"alice data"),
    )
    .await;

    // Bob tries to delete alice's file → 403.
    let r = raw(
        addr,
        "DELETE",
        "/storage/v1/object/del-bucket/alice_file.txt",
        &[("Authorization", bob_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status, 403,
        "bob must not delete alice's object; got {}",
        r.status
    );

    // Alice deletes her own file → 200.
    let r = raw(
        addr,
        "DELETE",
        "/storage/v1/object/del-bucket/alice_file.txt",
        &[("Authorization", alice_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status, 200,
        "alice must be able to delete her own object; got {}",
        r.status
    );

    println!("[storage_rls] delete policy PASS: bob denied, alice allowed");
}
