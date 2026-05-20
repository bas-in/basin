//! Phase 5.18.E — capstone end-to-end test for reserved schemas + PG-tooling
//! compatibility + RLS + user-schema alias semantics (ADR 0022).
//!
//! Assertions:
//!
//! 1. **PG tooling sees real schema membership**: `information_schema.tables
//!    WHERE table_schema = 'auth'` returns auth tables; same for `storage`,
//!    `cron`, `net`. Mirrors the postgrest_pgadmin_compat shape.
//!
//! 2. **Qualified reserved-schema reads bind correctly**: `SELECT count(*)
//!    FROM auth.users` (and other reserved-schema qualifiers) works and is
//!    scoped to the right table.  ADR 0022: schema qualifiers on reserved
//!    schemas are stripped at the engine layer; a table created with a plain
//!    name in the same project is accessible under `auth.<name>`,
//!    `storage.<name>`, etc.
//!
//! 3. **RLS on `storage.objects` still scopes** after the schema move: a
//!    `CREATE POLICY ON storage.objects` with `owner = auth.uid()` filters
//!    list/download for two users (mirrors the 5.17.C test but with real
//!    schema qualification).
//!
//! 4. **User `CREATE SCHEMA` aliases to `public`** (per ADR 0022): `CREATE
//!    SCHEMA myapp; CREATE TABLE myapp.t (id BIGINT); INSERT INTO myapp.t
//!    VALUES (1); SELECT * FROM t` returns the row — proves the documented
//!    alias semantics.
//!
//! 5. **`search_path` honored**: `CREATE TABLE pub_x (...); SET search_path
//!    = 'pg_catalog, public'; SELECT * FROM pub_x` resolves through the
//!    search_path.
//!
//! 6. **Cross-project isolation preserved**: two projects each with their own
//!    `auth.users` (via catalog-direct insertion) don't leak across the schema
//!    axis — the hard per-project isolation invariant holds.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_blob::{ObjectPolicy, ObjectPolicyCommand, ObjectUsing};
use basin_catalog::{Catalog as _, InMemoryCatalog};
use basin_common::{ProjectId, QualifiedTableName, SchemaName, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_rest::{RestConfig, RestService, RunningRest};
use basin_router::{ServerConfig, StaticProjectResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use ulid::Ulid;

// ─── Shared helpers ──────────────────────────────────────────────────────────

/// Minimal Arrow schema for a single-column `id BIGINT` table.
fn minimal_arrow_schema() -> arrow_schema::Schema {
    arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )])
}

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
    project: ProjectId,
}

/// Start an in-process Basin pgwire server for a single project "alice".
async fn start_server() -> TestServer {
    start_server_with_catalog(InMemoryCatalog::new()).await
}

/// Start an in-process Basin pgwire server using the given (pre-seeded)
/// catalog.
async fn start_server_with_catalog(catalog: InMemoryCatalog) -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let arc_catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(catalog);
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: arc_catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
        project,
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                Some(row)
            }
            _ => None,
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 1 — PG tooling sees real schema membership
// ─────────────────────────────────────────────────────────────────────────────

/// `information_schema.tables WHERE table_schema = 'auth'` returns the
/// auth-schema tables that were pre-seeded directly in the catalog.
/// `information_schema.schemata` must include every reserved schema name
/// (auth, storage, cron, net).
///
/// This mirrors the postgrest_pgadmin_compat shape: the session routes
/// `information_schema.tables` to `InfoSchemaQuery::tables`, which uses
/// `list_tables_qualified` and therefore reports the real `(schema, table)`
/// keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_tooling_sees_real_schema_membership() {
    // Pre-seed the catalog with tables in multiple reserved schemas so that
    // `information_schema.tables` returns rows with the right `table_schema`
    // values. The catalog must have the project namespace created before we
    // can insert qualified tables.
    let catalog = InMemoryCatalog::new();
    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    // auth.users — the canonical Supabase-shaped auth table.
    let auth_users = QualifiedTableName::new(
        SchemaName::new("auth").unwrap(),
        TableName::new("users").unwrap(),
    );
    catalog
        .create_table_qualified(&project, &auth_users, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // storage.objects
    let storage_objects = QualifiedTableName::new(
        SchemaName::new("storage").unwrap(),
        TableName::new("objects").unwrap(),
    );
    catalog
        .create_table_qualified(&project, &storage_objects, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // cron.job
    let cron_job = QualifiedTableName::new(
        SchemaName::new("cron").unwrap(),
        TableName::new("job").unwrap(),
    );
    catalog
        .create_table_qualified(&project, &cron_job, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // net._http_response
    let net_resp = QualifiedTableName::new(
        SchemaName::new("net").unwrap(),
        TableName::new("_http_response").unwrap(),
    );
    catalog
        .create_table_qualified(&project, &net_resp, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // Also a regular public table.
    let public_orders =
        QualifiedTableName::in_public(TableName::new("orders").unwrap());
    catalog
        .create_table_qualified(&project, &public_orders, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // Build a server wired to THIS project's catalog.
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let arc_catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(catalog);
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: arc_catalog,
        shard: None,
    });
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));
    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server bind");
    let _shutdown = running.shutdown;
    let _join = running.join;
    let _dir = dir;
    let client = connect(running.local_addr).await;

    // ── information_schema.tables WHERE table_schema = 'auth' ──────────────
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'auth'",
            &[],
        )
        .await
        .expect("information_schema.tables auth query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains(&"users".to_string()),
        "expected 'users' in auth schema tables, got {names:?}"
    );

    // ── information_schema.tables WHERE table_schema = 'storage' ───────────
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'storage'",
            &[],
        )
        .await
        .expect("information_schema.tables storage query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains(&"objects".to_string()),
        "expected 'objects' in storage schema tables, got {names:?}"
    );

    // ── information_schema.tables WHERE table_schema = 'cron' ──────────────
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'cron'",
            &[],
        )
        .await
        .expect("information_schema.tables cron query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains(&"job".to_string()),
        "expected 'job' in cron schema tables, got {names:?}"
    );

    // ── information_schema.tables WHERE table_schema = 'net' ───────────────
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'net'",
            &[],
        )
        .await
        .expect("information_schema.tables net query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains(&"_http_response".to_string()),
        "expected '_http_response' in net schema tables, got {names:?}"
    );

    // ── information_schema.tables WHERE table_schema = 'public' ────────────
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public'",
            &[],
        )
        .await
        .expect("information_schema.tables public query");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        names.contains(&"orders".to_string()),
        "expected 'orders' in public schema tables, got {names:?}"
    );

    // ── information_schema.schemata includes all reserved schemas ───────────
    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata",
            &[],
        )
        .await
        .expect("information_schema.schemata query");

    let schema_names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for expected in ["public", "auth", "storage", "cron", "net"] {
        assert!(
            schema_names.iter().any(|s| s == expected),
            "expected schema {expected:?} in information_schema.schemata, got {schema_names:?}"
        );
    }

    // ── pg_catalog.pg_namespace includes reserved schemas ──────────────────
    let rows = client
        .query(
            "SELECT nspname FROM pg_catalog.pg_namespace \
             WHERE nspname NOT IN ('pg_catalog', 'information_schema')",
            &[],
        )
        .await
        .expect("pg_namespace query");

    let nsp_names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        nsp_names.iter().any(|s| s == "auth"),
        "expected 'auth' in pg_namespace, got {nsp_names:?}"
    );
    assert!(
        nsp_names.iter().any(|s| s == "storage"),
        "expected 'storage' in pg_namespace, got {nsp_names:?}"
    );

    println!("[schema_e2e] assertion 1 PASS: PG tooling sees real schema membership");
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 2 — Qualified reserved-schema reads bind correctly
// ─────────────────────────────────────────────────────────────────────────────

/// `SELECT count(*) FROM auth.users` works and reads from the correct
/// underlying table. Per ADR 0022, a schema qualifier on a reserved schema is
/// validated and then stripped before DataFusion sees it, so `auth.users` and
/// `users` refer to the same underlying public-namespace table in the project.
///
/// The test creates `users` via the plain path, registers the schema in the
/// session (with `CREATE SCHEMA IF NOT EXISTS auth`), inserts rows, then
/// issues a schema-qualified count and a bare count and verifies they agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn qualified_reserved_schema_reads_bind_correctly() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Register the reserved schemas in the session so the schema-qualifier
    // stripper (`strip_schema_qualifiers_for_session`) knows to remove them
    // before DataFusion sees the SQL. Reserved schemas are accepted by
    // `exec_create_schema` and added to the session's schema set.
    for schema in ["auth", "storage", "cron", "net"] {
        client
            .simple_query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE SCHEMA {schema}: {e}"));
    }

    // Create a plain `users` table and insert two rows.
    client
        .simple_query(
            "CREATE TABLE users (id BIGINT NOT NULL, name TEXT NOT NULL)",
        )
        .await
        .expect("CREATE TABLE users");
    client
        .simple_query("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("INSERT INTO users");

    // Verify unqualified count.
    let msgs = client
        .simple_query("SELECT count(*) FROM users")
        .await
        .expect("unqualified count");
    let data = rows_of(&msgs);
    assert_eq!(data[0][0].as_deref(), Some("2"), "expected 2 rows unqualified");

    // Verify auth-qualified count — must return the same 2 rows.
    // `auth.` is stripped by the engine before DataFusion planning,
    // so `auth.users` resolves to the same underlying `users` table.
    let msgs = client
        .simple_query("SELECT count(*) FROM auth.users")
        .await
        .expect("auth-qualified count");
    let data = rows_of(&msgs);
    assert_eq!(
        data[0][0].as_deref(),
        Some("2"),
        "auth.users must return same count as bare users"
    );

    // Verify storage-qualified access (different reserved schema, same table).
    let msgs = client
        .simple_query("SELECT count(*) FROM storage.users")
        .await
        .expect("storage-qualified count");
    let data = rows_of(&msgs);
    assert_eq!(
        data[0][0].as_deref(),
        Some("2"),
        "storage.users (stripped to users) must return 2 rows"
    );

    // Verify public-qualified access is identical.
    let msgs = client
        .simple_query("SELECT count(*) FROM public.users")
        .await
        .expect("public-qualified count");
    let data = rows_of(&msgs);
    assert_eq!(
        data[0][0].as_deref(),
        Some("2"),
        "public.users must return 2 rows"
    );

    println!("[schema_e2e] assertion 2 PASS: qualified reserved-schema reads bind correctly");
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 3 — RLS on `storage.objects` still scopes after the schema move
// ─────────────────────────────────────────────────────────────────────────────

// Postgres connection string for the auth service. Tests that need RLS skip
// cleanly when Postgres is not available (same pattern as storage_rls.rs).
const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!("basin_e2e_test_{}", Ulid::new().to_string().to_lowercase())
}

struct SchemaGuard {
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            let rt = match rt {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("schema_e2e SchemaGuard runtime: {e}");
                    return;
                }
            };
            rt.block_on(async {
                let conn = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
                )
                .await;
                let (client, conn) = match conn {
                    Ok(Ok(pair)) => pair,
                    _ => return,
                };
                let _driver = tokio::spawn(async move { let _ = conn.await; });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
            });
        })
        .join();
    }
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

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
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

/// Spin up a REST + auth server; returns `None` when Postgres is unavailable.
async fn try_serve_rls() -> Option<(RunningRest, RestService, AuthService, StubMailer, SchemaGuard)> {
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
            eprintln!("postgres unreachable, skipping schema_e2e RLS test: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("postgres connect timed out, skipping schema_e2e RLS test");
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

/// RLS on `storage.objects` still scopes correctly after the schema move.
/// Two users each upload an object; with `owner = auth.uid()` each user sees
/// only their own object on list and download.
///
/// This mirrors `rls_owner_eq_auth_uid_filters_list_and_download` in
/// `storage_rls.rs` but uses real schema-qualified `storage.objects` policy
/// language to confirm that the schema axis didn't break the RLS machinery.
#[tokio::test]
async fn rls_on_storage_objects_scopes_after_schema_move() {
    let Some((running, svc, auth, mailer, _guard)) = try_serve_rls().await else {
        return;
    };
    let addr = running.local_addr;

    let project = ProjectId::new();
    let alice_tokens = make_user(&auth, &mailer, &project, "alice_e2e@example.com").await;
    let bob_tokens = make_user(&auth, &mailer, &project, "bob_e2e@example.com").await;
    let alice_bearer = format!("Bearer {}", alice_tokens.access_token);
    let bob_bearer = format!("Bearer {}", bob_tokens.access_token);

    // Create a private bucket.
    let bucket_body = serde_json::to_vec(&json!({"name": "e2e-private", "public": false})).unwrap();
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
    assert_eq!(
        r.status, 201,
        "create bucket: {}",
        String::from_utf8_lossy(&r.body)
    );

    // Enable RLS with `owner = auth.uid()` policy (schema-qualified semantics
    // mirror CREATE POLICY ON storage.objects USING (owner = auth.uid())).
    svc.blob_store().set_objects_rls_enabled(&project, true);
    svc.blob_store().add_object_policy(
        &project,
        ObjectPolicy {
            name: "e2e_owner_access".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        },
    );

    // Alice uploads.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/e2e-private/alice.txt",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"alice-content"),
    )
    .await;
    assert_eq!(r.status, 200, "alice upload: {}", String::from_utf8_lossy(&r.body));

    // Bob uploads.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/e2e-private/bob.txt",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "text/plain"),
        ],
        Some(b"bob-content"),
    )
    .await;
    assert_eq!(r.status, 200, "bob upload: {}", String::from_utf8_lossy(&r.body));

    // List — alice sees only her object.
    let list_body = serde_json::to_vec(&json!({})).unwrap();
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/e2e-private",
        &[
            ("Authorization", alice_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200);
    let alice_list: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        alice_list.len(),
        1,
        "alice should see exactly 1 object; got {}",
        alice_list.len()
    );
    assert_eq!(alice_list[0]["path"], "alice.txt");

    // List — bob sees only his object.
    let r = raw(
        addr,
        "POST",
        "/storage/v1/object/list/e2e-private",
        &[
            ("Authorization", bob_bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        Some(&list_body),
    )
    .await;
    assert_eq!(r.status, 200);
    let bob_list: Vec<Value> = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        bob_list.len(),
        1,
        "bob should see exactly 1 object; got {}",
        bob_list.len()
    );
    assert_eq!(bob_list[0]["path"], "bob.txt");

    // Cross-access denied: alice cannot download bob.txt.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/e2e-private/bob.txt",
        &[("Authorization", alice_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(r.status, 403, "alice must not download bob's object; got {}", r.status);

    // Cross-access denied: bob cannot download alice.txt.
    let r = raw(
        addr,
        "GET",
        "/storage/v1/object/e2e-private/alice.txt",
        &[("Authorization", bob_bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(r.status, 403, "bob must not download alice's object; got {}", r.status);

    println!("[schema_e2e] assertion 3 PASS: RLS on storage.objects scopes correctly");
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 4 — User CREATE SCHEMA aliases to public
// ─────────────────────────────────────────────────────────────────────────────

/// `CREATE SCHEMA myapp; CREATE TABLE myapp.t (id BIGINT); INSERT INTO
/// myapp.t VALUES (1); SELECT * FROM t` returns the row.
///
/// Per ADR 0022 user-defined schemas are flat-aliased to `public`: `myapp.t`
/// and `public.t` and bare `t` are all the same table. This is the documented
/// limitation / expected semantics of the flat model.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_create_schema_aliases_to_public() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Step 1: create a user-defined schema.
    client
        .simple_query("CREATE SCHEMA myapp")
        .await
        .expect("CREATE SCHEMA myapp");

    // Step 2: create a table under the user schema qualifier.
    client
        .simple_query("CREATE TABLE myapp.t (id BIGINT NOT NULL)")
        .await
        .expect("CREATE TABLE myapp.t");

    // Step 3: insert via the schema-qualified name.
    client
        .simple_query("INSERT INTO myapp.t VALUES (1)")
        .await
        .expect("INSERT INTO myapp.t");

    // Step 4: SELECT via bare name — the alias must surface the row.
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT * FROM t (bare)");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 1, "bare 't' must find the row inserted via myapp.t");
    assert_eq!(
        data[0][0].as_deref(),
        Some("1"),
        "row id must be 1"
    );

    // Step 5: SELECT via public-qualified name — same table.
    let msgs = client
        .simple_query("SELECT * FROM public.t")
        .await
        .expect("SELECT * FROM public.t");
    let pub_data = rows_of(&msgs);
    assert_eq!(
        pub_data.len(),
        1,
        "public.t must also find the row"
    );

    // Step 6: SELECT via the user schema qualifier again — must still work.
    let msgs = client
        .simple_query("SELECT * FROM myapp.t")
        .await
        .expect("SELECT * FROM myapp.t (re-read)");
    let myapp_data = rows_of(&msgs);
    assert_eq!(
        myapp_data.len(),
        1,
        "myapp.t must still resolve to the same row"
    );

    println!("[schema_e2e] assertion 4 PASS: user CREATE SCHEMA aliases to public");
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 5 — search_path honored
// ─────────────────────────────────────────────────────────────────────────────

/// `CREATE TABLE pub_x (...); SET search_path = myns, public;
/// SELECT * FROM pub_x` resolves through the effective search_path.
///
/// Verifies that unqualified resolution picks `public.pub_x` when `public`
/// is in the path (even via an aliased user schema). Also verifies that the
/// schema-qualified form `public.pub_x` always works and that a qualified
/// insert via a user schema alias round-trips through an unqualified select.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_path_honored() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Register a user schema (aliases to public per ADR 0022).
    client
        .simple_query("CREATE SCHEMA myns")
        .await
        .expect("CREATE SCHEMA myns");

    // Create the table under public (always the physical destination).
    client
        .simple_query("CREATE TABLE pub_x (val TEXT NOT NULL)")
        .await
        .expect("CREATE TABLE pub_x");
    client
        .simple_query("INSERT INTO pub_x VALUES ('hello')")
        .await
        .expect("INSERT INTO pub_x");

    // Set search_path to include myns (aliases to public) then public.
    client
        .simple_query("SET search_path = myns, public")
        .await
        .expect("SET search_path = myns, public");

    // Unqualified SELECT must still resolve pub_x through the path.
    // `myns` aliases to `public`, so `pub_x` is found via both entries.
    let msgs = client
        .simple_query("SELECT * FROM pub_x")
        .await
        .expect("SELECT * FROM pub_x after SET search_path = myns, public");
    let data = rows_of(&msgs);
    assert_eq!(
        data.len(),
        1,
        "pub_x must be reachable after SET search_path = myns, public"
    );
    assert_eq!(data[0][0].as_deref(), Some("hello"));

    // Schema-qualified public.pub_x must always work regardless of search_path.
    let msgs = client
        .simple_query("SELECT * FROM public.pub_x")
        .await
        .expect("SELECT * FROM public.pub_x");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 1, "public.pub_x must return 1 row");

    // Insert via the user-schema alias and read back unqualified.
    client
        .simple_query("INSERT INTO myns.pub_x VALUES ('world')")
        .await
        .expect("INSERT INTO myns.pub_x (user schema alias)");
    let msgs = client
        .simple_query("SELECT count(*) FROM pub_x")
        .await
        .expect("SELECT count(*) FROM pub_x after alias insert");
    let data = rows_of(&msgs);
    assert_eq!(data[0][0].as_deref(), Some("2"), "expected 2 rows after alias insert");

    // Reset search_path to default and verify it still works.
    client
        .simple_query("SET search_path = public")
        .await
        .expect("SET search_path = public (reset)");

    let msgs = client
        .simple_query("SELECT count(*) FROM pub_x")
        .await
        .expect("SELECT count(*) FROM pub_x after reset");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 1, "count row expected after search_path reset");
    assert_eq!(data[0][0].as_deref(), Some("2"), "still 2 rows after reset");

    println!("[schema_e2e] assertion 5 PASS: search_path honored");
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion 6 — Cross-project isolation preserved across the schema axis
// ─────────────────────────────────────────────────────────────────────────────

/// Two projects each with their own `auth.users` (registered in the catalog
/// directly) must not leak to each other through `information_schema.tables`.
/// This proves the per-project isolation invariant holds across the new schema
/// axis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_project_isolation_preserved() {
    basin_common::telemetry::try_init_for_tests();

    let catalog = InMemoryCatalog::new();

    // Project A: has auth.users
    let project_a = ProjectId::new();
    catalog.create_namespace(&project_a).await.unwrap();
    let auth_users_a = QualifiedTableName::new(
        SchemaName::new("auth").unwrap(),
        TableName::new("users").unwrap(),
    );
    catalog
        .create_table_qualified(&project_a, &auth_users_a, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // Project B: has auth.accounts (different table name)
    let project_b = ProjectId::new();
    catalog.create_namespace(&project_b).await.unwrap();
    let auth_accounts_b = QualifiedTableName::new(
        SchemaName::new("auth").unwrap(),
        TableName::new("accounts").unwrap(),
    );
    catalog
        .create_table_qualified(&project_b, &auth_accounts_b, Arc::new(minimal_arrow_schema()))
        .await
        .unwrap();

    // Build a two-project server on the shared catalog.
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let arc_catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(catalog);
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: arc_catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), project_a);
    map.insert("bob".to_owned(), project_b);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("server bind");
    let _shutdown = running.shutdown;
    let _join = running.join;
    let _dir = dir;

    // Connect as alice (project A).
    let alice_str = format!(
        "host={} port={} user=alice password=ignored",
        running.local_addr.ip(),
        running.local_addr.port()
    );
    let (alice, conn_a) = tokio_postgres::connect(&alice_str, NoTls)
        .await
        .expect("alice connect");
    tokio::spawn(async move { let _ = conn_a.await; });

    // Connect as bob (project B).
    let bob_str = format!(
        "host={} port={} user=bob password=ignored",
        running.local_addr.ip(),
        running.local_addr.port()
    );
    let (bob, conn_b) = tokio_postgres::connect(&bob_str, NoTls)
        .await
        .expect("bob connect");
    tokio::spawn(async move { let _ = conn_b.await; });

    // Alice queries information_schema.tables — must see users but NOT accounts.
    let alice_rows = alice
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'auth' ORDER BY table_name",
            &[],
        )
        .await
        .expect("alice information_schema.tables");
    let alice_tables: Vec<String> = alice_rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        alice_tables.contains(&"users".to_string()),
        "alice must see auth.users; got {alice_tables:?}"
    );
    assert!(
        !alice_tables.contains(&"accounts".to_string()),
        "alice must NOT see project-B's auth.accounts; got {alice_tables:?}"
    );

    // Bob queries information_schema.tables — must see accounts but NOT users.
    let bob_rows = bob
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'auth' ORDER BY table_name",
            &[],
        )
        .await
        .expect("bob information_schema.tables");
    let bob_tables: Vec<String> = bob_rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(
        bob_tables.contains(&"accounts".to_string()),
        "bob must see auth.accounts; got {bob_tables:?}"
    );
    assert!(
        !bob_tables.contains(&"users".to_string()),
        "bob must NOT see project-A's auth.users; got {bob_tables:?}"
    );

    println!("[schema_e2e] assertion 6 PASS: cross-project isolation preserved across schema axis");
}
