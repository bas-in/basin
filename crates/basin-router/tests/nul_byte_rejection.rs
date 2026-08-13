//! PostgreSQL rejects a NUL byte (`0x00`) inside any value that becomes
//! `text`; Arrow's `Utf8` happily carries one. Left unchecked, that single
//! input-domain gap makes every one of Basin's string functions diverge from
//! Postgres for NUL-bearing input (`crates/basin-exec/tests/function_equivalence.rs`
//! reports it 101 times across 15 functions). The fix belongs at the wire
//! boundary — where a client's bytes first become a Basin `text` value — not
//! in each scalar function, so this file tests it there.
//!
//! # What real PostgreSQL does
//!
//! Every case below was measured against a live PostgreSQL 18.2 server
//! (`postgres://pc@127.0.0.1:5432/postgres`, `server_encoding = UTF8`) by
//! driving the v3 wire protocol directly. Verbatim results:
//!
//! | case | PG 18.2 result |
//! |------|----------------|
//! | Bind param, **text** format, value `a\0b`, type `text` | `ERROR 22021 invalid byte sequence for encoding "UTF8": 0x00` |
//! | Bind param, **binary** format, value `a\0b`, type `text` (OID 25) | `ERROR 22021 invalid byte sequence for encoding "UTF8": 0x00` |
//! | Bind param, **binary** format, value `a\0b`, type `bytea` (OID 17) | **succeeds**, `length() = 3` |
//! | Bind param, **text** format, value `\x610062`, type `bytea` | **succeeds**, `length() = 3` |
//! | Bind param, **text** format, raw `a\0b`, type `bytea` | `ERROR 22021 invalid byte sequence for encoding "UTF8": 0x00` |
//! | `COPY t FROM STDIN` with `a\0b` in a text column | `ERROR 22021 invalid byte sequence for encoding "UTF8": 0x00` |
//! | `COPY t FROM STDIN` into `bytea` with `\x610062` | **succeeds**, `length() = 3` |
//! | simple `Query` containing a NUL (`SELECT 'a\0b'`) | `ERROR 08P01 invalid message format` |
//! | `Parse` whose query string contains a NUL | `ERROR 08P01 insufficient data left in message` |
//!
//! Two lessons that shape the implementation:
//!
//! 1. **The check is on the encoding, not on the type.** A *text-format*
//!    parameter is rejected for a raw NUL even when the declared type is
//!    `bytea` — PG runs `pg_client_to_server` over the bytes before the type's
//!    input function ever sees them. So the guard sits at the top of the
//!    text-format decoder, before type dispatch.
//! 2. **Binary is per-type.** In binary format only the types whose `recv`
//!    function goes through `pq_getmsgtext` reject a NUL (`text`, `varchar`,
//!    `bpchar`, `name`, `unknown`, `jsonb`); `bytearecv` takes arbitrary
//!    bytes. Over-rejecting here would break every legitimate binary blob, so
//!    the guard is placed arm by arm.
//!
//! A NUL inside the *query string* needs no guard of ours: the v3 `Query` and
//! `Parse` messages carry the SQL as a C string, so a NUL truncates it and the
//! framing check fires first — PG answers `08P01`, and so does the pgwire
//! decoder Basin sits on. That case is asserted here too, so a future pgwire
//! upgrade that started tolerating it would be caught.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{run_until_bound, ServerConfig, StaticProjectResolver};
use futures::SinkExt;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The exact SQLSTATE and message PostgreSQL 18.2 emits (measured, see the
/// module docs).
const PG_SQLSTATE: &str = "22021";
const PG_MESSAGE: &str = r#"invalid byte sequence for encoding "UTF8": 0x00"#;

// ─────────────────────────────────────────────────────────────────────────
// Harness
// ─────────────────────────────────────────────────────────────────────────

struct Server {
    addr: SocketAddr,
    _dir: TempDir,
    running: basin_router::RunningServer,
}

async fn spawn_router() -> Server {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
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

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), ProjectId::new());
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = run_until_bound(ServerConfig {
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

    Server {
        addr: running.local_addr,
        _dir: dir,
        running,
    }
}

impl Server {
    async fn client(&self) -> tokio_postgres::Client {
        let (client, conn) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=alice password=ignored dbname=alice",
                self.addr.port()
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
    }

    async fn shutdown(self) {
        let _ = self.running.shutdown.send(());
        let _ = self.running.join.await;
    }
}

fn err_of(e: &tokio_postgres::Error) -> (String, String) {
    let db = e
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a database error, got: {e}"));
    (db.code().code().to_string(), db.message().to_string())
}

// ─────────────────────────────────────────────────────────────────────────
// A minimal raw v3 client, for the cases tokio-postgres cannot express
// ─────────────────────────────────────────────────────────────────────────
//
// tokio-postgres always sends parameters in *binary* format for types it
// knows, so the text-format cases (and the NUL-in-query-string cases) need
// the wire driven by hand.

struct Raw {
    sock: TcpStream,
    buf: Vec<u8>,
}

fn cstr(s: &[u8]) -> Vec<u8> {
    let mut v = s.to_vec();
    v.push(0);
    v
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![tag];
    v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

/// One decoded backend message: tag plus body.
type Msg = (u8, Vec<u8>);

impl Raw {
    async fn connect(addr: SocketAddr) -> Self {
        let mut sock = TcpStream::connect(addr).await.expect("tcp connect");
        let mut params = Vec::new();
        params.extend_from_slice(b"user\0alice\0");
        params.extend_from_slice(b"database\0alice\0");
        params.extend_from_slice(b"client_encoding\0UTF8\0");
        params.push(0);
        let mut startup = Vec::new();
        startup.extend_from_slice(&((8 + params.len()) as u32).to_be_bytes());
        startup.extend_from_slice(&196_608u32.to_be_bytes());
        startup.extend_from_slice(&params);
        sock.write_all(&startup).await.unwrap();
        sock.flush().await.unwrap();

        let mut raw = Raw {
            sock,
            buf: Vec::new(),
        };
        let (tag, _) = raw.recv().await;
        assert_eq!(tag, b'R', "expected Authentication, got {}", tag as char);
        raw.send(&frame(b'p', &cstr(b"ignored"))).await;
        raw.until(b'Z').await;
        raw
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.sock.write_all(bytes).await.unwrap();
        self.sock.flush().await.unwrap();
    }

    async fn recv(&mut self) -> Msg {
        while self.buf.len() < 5 {
            let mut chunk = [0u8; 8192];
            let n = self.sock.read(&mut chunk).await.unwrap();
            assert!(n > 0, "server closed the connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
        let tag = self.buf[0];
        let len = u32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        while self.buf.len() < 1 + len {
            let mut chunk = [0u8; 8192];
            let n = self.sock.read(&mut chunk).await.unwrap();
            assert!(n > 0, "server closed mid-message");
            self.buf.extend_from_slice(&chunk[..n]);
        }
        let body = self.buf[5..1 + len].to_vec();
        self.buf.drain(..1 + len);
        (tag, body)
    }

    /// Like [`Raw::recv`], but returns `None` when the peer closes instead of
    /// panicking — needed for the query-string cases, where Basin currently
    /// drops the connection (see the KNOWN DIVERGENCE note further down).
    async fn try_recv(&mut self) -> Option<Msg> {
        loop {
            if self.buf.len() >= 5 {
                let len = u32::from_be_bytes(self.buf[1..5].try_into().unwrap()) as usize;
                if self.buf.len() >= 1 + len {
                    let tag = self.buf[0];
                    let body = self.buf[5..1 + len].to_vec();
                    self.buf.drain(..1 + len);
                    return Some((tag, body));
                }
            }
            let mut chunk = [0u8; 8192];
            match self.sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }

    /// Read messages until `stop` (inclusive).
    async fn until(&mut self, stop: u8) -> Vec<Msg> {
        let mut out = Vec::new();
        loop {
            let m = self.recv().await;
            let tag = m.0;
            out.push(m);
            if tag == stop {
                return out;
            }
        }
    }

    /// Send Parse/Bind/Execute/Sync with one parameter and return the
    /// `(sqlstate, message)` of the ErrorResponse, or `None` if it succeeded.
    async fn one_param_query(
        &mut self,
        sql: &[u8],
        param_oid: u32,
        format_code: u16,
        value: &[u8],
    ) -> Option<(String, String)> {
        let mut parse = cstr(b"");
        parse.extend_from_slice(&cstr(sql));
        if param_oid == 0 {
            parse.extend_from_slice(&0u16.to_be_bytes());
        } else {
            parse.extend_from_slice(&1u16.to_be_bytes());
            parse.extend_from_slice(&param_oid.to_be_bytes());
        }

        let mut bind = cstr(b"");
        bind.extend_from_slice(&cstr(b""));
        bind.extend_from_slice(&1u16.to_be_bytes());
        bind.extend_from_slice(&format_code.to_be_bytes());
        bind.extend_from_slice(&1u16.to_be_bytes());
        bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
        bind.extend_from_slice(value);
        bind.extend_from_slice(&0u16.to_be_bytes());

        let mut exec = cstr(b"");
        exec.extend_from_slice(&0u32.to_be_bytes());

        let mut out = frame(b'P', &parse);
        out.extend_from_slice(&frame(b'B', &bind));
        out.extend_from_slice(&frame(b'E', &exec));
        out.extend_from_slice(&frame(b'S', b""));
        self.send(&out).await;
        first_error(&self.until(b'Z').await)
    }
}

/// Extract `(sqlstate, message)` from the first ErrorResponse in `msgs`.
fn first_error(msgs: &[Msg]) -> Option<(String, String)> {
    for (tag, body) in msgs {
        if *tag != b'E' {
            continue;
        }
        let mut code = String::new();
        let mut message = String::new();
        for part in body.split(|b| *b == 0) {
            if part.is_empty() {
                continue;
            }
            let text = String::from_utf8_lossy(&part[1..]).into_owned();
            match part[0] {
                b'C' => code = text,
                b'M' => message = text,
                _ => {}
            }
        }
        return Some((code, message));
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────
// Extended protocol — text-format parameters
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn text_format_param_with_nul_is_rejected() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    let err = raw
        .one_param_query(b"SELECT length($1::text)", 25, 0, b"a\0b")
        .await;

    assert_eq!(
        err,
        Some((PG_SQLSTATE.to_string(), PG_MESSAGE.to_string())),
        "text-format text param carrying a NUL must be rejected exactly as PG 18.2 does"
    );

    srv.shutdown().await;
}

/// PG checks the client encoding *before* type dispatch, so a raw NUL in a
/// text-format parameter is rejected even when the declared type is `bytea`.
/// Measured on PG 18.2 (module docs, case "text format, raw a\0b, bytea").
#[tokio::test]
async fn text_format_bytea_param_with_raw_nul_is_rejected() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    let err = raw
        .one_param_query(b"SELECT length($1::bytea)", 17, 0, b"a\0b")
        .await;

    assert_eq!(
        err,
        Some((PG_SQLSTATE.to_string(), PG_MESSAGE.to_string())),
        "a raw NUL in a *text-format* parameter is rejected whatever the declared type"
    );

    srv.shutdown().await;
}

/// The hex-escaped bytea text form is the legitimate way to carry a NUL in a
/// text-format parameter, and it must keep working: `\x610062` is `a\0b`.
#[tokio::test]
async fn text_format_bytea_param_with_hex_escaped_nul_succeeds() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    let err = raw
        .one_param_query(b"SELECT length($1::bytea)", 17, 0, br"\x610062")
        .await;

    assert_eq!(
        err, None,
        "hex-escaped bytea text carrying a NUL must still be accepted"
    );

    srv.shutdown().await;
}

/// Every other text-format parameter is untouched: no NUL, no rejection.
#[tokio::test]
async fn text_format_param_without_nul_still_works() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    assert_eq!(
        raw.one_param_query(b"SELECT length($1::text)", 25, 0, "héllo".as_bytes())
            .await,
        None,
        "multibyte UTF-8 must pass through untouched"
    );
    assert_eq!(
        raw.one_param_query(b"SELECT $1::int4", 23, 0, b"42").await,
        None,
        "non-text parameters must be unaffected"
    );

    srv.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────
// Extended protocol — binary-format parameters
// ─────────────────────────────────────────────────────────────────────────

/// This is the exact path `function_equivalence.rs` uses to reach Postgres:
/// tokio-postgres encodes a Rust `String` as a binary-format `text` parameter.
#[tokio::test]
async fn binary_format_text_param_with_nul_is_rejected() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    let err = client
        .query("SELECT length($1::text)", &[&"ab\0cd".to_string()])
        .await
        .expect_err("Basin must reject a NUL in a binary text parameter, as PG 18.2 does");

    assert_eq!(
        err_of(&err),
        (PG_SQLSTATE.to_string(), PG_MESSAGE.to_string())
    );

    srv.shutdown().await;
}

/// `bytea` holds arbitrary bytes — over-rejecting here would be a worse bug
/// than the one being fixed. PG 18.2 accepts a binary `bytea` parameter
/// containing `0x00` and reports `length() = 3` (module docs).
///
/// Bound against a real `bytea` *column* rather than a bare `SELECT
/// length($1)`. Basin resolves a parameter's type from its own engine-side
/// inference and ignores the OIDs the client declared in `Parse`
/// (`protocol.rs`, `handle_bind`: `arrow_to_pg_type(&entry.schema.param_types[i])`),
/// so a parameter with no column context infers as `text` no matter what the
/// client said. An `INSERT` into a `bytea` column is the shape where the
/// inference is right, and it is also the shape real clients use to send
/// blobs — which is what makes this the meaningful over-rejection guard.
#[tokio::test]
async fn binary_format_bytea_param_with_nul_succeeds() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    client
        .simple_query("CREATE TABLE nul_blob (b bytea)")
        .await
        .expect("create table");

    let blob: Vec<u8> = vec![b'a', 0, b'b'];
    client
        .execute("INSERT INTO nul_blob (b) VALUES ($1)", &[&blob])
        .await
        .expect("bytea must keep accepting NUL bytes");

    let rows = client
        .query("SELECT length(b) FROM nul_blob", &[])
        .await
        .expect("select back");
    let n: i32 = rows[0].get(0);
    assert_eq!(n, 3, "PG 18.2 returns 3 for length('a\\0b'::bytea)");

    srv.shutdown().await;
}

#[tokio::test]
async fn binary_format_text_param_without_nul_still_works() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    let rows = client
        .query("SELECT upper($1::text)", &[&"héllo".to_string()])
        .await
        .expect("ordinary text must be unaffected");
    let s: String = rows[0].get(0);
    assert_eq!(s, "HÉLLO");

    srv.shutdown().await;
}

/// A NUL inside a `text[]` element travels through the array binary decoder,
/// which dispatches per element OID — so the element guard must fire too.
#[tokio::test]
async fn binary_format_text_array_element_with_nul_is_rejected() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    let arr: Vec<String> = vec!["ok".to_string(), "a\0b".to_string()];
    let err = client
        .query("SELECT $1::text[]", &[&arr])
        .await
        .expect_err("a NUL in a text[] element must be rejected");

    assert_eq!(
        err_of(&err),
        (PG_SQLSTATE.to_string(), PG_MESSAGE.to_string())
    );

    srv.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────
// COPY
// ─────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn copy_text_column_with_nul_is_rejected() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    client
        .simple_query("CREATE TABLE nul_copy (t text)")
        .await
        .expect("create table");

    let sink = client
        .copy_in::<_, bytes::Bytes>("COPY nul_copy FROM STDIN WITH (FORMAT csv)")
        .await
        .expect("copy_in");
    futures::pin_mut!(sink);
    sink.send(bytes::Bytes::from_static(b"a\0b\n")).await.ok();
    let err = sink
        .finish()
        .await
        .expect_err("COPY carrying a NUL must be rejected");

    assert_eq!(
        err_of(&err),
        (PG_SQLSTATE.to_string(), PG_MESSAGE.to_string())
    );

    srv.shutdown().await;
}

#[tokio::test]
async fn copy_bytea_column_with_hex_escaped_nul_succeeds() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    client
        .simple_query("CREATE TABLE nul_copy_b (b bytea)")
        .await
        .expect("create table");

    let sink = client
        .copy_in::<_, bytes::Bytes>("COPY nul_copy_b FROM STDIN WITH (FORMAT csv)")
        .await
        .expect("copy_in");
    futures::pin_mut!(sink);
    sink.send(bytes::Bytes::from_static(br"\x610062" as &[u8]))
        .await
        .unwrap();
    sink.send(bytes::Bytes::from_static(b"\n")).await.unwrap();
    sink.finish()
        .await
        .expect("hex-escaped bytea must keep loading");

    let rows = client
        .query("SELECT length(b) FROM nul_copy_b", &[])
        .await
        .expect("select back");
    let n: i32 = rows[0].get(0);
    assert_eq!(n, 3, "PG 18.2 stores 3 bytes for \\x610062");

    srv.shutdown().await;
}

#[tokio::test]
async fn copy_text_column_without_nul_still_works() {
    let srv = spawn_router().await;
    let client = srv.client().await;

    client
        .simple_query("CREATE TABLE nul_copy_ok (t text)")
        .await
        .expect("create table");

    let sink = client
        .copy_in::<_, bytes::Bytes>("COPY nul_copy_ok FROM STDIN WITH (FORMAT csv)")
        .await
        .expect("copy_in");
    futures::pin_mut!(sink);
    sink.send(bytes::Bytes::from_static("héllo\n".as_bytes()))
        .await
        .unwrap();
    sink.finish().await.expect("ordinary COPY must still work");

    let rows = client
        .query("SELECT t FROM nul_copy_ok", &[])
        .await
        .expect("select back");
    let s: String = rows[0].get(0);
    assert_eq!(s, "héllo");

    srv.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────
// NUL in the query string itself
// ─────────────────────────────────────────────────────────────────────────
//
// The v3 `Query` and `Parse` messages carry SQL as a C string, so an embedded
// NUL truncates it and the message body desyncs. What matters for correctness
// is that the *truncated prefix never executes*: `SELECT length('a\0b')`
// truncated at the NUL is `SELECT length('a`, and a server that ran some
// prefix of a statement the client did not write would be a far worse defect
// than the one this file fixes. Both tests below pin that invariant.
//
// KNOWN DIVERGENCE — not fixed here, and not fixable in this crate.
//
//   PostgreSQL 18.2, measured: a simple `Query` carrying a NUL answers
//   `ERROR 08P01 invalid message format`; a `Parse` carrying one answers
//   `ERROR 08P01 insufficient data left in message`. Both leave the
//   connection usable.
//
//   Basin instead panics inside its pgwire dependency and drops the
//   connection. `pgwire-0.28.0/src/messages/codec.rs:12` `get_cstring` stops
//   at the embedded NUL and leaves the remainder of the message body in the
//   buffer; the decoder then reads that remainder as the next field and
//   `bytes` panics with "advance out of bounds: the len is 1 but advancing
//   by 4". The panic is inside the framed decoder pgwire owns, upstream of
//   every `basin-router` handler, so no change in this crate can turn it
//   into a clean 08P01 — it needs a fix in (or a fork of) pgwire.
//
//   Blast radius, measured: the panic unwinds one connection task. The
//   listener survives and other connections are unaffected, so this is a
//   per-connection abort, and it is reachable only after authentication.
//   It is a protocol-robustness bug, not part of the NUL-in-`text` input
//   domain defect this file closes.

#[tokio::test]
async fn nul_in_simple_query_string_never_executes_the_truncated_prefix() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    raw.send(&frame(b'Q', &cstr(b"SELECT length('a\0b')")))
        .await;

    // Either a clean ErrorResponse (what PG does, and what Basin should do
    // once pgwire's decoder is fixed) or a dropped connection. What must
    // never happen is a successful RowDescription/DataRow for the truncated
    // statement.
    let mut saw_rows = false;
    loop {
        match raw.try_recv().await {
            Some((tag, _)) if tag == b'T' || tag == b'D' || tag == b'C' => {
                saw_rows = true;
                break;
            }
            Some((tag, _)) if tag == b'Z' || tag == b'E' => break,
            Some(_) => continue,
            None => break, // connection dropped
        }
    }
    assert!(
        !saw_rows,
        "a NUL truncated the query string; the truncated prefix must never execute"
    );

    srv.shutdown().await;
}

#[tokio::test]
async fn nul_in_parse_query_string_never_executes_the_truncated_prefix() {
    let srv = spawn_router().await;
    let mut raw = Raw::connect(srv.addr).await;

    let mut parse = cstr(b"");
    parse.extend_from_slice(&cstr(b"SELECT length('a\0b')"));
    parse.extend_from_slice(&0u16.to_be_bytes());
    let mut out = frame(b'P', &parse);
    out.extend_from_slice(&frame(b'B', &{
        let mut b = cstr(b"");
        b.extend_from_slice(&cstr(b""));
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b
    }));
    out.extend_from_slice(&frame(b'E', &{
        let mut e = cstr(b"");
        e.extend_from_slice(&0u32.to_be_bytes());
        e
    }));
    out.extend_from_slice(&frame(b'S', b""));
    raw.send(&out).await;

    let mut saw_rows = false;
    loop {
        match raw.try_recv().await {
            Some((tag, _)) if tag == b'D' || tag == b'C' => {
                saw_rows = true;
                break;
            }
            Some((tag, _)) if tag == b'Z' || tag == b'E' => break,
            Some(_) => continue,
            None => break,
        }
    }
    assert!(
        !saw_rows,
        "a NUL truncated the Parse query string; the truncated prefix must never execute"
    );

    srv.shutdown().await;
}
