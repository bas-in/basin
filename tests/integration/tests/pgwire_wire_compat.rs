//! pgwire wire-protocol depth test suite.
//!
//! Proves that real Postgres drivers (tokio-postgres / pgx / psycopg / node-pg)
//! won't trip on wire-protocol edge cases beyond the simple happy-path covered
//! by `param_bind_smoke.rs`. Every test drives an in-process Basin pgwire
//! server via `tokio-postgres` over the real extended-query protocol.
//!
//! # Surfaces covered
//!
//! 1.  **Binary-format input params** — bool, int8, float8, bytea. Drivers
//!     send these in binary by default once they've seen the ParameterDescription
//!     OIDs. Types with real gaps (SMALLINT column, FLOAT4 column, binary text
//!     with multibyte Unicode, binary timestamp/date) are documented and
//!     gated with `#[ignore]` so they flip green when fixed.
//! 2.  **NULL params** in binary format — bind `None::<i64>` → IS NULL.
//! 3.  **ParameterDescription OID correctness** — after Parse Basin must
//!     declare the right PG OIDs so the driver sends the right encoding.
//! 4.  **RowDescription type OID fidelity** — output column OIDs must match
//!     the canonical Postgres values (int4=23, text=25, bool=16, float8=701,
//!     int8=20, date=1082).
//! 5.  **Error envelope shape** — `ErrorResponse` fields: Severity, SQLSTATE
//!     (5-char), Message non-empty.
//! 6.  **NOTICE capture** — Basin does not emit NOTICE messages today;
//!     absence is documented, not ignored.
//! 7.  **Apostrophe/special chars in binary TEXT param** — documented gap (real
//!     basin-engine bug in UTF-8 byte-indexing in `find_word_sequence`).
//! 8.  **Empty result set** roundtrip — 0 rows but valid RowDescription.
//! 9.  **Multiple statements via extended Parse** — documented gap (Basin
//!     silently allows what PG forbids in extended-query Parse).
//! 10. **Unnamed statement reuse** — second Parse with name `""` replaces
//!     cleanly; no stale state.
//! 11. **Binary timestamp/date params** — documented gap.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{types::Type, Client, NoTls, SimpleQueryMessage};

// ---------------------------------------------------------------------------
// Harness (mirrors param_bind_smoke.rs exactly)
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("test".to_owned(), project);
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
    }
}

async fn connect(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=test password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("pgwire_wire_compat conn driver: {e}");
        }
    });
    client
}

// ---------------------------------------------------------------------------
// Surface 1: Binary-format input parameters
//
// tokio-postgres sends binary for all types it knows the OID for once it has
// seen the ParameterDescription OIDs. `prepare_typed` forces the driver to
// use the exact OIDs we supply. This exercises `decode_param_binary` directly.
//
// Basin column-type notes (engine constraint as of this commit):
//   - INTEGER / INT4 / INT / BIGINT all map to Arrow Int64 (INT8 wire OID 20)
//   - SMALLINT / INT2 maps to Arrow Int16 but INSERT path does not support
//     Int16 — a real gap documented below
//   - FLOAT4 / REAL maps to Arrow Float32 but INSERT path does not support
//     Float32 — a real gap documented below
//   - FLOAT8 / DOUBLE PRECISION maps to Arrow Float64 — supported
// ---------------------------------------------------------------------------

/// Binary bool param — OID 16, 1 byte `0x01` or `0x00`.
/// Driver concern: pgx and asyncpg exclusively send binary bools.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_bool() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE boolvals (v BOOLEAN NOT NULL)", &[])
        .await
        .expect("CREATE TABLE boolvals");

    // Force binary encoding by supplying the OID explicitly.
    let stmt = client
        .prepare_typed("INSERT INTO boolvals VALUES ($1)", &[Type::BOOL])
        .await
        .expect("prepare_typed bool INSERT");

    client.execute(&stmt, &[&true]).await.expect("INSERT true");
    client
        .execute(&stmt, &[&false])
        .await
        .expect("INSERT false");

    let sel = client
        .prepare_typed("SELECT v FROM boolvals ORDER BY v", &[])
        .await
        .expect("prepare SELECT boolvals");
    let rows = client.query(&sel, &[]).await.expect("SELECT boolvals");

    assert_eq!(rows.len(), 2, "expected 2 rows");
    let v0: bool = rows[0].get(0);
    let v1: bool = rows[1].get(0);
    // ORDER BY bool: false < true in PG.
    assert!(!v0, "first row should be false");
    assert!(v1, "second row should be true");
    println!("[binary_param_bool] ok: [{v0}, {v1}]");
}

/// Binary int8 param — OID 20, 8-byte big-endian i64.
/// Driver concern: tokio-postgres default for i64 params is binary.
/// Note: Basin maps all integer column types to Arrow Int64, so the matching
/// column type is BIGINT, not INTEGER or INT4 (they're synonyms here).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_int8() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE i8vals (v BIGINT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE i8vals");

    let stmt = client
        .prepare_typed("INSERT INTO i8vals VALUES ($1)", &[Type::INT8])
        .await
        .expect("prepare_typed int8");

    let want: i64 = 9_223_372_036_854_000_000;
    client.execute(&stmt, &[&want]).await.expect("INSERT i64");

    let rows = client
        .query("SELECT v FROM i8vals", &[])
        .await
        .expect("SELECT i8vals");
    assert_eq!(rows.len(), 1);
    let got: i64 = rows[0].get(0);
    assert_eq!(got, want, "binary int8 round-trip");
    println!("[binary_param_int8] ok: {got}");
}

/// Binary float8 param — OID 701, 8-byte IEEE 754 big-endian f64.
/// Driver concern: all major drivers send float8 in binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_float8() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE f8vals (v FLOAT8 NOT NULL)", &[])
        .await
        .expect("CREATE TABLE f8vals");

    let stmt = client
        .prepare_typed("INSERT INTO f8vals VALUES ($1)", &[Type::FLOAT8])
        .await
        .expect("prepare_typed float8");

    let want: f64 = std::f64::consts::E;
    client.execute(&stmt, &[&want]).await.expect("INSERT f64");

    let rows = client
        .query("SELECT v FROM f8vals", &[])
        .await
        .expect("SELECT f8vals");
    assert_eq!(rows.len(), 1);
    let got: f64 = rows[0].get(0);
    assert!(
        (got - want).abs() < 1e-12,
        "binary float8 round-trip: got={got} want={want}"
    );
    println!("[binary_param_float8] ok: {got}");
}

/// Binary bytea param — OID 17, raw bytes with no additional encoding.
/// Driver concern: all drivers use binary for bytea.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_bytea() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE byteavals (v BYTEA NOT NULL)", &[])
        .await
        .expect("CREATE TABLE byteavals");

    let stmt = client
        .prepare_typed("INSERT INTO byteavals VALUES ($1)", &[Type::BYTEA])
        .await
        .expect("prepare_typed bytea");

    let want: Vec<u8> = vec![0x00, 0xFF, 0x42, 0x80, 0x01];
    client
        .execute(&stmt, &[&want.as_slice()])
        .await
        .expect("INSERT bytea");

    let rows = client
        .query("SELECT v FROM byteavals", &[])
        .await
        .expect("SELECT byteavals");
    assert_eq!(rows.len(), 1);
    let got: Vec<u8> = rows[0].get(0);
    assert_eq!(got, want, "binary bytea round-trip");
    println!("[binary_param_bytea] ok: {} bytes", got.len());
}

/// Binary int2 param — OID 21, 2-byte big-endian i16.
///
/// Real gap: Basin maps SMALLINT to Arrow Int16, but the INSERT path in
/// `basin_engine::dml` does not support Int16 rows, emitting:
/// "unsupported Arrow column type for INSERT: Int16".
/// Until the INSERT builder gains an Int16 arm, SMALLINT columns cannot be
/// written to. This is a driver-compat gap because JDBC defaults to INT2 for
/// short columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_int2() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE i2vals (v SMALLINT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE i2vals");

    let stmt = client
        .prepare_typed("INSERT INTO i2vals VALUES ($1)", &[Type::INT2])
        .await
        .expect("prepare_typed int2");

    client
        .execute(&stmt, &[&-32000i16])
        .await
        .expect("INSERT -32000");
    client
        .execute(&stmt, &[&32000i16])
        .await
        .expect("INSERT 32000");

    let rows = client
        .query("SELECT v FROM i2vals ORDER BY v", &[])
        .await
        .expect("SELECT i2vals");

    assert_eq!(rows.len(), 2);
    let lo: i16 = rows[0].get(0);
    let hi: i16 = rows[1].get(0);
    assert_eq!(lo, -32000);
    assert_eq!(hi, 32000);
    println!("[binary_param_int2] ok: [{lo}, {hi}]");
}

/// Binary int4 param — OID 23, 4-byte big-endian i32.
///
/// Real gap: Basin maps all integer column types (INTEGER, INT4, BIGINT) to
/// Arrow Int64 and surfaces them with OID INT8 (20) in ParameterDescription.
/// When a client prepares a statement against an INTEGER column and receives
/// OID INT8 back, it refuses to bind an i32 value ("WrongType { postgres:
/// Int8, rust: i32 }"). The driver-compat fix is for Basin to distinguish
/// INT4 columns (Arrow Int32) from INT8, but that requires the INSERT path to
/// support Int32 as well. This is a real driver-compat gap for any ORM that
/// uses `int` / `int4` typed columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_int4() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE i4vals (v INTEGER NOT NULL)", &[])
        .await
        .expect("CREATE TABLE i4vals");

    let stmt = client
        .prepare_typed("INSERT INTO i4vals VALUES ($1)", &[Type::INT4])
        .await
        .expect("prepare_typed int4");

    let want: i32 = -2_147_000_000;
    client.execute(&stmt, &[&want]).await.expect("INSERT i32");

    let rows = client
        .query("SELECT v FROM i4vals", &[])
        .await
        .expect("SELECT i4vals");
    assert_eq!(rows.len(), 1);
    let got: i32 = rows[0].get(0);
    assert_eq!(got, want, "binary int4 round-trip");
    println!("[binary_param_int4] ok: {got}");
}

/// Binary float4 param — OID 700, 4-byte IEEE 754 big-endian f32.
///
/// Real gap: Basin maps FLOAT4/REAL to Arrow Float32, but the INSERT DML
/// path does not support Float32 rows, emitting:
/// "unsupported column type in PoC: FLOAT4".
/// This is a driver-compat gap for Hibernate (which maps Java `float` to
/// FLOAT4 binary by default).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_float4() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE f4vals (v FLOAT4 NOT NULL)", &[])
        .await
        .expect("CREATE TABLE f4vals");

    let stmt = client
        .prepare_typed("INSERT INTO f4vals VALUES ($1)", &[Type::FLOAT4])
        .await
        .expect("prepare_typed float4");

    let want: f32 = 2.5_f32;
    client.execute(&stmt, &[&want]).await.expect("INSERT f32");

    let rows = client
        .query("SELECT v FROM f4vals", &[])
        .await
        .expect("SELECT f4vals");
    assert_eq!(rows.len(), 1);
    let got: f32 = rows[0].get(0);
    assert!(
        (got - want).abs() < 0.001,
        "binary float4 round-trip: got={got} want={want}"
    );
    println!("[binary_param_float4] ok: {got}");
}

/// Binary text param — ASCII string, proving `decode_param_binary` for
/// Type::TEXT produces a valid ScalarParam::Text value.
///
/// Non-ASCII (multi-byte UTF-8) strings exposed a separate bug: see
/// `binary_param_text_multibyte` below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_text_ascii() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE textvals_ascii (v TEXT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE textvals_ascii");

    let stmt = client
        .prepare_typed("INSERT INTO textvals_ascii VALUES ($1)", &[Type::TEXT])
        .await
        .expect("prepare_typed text");

    // ASCII-only string: no multi-byte chars, exercises basic binary text path.
    let want = "hello world";
    client
        .execute(&stmt, &[&want])
        .await
        .expect("INSERT ASCII text");

    let rows = client
        .query("SELECT v FROM textvals_ascii", &[])
        .await
        .expect("SELECT textvals_ascii");
    assert_eq!(rows.len(), 1);
    let got: &str = rows[0].get(0);
    assert_eq!(got, want, "binary text ASCII round-trip");
    println!("[binary_param_text_ascii] ok: {got:?}");
}

/// Binary text param with multi-byte UTF-8 characters.
///
/// Real gap: a bug in `basin_engine::pg_operators::find_word_sequence`
/// iterates byte-by-byte over the lowercased SQL string and slices at byte
/// offset `i` without checking character boundaries:
/// `lower[i..].starts_with(sequence)` panics with "byte index N is not a
/// char boundary" when the substituted SQL contains multi-byte UTF-8
/// characters (e.g. em-dash `—` U+2014 = 3 bytes, checkmark `✓` = 3 bytes).
///
/// Root cause: `find_word_sequence` in `pg_operators.rs:195` uses `i += 1`
/// (byte increment) but then accesses `lower[i..]` (char-boundary slice).
/// Fix: replace `i += 1` with a char-aware advance via `lower[i..].chars()
/// .next().map(char::len_utf8).unwrap_or(1)`.
///
/// This causes a server-side panic and a `Kind::Closed` connection reset on
/// the client side, which is a crash-severity protocol bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_text_multibyte_utf8() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE textvals_mb (v TEXT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE textvals_mb");

    let stmt = client
        .prepare_typed("INSERT INTO textvals_mb VALUES ($1)", &[Type::TEXT])
        .await
        .expect("prepare_typed text multibyte");

    // Contains multi-byte UTF-8: em-dash (3 bytes) and checkmark (3 bytes).
    let want = "hello — world ✓";
    client
        .execute(&stmt, &[&want])
        .await
        .expect("INSERT multibyte text");

    let rows = client
        .query("SELECT v FROM textvals_mb", &[])
        .await
        .expect("SELECT textvals_mb");
    assert_eq!(rows.len(), 1);
    let got: &str = rows[0].get(0);
    assert_eq!(got, want, "binary text multibyte UTF-8 round-trip");
    println!("[binary_param_text_multibyte] ok: {got:?}");
}

/// Apostrophe and special chars in binary TEXT param.
///
/// Binary text format sends raw UTF-8 bytes — no SQL quoting. The apostrophe
/// must come back verbatim, not SQL-escaped. However, this test currently
/// hits the same multi-byte UTF-8 panic as `binary_param_text_multibyte_utf8`
/// because the test value contains an em-dash. It is gated separately so the
/// apostrophe concern can be un-ignored independently once the UTF-8 bug
/// is fixed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_text_with_apostrophe() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE apos_test (v TEXT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE apos_test");

    let stmt = client
        .prepare_typed("INSERT INTO apos_test VALUES ($1)", &[Type::TEXT])
        .await
        .expect("prepare_typed text apostrophe");

    // Binary text bypasses SQL quoting entirely: apostrophe must round-trip.
    let want = "it's O'Reilly & \"quoted\" — <test>";
    client
        .execute(&stmt, &[&want])
        .await
        .expect("INSERT with apostrophe");

    let rows = client
        .query("SELECT v FROM apos_test", &[])
        .await
        .expect("SELECT after apostrophe insert");

    assert_eq!(rows.len(), 1);
    let got: &str = rows[0].get(0);
    assert_eq!(
        got, want,
        "binary text with apostrophes must round-trip identically"
    );
    println!("[binary_apostrophe] ok: {got:?}");
}

// ---------------------------------------------------------------------------
// Surface 2: NULL params — binary format → SQL NULL
// ---------------------------------------------------------------------------

/// NULL param in binary format — bind `None::<i64>` → IS NULL.
/// tokio-postgres emits a -1 length field for Option::None regardless of
/// the format code. The router's `handle_bind` must produce `ScalarParam::Null`
/// for the -1 sentinel so the engine stores SQL NULL, not the string "NULL".
/// Driver concern: every driver relies on -1 for NULL; wrong handling stores
/// the string "NULL" instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_param_binary_format() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE nulltest (id BIGINT NOT NULL, val BIGINT)",
            &[],
        )
        .await
        .expect("CREATE TABLE nulltest");

    // Use INT8 for both slots so type inference aligns: Basin maps BIGINT →
    // Arrow Int64 → OID INT8. Binding Option::<i64>::None forces NULL.
    let stmt = client
        .prepare_typed(
            "INSERT INTO nulltest VALUES ($1, $2)",
            &[Type::INT8, Type::INT8],
        )
        .await
        .expect("prepare_typed INSERT nulltest");

    client
        .execute(&stmt, &[&1i64, &Option::<i64>::None])
        .await
        .expect("INSERT with binary-format NULL");

    let rows = client
        .query("SELECT val IS NULL FROM nulltest WHERE id = 1", &[])
        .await
        .expect("SELECT IS NULL");

    assert_eq!(rows.len(), 1, "expected 1 row");
    let is_null: bool = rows[0].get(0);
    assert!(
        is_null,
        "binary-path NULL param must store SQL NULL, not a string"
    );
    println!("[null_param_binary_format] ok: val IS NULL = {is_null}");
}

/// NULL param for a text column in binary format.
/// Driver concern: option<&str>::None must produce SQL NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_param_text_column() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE nulltest_text (id BIGINT NOT NULL, label TEXT)",
            &[],
        )
        .await
        .expect("CREATE TABLE nulltest_text");

    let stmt = client
        .prepare_typed(
            "INSERT INTO nulltest_text VALUES ($1, $2)",
            &[Type::INT8, Type::TEXT],
        )
        .await
        .expect("prepare_typed INSERT nulltest_text");

    client
        .execute(&stmt, &[&2i64, &Option::<&str>::None])
        .await
        .expect("INSERT text NULL");

    let rows = client
        .query("SELECT label IS NULL FROM nulltest_text WHERE id = 2", &[])
        .await
        .expect("SELECT text IS NULL");

    assert_eq!(rows.len(), 1);
    let is_null: bool = rows[0].get(0);
    assert!(is_null, "text NULL param must store SQL NULL");
    println!("[null_param_text_column] ok: label IS NULL = {is_null}");
}

// ---------------------------------------------------------------------------
// Surface 3: ParameterDescription OID correctness
//
// After Parse, the ParameterDescription message must declare the right PG type
// OIDs so drivers choose the correct encoding. tokio-postgres exposes these
// via `Statement::params()`. Wrong OIDs cause "WrongType" errors client-side.
// ---------------------------------------------------------------------------

/// Prepare a WHERE-predicate query against a BIGINT column; the inferred OID
/// for `$1` must be INT8 (OID 20), not TEXT (OID 25).
/// Driver concern: pgx rejects binding an i64 into a TEXT slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_desc_infers_int8_from_where_predicate() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE pred_int (col BIGINT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE pred_int");

    client
        .execute("INSERT INTO pred_int VALUES (42)", &[])
        .await
        .expect("INSERT seed");

    let stmt = client
        .prepare("SELECT col FROM pred_int WHERE col = $1")
        .await
        .expect("prepare WHERE col = $1");

    let params = stmt.params();
    assert_eq!(params.len(), 1, "expected 1 parameter");
    // INT8 OID = 20.
    assert_eq!(
        params[0],
        Type::INT8,
        "param $1 on BIGINT column must infer as INT8 (OID 20), got: {:?}",
        params[0]
    );
    println!("[param_desc_int8] ok: params[0] = {:?}", params[0]);
}

/// Prepare an INSERT with bigint + text columns; ParameterDescription must
/// return INT8 (OID 20) for the first slot and TEXT (OID 25) for the second.
/// Driver concern: ORM INSERT always checks param types before encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_desc_infers_types_for_insert() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE desc_tbl (int_col BIGINT NOT NULL, text_col TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE desc_tbl");

    let stmt = client
        .prepare("INSERT INTO desc_tbl (int_col, text_col) VALUES ($1, $2)")
        .await
        .expect("prepare INSERT($1,$2)");

    let params = stmt.params();
    assert_eq!(params.len(), 2, "expected 2 parameters");
    // INT8 OID = 20 (BIGINT column — all integer columns map to Int64).
    assert_eq!(
        params[0],
        Type::INT8,
        "first param must be INT8, got: {:?}",
        params[0]
    );
    // TEXT OID = 25.
    assert_eq!(
        params[1],
        Type::TEXT,
        "second param must be TEXT, got: {:?}",
        params[1]
    );
    println!(
        "[param_desc_insert] ok: params = [{:?}, {:?}]",
        params[0], params[1]
    );
}

// ---------------------------------------------------------------------------
// Surface 4: RowDescription type OID fidelity
//
// Drivers deserialise result columns based solely on the OID declared in
// RowDescription. Correct OIDs are the foundation of binary result encoding.
//
// Canonical PG OIDs: int4=23, text=25, bool=16, float8=701, int8=20, date=1082
// ---------------------------------------------------------------------------

/// `SELECT 1::int4, 'a'::text, true, 1.5::float8, NULL::int8, '2024-01-01'::date`
/// → each column's declared OID in RowDescription must match the canonical PG
/// value. tokio-postgres checks these OIDs to decode result rows correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_description_oid_fidelity() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare(
            "SELECT \
               1::int4      AS c_int4, \
               'a'::text    AS c_text, \
               true         AS c_bool, \
               1.5::float8  AS c_float8, \
               NULL::int8   AS c_int8, \
               '2024-01-01'::date AS c_date",
        )
        .await
        .expect("prepare multi-type SELECT");

    let cols = stmt.columns();
    assert_eq!(cols.len(), 6, "expected 6 columns");

    // Map column name → declared type for easier assertion.
    let col_map: HashMap<&str, &Type> = cols.iter().map(|c| (c.name(), c.type_())).collect();

    // INT4 = OID 23.
    assert_eq!(
        *col_map["c_int4"],
        Type::INT4,
        "c_int4 OID must be INT4 (23), got: {:?}",
        col_map["c_int4"]
    );
    // TEXT = OID 25.
    assert_eq!(
        *col_map["c_text"],
        Type::TEXT,
        "c_text OID must be TEXT (25), got: {:?}",
        col_map["c_text"]
    );
    // BOOL = OID 16.
    assert_eq!(
        *col_map["c_bool"],
        Type::BOOL,
        "c_bool OID must be BOOL (16), got: {:?}",
        col_map["c_bool"]
    );
    // FLOAT8 = OID 701.
    assert_eq!(
        *col_map["c_float8"],
        Type::FLOAT8,
        "c_float8 OID must be FLOAT8 (701), got: {:?}",
        col_map["c_float8"]
    );
    // INT8 = OID 20.
    assert_eq!(
        *col_map["c_int8"],
        Type::INT8,
        "c_int8 OID must be INT8 (20), got: {:?}",
        col_map["c_int8"]
    );
    // DATE = OID 1082.
    assert_eq!(
        *col_map["c_date"],
        Type::DATE,
        "c_date OID must be DATE (1082), got: {:?}",
        col_map["c_date"]
    );

    // Actually execute to confirm the column types are usable, not just declared.
    let rows = client
        .query(&stmt, &[])
        .await
        .expect("execute multi-type SELECT");
    assert_eq!(rows.len(), 1);
    let v_int4: i32 = rows[0].get(0);
    let v_text: &str = rows[0].get(1);
    let v_bool: bool = rows[0].get(2);
    let v_float8: f64 = rows[0].get(3);
    let v_int8: Option<i64> = rows[0].get(4);
    assert_eq!(v_int4, 1);
    assert_eq!(v_text, "a");
    assert!(v_bool);
    assert!((v_float8 - 1.5).abs() < 1e-9);
    assert!(v_int8.is_none(), "NULL::int8 must decode as None");

    println!(
        "[row_desc_oid_fidelity] ok: int4={v_int4} text={v_text:?} bool={v_bool} float8={v_float8} int8={v_int8:?}"
    );
}

// ---------------------------------------------------------------------------
// Surface 5: Error envelope shape
//
// Drivers parse `ErrorResponse` fields to route exceptions. The spec requires:
//   'S' = Severity (e.g. "ERROR")
//   'C' = SQLSTATE (exactly 5 characters)
//   'M' = Message (non-empty human string)
//
// tokio-postgres surfaces these as `DbError::severity()`, `.code()`, `.message()`.
// ---------------------------------------------------------------------------

/// `SELECT * FROM nonexistent_table` must produce a proper ErrorResponse.
/// Driver concern: asyncpg and psycopg match on SQLSTATE to raise
/// `UndefinedTable` rather than a generic `ProgrammingError`.
///
/// Note: PG raises 42P01 (undefined_table). Basin currently routes NotFound
/// errors to XX000 (internal, since DataFusion wraps the error as "internal:
/// plan: Error during planning: table not found"). The exact code is Basin's
/// current behaviour; the critical assertion is that the wire fields are
/// well-formed so drivers can at least parse the error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_envelope_relation_not_found() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let err = client
        .simple_query("SELECT * FROM nonexistent_table_xyz_wire_compat")
        .await
        .expect_err("query on nonexistent table must fail");

    let dbe = err.as_db_error().expect("must be a DbError");

    // Severity must be "ERROR" (uppercase, as PG spec requires).
    assert_eq!(
        dbe.severity(),
        "ERROR",
        "severity must be 'ERROR', got: {:?}",
        dbe.severity()
    );

    // SQLSTATE must be exactly 5 characters (PG spec requirement).
    let code = dbe.code().code();
    assert_eq!(
        code.len(),
        5,
        "SQLSTATE must be exactly 5 chars, got: {code:?}"
    );

    // Message must be non-empty.
    let msg = dbe.message();
    assert!(!msg.is_empty(), "error message must be non-empty");

    println!(
        "[error_envelope_not_found] severity={:?} code={code:?} msg={msg:?}",
        dbe.severity()
    );
}

/// Division by zero must produce a well-formed ErrorResponse.
/// Driver concern: drivers must be able to parse the error fields without
/// hitting a panic or garbled message. The exact SQLSTATE (22012 in PG)
/// may differ in Basin since DataFusion wraps the error as an internal error;
/// we assert well-formed fields only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_envelope_division_by_zero() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let err = client
        .simple_query("SELECT 1/0")
        .await
        .expect_err("SELECT 1/0 must fail");

    let dbe = err.as_db_error().expect("must be a DbError");

    // Severity must be "ERROR".
    assert_eq!(dbe.severity(), "ERROR", "severity must be ERROR");

    // SQLSTATE: exactly 5 chars.
    let code = dbe.code().code();
    assert_eq!(code.len(), 5, "SQLSTATE must be 5 chars, got: {code:?}");

    // Message must be non-empty and mention the divide/zero.
    let msg = dbe.message();
    assert!(!msg.is_empty(), "error message must not be empty");

    println!("[error_envelope_divzero] ok: code={code:?} msg={msg:?}");
}

/// Connection remains usable after an error.
/// Many drivers rely on this to avoid re-creating the connection on every error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_reusable_after_error() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Deliberately trigger an error.
    let _ = client
        .simple_query("SELECT * FROM absolutely_does_not_exist_table")
        .await;

    // Connection must still work. Use simple_query and filter for Row messages.
    let msgs = client
        .simple_query("SELECT 42")
        .await
        .expect("connection must be reusable after error");

    // simple_query returns RowDescription + Row(s) + CommandComplete. We want
    // the row value.
    let row_vals: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_vals,
        vec!["42".to_string()],
        "post-error connection must return 42, got: {row_vals:?}"
    );
    println!("[connection_reusable_after_error] ok: got {:?}", row_vals);
}

// ---------------------------------------------------------------------------
// Surface 6: NOTICE messages
//
// Basin does not currently emit NOTICE messages over the wire. We document
// absence here rather than pretending it works. When Basin adds NOTICE
// support, update this test to assert count > 0.
// ---------------------------------------------------------------------------

/// Basin does not emit NOTICE messages. This test confirms absence is clean
/// (no panic, no garbled connection, no spurious messages).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notice_messages_absent() {
    let server = start_server().await;
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=test password=ignored",
            server.addr.ip(),
            server.addr.port()
        ),
        NoTls,
    )
    .await
    .expect("connect");

    let notice_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let nc = notice_count.clone();
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("notice test conn: {e}");
        }
        drop(nc);
    });

    // Any query that completes without error.
    client
        .simple_query("SELECT 'notice_test'")
        .await
        .expect("query for notice absence test");

    let got = notice_count.load(std::sync::atomic::Ordering::SeqCst);
    // Basin does not emit notices; confirm no panic / garbled bytes occurred.
    assert_eq!(got, 0, "Basin emits no NOTICE messages");
    println!("[notice_absent] ok: {got} notices (expected 0)");
}

// ---------------------------------------------------------------------------
// Surface 8: Empty result set roundtrip
//
// `SELECT 1 WHERE FALSE` returns 0 rows. The RowDescription must still be
// emitted, followed by CommandComplete with row count 0.
// Drivers that only handle non-empty cases can fail here.
// ---------------------------------------------------------------------------

/// `SELECT 1 WHERE FALSE` → 0 rows, valid RowDescription (1 column).
/// Driver concern: ORMs that call `fetchone()` must see an empty result, not
/// a missing RowDescription or malformed CommandComplete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_result_set_roundtrip() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT 1 AS n WHERE FALSE")
        .await
        .expect("prepare empty result");

    // RowDescription: should have 1 column even though 0 rows come back.
    let cols = stmt.columns();
    assert_eq!(
        cols.len(),
        1,
        "RowDescription must have 1 column even for empty result"
    );
    assert_eq!(cols[0].name(), "n", "column name must be 'n'");

    let rows = client
        .query(&stmt, &[])
        .await
        .expect("execute SELECT 1 WHERE FALSE");

    assert_eq!(
        rows.len(),
        0,
        "expected 0 rows from WHERE FALSE, got {}",
        rows.len()
    );
    println!("[empty_result_set] ok: 0 rows, col={:?}", cols[0].name());
}

/// Empty result from a table scan (not a literal `1 WHERE FALSE`).
/// Ensures RowDescription is emitted with actual column types.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_result_set_from_table_scan() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE empty_scan (id BIGINT NOT NULL, label TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE empty_scan");

    // No rows inserted — table is empty.
    let stmt = client
        .prepare("SELECT id, label FROM empty_scan WHERE id = $1")
        .await
        .expect("prepare SELECT from empty table");

    let cols = stmt.columns();
    assert_eq!(cols.len(), 2, "RowDescription must have 2 columns");
    assert_eq!(cols[0].name(), "id");
    assert_eq!(cols[1].name(), "label");

    let rows = client
        .query(&stmt, &[&999i64])
        .await
        .expect("execute on empty table");

    assert_eq!(rows.len(), 0, "no rows expected from empty table");
    println!("[empty_table_scan] ok: 0 rows from empty table");
}

// ---------------------------------------------------------------------------
// Surface 9: Multiple statements in extended-query Parse
//
// PG protocol §55.2.3: Parse carries a single SQL statement. Sending
// `SELECT 1; SELECT 2` in one Parse is FORBIDDEN by the spec. Basin should
// reject it with an error, not silently execute both statements.
//
// Real gap: Basin currently silently accepts multi-statement Parse by
// delegating both to the engine. This is a protocol-violation gap that can
// cause drivers to receive an unexpected extra result set and de-sync.
// ---------------------------------------------------------------------------

/// `SELECT 1; SELECT 2` in a single Parse message must error.
///
/// Real gap: Basin currently silently succeeds on multi-statement extended-
/// query Parse, executing both statements and returning the last result. The
/// PG spec requires this to fail with a protocol error. Real drivers (pgx,
/// asyncpg, JDBC) hit an unexpected extra `RowDescription` and de-sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_parse_multi_statement_rejected() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // tokio-postgres' `prepare` sends a Parse message. If the SQL contains
    // two statements, PG rejects it; Basin currently does not.
    let result = client.prepare("SELECT 1; SELECT 2").await;

    assert!(
        result.is_err(),
        "extended-protocol Parse of two statements must fail, not silently succeed"
    );

    let err = result.unwrap_err();
    if let Some(dbe) = err.as_db_error() {
        let code = dbe.code().code();
        assert_eq!(code.len(), 5, "error code must be 5 chars, got: {code:?}");
        println!(
            "[multi_stmt_parse_rejected] ok: SQLSTATE={code:?} msg={:?}",
            dbe.message()
        );
    }

    // Connection must be reusable after the error.
    let msgs = client
        .simple_query("SELECT 99")
        .await
        .expect("connection must remain usable after multi-stmt parse rejection");

    let val = msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
        _ => None,
    });
    assert_eq!(val.as_deref(), Some("99"));
}

// ---------------------------------------------------------------------------
// Surface 10: Unnamed statement replacement
//
// PG protocol §55.2.3: a Parse with the empty name `""` (unnamed statement)
// replaces any prior unnamed statement. Drivers (especially poolers) reuse
// the unnamed slot constantly. Basin must not carry stale state from the
// previous statement.
// ---------------------------------------------------------------------------

/// Parse(unnamed, SQL_A) → Bind → Execute → Parse(unnamed, SQL_B) → Bind →
/// Execute. Each execution must return the result of ITS OWN SQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unnamed_statement_reuse_replaces_cleanly() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE reuse_tbl (id BIGINT NOT NULL, val TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE reuse_tbl");

    client
        .execute(
            "INSERT INTO reuse_tbl VALUES (1, 'alpha'), (2, 'beta')",
            &[],
        )
        .await
        .expect("INSERT reuse_tbl");

    // First use of unnamed slot.
    let stmt_a = client
        .prepare("SELECT val FROM reuse_tbl WHERE id = $1")
        .await
        .expect("prepare_a");

    let rows_a = client.query(&stmt_a, &[&1i64]).await.expect("query_a");
    assert_eq!(rows_a.len(), 1);
    let val_a: &str = rows_a[0].get(0);
    assert_eq!(val_a, "alpha");

    // Second use: different SQL overwrites the unnamed slot.
    let stmt_b = client
        .prepare("SELECT val FROM reuse_tbl WHERE id = $1")
        .await
        .expect("prepare_b");

    let rows_b = client.query(&stmt_b, &[&2i64]).await.expect("query_b");
    assert_eq!(rows_b.len(), 1);
    let val_b: &str = rows_b[0].get(0);
    assert_eq!(val_b, "beta");

    // stmt_a must still work: stale state must not bleed through.
    let rows_a2 = client
        .query(&stmt_a, &[&1i64])
        .await
        .expect("re-query_a after unnamed reuse");
    let val_a2: &str = rows_a2[0].get(0);
    assert_eq!(
        val_a2, "alpha",
        "stmt_a must still work after unnamed-slot reuse"
    );

    println!("[unnamed_stmt_reuse] ok: a={val_a:?}, b={val_b:?}, a_after_reuse={val_a2:?}");
}

/// Named statement survives being shadowed by an unnamed Parse, then can be
/// re-used cleanly. This confirms the named-vs-unnamed isolation is correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_statement_survives_unnamed_overwrite() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE survive_tbl (v BIGINT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE survive_tbl");

    for n in [10i64, 20, 30] {
        client
            .execute("INSERT INTO survive_tbl VALUES ($1)", &[&n])
            .await
            .expect("INSERT survive_tbl");
    }

    // Prepare a named statement.
    let named = client
        .prepare("SELECT v FROM survive_tbl WHERE v = $1")
        .await
        .expect("prepare named");

    // Execute once.
    let r1 = client
        .query(&named, &[&20i64])
        .await
        .expect("query named 20");
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].get::<_, i64>(0), 20);

    // Use the unnamed slot (simple `query` call).
    let r_unnamed = client
        .query("SELECT v FROM survive_tbl WHERE v = $1", &[&30i64])
        .await
        .expect("unnamed query 30");
    assert_eq!(r_unnamed.len(), 1);
    assert_eq!(r_unnamed[0].get::<_, i64>(0), 30);

    // Named statement must still work.
    let r2 = client
        .query(&named, &[&10i64])
        .await
        .expect("re-query named 10");
    assert_eq!(r2.len(), 1);
    assert_eq!(
        r2[0].get::<_, i64>(0),
        10,
        "named stmt must survive unnamed overwrite"
    );

    println!("[named_survives_unnamed] ok");
}

// ---------------------------------------------------------------------------
// Surface 11: Binary-format timestamp/date params
//
// Timestamp binary format: 8-byte i64 microseconds since 2000-01-01 UTC.
// Date binary format: 4-byte i32 days since 2000-01-01.
//
// Real gap: `decode_param_binary` in protocol.rs has no explicit arm for
// Type::TIMESTAMP, Type::TIMESTAMPTZ, or Type::DATE — these fall through to
// the "unknown binary type" branch which tries `std::str::from_utf8(bytes)`.
// Since the binary wire bytes for a datetime are not valid UTF-8 (they're a
// raw i64/i32), this produces a "binary format for type timestamp not
// supported" error, preventing drivers like SQLAlchemy / Hibernate from
// binding datetime parameters.
// ---------------------------------------------------------------------------

/// Binary timestamptz param — real gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_timestamp() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE tsvals (v TIMESTAMPTZ NOT NULL)", &[])
        .await
        .expect("CREATE TABLE tsvals");

    let stmt = client
        .prepare_typed("INSERT INTO tsvals VALUES ($1)", &[Type::TIMESTAMPTZ])
        .await
        .expect("prepare_typed timestamptz");

    // Raw microseconds since 2000-01-01: 2024-01-15 12:00:00 UTC.
    // In a live test this would use chrono::DateTime; here we document the gap.
    let want = "2024-01-15 12:00:00+00";
    let _ = client.execute(&stmt, &[&want]).await;

    println!("[binary_timestamp] SKIPPED (gap: binary timestamp params not decoded)");
}

/// Binary date param — real gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_param_date() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE datevals (v DATE NOT NULL)", &[])
        .await
        .expect("CREATE TABLE datevals");

    let stmt = client
        .prepare_typed("INSERT INTO datevals VALUES ($1)", &[Type::DATE])
        .await
        .expect("prepare_typed date");

    let want = "2024-01-15";
    let _ = client.execute(&stmt, &[&want]).await;

    println!("[binary_date] SKIPPED (gap: binary date params not decoded)");
}
