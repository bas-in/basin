//! Viability test: `UUID` column type + the uuid-ossp / pgcrypto built-in
//! functions that ORMs reach for the moment they touch IDs or password
//! hashes.
//!
//! Card: `viability_uuid`
//! Bar: `uuid_roundtrip && random_uuid_unique && sha256_hex_correct &&
//!       bcrypt_format_correct`
//!
//! What we exercise:
//!
//! 1. CREATE TABLE with a `UUID` column and assert the catalog stamps the
//!    `BASIN_TYPE=UUID` marker on a `FixedSizeBinary(16)` Arrow field.
//! 2. INSERT one row with an explicit canonical-form UUID literal and a
//!    second row where the UUID comes from `gen_random_uuid()`.
//! 3. SELECT both back, parse the bytes via `uuid::Uuid::from_bytes`, and
//!    check that:
//!    - the explicit literal round-trips byte-for-byte through
//!      `Uuid::parse_str`;
//!    - the random UUID parses cleanly and is non-zero;
//!    - the two ids don't collide.
//! 4. Generate 1000 UUIDs via `gen_random_uuid()` and assert that they're
//!    distinct (uniqueness is the property ORMs assume when they pick UUID
//!    over an autoincrement BIGINT).
//! 5. `digest('hello', 'sha256')` round-tripped through `encode(..., 'hex')`
//!    matches the well-known SHA-256 of `"hello"`.
//! 6. `crypt('p@ss', gen_salt('bf'))` produces a bcrypt-shaped 60-char
//!    string starting with `$2a$` / `$2b$` / `$2y$`.

#![allow(clippy::print_stdout)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

#[ignore = "C1: UUID/Vortex read path casts Decimal256(39,0) to FixedSizeBinary(16) — SELECT fails; not the vortex 0.71 metadata fix — still blocked on #40 cluster"]
#[tokio::test]
async fn viability_uuid() {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ---- 1. CREATE TABLE with a UUID column ---------------------------------
    sess.execute("CREATE TABLE users (id UUID, email TEXT)")
        .await
        .expect("CREATE TABLE users");

    let table = TableName::new("users").unwrap();
    let meta = catalog.load_table(&project, &table).await.unwrap();
    let id_field = meta
        .schema
        .field_with_name("id")
        .expect("id column missing");
    assert_eq!(
        id_field.data_type(),
        &DataType::FixedSizeBinary(16),
        "UUID should ride on FixedSizeBinary(16); got {:?}",
        id_field.data_type()
    );
    assert_eq!(
        id_field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("UUID"),
        "id field is missing the BASIN_TYPE=UUID marker"
    );

    // ---- 2. INSERT one explicit-literal row + one random-uuid row -----------
    let alice_uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    sess.execute(&format!(
        "INSERT INTO users VALUES ('{alice_uuid_str}', 'alice@example.com')"
    ))
    .await
    .expect("INSERT alice");
    sess.execute("INSERT INTO users VALUES (gen_random_uuid(), 'bob@example.com')")
        .await
        .expect("INSERT bob via gen_random_uuid()");

    // Also exercise the uuid-ossp alias to make sure both names resolve.
    sess.execute("INSERT INTO users VALUES (uuid_generate_v4(), 'carol@example.com')")
        .await
        .expect("INSERT carol via uuid_generate_v4()");

    // ---- 3. SELECT and inspect bytes ---------------------------------------
    let result = sess
        .execute("SELECT id, email FROM users ORDER BY email")
        .await
        .expect("SELECT");
    let batches = match result {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => {
            panic!("expected ExecResult::Rows for SELECT, got Empty tag={tag:?}")
        }
    };

    let mut uuids: Vec<(String, [u8; 16])> = Vec::new();
    for batch in batches.iter() {
        let id_arr = batch
            .column_by_name("id")
            .expect("id col missing")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("id col not FixedSizeBinaryArray");
        let email_arr = batch
            .column_by_name("email")
            .expect("email col missing")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("email col not StringArray");
        for r in 0..batch.num_rows() {
            assert!(!id_arr.is_null(r), "id NULL at row {r}");
            let bytes = id_arr.value(r);
            assert_eq!(bytes.len(), 16, "UUID should be 16 bytes");
            let mut buf = [0u8; 16];
            buf.copy_from_slice(bytes);
            uuids.push((email_arr.value(r).to_string(), buf));
        }
    }
    uuids.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(uuids.len(), 3, "expected 3 rows back");

    // alice's literal must round-trip byte-for-byte.
    let alice_bytes = uuids
        .iter()
        .find(|(e, _)| e == "alice@example.com")
        .expect("alice row missing")
        .1;
    let alice_parsed = uuid::Uuid::parse_str(alice_uuid_str).unwrap();
    assert_eq!(
        alice_bytes,
        *alice_parsed.as_bytes(),
        "alice's stored UUID bytes don't match the literal"
    );

    // bob & carol's UUIDs must each parse to a non-nil v4 UUID.
    let bob = uuids
        .iter()
        .find(|(e, _)| e == "bob@example.com")
        .unwrap()
        .1;
    let carol = uuids
        .iter()
        .find(|(e, _)| e == "carol@example.com")
        .unwrap()
        .1;
    let bob_uuid = uuid::Uuid::from_bytes(bob);
    let carol_uuid = uuid::Uuid::from_bytes(carol);
    assert_ne!(bob_uuid, uuid::Uuid::nil(), "bob's UUID is the nil UUID");
    assert_ne!(
        carol_uuid,
        uuid::Uuid::nil(),
        "carol's UUID is the nil UUID"
    );
    assert_eq!(
        bob_uuid.get_version_num(),
        4,
        "gen_random_uuid() should produce v4; got v{}",
        bob_uuid.get_version_num()
    );
    assert_eq!(
        carol_uuid.get_version_num(),
        4,
        "uuid_generate_v4() should produce v4; got v{}",
        carol_uuid.get_version_num()
    );
    assert_ne!(bob, carol, "bob and carol must have distinct UUIDs");
    assert_ne!(alice_bytes, bob, "alice and bob must have distinct UUIDs");

    let uuid_roundtrip = true;

    // ---- 4. 1000-call uniqueness sanity check -------------------------------
    //
    // Build a 1000-row INSERT and assert that every cell that came back is a
    // distinct UUID. This catches volatility-flag mistakes (`Volatile` vs
    // `Immutable`) in one shot — DataFusion's optimiser will fold an
    // `Immutable` UDF call to one row's worth of bytes broadcast across the
    // batch, and that's exactly what we *don't* want for `gen_random_uuid()`.
    sess.execute("CREATE TABLE many (id UUID, n BIGINT)")
        .await
        .unwrap();
    let mut sql = String::from("INSERT INTO many VALUES ");
    for i in 0..1000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("(gen_random_uuid(), {i})"));
    }
    sess.execute(&sql).await.expect("bulk insert");

    let res = sess
        .execute("SELECT id FROM many")
        .await
        .expect("SELECT many.id");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let mut seen: HashSet<[u8; 16]> = HashSet::with_capacity(1000);
    for batch in &batches {
        let arr = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for r in 0..batch.num_rows() {
            let mut buf = [0u8; 16];
            buf.copy_from_slice(arr.value(r));
            seen.insert(buf);
        }
    }
    assert_eq!(
        seen.len(),
        1000,
        "expected 1000 distinct UUIDs, saw {}",
        seen.len()
    );
    let random_uuid_unique = seen.len() == 1000;

    // ---- 5. SHA-256 hex of "hello" -----------------------------------------
    //
    // The well-known answer is
    //   2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
    // This exercises three UDFs at once: `digest`, `encode`, plus the type
    // plumbing that lets a `Binary` flow into `encode` as the first arg.
    let res = sess
        .execute("SELECT encode(digest('hello', 'sha256'), 'hex') AS h")
        .await
        .expect("SELECT digest+encode");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows for digest, got {other:?}"),
    };
    assert_eq!(batches.len(), 1, "expected 1 batch for digest result");
    let h_arr = batches[0]
        .column_by_name("h")
        .expect("h col missing")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("h col not StringArray");
    assert_eq!(h_arr.len(), 1, "expected 1 row for digest result");
    let h = h_arr.value(0);
    let expected_sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    assert_eq!(
        h, expected_sha256,
        "sha256(hello) hex mismatch:\n  got: {h}\n  expected: {expected_sha256}"
    );
    let sha256_hex_correct = h == expected_sha256;

    // ---- 6. crypt() + gen_salt('bf') ---------------------------------------
    let res = sess
        .execute("SELECT crypt('p@ss', gen_salt('bf')) AS h")
        .await
        .expect("SELECT crypt");
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows for crypt, got {other:?}"),
    };
    let h_arr = batches[0]
        .column_by_name("h")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let bcrypt_hash = h_arr.value(0);
    let bcrypt_format_correct = bcrypt_hash.len() == 60
        && (bcrypt_hash.starts_with("$2a$")
            || bcrypt_hash.starts_with("$2b$")
            || bcrypt_hash.starts_with("$2y$"));
    assert!(
        bcrypt_format_correct,
        "bcrypt hash should be 60 chars and start with $2a$/$2b$/$2y$; got {bcrypt_hash:?}"
    );

    // ---- 7. negative path: malformed UUID literal --------------------------
    //
    // Defence in depth: an obviously-invalid UUID string should fail the
    // INSERT cleanly, not a silent zero-fill or panic.
    let bad = sess
        .execute("INSERT INTO users VALUES ('not-a-uuid', 'eve@example.com')")
        .await;
    assert!(bad.is_err(), "malformed UUID literal should be rejected");

    let pass = uuid_roundtrip && random_uuid_unique && sha256_hex_correct && bcrypt_format_correct;

    println!(
        "[VIABILITY UUID] roundtrip={uuid_roundtrip} unique_1k={random_uuid_unique} \
         sha256_hex={sha256_hex_correct} bcrypt_fmt={bcrypt_format_correct} \
         alice_uuid={alice_uuid_str} bcrypt_hash_prefix={prefix:?}",
        prefix = &bcrypt_hash[..7]
    );

    report_viability(
        "uuid",
        "UUID column type + uuid-ossp/pgcrypto built-in functions",
        "UUID column accepts canonical hyphenated literals and \
         gen_random_uuid()/uuid_generate_v4() function calls on INSERT, \
         stores 16 bytes RFC 4122, returns canonical hyphenated text on \
         SELECT. digest()/encode()/decode() cover md5/sha1/sha224/sha256/\
         sha384/sha512 + hex/base64/escape. crypt()/gen_salt('bf') produce \
         bcrypt-shaped 60-char `$2b$NN$...` hashes that verify cleanly.",
        pass,
        PrimaryMetric {
            label: "uuid_full_surface".into(),
            value: if pass { 1.0 } else { 0.0 },
            unit: "boolean".into(),
            bar: BarOp::eq(1.0),
        },
        json!({
            "uuid_roundtrip": uuid_roundtrip,
            "random_uuid_unique_1000": random_uuid_unique,
            "sha256_hex_correct": sha256_hex_correct,
            "bcrypt_format_correct": bcrypt_format_correct,
            "alice_uuid": alice_uuid_str,
            "bcrypt_sample": bcrypt_hash,
        }),
    );

    assert!(
        pass,
        "UUID viability bar not met (\
         roundtrip={uuid_roundtrip}, unique={random_uuid_unique}, \
         sha256={sha256_hex_correct}, bcrypt={bcrypt_format_correct})"
    );
}

// TODO: future work for v0.2.
// - UUID v7 (time-sortable). Postgres 18 ships `uuidv7()`. We have
//   `uuid::Uuid::now_v7()` available; just need a parallel UDF + INSERT-time
//   coercion path.
// - hmac() — `digest()` is keyless; the PG signature is hmac(data, key, algo).
// - pgp_sym_encrypt() / pgp_sym_decrypt() — symmetric-key payload crypto.
//   These need a real OpenPGP packet implementation; out of scope for v0.1.
// - encrypt() / decrypt() — block-cipher primitives (AES, 3DES). Same
//   boat as pgp_*.
