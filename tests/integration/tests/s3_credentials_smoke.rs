//! Credentials smoke-check for `.basin-test.toml`.
//!
//! Runs against whatever S3-compatible service is configured. PUTs a small
//! object, reads it back byte-for-byte, lists the prefix, deletes the
//! object, then verifies a subsequent read returns NotFound.
//!
//! `#[ignore]`-gated so it doesn't run in normal `cargo test`. To run:
//!
//! ```sh
//! cargo test -p basin-integration-tests --test s3_credentials_smoke -- --ignored --nocapture
//! ```
//!
//! If `[s3]` isn't filled in, the test prints `[skip]` and returns Ok —
//! same pattern as the rest of the cloud-gated tests.

#![allow(clippy::print_stdout)]

use std::time::Instant;

use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde_json::json;

const TEST_NAME: &str = "s3_credentials_smoke";
const PAYLOAD: &[u8] = b"basin smoke test payload - if you can read this, sigv4 worked";

#[tokio::test]
#[ignore]
async fn s3_credentials_smoke() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("failed to parse .basin-test.toml: {e}"),
    };

    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    println!(
        "[s3 smoke] bucket={} region={} endpoint={} prefix={}",
        s3_cfg.bucket,
        s3_cfg.region,
        s3_cfg.endpoint.as_deref().unwrap_or("<aws default>"),
        s3_cfg.prefix
    );

    // Build the object store. Any failure here is a config / network issue,
    // not a test failure — surface the error message so it's diagnosable.
    let store = match s3_cfg.build_object_store() {
        Ok(s) => s,
        Err(e) => panic!("failed to build object store from config: {e}"),
    };

    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: store.clone(),
        prefix: run_prefix.clone(),
    };

    let key = ObjectPath::from(format!("{run_prefix}/hello.bin"));

    // PUT
    let t = Instant::now();
    store
        .put(&key, PutPayload::from_bytes(Bytes::from_static(PAYLOAD)))
        .await
        .unwrap_or_else(|e| panic!("PUT {key}: {e}"));
    let put_ms = t.elapsed().as_millis();
    println!("[s3 smoke] PUT  {} ms", put_ms);

    // GET full body and verify it round-trips byte-for-byte.
    let t = Instant::now();
    let result = store
        .get(&key)
        .await
        .unwrap_or_else(|e| panic!("GET {key}: {e}"));
    let bytes = result
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("read body of {key}: {e}"));
    let get_ms = t.elapsed().as_millis();
    assert_eq!(bytes.as_ref(), PAYLOAD, "body mismatch");
    println!("[s3 smoke] GET  {} ms ({} bytes)", get_ms, bytes.len());

    // LIST: the test's prefix should contain exactly our one object.
    let t = Instant::now();
    let prefix_path = ObjectPath::from(run_prefix.as_str());
    let mut stream = store.list(Some(&prefix_path));
    let mut listed = 0usize;
    while let Some(meta) = stream.next().await {
        let m = meta.unwrap_or_else(|e| panic!("LIST: {e}"));
        listed += 1;
        println!("[s3 smoke] LIST entry: {} ({} bytes)", m.location, m.size);
    }
    let list_ms = t.elapsed().as_millis();
    assert_eq!(listed, 1, "expected exactly one object under {run_prefix}");
    println!("[s3 smoke] LIST {} ms ({} entries)", list_ms, listed);

    // DELETE
    let t = Instant::now();
    store
        .delete(&key)
        .await
        .unwrap_or_else(|e| panic!("DELETE {key}: {e}"));
    let delete_ms = t.elapsed().as_millis();
    println!("[s3 smoke] DEL  {} ms", delete_ms);

    // Confirm the delete took effect: GET should return NotFound.
    match store.get(&key).await {
        Err(object_store::Error::NotFound { .. }) => {
            println!("[s3 smoke] post-delete GET correctly NotFound");
        }
        Ok(_) => panic!("post-delete GET unexpectedly succeeded for {key}"),
        Err(e) => panic!("post-delete GET expected NotFound, got: {e}"),
    }

    println!(
        "[s3 smoke] PASS - credentials work, round-trip is correct. \
         put={put_ms}ms get={get_ms}ms list={list_ms}ms del={delete_ms}ms"
    );

    // Emit a real-dashboard report so the user can see this round-trip on
    // the new benchmark/index_real.html. The "primary" metric is the
    // round-trip total — sub-2s end-to-end is the bar.
    let total_ms = (put_ms + get_ms + list_ms + delete_ms) as f64;
    report_real_viability(
        "s3_credentials_smoke",
        "S3 credentials and round-trip",
        "Real S3-compatible service handles PUT, GET, LIST, DELETE with byte-accurate body round-trip.",
        total_ms < 2_000.0,
        PrimaryMetric {
            label: "PUT+GET+LIST+DELETE total".into(),
            value: total_ms,
            unit: "ms".into(),
            bar: BarOp::lt(2_000.0),
        },
        json!({
            "bucket": s3_cfg.bucket,
            "region": s3_cfg.region,
            "endpoint": s3_cfg.endpoint.clone(),
            "put_ms": put_ms,
            "get_ms": get_ms,
            "list_ms": list_ms,
            "delete_ms": delete_ms,
            "payload_bytes": PAYLOAD.len(),
        }),
    );
}
