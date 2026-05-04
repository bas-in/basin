//! S3 port of `vector_smoke.rs` (vector_search viability).
//!
//! Storage on S3, so HNSW segments are GETs over HTTP. Same ANN claim:
//! HNSW top-10 should overlap brute-force top-10 by >= 7. The latency bar
//! is loosened (S3 GETs add round-trip cost) but the overlap claim is
//! backend-independent.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_vector::Distance;
use object_store::path::Path as ObjectPath;
use serde_json::json;

const TEST_NAME: &str = "s3_vector_search";
const DIM: usize = 64;
const TOTAL_ROWS: usize = 5_000;
const SQL_INSERT_ROWS: usize = 100;
const TOP_K: usize = 10;

fn det_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_add(0x9E3779B97F4A7C15);
    (0..dim)
        .map(|_| {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            ((z >> 33) as f32 / (1u32 << 31) as f32) - 0.5
        })
        .collect()
}

fn vector_lit(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("{:.6}", x));
    }
    s.push(']');
    s
}

fn embedding_schema(dim: i32) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        ),
    ]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn s3_vector_search() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });

    let tenant = TenantId::new();
    let session = engine.open_session(tenant).await.unwrap();
    let table = TableName::new("docs").unwrap();

    session
        .execute("CREATE TABLE docs (id BIGINT, embedding vector(64))")
        .await
        .unwrap();

    let mut tuples: Vec<String> = Vec::with_capacity(SQL_INSERT_ROWS);
    for id in 0..SQL_INSERT_ROWS as i64 {
        let v = det_vec(id as u64, DIM);
        tuples.push(format!("({id}, '{}')", vector_lit(&v)));
    }
    let sql = format!("INSERT INTO docs VALUES {}", tuples.join(", "));
    let res = session.execute(&sql).await.unwrap();
    if let ExecResult::Empty { tag } = res {
        assert!(tag.starts_with("INSERT 0"), "unexpected tag {tag}");
    } else {
        panic!("expected Empty for INSERT");
    }

    let bulk_rows = TOTAL_ROWS - SQL_INSERT_ROWS;
    let ids: Int64Array = (SQL_INSERT_ROWS as i64..(SQL_INSERT_ROWS + bulk_rows) as i64).collect();
    let raw_vecs: Vec<Option<Vec<Option<f32>>>> = (SQL_INSERT_ROWS..(SQL_INSERT_ROWS + bulk_rows))
        .map(|id| Some(det_vec(id as u64, DIM).into_iter().map(Some).collect()))
        .collect();
    let emb = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(raw_vecs, DIM as i32);
    let batch = RecordBatch::try_new(
        embedding_schema(DIM as i32),
        vec![Arc::new(ids), Arc::new(emb)],
    )
    .unwrap();
    let part = PartitionKey::default_key();
    let df = storage
        .write_batch(&tenant, &table, &part, &batch)
        .await
        .unwrap();

    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    catalog
        .append_data_files(
            &tenant,
            &table,
            meta.current_snapshot,
            vec![DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
            }],
        )
        .await
        .unwrap();

    drop(session);
    let session = engine.open_session(tenant).await.unwrap();

    let query = det_vec(424_242, DIM);
    let query_lit = vector_lit(&query);

    let brute_sql = format!(
        "SELECT id FROM docs ORDER BY l2_distance(embedding, '{query_lit}') LIMIT {TOP_K}"
    );
    let t0 = Instant::now();
    let res = session.execute(&brute_sql).await.unwrap();
    let brute_ms = t0.elapsed().as_secs_f64() * 1_000.0;
    let brute_ids: Vec<i64> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..arr.len() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        _ => panic!("expected Rows"),
    };
    assert!(!brute_ids.is_empty(), "brute SQL returned empty");

    let t0 = Instant::now();
    let hnsw_batches = session
        .vector_search(&table, "embedding", query.clone(), TOP_K, Distance::L2)
        .await
        .unwrap();
    let hnsw_ms = t0.elapsed().as_secs_f64() * 1_000.0;
    let hnsw_ids: Vec<i64> = {
        let mut out = Vec::new();
        for b in &hnsw_batches {
            let arr = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(arr.value(i));
            }
        }
        out
    };
    assert!(!hnsw_ids.is_empty(), "hnsw vector_search returned empty");

    let brute_set: std::collections::HashSet<i64> = brute_ids.iter().copied().collect();
    let overlap = hnsw_ids.iter().filter(|x| brute_set.contains(x)).count();

    // Backend-independent claim: overlap >= 7. Latency claim is dropped — S3
    // round-trip costs make absolute hnsw_ms numbers unreliable across
    // network/cloud environments.
    let speedup = brute_ms / hnsw_ms.max(0.001);
    let pass = overlap >= 7;
    println!(
        "[S3 vector_search] brute_ms={:.2}, hnsw_ms={:.2}, overlap={}/10 {}",
        brute_ms,
        hnsw_ms,
        overlap,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "vector_search",
        "Native vector search on real S3 (HNSW + brute-force agree)",
        "On real S3, HNSW top-10 overlaps brute-force top-10 by >=7.",
        pass,
        PrimaryMetric {
            label: "HNSW / brute-force overlap (out of 10)".into(),
            value: overlap as f64,
            unit: "matches".into(),
            bar: BarOp::ge(7.0),
        },
        json!({
            "dim": DIM,
            "rows": TOTAL_ROWS,
            "sql_rows": SQL_INSERT_ROWS,
            "top_k": TOP_K,
            "overlap": overlap,
            "brute_ms": brute_ms,
            "hnsw_ms": hnsw_ms,
            "hnsw_speedup_x": speedup,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        overlap >= 7,
        "HNSW overlap with brute force was {overlap}/10, expected >=7"
    );
}
