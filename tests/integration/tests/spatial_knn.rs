//! Integration tests for KNN nearest-neighbour spatial search:
//! `SELECT … FROM t ORDER BY <point_col> <-> ST_MakePoint(x,y) LIMIT k`.
//!
//! These exercise the end-to-end R-tree KNN path
//! (`index_probe::detect_knn_predicate` → `rtree_knn_scan::execute_knn_plan`):
//! a POINT column with a GIST index and a `.rtree` sidecar is probed per-file
//! with `nearest_neighbor_iter` (L2-degree order), over-fetched, then re-ranked
//! by exact Haversine meters to produce the true top-k.
//!
//! Coverage:
//!   * `knn_returns_k_nearest_in_order` — the k returned ARE the k actual
//!     nearest by Haversine, in ascending-distance order.
//!   * `knn_overfetch_rerank_beats_l2` — a high-latitude case where the
//!     L2-degree-nearest point is NOT the Haversine-nearest; the exact re-rank
//!     must win.
//!   * `knn_limit_exact` — LIMIT 3 over a 10-row table returns exactly 3.
//!   * `knn_does_not_hijack_pgvector` — a `vector(N)` column `<->` query still
//!     routes through the HNSW path and is unaffected by the spatial KNN
//!     recognizer.

#![allow(clippy::print_stdout)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{FileFormat, Storage, StorageConfig, WriteOptions};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const RG_SIZE: u32 = 4;

fn points_schema() -> Arc<Schema> {
    let mut meta = HashMap::new();
    meta.insert("BASIN_TYPE".to_string(), "POINT".to_string());
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "geom",
            DataType::FixedSizeBinary(basin_geo::POINT_WKB_LEN as i32),
            false,
        )
        .with_metadata(meta),
    ]))
}

/// Build a POINT batch from `(id, lon, lat)` triples.
fn build_batch(points: &[(i64, f64, f64)]) -> RecordBatch {
    let ids: Int64Array = points.iter().map(|(id, _, _)| *id).collect();
    let mut geom_b =
        FixedSizeBinaryBuilder::with_capacity(points.len(), basin_geo::POINT_WKB_LEN as i32);
    for (_, x, y) in points {
        let bytes = basin_geo::encode_point(&basin_geo::Point::new(*x, *y));
        geom_b.append_value(bytes).unwrap();
    }
    RecordBatch::try_new(
        points_schema(),
        vec![Arc::new(ids), Arc::new(geom_b.finish())],
    )
    .unwrap()
}

/// Seed a `points` table with the given rows + a GIST index + R-tree sidecar,
/// then return a cold engine to probe through.
async fn seed_points_engine(points: &[(i64, f64, f64)]) -> (Engine, ProjectId) {
    let dir = TempDir::new().unwrap();
    // Leak the TempDir so the LocalFileSystem stays valid for the engine's
    // lifetime within the test (the test process exits shortly after).
    let path = dir.keep();
    let fs = LocalFileSystem::new_with_prefix(&path).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("points").unwrap();
    let part = PartitionKey::default_key();
    let col_name = "geom";

    catalog.create_namespace(&project).await.unwrap();
    catalog
        .create_table(&project, &table, &points_schema())
        .await
        .unwrap();

    let batch = build_batch(points);
    let opts = WriteOptions {
        file_format: FileFormat::Parquet,
        max_row_group_size: Some(RG_SIZE as usize),
        row_block_size: Some(RG_SIZE),
        ..Default::default()
    };
    let data_file = storage
        .write_batch_with_options(&project, &table, &part, &batch, &opts)
        .await
        .expect("write batch");
    let data_path = data_file.path.as_ref().to_string();

    let meta_before = catalog.load_table(&project, &table).await.unwrap();
    catalog
        .append_data_files(
            &project,
            &table,
            meta_before.current_snapshot,
            vec![DataFileRef {
                path: data_path.clone(),
                size_bytes: data_file.size_bytes,
                row_count: data_file.row_count,
                column_stats: data_file.column_stats.clone(),
                bloom_filters: BTreeMap::new(),
                hll_sketches: BTreeMap::new(),
                tdigest_sketches: BTreeMap::new(),
            }],
        )
        .await
        .unwrap();
    catalog
        .set_file_format(&project, &table, basin_catalog::TableFileFormat::Parquet)
        .await
        .unwrap();

    // R-tree sidecar (mirror of the compactor's compact-time hook).
    let rtree = basin_storage::index::rtree::build_rtree_for_batch(&batch, 1, RG_SIZE);
    let bytes = basin_storage::index::rtree::serialize_rtree(&rtree).unwrap();
    let sidecar_key = basin_storage::index::rtree::rtree_segment_key_for_data_file(
        None, &project, &table, col_name, &data_path,
    )
    .expect("canonical path");
    {
        use object_store::ObjectStoreExt;
        storage
            .project_object_store(&project)
            .put(
                &sidecar_key,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes)),
            )
            .await
            .expect("write sidecar");
    }

    catalog
        .create_index_with_method(
            &project,
            &table,
            "points_geom_gist",
            &[col_name.to_string()],
            false,
            "gist",
            None,
        )
        .await
        .unwrap();

    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    (engine, project)
}

/// Run a SELECT and return the `id` column values in result order.
async fn select_ids(engine: &Engine, project: ProjectId, sql: &str) -> Vec<i64> {
    let sess = engine.open_session(project).await.unwrap();
    let res = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute({sql:?}): {e:?}"));
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => Vec::new(),
    };
    let mut out = Vec::new();
    for b in &batches {
        let idx = b.schema().index_of("id").expect("id column present");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_returns_k_nearest_in_order() {
    // Ten points on/near a diagonal. Query at (0,0); the nearest by Haversine
    // are simply the ones with smallest lon²+lat² (near the equator L2-degree
    // and Haversine agree, so the expected answer is unambiguous and checkable
    // by hand).
    let pts: Vec<(i64, f64, f64)> = vec![
        (0, 0.10, 0.10),
        (1, 0.20, 0.20),
        (2, 0.30, 0.30),
        (3, 0.05, 0.05),
        (4, 1.00, 1.00),
        (5, 2.00, 2.00),
        (6, 0.50, 0.40),
        (7, 3.00, 0.00),
        (8, 0.00, 0.80),
        (9, 5.00, 5.00),
    ];
    let (engine, project) = seed_points_engine(&pts).await;

    let ids = select_ids(
        &engine,
        project,
        "SELECT id FROM points ORDER BY geom <-> ST_MakePoint(0.0, 0.0) LIMIT 4",
    )
    .await;

    // Hand-computed Haversine order from (0,0): id3 (0.05,0.05) < id0 (0.1,0.1)
    // < id1 (0.2,0.2) < id2 (0.3,0.3) < id6 (0.5,0.4) < id8 (0,0.8) < ...
    assert_eq!(
        ids,
        vec![3, 0, 1, 2],
        "k-nearest ids in ascending distance: {ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_overfetch_rerank_beats_l2() {
    // High-latitude case where L2-degree order != Haversine-meter order.
    // At lat 80°, 1° of longitude ≈ 19.4 km but 1° of latitude ≈ 111 km.
    //
    // Query point: (0, 80).
    //   * id_lon at (3.0, 80.0): Δlon=3° at 80°N ≈ 58 km. L2-degree = 3.0.
    //   * id_lat at (0.0, 81.6): Δlat=1.6° ≈ 178 km. L2-degree = 1.6.
    //
    // By L2 degrees, id_lat (1.6) is "nearer" than id_lon (3.0). By Haversine
    // meters, id_lon (~58 km) is FAR nearer than id_lat (~178 km). A correct
    // KNN must over-fetch past the L2-nearest and re-rank to return id_lon
    // first. We add filler points further out so k=1 has to choose between
    // the two.
    let pts: Vec<(i64, f64, f64)> = vec![
        (100, 0.0, 81.6),  // L2-degree nearest, Haversine far (~178 km)
        (200, 3.0, 80.0),  // L2-degree farther, Haversine nearest (~58 km)
        (300, 5.0, 82.0),  // filler, far on both metrics
        (400, -4.0, 79.0), // filler
        (500, 6.0, 78.0),  // filler
        (600, -6.0, 83.0), // filler
        (700, 8.0, 80.0),  // filler
        (800, 0.0, 75.0),  // filler
    ];
    let (engine, project) = seed_points_engine(&pts).await;

    // Sanity: confirm the premise (L2-degree order vs Haversine order really
    // disagree for these two candidates) so the test proves the re-rank.
    let q = basin_geo::Point::new(0.0, 80.0);
    let d_lat = basin_geo::haversine_meters(&q, &basin_geo::Point::new(0.0, 81.6));
    let d_lon = basin_geo::haversine_meters(&q, &basin_geo::Point::new(3.0, 80.0));
    let l2_lat = (0.0_f64).hypot(1.6);
    let l2_lon = (3.0_f64).hypot(0.0);
    assert!(
        l2_lat < l2_lon,
        "premise: id100 is L2-degree nearer ({l2_lat} < {l2_lon})"
    );
    assert!(
        d_lon < d_lat,
        "premise: id200 is Haversine nearer ({d_lon} < {d_lat})"
    );

    let ids = select_ids(
        &engine,
        project,
        "SELECT id FROM points ORDER BY geom <-> ST_MakePoint(0.0, 80.0) LIMIT 1",
    )
    .await;
    assert_eq!(
        ids,
        vec![200],
        "Haversine-nearest (id200, lon-offset) must win over L2-degree-nearest \
         (id100, lat-offset); got {ids:?}"
    );

    // k=2 must be in true Haversine order: id200 (~58 km), id400 (~137 km).
    // The L2-degree-nearest point id100 (~178 km) must NOT appear in the top-2
    // — that is exactly what a pure L2-degree ordering would get wrong.
    let ids2 = select_ids(
        &engine,
        project,
        "SELECT id FROM points ORDER BY geom <-> ST_MakePoint(0.0, 80.0) LIMIT 2",
    )
    .await;
    assert_eq!(ids2, vec![200, 400], "top-2 in Haversine order: {ids2:?}");
    assert!(
        !ids2.contains(&100),
        "L2-degree-nearest id100 must be ranked out of the Haversine top-2: {ids2:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_limit_exact() {
    // 10 rows, LIMIT 3 → exactly 3 rows back.
    let pts: Vec<(i64, f64, f64)> = (0..10)
        .map(|i| (i, i as f64 * 0.1, i as f64 * 0.1))
        .collect();
    let (engine, project) = seed_points_engine(&pts).await;

    let ids = select_ids(
        &engine,
        project,
        "SELECT id FROM points ORDER BY geom <-> ST_MakePoint(0.0, 0.0) LIMIT 3",
    )
    .await;
    assert_eq!(ids.len(), 3, "LIMIT 3 must return exactly 3 rows: {ids:?}");
    assert_eq!(ids, vec![0, 1, 2], "nearest three to origin: {ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_does_not_hijack_pgvector() {
    // A `vector(N)` column `<->` query must still route through the HNSW path
    // and NOT be intercepted by the spatial KNN recognizer. We build a tiny
    // vector table entirely through the SQL surface (CREATE TABLE … vector(3),
    // INSERT, then an ORDER BY <-> LIMIT query) and assert the brute-force /
    // HNSW result is correct. The spatial recognizer rejects this because the
    // ORDER BY column is FixedSizeList<Float32> (vector), not BASIN_TYPE=POINT.
    let dir = TempDir::new().unwrap();
    let path = dir.keep();
    let fs = LocalFileSystem::new_with_prefix(&path).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: None,
    });
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE vecs (id BIGINT, embedding vector(3))")
        .await
        .expect("create vector table");
    sess.execute("INSERT INTO vecs VALUES (1, '[0.0, 0.0, 0.0]')")
        .await
        .expect("insert 1");
    sess.execute("INSERT INTO vecs VALUES (2, '[1.0, 0.0, 0.0]')")
        .await
        .expect("insert 2");
    sess.execute("INSERT INTO vecs VALUES (3, '[5.0, 5.0, 5.0]')")
        .await
        .expect("insert 3");

    let ids = select_ids(
        &engine,
        project,
        "SELECT id FROM vecs ORDER BY embedding <-> '[0.1, 0.0, 0.0]' LIMIT 2",
    )
    .await;
    // Nearest to [0.1,0,0]: id1 ([0,0,0], d=0.1) then id2 ([1,0,0], d=0.9).
    // id3 is far. If the spatial recognizer had hijacked this, the POINT
    // decode would have failed / the query would error — instead it routes to
    // the vector path and returns the correct top-2.
    assert_eq!(
        ids,
        vec![1, 2],
        "pgvector <-> path must be unaffected: {ids:?}"
    );
}
