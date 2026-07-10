//! Correctness gate for the bounded-memory streaming PRIMARY KEY check
//! (`enforce_pk_streaming`). Forces the streaming path for EVERY batch via
//! `BASIN_PK_STREAMING_MIN_ROWS=0` and asserts it is byte-equivalent to the
//! in-RAM path: clean unique loads pass, cross-file duplicates are rejected,
//! intra-batch duplicates are rejected, and a fresh key is never a false
//! positive. Single test in its own binary so the process-global env override
//! cannot race other tests.

use std::sync::Arc;

use arrow_schema::Schema;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig { storage, catalog, shard: None })
}

fn row(id: i64, x: i64) -> Vec<Option<String>> {
    vec![Some(id.to_string()), Some(x.to_string())]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_streaming_is_correct() {
    // Force the streaming path for every existing-table check, even at tiny
    // row counts.
    std::env::set_var("BASIN_PK_STREAMING_MIN_ROWS", "0");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let schema = Arc::new(Schema::empty());
    let cols = vec!["id".to_string(), "x".to_string()];

    sess.execute("DROP TABLE IF EXISTS t").await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x INT)").await.unwrap();

    // Seed ids 1..=30_000 across 3 separate chunks (each becomes its own data
    // file). Every chunk after the first runs the streaming check against the
    // prior files — all keys unique, so no false positive.
    for c in 0..3u64 {
        let batch: Vec<Vec<Option<String>>> = (c * 10_000..(c + 1) * 10_000)
            .map(|i| row(i as i64 + 1, (i % 7) as i64))
            .collect();
        let n = sess
            .ingest_csv_batch("t", schema.clone(), Some(&cols), batch)
            .await
            .expect("clean unique load must pass under the streaming path");
        assert_eq!(n, 10_000, "ingested count");
    }

    // Cross-file duplicate: id=5 lives in the first file. Must be rejected.
    let dup = vec![row(5, 0)];
    let err = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), dup)
        .await;
    assert!(
        err.is_err(),
        "streaming path must reject a cross-file duplicate PK (id=5)"
    );

    // Intra-batch duplicate: two rows with the same id in one batch.
    let intra = vec![row(900_001, 0), row(900_001, 1)];
    let err2 = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), intra)
        .await;
    assert!(
        err2.is_err(),
        "must reject an intra-batch duplicate PK (id=900001 twice)"
    );

    // A fresh unique id must still succeed — no false positive from pruning.
    let ok = vec![row(500_001, 9)];
    let n = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), ok)
        .await
        .expect("a fresh unique id must pass");
    assert_eq!(n, 1);

    // Final row count: 30_000 seeded + 1 fresh = 30_001.
    let cnt = match sess.execute("SELECT count(*) FROM t").await.unwrap() {
        basin_engine::ExecResult::Rows { batches, .. } => {
            use arrow_array::Array;
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(-1)
        }
        _ => -1,
    };
    assert_eq!(cnt, 30_001, "final row count after rejected dups + one insert");
}

/// Per-chunk PK enforcement cost must stay independent of table size for an
/// ascending key. The streaming check discovers candidate files from the
/// catalog (zero object-store RPCs) and zone-map-prunes them by the PK column's
/// [min,max]. This test asserts the precondition that makes the prune bound the
/// candidate set to ~0 for an ascending key: every live data file the catalog
/// reports carries a decodable PK `column_stats` min/max, AND each file's max
/// is strictly below the next-loaded chunk's keys (disjoint, increasing
/// ranges). When that holds, a strictly-above-range batch prunes EVERY existing
/// file — O(files) becomes O(1 candidate ≈ 0 reads) regardless of how many
/// files have accumulated. A regression (shard/sync files written without PK
/// min/max stats) would make the prune a no-op and re-introduce the
/// super-linear bulk-load cost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_ascending_files_carry_pk_zonemap() {
    use basin_common::TableName;
    use basin_storage::{
        evaluate_compound_for_pruning, CompoundPredicate, Predicate, PruneOutcome, ScalarValue,
    };

    std::env::set_var("BASIN_PK_STREAMING_MIN_ROWS", "0");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let schema = Arc::new(Schema::empty());
    let cols = vec!["id".to_string(), "x".to_string()];

    sess.execute("DROP TABLE IF EXISTS t").await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, x INT)").await.unwrap();

    // Load 5 ascending chunks of 10k rows each (ids 1..=50_000). Each chunk is
    // committed as its own data file with a disjoint, increasing PK range.
    for c in 0..5u64 {
        let batch: Vec<Vec<Option<String>>> = (c * 10_000..(c + 1) * 10_000)
            .map(|i| row(i as i64 + 1, (i % 7) as i64))
            .collect();
        sess.ingest_csv_batch("t", schema.clone(), Some(&cols), batch)
            .await
            .expect("clean ascending load");
    }

    // Catalog-sourced file discovery: zero object-store RPCs, carries stats.
    let table = TableName::new("t".to_string()).unwrap();
    let files = eng
        .config()
        .storage
        .catalog_data_files(&project, &table)
        .await
        .expect("catalog must back the file set");
    assert!(!files.is_empty(), "ascending load must produce data files");

    // Build the schema the pruner uses (id: BIGINT).
    use arrow_schema::{DataType, Field};
    let prune_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("x", DataType::Int32, true),
    ]);

    // Every file must carry a decodable id min/max so the zone-map prune works.
    for f in &files {
        let cs = f
            .column_stats
            .get("id")
            .unwrap_or_else(|| panic!("file {} missing id column_stats", f.path));
        assert!(
            cs.min_bytes.is_some() && cs.max_bytes.is_some(),
            "file {} must carry id min/max for the zone-map prune",
            f.path
        );
    }

    // A strictly-above-range batch (ids 1_000_000..) must prune EVERY existing
    // file to NoMatch — the candidate (read) set is 0, independent of file
    // count. This is the flat-cost property for ascending keys.
    let above = CompoundPredicate::And(vec![
        CompoundPredicate::Atom(Predicate::Gt("id".into(), ScalarValue::Int64(999_999))),
        CompoundPredicate::Atom(Predicate::Lt("id".into(), ScalarValue::Int64(1_000_002))),
    ]);
    let mut survivors = 0usize;
    for f in &files {
        if !matches!(
            evaluate_compound_for_pruning(&above, &f.column_stats, &prune_schema, f.row_count),
            PruneOutcome::NoMatch
        ) {
            survivors += 1;
        }
    }
    assert_eq!(
        survivors, 0,
        "an above-range ascending key must prune every existing file (got {survivors} survivors / {} files)",
        files.len()
    );

    // And the duplicate semantics are unchanged: an in-range existing key is
    // still rejected (correctness must not be weakened by the prune).
    let dup = vec![row(42, 0)];
    let err = sess
        .ingest_csv_batch("t", schema.clone(), Some(&cols), dup)
        .await;
    assert!(err.is_err(), "a genuine duplicate PK (id=42) must still be rejected");
}

/// #78: the streaming existence check must stay correct with the batch
/// key-range pushed into the per-file read and the bloom probe skipped
/// (bulk chunks exceed `PK_STREAMING_SMALL_BATCH`, where a 1%-FPP bloom
/// passes with P ≈ 1 anyway). Three shapes:
///
///   1. a bulk chunk overlapping an existing file WITH a real duplicate
///      → rejected (the pushdown-pruned read still surfaces the dup row);
///   2. a bulk chunk strictly BETWEEN two files' key ranges → accepted
///      (zone-map prune, no read at all);
///   3. a bulk chunk INSIDE a gappy file's [min,max] with no actual
///      collision → accepted (the pushed range filter must not fabricate
///      a match, and filtered-out rows must not hide one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_streaming_pushdown_read_detects_dups_and_passes_clean() {
    std::env::set_var("BASIN_PK_STREAMING_MIN_ROWS", "0");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    let schema = Arc::new(Schema::empty());
    let cols = vec!["id".to_string(), "x".to_string()];
    let rows = |ids: Vec<i64>| -> Vec<Vec<Option<String>>> {
        ids.into_iter().map(|i| row(i, i % 7)).collect()
    };

    sess.execute("CREATE TABLE t2 (id BIGINT PRIMARY KEY, x INT)")
        .await
        .unwrap();

    // File A: contiguous ids 1..=1000. File B: 2001..=3000 (disjoint).
    // File C: odd ids 4001..=4999 (in-range gaps for shape 3).
    for ids in [
        (1..=1000).collect::<Vec<i64>>(),
        (2001..=3000).collect(),
        (4001..=4999).step_by(2).collect(),
    ] {
        let n = ids.len();
        let loaded = sess
            .ingest_csv_batch("t2", schema.clone(), Some(&cols), rows(ids))
            .await
            .expect("seed chunk must load");
        assert_eq!(loaded as usize, n);
    }

    // Shape 1: bulk chunk (500 > PK_STREAMING_SMALL_BATCH) overlapping
    // file A with genuine duplicates (900..=1000).
    let dup = rows((900..=1399).collect());
    let err = sess
        .ingest_csv_batch("t2", schema.clone(), Some(&cols), dup)
        .await
        .expect_err("bulk chunk with a real dup must be rejected");
    assert!(
        err.to_string().contains("duplicate key"),
        "expected a duplicate-key violation, got: {err}"
    );

    // Shape 2: bulk chunk strictly between file A and file B key ranges.
    let between = rows((1201..=1700).collect());
    let n = sess
        .ingest_csv_batch("t2", schema.clone(), Some(&cols), between)
        .await
        .expect("chunk between existing key ranges must be accepted");
    assert_eq!(n, 500);

    // Shape 3: bulk chunk of EVEN ids inside file C's [4001,4999] span —
    // overlaps the file, survives the zone-map prune, but collides with
    // nothing.
    let evens = rows((4002..=4998).step_by(2).collect());
    let n = sess
        .ingest_csv_batch("t2", schema.clone(), Some(&cols), evens)
        .await
        .expect("gap-filling chunk with no real collision must be accepted");
    assert_eq!(n, 499);

    // And a genuine collision inside file C still fires.
    let dup_odd = rows((5001..=5499).chain(std::iter::once(4003)).collect());
    let err = sess
        .ingest_csv_batch("t2", schema.clone(), Some(&cols), dup_odd)
        .await
        .expect_err("bulk chunk containing an existing odd id must be rejected");
    assert!(
        err.to_string().contains("duplicate key"),
        "expected a duplicate-key violation, got: {err}"
    );
}
