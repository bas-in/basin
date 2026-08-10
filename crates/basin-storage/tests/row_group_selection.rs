//! End-to-end verification that `ReadOptions::row_group_selection` is
//! honoured by the Parquet reader path — i.e. the GIN row-group prune
//! decisions computed by `basin-engine::index_probe` actually reduce
//! physical I/O at the parquet reader level.
//!
//! # Why this test exists
//!
//! The engine-side index probe computes per-file allowlists of surviving
//! row groups (JSONB `@>` GIN prune, LIKE/ILIKE trigram prune). Those
//! allowlists are propagated through `ReadOptions::row_group_selection`.
//! Until this test, the wiring was visible in the reader source but no
//! end-to-end assertion proved that:
//!   1. The `HashMap<path, Vec<u32>>` keys are looked up correctly under
//!      the same `ObjectPath::as_ref()` string the writer returns.
//!   2. The kept row-group ids are passed to
//!      `ParquetRecordBatchReaderBuilder::with_row_groups(kept)` (so the
//!      parquet engine actually skips the pruned groups at decode time).
//!   3. The bookkeeping counter on `ReadCounters` reflects the prune.
//!
//! The test writes a multi-row-group Parquet file with disjoint id ranges
//! per row-group, then reads back with three different allowlists and
//! asserts the returned id range matches each allowlist exactly.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{FileFormat, ReadOptions, Storage, StorageConfig, WriteOptions};
use futures::stream::StreamExt;
use object_store::memory::InMemory;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Build a batch with `len` rows whose `id` runs `[start, start + len)` so
/// each row-group (when split by a small `max_row_group_size`) ends up with
/// a known, disjoint id range. The payload is a short string so the file
/// stays tiny but still has real columnar data.
fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len)
        .map(|i| format!("p-{:06}", start + i as i64))
        .collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

/// Make a fresh in-memory Storage. We use `InMemory` rather than the
/// `LocalFileSystem` so the test is fully hermetic.
fn storage_mem() -> Storage {
    let inner = Arc::new(InMemory::new());
    Storage::new(StorageConfig {
        object_store: inner,
        root_prefix: None,
        // Caches off so each read is a deterministic decode from the
        // underlying bytes — important for asserting on row-group counters.
        disk_cache: None,
        page_cache: None,
    })
}

/// Write a single Parquet file with N row groups by forcing
/// `max_row_group_size = rows_per_rg`. Row-group `i` will contain ids
/// `[i * rows_per_rg, (i + 1) * rows_per_rg)`. Returns the resulting
/// object-store path (the same string used as the `row_group_selection`
/// map key).
async fn write_multi_rg_file(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    part: &PartitionKey,
    rows_per_rg: usize,
    n_row_groups: usize,
) -> object_store::path::Path {
    let total = rows_per_rg * n_row_groups;
    let batch = build_batch(0, total);
    let opts = WriteOptions {
        file_format: FileFormat::Parquet,
        // `max_row_group_size` forces the parquet writer to flush a row
        // group every `rows_per_rg` rows, so we end up with exactly
        // `n_row_groups` groups whose ids are aligned to the value we use
        // when asserting later.
        max_row_group_size: Some(rows_per_rg),
        ..Default::default()
    };
    let df = storage
        .write_batch_with_options(project, table, part, &batch, &opts)
        .await
        .expect("write_batch_with_options");
    df.path
}

async fn read_ids(
    storage: &Storage,
    project: &ProjectId,
    paths: Vec<object_store::path::Path>,
    opts: ReadOptions,
) -> Vec<i64> {
    let mut stream = storage
        .read_paths_with_schema(project, paths, opts, Some(schema()))
        .await
        .expect("read_paths_with_schema");
    let mut ids = Vec::new();
    while let Some(b) = stream.next().await {
        let batch = b.expect("batch");
        let col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64Array");
        for v in col.iter() {
            ids.push(v.expect("non-null id"));
        }
    }
    ids.sort_unstable();
    ids
}

/// Selecting row-group 0 must yield exactly the ids written into rg=0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_group_selection_keeps_only_listed_row_groups() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("rg_sel").unwrap();
    let part = PartitionKey::default_key();

    // 4 row-groups of 1000 rows each. Total 4000 rows, ids [0, 4000).
    let rows_per_rg: usize = 1000;
    let n_row_groups: usize = 4;
    let path =
        write_multi_rg_file(&storage, &project, &table, &part, rows_per_rg, n_row_groups).await;

    // Baseline: no selection => all 4000 rows. Proves the writer actually
    // produced the multi-rg file we expected (otherwise the prune
    // assertions below would be meaningless).
    {
        let opts = ReadOptions::default();
        let ids = read_ids(&storage, &project, vec![path.clone()], opts).await;
        assert_eq!(ids.len(), 4000, "baseline: expected all rows");
        assert_eq!(ids.first().copied(), Some(0));
        assert_eq!(ids.last().copied(), Some(3999));
    }

    // Allowlist = {path -> [0]}. Only row-group 0 (ids [0, 1000)) should
    // come back.
    {
        let mut sel = HashMap::new();
        sel.insert(path.as_ref().to_string(), vec![0u32]);
        let opts = ReadOptions {
            row_group_selection: Some(sel),
            ..Default::default()
        };
        let ids = read_ids(&storage, &project, vec![path.clone()], opts).await;
        assert_eq!(
            ids.len(),
            rows_per_rg,
            "rg=0 only: expected exactly {} rows, got {}",
            rows_per_rg,
            ids.len()
        );
        // Strict range check: must be the rg-0 id range and nothing else.
        assert_eq!(*ids.first().unwrap(), 0);
        assert_eq!(*ids.last().unwrap(), (rows_per_rg - 1) as i64);
    }

    // Allowlist = {path -> [2]}. Only ids [2000, 3000) should come back.
    {
        let mut sel = HashMap::new();
        sel.insert(path.as_ref().to_string(), vec![2u32]);
        let opts = ReadOptions {
            row_group_selection: Some(sel),
            ..Default::default()
        };
        let ids = read_ids(&storage, &project, vec![path.clone()], opts).await;
        assert_eq!(
            ids.len(),
            rows_per_rg,
            "rg=2 only: expected exactly {} rows",
            rows_per_rg
        );
        let lo = (2 * rows_per_rg) as i64;
        let hi = (3 * rows_per_rg - 1) as i64;
        assert_eq!(*ids.first().unwrap(), lo);
        assert_eq!(*ids.last().unwrap(), hi);
    }

    // Allowlist = {path -> [0, 3]}. Non-contiguous selection: must yield
    // ids [0, 1000) ∪ [3000, 4000) — and nothing from rg=1 or rg=2.
    {
        let mut sel = HashMap::new();
        sel.insert(path.as_ref().to_string(), vec![0u32, 3u32]);
        let opts = ReadOptions {
            row_group_selection: Some(sel),
            ..Default::default()
        };
        let ids = read_ids(&storage, &project, vec![path.clone()], opts).await;
        assert_eq!(
            ids.len(),
            2 * rows_per_rg,
            "rg=[0,3]: expected exactly {} rows",
            2 * rows_per_rg
        );
        // None of the middle row-groups' ids may have leaked in.
        for &v in &ids {
            let in_rg0 = (0..rows_per_rg as i64).contains(&v);
            let in_rg3 = (3 * rows_per_rg as i64..4 * rows_per_rg as i64).contains(&v);
            assert!(
                in_rg0 || in_rg3,
                "id {v} leaked from a pruned row group (expected rg 0 or rg 3 only)"
            );
        }
    }
}

/// An empty allowlist (file present in the map but the Vec is empty)
/// must scan zero row groups — i.e. return zero rows even though the
/// file is materially non-empty. This pins the contract that the
/// selection is treated as an authoritative allowlist, not a "hint".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_row_group_selection_yields_no_rows() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("rg_sel_empty").unwrap();
    let part = PartitionKey::default_key();

    let path = write_multi_rg_file(&storage, &project, &table, &part, 500, 3).await;

    let mut sel = HashMap::new();
    sel.insert(path.as_ref().to_string(), Vec::<u32>::new());
    let opts = ReadOptions {
        row_group_selection: Some(sel),
        ..Default::default()
    };
    let ids = read_ids(&storage, &project, vec![path], opts).await;
    assert!(
        ids.is_empty(),
        "empty allowlist must prune everything; got {} rows",
        ids.len()
    );
}

/// A file ABSENT from the selection map must read in full — the API
/// contract is "Unknown → read all row groups", so unrelated files in
/// a multi-file query are not penalised by an index probe that only
/// summarised some of them. Two files written, only one is listed in
/// the selection: the unlisted one must contribute every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_absent_from_selection_is_scanned_in_full() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("rg_sel_missing").unwrap();
    let part = PartitionKey::default_key();

    // File A: 4 × 500-row groups (ids [0, 2000)).
    let path_a = write_multi_rg_file(&storage, &project, &table, &part, 500, 4).await;
    // File B: 4 × 500-row groups, but with a disjoint id range so we can
    // tell them apart in the merged stream.
    let batch_b = build_batch(10_000, 2000);
    let path_b = storage
        .write_batch_with_options(
            &project,
            &table,
            &part,
            &batch_b,
            &WriteOptions {
                file_format: FileFormat::Parquet,
                max_row_group_size: Some(500),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .path;

    // Selection map mentions only file A with row-group 0. File B is
    // absent from the map → must be read in full.
    let mut sel = HashMap::new();
    sel.insert(path_a.as_ref().to_string(), vec![0u32]);
    let opts = ReadOptions {
        row_group_selection: Some(sel),
        ..Default::default()
    };
    let ids = read_ids(
        &storage,
        &project,
        vec![path_a.clone(), path_b.clone()],
        opts,
    )
    .await;

    // From A we expect rg=0 only (500 rows, ids [0, 500)). From B we
    // expect every row (2000 rows, ids [10_000, 12_000)).
    assert_eq!(ids.len(), 500 + 2000, "expected 500 from A + 2000 from B");
    let from_a: Vec<i64> = ids.iter().copied().filter(|&v| v < 10_000).collect();
    let from_b: Vec<i64> = ids.iter().copied().filter(|&v| v >= 10_000).collect();
    assert_eq!(from_a.len(), 500, "file A must yield rg=0 only");
    assert_eq!(
        from_b.len(),
        2000,
        "file B (absent from map) must yield all"
    );
    assert_eq!(*from_a.first().unwrap(), 0);
    assert_eq!(*from_a.last().unwrap(), 499);
    assert_eq!(*from_b.first().unwrap(), 10_000);
    assert_eq!(*from_b.last().unwrap(), 11_999);
}

/// The `ReadCounters::row_groups_pruned_by_stats` counter must reflect
/// the selection-based prune (the reader bookkeeps allow-list rejects
/// against the same field as stats rejects — see `reader.rs` around
/// line 1496-1510). Pinning the counter behaviour stops a regression
/// that "silently disables the allowlist" from going undetected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_group_selection_increments_prune_counter() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("rg_sel_counter").unwrap();
    let part = PartitionKey::default_key();

    let rows_per_rg = 500;
    let n_row_groups = 4;
    let path =
        write_multi_rg_file(&storage, &project, &table, &part, rows_per_rg, n_row_groups).await;

    let counters = storage.read_counters().clone();
    counters.reset();

    let mut sel = HashMap::new();
    sel.insert(path.as_ref().to_string(), vec![1u32]); // keep rg=1 only
    let opts = ReadOptions {
        row_group_selection: Some(sel),
        ..Default::default()
    };
    let ids = read_ids(&storage, &project, vec![path], opts).await;
    assert_eq!(ids.len(), rows_per_rg, "rg=1 only");

    let snap = counters.snapshot();
    assert_eq!(
        snap.row_groups_considered, n_row_groups as u64,
        "every row group must be 'considered' (i.e. inspected before prune)"
    );
    assert_eq!(
        snap.row_groups_scanned, 1,
        "exactly one row group must be scanned (the surviving rg=1)"
    );
    // The remaining `n - 1` row groups were rejected by the allowlist
    // before the stats prune ran, so they're charged to the
    // `pruned_by_stats` counter alongside true stat rejects (see the
    // reader comment about the `pruned` local variable).
    assert!(
        snap.row_groups_pruned_by_stats >= (n_row_groups as u64) - 1,
        "expected at least {} row groups pruned, got {}",
        n_row_groups as u64 - 1,
        snap.row_groups_pruned_by_stats,
    );
}
