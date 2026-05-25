//! Sub-row-group page-index pruning via `RowSelection`.
//!
//! Verifies that when a Parquet file is written with many small data pages
//! (forced by a tight `data_pagesize_limit`) and a range predicate is pushed
//! into [`ReadOptions::filters`], the reader builds a [`RowSelection`] from
//! the Parquet column index (per-page min/max stats) so it decodes only the
//! data pages that could contain matching rows — not all ~65k rows in the
//! surviving row group.
//!
//! The scenario mirrors the IN-list bounding-range optimisation in
//! basin-engine: `WHERE id IN (val1, val2, ...)` synthesises
//! `Gt(min_val - 1) AND Lt(max_val + 1)` predicates so row-group stats can
//! narrow the file scan to one row group. This test proves the *next* tier of
//! pruning: within that row group, only the data pages whose id range overlaps
//! the bounding box are decoded.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{FileFormat, Predicate, ReadOptions, ScalarValue, Storage, StorageConfig, WriteOptions};
use futures::stream::StreamExt;
use object_store::memory::InMemory;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Build a batch with `len` rows whose `id` runs `[start, start + len)`.
fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> =
        (0..len).map(|i| format!("p-{:06}", start + i as i64)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

/// Build a storage backed by an in-memory store with caches off so every read
/// is a deterministic decode from bytes (no page-cache short-circuit).
fn storage_mem() -> Storage {
    Storage::new(StorageConfig {
        object_store: Arc::new(InMemory::new()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    })
}

/// Write a single Parquet file with many small data pages by capping the page
/// size. Returns the object-store path.
///
/// `rows_per_rg` controls when a row group is flushed; `page_size_bytes` caps
/// the size of each data page within a row group so we get several pages per
/// row group. Using a small page size (e.g. 512 bytes) and 65_000 rows per row
/// group produces tens of pages — enough to demonstrate intra-row-group
/// pruning.
async fn write_multi_page_file(
    storage: &Storage,
    project: &ProjectId,
    table: &TableName,
    part: &PartitionKey,
    total_rows: usize,
    rows_per_rg: usize,
    page_size_bytes: usize,
) -> object_store::path::Path {
    let batch = build_batch(0, total_rows);
    let opts = WriteOptions {
        file_format: FileFormat::Parquet,
        max_row_group_size: Some(rows_per_rg),
        // Small data-page size forces many pages per row group so the page
        // index has meaningful page-level min/max values we can prune against.
        data_pagesize_limit: Some(page_size_bytes),
        ..Default::default()
    };
    let df = storage
        .write_batch_with_options(project, table, part, &batch, &opts)
        .await
        .expect("write_batch_with_options");
    df.path
}

async fn read_ids_with_schema(
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
        let batch = b.expect("batch decode");
        let col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for v in col.iter() {
            ids.push(v.expect("non-null id"));
        }
    }
    ids.sort_unstable();
    ids
}

/// Main correctness + pruning-effectiveness test.
///
/// Writes 65_000 rows (ids 0..65000) in a single row group, forcing small
/// data pages (~512 bytes each) so the column index has many per-page
/// min/max entries. Then reads with a tight range predicate
/// `Gt(499) AND Lt(600)` — targeting only ids 500..=599.
///
/// Assertions:
/// 1. Correct rows returned: exactly ids [500, 599].
/// 2. The `rows_selected_by_page_index` counter is far smaller than the row
///    group size (65_000), proving page-level pruning fired and decimated the
///    decode footprint.
/// 3. A second read with NO predicate keeps `rows_selected_by_page_index` at
///    zero (no regression: no selection is built when there are no filters).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_index_prunes_within_row_group() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("page_prune").unwrap();
    let part = PartitionKey::default_key();

    let total_rows: usize = 65_000;
    // One row group containing all rows; many small pages within it.
    let rows_per_rg = total_rows;
    // ~512-byte pages with i64 id + payload string: expect O(100) pages.
    let page_size_bytes: usize = 512;

    let path = write_multi_page_file(
        &storage,
        &project,
        &table,
        &part,
        total_rows,
        rows_per_rg,
        page_size_bytes,
    )
    .await;

    // ---- Predicate read: ids in range (499, 600) exclusive ----
    // Matches ids 500..=599 (100 rows).
    let lo: i64 = 499;
    let hi: i64 = 600;
    let opts = ReadOptions {
        filters: vec![
            Predicate::Gt("id".into(), ScalarValue::Int64(lo)),
            Predicate::Lt("id".into(), ScalarValue::Int64(hi)),
        ],
        ..Default::default()
    };

    let counters = storage.read_counters().clone();
    counters.reset();

    let ids = read_ids_with_schema(&storage, &project, vec![path.clone()], opts).await;

    let snap = counters.snapshot();

    // 1. Correctness: must return exactly the 100 rows in the range.
    assert_eq!(
        ids.len(),
        100,
        "expected 100 rows in (499, 600); got {}",
        ids.len()
    );
    assert_eq!(*ids.first().unwrap(), 500);
    assert_eq!(*ids.last().unwrap(), 599);

    // 2. Pruning effectiveness: rows_selected_by_page_index must be
    //    substantially smaller than total_rows. We assert < 10 % of the
    //    row group to confirm real pruning (not just 1-page savings).
    //    The actual number depends on page size / row density; at 512-byte
    //    pages with Int64 + short Utf8, each page holds ~50-100 rows, so
    //    the selection should cover a handful of pages, not 65_000 rows.
    let selected = snap.rows_selected_by_page_index;
    assert!(
        selected > 0,
        "rows_selected_by_page_index must be > 0 when page pruning fires (got 0 — \
         page index may not have been loaded or the file has only one page per rg)"
    );
    let rg_size = total_rows as u64;
    assert!(
        selected < rg_size / 10,
        "rows_selected_by_page_index ({selected}) should be well below 10 % of rg_size \
         ({rg_size}) — page pruning should skip most pages in this narrow-range query"
    );

    // ---- Control read: no predicate — must decode all rows ----
    let opts_full = ReadOptions::default();
    counters.reset();

    let all_ids =
        read_ids_with_schema(&storage, &project, vec![path.clone()], opts_full).await;

    let snap_full = counters.snapshot();

    assert_eq!(
        all_ids.len(),
        total_rows,
        "full scan must return all rows"
    );
    // No filters → no page selection built → counter stays 0.
    assert_eq!(
        snap_full.rows_selected_by_page_index,
        0,
        "no-predicate read must not set rows_selected_by_page_index"
    );
}

/// Regression: when the predicate spans the ENTIRE row-group value range
/// (e.g. Gt(-1) AND Lt(MAX+1)), no page should be skipped — `any_page_pruned`
/// must stay false and `with_row_selection` must not be called. The read
/// must return all rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn page_index_keeps_all_pages_when_predicate_spans_full_range() {
    basin_common::telemetry::try_init_for_tests();
    let storage = storage_mem();
    let project = ProjectId::new();
    let table = TableName::new("page_prune_noop").unwrap();
    let part = PartitionKey::default_key();

    let total_rows: usize = 10_000;
    let path = write_multi_page_file(
        &storage,
        &project,
        &table,
        &part,
        total_rows,
        total_rows,
        512,
    )
    .await;

    // Predicate that matches everything: Gt(-1) AND Lt(10_001).
    let opts = ReadOptions {
        filters: vec![
            Predicate::Gt("id".into(), ScalarValue::Int64(-1)),
            Predicate::Lt("id".into(), ScalarValue::Int64(10_001)),
        ],
        ..Default::default()
    };

    let counters = storage.read_counters().clone();
    counters.reset();

    let ids = read_ids_with_schema(&storage, &project, vec![path.clone()], opts).await;

    assert_eq!(
        ids.len(),
        total_rows,
        "span-full-range predicate must return all rows"
    );
    // rows_selected_by_page_index == 0 means no page was pruned — this is
    // correct because every page is within the predicate range.
    // (The counter only goes > 0 when at least one page IS skipped.)
    assert_eq!(
        counters.snapshot().rows_selected_by_page_index,
        0,
        "no pages should be skipped when predicate spans the full value range"
    );
}
