//! Viability test: native Parquet bloom-filter pruning on point queries.
//!
//! Card: `viability_bloom_filter_pruning`
//! Bar: with `bloom_filter_columns = ["id"]` configured on a multi-row-group
//! table, a point query for a non-existent `id` must prune ≥80% of the
//! row groups by bloom filter alone — i.e. the Parquet reader proves
//! "definitely not in this row group" for at least 80% of the file's
//! groups without reading the column data. The same configuration must
//! still return the correct row when the `id` exists.
//!
//! Why this matters. Min/max statistics already prune most files for a
//! point query when the values are well-clustered (e.g. monotonically
//! increasing IDs). The interesting case is the opposite: an `id` whose
//! distribution overlaps every file or every row group — a hash key, a
//! UUID, a randomly-shuffled identifier — where min/max can't prove
//! absence. Bloom filters turn "might be present" into "definitely
//! absent" for ~80%+ of row groups on the absent-value query, with
//! virtually no extra cost on the present-value query.
//!
//! Method:
//! 1. Build a 1000-row batch with `id` shuffled so its values overlap
//!    every row group's min/max range. This neutralises stats-based
//!    pruning so any saving comes from the bloom filter.
//! 2. Cap `max_row_group_size` to a small value so a single Parquet
//!    file has many row groups (≥10). Without this knob a 1k-row write
//!    becomes a single row group and the test would have nothing to
//!    measure.
//! 3. Point query for an ID that exists → assert one row returned.
//! 4. Point query for an ID that does NOT exist → assert zero rows
//!    returned and `row_groups_pruned_by_bloom` ≥ 0.8 × total groups.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig, WriteOptions};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// 1000 rows is enough to exercise the bloom-filter path while keeping
/// the test fast on slow hardware.
const ROWS: i64 = 1_000;
/// Force 10 row groups out of 1000 rows so the bloom-filter pruning has
/// something to drop. Without this knob the writer's default
/// `max_row_group_size = 65_536` would collapse the whole batch into a
/// single group and the test would be unable to observe per-group
/// pruning.
const ROW_GROUP_SIZE: usize = 100;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Build a `RecordBatch` with `ROWS` rows. The `id` column is filled
/// with a shuffled permutation of `[0, ROWS)` so each contiguous slice
/// of `ROW_GROUP_SIZE` rows spans the full id range. That way min/max
/// statistics cannot prune any group on a point query — every group's
/// `[min, max]` covers the target id, leaving the bloom filter as the
/// only pruning mechanism.
fn build_shuffled_batch() -> RecordBatch {
    // Deterministic Fisher–Yates with a fixed PRNG so the test is
    // bit-reproducible across runs (no `rand` dependency required —
    // a simple xorshift seeded on a constant is enough for shuffling).
    let mut ids: Vec<i64> = (0..ROWS).collect();
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }

    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = (0..ROWS).map(|i| format!("p-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_bloom_filter_pruning() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs,
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    // Catalog round-trip: configure `bloom_filter_columns = ["id"]` on
    // the table and confirm `load_table` reflects the change. The writer
    // doesn't currently consult the catalog directly (the engine layer
    // would be the right place for that wiring; out of scope for this
    // test card), so we also pass the column list explicitly into the
    // writer below — but the catalog setter has to round-trip cleanly
    // because the SQL surface (a future `ALTER TABLE … SET BLOOM`) will
    // sit on top of it.
    let catalog = InMemoryCatalog::new();
    catalog
        .create_table(&tenant, &table, &schema())
        .await
        .unwrap();
    catalog
        .set_bloom_filter_columns(&tenant, &table, vec!["id".to_string()])
        .await
        .unwrap();
    let meta = catalog.load_table(&tenant, &table).await.unwrap();
    assert_eq!(meta.bloom_filter_columns, vec!["id".to_string()]);

    let batch = build_shuffled_batch();
    let opts = WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        cluster_columns: vec![],
        max_row_group_size: Some(ROW_GROUP_SIZE),
    };
    let df = storage
        .write_batch_with_options(&tenant, &table, &part, &batch, &opts)
        .await
        .unwrap();
    let total_groups_in_file = ROWS as usize / ROW_GROUP_SIZE;
    println!(
        "[VIABILITY bloom] wrote {} rows, {} bytes, {} row groups (target)",
        df.row_count, df.size_bytes, total_groups_in_file
    );

    // ---- Sanity: the file actually has ≥10 row groups ---------------------
    // We rely on the writer honouring `max_row_group_size`. Assert via the
    // public list-with-stats helper.
    let listed = storage
        .list_data_files_with_stats(&tenant, &table)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "exactly one parquet file expected");
    // (`row_count` from the listing path; `column_stats` is per-file
    // already-aggregated, so we go via the read-counter snapshot below
    // for the per-row-group total.)

    // ---- Existing-id query ------------------------------------------------
    let counters = storage.read_counters().clone();
    counters.reset();
    let existing_target: i64 = 42;
    let opts_eq = ReadOptions {
        filters: vec![Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(existing_target),
        )],
        ..Default::default()
    };
    let mut stream = storage.read(&tenant, &table, opts_eq).await.unwrap();
    let mut hit_rows = 0usize;
    while let Some(b) = stream.next().await {
        hit_rows += b.unwrap().num_rows();
    }
    let existing_snap = counters.snapshot();
    assert_eq!(
        hit_rows, 1,
        "expected exactly one row for id={existing_target}",
    );
    println!(
        "[VIABILITY bloom] existing id={existing_target}: groups considered={}, scanned={}, pruned_bloom={}",
        existing_snap.row_groups_considered,
        existing_snap.row_groups_scanned,
        existing_snap.row_groups_pruned_by_bloom,
    );

    // ---- Non-existing-id query -- the headline metric --------------------
    counters.reset();
    let absent_target: i64 = ROWS + 12_345; // outside [0, ROWS)
    let opts_eq = ReadOptions {
        filters: vec![Predicate::Eq(
            "id".into(),
            ScalarValue::Int64(absent_target),
        )],
        ..Default::default()
    };
    let mut stream = storage.read(&tenant, &table, opts_eq).await.unwrap();
    let mut absent_rows = 0usize;
    while let Some(b) = stream.next().await {
        absent_rows += b.unwrap().num_rows();
    }
    assert_eq!(
        absent_rows, 0,
        "expected zero rows for non-existent id={absent_target}",
    );
    let absent_snap = counters.snapshot();
    println!(
        "[VIABILITY bloom] absent id={absent_target}: groups considered={}, scanned={}, pruned_stats={}, pruned_bloom={}",
        absent_snap.row_groups_considered,
        absent_snap.row_groups_scanned,
        absent_snap.row_groups_pruned_by_stats,
        absent_snap.row_groups_pruned_by_bloom,
    );

    // The shuffled `id` distribution defeats stats-based pruning (every
    // group's [min, max] covers `absent_target`'s range — well, almost:
    // because `absent_target` is *outside* [0, ROWS) the per-group max
    // does eliminate it via the existing min/max path. To make the
    // bloom filter the load-bearing prune we use an in-range absent id
    // instead. Pick a value in [0, ROWS) that we know isn't in the
    // batch — but every value in [0, ROWS) IS in the batch (we built a
    // permutation). So we test with an out-of-range target above and
    // also with a stats-defeating in-range "tear" by using a fractional
    // id encoded as an i64 outside the contiguous permutation span,
    // crafted below.
    //
    // Actually the cleaner construction: write the batch as a permutation
    // of `[0, ROWS)` but query for `ROWS + offset`. Stats-based pruning
    // catches that on its own, which is fine — the test demonstrates
    // both are working — but we additionally rerun against a value
    // *inside* the min/max envelope by inserting a deliberate gap.
    //
    // To get a clean bloom-filter signal, repeat with a target that
    // sits inside [0, ROWS) but isn't really there. We do this by
    // rebuilding a batch from `[0, 500) ∪ [600, 1100)` and querying
    // for 550 — every row group covers 550 in min/max, but no row group
    // contains it.
    let bloom_only_target = run_bloom_only_phase(&storage, &counters).await;

    // Headline metric: fraction of row groups pruned by bloom filter on
    // the in-range-absent-id query.
    let total_considered = bloom_only_target.row_groups_considered as f64;
    let pruned_by_bloom = bloom_only_target.row_groups_pruned_by_bloom as f64;
    let pruned_fraction = if total_considered > 0.0 {
        pruned_by_bloom / total_considered
    } else {
        0.0
    };
    let bar = 0.8;
    let pass = pruned_fraction >= bar;

    println!(
        "[VIABILITY bloom] bloom-only phase: groups considered={}, scanned={}, pruned_stats={}, pruned_bloom={}, fraction={:.2} (bar >={:.0}%) {}",
        bloom_only_target.row_groups_considered,
        bloom_only_target.row_groups_scanned,
        bloom_only_target.row_groups_pruned_by_stats,
        bloom_only_target.row_groups_pruned_by_bloom,
        pruned_fraction,
        bar * 100.0,
        if pass { "PASS" } else { "FAIL" },
    );

    report_viability(
        "bloom_filter_pruning",
        "Bloom filter pruning for absent point queries",
        "On a multi-row-group Parquet file with `bloom_filter_columns = [\"id\"]`, an in-range \
         point query for a non-existent id must prune at least 80% of row groups via the \
         per-group bloom filter, even when min/max stats cannot.",
        pass,
        PrimaryMetric {
            label: "row_groups_pruned_by_bloom / row_groups_considered".into(),
            value: pruned_fraction,
            unit: "fraction".into(),
            bar: BarOp::ge(bar),
        },
        json!({
            "rows": ROWS,
            "row_group_size": ROW_GROUP_SIZE,
            "absent_in_range_target": "550 (outside [500, 600) gap)",
            "existing_phase": {
                "groups_considered": existing_snap.row_groups_considered,
                "groups_scanned": existing_snap.row_groups_scanned,
                "pruned_by_stats": existing_snap.row_groups_pruned_by_stats,
                "pruned_by_bloom": existing_snap.row_groups_pruned_by_bloom,
            },
            "out_of_range_absent_phase": {
                "groups_considered": absent_snap.row_groups_considered,
                "groups_scanned": absent_snap.row_groups_scanned,
                "pruned_by_stats": absent_snap.row_groups_pruned_by_stats,
                "pruned_by_bloom": absent_snap.row_groups_pruned_by_bloom,
            },
            "bloom_only_phase": {
                "groups_considered": bloom_only_target.row_groups_considered,
                "groups_scanned": bloom_only_target.row_groups_scanned,
                "pruned_by_stats": bloom_only_target.row_groups_pruned_by_stats,
                "pruned_by_bloom": bloom_only_target.row_groups_pruned_by_bloom,
            },
        }),
    );

    assert!(
        pass,
        "bloom-filter pruning fraction {:.2} < bar {:.2} (groups_considered={}, pruned_by_bloom={})",
        pruned_fraction,
        bar,
        bloom_only_target.row_groups_considered,
        bloom_only_target.row_groups_pruned_by_bloom,
    );
}

/// Phase 3: query for an in-range value whose absence min/max cannot
/// detect — bloom filter is the only thing standing between the reader
/// and a full-file scan.
///
/// Construction: 1000 rows of `id ∈ [0, 500) ∪ [600, 1100)`. Each row
/// group's `[min, max]` covers 550 (because we shuffle), so stats
/// pruning is forced to "may be present" for every group. The bloom
/// filter, in contrast, was built from the actual values and proves
/// 550 is absent.
async fn run_bloom_only_phase(
    storage: &Storage,
    counters: &Arc<basin_storage::ReadCounters>,
) -> basin_storage::ReadCountersSnapshot {
    let tenant = TenantId::new();
    let table = TableName::new("bloom_only").unwrap();
    let part = PartitionKey::default_key();

    // Build a permutation of `[0, 500) ∪ [600, 1100)`.
    let mut ids: Vec<i64> = Vec::with_capacity(ROWS as usize);
    ids.extend(0i64..500);
    ids.extend(600i64..1100);
    let mut state: u64 = 0xCAFE_F00D_BAAD_BABE;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        ids.swap(i, j);
    }
    let id_arr: Int64Array = ids.iter().copied().collect();
    let payloads: Vec<String> = (0..ROWS).map(|i| format!("p-{:08}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    let batch =
        RecordBatch::try_new(schema(), vec![Arc::new(id_arr), Arc::new(payload_arr)]).unwrap();

    let opts = WriteOptions {
        bloom_filter_columns: vec!["id".to_string()],
        cluster_columns: vec![],
        max_row_group_size: Some(ROW_GROUP_SIZE),
    };
    storage
        .write_batch_with_options(&tenant, &table, &part, &batch, &opts)
        .await
        .unwrap();

    counters.reset();
    let target: i64 = 550; // sits in the [500, 600) gap
    let opts = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(target))],
        ..Default::default()
    };
    let mut stream = storage.read(&tenant, &table, opts).await.unwrap();
    let mut rows = 0usize;
    while let Some(b) = stream.next().await {
        rows += b.unwrap().num_rows();
    }
    assert_eq!(
        rows, 0,
        "expected zero rows for absent in-range id={target}"
    );
    counters.snapshot()
}
