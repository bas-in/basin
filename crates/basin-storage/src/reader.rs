//! Parquet reader with projection + predicate pushdown.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use basin_common::{BasinError, Result, TableName, TenantId};
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::data_file::DataFile;
use crate::paths::table_data_prefix;
use crate::predicate::{self, Predicate, ScalarValue};
use crate::{ReadOptions, Storage};

pub(crate) async fn list_data_files(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
) -> Result<Vec<DataFile>> {
    let prefix = table_data_prefix(storage.root_prefix(), tenant, table);
    let mut files = Vec::new();
    let mut stream = storage.object_store().list(Some(&prefix));
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
        if !meta.location.as_ref().ends_with(".parquet") {
            continue;
        }
        files.push(DataFile {
            path: meta.location,
            size_bytes: meta.size as u64,
            row_count: 0,
            column_stats: BTreeMap::new(),
        });
    }
    Ok(files)
}

pub(crate) async fn read(
    storage: &Storage,
    tenant: &TenantId,
    table: &TableName,
    opts: ReadOptions,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let prefix = table_data_prefix(storage.root_prefix(), tenant, table);
    let store = storage.object_store().clone();
    let tenant_id_string = tenant.as_prefix();

    let mut paths: Vec<ObjectPath> = Vec::new();
    {
        let mut s = store.list(Some(&prefix));
        while let Some(meta) = s.next().await {
            let meta = meta.map_err(|e| BasinError::storage(format!("list: {e}")))?;
            if !meta.location.as_ref().ends_with(".parquet") {
                continue;
            }
            // Belt-and-braces: never read a file whose key isn't under the
            // tenant prefix. If this ever fired we'd want a P0; we treat it
            // as `IsolationViolation`, not `Storage`.
            let expected = format!("tenants/{tenant_id_string}/");
            if !meta.location.as_ref().contains(&expected) {
                return Err(BasinError::isolation(format!(
                    "listed object {} does not contain {}",
                    meta.location, expected
                )));
            }
            paths.push(meta.location);
        }
    }

    let opts = Arc::new(opts);
    let store_for_stream = store.clone();
    let stream = futures::stream::iter(paths)
        .map(move |p| {
            let store = store_for_stream.clone();
            let opts = opts.clone();
            async move { read_one(store, p, opts).await }
        })
        .buffered(4)
        .map(|res: Result<BoxStream<'static, Result<RecordBatch>>>| match res {
            Ok(s) => s,
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .flatten();

    Ok(stream.boxed())
}

async fn read_one(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    opts: Arc<ReadOptions>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let meta = store
        .head(&path)
        .await
        .map_err(|e| BasinError::storage(format!("head {path}: {e}")))?;
    let reader = ParquetObjectReader::new(store, meta);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| BasinError::storage(format!("open parquet {path}: {e}")))?;

    let arrow_schema: SchemaRef = builder.schema().clone();
    let parquet_schema = builder.metadata().file_metadata().schema_descr_ptr();

    // Projection.
    let projection_mask = match &opts.projection {
        Some(cols) => {
            let mut idxs = Vec::with_capacity(cols.len());
            for c in cols {
                let i = arrow_schema
                    .index_of(c)
                    .map_err(|_| BasinError::storage(format!("unknown column {c}")))?;
                idxs.push(i);
            }
            // Use roots() with column indexes — this works for flat schemas;
            // for nested schemas we'd descend, but Phase 1 is flat-only.
            ProjectionMask::roots(&parquet_schema, idxs)
        }
        None => ProjectionMask::all(),
    };

    // Row-group pruning by stats.
    let kept: Vec<usize> = {
        let row_groups = builder.metadata().row_groups();
        (0..row_groups.len())
            .filter(|i| !row_group_pruned(&row_groups[*i], &arrow_schema, &opts.filters))
            .collect()
    };

    let mut builder = builder
        .with_projection(projection_mask.clone())
        .with_row_groups(kept);

    // Per-row filtering as a fallback. Pushed in via RowFilter so the parquet
    // reader can also use it for page index pruning where available.
    if !opts.filters.is_empty() {
        let predicates = build_row_filter(
            &opts.filters,
            &arrow_schema,
            &parquet_schema,
        )?;
        builder = builder.with_row_filter(predicates);
    }

    let stream = builder
        .build()
        .map_err(|e| BasinError::storage(format!("parquet build {path}: {e}")))?;

    let mapped = stream.map(|res| res.map_err(|e| BasinError::storage(format!("parquet read: {e}"))));
    Ok(mapped.boxed())
}

fn build_row_filter(
    filters: &[Predicate],
    arrow_schema: &SchemaRef,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<RowFilter> {
    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
        Vec::with_capacity(filters.len());

    for filter in filters {
        let col = filter.column().to_string();
        let col_idx = arrow_schema
            .index_of(&col)
            .map_err(|_| BasinError::storage(format!("unknown column {col}")))?;
        let mask = ProjectionMask::roots(parquet_schema, [col_idx]);
        let f = filter.clone();
        let pred = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            predicate::evaluate(&batch, &f)
                .map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e)))
        });
        predicates.push(Box::new(pred));
    }

    // Combined RowFilter ANDs predicates.
    Ok(RowFilter::new(predicates))
}

/// Decide if an entire row group can be pruned given the conjunction of
/// filters.
fn row_group_pruned(
    rg: &RowGroupMetaData,
    arrow_schema: &SchemaRef,
    filters: &[Predicate],
) -> bool {
    if filters.is_empty() {
        return false;
    }
    for f in filters {
        if predicate_excludes_group(rg, arrow_schema, f) {
            return true;
        }
    }
    false
}

fn predicate_excludes_group(
    rg: &RowGroupMetaData,
    arrow_schema: &SchemaRef,
    filter: &Predicate,
) -> bool {
    let col = filter.column();
    let Ok(idx) = arrow_schema.index_of(col) else {
        return false;
    };
    let Some(col_meta) = rg.columns().get(idx) else {
        return false;
    };
    let Some(stats) = col_meta.statistics() else {
        return false;
    };

    let Some(value) = filter_value(filter) else {
        return false;
    };

    match (filter, &value, stats) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            let min = s.min_opt().copied();
            let max = s.max_opt().copied();
            match (min, max) {
                (Some(min), Some(max)) => *v < min || *v > max,
                _ => false,
            }
        }
        (Predicate::Lt(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            // exclude if every value >= v
            s.min_opt().copied().is_some_and(|min| min >= *v)
        }
        (Predicate::Gt(_, _), ScalarValue::Int64(v), Statistics::Int64(s)) => {
            s.max_opt().copied().is_some_and(|max| max <= *v)
        }
        (Predicate::Eq(_, _), ScalarValue::Utf8(v), Statistics::ByteArray(s)) => {
            let bytes = v.as_bytes();
            let min = s.min_opt().map(|b| b.data());
            let max = s.max_opt().map(|b| b.data());
            match (min, max) {
                (Some(min), Some(max)) => bytes < min || bytes > max,
                _ => false,
            }
        }
        _ => false,
    }
}

fn filter_value(filter: &Predicate) -> Option<ScalarValue> {
    match filter {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => Some(v.clone()),
    }
}

