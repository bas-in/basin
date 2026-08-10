//! TimescaleDB continuous-aggregate SQL surface.
//!
//! Continuous aggregates in Basin reuse the existing `basin.continuous`
//! materialised-view engine ([`crate::cv_ddl`] + [`crate::cv_time_bucket`]):
//! a continuous aggregate is a real Basin table holding bucketed aggregate
//! rows, populated by running the defining `time_bucket(...)` query over the
//! source hypertable and atomically swapping in a fresh Parquet file. The
//! materialisation *watermark* is `CvDef::last_bucket_max_unix_ms`.
//!
//! The TimescaleDB-specific surface this module adds on top of that engine:
//!
//! * `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) AS SELECT
//!   time_bucket(...), aggs FROM hypertable GROUP BY bucket` — the
//!   `timescaledb.continuous` flag is recognised in [`crate::cv_ddl`] and
//!   produces a `CvDef { timescaledb: true, .. }`. `DROP MATERIALIZED VIEW`
//!   reuses the existing CV drop. Both run through the standard MV dispatch.
//!
//! * `refresh_continuous_aggregate(cagg, window_start, window_end)` — re-runs
//!   the agg query for the `[start, end)` window and upserts (delete-then-
//!   append) the materialised rows whose bucket falls in that window, then
//!   advances the watermark. `NULL` bounds mean unbounded.
//!
//! * `add_continuous_aggregate_policy(cagg, start_offset, end_offset,
//!   schedule_interval)` — stores a [`basin_catalog::CaggPolicy`] on the
//!   `CvDef`. The policy refreshes `[now()-start_offset, now()-end_offset]`
//!   every `schedule_interval`. The policy is *driven* the same way the
//!   retention policy is: on-demand (deterministically triggerable in tests
//!   via [`run_cagg_policy`]) and, in production, by the background CV
//!   refresher tick.
//!
//! ## Real-time vs materialised-only (v1 decision)
//!
//! Reads of a continuous aggregate serve the **materialised store only** —
//! `SELECT ... FROM <cagg>` reads the Parquet-backed table exactly like any
//! other table (the existing CV read path). TimescaleDB's *real-time*
//! aggregation (materialised rows UNION a live `time_bucket` over source rows
//! above the watermark) is **not** merged in at read time. A read issued
//! between refreshes can therefore lag the source by up to the materialised
//! watermark. This matches the documented `basin.continuous` v0.1 behaviour
//! and is honest about the staleness bound: the freshness boundary is
//! `CvDef::last_bucket_max_unix_ms`. The union-with-live-tail rewrite is
//! deferred; it does not fall out cleanly because the materialised schema
//! (post-aggregation column names/types) must be reconciled with a freshly
//! computed tail, and partial-bucket double-counting at the watermark edge
//! needs careful handling — punting keeps v1 correct rather than subtly
//! wrong.
//!
//! ## drop_chunks interaction
//!
//! `drop_chunks` on the *source* hypertable deletes source rows but never
//! touches the continuous aggregate's table (a separate catalog relation).
//! Already-materialised aggregate rows therefore persist after the source
//! chunks are dropped — matching TimescaleDB semantics. Retention on the
//! cagg itself is just `DROP`/`DELETE` against the cagg table.

use std::sync::Arc;

use arrow_array::RecordBatch;
use basin_catalog::{CaggPolicy, Catalog, CvDef, DataFileRef, TableMetadata};
use basin_common::{BasinError, PartitionKey, Result, TableName};
use chrono::{DateTime, Utc};

use crate::{ExecResult, ProjectSession};

// ─── parsing: refresh_continuous_aggregate ────────────────────────────────────

/// One bound of a refresh window. `NULL` / omitted → unbounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WindowBound {
    /// No bound (`NULL`): unbounded in this direction.
    Unbounded,
    /// Absolute timestamp from a `TIMESTAMP '...'` / quoted literal.
    Timestamp(String),
}

/// Parsed `refresh_continuous_aggregate('cagg', <start>, <end>)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefreshCaggIntent {
    pub cagg: String,
    pub start: WindowBound,
    pub end: WindowBound,
}

/// Match `[CALL] refresh_continuous_aggregate('cagg', <start>, <end>)`.
/// Accepts the `SELECT refresh_continuous_aggregate(...)` and bare `CALL`
/// forms, with an optional `basin.`/`timescaledb.`/`public.` schema prefix on
/// the function name. The two window arguments may be `NULL`, a quoted
/// timestamp literal, or a `TIMESTAMP '...'` / `TIMESTAMPTZ '...'` cast.
pub(crate) fn match_refresh_continuous_aggregate(sql: &str) -> Option<RefreshCaggIntent> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.contains("refresh_continuous_aggregate") {
        return None;
    }
    // Statement must start with SELECT or CALL.
    if !(lower.starts_with("select") || lower.starts_with("call")) {
        return None;
    }
    let pos = lower.find("refresh_continuous_aggregate")? + "refresh_continuous_aggregate".len();
    let args = paren_contents(&trimmed[pos..])?;
    let parts = split_args(args);
    if parts.is_empty() {
        return None;
    }
    let cagg = strip_quotes(parts[0].trim());
    let start = parse_window_bound(parts.get(1).map(|s| s.trim()).unwrap_or(""));
    let end = parse_window_bound(parts.get(2).map(|s| s.trim()).unwrap_or(""));
    Some(RefreshCaggIntent { cagg, start, end })
}

/// Parse a single window-bound argument.
fn parse_window_bound(arg: &str) -> WindowBound {
    let a = arg.trim();
    if a.is_empty() || a.eq_ignore_ascii_case("null") {
        return WindowBound::Unbounded;
    }
    // `TIMESTAMP '...'` / `TIMESTAMPTZ '...'` cast — strip the keyword.
    let lower = a.to_ascii_lowercase();
    for kw in &["timestamptz", "timestamp"] {
        if lower.starts_with(kw) {
            let rest = a[kw.len()..].trim();
            return WindowBound::Timestamp(strip_quotes(rest));
        }
    }
    WindowBound::Timestamp(strip_quotes(a))
}

// ─── parsing: add_continuous_aggregate_policy ──────────────────────────────────

/// Parsed `add_continuous_aggregate_policy('cagg', start_offset, end_offset,
/// schedule_interval => '...')`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddCaggPolicyIntent {
    pub cagg: String,
    /// `start_offset` interval text, or `None` for `NULL` (unbounded start).
    pub start_offset: Option<String>,
    /// `end_offset` interval text, or `None` for `NULL` (up to now()).
    pub end_offset: Option<String>,
    /// `schedule_interval` interval text. Required.
    pub schedule_interval: String,
}

/// Match `SELECT add_continuous_aggregate_policy('cagg', start_offset =>
/// INTERVAL '...', end_offset => INTERVAL '...', schedule_interval =>
/// INTERVAL '...')`. Positional and named-arg forms both accepted; the three
/// interval arguments are matched left-to-right in the canonical Timescale
/// order (start_offset, end_offset, schedule_interval) when unnamed.
pub(crate) fn match_add_continuous_aggregate_policy(sql: &str) -> Option<AddCaggPolicyIntent> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.contains("add_continuous_aggregate_policy") {
        return None;
    }
    if !(lower.starts_with("select") || lower.starts_with("call")) {
        return None;
    }
    let pos =
        lower.find("add_continuous_aggregate_policy")? + "add_continuous_aggregate_policy".len();
    let args = paren_contents(&trimmed[pos..])?;
    let parts = split_args(args);
    if parts.is_empty() {
        return None;
    }
    let cagg = strip_quotes(parts[0].trim());

    // Resolve each named or positional interval argument.
    let mut start_offset: Option<Option<String>> = None;
    let mut end_offset: Option<Option<String>> = None;
    let mut schedule_interval: Option<Option<String>> = None;
    let mut positional: Vec<Option<String>> = Vec::new();

    for raw in &parts[1..] {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if let Some(eq) = token.find("=>") {
            let key = token[..eq].trim().to_ascii_lowercase();
            let val = token[eq + 2..].trim();
            let parsed = interval_arg_text(val);
            match key.as_str() {
                "start_offset" => start_offset = Some(parsed),
                "end_offset" => end_offset = Some(parsed),
                "schedule_interval" => schedule_interval = Some(parsed),
                _ => {}
            }
        } else {
            positional.push(interval_arg_text(token));
        }
    }

    // Fill remaining slots from positional args, in Timescale order.
    let mut pos_iter = positional.into_iter();
    if start_offset.is_none() {
        start_offset = Some(pos_iter.next().flatten());
    }
    if end_offset.is_none() {
        end_offset = Some(pos_iter.next().flatten());
    }
    if schedule_interval.is_none() {
        schedule_interval = Some(pos_iter.next().flatten());
    }

    let schedule_interval = schedule_interval.flatten()?;
    Some(AddCaggPolicyIntent {
        cagg,
        start_offset: start_offset.flatten(),
        end_offset: end_offset.flatten(),
        schedule_interval,
    })
}

/// Extract an interval's text from an argument that may be `NULL`,
/// `INTERVAL '...'`, or a bare quoted literal. Returns `None` for `NULL`.
fn interval_arg_text(arg: &str) -> Option<String> {
    let a = arg.trim();
    if a.is_empty() || a.eq_ignore_ascii_case("null") {
        return None;
    }
    let lower = a.to_ascii_lowercase();
    if lower.starts_with("interval") {
        let rest = a["interval".len()..].trim();
        return Some(strip_quotes(rest));
    }
    Some(strip_quotes(a))
}

// ─── executors ────────────────────────────────────────────────────────────────

/// Execute `refresh_continuous_aggregate(cagg, start, end)`.
pub(crate) async fn exec_refresh_continuous_aggregate(
    sess: &ProjectSession,
    intent: &RefreshCaggIntent,
) -> Result<ExecResult> {
    let table = TableName::new(&intent.cagg)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let meta = catalog
        .load_table(&sess.project, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => BasinError::not_found(format!(
                "refresh_continuous_aggregate: continuous aggregate {:?} does not exist",
                intent.cagg
            )),
            other => other,
        })?;
    let def = meta.continuous_aggregate.clone().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "refresh_continuous_aggregate: {:?} is not a continuous aggregate",
            intent.cagg
        ))
    })?;

    let start = resolve_bound(&intent.start)?;
    let end = resolve_bound(&intent.end)?;

    refresh_window(sess, &table, &meta, def, start, end).await?;

    Ok(ExecResult::Empty {
        tag: "refresh_continuous_aggregate".into(),
    })
}

/// Execute `add_continuous_aggregate_policy(...)` — stamp the policy on the
/// `CvDef`. Mirrors `add_retention_policy`: stores metadata, returns a job id.
pub(crate) async fn exec_add_continuous_aggregate_policy(
    sess: &ProjectSession,
    intent: &AddCaggPolicyIntent,
) -> Result<ExecResult> {
    let table = TableName::new(&intent.cagg)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let meta = catalog
        .load_table(&sess.project, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => BasinError::not_found(format!(
                "add_continuous_aggregate_policy: continuous aggregate {:?} does not exist",
                intent.cagg
            )),
            other => other,
        })?;
    let def = meta.continuous_aggregate.clone().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "add_continuous_aggregate_policy: {:?} is not a continuous aggregate",
            intent.cagg
        ))
    })?;

    let start_offset_secs = intent
        .start_offset
        .as_deref()
        .map(parse_interval_secs_or_err)
        .transpose()?;
    let end_offset_secs = intent
        .end_offset
        .as_deref()
        .map(parse_interval_secs_or_err)
        .transpose()?;
    let schedule_interval_secs = parse_interval_secs_or_err(&intent.schedule_interval)?;

    let policy = CaggPolicy {
        start_offset_secs,
        end_offset_secs,
        schedule_interval_secs,
    };
    // Also drive the refresh cadence off the schedule_interval so the
    // background CV refresher picks the cagg up at the policy's pace.
    let new_def = CvDef {
        refresh_interval_secs: schedule_interval_secs,
        cagg_policy: Some(policy),
        ..def
    };
    catalog
        .set_continuous_aggregate(&sess.project, &table, Some(new_def))
        .await?;

    // Mirror add_retention_policy's single-column `job_id` result shape.
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "add_continuous_aggregate_policy",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1000i64])) as arrow_array::ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("add_continuous_aggregate_policy result: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

/// Deterministically run a cagg's refresh policy once: refresh the window
/// `[now()-start_offset, now()-end_offset]`. Returns whether a policy existed
/// and was run. This is the on-demand driver mirroring `run_retention_policy`;
/// the production background refresher calls the same `refresh_window` core.
pub(crate) async fn run_cagg_policy(sess: &ProjectSession, cagg: &str) -> Result<bool> {
    let table = TableName::new(cagg)?;
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let meta = catalog.load_table(&sess.project, &table).await?;
    let Some(def) = meta.continuous_aggregate.clone() else {
        return Ok(false);
    };
    let Some(policy) = def.cagg_policy.clone() else {
        return Ok(false);
    };
    let now = Utc::now();
    let start = policy
        .start_offset_secs
        .map(|s| now - chrono::Duration::seconds(s as i64));
    let end = policy
        .end_offset_secs
        .map(|s| now - chrono::Duration::seconds(s as i64));
    refresh_window(sess, &table, &meta, def, start, end).await?;
    Ok(true)
}

// ─── windowed refresh core ──────────────────────────────────────────────────────

/// Re-run the cagg's defining query for the `[start, end)` window and upsert
/// the materialised rows whose bucket falls in that window.
///
/// `start`/`end` of `None` mean unbounded. When both bounds are `None` this
/// is a full re-materialisation. The watermark (`last_bucket_max_unix_ms`) is
/// advanced to the source's max bucket so reads have an honest freshness
/// boundary.
async fn refresh_window(
    sess: &ProjectSession,
    table: &TableName,
    meta: &TableMetadata,
    def: CvDef,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<()> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    let bucket = crate::cv_time_bucket::detect_time_bucket(&def.query_sql);

    // 1. Compute the fresh aggregate rows for the window. We wrap the source
    //    table in a windowing subquery on the bucket column when a bound is
    //    present; otherwise we run the full defining query.
    let windowed_sql = match (&bucket, start, end) {
        (Some(info), s, e) if s.is_some() || e.is_some() => {
            rewrite_for_window(&def.query_sql, &info.bucket_column, s, e)?
        }
        _ => def.query_sql.clone(),
    };
    let res = Box::pin(crate::executor::execute(sess, &windowed_sql)).await?;
    let (new_schema, new_batches) = match res {
        ExecResult::Rows { schema, batches } => (schema, batches),
        ExecResult::Empty { tag } => {
            return Err(BasinError::internal(format!(
                "refresh_continuous_aggregate: window query returned non-rows ({tag})"
            )));
        }
    };

    // 2. Read the existing materialised rows and keep the ones OUTSIDE the
    //    refreshed window (so we upsert only the window). When we have no
    //    bucket info we cannot identify which rows to keep — full replace.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    if let Some(info) = &bucket {
        let kept_sql = format!("SELECT * FROM \"{}\"", table.as_str());
        let kept_res = Box::pin(crate::executor::execute(sess, &kept_sql)).await?;
        if let ExecResult::Rows {
            schema: existing_schema,
            batches: existing_batches,
        } = kept_res
        {
            if existing_schema.fields() == new_schema.fields() {
                let kept =
                    filter_outside_window(&existing_batches, &info.bucket_alias, start, end)?;
                all_batches.extend(kept);
            }
        }
    }
    all_batches.extend(new_batches);

    // 3. Materialise: write a fresh Parquet file and atomically swap it in.
    let now = Utc::now();
    let new_watermark_ms = match &bucket {
        Some(info) => compute_source_watermark_ms(sess, &def.source_table, info)
            .await
            .or(def.last_bucket_max_unix_ms),
        None => def.last_bucket_max_unix_ms,
    };

    let removed_paths: Vec<String> = meta
        .snapshots
        .iter()
        .flat_map(|s| s.data_files.iter().map(|f| f.path.clone()))
        .collect();

    let new_def = CvDef {
        last_refreshed_at_unix_ms: Some(now.timestamp_millis()),
        last_bucket_max_unix_ms: new_watermark_ms,
        ..def
    };

    if all_batches.is_empty() {
        // Nothing materialised — clear the file set but stamp the watermark.
        if !removed_paths.is_empty() {
            catalog
                .replace_data_files(
                    &sess.project,
                    table,
                    meta.current_snapshot,
                    removed_paths.clone(),
                    vec![],
                )
                .await?;
            for path in &removed_paths {
                let p = object_store::path::Path::from(path.as_str());
                let _ = storage.delete_file(&sess.project, &p).await;
            }
        }
        catalog
            .set_continuous_aggregate(&sess.project, table, Some(new_def))
            .await?;
        crate::session::refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table)
            .await?;
        return Ok(());
    }

    let merged = if all_batches.len() == 1 {
        all_batches.remove(0)
    } else {
        let refs: Vec<&RecordBatch> = all_batches.iter().collect();
        arrow_select::concat::concat_batches(&new_schema, refs).map_err(|e| {
            BasinError::internal(format!("refresh_continuous_aggregate: concat: {e}"))
        })?
    };
    let part = PartitionKey::default_key();
    let written = storage
        .write_batch(&sess.project, table, &part, &merged)
        .await?;
    let added = vec![DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
        bloom_filters: ::std::collections::BTreeMap::new(),
        hll_sketches: ::std::collections::BTreeMap::new(),
        tdigest_sketches: ::std::collections::BTreeMap::new(),
    }];
    catalog
        .replace_data_files(
            &sess.project,
            table,
            meta.current_snapshot,
            removed_paths.clone(),
            added,
        )
        .await?;
    for path in &removed_paths {
        if path == written.path.as_ref() {
            continue;
        }
        let p = object_store::path::Path::from(path.as_str());
        let _ = storage.delete_file(&sess.project, &p).await;
    }

    catalog
        .set_continuous_aggregate(&sess.project, table, Some(new_def))
        .await?;
    crate::session::refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table)
        .await?;
    Ok(())
}

/// Rewrite the defining SELECT so the source table is wrapped in a subquery
/// filtering `bucket_column` to `[start, end)`. Reuses the same derived-table
/// splice strategy as [`crate::cv_time_bucket::rewrite_for_watermark`].
fn rewrite_for_window(
    query_sql: &str,
    bucket_column: &str,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<String> {
    use sqlparser::ast::{SetExpr, Statement, TableFactor};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, query_sql)
        .map_err(|e| BasinError::internal(format!("cagg window: parse failed: {e}")))?;
    if stmts.len() != 1 {
        return Err(BasinError::internal("cagg window: expected one statement"));
    }
    let query = match &mut stmts[0] {
        Statement::Query(q) => q.as_mut(),
        _ => return Err(BasinError::internal("cagg window: not a SELECT")),
    };
    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s.as_mut(),
        _ => {
            return Err(BasinError::internal(
                "cagg window: set-op SELECT not supported",
            ))
        }
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(BasinError::internal("cagg window: multi-table / JOIN FROM"));
    }
    let table_ref = &mut select.from[0].relation;
    let table_name = match table_ref {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(BasinError::internal(
                "cagg window: derived FROM not supported",
            ))
        }
    };

    let mut preds: Vec<String> = Vec::new();
    if let Some(s) = start {
        preds.push(format!(
            "{col} >= TIMESTAMPTZ '{ts}'",
            col = bucket_column,
            ts = s.format("%Y-%m-%d %H:%M:%S%.6f%:z")
        ));
    }
    if let Some(e) = end {
        preds.push(format!(
            "{col} < TIMESTAMPTZ '{ts}'",
            col = bucket_column,
            ts = e.format("%Y-%m-%d %H:%M:%S%.6f%:z")
        ));
    }
    let where_clause = if preds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", preds.join(" AND "))
    };
    let derived_sql = format!(
        "SELECT * FROM (SELECT * FROM {tbl}{where_clause}) AS {tbl}",
        tbl = table_name,
    );
    let derived_stmts = Parser::parse_sql(&PostgreSqlDialect {}, &derived_sql)
        .map_err(|e| BasinError::internal(format!("cagg window: build derived: {e}")))?;
    let new_factor = match &derived_stmts[0] {
        Statement::Query(q) => match q.body.as_ref() {
            SetExpr::Select(s) => s.from[0].relation.clone(),
            _ => return Err(BasinError::internal("cagg window: derived SELECT lost")),
        },
        _ => return Err(BasinError::internal("cagg window: derived not a SELECT")),
    };
    *table_ref = new_factor;
    Ok(format!("{}", stmts[0]))
}

/// Keep materialised rows whose `bucket_alias` is OUTSIDE `[start, end)`
/// (i.e. `< start` OR `>= end`). Rows inside the window are dropped because
/// the freshly computed window rows replace them (upsert). Works on
/// timestamp-typed bucket columns; any other type → keep nothing (full
/// replace, conservative).
fn filter_outside_window(
    batches: &[RecordBatch],
    bucket_alias: &str,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<Vec<RecordBatch>> {
    use arrow_array::{
        Array, BooleanArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let col_idx = schema.index_of(bucket_alias).ok().or_else(|| {
        schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(bucket_alias))
    });
    let Some(col_idx) = col_idx else {
        return Ok(Vec::new());
    };
    let mut kept: Vec<RecordBatch> = Vec::new();
    for batch in batches {
        let arr = batch.column(col_idx);
        let DataType::Timestamp(unit, _) = arr.data_type() else {
            return Ok(Vec::new());
        };
        let to_unit = |dt: DateTime<Utc>| -> i64 {
            match unit {
                TimeUnit::Second => dt.timestamp(),
                TimeUnit::Millisecond => dt.timestamp_millis(),
                TimeUnit::Microsecond => dt.timestamp_micros(),
                TimeUnit::Nanosecond => dt.timestamp_nanos_opt().unwrap_or(i64::MAX),
            }
        };
        let start_b = start.map(to_unit);
        let end_b = end.map(to_unit);
        let value_at = |i: usize| -> i64 {
            match unit {
                TimeUnit::Second => arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(i),
                TimeUnit::Millisecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(i),
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(i),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(i),
            }
        };
        let mask: Vec<Option<bool>> = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    return Some(true); // keep null buckets (can't window them)
                }
                let v = value_at(i);
                // Inside window iff (start is None or v >= start) AND
                // (end is None or v < end). Keep when OUTSIDE.
                let ge_start = start_b.map_or(true, |s| v >= s);
                let lt_end = end_b.map_or(true, |e| v < e);
                let inside = ge_start && lt_end;
                Some(!inside)
            })
            .collect();
        let mask = BooleanArray::from(mask);
        let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| BasinError::internal(format!("cagg filter: {e}")))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}

/// Query the source for `max(bucket_col)` floored to the bucket start,
/// returned as Unix-epoch ms. Mirrors `cv_ddl::compute_watermark_ms`.
async fn compute_source_watermark_ms(
    sess: &ProjectSession,
    source_table: &str,
    bucket: &crate::cv_time_bucket::BucketInfo,
) -> Option<i64> {
    if source_table.is_empty() {
        return None;
    }
    let sql = format!(
        "SELECT max({col}) AS m FROM {tbl}",
        col = bucket.bucket_column,
        tbl = source_table,
    );
    let res = Box::pin(crate::executor::execute(sess, &sql)).await.ok()?;
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => return None,
    };
    use arrow_array::{
        Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch.column(0);
        if arr.is_null(0) {
            return None;
        }
        let raw: Option<DateTime<Utc>> = match arr.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .and_then(|a| DateTime::<Utc>::from_timestamp_micros(a.value(0))),
            DataType::Timestamp(TimeUnit::Millisecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .and_then(|a| DateTime::<Utc>::from_timestamp_millis(a.value(0))),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .and_then(|a| {
                    let v = a.value(0);
                    DateTime::<Utc>::from_timestamp(v / 1_000_000_000, (v % 1_000_000_000) as u32)
                }),
            DataType::Timestamp(TimeUnit::Second, _) => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .and_then(|a| DateTime::<Utc>::from_timestamp(a.value(0), 0)),
            _ => None,
        };
        if let Some(ts) = raw {
            return Some(bucket.width.floor(ts).timestamp_millis());
        }
    }
    None
}

/// Resolve a [`WindowBound`] to an absolute timestamp.
fn resolve_bound(b: &WindowBound) -> Result<Option<DateTime<Utc>>> {
    match b {
        WindowBound::Unbounded => Ok(None),
        WindowBound::Timestamp(s) => parse_ts(s).map(Some).ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "refresh_continuous_aggregate: could not parse window bound {s:?}"
            ))
        }),
    }
}

/// Parse a timestamp string in common ISO / PG formats.
fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    use chrono::NaiveDateTime;
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // `%#z` accepts the colon-less / hours-only offset forms (`+00`, `+0000`,
    // `+00:00`) PG emits; `%:z` only accepts the colon form, so a bare `+00`
    // literal (e.g. `TIMESTAMP '2024-01-15 10:00:00+00'`) needs `%#z`.
    let formats = &[
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%#z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for fmt in formats {
        // Offset-aware parse first: `NaiveDateTime::parse_from_str` will happily
        // consume (and silently discard) a `%#z`/`%:z` offset, yielding a wrong
        // UTC value, so the timezone-bearing `DateTime` parse must win.
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ndt.and_utc());
        }
    }
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(nd.and_hms_opt(0, 0, 0)?.and_utc());
    }
    None
}

/// Parse an interval text to seconds, erroring on failure.
fn parse_interval_secs_or_err(s: &str) -> Result<u64> {
    crate::hypertable::parse_interval_secs(s).ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "add_continuous_aggregate_policy: could not parse interval {s:?}"
        ))
    })
}

// ─── small SQL text helpers (local; mirror hypertable.rs) ──────────────────────

fn paren_contents(s: &str) -> Option<&str> {
    let s = s.trim_start();
    if !s.starts_with('(') {
        return None;
    }
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut open_pos = None;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                if depth == 1 {
                    open_pos = Some(i);
                }
            }
            b')' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
                if depth == 0 {
                    let start = open_pos? + 1;
                    return Some(&s[start..i]);
                }
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn split_args(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' {
                    i += 1;
                }
            }
            b',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_refresh_basic() {
        let sql = "CALL refresh_continuous_aggregate('daily', NULL, NULL)";
        let i = match_refresh_continuous_aggregate(sql).expect("match");
        assert_eq!(i.cagg, "daily");
        assert_eq!(i.start, WindowBound::Unbounded);
        assert_eq!(i.end, WindowBound::Unbounded);
    }

    #[test]
    fn match_refresh_with_window() {
        let sql = "SELECT refresh_continuous_aggregate('m', TIMESTAMP '2024-01-01', TIMESTAMP '2024-02-01')";
        let i = match_refresh_continuous_aggregate(sql).expect("match");
        assert_eq!(i.cagg, "m");
        assert_eq!(i.start, WindowBound::Timestamp("2024-01-01".into()));
        assert_eq!(i.end, WindowBound::Timestamp("2024-02-01".into()));
    }

    #[test]
    fn match_add_policy_named() {
        let sql = "SELECT add_continuous_aggregate_policy('cagg', \
            start_offset => INTERVAL '1 month', \
            end_offset => INTERVAL '1 hour', \
            schedule_interval => INTERVAL '30 minutes')";
        let i = match_add_continuous_aggregate_policy(sql).expect("match");
        assert_eq!(i.cagg, "cagg");
        assert_eq!(i.start_offset.as_deref(), Some("1 month"));
        assert_eq!(i.end_offset.as_deref(), Some("1 hour"));
        assert_eq!(i.schedule_interval, "30 minutes");
    }

    #[test]
    fn match_add_policy_positional_with_null_start() {
        let sql = "SELECT add_continuous_aggregate_policy('cagg', NULL, \
            INTERVAL '1 hour', INTERVAL '1 hour')";
        let i = match_add_continuous_aggregate_policy(sql).expect("match");
        assert_eq!(i.cagg, "cagg");
        assert_eq!(i.start_offset, None);
        assert_eq!(i.end_offset.as_deref(), Some("1 hour"));
        assert_eq!(i.schedule_interval, "1 hour");
    }

    #[test]
    fn refresh_not_matched_for_unrelated() {
        assert!(match_refresh_continuous_aggregate("SELECT 1").is_none());
        assert!(match_add_continuous_aggregate_policy("SELECT 1").is_none());
    }

    #[test]
    fn window_bound_parses_timestamptz() {
        assert_eq!(parse_window_bound("NULL"), WindowBound::Unbounded);
        assert_eq!(
            parse_window_bound("TIMESTAMPTZ '2024-01-01 00:00:00+00'"),
            WindowBound::Timestamp("2024-01-01 00:00:00+00".into())
        );
    }

    #[test]
    fn parse_ts_accepts_hours_only_offset() {
        // PG emits a colon-less hours-only offset (`+00`); the bound parser
        // must accept it as well as the colon and naive forms.
        let want = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(parse_ts("2024-01-15 10:00:00+00"), Some(want));
        assert_eq!(parse_ts("2024-01-15 10:00:00+00:00"), Some(want));
        assert_eq!(parse_ts("2024-01-15 10:00:00"), Some(want));
        // A non-zero hours-only offset is normalised to UTC: 10:00 at +02
        // is 08:00 UTC.
        let want_offset = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(parse_ts("2024-01-15 10:00:00+02"), Some(want_offset));
    }
}
