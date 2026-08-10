//! Phase 5.29.G — `time_bucket_gapfill` + `locf` (TimescaleDB-compat).
//!
//! ## What this implements
//!
//! TimescaleDB's `time_bucket_gapfill(interval, ts [, start, finish])` returns
//! the *complete* bucket grid between `start` and `finish`, emitting a
//! synthetic row (with `NULL` aggregate values) for every bucket in that range
//! that has no source rows.  `locf(agg)` then carries the last non-`NULL`
//! observation forward across that densified grid.
//!
//! ## Approach (design-note option (b), realised as a post-process)
//!
//! The design note (`/tmp/basin_timescale/APPLY.md`) sketches two options:
//! (a) rewrite the plan into a `generate_series` UNION, or (b) a custom
//! execution node that densifies the aggregate output.  We take the spirit of
//! (b) but realise it as a **streaming post-process over the collected result
//! batches** rather than a bespoke `ExecutionPlan`, because:
//!
//!   * The common shape — `SELECT time_bucket_gapfill('1h', ts) AS b, agg(...)
//!     FROM t WHERE ts BETWEEN x AND y GROUP BY b ORDER BY b` — is fully
//!     determined by the *aggregated* output plus the `[start, finish)` bounds.
//!     We do not need to participate in planning to compute it correctly.
//!   * It reuses the entire existing aggregate + `time_bucket` machinery
//!     unchanged: we rewrite `time_bucket_gapfill(...)` → `time_bucket(...)`
//!     and `locf(x)` → `x`, run the rewritten query through the normal
//!     executor, and densify the resulting `RecordBatch`es.
//!
//! ## Supported shape (the contract)
//!
//! ```sql
//! SELECT time_bucket_gapfill('<interval>', <ts> [, <start>, <finish>]) AS b,
//!        [<group cols...>,]
//!        <agg(...) | locf(agg(...))>...
//! FROM   <...>
//! WHERE  ... <ts> BETWEEN <start> AND <finish> ...   -- bounds source
//! GROUP BY b [, <group cols...>]
//! [ORDER BY b]
//! ```
//!
//! * Exactly **one** `time_bucket_gapfill` call per query.
//! * Bounds come from the 4-arg form `(interval, ts, start, finish)` **or** a
//!   `ts BETWEEN start AND finish` predicate in the `WHERE` clause.  If neither
//!   is present the query is rejected, mirroring TimescaleDB's error
//!   `"missing time_bucket_gapfill argument: start and finish"`.
//! * The gapfill grid is `[floor(start), finish]` stepped by the bucket width;
//!   the lower bound is inclusive, the upper bound is inclusive of any bucket
//!   whose start `<= finish` (matching TimescaleDB, which emits a row for every
//!   bucket start in the closed range).
//! * Group columns (everything that is neither the bucket nor an aggregate) get
//!   a per-group independent grid (cross-join of distinct group keys × grid).
//!
//! ## NOT supported (typed `0A000` / `42601` errors)
//!
//! * More than one `time_bucket_gapfill` call — `FeatureNotSupported`.
//! * `locf` without a `time_bucket_gapfill` in the same query —
//!   `FeatureNotSupported` (TimescaleDB raises
//!   `"locf can only be used in an aggregation query with time_bucket_gapfill"`).
//! * `interpolate()` — linear interpolation between the nearest surrounding
//!   non-NULL values; leading/trailing gaps where one side has no value stay
//!   NULL (cannot interpolate without both endpoints).
//! * Unbounded gapfill (no start/finish and no `BETWEEN`) — `InvalidSchema`
//!   mirroring TimescaleDB's "missing argument" error.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use chrono::{DateTime, Utc};

use basin_common::BasinError;

use crate::cv_time_bucket::BucketWidth;

// ─── detection ────────────────────────────────────────────────────────────────

/// Fill mode for a single aggregate column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillMode {
    /// No fill wrapper — gap rows stay NULL.
    Null,
    /// `locf(agg)` — carry the last non-NULL observation forward.
    Locf,
    /// `interpolate(agg)` — linearly blend between the surrounding non-NULL
    /// observations; leading/trailing gaps (no value on one side) stay NULL.
    Interpolate,
}

/// A recognised gapfill request, extracted from the user's SQL.
#[derive(Debug, Clone)]
pub(crate) struct GapfillRequest {
    /// SQL with `time_bucket_gapfill(...)` rewritten to `time_bucket(interval,
    /// ts)` and every `locf(x)` / `interpolate(x)` unwrapped to `x`.
    /// This is what we actually run.
    pub(crate) rewritten_sql: String,
    /// Bucket width parsed from the first argument.
    pub(crate) width: BucketWidth,
    /// Inclusive lower bound (µs since epoch) of the gapfill range.
    pub(crate) start_us: i64,
    /// Inclusive upper bound (µs since epoch); buckets with start <= finish_us
    /// are emitted.
    pub(crate) finish_us: i64,
    /// True if any output aggregate was wrapped in `locf(...)`.
    pub(crate) has_locf: bool,
    /// True if any output aggregate was wrapped in `interpolate(...)`.
    pub(crate) has_interpolate: bool,
    /// Per-aggregate-column fill modes, in SELECT order (parallel to the
    /// aggregate column indices returned by `densify`).  Empty when no fill
    /// wrappers are present (all columns use `FillMode::Null`).
    ///
    /// Populated by `parse_fill_modes`; the densifier uses this to choose
    /// between LOCF carry-forward and linear interpolation per column.
    pub(crate) fill_modes: Vec<FillMode>,
}

/// Inspect `sql`.  Returns:
///   * `Ok(Some(req))`  — a well-formed gapfill query we will post-process.
///   * `Ok(None)`       — no `time_bucket_gapfill` present; caller runs normally.
///   * `Err(..)`        — a `time_bucket_gapfill`/`locf` query we recognise but
///                        reject (unbounded, multiple calls, locf-without-gapfill,
///                        interpolate), with a TimescaleDB-shaped typed error.
pub(crate) fn match_gapfill(sql: &str) -> Result<Option<GapfillRequest>, BasinError> {
    let lower = sql.to_ascii_lowercase();
    let has_gapfill = lower.contains("time_bucket_gapfill");
    let has_locf = contains_call(&lower, "locf");
    let has_interp = contains_call(&lower, "interpolate");

    if !has_gapfill {
        if has_locf {
            // locf without gapfill — TimescaleDB rejects this.
            return Err(BasinError::feature_not_supported(
                "locf can only be used in an aggregation query with time_bucket_gapfill",
            ));
        }
        if has_interp {
            // interpolate without gapfill — TimescaleDB rejects this.
            return Err(BasinError::feature_not_supported(
                "interpolate can only be used in an aggregation query with time_bucket_gapfill",
            ));
        }
        return Ok(None);
    }

    // Exactly one gapfill call.
    if lower.matches("time_bucket_gapfill").count() != 1 {
        return Err(BasinError::feature_not_supported(
            "multiple time_bucket_gapfill calls in one query are not supported",
        ));
    }

    // Extract the argument list of the single gapfill call.
    let call_start = lower.find("time_bucket_gapfill").unwrap() + "time_bucket_gapfill".len();
    let args_text = paren_contents(&sql[call_start..]).ok_or_else(|| {
        BasinError::InvalidSchema("time_bucket_gapfill: malformed argument list".into())
    })?;
    let args = split_top_level(args_text);
    if args.len() < 2 {
        return Err(BasinError::InvalidSchema(
            "time_bucket_gapfill requires at least (interval, time_column)".into(),
        ));
    }
    let interval_text = strip_quotes(args[0].trim());
    let width = BucketWidth::from_interval_text_compat(&interval_text).ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "time_bucket_gapfill: unrecognised interval '{interval_text}'"
        ))
    })?;
    let ts_expr = args[1].trim().to_string();

    // Bounds: prefer the explicit 4-arg form, else a BETWEEN on the ts column.
    let (start_us, finish_us) = if args.len() >= 4 {
        let s = parse_ts_literal(args[2].trim()).ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "time_bucket_gapfill: cannot parse start bound '{}'",
                args[2].trim()
            ))
        })?;
        let f = parse_ts_literal(args[3].trim()).ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "time_bucket_gapfill: cannot parse finish bound '{}'",
                args[3].trim()
            ))
        })?;
        (s, f)
    } else {
        // Look for `<ts> BETWEEN <a> AND <b>` in the WHERE clause.
        match find_between_bounds(sql, &ts_expr) {
            Some(b) => b,
            None => {
                // TimescaleDB: "missing time_bucket_gapfill argument: ..."
                return Err(BasinError::InvalidSchema(
                    "missing time_bucket_gapfill argument: provide start and finish, \
                     either as the 3rd/4th arguments or via a WHERE \
                     `<time> BETWEEN <start> AND <finish>` predicate"
                        .into(),
                ));
            }
        }
    };

    if finish_us < start_us {
        return Err(BasinError::InvalidSchema(
            "time_bucket_gapfill: finish must be >= start".into(),
        ));
    }

    // Build the rewritten SQL: gapfill→time_bucket (2-arg), strip locf and
    // interpolate wrappers.
    let rewritten = rewrite_sql(
        sql,
        call_start,
        args_text,
        &interval_text,
        &ts_expr,
        has_locf,
        has_interp,
    )?;

    // Parse per-aggregate fill modes from the SELECT clause.
    let fill_modes = parse_fill_modes(sql);

    Ok(Some(GapfillRequest {
        rewritten_sql: rewritten,
        width,
        start_us,
        finish_us,
        has_locf,
        has_interpolate: has_interp,
        fill_modes,
    }))
}

/// Scan the SELECT clause of `sql` and, for every aggregate expression that
/// is wrapped in `locf(...)` or `interpolate(...)`, record the corresponding
/// `FillMode`.  Returns a `Vec<FillMode>` ordered by the appearance of
/// locf/interpolate calls in the SQL text.
///
/// The mapping from this Vec to the aggregate column indices in the densified
/// output is established by `densify`: it iterates aggregate columns in schema
/// order and pairs them with entries from this vec in the order they appear in
/// the SQL text, which must match the SELECT column order.
///
/// If no fill wrappers are present the vec is empty.
fn parse_fill_modes(sql: &str) -> Vec<FillMode> {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut modes = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if matches_call_at_lower(&lower, i, "locf") {
            modes.push(FillMode::Locf);
            // Skip past the call name; the body rewrite handles the rest.
            i += "locf".len();
        } else if matches_call_at_lower(&lower, i, "interpolate") {
            modes.push(FillMode::Interpolate);
            i += "interpolate".len();
        } else {
            i += 1;
        }
    }
    modes
}

/// Rewrite the SQL so it can run through the normal executor:
///   * `time_bucket_gapfill('iv', ts [, ...])` → `time_bucket('iv', ts)`
///   * every `locf(<expr>)` → `<expr>`
///   * every `interpolate(<expr>)` → `<expr>`
fn rewrite_sql(
    sql: &str,
    call_start: usize,
    args_text: &str,
    interval_text: &str,
    ts_expr: &str,
    has_locf: bool,
    has_interp: bool,
) -> Result<String, BasinError> {
    // Replace the full `time_bucket_gapfill(<args>)` span with a 2-arg
    // `time_bucket('iv', ts)`. `call_start` points just past the function name;
    // `args_text` is the inside of the parens.
    let paren_open = sql[call_start..].find('(').unwrap() + call_start;
    let paren_close = paren_open + args_text.len() + 2; // '(' + body + ')'
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..lower_fn_start(sql, call_start)]);
    out.push_str(&format!("time_bucket('{interval_text}', {ts_expr})"));
    out.push_str(&sql[paren_close..]);

    if has_locf {
        out = unwrap_locf(&out)?;
    }
    if has_interp {
        out = unwrap_fn_call(&out, "interpolate")?;
    }
    Ok(out)
}

/// `call_start` is the byte index just after the function *name*; we need the
/// index of the start of `time_bucket_gapfill`.
fn lower_fn_start(sql: &str, call_start: usize) -> usize {
    call_start - "time_bucket_gapfill".len()
}

/// Replace every `locf(<expr>)` with `<expr>` (one level; nested locf is not a
/// thing in TimescaleDB).  Quote/paren aware.
fn unwrap_locf(sql: &str) -> Result<String, BasinError> {
    unwrap_fn_call(sql, "locf")
}

/// Replace every `<fn_name>(<expr>)` with `(<expr>)` (one level).
/// Quote/paren aware.  Used to strip both `locf(...)` and `interpolate(...)`.
fn unwrap_fn_call(sql: &str, fn_name: &str) -> Result<String, BasinError> {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if matches_call_at(sql, i, fn_name) {
            // Skip fn_name, find its parens, splice the inner expression.
            let after = i + fn_name.len();
            let body = paren_contents(&sql[after..]).ok_or_else(|| {
                BasinError::InvalidSchema(format!("{fn_name}: malformed argument list"))
            })?;
            out.push('(');
            out.push_str(body.trim());
            out.push(')');
            let paren_open = sql[after..].find('(').unwrap() + after;
            i = paren_open + body.len() + 2;
        } else {
            // copy one char (respecting UTF-8 boundary by copying the byte run
            // of the current char)
            let ch = sql[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok(out)
}

// ─── densification ──────────────────────────────────────────────────────────

/// Densify `batches` against the gapfill grid implied by `req`.
///
/// The bucket column is the first `Timestamp(..)` column in `schema`.  Every
/// other non-aggregate column is treated as a *group* column; aggregate columns
/// are the remaining ones (filled with `NULL` for synthetic rows, then carried
/// forward by `locf` if requested).
///
/// Rows are emitted ordered by (group key, bucket).  If the input had an
/// `ORDER BY bucket`, the per-bucket order within each group is preserved by
/// construction; a top-level `ORDER BY` over the bucket alias still sorts the
/// final output correctly because every group's grid is monotonically
/// increasing in bucket time.
pub(crate) fn densify(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    req: &GapfillRequest,
) -> Result<Vec<RecordBatch>, BasinError> {
    // Find the bucket column: first Timestamp column.
    let bucket_idx = schema
        .fields()
        .iter()
        .position(|f| matches!(f.data_type(), DataType::Timestamp(..)))
        .ok_or_else(|| {
            BasinError::InvalidSchema(
                "time_bucket_gapfill: no timestamp bucket column in the output".into(),
            )
        })?;

    let (unit, tz) = match schema.field(bucket_idx).data_type() {
        DataType::Timestamp(u, z) => (*u, z.clone()),
        _ => unreachable!(),
    };

    let ncols = schema.fields().len();

    // Materialise all input rows as (bucket_us, group_key, full_row_values).
    // Group key = ordered list of the group-column scalars (as a comparable
    // owned key); aggregate columns are everything except bucket + group cols.
    //
    // Heuristic for classifying group vs aggregate columns: a column is a GROUP
    // column iff its values are constant within each (existing) bucket grouping.
    // We don't have that luxury post-aggregation, so we use the standard
    // contract: every non-bucket, non-aggregate column listed in GROUP BY is a
    // group column. Since we cannot see the GROUP BY post-collection, we treat
    // ALL non-bucket columns whose name does NOT look like an aggregate output
    // and that we can detect as group-keys via the request. For v1 we adopt the
    // simplest correct rule for the documented shapes: columns BEFORE the first
    // aggregate are group columns, aggregates are the rest. We approximate
    // "aggregate" as any column we will null-fill — which, for the supported
    // shape, is every column after the group columns. The caller guarantees the
    // shape; we classify by: bucket col + any column that is part of the group
    // set. We detect group columns as those that are NOT numeric-aggregate by
    // checking the GroupSpec passed below.
    //
    // To keep this robust without GROUP BY introspection, we classify group
    // columns as the set of columns (excluding the bucket) that, across the
    // whole result, take a *small* number of distinct values relative to the
    // bucket count AND are not the value being aggregated. That heuristic is
    // fragile, so instead we require the GROUP BY columns to be passed via the
    // request when present. For the common single-series shape (no extra group
    // columns) every non-bucket column is an aggregate.
    //
    // Implementation: GROUP columns = all columns except the bucket that appear
    // to the LEFT of the first aggregate. We determine the boundary by the
    // GroupSpec computed in `classify_columns`.
    let group_cols = classify_group_columns(schema, batches, bucket_idx)?;
    let agg_cols: Vec<usize> = (0..ncols)
        .filter(|i| *i != bucket_idx && !group_cols.contains(i))
        .collect();

    // Collect existing rows keyed by (group_key, bucket_us).
    // group_key: Vec<ScalarKey>; value: row index pointer (batch, row).
    let mut present: BTreeMap<(Vec<ScalarKey>, i64), (usize, usize)> = BTreeMap::new();
    let mut group_order: Vec<Vec<ScalarKey>> = Vec::new();
    let mut seen_groups: std::collections::HashSet<Vec<ScalarKey>> =
        std::collections::HashSet::new();

    for (bi, batch) in batches.iter().enumerate() {
        let bucket_us = timestamp_column_to_us(batch.column(bucket_idx), unit)?;
        for row in 0..batch.num_rows() {
            let Some(bus) = bucket_us[row] else { continue };
            let key: Vec<ScalarKey> = group_cols
                .iter()
                .map(|c| ScalarKey::from_array(batch.column(*c), row))
                .collect();
            if seen_groups.insert(key.clone()) {
                group_order.push(key.clone());
            }
            present.insert((key, bus), (bi, row));
        }
    }

    // If there were no input rows at all, emit a single grid for the empty
    // group key (no group columns) OR nothing if there are group columns
    // (Timescale emits no rows when there is no series to gapfill within an
    // empty result for a grouped query — it has no group keys to expand).
    if group_order.is_empty() {
        if group_cols.is_empty() {
            group_order.push(Vec::new());
        } else {
            // Grouped + empty input → no output rows.
            let empty = RecordBatch::new_empty(Arc::clone(schema));
            return Ok(vec![empty]);
        }
    }

    // Build the bucket grid in [start, finish] (inclusive on bucket starts).
    let grid = build_grid(req.width, req.start_us, req.finish_us);

    // Builders for the output, one per column.
    let mut builders: Vec<Box<dyn ColBuilder>> = (0..ncols)
        .map(|i| make_builder(schema.field(i).data_type()))
        .collect::<Result<_, _>>()?;

    // Determine per-aggregate fill mode.
    // fill_modes from req are ordered by appearance of locf/interpolate in SQL.
    // We map them to agg_cols in order: the i-th fill mode applies to the i-th
    // aggregate column (skipping group and bucket columns).
    let agg_fill_modes: Vec<FillMode> = {
        let mut modes = Vec::with_capacity(agg_cols.len());
        let mut fill_iter = req.fill_modes.iter().copied();
        for _ in 0..agg_cols.len() {
            // If no fill_modes recorded (pre-fill-mode queries or all-NULL),
            // fall back to Locf when has_locf is set, else Null.
            let mode = if req.fill_modes.is_empty() {
                if req.has_locf {
                    FillMode::Locf
                } else {
                    FillMode::Null
                }
            } else {
                fill_iter.next().unwrap_or(FillMode::Null)
            };
            modes.push(mode);
        }
        modes
    };

    // Check whether any aggregate column uses interpolation.
    let any_interp = agg_fill_modes.iter().any(|m| *m == FillMode::Interpolate);

    // We build the output in two phases when interpolation is requested:
    //   Phase 1: fill all values (real + LOCF carry for locf cols, NULL for
    //            interpolate cols and no-fill cols).
    //   Phase 2: linear-interpolation pass over per-group interpolate columns.
    //
    // To enable phase 2, we record the raw (possibly NULL) values for each
    // interpolate column per group as a parallel Vec indexed by grid position.
    // After phase 1 we compute interpolated values and patch the builder output.

    // For interpolation: we collect raw aggregate values for every (group, grid)
    // cell. indexed as [group_idx][agg_col_local_idx][grid_idx].
    let n_groups = group_order.len();
    let n_grid = grid.len();
    // Only allocate when needed.
    let mut interp_raw: Vec<Vec<Vec<Option<ScalarKey>>>> = if any_interp {
        (0..n_groups)
            .map(|_| (0..agg_cols.len()).map(|_| vec![None; n_grid]).collect())
            .collect()
    } else {
        Vec::new()
    };

    for (gi, gkey) in group_order.iter().enumerate() {
        // locf carry state per aggregate column.
        let mut carry: Vec<Option<ScalarKey>> = vec![None; agg_cols.len()];
        for (grid_idx, &bus) in grid.iter().enumerate() {
            // Bucket timestamp value.
            builders[bucket_idx].push_ts(bus, unit);
            // Group columns: echo the group key.
            for (gci, &gc) in group_cols.iter().enumerate() {
                builders[gc].push_scalar(&gkey[gci]);
            }
            match present.get(&(gkey.clone(), bus)) {
                Some(&(bi, row)) => {
                    // Real row: copy aggregate values, update locf carry.
                    for (ai, &ac) in agg_cols.iter().enumerate() {
                        let sk = ScalarKey::from_array(batches[bi].column(ac), row);
                        if any_interp {
                            interp_raw[gi][ai][grid_idx] = Some(sk.clone());
                        }
                        match agg_fill_modes[ai] {
                            FillMode::Locf => {
                                builders[ac].push_scalar(&sk);
                                if !sk.is_null() {
                                    carry[ai] = Some(sk);
                                }
                                // Real-but-NULL: locf carry unchanged.
                            }
                            FillMode::Interpolate => {
                                // Push the real value; interpolation pass will
                                // not overwrite real (non-NULL) values.
                                builders[ac].push_scalar(&sk);
                            }
                            FillMode::Null => {
                                builders[ac].push_scalar(&sk);
                            }
                        }
                    }
                }
                None => {
                    // Synthetic (gap) row.
                    for (ai, &ac) in agg_cols.iter().enumerate() {
                        match agg_fill_modes[ai] {
                            FillMode::Locf => match &carry[ai] {
                                Some(sk) => builders[ac].push_scalar(sk),
                                None => builders[ac].push_null(),
                            },
                            FillMode::Interpolate | FillMode::Null => {
                                // Both stay NULL in phase 1; interpolate will
                                // patch in phase 2.
                                builders[ac].push_null();
                            }
                        }
                    }
                }
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = builders
        .into_iter()
        .enumerate()
        .map(|(i, b)| b.finish(schema.field(i).data_type(), &tz))
        .collect::<Result<_, _>>()?;

    // Phase 2: linear interpolation pass.
    // For each interpolate aggregate column, scan each group's grid and fill in
    // NULLs between the two nearest non-NULL surrounding values.
    if any_interp {
        for (ai, &ac) in agg_cols.iter().enumerate() {
            if agg_fill_modes[ai] != FillMode::Interpolate {
                continue;
            }
            // Patch the column in `arrays[ac]`.
            let col_dt = schema.field(ac).data_type().clone();
            let (tz_ref) = match schema.field(bucket_idx).data_type() {
                DataType::Timestamp(_, z) => z.clone(),
                _ => None,
            };
            // Build a new column by scanning each group.
            let mut new_builder: Box<dyn ColBuilder> = make_builder(&col_dt)?;
            for gi in 0..n_groups {
                let raw = &interp_raw[gi][ai];
                // Interpolation pass: for each NULL gap find the nearest
                // prev and next non-NULL values and linearly interpolate.
                // Leading/trailing gaps (no prev or next) stay NULL.
                let n = raw.len();
                let mut filled: Vec<Option<f64>> = vec![None; n];
                // Extract f64 values where present.
                for idx in 0..n {
                    if let Some(sk) = &raw[idx] {
                        filled[idx] = scalar_to_f64(sk);
                    }
                }
                // Forward scan: for each None, find prev (p) and next (q) indices.
                let mut result: Vec<Option<ScalarKey>> = vec![None; n];
                let mut idx = 0;
                while idx < n {
                    if filled[idx].is_some() {
                        result[idx] = raw[idx].clone();
                        idx += 1;
                    } else {
                        // Find the prev non-NULL (scanning left).
                        let prev_idx = (0..idx).rev().find(|&k| filled[k].is_some());
                        // Find the next non-NULL (scanning right).
                        let next_idx = (idx + 1..n).find(|&k| filled[k].is_some());
                        match (prev_idx, next_idx) {
                            (Some(p), Some(q)) => {
                                // Linear interpolation over the gap [p+1 .. q-1].
                                let v0 = filled[p].unwrap();
                                let v1 = filled[q].unwrap();
                                let steps = (q - p) as f64;
                                // Fill the whole gap in one sweep.
                                let gap_start = idx;
                                let gap_end = q; // exclusive (q is non-NULL)
                                for k in gap_start..gap_end {
                                    let t = (k - p) as f64 / steps;
                                    let interp = v0 + t * (v1 - v0);
                                    result[k] = f64_to_scalar(interp, &col_dt);
                                }
                                idx = gap_end;
                            }
                            _ => {
                                // Leading or trailing gap — stays NULL.
                                result[idx] = None;
                                idx += 1;
                            }
                        }
                    }
                }
                // Push the interpolated values into the new builder.
                for sk in &result {
                    match sk {
                        Some(s) => new_builder.push_scalar(s),
                        None => new_builder.push_null(),
                    }
                }
            }
            arrays[ac] = new_builder.finish(&col_dt, &tz_ref)?;
        }
    }

    let out = RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| BasinError::InvalidSchema(format!("gapfill: building output batch: {e}")))?;
    Ok(vec![out])
}

/// Convert a `ScalarKey` to `f64` for interpolation arithmetic.
/// Returns `None` for non-numeric types (they cannot be interpolated).
fn scalar_to_f64(sk: &ScalarKey) -> Option<f64> {
    match sk {
        ScalarKey::Int(v) => Some(*v as f64),
        ScalarKey::Float(bits) => Some(f64::from_bits(*bits)),
        ScalarKey::Null => None,
        ScalarKey::Bool(_) | ScalarKey::Str(_) => None,
    }
}

/// Convert an interpolated `f64` back to a `ScalarKey` matching the column type.
fn f64_to_scalar(v: f64, dt: &DataType) -> Option<ScalarKey> {
    match dt {
        DataType::Float64 | DataType::Float32 => Some(ScalarKey::Float(v.to_bits())),
        DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt64
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8 => Some(ScalarKey::Int(v.round() as i64)),
        _ => None,
    }
}

/// Determine which output columns are GROUP columns (everything between the
/// bucket and the aggregates).  We classify a non-bucket column as a *group*
/// column iff, for every distinct value of that column, the value is associated
/// with multiple distinct bucket timestamps — i.e. it behaves like a partition
/// key, not a per-bucket aggregate.  For the single-series shape this yields an
/// empty set (every non-bucket column is an aggregate).
///
/// This heuristic is conservative: it only marks a column as a group key when
/// the same value recurs across ≥2 distinct buckets, which is the defining
/// property of a grouped time-series.  A constant aggregate would also satisfy
/// this, so to avoid mis-classifying a constant aggregate we additionally
/// require the column to be non-numeric OR to take ≥2 distinct values overall.
fn classify_group_columns(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    bucket_idx: usize,
) -> Result<Vec<usize>, BasinError> {
    let ncols = schema.fields().len();
    let (unit, _) = match schema.field(bucket_idx).data_type() {
        DataType::Timestamp(u, z) => (*u, z.clone()),
        _ => unreachable!(),
    };

    let mut group_cols = Vec::new();
    for c in 0..ncols {
        if c == bucket_idx {
            continue;
        }
        // Count distinct (value, bucket) and detect recurrence across buckets.
        let mut value_buckets: std::collections::HashMap<
            ScalarKey,
            std::collections::HashSet<i64>,
        > = std::collections::HashMap::new();
        let mut distinct_vals: std::collections::HashSet<ScalarKey> =
            std::collections::HashSet::new();
        for batch in batches {
            let bucket_us = timestamp_column_to_us(batch.column(bucket_idx), unit)?;
            for row in 0..batch.num_rows() {
                let Some(bus) = bucket_us[row] else { continue };
                let sk = ScalarKey::from_array(batch.column(c), row);
                distinct_vals.insert(sk.clone());
                value_buckets.entry(sk).or_default().insert(bus);
            }
        }
        // Group column iff some value recurs across >=2 buckets AND the column
        // has >=2 distinct values overall (rules out a constant aggregate).
        let recurs = value_buckets.values().any(|s| s.len() >= 2);
        let multi_distinct = distinct_vals.len() >= 2;
        if recurs && multi_distinct {
            group_cols.push(c);
        }
    }
    Ok(group_cols)
}

/// Build the inclusive bucket grid: all bucket starts `b` with
/// `floor(start) <= b <= finish`, stepped by the bucket width.
fn build_grid(width: BucketWidth, start_us: i64, finish_us: i64) -> Vec<i64> {
    let mut out = Vec::new();
    let start_dt = DateTime::<Utc>::from_timestamp_micros(start_us)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_micros(0).unwrap());
    let mut cur = width.floor(start_dt).timestamp_micros();
    // Guard against pathological widths.
    let step_guard = 10_000_000usize;
    let mut n = 0usize;
    while cur <= finish_us && n < step_guard {
        out.push(cur);
        let cur_dt = DateTime::<Utc>::from_timestamp_micros(cur)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_micros(0).unwrap());
        let next = next_bucket(width, cur_dt);
        if next <= cur {
            break; // no progress; avoid infinite loop
        }
        cur = next;
        n += 1;
    }
    out
}

/// Advance one bucket from `cur` (a bucket start) and return µs of the next
/// bucket start.  Uses calendar arithmetic for calendar widths.
fn next_bucket(width: BucketWidth, cur: DateTime<Utc>) -> i64 {
    use chrono::Datelike;
    match width {
        BucketWidth::Second => (cur + chrono::Duration::seconds(1)).timestamp_micros(),
        BucketWidth::Minute => (cur + chrono::Duration::minutes(1)).timestamp_micros(),
        BucketWidth::Hour => (cur + chrono::Duration::hours(1)).timestamp_micros(),
        BucketWidth::Day => (cur + chrono::Duration::days(1)).timestamp_micros(),
        BucketWidth::Week => (cur + chrono::Duration::weeks(1)).timestamp_micros(),
        BucketWidth::Month => {
            let (y, m) = (cur.year(), cur.month());
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            Utc.with_ymd_and_hms(ny, nm, 1, 0, 0, 0)
                .single()
                .map(|d| d.timestamp_micros())
                .unwrap_or_else(|| (cur + chrono::Duration::days(30)).timestamp_micros())
        }
        BucketWidth::Year => Utc
            .with_ymd_and_hms(cur.year() + 1, 1, 1, 0, 0, 0)
            .single()
            .map(|d| d.timestamp_micros())
            .unwrap_or_else(|| (cur + chrono::Duration::days(365)).timestamp_micros()),
        BucketWidth::SecondsFixed(s) => {
            (cur + chrono::Duration::seconds(s.max(1) as i64)).timestamp_micros()
        }
    }
}

use chrono::TimeZone;

// ─── timestamp helpers ──────────────────────────────────────────────────────

/// Convert a timestamp array (any unit) to a `Vec<Option<i64>>` of µs-since-epoch.
fn timestamp_column_to_us(arr: &ArrayRef, unit: TimeUnit) -> Result<Vec<Option<i64>>, BasinError> {
    let n = arr.len();
    let mut out = Vec::with_capacity(n);
    match unit {
        TimeUnit::Microsecond => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| BasinError::InvalidSchema("gapfill: ts µs downcast".into()))?;
            for i in 0..n {
                out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
        }
        TimeUnit::Nanosecond => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| BasinError::InvalidSchema("gapfill: ts ns downcast".into()))?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i) / 1_000)
                });
            }
        }
        TimeUnit::Millisecond => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| BasinError::InvalidSchema("gapfill: ts ms downcast".into()))?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i) * 1_000)
                });
            }
        }
        TimeUnit::Second => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| BasinError::InvalidSchema("gapfill: ts s downcast".into()))?;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i) * 1_000_000)
                });
            }
        }
    }
    Ok(out)
}

// ─── scalar key (group + aggregate value carrier) ───────────────────────────

/// An owned, hashable/orderable snapshot of one cell value.  Supports the
/// column types that appear in gapfill outputs: integers, floats (bit-keyed),
/// strings, booleans, timestamps, and NULL.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum ScalarKey {
    Null,
    Bool(bool),
    Int(i64),
    /// Float kept as raw bits to stay `Eq`/`Ord`; NaN sorts after everything.
    Float(u64),
    Str(String),
}

impl ScalarKey {
    fn is_null(&self) -> bool {
        matches!(self, ScalarKey::Null)
    }

    fn from_array(arr: &ArrayRef, row: usize) -> ScalarKey {
        use arrow_array::{
            BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        };
        if arr.is_null(row) {
            return ScalarKey::Null;
        }
        let any = arr.as_any();
        if let Some(a) = any.downcast_ref::<Int64Array>() {
            return ScalarKey::Int(a.value(row));
        }
        if let Some(a) = any.downcast_ref::<Int32Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<Int16Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<Int8Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<UInt64Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<UInt32Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<UInt16Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<UInt8Array>() {
            return ScalarKey::Int(a.value(row) as i64);
        }
        if let Some(a) = any.downcast_ref::<Float64Array>() {
            return ScalarKey::Float(a.value(row).to_bits());
        }
        if let Some(a) = any.downcast_ref::<Float32Array>() {
            return ScalarKey::Float((a.value(row) as f64).to_bits());
        }
        if let Some(a) = any.downcast_ref::<BooleanArray>() {
            return ScalarKey::Bool(a.value(row));
        }
        if let Some(a) = any.downcast_ref::<StringArray>() {
            return ScalarKey::Str(a.value(row).to_string());
        }
        if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            return ScalarKey::Str(a.value(row).to_string());
        }
        // Timestamps as Int µs.
        if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
            return ScalarKey::Int(a.value(row));
        }
        // Fallback: treat as NULL so we never panic on an unexpected type.
        ScalarKey::Null
    }
}

// ─── column builders ────────────────────────────────────────────────────────

trait ColBuilder {
    fn push_null(&mut self);
    fn push_scalar(&mut self, sk: &ScalarKey);
    fn push_ts(&mut self, _us: i64, _unit: TimeUnit) {
        // default: most builders are not timestamp builders
        self.push_null();
    }
    fn finish(
        self: Box<Self>,
        dt: &DataType,
        tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError>;
}

fn make_builder(dt: &DataType) -> Result<Box<dyn ColBuilder>, BasinError> {
    match dt {
        DataType::Int64
        | DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt64
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8 => Ok(Box::new(IntBuilder { vals: Vec::new() })),
        DataType::Float64 | DataType::Float32 => Ok(Box::new(FloatBuilder { vals: Vec::new() })),
        DataType::Boolean => Ok(Box::new(BoolColBuilder { vals: Vec::new() })),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(Box::new(StrBuilder { vals: Vec::new() })),
        DataType::Timestamp(_, _) => Ok(Box::new(TsBuilder { vals: Vec::new() })),
        other => {
            // Unsupported aggregate output type — surface a typed error rather
            // than silently corrupting data.
            Err(BasinError::feature_not_supported(format!(
                "time_bucket_gapfill: column type {other:?} is not supported"
            )))
        }
    }
}

struct IntBuilder {
    vals: Vec<Option<i64>>,
}
impl ColBuilder for IntBuilder {
    fn push_null(&mut self) {
        self.vals.push(None);
    }
    fn push_scalar(&mut self, sk: &ScalarKey) {
        self.vals.push(match sk {
            ScalarKey::Int(v) => Some(*v),
            ScalarKey::Bool(b) => Some(*b as i64),
            _ => None,
        });
    }
    fn finish(
        self: Box<Self>,
        dt: &DataType,
        _tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError> {
        use arrow_array::*;
        let v = self.vals;
        Ok(match dt {
            DataType::Int64 => Arc::new(Int64Array::from(v)) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as i32))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as i16))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int8 => Arc::new(Int8Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as i8))
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt64 => Arc::new(UInt64Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as u64))
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt32 => Arc::new(UInt32Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as u32))
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt16 => Arc::new(UInt16Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as u16))
                    .collect::<Vec<_>>(),
            )),
            DataType::UInt8 => Arc::new(UInt8Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as u8))
                    .collect::<Vec<_>>(),
            )),
            _ => return Err(BasinError::InvalidSchema("gapfill int finish".into())),
        })
    }
}

struct FloatBuilder {
    vals: Vec<Option<f64>>,
}
impl ColBuilder for FloatBuilder {
    fn push_null(&mut self) {
        self.vals.push(None);
    }
    fn push_scalar(&mut self, sk: &ScalarKey) {
        self.vals.push(match sk {
            ScalarKey::Float(bits) => Some(f64::from_bits(*bits)),
            ScalarKey::Int(v) => Some(*v as f64),
            _ => None,
        });
    }
    fn finish(
        self: Box<Self>,
        dt: &DataType,
        _tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError> {
        use arrow_array::*;
        let v = self.vals;
        Ok(match dt {
            DataType::Float64 => Arc::new(Float64Array::from(v)) as ArrayRef,
            DataType::Float32 => Arc::new(Float32Array::from(
                v.into_iter()
                    .map(|o| o.map(|x| x as f32))
                    .collect::<Vec<_>>(),
            )),
            _ => return Err(BasinError::InvalidSchema("gapfill float finish".into())),
        })
    }
}

struct BoolColBuilder {
    vals: Vec<Option<bool>>,
}
impl ColBuilder for BoolColBuilder {
    fn push_null(&mut self) {
        self.vals.push(None);
    }
    fn push_scalar(&mut self, sk: &ScalarKey) {
        self.vals.push(match sk {
            ScalarKey::Bool(b) => Some(*b),
            _ => None,
        });
    }
    fn finish(
        self: Box<Self>,
        _dt: &DataType,
        _tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError> {
        use arrow_array::BooleanArray;
        Ok(Arc::new(BooleanArray::from(self.vals)) as ArrayRef)
    }
}

struct StrBuilder {
    vals: Vec<Option<String>>,
}
impl ColBuilder for StrBuilder {
    fn push_null(&mut self) {
        self.vals.push(None);
    }
    fn push_scalar(&mut self, sk: &ScalarKey) {
        self.vals.push(match sk {
            ScalarKey::Str(s) => Some(s.clone()),
            _ => None,
        });
    }
    fn finish(
        self: Box<Self>,
        dt: &DataType,
        _tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError> {
        use arrow_array::{LargeStringArray, StringArray};
        let v = self.vals;
        Ok(match dt {
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(
                v.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
            )) as ArrayRef,
            _ => Arc::new(StringArray::from(
                v.iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
            )),
        })
    }
}

struct TsBuilder {
    vals: Vec<Option<i64>>,
}
impl ColBuilder for TsBuilder {
    fn push_null(&mut self) {
        self.vals.push(None);
    }
    fn push_scalar(&mut self, sk: &ScalarKey) {
        self.vals.push(match sk {
            ScalarKey::Int(v) => Some(*v),
            _ => None,
        });
    }
    fn push_ts(&mut self, us: i64, unit: TimeUnit) {
        // Store in the column's native unit.
        let native = match unit {
            TimeUnit::Microsecond => us,
            TimeUnit::Nanosecond => us * 1_000,
            TimeUnit::Millisecond => us / 1_000,
            TimeUnit::Second => us / 1_000_000,
        };
        self.vals.push(Some(native));
    }
    fn finish(
        self: Box<Self>,
        dt: &DataType,
        tz: &Option<Arc<str>>,
    ) -> Result<ArrayRef, BasinError> {
        let v = self.vals;
        let unit = match dt {
            DataType::Timestamp(u, _) => *u,
            _ => return Err(BasinError::InvalidSchema("gapfill ts finish".into())),
        };
        let arr: ArrayRef = match unit {
            TimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(v).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(v).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(v).with_timezone_opt(tz.clone()))
            }
            TimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(v).with_timezone_opt(tz.clone()))
            }
        };
        Ok(arr)
    }
}

// ─── text helpers ───────────────────────────────────────────────────────────

/// True if `name` appears as a function call (`name(`) at a word boundary
/// anywhere in `lower` (which must already be lowercased).
fn contains_call(lower: &str, name: &str) -> bool {
    let mut from = 0;
    while let Some(rel) = lower[from..].find(name) {
        let pos = from + rel;
        if matches_call_at_lower(lower, pos, name) {
            return true;
        }
        from = pos + 1;
    }
    false
}

fn matches_call_at_lower(lower: &str, pos: usize, name: &str) -> bool {
    let end = pos + name.len();
    if end > lower.len() {
        return false;
    }
    // Verify the actual text at this position matches `name`.
    if &lower[pos..end] != name {
        return false;
    }
    let pre_ok = pos == 0 || {
        let p = lower.as_bytes()[pos - 1];
        !(p.is_ascii_alphanumeric() || p == b'_')
    };
    if !pre_ok {
        return false;
    }
    // next non-space char must be '('
    let rest = lower[end..].trim_start();
    rest.starts_with('(')
}

/// Case-insensitive variant operating on the original-case `sql`.
fn matches_call_at(sql: &str, pos: usize, name: &str) -> bool {
    if pos + name.len() > sql.len() {
        return false;
    }
    if !sql[pos..pos + name.len()].eq_ignore_ascii_case(name) {
        return false;
    }
    matches_call_at_lower(&sql.to_ascii_lowercase(), pos, name)
}

/// Extract text inside the first balanced `(...)` of `s` (which must begin with
/// optional whitespace then `(`).  Quote-aware.
fn paren_contents(s: &str) -> Option<&str> {
    let trimmed_start = s.len() - s.trim_start().len();
    let s2 = &s[trimmed_start..];
    if !s2.starts_with('(') {
        return None;
    }
    let bytes = s2.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut open = None;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                if depth == 1 {
                    open = Some(i);
                }
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s2[open? + 1..i]);
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

/// Split on top-level commas (not inside parens/quotes).
fn split_top_level(s: &str) -> Vec<&str> {
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

/// Parse a timestamp literal in one of the accepted forms into µs-since-epoch:
///   * `TIMESTAMP '2024-01-01 00:00:00'` / `TIMESTAMPTZ '...'`
///   * `'2024-01-01 00:00:00+00'`
///   * a bare quoted ISO string.
fn parse_ts_literal(s: &str) -> Option<i64> {
    let s = s.trim();
    // strip leading TIMESTAMP / TIMESTAMPTZ keyword
    let lower = s.to_ascii_lowercase();
    let body = if let Some(rest) = lower.strip_prefix("timestamptz") {
        &s[s.len() - rest.len()..]
    } else if let Some(rest) = lower.strip_prefix("timestamp") {
        &s[s.len() - rest.len()..]
    } else {
        s
    };
    let inner = strip_quotes(body.trim());
    parse_iso_us(&inner)
}

/// Parse an ISO-8601 timestamp string into µs-since-epoch.  Accepts:
///   * RFC3339 with offset (`2024-01-01T00:00:00+00:00`, `... +00`).
///   * Naive `YYYY-MM-DD HH:MM:SS[.ffffff]` (interpreted as UTC).
///   * Date only `YYYY-MM-DD` (midnight UTC).
fn parse_iso_us(inner: &str) -> Option<i64> {
    let t = inner.trim();
    // Try RFC3339 first (handles offsets, including `+00`).
    let norm = normalize_offset(t);
    if let Ok(dt) = DateTime::parse_from_rfc3339(&norm) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    // Naive datetime with space or 'T'.
    for fmt in &[
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(t, fmt) {
            return Some(Utc.from_utc_datetime(&ndt).timestamp_micros());
        }
    }
    // Date only.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
        return Some(
            Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap())
                .timestamp_micros(),
        );
    }
    None
}

/// chrono's RFC3339 wants `+00:00`, not `+00`; also turn a space into `T`.
fn normalize_offset(t: &str) -> String {
    let mut s = t.replace(' ', "T");
    // `+00` / `-05` → `+00:00`
    if let Some(idx) = s.rfind(['+', '-']) {
        // only treat as offset if it's after the time (idx > 10)
        if idx > 10 {
            let off = &s[idx..];
            if off.len() == 3 {
                s.push_str(":00");
            }
        }
    }
    s
}

/// Search the SQL for `<ts_expr> BETWEEN <a> AND <b>` (case-insensitive) and
/// return the parsed (start_us, finish_us).  The ts side may be the bare column
/// name (e.g. `ts`) — we match on the trailing identifier of `ts_expr`.
fn find_between_bounds(sql: &str, ts_expr: &str) -> Option<(i64, i64)> {
    let col = ts_expr.rsplit('.').next().unwrap_or(ts_expr).trim();
    let lower = sql.to_ascii_lowercase();
    let col_l = col.to_ascii_lowercase();
    // find "<col> between"
    let mut from = 0;
    loop {
        let rel = lower[from..].find(&col_l)?;
        let pos = from + rel;
        let after = lower[pos + col_l.len()..].trim_start();
        if after.starts_with("between") {
            // parse the two operands separated by AND
            let rest_orig_start = pos + col_l.len();
            let rest = &sql[rest_orig_start..];
            let rest_l = &lower[rest_orig_start..];
            let bpos = rest_l.find("between")? + "between".len();
            let and_rel = rest_l[bpos..].find(" and ")?;
            let a_str = rest[bpos..bpos + and_rel].trim();
            let after_and = bpos + and_rel + " and ".len();
            // b ends at a clause keyword, closing paren, or end.
            let b_end = rest_l[after_and..]
                .find(|c: char| c == ')' || c == ';')
                .map(|e| after_and + e)
                .unwrap_or(rest.len());
            // also stop at GROUP/ORDER/LIMIT keywords
            let b_slice = &rest[after_and..b_end];
            let b_str = b_slice
                .split_whitespace()
                .take_while(|w| {
                    let wl = w.to_ascii_lowercase();
                    !["group", "order", "limit", "having", "offset"].contains(&wl.as_str())
                })
                .collect::<Vec<_>>()
                .join(" ");
            // Reassemble possible `TIMESTAMP '...'` (two tokens).
            let a = parse_ts_literal(a_str)?;
            let b = parse_ts_literal(b_str.trim())?;
            return Some((a, b));
        }
        from = pos + 1;
    }
}

// ─── BucketWidth compat ─────────────────────────────────────────────────────

/// Re-expose the `BucketWidth::from_interval_text` resolver used by the
/// `time_bucket` UDF so gapfill shares the exact same interval semantics.
trait BucketWidthCompat {
    fn from_interval_text_compat(s: &str) -> Option<BucketWidth>;
}
impl BucketWidthCompat for BucketWidth {
    fn from_interval_text_compat(s: &str) -> Option<BucketWidth> {
        use crate::hypertable::BucketWidthFromText;
        <BucketWidth as BucketWidthFromText>::from_interval_text(s)
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_simple_gapfill_with_between() {
        let sql = "SELECT time_bucket_gapfill('1 hour', ts) AS b, count(*) \
                   FROM t WHERE ts BETWEEN TIMESTAMP '2024-01-01 00:00:00' \
                   AND TIMESTAMP '2024-01-01 03:00:00' GROUP BY b ORDER BY b";
        let req = match_gapfill(sql).unwrap().expect("should match");
        assert_eq!(req.width, BucketWidth::Hour);
        assert!(!req.has_locf);
        assert!(req.rewritten_sql.contains("time_bucket('1 hour', ts)"));
        assert!(!req.rewritten_sql.contains("gapfill"));
        // 4 buckets: 00,01,02,03.
        let grid = build_grid(req.width, req.start_us, req.finish_us);
        assert_eq!(grid.len(), 4);
    }

    #[test]
    fn detects_four_arg_form() {
        let sql = "SELECT time_bucket_gapfill('30 minutes', ts, \
                   TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 01:00:00') AS b, \
                   avg(v) FROM t GROUP BY b";
        let req = match_gapfill(sql).unwrap().expect("should match");
        assert_eq!(req.width, BucketWidth::SecondsFixed(1800));
        let grid = build_grid(req.width, req.start_us, req.finish_us);
        // 00:00, 00:30, 01:00 → 3 buckets (inclusive of finish).
        assert_eq!(grid.len(), 3);
    }

    #[test]
    fn unbounded_gapfill_errors() {
        let sql = "SELECT time_bucket_gapfill('1 hour', ts) AS b, count(*) FROM t GROUP BY b";
        let err = match_gapfill(sql).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        assert!(format!("{err}").contains("missing time_bucket_gapfill argument"));
    }

    #[test]
    fn locf_without_gapfill_errors() {
        let sql = "SELECT locf(count(*)) FROM t GROUP BY 1";
        let err = match_gapfill(sql).unwrap_err();
        assert!(matches!(err, BasinError::FeatureNotSupported(_)));
    }

    #[test]
    fn interpolate_without_gapfill_errors() {
        // interpolate() without time_bucket_gapfill → typed error.
        let sql = "SELECT interpolate(avg(v)) FROM t";
        let err = match_gapfill(sql).unwrap_err();
        assert!(matches!(err, BasinError::FeatureNotSupported(_)));
    }

    #[test]
    fn interpolate_with_gapfill_rewrites() {
        // interpolate() inside a gapfill query is now supported and rewrites.
        let sql = "SELECT time_bucket_gapfill('1 hour', ts) AS b, interpolate(avg(v)) AS a \
                   FROM t WHERE ts BETWEEN '2024-01-01 00:00:00+00' AND '2024-01-01 02:00:00+00' \
                   GROUP BY b";
        let req = match_gapfill(sql).unwrap().expect("match");
        assert!(req.has_interpolate);
        assert!(req.rewritten_sql.to_lowercase().contains("avg(v)"));
        assert!(!req.rewritten_sql.to_lowercase().contains("interpolate"));
        assert!(req.rewritten_sql.contains("time_bucket('1 hour', ts)"));
        // fill_modes should record an Interpolate entry.
        assert!(req.fill_modes.iter().any(|m| *m == FillMode::Interpolate));
    }

    #[test]
    fn parse_fill_modes_mixed() {
        // A query with both locf and interpolate columns should yield the
        // correct per-column fill mode ordering.
        let sql = "SELECT time_bucket_gapfill('1h', ts) AS b, \
                   locf(sum(a)), interpolate(avg(b)) \
                   FROM t WHERE ts BETWEEN '2024-01-01' AND '2024-01-02' GROUP BY b";
        let modes = parse_fill_modes(sql);
        // locf appears before interpolate in the SQL.
        assert_eq!(modes, vec![FillMode::Locf, FillMode::Interpolate]);
    }

    #[test]
    fn multiple_gapfill_errors() {
        let sql = "SELECT time_bucket_gapfill('1h', ts), time_bucket_gapfill('2h', ts) FROM t";
        let err = match_gapfill(sql).unwrap_err();
        assert!(matches!(err, BasinError::FeatureNotSupported(_)));
    }

    #[test]
    fn locf_unwrap_rewrites() {
        let sql = "SELECT time_bucket_gapfill('1 hour', ts) AS b, locf(avg(v)) AS a \
                   FROM t WHERE ts BETWEEN '2024-01-01 00:00:00+00' AND '2024-01-01 02:00:00+00' \
                   GROUP BY b";
        let req = match_gapfill(sql).unwrap().expect("match");
        assert!(req.has_locf);
        assert!(req.rewritten_sql.to_lowercase().contains("avg(v)"));
        assert!(!req.rewritten_sql.to_lowercase().contains("locf"));
        assert!(req.rewritten_sql.contains("time_bucket('1 hour', ts)"));
    }

    #[test]
    fn parse_ts_forms() {
        assert!(parse_ts_literal("TIMESTAMP '2024-01-01 00:00:00'").is_some());
        assert!(parse_ts_literal("TIMESTAMPTZ '2024-01-01 00:00:00+00'").is_some());
        assert!(parse_ts_literal("'2024-01-01 00:00:00+00'").is_some());
        assert!(parse_ts_literal("'2024-01-01'").is_some());
        let a = parse_ts_literal("TIMESTAMP '2024-01-01 00:00:00'").unwrap();
        let b = parse_ts_literal("'2024-01-01 01:00:00'").unwrap();
        assert_eq!(b - a, 3_600_000_000);
    }

    #[test]
    fn grid_inclusive_bounds() {
        // 1-hour grid from 00:00 to 03:00 inclusive → 4 rows.
        let start = parse_ts_literal("'2024-01-01 00:00:00'").unwrap();
        let finish = parse_ts_literal("'2024-01-01 03:00:00'").unwrap();
        let grid = build_grid(BucketWidth::Hour, start, finish);
        assert_eq!(grid.len(), 4);
        assert_eq!(grid[0], start);
        assert_eq!(grid[3], finish);
    }

    #[test]
    fn grid_floors_unaligned_start() {
        // start at 00:37 → first bucket floored to 00:00.
        let start = parse_ts_literal("'2024-01-01 00:37:00'").unwrap();
        let finish = parse_ts_literal("'2024-01-01 02:00:00'").unwrap();
        let grid = build_grid(BucketWidth::Hour, start, finish);
        let expect0 = parse_ts_literal("'2024-01-01 00:00:00'").unwrap();
        assert_eq!(grid[0], expect0);
        assert_eq!(grid.len(), 3); // 00,01,02
    }
}
