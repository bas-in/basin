//! Time-bucket detection and incremental-refresh query rewriting.
//!
//! ## Watermark / bucket-spanning correctness invariant
//!
//! Incremental refresh keeps the materialised view's state byte-for-byte
//! identical to a single full rebuild *as long as no rows arrive late into
//! a bucket that started before the watermark*. The watermark is the start
//! of the last bucket the previous refresh saw any source rows in. On
//! every tick we re-aggregate every source row whose bucket-column value
//! is `>= watermark`, which:
//!
//! - Re-includes the full content of the prior "last bucket" — so any new
//!   rows that arrived in it since the last refresh are merged in.
//! - Picks up any rows in newer buckets the previous refresh missed.
//!
//! The matview update step is read-modify-write: we delete every row in
//! the current materialisation whose bucket value is `>= watermark`, then
//! append the freshly-aggregated rows for that range. The output is
//! identical to a full rebuild's output for the same source state.
//!
//! ## Detection heuristic
//!
//! [`detect_time_bucket`] looks at the user's SELECT body and tries to
//! recognise the GROUP BY shape:
//!
//! - `date_trunc('<unit>', <col>)` — supported. `<col>` is a bare column
//!   reference; expressions are rejected.
//! - `time_bucket('<interval>', <col>)` — supported.
//! - `CAST(date_trunc(_, col) AS …)` / `CAST(time_bucket(_, col) AS …)` —
//!   supported (the cast is transparent for our purposes).
//! - Anything else (`extract(...)`, arithmetic on the column, multiple
//!   GROUP BY expressions where none is a time-bucket, etc.) — falls back
//!   to full re-execution. The CV is still correct, just not incremental.

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use crate::pg_ast::ObjectNamePartExt;
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem, SetExpr,
    Statement, TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use basin_common::{BasinError, Result};

/// Outcome of inspecting a CV's SELECT body for a time-bucket shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketInfo {
    /// The source-table column the bucket function reads from. The column
    /// name is the unqualified identifier — the source table is implicit
    /// from the FROM clause (CVs reference exactly one source table in
    /// v0.1).
    pub bucket_column: String,
    /// Output column name of the bucket projection. Used to delete prior
    /// rows from the matview where `bucket_alias >= watermark`.
    pub bucket_alias: String,
    /// Bucket width as a chrono `Duration`. Used to align the watermark
    /// to a bucket boundary.
    pub width: BucketWidth,
}

/// Recognised bucket widths. We canonicalise to a small enum so the
/// alignment logic (truncate-to-bucket) is exact for date-based units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketWidth {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
    /// Generic fixed-width interval (in seconds). Used for `time_bucket`
    /// where the user wrote `'5 minutes'` etc.
    SecondsFixed(u64),
}

impl BucketWidth {
    /// Truncate `t` to the start of its bucket. Date-based units (day,
    /// week, month, year) are anchored to the calendar; fixed-width
    /// units are anchored to Unix epoch (matches PostgreSQL's
    /// `date_trunc` and TimescaleDB's `time_bucket` defaults).
    pub fn floor(self, t: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            BucketWidth::Second => Utc
                .with_ymd_and_hms(
                    t.year(),
                    t.month(),
                    t.day(),
                    t.hour(),
                    t.minute(),
                    t.second(),
                )
                .unwrap(),
            BucketWidth::Minute => Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), t.hour(), t.minute(), 0)
                .unwrap(),
            BucketWidth::Hour => Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), t.hour(), 0, 0)
                .unwrap(),
            BucketWidth::Day => Utc
                .with_ymd_and_hms(t.year(), t.month(), t.day(), 0, 0, 0)
                .unwrap(),
            BucketWidth::Week => {
                // PG's date_trunc('week', ts) → Monday 00:00 of t's week.
                let day = t.date_naive();
                let weekday = day.weekday().num_days_from_monday() as i64;
                let monday = day - chrono::Duration::days(weekday);
                Utc.from_utc_datetime(&monday.and_hms_opt(0, 0, 0).unwrap())
            }
            BucketWidth::Month => Utc
                .with_ymd_and_hms(t.year(), t.month(), 1, 0, 0, 0)
                .unwrap(),
            BucketWidth::Year => Utc.with_ymd_and_hms(t.year(), 1, 1, 0, 0, 0).unwrap(),
            BucketWidth::SecondsFixed(secs) => {
                let s = secs.max(1) as i64;
                let epoch = t.timestamp();
                let floored = epoch - epoch.rem_euclid(s);
                DateTime::<Utc>::from_timestamp(floored, 0).unwrap_or(t)
            }
        }
    }
}

/// Inspect `query_sql` and return `Some(BucketInfo)` if the SELECT's
/// GROUP BY references exactly one `date_trunc(_, col)` or
/// `time_bucket(_, col)` expression. Returns `None` for any shape we
/// don't recognise — the refresher then falls back to full re-execution.
pub fn detect_time_bucket(query_sql: &str) -> Option<BucketInfo> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, query_sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        Statement::Query(q) => q.as_ref(),
        _ => return None,
    };
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => return None,
    };
    // The bucket expression we're looking for. Either in GROUP BY directly,
    // or referenced by alias / ordinal.
    let group_exprs: &[Expr] = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => return None,
    };
    if group_exprs.is_empty() {
        return None;
    }
    // Walk every projection item once so we can resolve `GROUP BY 1` /
    // `GROUP BY <alias>` against it.
    let projections: Vec<(Option<String>, &Expr)> = select
        .projection
        .iter()
        .filter_map(|p| match p {
            SelectItem::UnnamedExpr(e) => Some((expr_default_alias(e), e)),
            SelectItem::ExprWithAlias { expr, alias } => Some((Some(alias.value.clone()), expr)),
            _ => None,
        })
        .collect();
    // For each GROUP BY expression, try to resolve to a (alias, bucket-fn-call)
    // pair. The first match wins.
    for ge in group_exprs {
        // GROUP BY 1 / 2 / ...
        if let Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. }) = ge {
            if let Ok(idx) = n.parse::<usize>() {
                if idx > 0 && idx <= projections.len() {
                    let (alias_opt, expr) = &projections[idx - 1];
                    if let Some((col, width)) = match_bucket_call(expr) {
                        return Some(BucketInfo {
                            bucket_column: col,
                            bucket_alias: alias_opt.clone().unwrap_or_else(|| format!("col{idx}")),
                            width,
                        });
                    }
                }
            }
            continue;
        }
        // GROUP BY <alias>: a bare identifier might match a projection alias.
        if let Expr::Identifier(ident) = ge {
            for (alias_opt, expr) in &projections {
                if alias_opt.as_deref() == Some(ident.value.as_str()) {
                    if let Some((col, width)) = match_bucket_call(expr) {
                        return Some(BucketInfo {
                            bucket_column: col,
                            bucket_alias: ident.value.clone(),
                            width,
                        });
                    }
                }
            }
        }
        // GROUP BY date_trunc('hour', ts) — full expression.
        if let Some((col, width)) = match_bucket_call(ge) {
            // Find the alias by matching the projection by structural equality.
            let alias = projections
                .iter()
                .find_map(|(alias_opt, expr)| {
                    if exprs_match(expr, ge) {
                        alias_opt.clone()
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| col.clone());
            return Some(BucketInfo {
                bucket_column: col,
                bucket_alias: alias,
                width,
            });
        }
    }
    None
}

/// Default alias for a projection expression with no explicit `AS`. We
/// only synthesize one when sqlparser doesn't provide it — used for
/// matching `GROUP BY <ordinal>` against a projection.
fn expr_default_alias(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        _ => None,
    }
}

fn exprs_match(a: &Expr, b: &Expr) -> bool {
    // Cheap structural-equality via Display. sqlparser's Display emits the
    // canonical SQL for each expression, which is enough to recognise that
    // two `date_trunc('hour', ts)` references are the same expression even
    // if their internal Span / position fields differ.
    format!("{a}") == format!("{b}")
}

/// Match a bucket-function call. Looks through a leading CAST. Returns
/// `(column_name, width)` on a recognised shape.
fn match_bucket_call(e: &Expr) -> Option<(String, BucketWidth)> {
    let inner = match e {
        Expr::Cast { expr, .. } => expr.as_ref(),
        Expr::Nested(inner) => inner.as_ref(),
        other => other,
    };
    let func = match inner {
        Expr::Function(f) => f,
        _ => return None,
    };
    let name = func
        .name
        .0
        .last()
        .map(|p| p.id_val().to_ascii_lowercase())
        .unwrap_or_default();
    let args: &[FunctionArg] = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => return None,
    };
    if args.len() < 2 {
        return None;
    }
    match name.as_str() {
        "date_trunc" => {
            let unit = literal_string(&args[0])?.to_ascii_lowercase();
            let col = column_arg(&args[1])?;
            let width = match unit.as_str() {
                "second" | "seconds" => BucketWidth::Second,
                "minute" | "minutes" => BucketWidth::Minute,
                "hour" | "hours" => BucketWidth::Hour,
                "day" | "days" => BucketWidth::Day,
                "week" | "weeks" => BucketWidth::Week,
                "month" | "months" => BucketWidth::Month,
                "year" | "years" => BucketWidth::Year,
                _ => return None,
            };
            Some((col, width))
        }
        "time_bucket" => {
            // time_bucket(<interval>, <col>) — the interval is either an
            // INTERVAL literal or a string we can parse as `<n> <unit>`.
            let lit = literal_string(&args[0])?;
            let col = column_arg(&args[1])?;
            let secs = parse_interval_secs(&lit)?;
            Some((col, BucketWidth::SecondsFixed(secs)))
        }
        _ => None,
    }
}

fn literal_string(arg: &FunctionArg) -> Option<String> {
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        _ => return None,
    };
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s.clone()),
        Expr::Interval(iv) => match iv.value.as_ref() {
            Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
                // INTERVAL '5 minutes' — sqlparser stores the raw text.
                Some(s.clone())
            }
            _ => None,
        },
        _ => None,
    }
}

fn column_arg(arg: &FunctionArg) -> Option<String> {
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        _ => return None,
    };
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

/// Parse `'5 minutes'`, `'1 hour'`, etc. to seconds. Subset of PG's
/// interval grammar — the same units `parse_duration_secs` in
/// `basin-engine::cv_ddl` accepts.
fn parse_interval_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut total: u64 = 0;
    let mut saw = false;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == num_start {
            return None;
        }
        let n: u64 = s[num_start..i].parse().ok()?;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
            i += 1;
        }
        let unit = s[unit_start..i].to_ascii_lowercase();
        let factor: u64 = match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => 1,
            "m" | "min" | "mins" | "minute" | "minutes" => 60,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3600,
            "d" | "day" | "days" => 86_400,
            "w" | "week" | "weeks" => 604_800,
            _ => return None,
        };
        total = total.checked_add(n.checked_mul(factor)?)?;
        saw = true;
    }
    if saw {
        Some(total)
    } else {
        None
    }
}

/// Rewrite the user's CV SELECT body to filter the source table by
/// `<bucket_column> >= '<watermark>'`. We do this by parsing, finding
/// the FROM clause's single base-table reference, and wrapping it as
/// `(SELECT * FROM <table> WHERE <col> >= TIMESTAMP '<watermark>') AS <alias>`.
///
/// Returns the rewritten SQL on success. On any unsupported shape (set
/// op, multi-table FROM, JOIN), returns an error so the caller can fall
/// back to a full refresh.
pub fn rewrite_for_watermark(
    query_sql: &str,
    bucket_column: &str,
    watermark: DateTime<Utc>,
) -> Result<String> {
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, query_sql)
        .map_err(|e| BasinError::internal(format!("cv incremental: parse failed: {e}")))?;
    if stmts.len() != 1 {
        return Err(BasinError::internal(
            "cv incremental: expected exactly one statement",
        ));
    }
    let stmt = &mut stmts[0];
    let query = match stmt {
        Statement::Query(q) => q.as_mut(),
        _ => return Err(BasinError::internal("cv incremental: not a SELECT")),
    };
    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s.as_mut(),
        _ => {
            return Err(BasinError::internal(
                "cv incremental: set-op SELECT not supported",
            ))
        }
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(BasinError::internal(
            "cv incremental: multi-table / JOIN FROM not supported",
        ));
    }
    let table_ref = &mut select.from[0].relation;
    let table_name = match table_ref {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(BasinError::internal(
                "cv incremental: derived/lateral FROM not supported",
            ))
        }
    };
    // Build the watermark predicate. `TIMESTAMPTZ '...'` matches the
    // PG-style cast and works for both `Timestamp` and `Timestamptz`
    // columns under DataFusion's permissive coercion.
    let ts_lit = watermark.format("%Y-%m-%d %H:%M:%S%.6f%:z").to_string();
    let predicate = format!(
        "{col} >= TIMESTAMPTZ '{ts}'",
        col = bucket_column,
        ts = ts_lit,
    );
    // Wrap the table in a subquery: `(SELECT * FROM <table> WHERE ...) AS <table>`.
    // Re-parse the full subquery to get a Derived TableFactor we can splice
    // back in. This is simpler than constructing the AST nodes by hand and
    // sidesteps sqlparser version drift.
    let derived_sql = format!(
        "SELECT * FROM (SELECT * FROM {tbl} WHERE {pred}) AS {tbl}",
        tbl = table_name,
        pred = predicate,
    );
    let derived_stmts = Parser::parse_sql(&PostgreSqlDialect {}, &derived_sql)
        .map_err(|e| BasinError::internal(format!("cv incremental: build derived: {e}")))?;
    let new_factor = match &derived_stmts[0] {
        Statement::Query(q) => match q.body.as_ref() {
            SetExpr::Select(s) => s.from[0].relation.clone(),
            _ => return Err(BasinError::internal("cv incremental: derived SELECT lost")),
        },
        _ => return Err(BasinError::internal("cv incremental: derived not a SELECT")),
    };
    *table_ref = new_factor;
    Ok(format!("{stmt}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn detect_date_trunc_hour_group_by_alias() {
        let sql =
            "SELECT date_trunc('hour', ts) AS bucket, count(*) AS n FROM events GROUP BY bucket";
        let info = detect_time_bucket(sql).expect("should detect");
        assert_eq!(info.bucket_column, "ts");
        assert_eq!(info.bucket_alias, "bucket");
        assert_eq!(info.width, BucketWidth::Hour);
    }

    #[test]
    fn detect_date_trunc_day_group_by_ordinal() {
        let sql = "SELECT date_trunc('day', ts) AS d, count(*) FROM events GROUP BY 1";
        let info = detect_time_bucket(sql).expect("should detect");
        assert_eq!(info.bucket_column, "ts");
        assert_eq!(info.width, BucketWidth::Day);
    }

    #[test]
    fn detect_through_cast() {
        let sql =
            "SELECT cast(date_trunc('day', ts) AS TEXT) AS d, count(*) FROM events GROUP BY 1";
        let info = detect_time_bucket(sql).expect("should detect");
        assert_eq!(info.bucket_column, "ts");
        assert_eq!(info.width, BucketWidth::Day);
    }

    #[test]
    fn detect_time_bucket_5min() {
        let sql = "SELECT time_bucket('5 minutes', ts) AS b, count(*) FROM events GROUP BY 1";
        let info = detect_time_bucket(sql).expect("should detect");
        assert_eq!(info.bucket_column, "ts");
        assert_eq!(info.width, BucketWidth::SecondsFixed(300));
    }

    #[test]
    fn unsupported_shape_returns_none() {
        // No GROUP BY — incremental does not apply.
        assert!(detect_time_bucket("SELECT count(*) FROM events").is_none());
        // GROUP BY a non-time-bucket column.
        assert!(detect_time_bucket("SELECT user_id, count(*) FROM events GROUP BY 1").is_none());
        // extract() — out of scope.
        assert!(detect_time_bucket(
            "SELECT extract(hour FROM ts) AS h, count(*) FROM events GROUP BY 1"
        )
        .is_none());
    }

    #[test]
    fn floor_hour_aligns_to_top_of_hour() {
        let t = Utc.with_ymd_and_hms(2026, 5, 9, 12, 34, 56).unwrap();
        let f = BucketWidth::Hour.floor(t);
        assert_eq!(f, Utc.with_ymd_and_hms(2026, 5, 9, 12, 0, 0).unwrap());
    }

    #[test]
    fn floor_day_aligns_to_midnight() {
        let t = Utc.with_ymd_and_hms(2026, 5, 9, 12, 34, 56).unwrap();
        let f = BucketWidth::Day.floor(t);
        assert_eq!(f, Utc.with_ymd_and_hms(2026, 5, 9, 0, 0, 0).unwrap());
    }

    #[test]
    fn floor_5min_aligns_to_epoch_grid() {
        // 12:34:56 → 12:30:00 (5-minute grid relative to epoch).
        let t = Utc.with_ymd_and_hms(2026, 5, 9, 12, 34, 56).unwrap();
        let f = BucketWidth::SecondsFixed(300).floor(t);
        assert_eq!(f, Utc.with_ymd_and_hms(2026, 5, 9, 12, 30, 0).unwrap());
    }

    #[test]
    fn rewrite_injects_watermark_predicate() {
        let watermark = Utc.with_ymd_and_hms(2026, 5, 9, 12, 0, 0).unwrap();
        let out = rewrite_for_watermark(
            "SELECT date_trunc('hour', ts) AS b, count(*) FROM events GROUP BY 1",
            "ts",
            watermark,
        )
        .unwrap();
        assert!(
            out.to_ascii_lowercase().contains("ts >= timestamptz"),
            "rewrite missing watermark predicate: {out}"
        );
        // The original alias and GROUP BY must still be present.
        assert!(out.to_ascii_lowercase().contains("group by 1"));
    }
}
