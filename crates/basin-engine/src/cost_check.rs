//! Cost-based query rejection (Phase 6).
//!
//! Reject queries at planning time when their estimated cost exceeds the
//! configured per-project cap. The wedge story is multi-project SaaS: a
//! single project's runaway `SELECT * FROM huge_table` (no LIMIT, no
//! WHERE) can saturate a shard's IO budget and steal latency from
//! every other project on the same shard. Pre-emptive rejection keeps
//! the noisy-neighbour blast radius bounded — drivers see PG SQLSTATE
//! `54000` (`program_limit_exceeded`), an explicit retry-with-backoff
//! signal that's distinct from a parse / permission error.
//!
//! ## v0.1 scope
//!
//! Estimates **rows touched** for single-table SELECTs only:
//!
//! - `SELECT … FROM single_table` → row_count of the current snapshot
//! - `SELECT … FROM t LIMIT N` → unchecked (the planner caps output)
//! - `JOIN`, multi-`FROM`, sub-queries, CTEs → unchecked
//!
//! "Unchecked" means we let the query through. v0.1 catches the dominant
//! footgun (forgetting a `WHERE` on the audit-log table); complex shapes
//! get a proper logical-plan walker in v0.2 that uses the A4 catalog
//! `ColumnStats` for selectivity estimation.
//!
//! ## Configuration
//!
//! `BASIN_QUERY_COST_LIMIT_ROWS=N` — sets the per-query row cap. Unset /
//! empty / `0` disables the check entirely (the default — bench /
//! benchmark suites and PoC demos run unbounded). Read once at startup
//! and cached for the process lifetime via `OnceLock` so a query never
//! pays a getenv on the hot path.

use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
use std::sync::OnceLock;

use basin_catalog::Catalog;
use basin_common::{BasinError, ProjectId, Result, TableName};
use sqlparser::ast::{Statement, TableFactor};

/// Process-wide cost cap. `None` = unbounded; `Some(n)` = reject queries
/// whose estimate exceeds `n` rows. Cached so repeated calls are free.
pub fn cost_limit_rows() -> Option<u64> {
    static CACHED: OnceLock<Option<u64>> = OnceLock::new();
    *CACHED.get_or_init(|| {
        parse_cost_limit(std::env::var("BASIN_QUERY_COST_LIMIT_ROWS").ok().as_deref())
    })
}

/// Pure parse: takes the raw string (or `None` for unset) and produces the
/// effective cap. Pulled out of `cost_limit_rows` so unit tests pass strings
/// directly instead of mutating the process env (which races under
/// `cargo test`'s default thread-parallel runner).
fn parse_cost_limit(raw: Option<&str>) -> Option<u64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let n: u64 = raw.parse().ok()?;
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

/// Estimate how many rows `stmt` will scan against `project`'s catalog.
/// Returns `Ok(None)` when v0.1 can't estimate (multi-FROM / JOIN /
/// subquery / non-Query statement / explicit LIMIT) — callers treat
/// `None` as "pass through unchecked." A genuine catalog failure
/// propagates as `Err`.
pub async fn estimate_query_rows(
    catalog: &dyn Catalog,
    project: &ProjectId,
    stmt: &Statement,
) -> Result<Option<u64>> {
    let q = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };
    if q.ext_limit().is_some() {
        return Ok(None);
    }
    let body = match q.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return Ok(None),
    };
    if body.from.len() != 1 {
        return Ok(None);
    }
    let twj = &body.from[0];
    if !twj.joins.is_empty() {
        return Ok(None);
    }
    let raw_name = match &twj.relation {
        TableFactor::Table { name, .. } => match name.0.last() {
            Some(ident) => ident.id_val().clone(),
            None => return Ok(None),
        },
        _ => return Ok(None),
    };
    let table = match TableName::new(raw_name) {
        Ok(t) => t,
        Err(_) => return Ok(None),
    };
    let meta = match catalog.load_table(project, &table).await {
        Ok(m) => m,
        // A missing table isn't a cost-check failure; let the regular
        // planner produce its own `relation does not exist` error so the
        // user sees the existing message they expect.
        Err(_) => return Ok(None),
    };
    let total: u64 = match meta.current() {
        Some(snap) => snap.data_files.iter().map(|f| f.row_count).sum(),
        None => 0,
    };
    Ok(Some(total))
}

/// Compare an estimate against a cap; returns the appropriate
/// `BasinError::QueryCostExceeded` when the estimate wins. The router's
/// `classify` maps that variant to PG SQLSTATE 54000 so drivers raise a
/// distinct exception class.
pub fn check_cost(estimate: u64, limit: u64) -> Result<()> {
    if estimate > limit {
        return Err(BasinError::QueryCostExceeded(format!(
            "query estimated to scan {estimate} rows, exceeds \
             BASIN_QUERY_COST_LIMIT_ROWS={limit}; add a LIMIT clause or \
             a more selective WHERE predicate"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut s = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        assert_eq!(s.len(), 1);
        s.pop().unwrap()
    }

    #[test]
    fn cost_limit_under_threshold_passes() {
        check_cost(1_000, 10_000).unwrap();
    }

    #[test]
    fn cost_limit_at_threshold_passes() {
        // Threshold is `> limit`, so equal-to-limit is fine. This makes
        // it possible to set the bar to exactly a known table size.
        check_cost(10_000, 10_000).unwrap();
    }

    #[test]
    fn cost_limit_over_threshold_fails_with_helpful_message() {
        let err = check_cost(50_000, 10_000).unwrap_err();
        match err {
            BasinError::QueryCostExceeded(msg) => {
                assert!(msg.contains("50000"), "estimate missing: {msg}");
                assert!(msg.contains("10000"), "limit missing: {msg}");
                assert!(
                    msg.contains("LIMIT") || msg.contains("WHERE"),
                    "advice missing: {msg}",
                );
            }
            other => panic!("expected QueryCostExceeded, got {other:?}"),
        }
    }

    #[test]
    fn parse_zero_means_unset() {
        // "0" is a legitimate way for an operator to disable the check
        // from a config-management tool that can't easily *unset* a
        // variable.
        assert!(parse_cost_limit(Some("0")).is_none());
    }

    #[test]
    fn parse_unset_means_disabled() {
        assert!(parse_cost_limit(None).is_none());
    }

    #[test]
    fn parse_empty_means_disabled() {
        assert!(parse_cost_limit(Some("")).is_none());
        assert!(parse_cost_limit(Some("   ")).is_none());
    }

    #[test]
    fn parse_valid_returns_limit() {
        assert_eq!(parse_cost_limit(Some("12345")), Some(12345));
    }

    #[test]
    fn parse_trims_whitespace() {
        assert_eq!(parse_cost_limit(Some("  17  ")), Some(17));
    }

    #[test]
    fn parse_garbage_means_disabled() {
        // Permissive: a typo doesn't break the server. The startup log
        // (added by the call site) surfaces the parse failure to the
        // operator while the server keeps running.
        assert!(parse_cost_limit(Some("not-a-number")).is_none());
    }

    #[test]
    fn unestimable_shapes_return_none() {
        // The async helper needs a real Catalog so we can't drive it
        // directly here. We assert the *static* shape rejection: any
        // statement we'd want to skip the cost check for must produce
        // a structure that signals "no estimate possible."
        let stmt = parse_one("INSERT INTO t VALUES (1)");
        assert!(matches!(stmt, Statement::Insert(_)));

        let stmt = parse_one("SELECT * FROM t LIMIT 10");
        if let Statement::Query(q) = &stmt {
            assert!(q.ext_limit().is_some(), "LIMIT must be parsed");
        } else {
            panic!("expected Query");
        }

        let stmt = parse_one("SELECT * FROM a JOIN b ON a.id = b.id");
        if let Statement::Query(q) = &stmt {
            if let sqlparser::ast::SetExpr::Select(s) = q.body.as_ref() {
                assert_eq!(s.from.len(), 1);
                assert!(!s.from[0].joins.is_empty(), "JOIN must populate joins");
            }
        }
    }
}
