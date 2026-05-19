//! Per-engine query history for adaptive write-time multi-sort (Phase 5.14.D2).
//!
//! Records ORDER BY and GROUP BY column patterns per `(project, table)` so the
//! compactor can later detect common access patterns and pre-sort files for
//! them (Phase 5.14.D1).
//!
//! ## Design
//!
//! A single [`QueryHistory`] is owned by the [`Engine`](crate::Engine).
//! All writes go through a `RwLock`-guarded `HashMap`; reads (including
//! [`top_pattern`]) take a shared read lock so they impose no overhead on the
//! query path beyond the lock acquisition itself.
//!
//! Per `(project, table)` we keep a [`TableHistory`] that maps column-tuples
//! (a sorted `Vec<String>`) to an occurrence count plus a total-query counter
//! and a window-start timestamp.  The window resets either after 10 000
//! queries or after seven days — whichever comes first.  This bounds both
//! memory (counters are discarded on reset) and staleness (patterns from a
//! workload profile that changed a week ago stop influencing the compactor).
//!
//! [`top_pattern`] returns the winning tuple only when it holds ≥ 30 % of all
//! queries **and** at least 100 total queries have been observed.  Below either
//! threshold the signal is too weak to justify a re-sort.

use std::collections::HashMap;
use std::sync::RwLock;

use basin_common::{ProjectId, TableName};
use chrono::{DateTime, Duration, Utc};

/// Total-query threshold per window: reset the window after this many queries.
const WINDOW_QUERY_LIMIT: u64 = 10_000;

/// Calendar duration of one window.  After this much wall-clock time the
/// window resets on the next write regardless of query count.
const WINDOW_DURATION_DAYS: i64 = 7;

/// Minimum total-query count before [`QueryHistory::top_pattern`] will report
/// a winner.  Below this the sample is too small to be meaningful.
const MIN_QUERIES_FOR_TOP: u64 = 100;

/// Fraction of total queries (in the range 0.0–1.0) the winner must hold.
const TOP_FRACTION_THRESHOLD: f64 = 0.30;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Process-wide in-memory query history.
///
/// Cheap to create (`RwLock` + empty `HashMap`).  Intended to be constructed
/// once inside [`crate::EngineInner`] and shared via `Arc`.
pub(crate) struct QueryHistory {
    inner: RwLock<HashMap<(ProjectId, TableName), TableHistory>>,
}

impl QueryHistory {
    /// Create an empty history.
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Record an ORDER BY column set for the given `(project, table)`.
    ///
    /// `cols` is the raw column list in query order; it is sorted before
    /// storage so `ORDER BY a, b` and `ORDER BY b, a` hash to the same
    /// bucket.  Non-column expressions (e.g. `ORDER BY 1`) must be filtered
    /// out by the caller before passing `cols`; an empty `cols` is silently
    /// ignored.
    pub(crate) fn record_order_by(
        &self,
        project: &ProjectId,
        table: &TableName,
        cols: Vec<String>,
    ) {
        if cols.is_empty() {
            return;
        }
        self.record(project, table, cols);
    }

    /// Record a GROUP BY column set for the given `(project, table)`.
    ///
    /// The same sort-normalisation as `record_order_by` applies.
    pub(crate) fn record_group_by(
        &self,
        project: &ProjectId,
        table: &TableName,
        cols: Vec<String>,
    ) {
        if cols.is_empty() {
            return;
        }
        self.record(project, table, cols);
    }

    /// Return the most frequent column tuple for `(project, table)` when it
    /// meets both thresholds:
    ///
    /// * ≥ [`MIN_QUERIES_FOR_TOP`] total queries in the current window.
    /// * The winning tuple accounts for ≥ [`TOP_FRACTION_THRESHOLD`] of those
    ///   queries.
    ///
    /// Returns `None` when the window is empty, too small, or no single tuple
    /// crosses the fraction threshold.
    pub(crate) fn top_pattern(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Option<Vec<String>> {
        let key = (*project, table.clone());
        let guard = self.inner.read().expect("query_history read lock poisoned");
        let hist = guard.get(&key)?;

        if hist.total_queries < MIN_QUERIES_FOR_TOP {
            return None;
        }

        let (best_tuple, &best_count) = hist
            .counts
            .iter()
            .max_by_key(|(_, &c)| c)?;

        let fraction = best_count as f64 / hist.total_queries as f64;
        if fraction < TOP_FRACTION_THRESHOLD {
            return None;
        }

        Some(best_tuple.clone())
    }

    /// Reset the window for `(project, table)` if it has become stale.
    ///
    /// Staleness is defined as either:
    /// * The window has accumulated ≥ [`WINDOW_QUERY_LIMIT`] queries, or
    /// * The window started more than [`WINDOW_DURATION_DAYS`] days ago.
    ///
    /// Called from the write path; when the window is not stale this is a
    /// no-op (one read-lock acquisition, no allocation).
    pub(crate) fn cycle_window_if_stale(&self, project: &ProjectId, table: &TableName) {
        let key = (*project, table.clone());

        // Fast path: read lock only — avoid write lock when window is fresh.
        {
            let guard = self
                .inner
                .read()
                .expect("query_history read lock poisoned");
            if let Some(hist) = guard.get(&key) {
                if !hist.is_stale() {
                    return;
                }
            } else {
                // No entry yet — nothing to cycle.
                return;
            }
        }

        // Slow path: stale window found; reset under write lock.
        let mut guard = self
            .inner
            .write()
            .expect("query_history write lock poisoned");
        if let Some(hist) = guard.get_mut(&key) {
            if hist.is_stale() {
                hist.reset();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl QueryHistory {
    /// Core write path: normalise `cols`, cycle the window if needed, and
    /// increment the counter.
    fn record(&self, project: &ProjectId, table: &TableName, mut cols: Vec<String>) {
        cols.sort_unstable();

        let key = (*project, table.clone());
        // Write lock required; this is the only path that mutates inner.
        let mut guard = self
            .inner
            .write()
            .expect("query_history write lock poisoned");

        let hist = guard.entry(key).or_insert_with(TableHistory::new);
        if hist.is_stale() {
            hist.reset();
        }
        *hist.counts.entry(cols).or_insert(0) += 1;
        hist.total_queries += 1;
    }
}

// ---------------------------------------------------------------------------
// Per-table window
// ---------------------------------------------------------------------------

struct TableHistory {
    /// Map of sorted column tuples → occurrence count.
    counts: HashMap<Vec<String>, u64>,
    /// Total queries recorded in the current window.
    total_queries: u64,
    /// When the current window started.
    window_start: DateTime<Utc>,
}

impl TableHistory {
    fn new() -> Self {
        Self {
            counts: HashMap::new(),
            total_queries: 0,
            window_start: Utc::now(),
        }
    }

    /// True if this window should be discarded and a fresh one started.
    fn is_stale(&self) -> bool {
        if self.total_queries >= WINDOW_QUERY_LIMIT {
            return true;
        }
        let age = Utc::now() - self.window_start;
        age >= Duration::days(WINDOW_DURATION_DAYS)
    }

    /// Discard counts and begin a new window.
    fn reset(&mut self) {
        self.counts.clear();
        self.total_queries = 0;
        self.window_start = Utc::now();
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> ProjectId {
        ProjectId::new()
    }

    fn table(name: &str) -> TableName {
        TableName::new(name).expect("valid table name")
    }

    /// Insert N copies of one ORDER BY pattern and a few others, then assert
    /// that top_pattern returns the dominant tuple.
    #[test]
    fn top_pattern_dominant_tuple_returned() {
        let hist = QueryHistory::new();
        let p = project();
        let t = table("events");

        // 100 queries with ORDER BY (b, id).
        for _ in 0..100 {
            hist.record_order_by(&p, &t, vec!["b".into(), "id".into()]);
        }
        // 10 with ORDER BY (id).
        for _ in 0..10 {
            hist.record_order_by(&p, &t, vec!["id".into()]);
        }
        // 5 with ORDER BY (k).
        for _ in 0..5 {
            hist.record_order_by(&p, &t, vec!["k".into()]);
        }

        // Total = 115; winner is ["b", "id"] with 100/115 ≈ 87 %.
        let pattern = hist.top_pattern(&p, &t).expect("should have a top pattern");
        // Tuple is stored in sorted order.
        assert_eq!(pattern, vec!["b".to_string(), "id".to_string()]);
    }

    /// When no single pattern reaches 30 % of queries, top_pattern is None.
    #[test]
    fn top_pattern_none_when_below_threshold() {
        let hist = QueryHistory::new();
        let p = project();
        let t = table("logs");

        // Three patterns, each with ~33 queries; none > 30 % clearly…
        // Use 34 + 33 + 33 = 100 total; leader is 34/100 = 34 % → should win.
        // To stay clearly below 30 % use equal counts of 4 groups: 25 each.
        for _ in 0..25 {
            hist.record_order_by(&p, &t, vec!["a".into()]);
        }
        for _ in 0..25 {
            hist.record_order_by(&p, &t, vec!["b".into()]);
        }
        for _ in 0..25 {
            hist.record_order_by(&p, &t, vec!["c".into()]);
        }
        for _ in 0..25 {
            hist.record_order_by(&p, &t, vec!["d".into()]);
        }
        // Each has exactly 25 % — ties at the boundary.  Use strict < so 25 %
        // < 30 % → None.
        assert!(hist.top_pattern(&p, &t).is_none());
    }

    /// Below 100 total queries top_pattern returns None regardless of fraction.
    #[test]
    fn top_pattern_none_when_too_few_queries() {
        let hist = QueryHistory::new();
        let p = project();
        let t = table("tiny");

        // 99 identical queries — fraction is 100 % but count < threshold.
        for _ in 0..99 {
            hist.record_order_by(&p, &t, vec!["x".into()]);
        }
        assert!(hist.top_pattern(&p, &t).is_none());
    }

    /// Column tuples are sorted before storage, so ORDER BY a, b and
    /// ORDER BY b, a both increment the same bucket.
    #[test]
    fn tuple_sort_normalisation() {
        let hist = QueryHistory::new();
        let p = project();
        let t = table("items");

        for _ in 0..60 {
            hist.record_order_by(&p, &t, vec!["b".into(), "a".into()]);
        }
        for _ in 0..60 {
            hist.record_order_by(&p, &t, vec!["a".into(), "b".into()]);
        }
        // Both spellings end up in the same ["a", "b"] bucket: 120/120 = 100 %.
        let pattern = hist.top_pattern(&p, &t).unwrap();
        assert_eq!(pattern, vec!["a".to_string(), "b".to_string()]);
    }

    /// GROUP BY recording follows the same rules as ORDER BY recording.
    #[test]
    fn group_by_recording() {
        let hist = QueryHistory::new();
        let p = project();
        let t = table("sales");

        for _ in 0..110 {
            hist.record_group_by(&p, &t, vec!["region".into(), "month".into()]);
        }
        let pattern = hist.top_pattern(&p, &t).unwrap();
        assert_eq!(
            pattern,
            vec!["month".to_string(), "region".to_string()]
        );
    }

    /// Window reset clears counts and total_queries.
    #[test]
    fn window_reset_clears_history() {
        let p = project();
        let t = table("audit");

        // Manually build a history and force-reset it.
        let mut h = TableHistory::new();
        h.counts.insert(vec!["col".into()], 500);
        h.total_queries = 500;
        h.reset();

        assert_eq!(h.total_queries, 0);
        assert!(h.counts.is_empty());

        // After reset a new QueryHistory backed by it will return None.
        let hist = QueryHistory::new();
        // Record once — still below MIN_QUERIES_FOR_TOP.
        hist.record_order_by(&p, &t, vec!["col".into()]);
        assert!(hist.top_pattern(&p, &t).is_none());
    }
}
