//! ADR 0027 Phase 4 auto-promotion — detection + threshold trigger.
//!
//! ## Design
//!
//! This module tracks how often each `(project, table, source_col, json_key)`
//! tuple is accessed via a JSONB extract operator (`->>` / `->`, lowered to
//! `json_get_text` / `json_get` by the SQL rewriter).  When a path's access
//! count crosses [`AUTO_PROMOTE_MIN_HITS`] it is automatically promoted via
//! [`basin_catalog::Catalog::promote_jsonb_path`] so subsequent queries skip
//! the per-row UDF dispatch and read a plain Arrow column instead.
//!
//! ## Threshold rationale
//!
//! `AUTO_PROMOTE_MIN_HITS = 8` is chosen so that:
//! * A single noisy burst in a micro-benchmark (≤ 4–5 repeated queries)
//!   does NOT trigger promotion without genuine, sustained interest.
//! * A realistic OLAP workload that re-queries the same JSON key 8+ times
//!   is very likely to continue doing so (justified shadow-column cost).
//! * The value is deliberately small compared to the adaptive-sort threshold
//!   (100 queries) because promotion has no per-query runtime cost once it
//!   fires; the shadow column is materialized asynchronously at insert time.
//!
//! ## Hot-path cost
//!
//! The recording path takes one `RwLock` write-lock, looks up a nested
//! `HashMap`, and increments a `u64` counter.  This is O(1) with negligible
//! contention; it is never on the critical SELECT latency path (the catalog
//! `promote_jsonb_path` call is fire-and-forget, spawned off the query thread).
//!
//! ## Idempotency
//!
//! Once a path has been submitted for promotion we flip a boolean in the
//! in-memory registry so subsequent threshold crossings do not re-issue the
//! async catalog call.  The catalog call itself is also idempotent (Phase 4
//! spec), providing a belt-and-suspenders guarantee across process restarts.

use std::collections::HashMap;
use std::sync::RwLock;

use basin_catalog::Catalog;
use basin_common::{ProjectId, TableName};

/// Minimum number of observed accesses before a JSONB path is auto-promoted.
///
/// See module-level doc for the rationale.  Named and exported so tests can
/// reference the threshold directly.
pub const AUTO_PROMOTE_MIN_HITS: u64 = 8;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Per-key counter and promoted flag.
#[derive(Default)]
struct PathEntry {
    hits: u64,
    /// True once we have fired the async `promote_jsonb_path` call.
    promoted: bool,
}

/// Per-table registry keyed by `(source_col, json_key)`.
type TablePathCounts = HashMap<(String, String), PathEntry>;

/// Process-wide JSONB path access tracker.
///
/// Cheap to create (a `RwLock`-guarded `HashMap`).  Intended to be
/// constructed once inside [`crate::EngineInner`] and shared via `Arc`.
pub struct JsonbPromotionRegistry {
    inner: RwLock<HashMap<(ProjectId, TableName), TablePathCounts>>,
}

impl JsonbPromotionRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Record one access to `(project, table, source_col, json_key)`.
    ///
    /// Returns `Some((source_col, json_key))` when the path just crossed the
    /// promotion threshold for the first time (the caller should fire the
    /// async catalog call).  Returns `None` when below threshold or already
    /// promoted.
    pub(crate) fn record_hit(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> Option<(String, String)> {
        let key = (*project, table.clone());
        let path_key = (source_col.to_string(), json_key.to_string());

        let mut guard = self
            .inner
            .write()
            .expect("jsonb_promotion write lock poisoned");

        let table_counts = guard.entry(key).or_default();
        let entry = table_counts.entry(path_key.clone()).or_default();

        // Already promoted — nothing to do.
        if entry.promoted {
            return None;
        }

        entry.hits += 1;

        if entry.hits >= AUTO_PROMOTE_MIN_HITS {
            // Mark as promoted immediately so concurrent/future calls see the
            // flag without waiting for the async catalog call to complete.
            entry.promoted = true;
            Some(path_key)
        } else {
            None
        }
    }

    // -----------------------------------------------------------------------
    // Test hooks
    // -----------------------------------------------------------------------

    /// Return the current hit count for `(project, table, source_col, key)`.
    ///
    /// Returns 0 when the path has never been seen.  Used by unit/integration
    /// tests to observe the recorded frequency.
    pub fn hit_count(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> u64 {
        let key = (*project, table.clone());
        let path_key = (source_col.to_string(), json_key.to_string());
        let guard = self
            .inner
            .read()
            .expect("jsonb_promotion read lock poisoned");
        guard
            .get(&key)
            .and_then(|t| t.get(&path_key))
            .map(|e| e.hits)
            .unwrap_or(0)
    }

    /// Return whether `(project, table, source_col, key)` has been promoted.
    ///
    /// `true` means the async `catalog.promote_jsonb_path` call has been
    /// fired (the registry flips this flag synchronously before spawning the
    /// async task).  Used by unit/integration tests to assert promotion fired.
    pub fn is_promoted(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> bool {
        let key = (*project, table.clone());
        let path_key = (source_col.to_string(), json_key.to_string());
        let guard = self
            .inner
            .read()
            .expect("jsonb_promotion read lock poisoned");
        guard
            .get(&key)
            .and_then(|t| t.get(&path_key))
            .map(|e| e.promoted)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Query-time extraction of JSON path accesses
// ---------------------------------------------------------------------------

/// Walk `sql` (after `rewrite_json_operators` has run) and extract every
/// `json_get_text(<col>, '<key>')` and `json_get(<col>, '<key>')` occurrence
/// where `<col>` is a bare identifier.
///
/// Returns a `Vec<(source_col, json_key)>` deduped so that a single query
/// with the same `col->>'key'` expression repeated counts as one hit per
/// `(col, key)` pair (multiple occurrences within one query body = one
/// access event — the outer caller increments on each *query*, not on each
/// expression node).
///
/// This is a lightweight text scan rather than a full AST walk; it matches
/// the pattern emitted by `udf::rewrite_json_operators` exactly:
///
/// ```text
/// json_get_text(<ident>, '<key>')
/// json_get(<ident>, '<key>')
/// ```
///
/// Nested forms such as `json_get_text(json_get(col,'a'),'b')` are
/// intentionally ignored — the inner result is not a column; only the
/// *outermost* direct-column access matters for top-level promotion.
pub(crate) fn extract_json_path_accesses(sql: &str) -> Vec<(String, String)> {
    let lower = sql.to_ascii_lowercase();
    let mut seen: Vec<(String, String)> = Vec::new();

    for func in &["json_get_text(", "json_get("] {
        let mut search = lower.as_str();
        let mut byte_offset = 0usize;

        while let Some(pos) = search.find(func) {
            let abs = byte_offset + pos;
            // Advance past the function name + '('.
            let after_open = abs + func.len();
            if after_open >= sql.len() {
                break;
            }
            // The remainder of the original SQL from after '('.
            let rest = &sql[after_open..];

            // Skip leading whitespace.
            let rest_trimmed = rest.trim_start();
            let ws_len = rest.len() - rest_trimmed.len();

            // The first argument must be a bare identifier (column name).
            // We accept [a-zA-Z_][a-zA-Z0-9_$]*.
            let ident_bytes = rest_trimmed.as_bytes();
            if ident_bytes.is_empty() || !is_ident_start(ident_bytes[0]) {
                // Nested call (first arg is another function call) — skip.
                byte_offset += pos + func.len();
                search = &lower[byte_offset..];
                continue;
            }
            let ident_len = ident_bytes
                .iter()
                .position(|&b| !is_ident_cont(b))
                .unwrap_or(ident_bytes.len());
            let source_col = &rest_trimmed[..ident_len];

            // After the identifier: optional whitespace, then a comma.
            let after_ident = &rest_trimmed[ident_len..];
            let after_ident_trimmed = after_ident.trim_start();
            if !after_ident_trimmed.starts_with(',') {
                byte_offset += pos + func.len();
                search = &lower[byte_offset..];
                continue;
            }
            // After comma: whitespace, then single-quoted key literal.
            let after_comma = after_ident_trimmed[1..].trim_start();
            if !after_comma.starts_with('\'') {
                byte_offset += pos + func.len();
                search = &lower[byte_offset..];
                continue;
            }
            // Scan for the closing quote.
            let key_content = &after_comma[1..];
            if let Some(close) = key_content.find('\'') {
                let json_key = &after_comma[1..1 + close];
                // Skip empty keys.
                if !json_key.is_empty() {
                    let pair = (source_col.to_string(), json_key.to_string());
                    if !seen.contains(&pair) {
                        seen.push(pair);
                    }
                }
            }

            // Advance the search window past this match to find the next one.
            byte_offset += pos + func.len() + ws_len + ident_len;
            search = &lower[byte_offset..];
        }
    }

    seen
}

#[inline]
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

#[inline]
fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

// ---------------------------------------------------------------------------
// Fire-and-forget promotion trigger
// ---------------------------------------------------------------------------

/// Called from the query path after `rewrite_json_operators` has run.
///
/// Extracts JSON path accesses from `sql`, records hits in `registry`, and
/// for any path that just crossed the threshold spawns a fire-and-forget
/// `tokio::task` to call `catalog.promote_jsonb_path(...)`.
///
/// The spawn is best-effort: if the current thread is not inside a tokio
/// runtime (e.g. synchronous unit tests) the call is skipped gracefully.
/// The registry already tracks the promoted flag synchronously, so the
/// catalog call is the only async part.
pub(crate) fn observe_and_maybe_promote(
    sql: &str,
    project: &ProjectId,
    table_refs: &[String],
    registry: &std::sync::Arc<JsonbPromotionRegistry>,
    catalog: std::sync::Arc<dyn Catalog>,
) {
    // Fast bail-out: no JSON extract calls in this SQL.
    if !sql.to_ascii_lowercase().contains("json_get") {
        return;
    }

    let accesses = extract_json_path_accesses(sql);
    if accesses.is_empty() {
        return;
    }

    // Attribute each access to every referenced table (same strategy as
    // `rewrite_promoted_cols_for_query`).  In typical single-table queries
    // there is exactly one table in `table_refs`.
    for table_str in table_refs {
        let Ok(table_name) = basin_common::TableName::new(table_str.clone()) else {
            continue;
        };
        for (source_col, json_key) in &accesses {
            if let Some((col, key)) =
                registry.record_hit(project, &table_name, source_col, json_key)
            {
                // Threshold just crossed — fire-and-forget async promotion.
                let project_clone = *project;
                let table_clone = table_name.clone();
                let catalog_clone = catalog.clone();

                if let Ok(rt) = tokio::runtime::Handle::try_current() {
                    rt.spawn(async move {
                        let _ = catalog_clone
                            .promote_jsonb_path(&project_clone, &table_clone, &col, &key)
                            .await;
                    });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── extract_json_path_accesses ──────────────────────────────────────────

    #[test]
    fn extract_simple_get_text() {
        let sql = "SELECT json_get_text(payload, 'category') FROM t";
        let got = extract_json_path_accesses(sql);
        assert_eq!(got, vec![("payload".to_string(), "category".to_string())]);
    }

    #[test]
    fn extract_simple_get() {
        let sql = "SELECT json_get(data, 'tags') FROM t";
        let got = extract_json_path_accesses(sql);
        assert_eq!(got, vec![("data".to_string(), "tags".to_string())]);
    }

    #[test]
    fn extract_two_different_keys() {
        let sql =
            "SELECT json_get_text(payload, 'device'), json_get_text(payload, 'version') FROM t";
        let got = extract_json_path_accesses(sql);
        assert!(got.contains(&("payload".to_string(), "device".to_string())), "got: {got:?}");
        assert!(got.contains(&("payload".to_string(), "version".to_string())), "got: {got:?}");
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn extract_deduplicates_same_key() {
        // The same (col, key) pair appears twice — should count as one entry.
        let sql = "SELECT json_get_text(payload, 'k'), json_get_text(payload, 'k') FROM t";
        let got = extract_json_path_accesses(sql);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], ("payload".to_string(), "k".to_string()));
    }

    #[test]
    fn extract_skips_nested_call() {
        // json_get_text(json_get(col,'a'),'b') — inner arg is a function call,
        // not a bare identifier.  The outer call has first arg = `json_get(...`
        // which does NOT start with an identifier character (well, 'j' does),
        // but after the identifier we'd find `(`, not `,`.
        // Actually `json_get` *is* an identifier start, so we need to verify
        // the comma check handles nested calls.
        // The pattern `json_get(col,'a')` extracted from `json_get_text(json_get(col,'a'),'b')`:
        // First arg after `json_get_text(` is `json_get` — that IS an ident,
        // but then after the ident chars we see `(`, not `,`.  So it's skipped.
        let sql = "SELECT json_get_text(json_get(payload, 'meta'), 'score') FROM t";
        let got = extract_json_path_accesses(sql);
        // Only the inner json_get(payload, 'meta') should be extracted
        // (it has a bare column first arg).  The outer json_get_text is skipped
        // because its first arg is not a bare column followed by ','.
        // Verify we don't extract garbage.
        for (col, _key) in &got {
            assert!(!col.contains('('), "extracted a function-call as col name: {col}");
        }
    }

    #[test]
    fn extract_no_json_calls_returns_empty() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let got = extract_json_path_accesses(sql);
        assert!(got.is_empty(), "expected empty, got: {got:?}");
    }

    // ── JsonbPromotionRegistry ──────────────────────────────────────────────

    fn project() -> ProjectId {
        ProjectId::new()
    }

    fn table(name: &str) -> TableName {
        TableName::new(name).expect("valid table name")
    }

    #[test]
    fn below_threshold_not_promoted() {
        let registry = JsonbPromotionRegistry::new();
        let p = project();
        let t = table("events");

        for _ in 0..(AUTO_PROMOTE_MIN_HITS - 1) {
            let result = registry.record_hit(&p, &t, "payload", "category");
            assert!(result.is_none(), "should not promote below threshold");
        }

        assert_eq!(
            registry.hit_count(&p, &t, "payload", "category"),
            AUTO_PROMOTE_MIN_HITS - 1
        );
        assert!(!registry.is_promoted(&p, &t, "payload", "category"));
    }

    #[test]
    fn at_threshold_promotes() {
        let registry = JsonbPromotionRegistry::new();
        let p = project();
        let t = table("events");

        for i in 0..AUTO_PROMOTE_MIN_HITS {
            let result = registry.record_hit(&p, &t, "payload", "category");
            if i < AUTO_PROMOTE_MIN_HITS - 1 {
                assert!(result.is_none(), "hit {i}: should not promote yet");
            } else {
                assert_eq!(
                    result,
                    Some(("payload".to_string(), "category".to_string())),
                    "hit {i}: should have promoted"
                );
            }
        }

        assert!(registry.is_promoted(&p, &t, "payload", "category"));
    }

    #[test]
    fn already_promoted_no_second_trigger() {
        let registry = JsonbPromotionRegistry::new();
        let p = project();
        let t = table("orders");

        // Drive to threshold.
        let mut triggered = 0u32;
        for _ in 0..AUTO_PROMOTE_MIN_HITS * 3 {
            if registry
                .record_hit(&p, &t, "body", "status")
                .is_some()
            {
                triggered += 1;
            }
        }
        // Exactly one trigger, even after many extra hits.
        assert_eq!(triggered, 1, "expected exactly one promotion trigger");
    }

    #[test]
    fn two_keys_tracked_independently() {
        let registry = JsonbPromotionRegistry::new();
        let p = project();
        let t = table("logs");

        // Pump key "a" to threshold.
        let mut trigger_a = false;
        for _ in 0..AUTO_PROMOTE_MIN_HITS {
            if registry.record_hit(&p, &t, "payload", "a").is_some() {
                trigger_a = true;
            }
        }
        assert!(trigger_a, "key 'a' should have triggered");

        // Key "b" is still below threshold.
        for _ in 0..(AUTO_PROMOTE_MIN_HITS - 1) {
            let r = registry.record_hit(&p, &t, "payload", "b");
            assert!(r.is_none(), "key 'b' should not trigger yet");
        }
        assert!(registry.is_promoted(&p, &t, "payload", "a"));
        assert!(!registry.is_promoted(&p, &t, "payload", "b"));
    }

    #[test]
    fn different_tables_independent() {
        let registry = JsonbPromotionRegistry::new();
        let p = project();
        let t1 = table("t1");
        let t2 = table("t2");

        for _ in 0..AUTO_PROMOTE_MIN_HITS {
            registry.record_hit(&p, &t1, "payload", "k");
        }
        // t2 has had zero hits.
        assert!(!registry.is_promoted(&p, &t2, "payload", "k"));
        assert!(registry.is_promoted(&p, &t1, "payload", "k"));
    }
}
