//! Self-driving physical layout — step 1: the auto-index advisor.
//!
//! ## What it does
//!
//! This module observes repeated non-PK **equality** predicates
//! (`WHERE <col> = <literal>`) flowing through the SELECT path and, once a
//! `(project, table, column)` triple has been queried often enough, fires a
//! one-shot `CREATE INDEX` for that column. The new secondary B-tree index
//! then lets subsequent point queries prune to the matching file(s) instead of
//! scanning the whole table.
//!
//! It is modelled *exactly* on the proven [`crate::jsonb_promotion`] pattern:
//!
//! * an in-RAM registry keyed by `(project, table, column)` with a `u64` hit
//!   counter and a fire-once `fired` flag,
//! * a frequency threshold ([`AUTO_INDEX_MIN_HITS`]) that, once crossed, flips
//!   the `fired` flag *synchronously* (so concurrent threshold-crossers do not
//!   double-fire) and spawns a fire-and-forget `tokio` task,
//! * the async task executes the **same** code path a user `CREATE INDEX`
//!   takes, so the index creation, catalog write, and registry backfill are
//!   byte-for-byte identical to the hand-written DDL.
//!
//! ## Threshold rationale — why 8, not 3
//!
//! JSONB promotion uses `AUTO_PROMOTE_MIN_HITS = 3` because a promoted shadow
//! column is *cheap*: it is materialised asynchronously at write time and adds
//! zero per-row overhead afterwards, so promoting "too early" only wastes a
//! one-time compaction.
//!
//! A secondary index is **not** free after creation:
//!
//! * **Storage cost.** Every indexed `(key → file/row-group/row)` location is
//!   persisted to the object store as an `.idx` file and held in RAM.
//! * **Write amplification.** Every subsequent INSERT / UPDATE / DELETE /
//!   compaction on the table must maintain the index, so an index we create
//!   speculatively taxes *all future writes* — not just a one-time scan.
//!
//! Because the ongoing cost is borne forever, the advisor is deliberately more
//! conservative than promotion: [`AUTO_INDEX_MIN_HITS`] = `8`. Eight identical
//! point-query shapes is a strong signal of a durable application access
//! pattern (a primary read path), which is exactly the case where the index
//! pays for its write-amplification tax. One-off and exploratory queries stop
//! far short of 8 and never trigger an index.
//!
//! ## Hot-path cost
//!
//! Recording a hit takes one `RwLock` write-lock, a nested `HashMap` lookup,
//! and a `u64` increment — O(1), never on the SELECT latency critical path
//! (the `CREATE INDEX` is spawned fire-and-forget off the query thread). The
//! kill switch and an empty-Eq-column fast bail keep the common case (no
//! eligible predicate) to a couple of cheap branches before any lock is taken.
//!
//! ## Eligibility (what we observe)
//!
//! At observe time a candidate column must satisfy ALL of:
//!
//! * appear in an `Eq` predicate (point lookup) — `Gt` / `Lt` / `StartsWith`
//!   are ignored (a B-tree point index does not serve them on the fast path),
//! * **not** be a primary-key column (the PK already has its own fast path; a
//!   redundant secondary index would only add write amplification),
//! * have an **indexable** Arrow type — exactly the types the secondary-index
//!   extractor (`crate::secondary_index::extract_entries_from_batch`)
//!   supports: `Int64`, `UInt64`, `Float64`, `Utf8`, `LargeUtf8`, `Boolean`.
//!   Observing an unsupported type would let the counter cross the threshold
//!   and fire a `CREATE INDEX` whose backfill silently extracts nothing, so we
//!   filter at the source.
//!
//! RLS is intentionally **not** a gate: secondary indexes are RLS-orthogonal
//! (the index only maps value → physical location; row visibility is still
//! enforced by the RLS predicate rewrite at query time), and the user
//! `exec_create_index` path itself imposes no RLS restriction. Auto-indexing an
//! RLS table is therefore correct and beneficial.
//!
//! The "already indexed?" check is performed at **fire time** rather than
//! observe time: the async task reloads the table metadata and skips creation
//! if any index already covers the column. This keeps the hot path free of an
//! index-list scan and naturally races-safely with a concurrent user
//! `CREATE INDEX`. `CREATE INDEX IF NOT EXISTS` is the final idempotency
//! backstop.

use std::collections::HashMap;
use std::sync::RwLock;

use arrow_schema::DataType;
use basin_common::{ProjectId, TableName};
use tracing::info;

use crate::{Engine, ProjectSession};

/// Minimum number of observed non-PK equality-predicate accesses before a
/// column is auto-indexed.
///
/// See the module-level doc for why this is more conservative (8) than JSONB
/// promotion's 3: a secondary index costs storage + perpetual write
/// amplification, whereas a promoted shadow column is effectively free after
/// the first compaction. Named and exported so tests can reference it.
pub const AUTO_INDEX_MIN_HITS: u64 = 8;

/// Environment kill switch. When `BASIN_AUTO_INDEX_DISABLE=1` (or `true`) the
/// advisor records nothing and never fires — operators can disable the
/// self-driving behaviour entirely without a redeploy.
const DISABLE_ENV: &str = "BASIN_AUTO_INDEX_DISABLE";

/// Return `true` when the advisor is disabled via the environment kill switch.
fn advisor_disabled() -> bool {
    matches!(
        std::env::var(DISABLE_ENV).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// The name an auto-created index gets: `auto_idx_<table>_<col>`. The
/// `auto_idx_` prefix lets operators (and tests) distinguish advisor-created
/// indexes from hand-written ones in introspection.
pub fn auto_index_name(table: &TableName, col: &str) -> String {
    format!("auto_idx_{table}_{col}")
}

/// Whether `dt` is a type the secondary-index extractor can key on.
///
/// MUST stay in lock-step with
/// [`crate::secondary_index::extract_entries_from_batch`], which downcasts to
/// `Int64Array`, `UInt64Array`, `Float64Array`, `StringArray` (`Utf8`),
/// `LargeStringArray` (`LargeUtf8`), and `BooleanArray`. Any other type would
/// produce an empty backfill, so we never observe it.
fn is_indexable_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
    )
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Per-column counter + fire-once flag.
#[derive(Default)]
struct ColEntry {
    hits: u64,
    /// True once we have fired the async `CREATE INDEX` for this column.
    fired: bool,
}

/// Per-table registry keyed by column name.
type TableColCounts = HashMap<String, ColEntry>;

/// Process-wide auto-index access tracker.
///
/// Cheap to create (a `RwLock`-guarded `HashMap`). Constructed once inside
/// `crate::EngineInner` and shared via `Arc`.
pub struct IndexAdvisorRegistry {
    inner: RwLock<HashMap<(ProjectId, TableName), TableColCounts>>,
}

impl Default for IndexAdvisorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexAdvisorRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Record one access to `(project, table, column)`.
    ///
    /// Returns `true` when the column just crossed [`AUTO_INDEX_MIN_HITS`] for
    /// the first time — the caller should then fire the async `CREATE INDEX`.
    /// Returns `false` when below threshold or already fired. The `fired` flag
    /// is flipped *inside the write lock* so two threads crossing the threshold
    /// concurrently see exactly one `true`.
    fn record_hit(&self, project: &ProjectId, table: &TableName, col: &str) -> bool {
        let key = (*project, table.clone());
        let mut guard = self
            .inner
            .write()
            .expect("index_advisor write lock poisoned");
        let table_counts = guard.entry(key).or_default();
        let entry = table_counts.entry(col.to_string()).or_default();

        if entry.fired {
            return false;
        }
        entry.hits += 1;
        if entry.hits >= AUTO_INDEX_MIN_HITS {
            entry.fired = true;
            true
        } else {
            false
        }
    }

    // ── Test hooks ──────────────────────────────────────────────────────────

    /// Current hit count for `(project, table, column)`; 0 if never seen.
    pub fn hit_count(&self, project: &ProjectId, table: &TableName, col: &str) -> u64 {
        let guard = self.inner.read().expect("index_advisor read lock poisoned");
        guard
            .get(&(*project, table.clone()))
            .and_then(|t| t.get(col))
            .map(|e| e.hits)
            .unwrap_or(0)
    }

    /// Whether `(project, table, column)` has fired its auto-`CREATE INDEX`.
    pub fn has_fired(&self, project: &ProjectId, table: &TableName, col: &str) -> bool {
        let guard = self.inner.read().expect("index_advisor read lock poisoned");
        guard
            .get(&(*project, table.clone()))
            .and_then(|t| t.get(col))
            .map(|e| e.fired)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Eligible-column extraction
// ---------------------------------------------------------------------------

/// From a slice of `Predicate`s, return the column names that are eligible for
/// auto-indexing: `Eq` predicates on a non-PK, indexable-typed column.
///
/// Kept in this module (not the hook site) so the hook diff in `fast_select` /
/// `executor` is a single call. Deduplicates so a query mentioning the same
/// column twice counts once.
fn eligible_columns<'a>(
    predicates: &'a [basin_storage::Predicate],
    schema: &arrow_schema::Schema,
    pk_columns: &[String],
) -> Vec<&'a str> {
    use basin_storage::Predicate;
    let mut out: Vec<&str> = Vec::new();
    for pred in predicates {
        // Only equality point-lookups benefit from a B-tree point index.
        let col = match pred {
            Predicate::Eq(c, _) => c.as_str(),
            Predicate::Gt(..)
            | Predicate::Lt(..)
            | Predicate::StartsWith { .. }
            | Predicate::InInt64(..) => continue,
        };
        // Skip PK columns — they already have a fast path; an extra index would
        // only add write amplification.
        if pk_columns.iter().any(|pk| pk == col) {
            continue;
        }
        // Skip non-indexable types (the backfill extractor can't key on them).
        let Ok(field) = schema.field_with_name(col) else {
            continue;
        };
        if !is_indexable_type(field.data_type()) {
            continue;
        }
        if !out.contains(&col) {
            out.push(col);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Observe + fire-and-forget trigger
// ---------------------------------------------------------------------------

/// Observe the equality predicates of one served SELECT and, for any eligible
/// `(project, table, column)` that just crossed [`AUTO_INDEX_MIN_HITS`], fire a
/// one-shot `CREATE INDEX IF NOT EXISTS auto_idx_<table>_<col> ON <table>
/// (<col>)` on a detached `tokio` task.
///
/// Designed so the call site is a single line: callers pass the session, the
/// table, the table metadata (for the schema + PK list), and the parsed
/// predicates; all extraction/eligibility logic lives here.
///
/// The spawn is best-effort: if no tokio runtime is current (synchronous unit
/// tests) the registry still flips the fired flag synchronously, so the
/// fire-once accounting is correct either way.
pub fn observe_eq_predicates(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    predicates: &[basin_storage::Predicate],
) {
    // Fast bail-outs before touching the registry lock.
    if advisor_disabled() || predicates.is_empty() {
        return;
    }
    let cols = eligible_columns(predicates, meta.schema.as_ref(), &meta.pk_columns);
    if cols.is_empty() {
        return;
    }

    let registry = sess.engine.index_advisor_registry();
    for col in cols {
        if registry.record_hit(&sess.project, table, col) {
            spawn_auto_create_index(&sess.engine, sess.project, table.clone(), col.to_string());
        }
    }
}

/// Spawn the fire-and-forget `CREATE INDEX`. Re-checks "already indexed" at fire
/// time against fresh catalog metadata and routes through the user
/// `CREATE INDEX` path (via a fresh session) so the catalog write + registry
/// backfill are identical to hand-written DDL.
fn spawn_auto_create_index(engine: &Engine, project: ProjectId, table: TableName, col: String) {
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        // No runtime (sync unit test): the registry already recorded `fired`,
        // so accounting is correct; there is simply nothing to spawn onto.
        return;
    };
    let engine = engine.clone();
    rt.spawn(async move {
        // Fire-time "already indexed?" check against fresh catalog metadata.
        // Cheap, race-safe with a concurrent user CREATE INDEX, and avoids an
        // index-list scan on the hot observe path.
        if let Ok(meta) = engine.config().catalog.load_table(&project, &table).await {
            let already = meta
                .indexes
                .iter()
                .any(|idx| idx.columns.len() == 1 && idx.columns[0] == col);
            if already {
                return;
            }
        }

        let index_name = auto_index_name(&table, &col);
        // Open a short-lived session for the project and run the exact same DDL
        // a user would. `IF NOT EXISTS` is the final idempotency backstop
        // against any race that slipped past the metadata check above.
        let sess = match engine.open_session(project).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    %project, %table, col = %col, err = %e,
                    "auto-index: failed to open session for CREATE INDEX (skipping)"
                );
                return;
            }
        };
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col})"
        );
        match sess.execute(&sql).await {
            Ok(_) => {
                // Observability: a single, greppable line so operators see the
                // self-driving advisor act. `auto_index_created` is the event
                // marker; the counter is the registry's per-column `fired` flag.
                info!(
                    target: "basin::index_advisor",
                    event = "auto_index_created",
                    %project,
                    %table,
                    column = %col,
                    index = %index_name,
                    threshold = AUTO_INDEX_MIN_HITS,
                    "auto-index advisor created a secondary index after {AUTO_INDEX_MIN_HITS} non-PK equality-predicate hits"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "basin::index_advisor",
                    %project, %table, col = %col, index = %index_name, err = %e,
                    "auto-index: CREATE INDEX failed (will not retry this column)"
                );
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use basin_storage::{Predicate, ScalarValue};

    fn project() -> ProjectId {
        ProjectId::new()
    }
    fn table(name: &str) -> TableName {
        TableName::new(name).expect("valid table name")
    }

    // ── registry ──────────────────────────────────────────────────────────

    #[test]
    fn below_threshold_does_not_fire() {
        let reg = IndexAdvisorRegistry::new();
        let p = project();
        let t = table("events");
        for _ in 0..(AUTO_INDEX_MIN_HITS - 1) {
            assert!(!reg.record_hit(&p, &t, "user_id"));
        }
        assert_eq!(reg.hit_count(&p, &t, "user_id"), AUTO_INDEX_MIN_HITS - 1);
        assert!(!reg.has_fired(&p, &t, "user_id"));
    }

    #[test]
    fn at_threshold_fires_exactly_once() {
        let reg = IndexAdvisorRegistry::new();
        let p = project();
        let t = table("events");
        let mut fires = 0u32;
        for _ in 0..(AUTO_INDEX_MIN_HITS * 3) {
            if reg.record_hit(&p, &t, "user_id") {
                fires += 1;
            }
        }
        assert_eq!(fires, 1, "must fire exactly once across the threshold");
        assert!(reg.has_fired(&p, &t, "user_id"));
    }

    #[test]
    fn distinct_columns_tracked_independently() {
        let reg = IndexAdvisorRegistry::new();
        let p = project();
        let t = table("events");
        for _ in 0..AUTO_INDEX_MIN_HITS {
            reg.record_hit(&p, &t, "user_id");
        }
        for _ in 0..(AUTO_INDEX_MIN_HITS - 1) {
            assert!(!reg.record_hit(&p, &t, "tenant_id"));
        }
        assert!(reg.has_fired(&p, &t, "user_id"));
        assert!(!reg.has_fired(&p, &t, "tenant_id"));
    }

    // ── eligibility ─────────────────────────────────────────────────────────

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("score", DataType::Float32, true), // NOT indexable
        ])
    }

    #[test]
    fn eq_on_non_pk_indexable_is_eligible() {
        let s = schema();
        let pk = vec!["id".to_string()];
        let preds = vec![Predicate::Eq("user_id".into(), ScalarValue::Int64(7))];
        assert_eq!(eligible_columns(&preds, &s, &pk), vec!["user_id"]);
    }

    #[test]
    fn eq_on_pk_is_skipped() {
        let s = schema();
        let pk = vec!["id".to_string()];
        let preds = vec![Predicate::Eq("id".into(), ScalarValue::Int64(7))];
        assert!(eligible_columns(&preds, &s, &pk).is_empty());
    }

    #[test]
    fn non_eq_predicates_skipped() {
        let s = schema();
        let pk = vec!["id".to_string()];
        let preds = vec![
            Predicate::Gt("user_id".into(), ScalarValue::Int64(1)),
            Predicate::Lt("user_id".into(), ScalarValue::Int64(9)),
            Predicate::StartsWith {
                column: "email".into(),
                prefix: "a".into(),
                case_insensitive: false,
            },
        ];
        assert!(eligible_columns(&preds, &s, &pk).is_empty());
    }

    #[test]
    fn unindexable_type_skipped() {
        let s = schema();
        let pk = vec!["id".to_string()];
        // Float32 `score` is not in the extractor's supported set.
        let preds = vec![Predicate::Eq("score".into(), ScalarValue::Float64(1.0))];
        assert!(eligible_columns(&preds, &s, &pk).is_empty());
    }

    #[test]
    fn utf8_eq_is_eligible_and_deduped() {
        let s = schema();
        let pk = vec!["id".to_string()];
        let preds = vec![
            Predicate::Eq("email".into(), ScalarValue::Utf8("a@b".into())),
            Predicate::Eq("email".into(), ScalarValue::Utf8("a@b".into())),
        ];
        assert_eq!(eligible_columns(&preds, &s, &pk), vec!["email"]);
    }

    #[test]
    fn unknown_column_skipped() {
        let s = schema();
        let pk = vec!["id".to_string()];
        let preds = vec![Predicate::Eq("nope".into(), ScalarValue::Int64(1))];
        assert!(eligible_columns(&preds, &s, &pk).is_empty());
    }
}
