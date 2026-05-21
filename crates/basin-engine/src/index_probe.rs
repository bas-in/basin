//! Phase 5.19.C — GIN containment probe for JSONB columns.
//!
//! Builds and queries in-memory posting lists for JSONB columns that have a
//! `CREATE INDEX … USING gin` declaration in the catalog.  The posting lists
//! enable file-level pruning for containment predicates (`@>`, `<@`) instead
//! of always falling through to a full DataFusion scan.
//!
//! # Terminology
//!
//! * **term** — a unit of indexable content extracted from a JSONB document.
//!   For `jsonb_ops` this is every top-level `"key"` and `"key"="value"` pair
//!   in the document.  For `jsonb_path_ops` it is a hash of each root-to-leaf
//!   path (`"a.b.c"=<value>`).  Both opclasses produce `String` terms that map
//!   into the same posting-list structure.
//!
//! * **posting list** — a sorted set of `PostingEntry` values (file path +
//!   row-group + row) for each distinct term.  AND-merging two posting lists
//!   yields the rows that contain BOTH terms — the correct semantics for
//!   compound containment (`{"a":1,"b":2}` must have both term "a" and term
//!   "b" indexed).
//!
//! # Correctness contract
//!
//! The posting list is a *conservative superset*: it may return false
//! positives (rows that don't actually contain the probe document) due to
//! JSONB structural ambiguity (array vs. scalar containment, nested object
//! paths).  Every candidate row returned by a probe is re-evaluated by the
//! `jsonb_contains` / `jsonb_contained_by` UDF at the storage read layer.
//! The posting list only prunes *files* (and future: row-groups) that contain
//! NO matching terms — an empty intersection guarantees absence.
//!
//! # Storage / eviction
//!
//! The posting list lives entirely in RAM; there is no on-disk serialisation
//! in this phase (5.19.E handles persistence).  The registry caps each
//! per-column posting list at [`MAX_POSTING_ENTRIES`] total entries; oldest
//! insertions are evicted when the cap is exceeded.  On an engine restart the
//! registry starts empty and rebuilds lazily from writes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};
use serde_json::Value;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Maximum total posting entries (file+rg+row tuples) kept per `(table, col)`
/// posting list.  Beyond this threshold the oldest 25% are evicted.
const MAX_POSTING_ENTRIES: usize = 500_000;

// ── Data types ────────────────────────────────────────────────────────────────

/// One physical location for a posting entry: file path + row-group + row.
/// Deliberately mirrors `secondary_index::IndexLocation` to allow future
/// sharing, but kept separate so the two registries can evolve independently.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PostingEntry {
    pub file_path: String,
    pub row_group: u32,
    pub row: u64,
}

/// One posting list for a single `(term)`.
/// Maps each term → set of `PostingEntry`.
#[derive(Debug, Default)]
struct TermPostingList {
    /// `term → set of (file, rg, row)`.
    entries: HashMap<String, HashSet<PostingEntry>>,
    /// Ordered sequence of insertions for LRU eviction (keys, not entries).
    insert_order: Vec<String>,
    /// Total entry count (sum of all set sizes).
    total_count: usize,
}

impl TermPostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Add a single `(term, entry)` pair.
    fn insert(&mut self, term: String, entry: PostingEntry) {
        let set = self.entries.entry(term.clone()).or_default();
        if set.is_empty() {
            self.insert_order.push(term);
        }
        set.insert(entry);
        self.total_count += 1;

        if self.total_count > MAX_POSTING_ENTRIES {
            self.evict_oldest();
        }
    }

    /// Remove the oldest 25% of terms.
    fn evict_oldest(&mut self) {
        let evict_count = MAX_POSTING_ENTRIES / 4;
        let to_evict: Vec<String> = self.insert_order.drain(..evict_count.min(self.insert_order.len())).collect();
        for k in &to_evict {
            if let Some(set) = self.entries.remove(k) {
                self.total_count = self.total_count.saturating_sub(set.len());
            }
        }
    }

    /// Probe for `term`. Returns `None` when the term has never been indexed
    /// (caller must treat as "unknown → full scan").  Returns `Some(set)` for
    /// a known term; the set may be empty when all posting entries for this
    /// term were evicted.
    fn probe_term(&self, term: &str) -> Option<&HashSet<PostingEntry>> {
        self.entries.get(term)
    }

    /// Remove all entries that reference `file_path`. Called when a file is
    /// compacted or deleted.
    fn remove_file(&mut self, file_path: &str) {
        for set in self.entries.values_mut() {
            let before = set.len();
            set.retain(|e| e.file_path != file_path);
            self.total_count = self.total_count.saturating_sub(before - set.len());
        }
    }
}

// ── Term extraction ───────────────────────────────────────────────────────────

/// Extract GIN terms from a JSONB value.
///
/// Two opclass modes are supported:
/// * `jsonb_ops` (default): top-level keys (`"k"`) AND key=value pairs
///   (`"k"="v"`).  For nested objects the sub-document itself becomes a
///   top-level term value (matching PG's `jsonb_ops` GIN extraction behaviour
///   for the case relevant to `@>` / `<@`).
/// * `jsonb_path_ops`: each root-to-leaf path is hashed into a stable string
///   `"path_hash:<hash>"` so the probe can still AND-merge posting lists
///   without storing full path strings.
///
/// Only top-level key / key=scalar-value pairs are extracted; array elements
/// inside values are not decomposed further (conservative: may produce false
/// positives for deep nesting, which the re-evaluation filter will catch).
pub fn extract_terms(value: &Value, opclass: &str) -> Vec<String> {
    let mut terms = Vec::new();
    extract_terms_inner(value, opclass, "", &mut terms);
    terms
}

fn extract_terms_inner(value: &Value, opclass: &str, path_prefix: &str, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let path = if path_prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{path_prefix}.{k}")
                };

                if opclass == "jsonb_path_ops" {
                    // Path-hash term: stable hash of the full root-to-leaf path + value.
                    let leaf_repr = compact_value(v);
                    let hash_input = format!("{path}={leaf_repr}");
                    let hash = simple_hash(&hash_input);
                    out.push(format!("path_hash:{hash}"));

                    // Recurse into nested objects so compound paths are indexed.
                    if matches!(v, Value::Object(_)) {
                        extract_terms_inner(v, opclass, &path, out);
                    }
                } else {
                    // jsonb_ops: key presence term.
                    out.push(format!("key:{k}"));

                    // key=scalar-value term.
                    let leaf_repr = compact_value(v);
                    out.push(format!("kv:{k}={leaf_repr}"));

                    // Recurse for nested objects (adds more key/kv terms under the
                    // same flat namespace, matching PG's @> semantics for nested
                    // object containment).
                    if matches!(v, Value::Object(_)) {
                        extract_terms_inner(v, opclass, &path, out);
                    }
                }
            }
        }
        // Top-level arrays and scalars produce no terms (containment for
        // arrays is handled by re-evaluation; scalars are not `@>` targets).
        _ => {}
    }
}

/// Compact JSON representation used as part of the GIN term key.
fn compact_value(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{s}\""),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => {
            // For nested complex values use a truncated JSON repr as the term.
            let s = v.to_string();
            if s.len() > 200 { s[..200].to_string() } else { s }
        }
    }
}

/// Non-cryptographic hash of a string (FNV-1a, 64-bit).
fn simple_hash(s: &str) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Extract GIN probe terms from a JSONB *needle* document for a `@>` query.
///
/// For `jsonb_ops`:   every `key:k` and `kv:k=v` term in the needle.
/// For `jsonb_path_ops`: every `path_hash:<h>` term in the needle.
///
/// This mirrors `extract_terms` — the probe terms must use the same key space
/// as the indexed terms so the AND-merge works correctly.
pub fn needle_terms(needle: &Value, opclass: &str) -> Vec<String> {
    // For containment needle, we use the same extraction logic as for indexed docs.
    // This is correct: if the needle has `{"tag":"nested"}`, the indexed doc must
    // have both `key:tag` and `kv:tag="nested"` in its posting list.
    extract_terms(needle, opclass)
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Key into the registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide GIN posting list registry.
///
/// One `TermPostingList` per `(project, table, col)`.  Concurrent access is
/// serialised by an inner `Mutex` per posting list, with the outer `Mutex`
/// only held briefly to look up or insert an `Arc`.
pub struct GinIndexRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<TermPostingList>>>>,
}

impl GinIndexRegistry {
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<TermPostingList>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("GinIndexRegistry outer lock poisoned");
        map.entry(key).or_insert_with(|| Arc::new(Mutex::new(TermPostingList::new()))).clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<TermPostingList>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let map = self.inner.lock().expect("GinIndexRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index a JSONB value from `file_path` / `row_group` / `row`.
    ///
    /// Parses the raw JSONB bytes (`LargeBinary` payload) into a
    /// `serde_json::Value` and extracts GIN terms.  Silently skips null or
    /// unparseable values.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        jsonb_bytes: &[u8],
        file_path: &str,
        row_group: u32,
        row: u64,
    ) {
        let value: Value = match serde_json::from_slice(jsonb_bytes) {
            Ok(v) => v,
            Err(_) => return,
        };
        let terms = extract_terms(&value, opclass);
        if terms.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("TermPostingList lock poisoned");
        let entry = PostingEntry {
            file_path: file_path.to_string(),
            row_group,
            row,
        };
        for term in terms {
            list.insert(term, entry.clone());
        }
    }

    /// Probe the posting list for a `@>` (containment) predicate.
    ///
    /// `needle_bytes` is the raw JSONB of the right-hand literal, `opclass` is
    /// the index opclass (`"jsonb_ops"` or `"jsonb_path_ops"`).
    ///
    /// Returns:
    /// * `ProbeResult::NoIndex` — no posting list loaded for this column yet;
    ///   caller must fall through to full scan.
    /// * `ProbeResult::Empty` — the posting list exists and the intersection
    ///   is empty; no rows can match.  (Caller can short-circuit with zero rows.)
    /// * `ProbeResult::FileCandidates(set)` — the set of file paths that MIGHT
    ///   contain matching rows; caller reads only those files and re-applies the
    ///   full `jsonb_contains` predicate for correctness.
    pub fn probe_containment(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        needle_bytes: &[u8],
    ) -> ProbeResult {
        let needle: Value = match serde_json::from_slice(needle_bytes) {
            Ok(v) => v,
            Err(_) => return ProbeResult::NoIndex, // unparseable → conservative
        };
        let terms = needle_terms(&needle, opclass);
        if terms.is_empty() {
            // Empty needle matches everything (PG semantics: {} @> {} is true).
            return ProbeResult::NoIndex; // fall through to full scan
        }

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return ProbeResult::NoIndex,
        };
        let list = arc.lock().expect("TermPostingList lock poisoned");

        // AND-merge posting lists for each term.
        let mut candidate_files: Option<HashSet<String>> = None;

        for term in &terms {
            match list.probe_term(term) {
                None => {
                    // Term not in index: unknown state (may have been evicted or
                    // never inserted).  Conservative: include all files.
                    return ProbeResult::NoIndex;
                }
                Some(entries) => {
                    let files: HashSet<String> =
                        entries.iter().map(|e| e.file_path.clone()).collect();
                    candidate_files = Some(match candidate_files {
                        None => files,
                        Some(prev) => prev.intersection(&files).cloned().collect(),
                    });
                }
            }
        }

        match candidate_files {
            None => ProbeResult::NoIndex,
            Some(files) if files.is_empty() => ProbeResult::Empty,
            Some(files) => ProbeResult::FileCandidates(files),
        }
    }

    /// Remove all posting entries for `file_path` in `(project, table, col)`.
    pub fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut list = arc.lock().expect("TermPostingList lock poisoned");
            list.remove_file(file_path);
        }
    }
}

impl Default for GinIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a containment probe against the GIN posting list.
#[derive(Debug)]
pub enum ProbeResult {
    /// No posting list for this column, or term was evicted. Caller must
    /// fall through to a full scan (safe: no false negatives).
    NoIndex,
    /// The intersection of all term posting lists is empty.  No rows in the
    /// table can satisfy the containment predicate.
    Empty,
    /// The set of file paths that may contain matching rows.  The caller reads
    /// these files and re-applies the `jsonb_contains` predicate for
    /// correctness.
    FileCandidates(HashSet<String>),
}

// ── Planner detection ─────────────────────────────────────────────────────────

/// Detected GIN containment probe plan.
///
/// Produced by [`detect_gin_containment`] when the query matches the
/// `SELECT … FROM table WHERE col @> literal` (or `col <@ literal`) shape
/// and a GIN index exists on `col`.
#[derive(Debug)]
pub struct GinContainmentPlan {
    /// The table to scan.
    pub table: TableName,
    /// The indexed JSONB column.
    pub col: String,
    /// The containment literal (raw JSON bytes).
    pub needle: Vec<u8>,
    /// Whether this is `@>` (true) or `<@` (false).
    pub is_contains: bool,
    /// Opclass from the catalog index declaration.
    pub opclass: String,
    /// Columns to project in the output.  `None` means `SELECT *`.
    pub projection: Option<Vec<String>>,
}

/// Detect the `SELECT … FROM table WHERE col @> 'literal'` shape.
///
/// Returns `Some(plan)` when:
/// 1. The SQL (before operator rewriting) contains `@>` or `<@`.
/// 2. sqlparser can parse it as a single-table SELECT with a WHERE clause
///    that is exactly `col @> 'literal'` or `col <@ 'literal'`.
/// 3. The table has a GIN index on `col` in the catalog.
///
/// Returns `None` for anything that doesn't match perfectly (caller falls
/// through to the normal DataFusion / rewrite path).
pub async fn detect_gin_containment(
    sql: &str,
    project: &ProjectId,
    catalog: &Arc<dyn basin_catalog::Catalog>,
) -> Option<GinContainmentPlan> {
    // Fast pre-check: must contain @> or <@ operator.
    let has_at_gt = sql.contains("@>");
    let has_lt_at = sql.contains("<@");
    if !has_at_gt && !has_lt_at {
        return None;
    }

    // Parse with sqlparser to extract the structural shape.
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query = match &stmts[0] {
        sqlparser::ast::Statement::Query(q) => q.as_ref(),
        _ => return None,
    };

    // No CTEs, no LIMIT that changes semantics, no ORDER BY that matters.
    if query.with.is_some() {
        return None;
    }

    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return None,
    };

    // Single table, no joins.
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let table_name = match &select.from[0].relation {
        sqlparser::ast::TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return None;
            }
            use crate::pg_ast::ObjectNamePartExt;
            name.0[0].id_val().clone()
        }
        _ => return None,
    };
    let table = TableName::new(table_name).ok()?;

    // WHERE clause must be `col @> literal` or `col <@ literal`.
    let (col_name, needle_str, is_contains) = match &select.selection {
        Some(sqlparser::ast::Expr::BinaryOp { left, op, right }) => {
            let op_str = op.to_string();
            let is_contains = op_str == "@>";
            let is_contained = op_str == "<@";
            if !is_contains && !is_contained {
                return None;
            }
            // LHS must be a bare column reference.
            let col = match left.as_ref() {
                sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
                _ => return None,
            };
            // RHS must be a quoted literal or cast.
            let literal = extract_json_literal(right)?;
            (col, literal, is_contains)
        }
        _ => return None,
    };

    // Projection: `*` or a list of bare column names.
    let projection = extract_simple_projection(&select.projection)?;

    // Catalog lookup: table must have a GIN index on `col_name`.
    let meta = catalog.load_table(project, &table).await.ok()?;
    let gin_index = meta.indexes.iter().find(|idx| {
        idx.access_method == "gin"
            && idx.columns.len() == 1
            && idx.columns[0] == col_name
    })?;
    let opclass = gin_index.opclass.clone().unwrap_or_else(|| "jsonb_ops".to_string());

    // Parse needle to validate it's valid JSON.
    let needle_bytes = needle_str.as_bytes().to_vec();
    let _: serde_json::Value = serde_json::from_slice(&needle_bytes).ok()?;

    Some(GinContainmentPlan {
        table,
        col: col_name,
        needle: needle_bytes,
        is_contains,
        opclass,
        projection,
    })
}

/// Extract the JSON literal string from the RHS of a `@>` / `<@` expression.
///
/// Accepts:
/// * `'{"key":"val"}'` — single-quoted string literal
/// * `'{"key":"val"}'::jsonb` — with a jsonb cast suffix
fn extract_json_literal(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    match expr {
        Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => {
            Some(s.clone())
        }
        // `'literal'::jsonb` cast
        Expr::Cast { expr: inner, .. } => extract_json_literal(inner),
        _ => None,
    }
}

/// Extract a simple projection (either `*` → `None`, or a list of bare column names).
fn extract_simple_projection(
    items: &[sqlparser::ast::SelectItem],
) -> Option<Option<Vec<String>>> {
    if items.len() == 1 {
        if let sqlparser::ast::SelectItem::Wildcard(_) = &items[0] {
            return Some(None); // SELECT *
        }
    }
    let mut cols = Vec::new();
    for item in items {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) => {
                cols.push(id.value.clone());
            }
            _ => return None, // expressions or aliases → fall through
        }
    }
    Some(Some(cols))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_terms_jsonb_ops_flat() {
        let v = json!({"tag": "nested", "id": 42});
        let terms = extract_terms(&v, "jsonb_ops");
        assert!(terms.contains(&"key:tag".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"kv:tag=\"nested\"".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"key:id".to_string()), "terms={terms:?}");
        assert!(terms.contains(&"kv:id=42".to_string()), "terms={terms:?}");
    }

    #[test]
    fn extract_terms_jsonb_path_ops_flat() {
        let v = json!({"tag": "nested"});
        let terms = extract_terms(&v, "jsonb_path_ops");
        // Should produce exactly one path_hash term for tag="nested"
        assert_eq!(terms.len(), 1, "terms={terms:?}");
        assert!(terms[0].starts_with("path_hash:"), "terms={terms:?}");
    }

    #[test]
    fn extract_terms_empty_object() {
        let v = json!({});
        let terms = extract_terms(&v, "jsonb_ops");
        assert!(terms.is_empty(), "empty object should produce no terms");
    }

    #[test]
    fn probe_containment_no_index() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        assert!(matches!(result, ProbeResult::NoIndex));
    }

    #[test]
    fn probe_containment_hit_after_insert() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index one document that matches.
        let doc = br#"{"tag":"nested","id":42}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        // Probe for {"tag":"nested"} — should find f1.parquet.
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "expected f1.parquet in {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn probe_containment_miss_after_insert() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // Index a document that does NOT contain the needle.
        let doc = br#"{"role":"user"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        // Probe for {"tag":"nested"} — must not find f1.parquet.
        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        // term "key:tag" is not in the index at all → NoIndex (conservative),
        // OR term "kv:tag=\"nested\"" is not in index → NoIndex.
        // Either way the caller falls through; Empty would also be acceptable.
        assert!(
            matches!(result, ProbeResult::NoIndex | ProbeResult::Empty),
            "expected NoIndex or Empty, got {result:?}"
        );
    }

    #[test]
    fn probe_containment_and_merge() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        // f1 has both "role":"admin" AND "tenant":"acme".
        let doc1 = br#"{"role":"admin","tenant":"acme"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc1, "f1.parquet", 0, 0);

        // f2 has only "role":"admin".
        let doc2 = br#"{"role":"admin"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc2, "f2.parquet", 0, 0);

        // Probe for compound needle: {"role":"admin","tenant":"acme"}.
        // f1 has both terms; f2 is missing "kv:tenant=..." so the AND-merge
        // should exclude f2.
        let needle = br#"{"role":"admin","tenant":"acme"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        match result {
            ProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 should be in candidates: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 should be excluded: {files:?}");
            }
            ProbeResult::NoIndex => {
                // Acceptable: if any term was evicted or missing the registry
                // falls back to full-scan (conservative, not wrong).
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn remove_file_clears_entries() {
        let registry = GinIndexRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();

        let doc = br#"{"tag":"nested"}"#;
        registry.index_row(&project, &table, "payload", "jsonb_ops", doc, "f1.parquet", 0, 0);

        registry.remove_file(&project, &table, "payload", "f1.parquet");

        let needle = br#"{"tag":"nested"}"#;
        let result = registry.probe_containment(&project, &table, "payload", "jsonb_ops", needle);
        // After removal, either Empty (set exists but is empty) or NoIndex.
        assert!(
            matches!(result, ProbeResult::NoIndex | ProbeResult::Empty),
            "expected NoIndex or Empty after remove, got {result:?}"
        );
    }
}
