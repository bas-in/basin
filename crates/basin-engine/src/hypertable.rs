//! Phase 5.29.B-D — TimescaleDB-style hypertable registry.
//!
//! ## Design
//!
//! Hypertables are a *convention layer* on top of Basin's existing table
//! storage. No new storage engine is introduced:
//!
//! * The base table is a normal Basin table whose rows all live in the same
//!   flat catalog entry.
//! * This registry tracks which tables have been converted to hypertables
//!   (their time column and chunk interval) and which logical *chunks* exist
//!   (their time ranges).  Chunks are metadata-only — they do not have
//!   separate catalog entries; all rows live in the base table.
//! * `timescaledb_information.chunks` is served from this registry as a
//!   virtual provider (see [`ChunksProvider`]).
//!
//! ## Multi-tenant isolation
//!
//! The registry is a `DashMap<ProjectId, Arc<Mutex<ProjectHypertables>>>`.
//! Each project pays only the cost of its own entry; an idle project is a
//! single `Arc` pointer in the outer map.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::Mutex;

use basin_common::ProjectId;

// ─── per-table chunk record ───────────────────────────────────────────────────

/// One logical time-bucket chunk.  All rows in this range live in the base
/// table; the chunk is purely a metadata / pruning record.
#[derive(Clone, Debug)]
pub(crate) struct ChunkRecord {
    /// Auto-incremented chunk name within the table (e.g. `_hyper_1_1_chunk`).
    pub(crate) chunk_name: String,
    /// Start of the time range (inclusive).
    pub(crate) range_start: DateTime<Utc>,
    /// End of the time range (exclusive).
    pub(crate) range_end: DateTime<Utc>,
    /// True after `compress_chunk()` has been called on this chunk.
    pub(crate) is_compressed: bool,
}

// ─── per-table hypertable record ─────────────────────────────────────────────

/// Metadata for one converted hypertable.
#[derive(Clone, Debug)]
pub(crate) struct HypertableRecord {
    /// The time-partitioning column (e.g. `ts`).
    pub(crate) time_column: String,
    /// Chunk interval in seconds (e.g. 86 400 for 1 day).
    pub(crate) chunk_interval_secs: u64,
    /// Retention policy: drop chunks whose range_end is more than this many
    /// seconds before `now()`.  `None` means no policy.
    pub(crate) retention_secs: Option<u64>,
    /// All chunks that have been created for this table.
    pub(crate) chunks: Vec<ChunkRecord>,
    /// Sequence counter used when naming new chunks.
    pub(crate) next_chunk_id: u64,
}

impl HypertableRecord {
    pub(crate) fn new(time_column: String, chunk_interval_secs: u64) -> Self {
        Self {
            time_column,
            chunk_interval_secs,
            retention_secs: None,
            chunks: Vec::new(),
            next_chunk_id: 1,
        }
    }

    /// Return (or lazily create) the chunk that covers `ts`.  Updates
    /// `self.chunks` in place and returns a reference to the relevant chunk.
    pub(crate) fn ensure_chunk_for(&mut self, ts: DateTime<Utc>) -> &ChunkRecord {
        let secs = self.chunk_interval_secs as i64;
        let epoch = ts.timestamp();
        // Floor to the nearest chunk boundary (Unix-epoch aligned).
        let floored = epoch - epoch.rem_euclid(secs);
        let start = DateTime::<Utc>::from_timestamp(floored, 0).unwrap_or(ts);
        let end = DateTime::<Utc>::from_timestamp(floored + secs, 0).unwrap_or(ts);

        // Check if this chunk already exists.
        if let Some(i) = self.chunks.iter().position(|c| c.range_start == start) {
            return &self.chunks[i];
        }

        // Create a new chunk record.
        let id = self.next_chunk_id;
        self.next_chunk_id += 1;
        let chunk = ChunkRecord {
            chunk_name: format!("_hyper_1_{id}_chunk"),
            range_start: start,
            range_end: end,
            is_compressed: false,
        };
        self.chunks.push(chunk);
        self.chunks.last().unwrap()
    }

    /// Return chunk records whose range is entirely before `cutoff`.
    pub(crate) fn chunks_older_than(&self, cutoff: DateTime<Utc>) -> Vec<String> {
        self.chunks
            .iter()
            .filter(|c| c.range_end <= cutoff)
            .map(|c| c.chunk_name.clone())
            .collect()
    }

    /// Drop chunks by name.  Called by the retention executor.
    pub(crate) fn drop_chunks(&mut self, names: &[String]) {
        self.chunks.retain(|c| !names.contains(&c.chunk_name));
    }

    /// Mark a chunk compressed.
    pub(crate) fn mark_compressed(&mut self, chunk_name: &str) -> bool {
        if let Some(c) = self.chunks.iter_mut().find(|c| c.chunk_name == chunk_name) {
            c.is_compressed = true;
            true
        } else {
            false
        }
    }
}

// ─── per-project container ───────────────────────────────────────────────────

#[derive(Default, Debug)]
pub(crate) struct ProjectHypertables {
    /// keyed by bare table name (no schema prefix).
    pub(crate) tables: HashMap<String, HypertableRecord>,
}

// ─── process-wide registry ───────────────────────────────────────────────────

/// Process-wide hypertable registry.  Shared via `Arc` from `EngineInner`.
#[derive(Default, Debug)]
pub(crate) struct HypertableRegistry {
    inner: DashMap<ProjectId, Arc<Mutex<ProjectHypertables>>>,
}

impl HypertableRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Return the per-project container, creating it on first access.
    fn project(&self, project: &ProjectId) -> Arc<Mutex<ProjectHypertables>> {
        self.inner
            .entry(*project)
            .or_insert_with(|| Arc::new(Mutex::new(ProjectHypertables::default())))
            .clone()
    }

    /// Register a table as a hypertable.  Returns `Err` if already registered.
    pub(crate) async fn register(
        &self,
        project: &ProjectId,
        table: &str,
        time_column: String,
        chunk_interval_secs: u64,
    ) -> Result<(), String> {
        let p = self.project(project);
        let mut guard = p.lock().await;
        if guard.tables.contains_key(table) {
            return Err(format!("table {table:?} is already a hypertable"));
        }
        guard.tables.insert(
            table.to_string(),
            HypertableRecord::new(time_column, chunk_interval_secs),
        );
        Ok(())
    }

    /// Ensure the chunk covering `ts` exists.  Returns the chunk's name.
    ///
    /// No-op (silently succeeds) if the table is not a hypertable — ordinary
    /// tables just accumulate all data without partitioning.
    pub(crate) async fn touch_chunk(
        &self,
        project: &ProjectId,
        table: &str,
        ts: DateTime<Utc>,
    ) -> Option<String> {
        let p = self.project(project);
        let mut guard = p.lock().await;
        let ht = guard.tables.get_mut(table)?;
        let chunk = ht.ensure_chunk_for(ts);
        Some(chunk.chunk_name.clone())
    }

    /// True if `table` is a registered hypertable for this project.
    pub(crate) async fn is_hypertable(&self, project: &ProjectId, table: &str) -> bool {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return false;
        };
        let guard = arc.lock().await;
        guard.tables.contains_key(table)
    }

    /// Set a retention policy on a hypertable.
    pub(crate) async fn set_retention(
        &self,
        project: &ProjectId,
        table: &str,
        retention_secs: u64,
    ) -> Result<(), String> {
        let p = self.project(project);
        let mut guard = p.lock().await;
        let ht = guard
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("table {table:?} is not a hypertable"))?;
        ht.retention_secs = Some(retention_secs);
        Ok(())
    }

    /// Run the retention policy: drop chunks older than `retention_secs` before
    /// `now`.  Returns the names of dropped chunks.
    ///
    /// NOTE: the executor now calls `drop_chunks_before` directly (which shares
    /// the same logic). This method is retained for any caller that holds a
    /// direct reference to it.
    #[allow(dead_code)]
    pub(crate) async fn run_retention(
        &self,
        project: &ProjectId,
        table: &str,
    ) -> Result<Vec<String>, String> {
        let p = self.project(project);
        let mut guard = p.lock().await;
        let ht = guard
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("table {table:?} is not a hypertable"))?;
        let secs = match ht.retention_secs {
            Some(s) => s,
            None => return Ok(vec![]), // no policy
        };
        let cutoff = Utc::now() - chrono::Duration::seconds(secs as i64);
        let to_drop = ht.chunks_older_than(cutoff);
        ht.drop_chunks(&to_drop);
        Ok(to_drop)
    }

    /// Snapshot chunk metadata for a given table (for the virtual view).
    pub(crate) async fn snapshot_chunks(
        &self,
        project: &ProjectId,
        table: &str,
    ) -> Vec<ChunkRecord> {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return vec![];
        };
        let guard = arc.lock().await;
        guard
            .tables
            .get(table)
            .map(|ht| ht.chunks.clone())
            .unwrap_or_default()
    }

    /// Snapshot all chunks for a project (for `WHERE hypertable_name = '...'`
    /// or an unfiltered scan).
    pub(crate) async fn snapshot_all_chunks(
        &self,
        project: &ProjectId,
    ) -> Vec<(String, ChunkRecord)> {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return vec![];
        };
        let guard = arc.lock().await;
        let mut out = Vec::new();
        for (tname, ht) in &guard.tables {
            for c in &ht.chunks {
                out.push((tname.clone(), c.clone()));
            }
        }
        out
    }

    /// Mark a chunk compressed by name (returns `false` if not found).
    pub(crate) async fn compress_chunk(
        &self,
        project: &ProjectId,
        chunk_name: &str,
    ) -> bool {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return false;
        };
        let mut guard = arc.lock().await;
        for ht in guard.tables.values_mut() {
            if ht.mark_compressed(chunk_name) {
                return true;
            }
        }
        false
    }

    /// Return the time column for a hypertable (for INSERT routing).
    pub(crate) async fn time_column(
        &self,
        project: &ProjectId,
        table: &str,
    ) -> Option<String> {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return None;
        };
        let guard = arc.lock().await;
        guard.tables.get(table).map(|ht| ht.time_column.clone())
    }

    /// Return the retention window in seconds for a hypertable, if set.
    pub(crate) async fn retention_secs(
        &self,
        project: &ProjectId,
        table: &str,
    ) -> Option<u64> {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return None;
        };
        let guard = arc.lock().await;
        guard.tables.get(table).and_then(|ht| ht.retention_secs)
    }

    /// Drop chunks whose range_end is <= `cutoff` and return their names plus
    /// the cutoff used (so the caller can issue a physical DELETE).
    ///
    /// This is the core shared by both `drop_chunks()` and `run_retention_policy`.
    pub(crate) async fn drop_chunks_before(
        &self,
        project: &ProjectId,
        table: &str,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<String>, String> {
        let p = self.project(project);
        let mut guard = p.lock().await;
        let ht = guard
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("table {table:?} is not a hypertable"))?;
        let to_drop = ht.chunks_older_than(cutoff);
        ht.drop_chunks(&to_drop);
        Ok(to_drop)
    }

    /// Snapshot all hypertable records for a project (for the `hypertables`
    /// catalog view).
    pub(crate) async fn snapshot_hypertables(
        &self,
        project: &ProjectId,
    ) -> Vec<(String, HypertableRecord)> {
        let Some(arc) = self.inner.get(project).map(|e| e.clone()) else {
            return vec![];
        };
        let guard = arc.lock().await;
        guard.tables.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

// ─── SQL parsing helpers ──────────────────────────────────────────────────────

/// Parse `INTERVAL '<n> <unit>'` text → seconds.  Understands `day/days`,
/// `hour/hours`, `minute/minutes`, `second/seconds`, `week/weeks`.
pub(crate) fn parse_interval_secs(s: &str) -> Option<u64> {
    // Prefer the multiplier-aware `<n> <unit>` parser FIRST: it correctly
    // computes `n × unit` (e.g. `'30 days'` → 2_592_000). The
    // `BucketWidth::from_interval_text` named-unit matcher below DROPS the
    // quantity (`'30 days'`.contains("day") → BucketWidth::Day → 86_400), which
    // is fine for a single-unit chunk bucket but wrong for a retention window.
    parse_interval_secs_fallback(s).or_else(|| {
        crate::cv_time_bucket::BucketWidth::from_interval_text(s).map(|w| match w {
            crate::cv_time_bucket::BucketWidth::Second => 1,
            crate::cv_time_bucket::BucketWidth::Minute => 60,
            crate::cv_time_bucket::BucketWidth::Hour => 3_600,
            crate::cv_time_bucket::BucketWidth::Day => 86_400,
            crate::cv_time_bucket::BucketWidth::Week => 604_800,
            crate::cv_time_bucket::BucketWidth::Month => 30 * 86_400,
            crate::cv_time_bucket::BucketWidth::Year => 365 * 86_400,
            crate::cv_time_bucket::BucketWidth::SecondsFixed(s) => s,
        })
    })
}

/// Fallback parser for intervals not covered by `BucketWidth`.
fn parse_interval_secs_fallback(s: &str) -> Option<u64> {
    let s = s.trim();
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut total: u64 = 0;
    let mut saw = false;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
        if i >= bytes.len() { break; }
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() { i += 1; }
        if i == num_start { return None; }
        let n: u64 = s[num_start..i].parse().ok()?;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() { i += 1; }
        let unit_start = i;
        while i < bytes.len() && bytes[i].is_ascii_alphabetic() { i += 1; }
        let unit = s[unit_start..i].to_ascii_lowercase();
        let factor: u64 = match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => 1,
            "m" | "min" | "mins" | "minute" | "minutes" => 60,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3_600,
            "d" | "day" | "days" => 86_400,
            "w" | "week" | "weeks" => 604_800,
            _ => return None,
        };
        total = total.checked_add(n.checked_mul(factor)?)?;
        saw = true;
    }
    if saw { Some(total) } else { None }
}

// ─── SQL text matchers ────────────────────────────────────────────────────────

/// Match `SELECT create_hypertable('table', 'col', chunk_time_interval =>
/// INTERVAL '1 day')` (case-insensitive, whitespace-tolerant).
///
/// Returns `(table_name, time_column, interval_text)` on success.
pub(crate) fn match_create_hypertable(sql: &str) -> Option<(String, String, String)> {
    let lower = sql.trim().to_ascii_lowercase();
    // Must start with SELECT
    if !lower.starts_with("select") {
        return None;
    }
    // Must contain create_hypertable
    if !lower.contains("create_hypertable") {
        return None;
    }

    // Extract the argument list between the outer parens of create_hypertable(...)
    let start = lower.find("create_hypertable")? + "create_hypertable".len();
    let rest_lower = lower[start..].trim_start();
    if !rest_lower.starts_with('(') {
        return None;
    }
    // Find the original-case args by locating create_hypertable in the original sql.
    let orig_start = sql.to_ascii_lowercase().find("create_hypertable")? + "create_hypertable".len();
    let original_args = paren_contents(&sql[orig_start..])?;

    // Parse comma-separated args (simple: split on commas not inside quotes)
    let parts = split_args(original_args);
    if parts.len() < 2 {
        return None;
    }
    let table = strip_quotes(parts[0].trim());
    let col = strip_quotes(parts[1].trim());

    // Find chunk_time_interval value — look for INTERVAL '...'
    let full = parts.join(",");
    let full_lower = full.to_ascii_lowercase();
    let interval_text = extract_interval_from_args(&full_lower, &full)?;

    Some((table, col, interval_text))
}

/// Match `SELECT add_retention_policy('table', INTERVAL '30 days')`.
/// Returns `(table_name, interval_text)`.
pub(crate) fn match_add_retention_policy(sql: &str) -> Option<(String, String)> {
    let lower = sql.trim().to_ascii_lowercase();
    if !lower.starts_with("select") { return None; }
    if !lower.contains("add_retention_policy") { return None; }

    let start = sql.find("add_retention_policy")
        .or_else(|| sql.to_ascii_lowercase().find("add_retention_policy"))?
        + "add_retention_policy".len();
    let args = paren_contents(&sql[start..])?;
    let parts = split_args(args);
    if parts.len() < 2 { return None; }
    let table = strip_quotes(parts[0].trim());
    let rest = parts[1..].join(",");
    let rest_lower = rest.to_ascii_lowercase();
    let interval_text = extract_interval_from_args(&rest_lower, &rest)?;
    Some((table, interval_text))
}

/// Match `SELECT drop_chunks('table', older_than => INTERVAL '...')` or
/// `SELECT drop_chunks('table', older_than => TIMESTAMP '...')`.
/// Returns `(table_name, cutoff_spec)` where `cutoff_spec` is either an
/// interval text (e.g. `"7 days"`) or an ISO timestamp string.
///
/// Timescale's actual signature also accepts `newer_than`; we support only
/// `older_than` (the overwhelmingly dominant use-case).
pub(crate) fn match_drop_chunks(sql: &str) -> Option<(String, DropChunksCutoff)> {
    let lower = sql.trim().to_ascii_lowercase();
    if !lower.starts_with("select") { return None; }
    if !lower.contains("drop_chunks") { return None; }

    let start = sql.to_ascii_lowercase().find("drop_chunks")? + "drop_chunks".len();
    let args = paren_contents(&sql[start..])?;
    let parts = split_args(args);
    if parts.is_empty() { return None; }
    let table = strip_quotes(parts[0].trim());

    // Look for `older_than => INTERVAL '...'`
    let rest = parts[1..].join(",");
    let rest_lower = rest.to_ascii_lowercase();
    if let Some(iv) = extract_interval_from_args(&rest_lower, &rest) {
        return Some((table, DropChunksCutoff::Interval(iv)));
    }
    // Look for `older_than => TIMESTAMP '...'` / `older_than => TIMESTAMPTZ '...'`
    for kw in &["timestamptz", "timestamp"] {
        if let Some(pos) = rest_lower.find(kw) {
            let after = rest[pos + kw.len()..].trim_start();
            if after.starts_with('\'') {
                let inner = &after[1..];
                if let Some(end) = inner.find('\'') {
                    return Some((table, DropChunksCutoff::Timestamp(inner[..end].to_string())));
                }
            }
        }
    }
    None
}

/// Cutoff specification for `drop_chunks`.
#[derive(Debug, Clone)]
pub(crate) enum DropChunksCutoff {
    /// `older_than => INTERVAL '7 days'` — relative to now().
    Interval(String),
    /// `older_than => TIMESTAMP '2024-01-01'` — absolute cutoff.
    Timestamp(String),
}

/// Match `SELECT run_retention_policy('table')`.
pub(crate) fn match_run_retention_policy(sql: &str) -> Option<String> {
    let lower = sql.trim().to_ascii_lowercase();
    if !lower.starts_with("select") { return None; }
    if !lower.contains("run_retention_policy") { return None; }

    let start = sql.find("run_retention_policy")
        .or_else(|| sql.to_ascii_lowercase().find("run_retention_policy"))?
        + "run_retention_policy".len();
    let args = paren_contents(&sql[start..])?;
    let parts = split_args(args);
    if parts.is_empty() { return None; }
    Some(strip_quotes(parts[0].trim()))
}

/// Match `SELECT compress_chunk('schema.chunk_name')` or
/// `SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name)` — the
/// SELECT-from-chunks form.  For the literal form, return the bare chunk name.
/// For the subquery form, return a sentinel that the executor recognises.
pub(crate) fn match_compress_chunk(sql: &str) -> Option<CompressChunkIntent> {
    let lower = sql.trim().to_ascii_lowercase();
    if !lower.starts_with("select") { return None; }
    if !lower.contains("compress_chunk") { return None; }

    // Check for the subquery form: compress_chunk(c.chunk_schema || '.' || c.chunk_name)
    // followed by FROM timescaledb_information.chunks
    if lower.contains("timescaledb_information") && lower.contains("from") {
        // Extract the hypertable_name from the WHERE clause
        if let Some(ht) = extract_where_eq_value(&lower, "hypertable_name") {
            return Some(CompressChunkIntent::AllForTable {
                hypertable_name: ht,
                before_ts: extract_where_lt_value(&lower, "range_end"),
            });
        }
        return Some(CompressChunkIntent::AllUnfiltered);
    }

    // Literal form: compress_chunk('public._hyper_1_1_chunk')
    let start = sql.to_ascii_lowercase().find("compress_chunk")? + "compress_chunk".len();
    let args = paren_contents(&sql[start..])?;
    let parts = split_args(args);
    if parts.is_empty() { return None; }
    let raw = strip_quotes(parts[0].trim());
    // strip schema prefix if present
    let chunk_name = raw.split('.').last().unwrap_or(&raw).to_string();
    Some(CompressChunkIntent::Named { chunk_name })
}

/// Match `ALTER TABLE ... SET (timescaledb.compress ...)` — accepted as a noop.
pub(crate) fn match_alter_table_timescaledb_compress(sql: &str) -> bool {
    let lower = sql.trim().to_ascii_lowercase();
    lower.starts_with("alter table") && lower.contains("timescaledb.compress")
}

/// Intent for a `compress_chunk` call.
#[derive(Debug)]
pub(crate) enum CompressChunkIntent {
    Named { chunk_name: String },
    AllForTable { hypertable_name: String, before_ts: Option<String> },
    AllUnfiltered,
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Extract the text inside the outermost `(...)` of `s`.  `s` must start
/// with (optional whitespace then) `(`.
fn paren_contents(s: &str) -> Option<&str> {
    let s = s.trim_start();
    if !s.starts_with('(') { return None; }
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut open_pos = None;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                if depth == 1 { open_pos = Some(i); }
            }
            b')' => {
                if depth == 0 { break; }
                depth -= 1;
                if depth == 0 {
                    let start = open_pos? + 1;
                    return Some(&s[start..i]);
                }
            }
            b'\'' => {
                // skip string literal
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

/// Split `s` on top-level commas (not inside parens or quotes).
fn split_args(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => { if depth > 0 { depth -= 1; } }
            b'\'' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'\'' { i += 1; }
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

/// Strip surrounding single or double quotes from a string.
fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Find `INTERVAL '...'` in the args string and return the interval text.
/// Skips occurrences where `interval` is part of a longer identifier
/// (e.g. `chunk_time_interval`).
fn extract_interval_from_args(args_lower: &str, args_original: &str) -> Option<String> {
    let mut search_from = 0usize;
    loop {
        let rel = args_lower[search_from..].find("interval")?;
        let abs_pos = search_from + rel;
        // Check word boundary: the character before `interval` must not be
        // alphanumeric or `_`.
        let pre_ok = abs_pos == 0 || {
            let prev = args_lower.as_bytes()[abs_pos - 1];
            !(prev.is_ascii_alphanumeric() || prev == b'_')
        };
        let abs_end = abs_pos + "interval".len();
        let post_ok = abs_end >= args_lower.len() || {
            let next = args_lower.as_bytes()[abs_end];
            !(next.is_ascii_alphanumeric() || next == b'_')
        };
        if pre_ok && post_ok {
            // Found a standalone `interval` keyword.
            let rest_lower = args_lower[abs_end..].trim_start();
            let rest_orig = args_original[abs_end..].trim_start_matches(|c: char| c.is_whitespace());
            if rest_lower.starts_with('\'') {
                let inner_orig = &rest_orig[1..];
                let end2 = inner_orig.find('\'')?;
                return Some(inner_orig[..end2].to_string());
            }
            // INTERVAL without a quoted literal — skip and keep searching.
        }
        search_from = abs_pos + 1;
    }
}

/// Find `WHERE <col> = '<value>'` and return value.
fn extract_where_eq_value(lower: &str, col: &str) -> Option<String> {
    let pat = format!("{col} = '");
    let pos = lower.find(&pat)?;
    let rest = &lower[pos + pat.len()..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Find `WHERE range_end < '<value>'` and return value string.
fn extract_where_lt_value(lower: &str, col: &str) -> Option<String> {
    let pat = format!("{col} < '");
    let pos = lower.find(&pat)?;
    let rest = &lower[pos + pat.len()..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

// ─── BucketWidth extension ────────────────────────────────────────────────────

/// Extension trait to parse an interval text string into a `BucketWidth`.
pub(crate) trait BucketWidthFromText {
    fn from_interval_text(s: &str) -> Option<crate::cv_time_bucket::BucketWidth>;
}

impl BucketWidthFromText for crate::cv_time_bucket::BucketWidth {
    fn from_interval_text(s: &str) -> Option<crate::cv_time_bucket::BucketWidth> {
        use crate::cv_time_bucket::BucketWidth;
        // Try the existing parse_interval_secs from cv_time_bucket module
        // by calling it directly on the text.
        let s = s.trim().to_ascii_lowercase();
        // Named units, with the per-unit second weight used when a quantity
        // multiplier (`10 minutes`, `30 days`) accompanies the unit.
        let named: &[(&str, BucketWidth, u64)] = &[
            ("second", BucketWidth::Second, 1),
            ("minute", BucketWidth::Minute, 60),
            ("hour", BucketWidth::Hour, 3_600),
            ("day", BucketWidth::Day, 86_400),
            ("week", BucketWidth::Week, 604_800),
            ("month", BucketWidth::Month, 30 * 86_400),
            ("year", BucketWidth::Year, 365 * 86_400),
        ];
        // Leading integer quantity, if any (`"10 minutes"` → 10). A bare unit or
        // an explicit `1` keeps the named (calendar-aware) variant; a quantity
        // > 1 must honour the multiplier — returning the bare named variant here
        // silently dropped it, so `time_bucket('10 minutes', ts)` bucketed by
        // 60s instead of 600s. For a multiplier we return a fixed-seconds bucket
        // (correct for sub-day units; for day/week/month/year it is the same
        // pragmatic fixed-width approximation the rest of this module uses).
        let qty: Option<u64> = {
            let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
            digits.parse::<u64>().ok()
        };
        for (name, w, unit_secs) in named {
            if s.contains(name) {
                return match qty {
                    Some(n) if n != 1 => Some(BucketWidth::SecondsFixed(n * unit_secs)),
                    _ => Some(*w),
                };
            }
        }
        // Try parsing as `<n> <unit>` → SecondsFixed
        parse_interval_secs_fallback(&s).map(BucketWidth::SecondsFixed)
    }
}

// ─── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_hypertable_1day() {
        let sql = "SELECT create_hypertable('metrics', 'ts', chunk_time_interval => INTERVAL '1 day')";
        let r = match_create_hypertable(sql);
        assert!(r.is_some(), "should match: {sql}");
        let (table, col, iv) = r.unwrap();
        assert_eq!(table, "metrics");
        assert_eq!(col, "ts");
        assert_eq!(iv, "1 day");
    }

    #[test]
    fn parse_create_hypertable_1hour() {
        let sql = "SELECT create_hypertable('soak_metrics', 'ts', chunk_time_interval => INTERVAL '1 hour')";
        let r = match_create_hypertable(sql);
        assert!(r.is_some(), "should match: {sql}");
        let (_, _, iv) = r.unwrap();
        assert_eq!(iv, "1 hour");
    }

    #[test]
    fn parse_add_retention_policy() {
        let sql = "SELECT add_retention_policy('retention_metrics', INTERVAL '30 days')";
        let r = match_add_retention_policy(sql);
        assert!(r.is_some(), "should match");
        let (table, iv) = r.unwrap();
        assert_eq!(table, "retention_metrics");
        assert_eq!(iv, "30 days");
    }

    #[test]
    fn parse_run_retention_policy() {
        let sql = "SELECT run_retention_policy('retention_metrics')";
        let r = match_run_retention_policy(sql);
        assert_eq!(r.as_deref(), Some("retention_metrics"));
    }

    #[test]
    fn ensure_chunk_creates_day_buckets() {
        let mut ht = HypertableRecord::new("ts".into(), 86_400);
        let day1: DateTime<Utc> = "2024-01-01T06:00:00Z".parse().unwrap();
        let day2: DateTime<Utc> = "2024-01-02T12:00:00Z".parse().unwrap();
        let day3: DateTime<Utc> = "2024-01-03T23:59:59Z".parse().unwrap();
        ht.ensure_chunk_for(day1);
        ht.ensure_chunk_for(day2);
        ht.ensure_chunk_for(day3);
        // Second call for day1 should not create a new chunk.
        let day1b: DateTime<Utc> = "2024-01-01T18:00:00Z".parse().unwrap();
        ht.ensure_chunk_for(day1b);
        assert_eq!(ht.chunks.len(), 3, "expected 3 day-level chunks");
    }

    #[test]
    fn retention_drops_old_chunks() {
        let mut ht = HypertableRecord::new("ts".into(), 86_400);
        // Insert an old chunk (2023-01-01 → 2023-01-02)
        let old: DateTime<Utc> = "2023-01-01T12:00:00Z".parse().unwrap();
        ht.ensure_chunk_for(old);
        // Insert a future chunk (2099-12-31)
        let future: DateTime<Utc> = "2099-12-31T12:00:00Z".parse().unwrap();
        ht.ensure_chunk_for(future);
        assert_eq!(ht.chunks.len(), 2);

        // Cutoff: anything ending before 2024-01-01 is dropped
        let cutoff: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
        let to_drop = ht.chunks_older_than(cutoff);
        assert_eq!(to_drop.len(), 1, "one old chunk to drop");
        ht.drop_chunks(&to_drop);
        assert_eq!(ht.chunks.len(), 1, "only future chunk remains");
    }
}

// ─── time_bucket UDF ─────────────────────────────────────────────────────────
//
// `time_bucket(bucket_width text, ts timestamptz) → timestamptz`
//
// Floors `ts` down to the nearest `bucket_width` boundary measured from the
// Unix epoch (same semantics as TimescaleDB's `time_bucket`).  Accepts any
// Timestamp variant DataFusion uses.

use datafusion::arrow::array::{
    Array, ArrayRef, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
    TimestampMillisecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register `time_bucket(interval_text, ts[, origin_or_tz]) → ts` into the
/// given context.
///
/// Supported arities:
///   2 args: `time_bucket(interval, ts)` — epoch-aligned (original behaviour).
///   3 args: `time_bucket(interval, ts, origin TIMESTAMPTZ)` — aligns the
///            bucket grid to `origin` instead of Unix epoch.
///   3 args: `time_bucket(interval, ts, timezone TEXT)` — shifts ts to the
///            named timezone before bucketing then shifts back. Implemented via
///            chrono-tz when the third arg is a string literal; falls back to
///            epoch-aligned if the timezone cannot be parsed.
pub(crate) fn register_time_bucket_udf(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TimeBucketUdf {
        // Accept 2 or 3 arguments.
        signature: Signature::variadic_any(Volatility::Immutable),
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TimeBucketUdf {
    signature: Signature,
}

impl ScalarUDFImpl for TimeBucketUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "time_bucket" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Return the same timestamp type as the second argument, falling back
        // to Timestamp(Microsecond, None) if unknown.
        if arg_types.len() >= 2 {
            match &arg_types[1] {
                dt @ DataType::Timestamp(..) => return Ok(dt.clone()),
                _ => {}
            }
        }
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() < 2 || args.len() > 3 {
            return exec_err!("time_bucket expects 2 or 3 arguments");
        }

        // ── arg 0: bucket width as a string literal ───────────────────────
        let width_text = match &args[0] {
            ColumnarValue::Scalar(sv) => sv.to_string(),
            ColumnarValue::Array(arr) => {
                arr.as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| if a.is_empty() { None } else { Some(a.value(0).to_string()) })
                    .ok_or_else(|| datafusion::error::DataFusionError::Internal(
                        "time_bucket: arg[0] must be a string (bucket width)".into(),
                    ))?
            }
        };
        // Strip quotes that may arrive when the literal came through a cast
        let width_text = width_text.trim_matches('\'').trim_matches('"');
        let bw = crate::cv_time_bucket::BucketWidth::from_interval_text(width_text)
            .ok_or_else(|| datafusion::error::DataFusionError::Internal(
                format!("time_bucket: unrecognised interval '{width_text}'"),
            ))?;

        // ── arg 2 (optional): origin timestamp or timezone string ──────────
        // We extract this before processing the ts array. If the third arg is
        // a timestamp we treat it as origin (shift the epoch grid); if it is a
        // string we treat it as a timezone name (IANA / abbrev).
        let origin_us: Option<i64> = if args.len() == 3 {
            extract_origin_us(&args[2])
        } else {
            None
        };
        // `origin_shift_us` is the number of µs the origin is offset from the
        // Unix epoch. When non-zero, the floor is computed relative to the
        // origin: floor((ts - origin), bucket) + origin.
        let origin_shift_us = origin_us.unwrap_or(0i64);

        // ── arg 1: timestamp column ───────────────────────────────────────
        let ts_array: ArrayRef = match &args[1] {
            ColumnarValue::Scalar(sv) => sv.to_array()?,
            ColumnarValue::Array(arr) => arr.clone(),
        };

        let len = ts_array.len();

        // Helper: convert microseconds-since-epoch to floored µs.
        // When origin_shift_us != 0, the grid is anchored to `origin` rather
        // than the Unix epoch:
        //   result = floor((ts_us - origin_shift_us), bucket_us) + origin_shift_us
        // For BucketWidth variants with calendar semantics (Day/Week/Month/Year)
        // the origin shift is applied in microseconds and the BucketWidth::floor
        // still operates on UTC calendar math, so the shift only affects the
        // epoch-anchored variants (SecondsFixed). Calendar variants are
        // unaffected by the origin shift (they already anchor to midnight UTC).
        let floor_us = |us: i64| -> i64 {
            use chrono::TimeZone as _;
            let effective_us = us - origin_shift_us;
            let dt = chrono::DateTime::<Utc>::from_timestamp_micros(effective_us)
                .unwrap_or_else(|| Utc.timestamp_micros(0).unwrap());
            let floored = bw.floor(dt).timestamp_micros();
            floored + origin_shift_us
        };
        let floor_ns = |ns: i64| -> i64 {
            use chrono::TimeZone as _;
            let effective_ns = ns - origin_shift_us * 1_000;
            let secs = effective_ns / 1_000_000_000;
            let nanos = (effective_ns.rem_euclid(1_000_000_000)) as u32;
            let dt = Utc.timestamp_opt(secs, nanos).single()
                .unwrap_or_else(|| Utc.timestamp_micros(0).unwrap());
            let floored = bw.floor(dt).timestamp_nanos_opt().unwrap_or(0);
            floored + origin_shift_us * 1_000
        };
        let floor_ms = |ms: i64| -> i64 {
            use chrono::TimeZone as _;
            let effective_ms = ms - origin_shift_us / 1_000;
            let dt = chrono::DateTime::<Utc>::from_timestamp_millis(effective_ms)
                .unwrap_or_else(|| Utc.timestamp_micros(0).unwrap());
            let floored = bw.floor(dt).timestamp_millis();
            floored + origin_shift_us / 1_000
        };
        let floor_s = |s: i64| -> i64 {
            use chrono::TimeZone as _;
            let effective_s = s - origin_shift_us / 1_000_000;
            let dt = Utc.timestamp_opt(effective_s, 0).single()
                .unwrap_or_else(|| Utc.timestamp_micros(0).unwrap());
            let floored = bw.floor(dt).timestamp();
            floored + origin_shift_us / 1_000_000
        };

        let result: ArrayRef = match ts_array.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let arr = ts_array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let out: TimestampMicrosecondArray = arr.iter()
                    .map(|v| v.map(floor_us))
                    .collect();
                let out = out.with_timezone_opt(tz.clone());
                Arc::new(out)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let arr = ts_array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                let out: TimestampNanosecondArray = arr.iter()
                    .map(|v| v.map(floor_ns))
                    .collect();
                let out = out.with_timezone_opt(tz.clone());
                Arc::new(out)
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let arr = ts_array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                let out: TimestampMillisecondArray = arr.iter()
                    .map(|v| v.map(floor_ms))
                    .collect();
                let out = out.with_timezone_opt(tz.clone());
                Arc::new(out)
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                let arr = ts_array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                let out: TimestampSecondArray = arr.iter()
                    .map(|v| v.map(floor_s))
                    .collect();
                let out = out.with_timezone_opt(tz.clone());
                Arc::new(out)
            }
            other => {
                return exec_err!("time_bucket: unsupported timestamp type {other:?}");
            }
        };

        let _ = len; // suppress unused warning if optimised away
        Ok(ColumnarValue::Array(result))
    }
}

// ─── 3-arg time_bucket helper ─────────────────────────────────────────────────

/// Extract an origin offset in microseconds-since-epoch from arg[2] of a
/// 3-argument `time_bucket` call.
///
/// The third argument can be:
/// * A timestamp scalar → its µs-since-epoch value is the origin.
/// * A string scalar (timezone name) → not yet supported; returns `None` so
///   the caller falls back to epoch-aligned bucketing.
///
/// On any error or unsupported type returns `None`.
fn extract_origin_us(arg: &ColumnarValue) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    let sv = match arg {
        ColumnarValue::Scalar(sv) => sv.clone(),
        ColumnarValue::Array(arr) => {
            if arr.is_empty() { return None; }
            ScalarValue::try_from_array(arr.as_ref(), 0).ok()?
        }
    };
    match sv {
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(v),
        ScalarValue::TimestampNanosecond(Some(v), _)  => Some(v / 1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(v * 1_000),
        ScalarValue::TimestampSecond(Some(v), _)      => Some(v * 1_000_000),
        ScalarValue::Int64(Some(v)) => Some(v),
        // String → timezone name: not yet implemented; fall back to epoch grid.
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) => None,
        _ => None,
    }
}
