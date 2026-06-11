//! Phase 5.20.E — GIN posting-list structure for `tsvector` columns.
//!
//! Builds and queries in-memory posting lists keyed per *lexeme* for columns
//! that have a `CREATE INDEX … USING gin` declaration with an FTS opclass
//! (`tsvector_ops`).  The posting lists enable per-file pruning for FTS
//! predicates (`@@`) instead of always falling through to a full DataFusion
//! scan.
//!
//! # Terminology
//!
//! * **lexeme** — a normalised token extracted from a `tsvector` value.  A
//!   `tsvector` is represented as a space-separated list of single-quoted
//!   lexemes, optionally followed by positional weights
//!   (`'cat':1 'dog':2 'run':3`).  This module extracts only the lexeme
//!   strings and ignores positions/weights — that is the correct GIN
//!   granularity for `@@` matching (positions matter for `<->` phrase queries,
//!   which require a re-evaluation pass regardless).
//!
//! * **posting list** — a sorted set of `TsvPostingEntry` values (file path +
//!   row-group + row) for each distinct lexeme.
//!
//! # Correctness contract
//!
//! The probe result is a *conservative superset*: it prunes only files that
//! provably contain NO matching row.  Every candidate file returned by
//! [`GinTsvectorRegistry::probe_query`] is re-evaluated by the full `@@`
//! predicate at read time.  The caller must never skip that re-evaluation.
//!
//! ## Per-operator probe soundness
//!
//! The probe parses the *canonical* `tsquery` text (the form produced by
//! `to_tsquery`/`plainto_tsquery` canonicalisation in `basin-engine`) and
//! evaluates the boolean structure over posting lists:
//!
//! | tsquery node | probe evaluation | why it is sound                        |
//! |--------------|------------------|----------------------------------------|
//! | `'lex'`      | files containing the lexeme; **Unknown** if the lexeme was never indexed (or was evicted) | a marked file has all of its lexemes present |
//! | `a & b`      | intersection of file sets (`Unknown ∩ X = X`) | a matching row must satisfy both sides, so it lives in a file from each side's set |
//! | `a \| b`     | union of file sets; **Unknown** if either side is Unknown | a matching row satisfies at least one side |
//! | `a <-> b`, `a <N> b` | treated as `a & b` | a phrase match requires both lexemes in the SAME row; positions are re-checked at read time |
//! | `!a`         | **Unknown** | a matching row may contain none of the indexed lexemes |
//!
//! `Unknown` anywhere at the top level degrades to a full scan (no pruning,
//! no `Empty` short-circuit) — never a wrong answer.  A plain AND-merge of
//! every lexeme atom (the pre-5.20.E shape) is UNSOUND for `|` and `!`
//! queries: `'cat' | 'dog'` over files that each hold only one of the two
//! lexemes would AND-merge to ∅ and short-circuit to zero rows while real
//! matches exist.  The structural evaluation above is what makes OR queries
//! prunable (file-set union) and NOT queries safe (decline).
//!
//! # Storage / eviction / budget story
//!
//! The posting list lives entirely in RAM.  The registry caps each per-column
//! posting list at [`DEFAULT_POSTING_BUDGET`] total entries (operator-tunable
//! via `BASIN_GIN_POSTING_BUDGET`, the same knob the JSONB GIN registry
//! reads); oldest lexemes are evicted in 25%-of-current-lexemes batches when
//! the cap is exceeded.  On engine restart the registry starts empty and
//! rebuilds from writes and CREATE INDEX backfills.
//!
//! **Accounting note (vs. the JSONB GIN registry):** the JSONB registry
//! de-duplicates to interned `(term, file)` pairs, so its budget counts
//! distinct pairs.  The tsvector posting list must keep *row-level* entries
//! (`lexeme → (file, row-group, row)`) because the row-group-granular prune
//! path (`probe_query_with_row_groups`) drives `row_group_selection` from
//! them.  The budget therefore counts row-level postings and trips earlier
//! for the same corpus; file paths are interned (`Arc<str>`) so the per-entry
//! cost is one small struct + one `Arc` bump, not a path clone.
//!
//! **Per-file completeness:** eviction un-marks only the *affected files*
//! (those whose posting entries were dropped), not the entire column.  Files
//! that still have complete lexeme coverage remain prunable.  Un-marked files
//! fail the engine's completeness guard (`indexed_files ⊇ live_files`) and
//! force a full scan — correctness is scale-independent.
//!
//! # Engine wiring (Phase 5.20.E)
//!
//! 1. **Populate on write** — `maintain_gin_fts_index_on_insert` in
//!    `basin-engine/src/executor.rs` calls `index_row` for every tsvector
//!    column in a newly written data file.  `mark_file_indexed` is called
//!    after all rows are processed so the completeness guard is valid.
//!
//! 2. **Backfill on CREATE INDEX** — `backfill_index_over_live_files` in
//!    `basin-engine/src/executor.rs` walks pre-existing live files (after
//!    `materialize_overlay_for_table` settles any hot-tier overlay) and feeds
//!    them through `index_row` + `mark_file_indexed`.
//!
//! 3. **Probe at query** — `detect_tsvector_match` in
//!    `basin-engine/src/index_probe.rs` recognises `col @@ to_tsquery(…)` on
//!    a GIN-indexed tsvector column.  The executor probes `probe_query` for
//!    an `Empty` short-circuit, gated on no-live-overlay AND per-file
//!    completeness (`fts_empty_probe_is_trustworthy`).
//!
//! 4. **Prune the scan** — `apply_gin_fts_pruning_for_query` in
//!    `basin-engine/src/session.rs` intersects the probe result with live
//!    files and re-registers a pruned provider ONLY when the table has no
//!    live overlay AND the completeness guard passes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use basin_common::{ProjectId, TableName};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Maximum total posting entries (file+rg+row tuples) kept per `(table, col)`
/// posting list.  Beyond this threshold the oldest 25% of lexemes are evicted.
///
/// This default can be overridden at process start via the
/// `BASIN_GIN_POSTING_BUDGET` environment variable (shared with the JSONB GIN
/// registry).  Example: `BASIN_GIN_POSTING_BUDGET=2000000 basin-server`.
const DEFAULT_POSTING_BUDGET: usize = 500_000;

/// Return the effective per-column posting-entry budget.
///
/// Reads `BASIN_GIN_POSTING_BUDGET` once and caches the result.  The same env
/// var governs both the JSONB and tsvector GIN registries so operators have a
/// single knob for both.
fn posting_budget() -> usize {
    use std::sync::OnceLock;
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("BASIN_GIN_POSTING_BUDGET")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_POSTING_BUDGET)
    })
}

// ── Data types ────────────────────────────────────────────────────────────────

/// One physical location for a posting entry: file path + row-group + row.
///
/// The file path is interned (`Arc<str>`, one allocation per distinct file
/// per posting list) — at row-level posting granularity a cloned `String`
/// per entry would dominate the registry's memory footprint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TsvPostingEntry {
    /// Parquet/Vortex file path (or object-store key), interned.
    pub file_path: Arc<str>,
    /// Row-group index within the file.
    pub row_group: u32,
    /// Row offset within the row-group.
    pub row: u64,
}

// ── tsvector parsing ──────────────────────────────────────────────────────────

/// Extract the set of normalised lexemes from a raw `tsvector` string.
///
/// PostgreSQL serialises a `tsvector` as:
/// ```text
/// 'lexeme1':pos1,pos2 'lexeme2':pos3 …
/// ```
///
/// This function extracts only the lexeme strings (the single-quoted tokens)
/// and ignores positional information.  Positions are irrelevant for the GIN
/// probe — the re-evaluation layer handles phrase proximity.
///
/// # Examples
///
/// ```
/// use basin_storage::index::gin_tsvector::extract_lexemes;
/// let lexemes = extract_lexemes("'cat':1 'dog':2,3 'run':4");
/// assert!(lexemes.contains("cat"));
/// assert!(lexemes.contains("dog"));
/// assert!(lexemes.contains("run"));
/// assert_eq!(lexemes.len(), 3);
/// ```
pub fn extract_lexemes(tsvector: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let s = tsvector.trim();
    let mut chars = s.chars().peekable();

    while let Some(&ch) = chars.peek() {
        // Skip whitespace between tokens.
        if ch.is_ascii_whitespace() {
            chars.next();
            continue;
        }

        if ch == '\'' {
            // Start of a quoted lexeme.
            chars.next(); // consume opening quote
            let mut lexeme = String::new();
            let mut escaped = false;

            loop {
                match chars.next() {
                    None => break, // unterminated quote — best-effort
                    Some('\\') if !escaped => {
                        escaped = true;
                    }
                    Some('\'') if !escaped => {
                        // Peek: if next char is also `'` it's an escaped quote
                        // (`''` inside a tsvector lexeme — unusual but possible
                        // in round-tripped literals).
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            lexeme.push('\'');
                        } else {
                            break; // closing quote
                        }
                    }
                    Some(c) => {
                        escaped = false;
                        lexeme.push(c);
                    }
                }
            }

            if !lexeme.is_empty() {
                out.insert(lexeme);
            }

            // Skip the positional annotation (`:1,2B`) if present.
            if chars.peek() == Some(&':') {
                // Consume everything until the next whitespace or end-of-string.
                for c in chars.by_ref() {
                    if c.is_ascii_whitespace() {
                        break;
                    }
                }
            }
        } else {
            // Unexpected character outside a quote: skip until whitespace.
            for c in chars.by_ref() {
                if c.is_ascii_whitespace() {
                    break;
                }
            }
        }
    }

    out
}

// ── tsquery structural parsing (probe side) ──────────────────────────────────

/// Boolean structure of a canonical `tsquery`, as far as the GIN probe needs
/// it.  Phrase distance is irrelevant for pruning (a phrase match implies
/// both operands occur in the same row), so `a <N> b` folds to [`TsqNode::Phrase`]
/// without the distance.
#[derive(Debug, Clone, PartialEq)]
enum TsqNode {
    Term(String),
    Not(Box<TsqNode>),
    And(Box<TsqNode>, Box<TsqNode>),
    Or(Box<TsqNode>, Box<TsqNode>),
    Phrase(Box<TsqNode>, Box<TsqNode>),
}

#[derive(Debug, Clone, PartialEq)]
enum TsqTok {
    Term(String),
    And,
    Or,
    Not,
    Phrase,
    LParen,
    RParen,
}

/// Lex a canonical tsquery string (`'cat' & !'dog' | 'a' <-> 'b'`).  Terms
/// are single-quoted (with `''` as the embedded-quote escape); bare
/// alphanumeric runs are accepted defensively and lowercased.  Lexemes may
/// carry multi-byte UTF-8 (`'café'`), so the lexer iterates `char`s — the
/// same granularity `extract_lexemes` uses on the indexing side.  Returns
/// `None` on anything unexpected — the caller treats that as "structure
/// unknown → full scan" (never a wrong prune).
fn lex_tsquery(input: &str) -> Option<Vec<TsqTok>> {
    let mut chars = input.chars().peekable();
    let mut out = Vec::new();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_whitespace() {
            chars.next();
            continue;
        }
        match c {
            '&' => {
                chars.next();
                out.push(TsqTok::And);
            }
            '|' => {
                chars.next();
                out.push(TsqTok::Or);
            }
            '!' => {
                chars.next();
                out.push(TsqTok::Not);
            }
            '(' => {
                chars.next();
                out.push(TsqTok::LParen);
            }
            ')' => {
                chars.next();
                out.push(TsqTok::RParen);
            }
            '<' => {
                // `<->` or `<N>` — both fold to Phrase for the probe.
                chars.next();
                match chars.peek() {
                    Some('-') => {
                        chars.next();
                        if chars.next() != Some('>') {
                            return None;
                        }
                        out.push(TsqTok::Phrase);
                    }
                    Some(d) if d.is_ascii_digit() => {
                        let mut saw_digit = false;
                        while matches!(chars.peek(), Some(d) if d.is_ascii_digit()) {
                            chars.next();
                            saw_digit = true;
                        }
                        if !saw_digit || chars.next() != Some('>') {
                            return None;
                        }
                        out.push(TsqTok::Phrase);
                    }
                    _ => return None,
                }
            }
            '\'' => {
                chars.next(); // consume opening quote
                let mut buf = String::new();
                loop {
                    match chars.next() {
                        None => return None, // unterminated quote
                        Some('\'') => {
                            if chars.peek() == Some(&'\'') {
                                chars.next();
                                buf.push('\'');
                            } else {
                                break; // closing quote
                            }
                        }
                        Some(ch) => buf.push(ch),
                    }
                }
                if !buf.is_empty() {
                    out.push(TsqTok::Term(buf));
                }
            }
            _ => {
                // Defensive: bare alphanumeric run as a term (lowercased).
                let mut buf = String::new();
                while matches!(chars.peek(), Some(ch) if ch.is_alphanumeric()) {
                    buf.push(chars.next()?);
                }
                if buf.is_empty() {
                    return None; // unexpected character
                }
                out.push(TsqTok::Term(buf.to_lowercase()));
            }
        }
    }
    Some(out)
}

/// Parse a canonical tsquery into a [`TsqNode`].  Grammar mirrors the engine
/// tsquery parser (precedence: `|` < `&` < `<->`/`<N>` < `!`):
///
/// ```text
/// or     := and ('|' and)*
/// and    := phrase ('&' phrase)*
/// phrase := unary (('<->' | '<N>') unary)*
/// unary  := '!' unary | '(' or ')' | term
/// ```
///
/// Returns `None` for empty or malformed input — the probe treats that as
/// "unknown structure → full scan".
fn parse_tsquery(input: &str) -> Option<TsqNode> {
    let toks = lex_tsquery(input)?;
    if toks.is_empty() {
        return None;
    }
    let mut p = TsqParser { toks, pos: 0 };
    let node = p.parse_or()?;
    if p.pos != p.toks.len() {
        return None; // trailing tokens — malformed
    }
    Some(node)
}

struct TsqParser {
    toks: Vec<TsqTok>,
    pos: usize,
}

impl TsqParser {
    fn peek(&self) -> Option<&TsqTok> {
        self.toks.get(self.pos)
    }
    fn bump(&mut self) -> Option<TsqTok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn parse_or(&mut self) -> Option<TsqNode> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(TsqTok::Or)) {
            self.bump();
            let right = self.parse_and()?;
            left = TsqNode::Or(Box::new(left), Box::new(right));
        }
        Some(left)
    }

    fn parse_and(&mut self) -> Option<TsqNode> {
        let mut left = self.parse_phrase()?;
        while matches!(self.peek(), Some(TsqTok::And)) {
            self.bump();
            let right = self.parse_phrase()?;
            left = TsqNode::And(Box::new(left), Box::new(right));
        }
        Some(left)
    }

    fn parse_phrase(&mut self) -> Option<TsqNode> {
        let mut left = self.parse_unary()?;
        while matches!(self.peek(), Some(TsqTok::Phrase)) {
            self.bump();
            let right = self.parse_unary()?;
            left = TsqNode::Phrase(Box::new(left), Box::new(right));
        }
        Some(left)
    }

    fn parse_unary(&mut self) -> Option<TsqNode> {
        match self.peek() {
            Some(TsqTok::Not) => {
                self.bump();
                Some(TsqNode::Not(Box::new(self.parse_unary()?)))
            }
            Some(TsqTok::LParen) => {
                self.bump();
                let q = self.parse_or()?;
                match self.bump() {
                    Some(TsqTok::RParen) => Some(q),
                    _ => None,
                }
            }
            Some(TsqTok::Term(_)) => match self.bump() {
                Some(TsqTok::Term(t)) => Some(TsqNode::Term(t)),
                _ => None,
            },
            _ => None,
        }
    }
}

// ── Probe evaluation lattices ─────────────────────────────────────────────────

/// File-level evaluation result for one tsquery sub-tree.  `Unknown` means
/// the sub-tree's matches cannot be bounded by the posting list (a `!`
/// operand, a never-indexed lexeme) — anything ANDed with it falls back to
/// the other side; anything ORed with it poisons the whole branch.
enum FileEval {
    Unknown,
    Files(HashSet<String>),
}

/// Row-group-level evaluation result (file → candidate row-groups).
enum RgEval {
    Unknown,
    Map(HashMap<String, HashSet<u32>>),
}

fn eval_files(node: &TsqNode, list: &LexemePostingList) -> FileEval {
    match node {
        TsqNode::Term(t) => match list.probe_lexeme(t) {
            // Never indexed (or evicted): cannot bound this term's matches.
            None => FileEval::Unknown,
            Some(entries) => FileEval::Files(
                entries.iter().map(|e| e.file_path.to_string()).collect(),
            ),
        },
        // A row matching `!a` may contain none of the indexed lexemes; the
        // posting list cannot bound it.
        TsqNode::Not(_) => FileEval::Unknown,
        // A phrase match requires both operands in the same row → at least
        // the AND of the operands' file sets (sound necessary condition).
        TsqNode::And(a, b) | TsqNode::Phrase(a, b) => {
            match (eval_files(a, list), eval_files(b, list)) {
                (FileEval::Unknown, x) | (x, FileEval::Unknown) => x,
                (FileEval::Files(x), FileEval::Files(y)) => {
                    FileEval::Files(x.intersection(&y).cloned().collect())
                }
            }
        }
        TsqNode::Or(a, b) => match (eval_files(a, list), eval_files(b, list)) {
            (FileEval::Files(mut x), FileEval::Files(y)) => {
                x.extend(y);
                FileEval::Files(x)
            }
            // Either side unbounded → the union is unbounded.
            _ => FileEval::Unknown,
        },
    }
}

fn eval_row_groups(node: &TsqNode, list: &LexemePostingList) -> RgEval {
    match node {
        TsqNode::Term(t) => match list.probe_lexeme(t) {
            None => RgEval::Unknown,
            Some(entries) => {
                let mut by_file: HashMap<String, HashSet<u32>> = HashMap::new();
                for e in entries {
                    by_file
                        .entry(e.file_path.to_string())
                        .or_default()
                        .insert(e.row_group);
                }
                RgEval::Map(by_file)
            }
        },
        TsqNode::Not(_) => RgEval::Unknown,
        // AND / phrase: intersect file sets; union row-groups within each
        // surviving file.  The union (rather than rg-level intersection)
        // mirrors the JSONB GIN row-group prune and is trivially sound — a
        // row matching both operands sits in a row-group touched by each.
        TsqNode::And(a, b) | TsqNode::Phrase(a, b) => {
            match (eval_row_groups(a, list), eval_row_groups(b, list)) {
                (RgEval::Unknown, x) | (x, RgEval::Unknown) => x,
                (RgEval::Map(mut x), RgEval::Map(y)) => {
                    x.retain(|f, _| y.contains_key(f));
                    for (f, rgs) in x.iter_mut() {
                        if let Some(other) = y.get(f) {
                            rgs.extend(other.iter().copied());
                        }
                    }
                    RgEval::Map(x)
                }
            }
        }
        TsqNode::Or(a, b) => match (eval_row_groups(a, list), eval_row_groups(b, list)) {
            (RgEval::Map(mut x), RgEval::Map(y)) => {
                for (f, rgs) in y {
                    x.entry(f).or_default().extend(rgs);
                }
                RgEval::Map(x)
            }
            _ => RgEval::Unknown,
        },
    }
}

// ── Internal posting list ─────────────────────────────────────────────────────

/// Per-`(table, col)` in-memory posting list keyed by lexeme.
#[derive(Debug, Default)]
struct LexemePostingList {
    /// `lexeme → set of (interned file, rg, row)`.
    entries: HashMap<String, HashSet<TsvPostingEntry>>,
    /// Insertion-ordered list of lexeme keys for FIFO eviction.
    insert_order: Vec<String>,
    /// Total entry count (sum of all per-lexeme set sizes).
    total_count: usize,
    /// Interned file paths (one `Arc<str>` per distinct file).
    file_interner: HashSet<Arc<str>>,
    /// Set once the first eviction fires so the operator warning logs once.
    eviction_warned: bool,
}

impl LexemePostingList {
    fn new() -> Self {
        Self::default()
    }

    /// Return the interned `Arc<str>` for `file_path`, creating it on first use.
    fn intern_file(&mut self, file_path: &str) -> Arc<str> {
        if let Some(existing) = self.file_interner.get(file_path) {
            return existing.clone();
        }
        let arc: Arc<str> = Arc::from(file_path);
        self.file_interner.insert(arc.clone());
        arc
    }

    /// Add a `(lexeme, entry)` pair.
    ///
    /// Returns the set of file paths whose posting entries were **evicted**
    /// by this insert (empty when no eviction occurred).  The caller
    /// (`GinTsvectorRegistry::index_row`) un-marks exactly those files in the
    /// per-file completeness map: a file whose lexemes were dropped no longer
    /// has full coverage and must be force-scanned, while untouched files
    /// stay prunable.
    fn insert(&mut self, lexeme: String, entry: TsvPostingEntry) -> HashSet<String> {
        let is_new_lexeme = !self.entries.contains_key(&lexeme);
        let set = self.entries.entry(lexeme.clone()).or_default();
        if set.insert(entry) {
            self.total_count += 1;
        }

        // Track insertion order only on the first posting for this lexeme so
        // eviction correctly identifies "oldest lexemes first".
        if is_new_lexeme {
            self.insert_order.push(lexeme);
        }

        if self.total_count > posting_budget() {
            if !self.eviction_warned {
                self.eviction_warned = true;
                tracing::warn!(
                    budget = posting_budget(),
                    "tsvector GIN posting budget exceeded; evicting oldest \
                     lexemes (affected files degrade to full scans). Raise \
                     BASIN_GIN_POSTING_BUDGET to keep more of the index in RAM."
                );
            }
            self.evict_oldest()
        } else {
            HashSet::new()
        }
    }

    /// Evict the oldest 25% of current lexemes.
    ///
    /// Returns the set of file paths that were referenced by the evicted
    /// posting entries — those files lose full coverage and the caller must
    /// un-mark them in the completeness map.
    fn evict_oldest(&mut self) -> HashSet<String> {
        let evict_count = (self.insert_order.len() / 4).max(1);
        let to_evict: Vec<String> =
            self.insert_order.drain(..evict_count.min(self.insert_order.len())).collect();
        let mut affected_files: HashSet<String> = HashSet::new();
        for k in &to_evict {
            if let Some(set) = self.entries.remove(k) {
                for e in &set {
                    affected_files.insert(e.file_path.to_string());
                }
                self.total_count = self.total_count.saturating_sub(set.len());
            }
        }
        affected_files
    }

    /// Probe for `lexeme`.
    ///
    /// Returns `None` when the lexeme has never been indexed or was evicted
    /// (caller treats as "unknown → cannot bound").  Returns `Some(set)` for
    /// a known lexeme; the set may be empty when all posting entries for this
    /// lexeme referenced files that have since been removed.
    fn probe_lexeme(&self, lexeme: &str) -> Option<&HashSet<TsvPostingEntry>> {
        self.entries.get(lexeme)
    }

    /// Remove all entries that reference `file_path`.  Called when a file is
    /// compacted or deleted.
    fn remove_file(&mut self, file_path: &str) {
        for set in self.entries.values_mut() {
            let before = set.len();
            set.retain(|e| e.file_path.as_ref() != file_path);
            self.total_count = self.total_count.saturating_sub(before - set.len());
        }
        self.file_interner.remove(file_path);
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Key into the global registry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

/// Process-wide GIN posting-list registry for `tsvector` columns.
///
/// One [`LexemePostingList`] per `(project, table, col)`.  Concurrent access
/// is serialised by a per-posting-list `Mutex`, with the outer `Mutex` only
/// held briefly to look up or insert the `Arc`.
///
/// # Thread safety
///
/// `GinTsvectorRegistry` is `Send + Sync`.  The outer mutex guards the
/// `HashMap` of `Arc<Mutex<…>>` pointers; each inner mutex guards one
/// per-column posting list.  The outer lock is never held while the inner
/// lock is taken, preventing deadlock.
///
/// `indexed_files` tracks, for each `(project, table, col)`, the set of file
/// paths whose rows have been fully loaded into the posting list.  This is the
/// completeness guard required by Phase 5.20.E: file-level pruning and the
/// `Empty` short-circuit are only safe when every live data file appears in
/// this set.  Eviction un-marks affected files *while still holding the
/// posting-list lock*, so a reader that acquires the list after an eviction
/// can never observe "lexeme evicted but file still marked".
pub struct GinTsvectorRegistry {
    inner: Mutex<HashMap<RegKey, Arc<Mutex<LexemePostingList>>>>,
    /// File-completeness tracking: `RegKey → set of fully-indexed file paths`.
    indexed_files: Mutex<HashMap<RegKey, HashSet<String>>>,
}

impl GinTsvectorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            indexed_files: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<Mutex<LexemePostingList>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let mut map = self.inner.lock().expect("GinTsvectorRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(LexemePostingList::new())))
            .clone()
    }

    fn get(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Option<Arc<Mutex<LexemePostingList>>> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        let map = self.inner.lock().expect("GinTsvectorRegistry outer lock poisoned");
        map.get(&key).cloned()
    }

    /// Index one row's `tsvector` value.
    ///
    /// `tsvector_str` is the textual representation of the `tsvector` column
    /// value (e.g. `"'cat':1 'dog':2"`).  Silently skips empty or NULL-like
    /// values.
    ///
    /// Called from the engine write path for every row written to a tsvector
    /// column with a GIN index, and from the CREATE INDEX backfill for
    /// pre-existing files.
    pub fn index_row(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        tsvector_str: &str,
        file_path: &str,
        row_group: u32,
        row: u64,
    ) {
        let lexemes = extract_lexemes(tsvector_str);
        if lexemes.is_empty() {
            return;
        }
        let arc = self.get_or_create(project, table, col);
        let mut list = arc.lock().expect("LexemePostingList lock poisoned");
        let interned = list.intern_file(file_path);
        let entry = TsvPostingEntry { file_path: interned, row_group, row };
        let mut affected_files: HashSet<String> = HashSet::new();
        for lexeme in lexemes {
            affected_files.extend(list.insert(lexeme, entry.clone()));
        }
        // Per-file completeness: if eviction dropped lexemes, only the files
        // referenced by the dropped entries lose coverage — un-mark exactly
        // those.  Files untouched by the eviction keep full lexeme coverage
        // and stay prunable.  The indexed_files update happens while the
        // posting-list lock is still held (`list` guard in scope) so probes
        // can never observe "lexeme evicted but file still marked".
        if !affected_files.is_empty() {
            if let Ok(mut map) = self.indexed_files.lock() {
                let key = RegKey {
                    project: *project,
                    table: table.clone(),
                    col: col.to_string(),
                };
                if let Some(set) = map.get_mut(&key) {
                    for f in &affected_files {
                        set.remove(f);
                    }
                }
            }
        }
        drop(list);
    }

    /// Probe the posting list for a `tsquery` predicate (`tsvector @@ tsquery`).
    ///
    /// `tsquery_str` must be the *canonical* textual form of the query-side
    /// operand (e.g. `"'cat' & 'dog'"`, as produced by the engine's tsquery
    /// canonicalisation — the same Snowball-stemmed pipeline that produced
    /// the indexed tsvector lexemes).
    ///
    /// The query's boolean structure is honoured (see the module-level
    /// per-operator soundness table): `&`/`<->` intersect file sets, `|`
    /// unions them, `!` and unknown lexemes degrade to "cannot bound".
    /// The result is a conservative superset; the caller must re-evaluate the
    /// full `@@` predicate on every candidate row.
    ///
    /// Returns:
    /// * [`TsvProbeResult::NoIndex`] — no posting list loaded, malformed
    ///   query, or the structure cannot be bounded (NOT branch, evicted /
    ///   never-indexed lexeme in a required position).  Caller must fall
    ///   through to a full scan.
    /// * [`TsvProbeResult::Empty`] — the structural evaluation is provably
    ///   empty; no *cold-file* rows can match.  The caller may short-circuit
    ///   ONLY behind the no-live-overlay + per-file-completeness gates.
    /// * [`TsvProbeResult::FileCandidates`] — the set of file paths that
    ///   MIGHT contain matching rows.
    pub fn probe_query(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        tsquery_str: &str,
    ) -> TsvProbeResult {
        let node = match parse_tsquery(tsquery_str) {
            Some(n) => n,
            None => return TsvProbeResult::NoIndex, // malformed/empty → full scan
        };

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return TsvProbeResult::NoIndex,
        };
        let list = arc.lock().expect("LexemePostingList lock poisoned");

        match eval_files(&node, &list) {
            FileEval::Unknown => TsvProbeResult::NoIndex,
            FileEval::Files(files) if files.is_empty() => TsvProbeResult::Empty,
            FileEval::Files(files) => TsvProbeResult::FileCandidates(files),
        }
    }

    /// Probe the posting list at row-group granularity.
    ///
    /// Same correctness contract and per-operator semantics as
    /// [`Self::probe_query`] — the result is a conservative superset and the
    /// caller MUST re-evaluate the full `@@` predicate on every emitted row.
    /// This variant preserves the per-row-group granularity stored in the
    /// underlying posting list so the read path can drive
    /// `ReadOptions.row_group_selection`, opening only the surviving
    /// row-groups within each candidate file.
    ///
    /// `&`/`<->` sub-queries intersect at the *file* level and union the
    /// operands' row-groups within each surviving file (a row-group is a
    /// candidate if any required lexeme touches it; the row-level `@@`
    /// re-evaluation filters false positives).  This union strategy matches
    /// the JSONB GIN row-group prune.  `|` unions both files and per-file
    /// row-groups; `!` and unknown lexemes degrade to `NoIndex`.
    pub fn probe_query_with_row_groups(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        tsquery_str: &str,
    ) -> TsvProbeRowGroupResult {
        let node = match parse_tsquery(tsquery_str) {
            Some(n) => n,
            None => return TsvProbeRowGroupResult::NoIndex,
        };

        let arc = match self.get(project, table, col) {
            Some(a) => a,
            None => return TsvProbeRowGroupResult::NoIndex,
        };
        let list = arc.lock().expect("LexemePostingList lock poisoned");

        let merged = match eval_row_groups(&node, &list) {
            RgEval::Unknown => return TsvProbeRowGroupResult::NoIndex,
            RgEval::Map(m) => m,
        };

        if merged.is_empty() {
            return TsvProbeRowGroupResult::Empty;
        }

        // Materialise as sorted Vec<u32> per file (the shape expected by
        // `GinRowGroupPrunedTable::row_group_selection`).
        let out: HashMap<String, Vec<u32>> = merged
            .into_iter()
            .map(|(file, rgs)| {
                let mut v: Vec<u32> = rgs.into_iter().collect();
                v.sort_unstable();
                (file, v)
            })
            .collect();
        TsvProbeRowGroupResult::FileRowGroups(out)
    }

    /// Remove all posting entries that reference `file_path` for
    /// `(project, table, col)`.  Call this when a data file is compacted
    /// or deleted so the posting list does not return stale candidates.
    ///
    /// Also removes `file_path` from the indexed-files completeness set so
    /// future probes do not erroneously claim full coverage after this file
    /// is gone.
    pub fn remove_file(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        if let Some(arc) = self.get(project, table, col) {
            let mut list = arc.lock().expect("LexemePostingList lock poisoned");
            list.remove_file(file_path);
            // Remove from completeness tracking while the list lock is held
            // (same ordering as eviction in `index_row`).
            let key =
                RegKey { project: *project, table: table.clone(), col: col.to_string() };
            if let Ok(mut map) = self.indexed_files.lock() {
                if let Some(set) = map.get_mut(&key) {
                    set.remove(file_path);
                }
            }
            drop(list);
        } else {
            let key =
                RegKey { project: *project, table: table.clone(), col: col.to_string() };
            if let Ok(mut map) = self.indexed_files.lock() {
                if let Some(set) = map.get_mut(&key) {
                    set.remove(file_path);
                }
            }
        }
    }

    /// Record that `file_path` has been fully indexed for `(project, table,
    /// col)`.  Called immediately after all rows in a new file have been
    /// passed to [`Self::index_row`].  This is the write side of the
    /// completeness guard: a file that appears here is safe to use as a
    /// prune boundary.
    pub fn mark_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        if let Ok(mut map) = self.indexed_files.lock() {
            map.entry(key).or_default().insert(file_path.to_string());
        }
    }

    /// Return the set of file paths that have been completely indexed for
    /// `(project, table, col)`.  The caller uses this to decide whether the
    /// GIN posting list provides FULL coverage of the live file set:
    ///
    /// ```text
    /// completeness = indexed_files ⊇ live_files
    /// ```
    ///
    /// If `completeness` is true, file-level pruning to `FileCandidates` (and
    /// the `Empty` short-circuit, absent a live overlay) is safe.  If any
    /// live file is missing from this set, pruning must NOT happen (full scan
    /// instead, no false negatives).
    pub fn indexed_files_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> HashSet<String> {
        let key = RegKey { project: *project, table: table.clone(), col: col.to_string() };
        if let Ok(map) = self.indexed_files.lock() {
            map.get(&key).cloned().unwrap_or_default()
        } else {
            HashSet::new()
        }
    }

    /// Return the total number of posting entries for `(project, table, col)`.
    ///
    /// Primarily used in tests and diagnostics.
    pub fn total_entries(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> usize {
        match self.get(project, table, col) {
            None => 0,
            Some(arc) => {
                arc.lock().expect("LexemePostingList lock poisoned").total_count
            }
        }
    }
}

impl Default for GinTsvectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a `tsvector @@ tsquery` probe against the GIN posting list.
#[derive(Debug)]
pub enum TsvProbeResult {
    /// No posting list for this column, a malformed query, or a structure
    /// the posting list cannot bound (NOT branch, evicted/never-indexed
    /// lexeme in a required position).  Caller must fall through to a full
    /// scan (no false negatives).
    NoIndex,
    /// The structural evaluation over the posting lists is empty.  No
    /// *cold-file* rows can satisfy the FTS predicate; the caller may
    /// short-circuit only behind the overlay + completeness gates.
    Empty,
    /// The set of file paths that may contain matching rows.  The caller reads
    /// only these files and re-applies the full `@@` predicate for correctness.
    FileCandidates(HashSet<String>),
}

/// Row-group-granular variant of [`TsvProbeResult`].
///
/// Same conservative-superset contract as [`TsvProbeResult`].  Returned by
/// [`GinTsvectorRegistry::probe_query_with_row_groups`] for callers that can
/// drive `ReadOptions.row_group_selection` (e.g. the read-path
/// `GinRowGroupPrunedTable` provider).  The per-file `Vec<u32>` is sorted in
/// ascending order.
#[derive(Debug)]
pub enum TsvProbeRowGroupResult {
    /// No posting list, malformed query, or an unboundable structure.
    NoIndex,
    /// The structural evaluation is empty — short-circuit with zero rows
    /// (behind the overlay + completeness gates only).
    Empty,
    /// `file_path → sorted row-group indices`.  Files absent from this map
    /// must be read in full (no false negatives).
    FileRowGroups(HashMap<String, Vec<u32>>),
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── extract_lexemes ───────────────────────────────────────────────────────

    #[test]
    fn extract_lexemes_basic() {
        let lexemes = extract_lexemes("'cat':1 'dog':2,3 'run':4");
        assert_eq!(lexemes.len(), 3, "lexemes={lexemes:?}");
        assert!(lexemes.contains("cat"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("dog"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("run"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_lexemes_no_positions() {
        // tsvector values without positional annotations are valid.
        let lexemes = extract_lexemes("'hello' 'world'");
        assert_eq!(lexemes.len(), 2, "lexemes={lexemes:?}");
        assert!(lexemes.contains("hello"));
        assert!(lexemes.contains("world"));
    }

    #[test]
    fn extract_lexemes_weighted_positions() {
        // Weighted positions e.g. :1A,3B should be ignored.
        let lexemes = extract_lexemes("'rust':1A 'db':2B,3C");
        assert!(lexemes.contains("rust"), "lexemes={lexemes:?}");
        assert!(lexemes.contains("db"), "lexemes={lexemes:?}");
    }

    #[test]
    fn extract_lexemes_empty_string() {
        let lexemes = extract_lexemes("");
        assert!(lexemes.is_empty());
    }

    #[test]
    fn extract_lexemes_deduplicates() {
        // A tsvector should not have duplicate lexemes, but our extractor must
        // be safe if it does (e.g. from a raw string before normalisation).
        let lexemes = extract_lexemes("'cat':1 'cat':2");
        assert_eq!(lexemes.len(), 1);
    }

    // ── tsquery structural parser ─────────────────────────────────────────────

    #[test]
    fn parse_tsquery_and_or_not_phrase() {
        use TsqNode::*;
        assert_eq!(
            parse_tsquery("'cat' & 'dog'"),
            Some(And(Box::new(Term("cat".into())), Box::new(Term("dog".into()))))
        );
        assert_eq!(
            parse_tsquery("'cat' | 'dog'"),
            Some(Or(Box::new(Term("cat".into())), Box::new(Term("dog".into()))))
        );
        assert_eq!(
            parse_tsquery("!'dog'"),
            Some(Not(Box::new(Term("dog".into()))))
        );
        assert_eq!(
            parse_tsquery("'quick' <-> 'brown'"),
            Some(Phrase(Box::new(Term("quick".into())), Box::new(Term("brown".into()))))
        );
        assert_eq!(
            parse_tsquery("'quick' <2> 'fox'"),
            Some(Phrase(Box::new(Term("quick".into())), Box::new(Term("fox".into()))))
        );
        // Precedence: | binds loosest.
        assert_eq!(
            parse_tsquery("'a' | 'b' & 'c'"),
            Some(Or(
                Box::new(Term("a".into())),
                Box::new(And(Box::new(Term("b".into())), Box::new(Term("c".into()))))
            ))
        );
        // Parens override.
        assert_eq!(
            parse_tsquery("('a' | 'b') & 'c'"),
            Some(And(
                Box::new(Or(Box::new(Term("a".into())), Box::new(Term("b".into())))),
                Box::new(Term("c".into()))
            ))
        );
    }

    #[test]
    fn parse_tsquery_malformed_is_none() {
        assert_eq!(parse_tsquery(""), None);
        assert_eq!(parse_tsquery("   "), None);
        assert_eq!(parse_tsquery("'a' & ('b'"), None); // unbalanced paren
        assert_eq!(parse_tsquery("'a' 'b'"), None); // missing operator
        assert_eq!(parse_tsquery("'unterminated"), None);
    }

    // ── GinTsvectorRegistry: no-index path ───────────────────────────────────

    #[test]
    fn probe_query_no_index() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    // ── GinTsvectorRegistry: single-lexeme hit ────────────────────────────────

    #[test]
    fn probe_single_lexeme_hit() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1 'run':2", "f1.parquet", 0, 0);

        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "expected f1.parquet, got {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: multi-lexeme AND-merge ───────────────────────────

    #[test]
    fn probe_and_merge_two_files() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1 has both "cat" AND "dog".
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f1.parquet", 0, 0);
        // f2 has only "cat".
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f2.parquet", 0, 0);

        // Probe for "cat" AND "dog": only f1 should survive the AND-merge.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be a candidate: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 must be excluded: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    #[test]
    fn probe_and_merge_no_common_file() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1 has "cat" only; f2 has "dog" only.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f2.parquet", 0, 0);

        // No file has both lexemes → Empty.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        assert!(
            matches!(result, TsvProbeResult::Empty),
            "expected Empty when no file has both lexemes, got {result:?}"
        );
    }

    // ── GinTsvectorRegistry: OR unions file sets (soundness) ─────────────────

    #[test]
    fn probe_or_unions_files() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // "cat" only in f1; "dog" only in f2; "fish" only in f3.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f2.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'fish':1", "f3.parquet", 0, 0);

        // 'cat' | 'dog' must include BOTH f1 and f2 (an AND-merge would
        // intersect to ∅ and wrongly short-circuit) and may exclude f3.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' | 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "OR must keep f1: {files:?}");
                assert!(files.contains("f2.parquet"), "OR must keep f2: {files:?}");
                assert!(!files.contains("f3.parquet"), "OR may prune f3: {files:?}");
            }
            other => panic!("expected FileCandidates for OR, got {other:?}"),
        }
    }

    #[test]
    fn probe_or_with_unknown_branch_declines() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);

        // 'dog' was never indexed: the OR cannot be bounded → NoIndex.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' | 'dog'");
        assert!(
            matches!(result, TsvProbeResult::NoIndex),
            "OR with an unknown branch must decline to full scan, got {result:?}"
        );
    }

    // ── GinTsvectorRegistry: NOT declines / narrows soundly ─────────────────

    #[test]
    fn probe_not_alone_declines() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f1.parquet", 0, 0);

        // !'dog' rows may live in files with no indexed lexeme at all.
        let result = reg.probe_query(&proj, &tbl, "body", "!'dog'");
        assert!(
            matches!(result, TsvProbeResult::NoIndex),
            "bare NOT must decline to full scan, got {result:?}"
        );
    }

    #[test]
    fn probe_and_not_narrows_to_positive_side() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1: cat only.  f2: cat+dog.  f3: dog only.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f2.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f3.parquet", 0, 0);

        // 'cat' & !'dog': candidates must include EVERY file containing
        // 'cat' (f1 AND f2 — the !'dog' side cannot exclude f2 at file level
        // because another row of f2 may lack 'dog').  f3 is prunable.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & !'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be kept: {files:?}");
                assert!(files.contains("f2.parquet"), "f2 must be kept: {files:?}");
                assert!(!files.contains("f3.parquet"), "f3 may be pruned: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: phrase folds to AND ──────────────────────────────

    #[test]
    fn probe_phrase_intersects_files() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1 has both lexemes (order unknown to the index); f2 only "quick".
        reg.index_row(&proj, &tbl, "body", "'quick':1 'fox':2", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'quick':1", "f2.parquet", 0, 0);

        let result = reg.probe_query(&proj, &tbl, "body", "'quick' <-> 'fox'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be kept: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 may be pruned: {files:?}");
            }
            other => panic!("expected FileCandidates for phrase, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: miss path ────────────────────────────────────────

    #[test]
    fn probe_missing_lexeme_returns_no_index() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Index a doc with "cat" only.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);

        // Probe for "dog" — never indexed → NoIndex (conservative).
        let result = reg.probe_query(&proj, &tbl, "body", "'dog'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    #[test]
    fn probe_and_with_missing_lexeme_narrows_to_known_side() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'fish':1", "f2.parquet", 0, 0);

        // 'cat' & 'dog' where 'dog' was never indexed: the AND can still be
        // bounded by the known side — candidates = files('cat') = {f1}.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be kept: {files:?}");
                assert!(!files.contains("f2.parquet"), "f2 may be pruned: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: multiple row-groups / rows ───────────────────────

    #[test]
    fn probe_multiple_row_groups() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Same file, different row-groups and rows.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f1.parquet", 1, 5);

        // Both lexemes are in f1 (across different row-groups) → candidate.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeResult::FileCandidates(files) => {
                assert!(files.contains("f1.parquet"), "f1 must be a candidate: {files:?}");
            }
            other => panic!("expected FileCandidates, got {other:?}"),
        }
    }

    // ── GinTsvectorRegistry: remove_file ─────────────────────────────────────

    #[test]
    fn remove_file_clears_entries() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f1.parquet", 0, 0);
        reg.mark_file_indexed(&proj, &tbl, "body", "f1.parquet");

        reg.remove_file(&proj, &tbl, "body", "f1.parquet");

        // After removal the posting lists are empty for the indexed lexemes.
        // The probe may return Empty (set exists but empty) or NoIndex (evicted).
        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        assert!(
            matches!(result, TsvProbeResult::NoIndex | TsvProbeResult::Empty),
            "expected NoIndex or Empty after remove, got {result:?}"
        );
        // Completeness set no longer claims coverage of the removed file.
        assert!(
            !reg.indexed_files_for(&proj, &tbl, "body").contains("f1.parquet"),
            "remove_file must un-mark the file in the completeness set"
        );
    }

    // ── eviction un-marks affected files (per-file completeness) ─────────────

    #[test]
    fn evict_oldest_reports_affected_files() {
        let mut list = LexemePostingList::new();
        let f1 = list.intern_file("f1.parquet");
        let f2 = list.intern_file("f2.parquet");
        list.insert(
            "old_lexeme".into(),
            TsvPostingEntry { file_path: f1.clone(), row_group: 0, row: 0 },
        );
        list.insert(
            "new_lexeme".into(),
            TsvPostingEntry { file_path: f2.clone(), row_group: 0, row: 0 },
        );
        assert_eq!(list.total_count, 2);

        // Manually evict (budget-independent): the oldest 25% of 2 lexemes
        // is max(0,1)=1 lexeme → "old_lexeme", which references only f1.
        let affected = list.evict_oldest();
        assert!(affected.contains("f1.parquet"), "affected={affected:?}");
        assert!(!affected.contains("f2.parquet"), "affected={affected:?}");
        assert_eq!(list.total_count, 1);
        // The surviving lexeme still probes.
        assert!(list.probe_lexeme("new_lexeme").is_some());
        assert!(list.probe_lexeme("old_lexeme").is_none());
    }

    #[test]
    fn duplicate_insert_does_not_inflate_count() {
        let mut list = LexemePostingList::new();
        let f1 = list.intern_file("f1.parquet");
        let e = TsvPostingEntry { file_path: f1, row_group: 0, row: 0 };
        list.insert("cat".into(), e.clone());
        list.insert("cat".into(), e);
        assert_eq!(list.total_count, 1, "duplicate (lexeme, entry) must not double-count");
        assert_eq!(list.insert_order.len(), 1, "lexeme must appear once in insert order");
    }

    // ── GinTsvectorRegistry: total_entries ───────────────────────────────────

    #[test]
    fn total_entries_counts_correctly() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // Three lexemes → three posting entries.
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2 'run':3", "f1.parquet", 0, 0);
        assert_eq!(reg.total_entries(&proj, &tbl, "body"), 3);

        // Same lexeme in a different file → one more posting entry for each lexeme.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f2.parquet", 0, 0);
        assert_eq!(reg.total_entries(&proj, &tbl, "body"), 4);
    }

    // ── GinTsvectorRegistry: column isolation ─────────────────────────────────

    #[test]
    fn different_columns_are_isolated() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "title", "'cat':1", "f1.parquet", 0, 0);

        // Probe on "body" — different column, no index.
        let result = reg.probe_query(&proj, &tbl, "body", "'cat'");
        assert!(matches!(result, TsvProbeResult::NoIndex));
    }

    // ── GinTsvectorRegistry: row-group granularity round-trip ────────────────

    #[test]
    fn probe_query_with_row_groups_preserves_granularity() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        // f1: "cat" in rg 0 and rg 2, "dog" in rg 2.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'cat':1 'dog':2", "f1.parquet", 2, 7);
        // f2: "cat" only — must be excluded by the AND-merge below.
        reg.index_row(&proj, &tbl, "body", "'cat':1", "f2.parquet", 5, 0);

        let result =
            reg.probe_query_with_row_groups(&proj, &tbl, "body", "'cat' & 'dog'");
        match result {
            TsvProbeRowGroupResult::FileRowGroups(map) => {
                // Only f1 satisfies both lexemes (file-level AND).
                assert_eq!(map.len(), 1, "expected only f1, got {map:?}");
                let rgs = map.get("f1.parquet").expect("f1 missing");
                // Union of row-groups touched by "cat" (rg 0, rg 2) and "dog"
                // (rg 2) within f1: {0, 2}.  Sorted ascending.
                assert_eq!(rgs, &vec![0u32, 2u32], "row-group set wrong: {rgs:?}");
                assert!(!map.contains_key("f2.parquet"), "f2 must be excluded");
            }
            other => panic!("expected FileRowGroups, got {other:?}"),
        }
    }

    #[test]
    fn probe_query_with_row_groups_or_unions() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("docs").unwrap();

        reg.index_row(&proj, &tbl, "body", "'cat':1", "f1.parquet", 0, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f1.parquet", 3, 0);
        reg.index_row(&proj, &tbl, "body", "'dog':1", "f2.parquet", 1, 0);

        let result =
            reg.probe_query_with_row_groups(&proj, &tbl, "body", "'cat' | 'dog'");
        match result {
            TsvProbeRowGroupResult::FileRowGroups(map) => {
                assert_eq!(
                    map.get("f1.parquet"),
                    Some(&vec![0u32, 3u32]),
                    "f1 must carry the union of cat/dog row-groups: {map:?}"
                );
                assert_eq!(
                    map.get("f2.parquet"),
                    Some(&vec![1u32]),
                    "f2 must be kept by the OR: {map:?}"
                );
            }
            other => panic!("expected FileRowGroups for OR, got {other:?}"),
        }
    }

    // ── Smoke test: 50k rows ──────────────────────────────────────────────────

    #[test]
    fn fifty_thousand_rows_smoke() {
        let reg = GinTsvectorRegistry::new();
        let proj = ProjectId::new();
        let tbl = TableName::new("large_corpus").unwrap();

        // Index 50k rows, each with a unique lexeme "word_i" plus a shared
        // lexeme "common".
        for i in 0u64..50_000 {
            let tsv = format!("'word_{i}':1 'common':2");
            let file = format!("chunk_{}.parquet", i / 1000);
            reg.index_row(&proj, &tbl, "body", &tsv, &file, 0, i % 1000);
        }

        // Probing for "common" should return FileCandidates (every chunk file).
        let result = reg.probe_query(&proj, &tbl, "body", "'common'");
        assert!(
            matches!(result, TsvProbeResult::FileCandidates(_) | TsvProbeResult::NoIndex),
            "smoke probe returned unexpected {result:?}"
        );
    }
}
