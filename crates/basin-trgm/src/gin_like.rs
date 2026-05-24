//! `(I)LIKE` candidate pruning via a trigram GIN index.
//!
//! This is the sibling of [`crate::gin::TrigramIndex`] (which scores
//! Jaccard *similarity*). Here the goal is different: given a SQL
//! `LIKE` / `ILIKE` pattern such as `'%@gmail.com'`, return the *set of
//! rows that could possibly match* so the engine can prune a sequential
//! scan down to a handful of candidates and then re-check each with the
//! real pattern matcher.
//!
//! ## Why trigrams prune `LIKE`
//!
//! If a row matches `LIKE '%abc%'`, then the row's text literally
//! contains the substring `abc`, and therefore contains every
//! contiguous 3-byte window of `abc` (here just `abc` itself). More
//! generally, every literal run of length ≥ 3 between wildcards
//! contributes a set of *required* trigrams: a matching row's text must
//! contain all of them. So the candidate set is exactly the rows whose
//! indexed trigram set is a **superset** of the pattern's required
//! trigrams — a multi-key AND lookup over the inverted index.
//!
//! This mirrors Postgres's `pg_trgm` `LIKE`/`ILIKE` support
//! (`generate_wildcard_trgm` + the GIN `trgm_ops` `extractQuery` path).
//!
//! ## Correctness invariant (no false negatives)
//!
//! [`TrigramGinIndex::candidates`] returns a **superset** of the true
//! matches. It must never drop a row that genuinely matches the
//! pattern, or the engine would return wrong results. Two rules keep
//! this safe:
//!
//! 1. Literal runs are turned into trigrams with **no word-boundary
//!    padding**. The normal [`crate::extract`] pads each word with
//!    `"  "`/`" "` and splits on non-alphanumerics; that is wrong for
//!    `LIKE`, where `'@gmail.com'` is one contiguous run the row must
//!    contain verbatim. Padding or splitting could invent trigrams the
//!    matching row never stores, dropping real matches.
//! 2. When the pattern yields **no** required trigrams (every literal
//!    run is shorter than 3 chars, e.g. `'%a%'` or `'_b_'`), the index
//!    cannot prune. [`candidates`](TrigramGinIndex::candidates) then
//!    returns [`Candidates::All`] — the correct, if unhelpful, fallback
//!    of "scan everything".

use std::collections::HashSet;

/// Row identifier used by the index. Matches the engine's row-id width.
pub type RowId = u64;

/// A required trigram extracted from a `LIKE`/`ILIKE` pattern.
///
/// Three bytes, like [`crate::Trigram`], but produced by
/// [`trigrams_for_pattern`] rather than [`crate::extract`]: pattern
/// trigrams are the raw sliding windows of a literal run, **without** the
/// word padding the similarity path uses.
pub type Trigram = [u8; 3];

/// Result of [`TrigramGinIndex::candidates`].
///
/// Either a concrete pruned set, or the "can't prune" sentinel telling
/// the caller to fall back to a full scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Candidates {
    /// The pattern produced usable trigrams; only these rows can match.
    /// Always a superset of the true matches — the caller still re-checks
    /// each row against the real pattern.
    Some(HashSet<RowId>),
    /// The pattern was too short / too wildcard-heavy to prune. The
    /// caller must consider **every** row a candidate (full scan).
    All,
}

impl Candidates {
    /// Convenience: materialise the candidate set against a known row
    /// universe. For [`Candidates::All`] this clones `universe`; for
    /// [`Candidates::Some`] it returns the pruned set directly.
    ///
    /// Most callers will instead branch on the variant to avoid
    /// materialising the universe at all when the result is `All`.
    pub fn resolve(self, universe: &HashSet<RowId>) -> HashSet<RowId> {
        match self {
            Candidates::Some(set) => set,
            Candidates::All => universe.clone(),
        }
    }
}

/// Extract the **required** trigrams of a SQL `LIKE`/`ILIKE` pattern.
///
/// The pattern is split on the SQL wildcards `%` (any run) and `_` (any
/// single char); `\` escapes the following character so a literal `%`,
/// `_`, or `\` can appear in a run. For each maximal literal run of
/// length ≥ 3, every contiguous 3-byte window is emitted. Runs shorter
/// than 3 bytes contribute nothing (they can't pin down a trigram).
///
/// When `case_insensitive` is `true` (the `ILIKE` case) the literal runs
/// are ASCII-lowercased first, matching the lowercasing the index does
/// at [`TrigramGinIndex::insert`] time.
///
/// The output is sorted and deduplicated. An empty result means "no
/// trigram constraint" — see [`Candidates::All`].
///
/// # Examples
///
/// ```
/// use basin_trgm::gin_like::trigrams_for_pattern;
/// // '%@gmail.com' → the run "@gmail.com" → windows "@gm","gma",...
/// let tg = trigrams_for_pattern("%@gmail.com", true);
/// assert!(tg.contains(&[b'@', b'g', b'm']));
/// assert!(tg.contains(&[b'c', b'o', b'm']));
/// // Too short to prune:
/// assert!(trigrams_for_pattern("%a%", false).is_empty());
/// ```
pub fn trigrams_for_pattern(pattern: &str, case_insensitive: bool) -> Vec<Trigram> {
    let mut out = Vec::new();
    let mut run: Vec<u8> = Vec::new();

    // Flush the accumulated literal run into trigrams, then clear it.
    let flush = |run: &mut Vec<u8>, out: &mut Vec<Trigram>| {
        if run.len() >= 3 {
            for w in run.windows(3) {
                out.push([w[0], w[1], w[2]]);
            }
        }
        run.clear();
    };

    let bytes = pattern.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\\' => {
                // Escaped char: the *next* byte is a literal, wildcard
                // status stripped. A trailing backslash escapes nothing.
                if i + 1 < bytes.len() {
                    let lit = bytes[i + 1];
                    run.push(fold(lit, case_insensitive));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            b'%' | b'_' => {
                // Wildcard breaks the literal run.
                flush(&mut run, &mut out);
                i += 1;
            }
            _ => {
                run.push(fold(b, case_insensitive));
                i += 1;
            }
        }
    }
    flush(&mut run, &mut out);

    out.sort_unstable();
    out.dedup();
    out
}

#[inline]
fn fold(b: u8, case_insensitive: bool) -> u8 {
    if case_insensitive {
        b.to_ascii_lowercase()
    } else {
        b
    }
}

/// GIN-style inverted index over **substring** trigrams, for `LIKE` /
/// `ILIKE` candidate pruning.
///
/// Unlike [`crate::gin::TrigramIndex`], this index stores the trigrams of
/// each row's text as raw contiguous windows (no word padding), so a
/// `candidates` lookup can ask "which rows contain substring `S`?"
/// soundly. Build it once over a column, then call
/// [`candidates`](Self::candidates) per query pattern.
#[derive(Debug, Clone, Default)]
pub struct TrigramGinIndex {
    /// Inverted index: trigram → set of row-ids whose text contains it.
    inverted: std::collections::HashMap<Trigram, HashSet<RowId>>,
    /// Every row-id ever inserted (the universe, for the `All` fallback
    /// and for handling rows too short to have any trigram).
    rows: HashSet<RowId>,
}

impl TrigramGinIndex {
    /// Empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build an index from an iterator of `(row_id, text)` pairs.
    ///
    /// Each row contributes both its **case-preserved** substring
    /// trigrams and its **ASCII-lowercased** ones (see
    /// [`insert`](Self::insert) for why). This single index therefore
    /// serves *both* case-sensitive `LIKE` and case-insensitive `ILIKE`
    /// soundly.
    pub fn build<'a, I>(rows: I) -> Self
    where
        I: IntoIterator<Item = (RowId, &'a str)>,
    {
        let mut idx = Self::new();
        for (row_id, text) in rows {
            idx.insert(row_id, text);
        }
        idx
    }

    /// Number of indexed rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the index has any rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The full row universe (every row inserted). Useful for the caller
    /// when [`candidates`](Self::candidates) returns [`Candidates::All`].
    pub fn universe(&self) -> &HashSet<RowId> {
        &self.rows
    }

    /// Insert (or re-insert) a row's text.
    ///
    /// Stores the row's raw substring trigrams (contiguous 3-byte
    /// windows, no word padding) in **two** forms: case-preserved and
    /// ASCII-lowercased. Storing both keeps a single index sound for
    /// both case-sensitive `LIKE` (query trigrams keep their case, and a
    /// matching row's case-preserved trigrams contain them) and `ILIKE`
    /// (query trigrams are lowercased, matched against the row's
    /// lowercased trigrams). The cost is at most 2× the postings of a
    /// single-case index; in practice far less since many trigrams are
    /// already lowercase and dedupe away.
    ///
    /// Re-inserting the same `row_id` does **not** purge stale trigrams —
    /// callers that update rows should [`remove`](Self::remove) first.
    /// (The `build` path inserts each row once, so this is only a concern
    /// for incremental maintenance.)
    pub fn insert(&mut self, row_id: RowId, text: &str) {
        self.rows.insert(row_id);
        for tg in substring_trigrams(text) {
            self.inverted.entry(tg).or_default().insert(row_id);
        }
    }

    /// Remove a row from every posting list and the universe. No-op if
    /// the row was never inserted.
    pub fn remove(&mut self, row_id: RowId) {
        if !self.rows.remove(&row_id) {
            return;
        }
        self.inverted.retain(|_, set| {
            set.remove(&row_id);
            !set.is_empty()
        });
    }

    /// Return the rows that could match `pattern` under `LIKE`/`ILIKE`.
    ///
    /// `case_insensitive = true` is the `ILIKE` path. The result is
    /// always a **superset** of the true matches; the caller must
    /// re-check each candidate against the actual pattern (the index
    /// cannot, for example, verify that the literal runs appear in the
    /// right *order* or that `_` matched exactly one char).
    ///
    /// Returns [`Candidates::All`] when the pattern yields no usable
    /// trigrams (all literal runs shorter than 3 bytes) — the index can
    /// prune nothing and the caller must scan every row.
    ///
    /// ## Soundness for both `LIKE` and `ILIKE`
    ///
    /// The index stores each row's trigrams in both case-preserved and
    /// lowercased form (see [`insert`](Self::insert)). So a case-
    /// sensitive `LIKE` query (`case_insensitive = false`, query trigrams
    /// keep their case) and an `ILIKE` query (`case_insensitive = true`,
    /// query trigrams lowercased) are *both* matched against a stored
    /// superset — no false negatives either way.
    pub fn candidates(&self, pattern: &str, case_insensitive: bool) -> Candidates {
        let required = trigrams_for_pattern(pattern, case_insensitive);
        if required.is_empty() {
            // No trigram constraint → cannot prune.
            return Candidates::All;
        }

        // Multi-key AND: a candidate must appear in *every* required
        // trigram's posting list. Start from the smallest posting list
        // (cheapest seed) and intersect the rest.
        let mut lists: Vec<&HashSet<RowId>> = Vec::with_capacity(required.len());
        for tg in &required {
            match self.inverted.get(tg) {
                Some(set) => lists.push(set),
                // A required trigram with an empty posting list means no
                // row contains it → no row can match → empty candidate set.
                None => return Candidates::Some(HashSet::new()),
            }
        }
        lists.sort_by_key(|s| s.len());

        let mut acc: HashSet<RowId> = lists[0].clone();
        for list in &lists[1..] {
            acc.retain(|r| list.contains(r));
            if acc.is_empty() {
                break;
            }
        }
        Candidates::Some(acc)
    }
}

/// Substring trigrams of `text` for indexing: every contiguous 3-byte
/// window of the **case-preserved** input *and* of its **ASCII-
/// lowercased** form, sorted and deduped. Unlike [`crate::extract`],
/// there is **no** word splitting or padding — the row is treated as one
/// byte run so `LIKE` substring matching is sound.
///
/// Emitting both casings lets one index serve case-sensitive `LIKE`
/// (query trigrams keep their case) and `ILIKE` (query trigrams are
/// lowercased) without false negatives.
fn substring_trigrams(text: &str) -> Vec<Trigram> {
    let raw = text.as_bytes();
    let lowered: Vec<u8> = raw.iter().map(|b| b.to_ascii_lowercase()).collect();
    let mut out: Vec<Trigram> = Vec::with_capacity(raw.len().saturating_sub(2) * 2);
    out.extend(raw.windows(3).map(|w| [w[0], w[1], w[2]]));
    out.extend(lowered.windows(3).map(|w| [w[0], w[1], w[2]]));
    out.sort_unstable();
    out.dedup();
    out
}
