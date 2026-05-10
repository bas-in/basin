//! `TrigramIndex` — GIN-style inverted index for trigram similarity search.
//!
//! Mirrors the structural shape of Postgres's `pg_trgm` GIN opclass: a
//! per-trigram posting list mapping trigram → row-ids. Search extracts
//! the query trigrams, looks each up in the inverted index, accumulates
//! per-candidate hit counts, and computes the same Jaccard score that
//! [`crate::similarity`] returns — over the candidate set only, not the
//! full corpus.
//!
//! ## Correctness invariant
//!
//! The score this index reports for `(row_id, query)` MUST equal
//! `similarity(query, original_text_of(row_id))` to within f32 noise.
//! The `gin_results_match_brute_force` integration test pins this.
//!
//! Why it holds: Jaccard = `hits / (q + r - hits)` where `q` is the
//! query trigram count, `r` is the row trigram count, and `hits` is the
//! intersection cardinality. The inverted index gives us `hits` exactly
//! (each query trigram present in the row contributes 1), and we store
//! `r` per row in `row_trigram_counts`. So the formula is identical to
//! the brute-force path.
//!
//! ## v0.1 scope
//!
//! Pure in-memory Rust API. No SQL surface. No persistence. The
//! basin-storage segment format mirroring this for on-disk indexes is
//! the next step (gated on 5.7 B1 secondary-index work).

use std::collections::HashMap;

use crate::trigram::{self, Trigram};

/// In-memory GIN-style trigram index.
///
/// Cheap to clone via `Mutex<TrigramIndex>` for concurrent inserts +
/// reads; PG's GIN is similarly lock-managed at the page level.
#[derive(Debug, Default, Clone)]
pub struct TrigramIndex {
    /// Inverted index: trigram → sorted list of row_ids that contain it.
    /// Sorted so multi-trigram intersection / union can use merge-style
    /// algorithms.
    inverted: HashMap<Trigram, Vec<u64>>,
    /// Per-row trigram count (for similarity normalisation).
    row_trigram_counts: HashMap<u64, usize>,
}

impl TrigramIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of indexed rows.
    pub fn len(&self) -> usize {
        self.row_trigram_counts.len()
    }

    /// Whether the index has any rows.
    pub fn is_empty(&self) -> bool {
        self.row_trigram_counts.is_empty()
    }

    /// Insert (or replace) a row.
    ///
    /// If `row_id` already exists in the index, its old postings are
    /// removed before the new ones are added — so re-inserting is
    /// equivalent to update.
    pub fn insert(&mut self, row_id: u64, text: &str) {
        if self.row_trigram_counts.contains_key(&row_id) {
            self.remove(row_id);
        }
        let trigrams = trigram::extract(text);
        for tg in &trigrams {
            let bucket = self.inverted.entry(*tg).or_default();
            // Insert in sorted order so postings stay sorted.
            match bucket.binary_search(&row_id) {
                Ok(_) => { /* already present (shouldn't happen post-remove) */ }
                Err(pos) => bucket.insert(pos, row_id),
            }
        }
        self.row_trigram_counts.insert(row_id, trigrams.len());
    }

    /// Remove a row. No-op if `row_id` isn't indexed.
    pub fn remove(&mut self, row_id: u64) {
        if self.row_trigram_counts.remove(&row_id).is_none() {
            return;
        }
        // Walk every bucket — simpler than tracking per-row trigram
        // sets, and the cost is bounded by the total posting count.
        // Buckets that empty out are deleted to keep the map tight.
        self.inverted.retain(|_, bucket| {
            if let Ok(pos) = bucket.binary_search(&row_id) {
                bucket.remove(pos);
            }
            !bucket.is_empty()
        });
    }

    /// Find row_ids whose text has `similarity >= threshold` to `query`.
    /// Returns `(row_id, score)` pairs sorted by descending score (ties
    /// broken by ascending row_id for determinism). At most `limit`
    /// rows are returned.
    pub fn search(&self, query: &str, threshold: f32, limit: usize) -> Vec<(u64, f32)> {
        let q = trigram::extract(query);
        if q.is_empty() || limit == 0 {
            return Vec::new();
        }
        let q_size = q.len();

        // Accumulate per-candidate hit counts via the inverted index.
        let mut hits: HashMap<u64, usize> = HashMap::new();
        for tg in &q {
            if let Some(bucket) = self.inverted.get(tg) {
                for row_id in bucket {
                    *hits.entry(*row_id).or_insert(0) += 1;
                }
            }
        }

        score_and_rank(&hits, q_size, &self.row_trigram_counts, threshold, limit)
    }

    /// Word-similarity variant: returns rows where some word of `query`
    /// has trigram overlap with the row, scored as the row's max-over-
    /// query-words Jaccard.
    ///
    /// Mirrors [`crate::word_similarity`] but with `query` and the row
    /// text swapped in role: each word of `query` is scored against
    /// the row's full trigram set, and the row keeps its best.
    pub fn search_word_similarity(
        &self,
        query: &str,
        threshold: f32,
        limit: usize,
    ) -> Vec<(u64, f32)> {
        if limit == 0 {
            return Vec::new();
        }

        // Per-row best score across all query words.
        let mut best: HashMap<u64, f32> = HashMap::new();

        let bytes = query.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            while i < bytes.len() && !bytes[i].is_ascii_alphanumeric() {
                i += 1;
            }
            if i >= bytes.len() {
                break;
            }
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
                i += 1;
            }
            let word = std::str::from_utf8(&bytes[start..i]).unwrap_or("");
            let word_q = trigram::extract(word);
            if word_q.is_empty() {
                continue;
            }
            let q_size = word_q.len();

            let mut hits: HashMap<u64, usize> = HashMap::new();
            for tg in &word_q {
                if let Some(bucket) = self.inverted.get(tg) {
                    for row_id in bucket {
                        *hits.entry(*row_id).or_insert(0) += 1;
                    }
                }
            }

            for (row_id, h) in &hits {
                let r_size = match self.row_trigram_counts.get(row_id) {
                    Some(c) => *c,
                    None => continue,
                };
                let union = q_size + r_size - h;
                if union == 0 {
                    continue;
                }
                let score = *h as f32 / union as f32;
                let entry = best.entry(*row_id).or_insert(0.0);
                if score > *entry {
                    *entry = score;
                }
            }
        }

        let mut out: Vec<(u64, f32)> = best
            .into_iter()
            .filter(|(_, s)| *s >= threshold)
            .collect();
        // Descending score, ascending row_id on ties for determinism.
        out.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        out.truncate(limit);
        out
    }
}

/// Convert per-row hit counts into Jaccard scores, filter, sort, truncate.
fn score_and_rank(
    hits: &HashMap<u64, usize>,
    q_size: usize,
    row_trigram_counts: &HashMap<u64, usize>,
    threshold: f32,
    limit: usize,
) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = Vec::with_capacity(hits.len());
    for (row_id, h) in hits {
        let r_size = match row_trigram_counts.get(row_id) {
            Some(c) => *c,
            None => continue,
        };
        // Same Jaccard formula as `similarity_of_sets`: q + r - hits in
        // the denominator. This is what guarantees the GIN-vs-brute-
        // force correctness invariant.
        let union = q_size + r_size - h;
        if union == 0 {
            continue;
        }
        let score = *h as f32 / union as f32;
        if score >= threshold {
            scored.push((*row_id, score));
        }
    }
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.truncate(limit);
    scored
}
