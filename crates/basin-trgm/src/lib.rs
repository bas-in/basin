//! `basin-trgm` — a `pg_trgm`-compatible subset for fuzzy text matching.
//!
//! ## v0.1 surface
//!
//! Three functions and one debug helper that together cover the bulk of
//! production `pg_trgm` use (typo tolerance, autocomplete, dedup):
//!
//! - [`similarity`] — `pg_trgm.similarity(a, b) -> real`. Jaccard index
//!   over trigram sets, in `[0.0, 1.0]`.
//! - [`word_similarity`] — `pg_trgm.word_similarity(a, b) -> real`. Best
//!   per-word match (see module docs in [`similarity`] for the exact
//!   definition vs. Postgres).
//! - [`show_trgm`] — `pg_trgm.show_trgm(t) -> text[]`. Debug helper:
//!   returns the trigram set as a `Vec<String>` so test cards can print
//!   them.
//! - [`trigram::extract`] — the underlying byte-trigram extractor.
//!   Exposed for callers that want to compute and persist their own
//!   trigram sets (the v0.2 GIN-style index will use this).
//!
//! ## SQL-surface staging
//!
//! Same pattern as `basin-cron` / `basin-net` / `basin-cv` / `basin-geo`:
//! this crate ships a Rust API in v0.1, and the engine glue
//! (`basin-engine::trgm_glue`) is the no-op stub where the future
//! DataFusion `ScalarUDF` registrations and operator-rewrite hooks for
//! `%`, `<%`, and `<->` will land. Until that wiring is built, viability
//! tests and application code call this crate's Rust API directly.
//!
//! ## What's NOT in v0.1
//!
//! - **GIN / GIST trigram index.** Without an inverted index over the
//!   trigram column, `WHERE name % 'foo'` still scans every partition.
//!   That's fine for tens of thousands of rows per tenant but quadratic
//!   on cross-tenant joins. The fix in v0.2 is a basin-storage-side
//!   trigram-index segment format mirroring the HNSW vector index — the
//!   trigram → row-id postings list is the only structural change
//!   needed; everything else (similarity scoring, threshold filter)
//!   already lives here.
//! - **SQL operators `%`, `<%`, `<->`.** The Rust API is the v0.1
//!   surface. The planner-level operator recognition lives in the
//!   `trgm_glue` stub and is the obvious next wiring step.
//! - **`set_limit()` / `pg_trgm.similarity_threshold` GUC.** The Rust
//!   API takes the threshold inline; the GUC wiring is a v0.2 add.
//! - **Locale-aware case folding.** v0.1 case-folds with
//!   `to_ascii_lowercase`. Non-ASCII multibyte runs are treated as word
//!   separators (matching `pg_trgm`'s `iswordchr` for the ASCII subset).

#![forbid(unsafe_code)]

pub mod similarity;
pub mod trigram;

pub use similarity::{similarity, word_similarity};
pub use trigram::{extract, set_bytes, Trigram};

/// Default v0.2 SQL operator threshold for `a % b` (`pg_trgm.similarity_threshold`).
/// Exposed as a constant so the engine's planner glue can lift it
/// directly when the operator surface lands.
pub const DEFAULT_SIMILARITY_THRESHOLD: f32 = 0.3;

/// Default v0.2 SQL operator threshold for `a <% b`
/// (`pg_trgm.word_similarity_threshold`).
pub const DEFAULT_WORD_SIMILARITY_THRESHOLD: f32 = 0.6;

/// `show_trgm(t)` — debug helper. Returns the trigram set of `t` as a
/// `Vec<String>`; mirrors `pg_trgm.show_trgm(text) -> text[]`.
///
/// The output is sorted (lexicographically by byte order) so callers
/// comparing against a reference list don't have to re-sort.
pub fn show_trgm(t: &str) -> Vec<String> {
    trigram::extract(t)
        .into_iter()
        .map(|tg| {
            // Trigrams are ASCII (case-folded), so the bytes always
            // round-trip into a `String` losslessly.
            String::from_utf8_lossy(&tg).into_owned()
        })
        .collect()
}
