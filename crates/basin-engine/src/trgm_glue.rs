//! Glue point for `basin-trgm`.
//!
//! `basin-trgm` is the `pg_trgm`-compatible fuzzy-text-matching layer
//! (`similarity`, `word_similarity`, `show_trgm`, plus the operator
//! sketches `%`, `<%`, `<->`). To keep the dependency graph
//! one-directional we cannot pull `basin-trgm` into `basin-engine`; what
//! we can do here is reserve the symbol the engine calls during
//! [`crate::Engine::new`] so a future wiring has an obvious, named
//! landing spot in the source tree.
//!
//! Today's implementation is a deliberate no-op: the v0.1 surface
//! (`similarity`, `word_similarity`, `show_trgm`) is exercised by calling
//! `basin_trgm::*` directly from application code and viability tests.
//! The same staging pattern is used by `basin-cron` for
//! `cron.schedule(...)`, `basin-net` for `http_get(...)` /
//! `net.http_post(...)`, `basin-cv` for
//! `CALL basin.refresh_continuous_aggregate(...)`, and `basin-geo` for
//! `ST_Distance` / `ST_DWithin` / `ST_MakePoint`.
//!
//! ## v0.2 plan
//!
//! When the SQL surface is wired up, this stub becomes the registration
//! site for the scalar UDF set and the operator-rewrite hooks:
//!
//! 1. **ScalarUDFs**. Register one DataFusion `ScalarUDF` per public
//!    function, each thin-wrapping the `basin_trgm` Rust API:
//!
//!    | UDF                | Signature                  | Backed by                       |
//!    |--------------------|----------------------------|---------------------------------|
//!    | `similarity`       | `(TEXT, TEXT) -> REAL`     | `basin_trgm::similarity`        |
//!    | `word_similarity`  | `(TEXT, TEXT) -> REAL`     | `basin_trgm::word_similarity`   |
//!    | `show_trgm`        | `(TEXT) -> TEXT[]`         | `basin_trgm::show_trgm`         |
//!
//! 2. **Operator rewrites** in the SQL planner's pre-analysis pass:
//!
//!    | Operator | Rewrite                                                   | Backed by                                   |
//!    |----------|-----------------------------------------------------------|---------------------------------------------|
//!    | `a % b`  | `similarity(a, b) >= pg_trgm.similarity_threshold`        | `basin_trgm::DEFAULT_SIMILARITY_THRESHOLD`  |
//!    | `a <% b` | `word_similarity(a, b) >= pg_trgm.word_similarity_threshold` | `basin_trgm::DEFAULT_WORD_SIMILARITY_THRESHOLD` |
//!    | `a <-> b`| `1.0 - similarity(a, b)`                                  | `basin_trgm::similarity`                    |
//!
//! 3. **Threshold GUCs**. Recognise `SET pg_trgm.similarity_threshold =
//!    0.4` and `SET pg_trgm.word_similarity_threshold = 0.5` in the
//!    session-config layer. Default values come from
//!    `basin_trgm::DEFAULT_SIMILARITY_THRESHOLD` /
//!    `DEFAULT_WORD_SIMILARITY_THRESHOLD`.
//!
//! 4. **GIN-style trigram index** in `basin-storage`. Without an
//!    inverted trigram-postings index, `WHERE name % 'foo'` is still a
//!    brute-force partition scan; the structural change needed is a
//!    `trigram → row-id` postings format mirrored on the HNSW vector
//!    index. The scoring math (this crate) doesn't change.
//!
//! See `basin-trgm`'s crate docs for the full Rust API and the v0.2
//! TODO list.

/// Hook called once from [`crate::Engine::new`]. No-op today; reserved
/// for the future SQL-surface wiring described in the module doc.
#[inline]
pub(crate) fn install() {
    // Intentionally empty. See the module-level doc for why.
}
