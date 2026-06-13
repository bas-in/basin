//! Env-driven SIZE-LADDER config for the `ext_bench_*` extension benchmark
//! family — the analogue, for the extensions suite, of the `_10k`/`_100k`/`1M`
//! variant pattern the core `compare_postgres` benchmark publishes.
//!
//! ## Why this exists
//!
//! Every `ext_bench_<family>.rs` card already reads `BASIN_EXT_BENCH_ROWS` and
//! emits a SINGLE artifact at a SINGLE scale (`benchmark/data/ext_bench_<family>.json`).
//! The core benchmark instead publishes a *ladder* (10k / 100k / 1M) so a reader
//! can see how Basin-vs-PG scales. The size ladder for the extensions suite is
//! primarily a RUNNER concern (`benchmark/run/extensions-suite.sh`): the runner
//! invokes each family once per size point with `BASIN_EXT_BENCH_ROWS` set, then
//! routes the emitted artifact to a size-suffixed path
//! (`ext_bench_<family>_<label>.json`, under `data_seaweedfs/` when
//! `BASIN_BENCH_STORE=s3`).
//!
//! This module is the additive, test-side counterpart (mirrors the shape of
//! `ext_store_select`): it exposes the size-ladder config + the size-suffixed
//! artifact filename so future per-family cards can adopt size-suffixing
//! WITHOUT coupling to the runner's post-hoc `mv`. A card that wants to own its
//! own size-suffixed artifact name just calls
//! [`size_suffixed_artifact`]`("ext_bench_fts", "ext_bench_fts.json")`.
//!
//! ## Environment variables
//!
//! * `BASIN_EXT_BENCH_ROWS` — the active row count for THIS process (the card
//!   already reads this). [`active_rows`] re-reads it with the family default.
//! * `BASIN_EXT_BENCH_SIZE_LABEL` — optional explicit artifact label
//!   (e.g. `10k`, `100k`, `1m`, `128d_100k`). When set, it is used verbatim as
//!   the artifact suffix. When unset, [`size_label`] derives a human label from
//!   the active row count (`10000 -> "10k"`, `1000000 -> "1m"`, …).
//! * `BASIN_EXT_BENCH_SIZES` — the ladder the *runner* iterates
//!   (`"10000,100000,1000000"` default). Parsed by [`parse_sizes`] for any
//!   harness/tooling that wants the same list the runner uses. Not read by the
//!   cards directly (the runner sets `BASIN_EXT_BENCH_ROWS` per point).

#![allow(dead_code)]

/// Default size ladder the runner iterates when `BASIN_EXT_BENCH_SIZES` is
/// unset. Kept in sync with `benchmark/run/extensions-suite.sh`.
pub const DEFAULT_SIZES: &[usize] = &[10_000, 100_000, 1_000_000];

/// Parse a comma-separated size list (e.g. `"10000,100000,1000000"`) into a
/// deduplicated, ascending vector. Whitespace and empty entries are ignored.
/// Returns [`DEFAULT_SIZES`] when the string yields nothing parseable.
pub fn parse_sizes(spec: &str) -> Vec<usize> {
    let mut v: Vec<usize> = spec
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect();
    if v.is_empty() {
        return DEFAULT_SIZES.to_vec();
    }
    v.sort_unstable();
    v.dedup();
    v
}

/// The size ladder for the current process, from `BASIN_EXT_BENCH_SIZES`
/// (falling back to [`DEFAULT_SIZES`]).
pub fn sizes() -> Vec<usize> {
    match std::env::var("BASIN_EXT_BENCH_SIZES") {
        Ok(s) if !s.trim().is_empty() => parse_sizes(&s),
        _ => DEFAULT_SIZES.to_vec(),
    }
}

/// The active row count for THIS process: `BASIN_EXT_BENCH_ROWS` if set,
/// else `default` (the card's per-family default — e.g. 100_000 for most,
/// 1_000_000 for timescale).
pub fn active_rows(default: usize) -> usize {
    std::env::var("BASIN_EXT_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Human-readable label for a row count: `10_000 -> "10k"`, `1_000_000 -> "1m"`,
/// `10_000_000 -> "10m"`, `2_500 -> "2500"` (non-round counts kept verbatim).
pub fn label_for_rows(rows: usize) -> String {
    if rows == 0 {
        return "0".to_string();
    }
    if rows % 1_000_000 == 0 {
        format!("{}m", rows / 1_000_000)
    } else if rows % 1_000 == 0 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}

/// The artifact suffix label for the current process. Prefers an explicit
/// `BASIN_EXT_BENCH_SIZE_LABEL` (so the runner can compose e.g. `128d_100k`
/// for the vector card's dim×rows matrix); otherwise derives from the active
/// row count via [`label_for_rows`] with the given `default_rows`.
pub fn size_label(default_rows: usize) -> String {
    if let Ok(l) = std::env::var("BASIN_EXT_BENCH_SIZE_LABEL") {
        let l = l.trim();
        if !l.is_empty() {
            return l.to_string();
        }
    }
    label_for_rows(active_rows(default_rows))
}

/// Compose a size-suffixed artifact filename from a base filename.
///
/// `("ext_bench_fts.json", "100k") -> "ext_bench_fts_100k.json"`.
/// A filename without a `.json` extension just gets `_<label>` appended.
pub fn size_suffixed_filename(base_filename: &str, label: &str) -> String {
    match base_filename.strip_suffix(".json") {
        Some(stem) => format!("{stem}_{label}.json"),
        None => format!("{base_filename}_{label}"),
    }
}

/// Convenience: the size-suffixed artifact filename for the current process,
/// using the active size label. `card_default_rows` is the card's own default
/// row count (so the label is correct when `BASIN_EXT_BENCH_ROWS` is unset).
pub fn size_suffixed_artifact(base_filename: &str, card_default_rows: usize) -> String {
    size_suffixed_filename(base_filename, &size_label(card_default_rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sizes_basic() {
        assert_eq!(parse_sizes("10000,100000,1000000"), vec![10_000, 100_000, 1_000_000]);
        assert_eq!(parse_sizes(" 100000 , 10000 "), vec![10_000, 100_000]);
        assert_eq!(parse_sizes("100000,100000"), vec![100_000]);
        assert_eq!(parse_sizes(""), DEFAULT_SIZES.to_vec());
        assert_eq!(parse_sizes("garbage"), DEFAULT_SIZES.to_vec());
    }

    #[test]
    fn label_round_trips() {
        assert_eq!(label_for_rows(10_000), "10k");
        assert_eq!(label_for_rows(100_000), "100k");
        assert_eq!(label_for_rows(1_000_000), "1m");
        assert_eq!(label_for_rows(10_000_000), "10m");
        assert_eq!(label_for_rows(2_500), "2500");
        assert_eq!(label_for_rows(0), "0");
    }

    #[test]
    fn suffixing() {
        assert_eq!(size_suffixed_filename("ext_bench_fts.json", "100k"), "ext_bench_fts_100k.json");
        assert_eq!(size_suffixed_filename("ext_bench_vector.json", "128d_100k"), "ext_bench_vector_128d_100k.json");
        assert_eq!(size_suffixed_filename("noext", "1m"), "noext_1m");
    }
}
