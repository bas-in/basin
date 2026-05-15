//! Storage tier classification for a single Parquet data file.
//!
//! Phase 5.5: per-table age policy moves cold files (those whose timestamp
//! column's max is older than the configured threshold) from the default
//! "hot" prefix (`tables/{t}/data/...`) to a "cold" prefix
//! (`tables/{t}/cold/...`). Reads transparently follow whichever path the
//! catalog records; the compactor enforces the policy.
//!
//! On real deployments, the cold prefix maps to a cheaper storage class
//! (S3 Infrequent Access, Tigris infrequent-access, or provider equivalent) via a bucket
//! lifecycle rule. Wiring that up is out of scope for the in-process viability test —
//! we only need the path layout + atomic catalog swap to be honest.

use serde::{Deserialize, Serialize};

/// Hot vs cold tier for a single data file. New variants are intentionally
/// additive — existing manifests deserialise to [`Tier::Hot`] thanks to
/// `#[serde(default)]` on the surrounding fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Tier {
    /// Standard / hot tier. The default for fresh writes.
    #[default]
    Hot,
    /// Cold tier. Recovered from any Parquet file that happens to live under
    /// the `cold/` segment of a project's table.
    Cold,
}

impl Tier {
    /// Path segment used in object keys to denote this tier. `data` for hot,
    /// `cold` for cold. Centralising the literal here is the single source of
    /// truth — every reader / writer / compactor branch consults this.
    pub fn segment(self) -> &'static str {
        match self {
            Tier::Hot => "data",
            Tier::Cold => "cold",
        }
    }

    /// Best-effort derivation from an object key. Returns [`Tier::Cold`] iff
    /// the key contains a `/cold/` (or leading `cold/`) segment under the
    /// project table prefix; otherwise [`Tier::Hot`]. The catalog never has to
    /// remember the tier explicitly because the path is self-describing —
    /// callers serialise the path round-trip and recover the tier on load.
    pub fn from_path(path: &str) -> Self {
        // Match the `cold` segment only when it appears in the tier slot —
        // i.e. directly under `tables/<name>/`. A column named `cold` or a
        // partition value of `cold` would otherwise false-positive.
        if path_has_cold_segment(path) {
            Tier::Cold
        } else {
            Tier::Hot
        }
    }
}

fn path_has_cold_segment(path: &str) -> bool {
    // Pattern: `.../tables/<table>/cold/...`
    // We scan for the literal `/tables/` and then check whether the segment
    // two slashes ahead is `cold`.
    let needle = "/tables/";
    let mut start = 0usize;
    while let Some(idx) = path[start..].find(needle) {
        let after = start + idx + needle.len();
        // Skip table name segment.
        let rest = &path[after..];
        if let Some(next_slash) = rest.find('/') {
            let tier_slot_start = after + next_slash + 1;
            let tier_rest = &path[tier_slot_start..];
            if let Some(end) = tier_rest.find('/') {
                if &tier_rest[..end] == "cold" {
                    return true;
                }
            } else if tier_rest == "cold" {
                return true;
            }
        }
        start = after;
    }
    // Also handle the bucket-relative form where the leading slash is omitted
    // and `tables/` sits at position 0.
    if let Some(rest) = path.strip_prefix("tables/") {
        if let Some(slash) = rest.find('/') {
            let tier_slot = &rest[slash + 1..];
            if let Some(end) = tier_slot.find('/') {
                if &tier_slot[..end] == "cold" {
                    return true;
                }
            } else if tier_slot == "cold" {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_hot() {
        assert_eq!(Tier::default(), Tier::Hot);
    }

    #[test]
    fn segment_strings() {
        assert_eq!(Tier::Hot.segment(), "data");
        assert_eq!(Tier::Cold.segment(), "cold");
    }

    #[test]
    fn from_path_recognises_cold() {
        let hot = "projects/01HABC/tables/events/data/2026/04/30/X.parquet";
        let cold = "projects/01HABC/tables/events/cold/2026/04/30/X.parquet";
        let cold_part = "projects/01HABC/tables/events/cold/year=2026/month=04/X.parquet";
        let with_root = "warehouse/projects/01HABC/tables/events/cold/X.parquet";
        assert_eq!(Tier::from_path(hot), Tier::Hot);
        assert_eq!(Tier::from_path(cold), Tier::Cold);
        assert_eq!(Tier::from_path(cold_part), Tier::Cold);
        assert_eq!(Tier::from_path(with_root), Tier::Cold);
    }

    #[test]
    fn from_path_does_not_misclassify_columns_named_cold() {
        // A partition value or sub-segment shouldn't trigger.
        let p = "projects/01HABC/tables/events/data/cold=true/X.parquet";
        assert_eq!(Tier::from_path(p), Tier::Hot);
    }

    #[test]
    fn serde_default_yields_hot() {
        // Old manifests: a struct with `#[serde(default)] tier: Tier` and no
        // tier field present must round-trip to Hot.
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            tier: Tier,
        }
        let w: Wrapper = serde_json::from_str("{}").unwrap();
        assert_eq!(w.tier, Tier::Hot);

        let w: Wrapper = serde_json::from_str(r#"{"tier":"cold"}"#).unwrap();
        assert_eq!(w.tier, Tier::Cold);
    }
}
