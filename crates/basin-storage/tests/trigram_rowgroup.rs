//! Integration tests for `basin_storage::index::trigram_rowgroup` — the
//! persisted per-row-group trigram summary that powers `LIKE` / `ILIKE`
//! sub-file pruning.
//!
//! # Fairness rule
//!
//! Every text corpus and probe pattern in this file is GENERIC and
//! synthetic — phonetic-alphabet tokens ("alpha", "bravo", "charlie",
//! "kilo", "tango", "uniform", "zulu") and `uniform_<N>` synthetic IDs.
//! There is intentionally no bench-specific literal (no `@gmail.com`,
//! `pending`, `status`, etc.); the assertions key on the API shape
//! (RowGroupProbe variants, surviving-rg cardinality) — never on a
//! data-specific value.

use basin_common::{ProjectId, TableName};
use basin_storage::index::trigram_rowgroup::{RowGroupProbe, Trigram, TrigramRowGroupRegistry};

/// Build the case-preserved trigram windows of `s` — the same shape
/// `basin_engine::index_probe::trigrams_for_pattern` returns for a
/// pattern's literal run. Production callers should use that function;
/// tests use this tiny helper to avoid a basin-engine dev-dep.
fn tg(s: &str) -> Vec<Trigram> {
    let b = s.as_bytes();
    b.windows(3).map(|w| [w[0], w[1], w[2]]).collect()
}

fn proj_tbl() -> (ProjectId, TableName) {
    (ProjectId::new(), TableName::new("docs").unwrap())
}

/// Build text across 4 row-groups where a substring exists in ONLY rg=2;
/// the probe must return [2] (modulo bloom FP, but the small distinct
/// corpus makes FP vanishingly unlikely at 1% FPP).
#[test]
fn substring_present_in_one_row_group_returns_just_that_rg() {
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "f.parquet";

    reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple acorn");
    reg.index_row(&proj, &tbl, "doc", file, 1, "bravo banana berry");
    reg.index_row(&proj, &tbl, "doc", file, 2, "charlie kilowatt drink");
    reg.index_row(&proj, &tbl, "doc", file, 3, "delta donut dynamo");
    reg.seal_file_indexed(&proj, &tbl, "doc", file);

    let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("kilo"));
    match probe {
        RowGroupProbe::RowGroups(rgs) => {
            // No false negative: rg=2 must be in.
            assert!(rgs.contains(&2), "rg 2 must survive, got {rgs:?}");
            // Pruning happened: at least one of {0,1,3} was skipped.
            assert!(
                rgs.len() < 4,
                "expected sub-file pruning, kept {} of 4",
                rgs.len()
            );
        }
        other => panic!("expected RowGroups, got {other:?}"),
    }
}

/// When every row-group contains the substring the probe must return all
/// row-groups (no spurious pruning).
#[test]
fn substring_in_every_row_group_returns_all() {
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "f.parquet";

    for rg in 0u32..4 {
        let row = format!("zulu prefix uniform_{rg} trailing");
        reg.index_row(&proj, &tbl, "doc", file, rg, &row);
    }
    reg.seal_file_indexed(&proj, &tbl, "doc", file);

    let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("zulu"));
    match probe {
        RowGroupProbe::RowGroups(rgs) => {
            assert_eq!(rgs, vec![0, 1, 2, 3], "all rgs should survive, got {rgs:?}");
        }
        other => panic!("expected RowGroups, got {other:?}"),
    }
}

/// A pattern whose trigrams are absent from every row-group prunes the
/// entire file.
#[test]
fn absent_pattern_returns_empty() {
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "f.parquet";

    reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");
    reg.index_row(&proj, &tbl, "doc", file, 1, "bravo banana");
    reg.seal_file_indexed(&proj, &tbl, "doc", file);

    let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("qxzqxz"));
    match probe {
        RowGroupProbe::RowGroups(rgs) => {
            assert!(
                rgs.is_empty(),
                "absent pattern must prune whole file, got {rgs:?}"
            );
        }
        other => panic!("expected RowGroups (empty), got {other:?}"),
    }
}

/// Pre-`seal_file_indexed`: indexing has started but the file is not yet
/// committed. The probe must return Unknown so the engine falls back to
/// its file-granular path (mid-write safety; mirrors the gin_rowgroup
/// contract).
#[test]
fn pre_seal_returns_unknown() {
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "f.parquet";

    // Index a row but do NOT seal.
    reg.index_row(&proj, &tbl, "doc", file, 0, "alpha apple");

    let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("alpha"));
    assert_eq!(probe, RowGroupProbe::Unknown);
}

/// Many row-groups at scale (≥ 32): a substring unique to one row-group
/// must keep that row-group and strictly fewer than the total — proving
/// the persisted summary delivers real sub-file pruning at scale.
#[test]
fn many_row_groups_at_scale_yields_strict_subset() {
    const N: u32 = 64; // ≥ 32 as required by the task.
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "big.parquet";

    for rg in 0..N {
        let row = format!("common shared body uniform_{rg:03} mike november oscar");
        reg.index_row(&proj, &tbl, "doc", file, rg, &row);
    }
    reg.seal_file_indexed(&proj, &tbl, "doc", file);

    // "uniform_042" appears in only rg=42.
    let probe =
        reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("uniform_042"));
    match probe {
        RowGroupProbe::RowGroups(rgs) => {
            assert!(rgs.contains(&42), "rg 42 must survive, got {rgs:?}");
            assert!(
                (rgs.len() as u32) < N,
                "sub-file pruning must skip some row-groups at scale, kept {} of {N}",
                rgs.len()
            );
        }
        other => panic!("expected RowGroups, got {other:?}"),
    }
}

/// `ILIKE` parity: a value stored with mixed case must be findable by the
/// lowercased trigrams an ILIKE pattern produces — the both-case storage
/// strategy guarantees a single persisted summary serves both LIKE and
/// ILIKE without a second index.
#[test]
fn ilike_lowercased_trigrams_hit_mixed_case_storage() {
    let reg = TrigramRowGroupRegistry::new();
    let (proj, tbl) = proj_tbl();
    let file = "f.parquet";

    // Mixed-case value: "Alpha".
    reg.index_row(&proj, &tbl, "doc", file, 0, "Alpha Bravo");
    reg.index_row(&proj, &tbl, "doc", file, 1, "Charlie Delta");
    reg.seal_file_indexed(&proj, &tbl, "doc", file);

    // ILIKE 'alpha%' produces lowercase trigrams.
    let probe = reg.rowgroups_maybe_containing_trigrams(&proj, &tbl, "doc", file, &tg("alpha"));
    match probe {
        RowGroupProbe::RowGroups(rgs) => {
            assert!(
                rgs.contains(&0),
                "rg 0 must survive (lowercased windows stored), got {rgs:?}"
            );
            assert!(
                !rgs.contains(&1),
                "rg 1 has no alpha — should be pruned, got {rgs:?}"
            );
        }
        other => panic!("expected RowGroups, got {other:?}"),
    }
}
