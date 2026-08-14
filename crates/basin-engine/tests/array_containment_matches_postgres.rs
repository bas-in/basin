//! `@>`, `<@` and `&&` on arrays answer what PostgreSQL answers.
//!
//! ## What this pins
//!
//! `array_contains` and `arrays_overlap` were both admitted stubs. The first
//! returned `rhs.len() <= lhs.len()`; the second returned "both arrays are
//! non-empty". Neither looked at a single element. `arrays_overlap` is what
//! `pg_operators` rewrites the `&&` operator into, so that one was answering
//! clients — `ARRAY[1,2] && ARRAY[3,4]` came back true.
//!
//! `f97ba3ba` made `array_contains` resolve deterministically to Basin's
//! implementation instead of coin-flipping with DataFusion's `array_has`
//! alias, which is what the registration order always intended — and which
//! turned an unpredictable answer into a reliably wrong one. This file is the
//! other half of that fix.
//!
//! ## Why the cases below and not others
//!
//! Every expected value in this file is `psql` output from the PostgreSQL 18.2
//! server this branch is developed against, copied verbatim. Several read the
//! other way from memory, which is the reason they are here:
//!
//! - An empty right operand is contained by *everything*, including the empty
//!   array. `ARRAY[]::int[] <@ ARRAY[1,2]` is true and so is
//!   `ARRAY[]::int[] @> ARRAY[]::int[]`.
//! - Duplicates never matter. `ARRAY[1,2,3] @> ARRAY[2,2,2]` is true.
//! - A NULL *element* is never contained and never overlaps, not even with
//!   itself: `ARRAY[1,2,NULL] @> ARRAY[NULL]` is false, and
//!   `ARRAY[1,NULL] && ARRAY[NULL,2]` is false. It is false, not NULL.
//! - A NULL *array* propagates: `NULL::int[] @> ARRAY[1]` is NULL. The
//!   operators are strict in the argument, not in its contents.
//! - Dimensionality is ignored entirely — PG compares the flattened element
//!   lists, so `ARRAY[[1,2],[3,4]] @> ARRAY[1,4]` is true.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// `"t"`, `"f"` or `"NULL"` — the same three spellings `psql` prints, so the
/// expected column below can be read straight off a `psql` session.
async fn one_bool(sess: &ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            for b in &batches {
                if b.num_columns() == 0 || b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap_or_else(|| {
                        panic!(
                            "{sql}: expected a boolean column, got {:?}",
                            b.column(0).data_type()
                        )
                    });
                return if arr.is_null(0) {
                    "NULL".to_string()
                } else if arr.value(0) {
                    "t".to_string()
                } else {
                    "f".to_string()
                };
            }
            panic!("{sql}: no rows")
        }
        Ok(other) => panic!("{sql}: expected rows, got {other:?}"),
        Err(e) => panic!("{sql}: {e}"),
    }
}

async fn check(sess: &ProjectSession, cases: &[(&str, &str)]) {
    for (sql, pg) in cases {
        assert_eq!(&one_bool(sess, sql).await, pg, "{sql}");
    }
}

/// `array_contains` is PG's `@>`: set containment, not a length comparison.
///
/// The stub it replaced returned `rhs.len() <= lhs.len()`, which gets the
/// first case here right by accident and the second one wrong.
#[tokio::test]
async fn array_contains_matches_postgres() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    check(
        &sess,
        &[
            ("SELECT array_contains(ARRAY[1,2,3], ARRAY[2,3])", "t"),
            // The stub said `t`: two elements fit inside three.
            ("SELECT array_contains(ARRAY[1,2,3], ARRAY[2,4])", "f"),
            // Set semantics — three copies of one element still only need one.
            ("SELECT array_contains(ARRAY[1,2,3], ARRAY[2,2,2])", "t"),
            ("SELECT array_contains(ARRAY[1,1,2], ARRAY[1,2])", "t"),
            // Empty right operand: contained by everything.
            ("SELECT array_contains(ARRAY[1,2], ARRAY[]::int[])", "t"),
            ("SELECT array_contains(ARRAY[]::int[], ARRAY[]::int[])", "t"),
            ("SELECT array_contains(ARRAY[]::int[], ARRAY[1])", "f"),
            // NULL elements. `f`, not `NULL` — the stub returned `t` for the
            // first of these because one element fits inside three.
            (
                "SELECT array_contains(ARRAY[1,2,NULL], ARRAY[NULL]::int[])",
                "f",
            ),
            (
                "SELECT array_contains(ARRAY[NULL]::int[], ARRAY[NULL]::int[])",
                "f",
            ),
            // A NULL on the left is simply never matched, and never in the way.
            ("SELECT array_contains(ARRAY[1,NULL], ARRAY[1])", "t"),
            ("SELECT array_contains(ARRAY[1,NULL], ARRAY[1,NULL])", "f"),
            // Strict in the argument: a NULL array yields NULL.
            ("SELECT array_contains(NULL::int[], ARRAY[1])", "NULL"),
            ("SELECT array_contains(ARRAY[1], NULL::int[])", "NULL"),
            // Text elements compare by value, case-sensitively.
            (
                "SELECT array_contains(ARRAY['go','rust'], ARRAY['rust'])",
                "t",
            ),
            (
                "SELECT array_contains(ARRAY['go','rust'], ARRAY['RUST'])",
                "f",
            ),
        ],
    )
    .await;
}

/// `arrays_overlap` is PG's `&&`, and it is the one that was reaching clients:
/// `pg_operators` rewrites every `&&` on arrays into a call to it.
///
/// The stub returned `lhs_len > 0 && rhs_len > 0`, so it answered `t` for
/// every pair of non-empty arrays regardless of what was in them.
#[tokio::test]
async fn arrays_overlap_matches_postgres() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    check(
        &sess,
        &[
            ("SELECT arrays_overlap(ARRAY[1,2,3], ARRAY[3,4])", "t"),
            // The stub said `t` — two non-empty arrays.
            ("SELECT arrays_overlap(ARRAY[1,2], ARRAY[3,4])", "f"),
            ("SELECT arrays_overlap(ARRAY[1,2], ARRAY[]::int[])", "f"),
            ("SELECT arrays_overlap(ARRAY[]::int[], ARRAY[]::int[])", "f"),
            // Two arrays that both contain NULL do not overlap on that account.
            ("SELECT arrays_overlap(ARRAY[1,NULL], ARRAY[NULL,2])", "f"),
            // …but a shared non-NULL element still counts.
            ("SELECT arrays_overlap(ARRAY[1,NULL], ARRAY[NULL,1])", "t"),
            ("SELECT arrays_overlap(NULL::int[], ARRAY[1])", "NULL"),
            ("SELECT arrays_overlap(ARRAY['go','rust'], ARRAY['c'])", "f"),
            (
                "SELECT arrays_overlap(ARRAY['go','rust'], ARRAY['c','go'])",
                "t",
            ),
        ],
    )
    .await;
}

/// The operators, not the function names. `&&` lowers to `arrays_overlap`;
/// `@>` and `<@` lower to `list_has_all`, DataFusion's own (array, array)
/// containment. Both spellings have to agree with PostgreSQL, and the
/// operator spelling is the one clients actually write.
#[tokio::test]
async fn array_operators_match_postgres() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    check(
        &sess,
        &[
            ("SELECT ARRAY[1,2,3] @> ARRAY[2,3]", "t"),
            ("SELECT ARRAY[1,2,3] @> ARRAY[2,4]", "f"),
            ("SELECT ARRAY[1,2,3] @> ARRAY[2,2,2]", "t"),
            // `SELECT ARRAY[]::int[] <@ ARRAY[1,2]` — PG says `t` — is NOT
            // asserted here, because it never reaches either function.
            // `array_extract_left` walks back from the operator over a closing
            // `]` and stops at the matching `[`, then only widens if the text
            // in front spells `array`. For a cast it spells `int`, so the left
            // operand comes out as the two characters `[]` and the rewritten
            // SQL fails to parse. Same for `(ARRAY[…])::varchar(50)[] <@ …`.
            // That is a defect in the operator *rewriter*, not in containment,
            // and a parse error is an honest refusal rather than a wrong
            // answer — the empty-right-operand semantics it would have covered
            // are pinned above through the function spelling instead.
            ("SELECT ARRAY[1,2] <@ ARRAY[1,2,3]", "t"),
            ("SELECT ARRAY[1,4] <@ ARRAY[1,2,3]", "f"),
            ("SELECT ARRAY[1,2,3] && ARRAY[3,4]", "t"),
            ("SELECT ARRAY[1,2] && ARRAY[3,4]", "f"),
            ("SELECT ARRAY['go','rust'] && ARRAY['c','go']", "t"),
            ("SELECT ARRAY['go','rust'] && ARRAY['c']", "f"),
        ],
    )
    .await;
}
