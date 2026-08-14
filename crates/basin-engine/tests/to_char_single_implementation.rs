//! `to_char` has exactly one implementation, and its answer does not depend on
//! which process asks.
//!
//! ## The bug this pins
//!
//! `to_char(DATE '2024-01-15', 'YYYY-MM-DD')` used to return `2024-01-15` in
//! some processes and the literal string `YYYY-MM-DD` in others — same binary,
//! same query. Measured over 12 runs before the fix: 7 literal, 5 correct.
//! `to_char(int, text)` did not even plan in the losing processes.
//!
//! Mechanism: the session UDF cache was built by flattening a name-keyed
//! `HashMap` with `values().collect()`. DataFusion's built-in `to_char` carries
//! the alias `date_format`, so after Basin's `to_char` overwrote the `to_char`
//! key the built-in survived under `date_format` — two entries, both answering
//! to the name `to_char`. `SessionStateBuilder::build` re-registers every entry
//! under its own `name()`, so whichever landed later in the vector won, and the
//! vector's order came from a per-process hash seed.
//!
//! `session::flatten_registry` now emits that vector in a topological order in
//! which a function is always replayed before anything that would steal a name
//! it owns. `session::tests::udf_cache_resolves_every_name_to_its_owner` pins
//! the ordering rule; this file pins the answer a client actually sees.
//!
//! Expected values are `psql` output from the PostgreSQL 18.2 server this
//! branch is developed against, copied verbatim.

use std::sync::Arc;

use arrow_array::{Array, StringArray};
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

async fn one_string(sess: &ProjectSession, sql: &str) -> String {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            for b in &batches {
                if b.num_columns() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| {
                        panic!(
                            "{sql}: expected a text column, got {:?}",
                            b.column(0).data_type()
                        )
                    });
                if !arr.is_empty() {
                    return if arr.is_null(0) {
                        "NULL".to_string()
                    } else {
                        arr.value(0).to_string()
                    };
                }
            }
            panic!("{sql}: no rows")
        }
        Ok(other) => panic!("{sql}: expected rows, got {other:?}"),
        Err(e) => panic!("{sql}: {e}"),
    }
}

/// Values verified against PostgreSQL 18.2 by running the same `to_char` calls
/// through `psql`. Only cases where Basin already agrees with PostgreSQL are
/// asserted; the known divergences (`Month` blank-padding, `DY`/`DAY` letter
/// case, `DDD`, `IYYY`, `WW`, numeric picture overflow) are documented on
/// `ToCharMoreUdf` and are a separate job from the collision this test guards.
#[tokio::test]
async fn to_char_matches_postgres_on_the_shapes_it_supports() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // (SQL, PostgreSQL 18.2's answer)
    let cases: &[(&str, &str)] = &[
        (
            "SELECT to_char(DATE '2024-01-15','YYYY-MM-DD')",
            "2024-01-15",
        ),
        ("SELECT to_char(DATE '2024-01-15','FMDay')", "Monday"),
        ("SELECT to_char(DATE '2024-01-15','HH24:MI:SS')", "00:00:00"),
        (
            "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','YYYY-MM-DD HH24:MI:SS')",
            "2024-01-15 13:45:06",
        ),
        (
            "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','HH12:MI:SS AM')",
            "01:45:06 PM",
        ),
        (
            "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','YYYY')",
            "2024",
        ),
        (
            "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','Mon DD, YYYY')",
            "Jan 15, 2024",
        ),
        ("SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','Q')", "1"),
        // Numeric overloads. These reach `udf::format_numeric_pg`, the
        // formatter kept from the deleted duplicate — it is the half of that
        // implementation that beat the survivor's own.
        ("SELECT to_char(3.14159,'FM999.99')", "3.14"),
        ("SELECT to_char(42,'S9999')", "  +42"),
    ];

    for (sql, pg) in cases {
        assert_eq!(&one_string(&sess, sql).await, pg, "{sql}");
    }
}

/// The duplicate is gone, so `to_char` no longer refuses integer input in some
/// processes — planning it was the other half of the same coin flip.
#[tokio::test]
async fn to_char_accepts_every_overload_in_every_process() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    for sql in [
        "SELECT to_char(DATE '2024-01-15','YYYY-MM-DD')",
        "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06','YYYY-MM-DD')",
        "SELECT to_char(42,'999')",
        "SELECT to_char(3.5,'999.9')",
    ] {
        // Panics with the planner error if the built-in wins the name.
        let _ = one_string(&sess, sql).await;
    }
}

/// `date_format` is DataFusion's built-in under its own alias. It keeps that
/// alias — the fix is that it stops capturing `to_char` along with it — and it
/// takes chrono-style formats, so a PG picture stays literal. Asserting this
/// keeps anyone from "fixing" the collision by deleting the alias instead.
#[tokio::test]
async fn date_format_alias_still_belongs_to_the_datafusion_builtin() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        one_string(&sess, "SELECT date_format(DATE '2024-01-15','YYYY-MM-DD')").await,
        "YYYY-MM-DD",
    );
    assert_eq!(
        one_string(&sess, "SELECT date_format(DATE '2024-01-15','%Y-%m-%d')").await,
        "2024-01-15",
    );
}
