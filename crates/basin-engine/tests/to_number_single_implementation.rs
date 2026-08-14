//! `to_number` has exactly one implementation, and it reads the format picture.
//!
//! ## The bug this pins
//!
//! There were two `to_number` UDFs. `pg_scalar_aliases` registers after
//! `udf`, so `pg_scalar_aliases::ToNumberUdf` won every time — the same
//! silent same-name shadowing that hid `to_char`'s better half before
//! `f97ba3ba`, minus the alias nondeterminism, so it was a stable wrong
//! answer rather than a coin flip.
//!
//! The winner ignored its second argument entirely. It filtered the input
//! down to `[0-9.-]` and called `parse::<f64>`, which means every way
//! PostgreSQL has of writing a negative number other than a leading minus
//! came back wrong:
//!
//! | call                                  | PG 18.2   | old winner |
//! |---------------------------------------|-----------|------------|
//! | `to_number('12,454.8-','99G999D9S')`  | `-12454.8`| `NULL`     |
//! | `to_number('1234-','9999MI')`         | `-1234`   | `NULL`     |
//! | `to_number('<1234>','9999PR')`        | `-1234`   | `1234`     |
//!
//! The `PR` row is the bad one: a bracketed negative came back positive,
//! which is a plausible number rather than a visible failure.
//!
//! The shadowed implementation walks the picture and has explicit `S` / `MI`
//! / `PR` handling, so it was deleted rather than merged — unlike the
//! `to_char` pair, the loser had nothing the survivor lacked. It did get
//! `to_number('$1,234.56','L9,999.99')` right by accident, though, which the
//! survivor's picture walk did not handle; `L` is now a case in
//! `parse_pg_number`, so nothing was traded away in the swap.
//!
//! Expected values are `psql` output from the PostgreSQL 18.2 server this
//! branch is developed against, copied verbatim.

use std::sync::Arc;

use arrow_array::{Array, Float64Array};
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

async fn one_f64(sess: &ProjectSession, sql: &str) -> f64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            for b in &batches {
                if b.num_columns() == 0 || b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap_or_else(|| {
                        panic!(
                            "{sql}: expected a float column, got {:?}",
                            b.column(0).data_type()
                        )
                    });
                assert!(!arr.is_null(0), "{sql}: got NULL");
                return arr.value(0);
            }
            panic!("{sql}: no rows")
        }
        Ok(other) => panic!("{sql}: expected rows, got {other:?}"),
        Err(e) => panic!("{sql}: {e}"),
    }
}

#[tokio::test]
async fn to_number_reads_the_format_picture() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // (SQL, PostgreSQL 18.2's answer)
    let cases: &[(&str, f64)] = &[
        ("SELECT to_number('1,234.56','9,999.99')", 1234.56),
        ("SELECT to_number('1234','9999')", 1234.0),
        // The three the deleted duplicate got wrong.
        ("SELECT to_number('12,454.8-','99G999D9S')", -12454.8),
        ("SELECT to_number('1234-','9999MI')", -1234.0),
        ("SELECT to_number('<1234>','9999PR')", -1234.0),
        // The one it got right, kept working by teaching `L` to the survivor.
        ("SELECT to_number('$1,234.56','L9,999.99')", 1234.56),
        // Leading sign, and the locale group/decimal spellings.
        ("SELECT to_number('-12,454.8','S99G999D9')", -12454.8),
        ("SELECT to_number('12,345','99G999')", 12345.0),
        ("SELECT to_number('12.3','99D9')", 12.3),
        ("SELECT to_number('0001234','0000000')", 1234.0),
    ];

    for (sql, pg) in cases {
        let got = one_f64(&sess, sql).await;
        assert!(
            (got - pg).abs() < 1e-9,
            "{sql}: expected {pg} (PostgreSQL 18.2), got {got}"
        );
    }
}
