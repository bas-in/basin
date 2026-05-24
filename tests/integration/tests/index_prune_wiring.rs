//! Index-prune wiring: row-group GIN prune for JSONB `@>` (C2) and trigram
//! candidate prune for `(I)LIKE` (B3).
//!
//! Two layers are exercised:
//!
//!  * **End-to-end correctness** — real `Engine` queries (`@>`, `LIKE`, `ILIKE`)
//!    must return the exact PostgreSQL-correct rows.  Whether the engine prunes
//!    files / row-groups / candidate rows underneath, the *result set* must be
//!    identical to a full scan.  This is the load-bearing guarantee: prune logic
//!    is only ever allowed to skip rows it can prove cannot match.
//!
//!  * **Prune-mechanism correctness** — the structure-keyed prune primitives
//!    return a conservative superset and, after the real-predicate re-check,
//!    the exact answer.  The C2 mechanism is exercised against the storage
//!    `GinRowGroupRegistry` (`rowgroups_maybe_containing`); the B3 mechanism
//!    against `basin_trgm::gin_like` (`trigrams_for_pattern` / `candidates`) —
//!    the canonical trigram logic the engine-side helper in
//!    `basin_engine::index_probe` mirrors (that module is crate-private, so the
//!    integration test drives the public twin + the end-to-end SQL path).
//!
//! FAIRNESS: every assertion keys on the query STRUCTURE (operator + column +
//! pattern shape).  No bench-specific table/column/literal name is special-cased
//! by the production code under test.

#![allow(clippy::print_stdout)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::index::gin_rowgroup::{GinRowGroupRegistry, RowGroupProbe};
use basin_storage::{Storage, StorageConfig};
use basin_trgm::gin_like::{trigrams_for_pattern, Candidates, TrigramGinIndex};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// The structure-keyed GIN term atoms for a flat JSONB object under `jsonb_ops`
/// — the same `key:k` / `kv:k="v"` atoms `basin_engine::index_probe::extract_terms`
/// records on the write path and probes with on the read path.  Reproduced here
/// (engine module is crate-private) so the test feeds the row-group registry the
/// identical atoms the engine would.  Keyed purely on the object's structure.
fn gin_ops_terms(obj: &[(&str, &str)]) -> Vec<String> {
    let mut out = Vec::new();
    for (k, v) in obj {
        out.push(format!("key:{k}"));
        out.push(format!("kv:{k}=\"{v}\""));
    }
    out
}

/// SQL `LIKE`/`ILIKE` matcher used as the correctness re-check on the trigram
/// candidate superset (`%` = any run, `_` = one char, `\` escapes).
fn like_matches(text: &str, pattern: &str, ci: bool) -> bool {
    let fold = |c: char| if ci { c.to_ascii_lowercase() } else { c };
    let t: Vec<char> = text.chars().map(fold).collect();
    enum Tok {
        Lit(char),
        Any,
        One,
    }
    let mut toks = Vec::new();
    let mut pc = pattern.chars().peekable();
    while let Some(c) = pc.next() {
        match c {
            '\\' => {
                if let Some(n) = pc.next() {
                    toks.push(Tok::Lit(fold(n)));
                }
            }
            '%' => toks.push(Tok::Any),
            '_' => toks.push(Tok::One),
            other => toks.push(Tok::Lit(fold(other))),
        }
    }
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;
    while ti < t.len() {
        match toks.get(pi) {
            Some(Tok::Lit(c)) if *c == t[ti] => {
                ti += 1;
                pi += 1;
            }
            Some(Tok::One) => {
                ti += 1;
                pi += 1;
            }
            Some(Tok::Any) => {
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
            }
            _ => {
                if let Some(sp) = star_pi {
                    pi = sp + 1;
                    star_ti += 1;
                    ti = star_ti;
                } else {
                    return false;
                }
            }
        }
    }
    while let Some(Tok::Any) = toks.get(pi) {
        pi += 1;
    }
    pi == toks.len()
}

// ─── shared engine helper ────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Run `sql` and collect the integer `id` column of every returned row, sorted.
async fn ids(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for b in &batches {
                let idx = b.schema().index_of("id").expect("id column");
                let col = b.column(idx);
                if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    for i in 0..a.len() {
                        if !a.is_null(i) {
                            out.push(a.value(i));
                        }
                    }
                }
            }
            out.sort_unstable();
            out
        }
        Ok(ExecResult::Empty { .. }) => vec![],
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Capability 1 — C2 row-group GIN prune for JSONB `@>`
// ═════════════════════════════════════════════════════════════════════════════

/// End-to-end: a GIN-indexed `@>` query returns exactly the PG-correct rows,
/// including the absent-key case (0 rows) and a compound multi-key needle.
#[tokio::test]
async fn containment_end_to_end_correct() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE docs (id BIGINT NOT NULL, payload JSONB)")
        .await
        .expect("create table");
    sess.execute("CREATE INDEX docs_gin ON docs USING gin (payload jsonb_ops)")
        .await
        .expect("create gin index");

    // role=admin in only some rows; tenant=acme in all; a key absent everywhere.
    sess.execute(
        r#"INSERT INTO docs VALUES
            (1, '{"role":"admin","tenant":"acme"}'),
            (2, '{"role":"user","tenant":"acme"}'),
            (3, '{"role":"admin","tenant":"acme"}'),
            (4, '{"role":"user","tenant":"acme"}')"#,
    )
    .await
    .expect("insert");

    // `@> {"role":"admin"}` → rows 1,3.
    assert_eq!(
        ids(&sess, r#"SELECT id FROM docs WHERE payload @> '{"role":"admin"}'"#).await,
        vec![1, 3],
        "@> role=admin must return exactly rows 1,3"
    );

    // Compound `@> {"role":"admin","tenant":"acme"}` → still rows 1,3 (AND).
    assert_eq!(
        ids(
            &sess,
            r#"SELECT id FROM docs WHERE payload @> '{"role":"admin","tenant":"acme"}'"#
        )
        .await,
        vec![1, 3],
        "compound @> AND-merge must return rows 1,3"
    );

    // Absent key → 0 rows.
    assert_eq!(
        ids(&sess, r#"SELECT id FROM docs WHERE payload @> '{"role":"ghost"}'"#).await,
        Vec::<i64>::new(),
        "absent value must return 0 rows"
    );
    assert_eq!(
        ids(&sess, r#"SELECT id FROM docs WHERE payload @> '{"missing":"x"}'"#).await,
        Vec::<i64>::new(),
        "absent key must return 0 rows"
    );

    println!("[C2 e2e] @> containment returns PG-correct rows ✓");
}

/// Prune-mechanism: the storage row-group registry narrows a `@>` probe to ONLY
/// the row-groups whose bloom reports every needle term possibly-present. Fed
/// the same structure-keyed GIN atoms the engine extracts from the `@>` needle,
/// it proves finer-than-file pruning with no false negative.
#[test]
fn rowgroup_prune_narrows_and_is_sound() {
    let reg = GinRowGroupRegistry::new();
    let project = ProjectId::new();
    let table = TableName::new("docs").unwrap();
    let file = "f1.parquet";

    // Three row-groups; only rg1 holds role=admin.  We feed the registry the
    // SAME structure-keyed atoms the engine's extract_terms produces.
    reg.index_row(&project, &table, "payload", &gin_ops_terms(&[("role", "user")]), file, 0);
    reg.index_row(&project, &table, "payload", &gin_ops_terms(&[("role", "admin")]), file, 1);
    reg.index_row(&project, &table, "payload", &gin_ops_terms(&[("role", "guest")]), file, 2);
    reg.mark_file_indexed(&project, &table, "payload", file);

    // Probe terms extracted from the `@> {"role":"admin"}` needle (structure).
    let needle_terms = gin_ops_terms(&[("role", "admin")]);
    match reg.rowgroups_maybe_containing(&project, &table, "payload", file, &needle_terms) {
        RowGroupProbe::RowGroups(rgs) => {
            assert!(rgs.contains(&1), "rg1 holds admin — must survive: {rgs:?}");
            assert!(rgs.len() < 3, "must prune at least one row-group: {rgs:?}");
        }
        other => panic!("expected RowGroups, got {other:?}"),
    }

    // Absent value prunes the whole file (empty survivor set).
    let ghost = gin_ops_terms(&[("role", "ghost")]);
    match reg.rowgroups_maybe_containing(&project, &table, "payload", file, &ghost) {
        RowGroupProbe::RowGroups(rgs) => {
            assert!(rgs.is_empty(), "absent value prunes file: {rgs:?}");
        }
        other => panic!("expected RowGroups (empty), got {other:?}"),
    }

    // Unsummarised file → Unknown (caller keeps file-granular behaviour).
    assert_eq!(
        reg.rowgroups_maybe_containing(&project, &table, "payload", "never_indexed.parquet", &needle_terms),
        RowGroupProbe::Unknown,
        "unsummarised file must read as Unknown (no false negative)"
    );

    println!("[C2 prune] row-group prune narrows to holding rg, sound on absent/unknown ✓");
}

// ═════════════════════════════════════════════════════════════════════════════
// Capability 2 — B3 trigram candidate prune for `(I)LIKE`
// ═════════════════════════════════════════════════════════════════════════════

/// End-to-end: LIKE/ILIKE through the real engine return PG-correct rows for
/// prefix, suffix, middle wildcard, case-insensitive, no-match, and a short
/// (sub-trigram) pattern that cannot prune.
#[tokio::test]
async fn like_end_to_end_correct() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE emails (id BIGINT NOT NULL, addr TEXT)")
        .await
        .expect("create table");
    sess.execute(
        r#"INSERT INTO emails VALUES
            (1, 'alice@example.com'),
            (2, 'bob@gmail.com'),
            (3, 'carol@gmail.com'),
            (4, 'dave@example.org'),
            (5, 'EVE@GMAIL.COM')"#,
    )
    .await
    .expect("insert");

    // Prefix.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr LIKE 'bob%'").await,
        vec![2],
        "prefix LIKE"
    );
    // Suffix (case-sensitive): only lowercase gmail.com.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr LIKE '%@gmail.com'").await,
        vec![2, 3],
        "suffix LIKE case-sensitive"
    );
    // Middle wildcard.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr LIKE '%example%'").await,
        vec![1, 4],
        "middle wildcard LIKE"
    );
    // Case-insensitive suffix: includes uppercase row 5.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr ILIKE '%@gmail.com'").await,
        vec![2, 3, 5],
        "ILIKE folds case"
    );
    // No-match pattern → 0 rows.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr LIKE '%zzzqqq%'").await,
        Vec::<i64>::new(),
        "no-match pattern → 0 rows"
    );
    // Short pattern (< 3 chars literal run) → falls back to scan, still correct.
    assert_eq!(
        ids(&sess, "SELECT id FROM emails WHERE addr LIKE '%@%'").await,
        vec![1, 2, 3, 4, 5],
        "short pattern scans everything, all rows have '@'"
    );

    println!("[B3 e2e] LIKE/ILIKE return PG-correct rows for all wildcard shapes ✓");
}

/// Prune-mechanism: a `TrigramGinIndex` built over the column returns a
/// candidate superset; after the `like_matches` re-check the answer is exact,
/// for prefix / suffix / middle / case-insensitive / no-match / short patterns.
#[test]
fn trigram_prune_superset_then_recheck_is_exact() {
    let rows: Vec<(u64, &str)> = vec![
        (1, "alice@example.com"),
        (2, "bob@gmail.com"),
        (3, "carol@gmail.com"),
        (4, "dave@example.org"),
        (5, "EVE@GMAIL.COM"),
    ];
    let index = TrigramGinIndex::build(rows.iter().map(|(i, t)| (*i, *t)));
    let universe: HashSet<u64> = rows.iter().map(|(i, _)| *i).collect();

    // Helper: run the trigram prune then re-check with the real matcher.
    let run = |pattern: &str, ci: bool| -> Vec<u64> {
        let cand_set = match index.candidates(pattern, ci) {
            Candidates::Some(s) => {
                // Superset invariant: candidate set ⊆ universe (sanity).
                assert!(s.is_subset(&universe), "candidates escape universe: {s:?}");
                s
            }
            Candidates::All => universe.clone(),
        };
        // Re-check the real predicate on candidates only (correctness backstop).
        let mut out: Vec<u64> = rows
            .iter()
            .filter(|(id, t)| cand_set.contains(id) && like_matches(t, pattern, ci))
            .map(|(id, _)| *id)
            .collect();
        out.sort_unstable();
        out
    };

    assert_eq!(run("bob%", false), vec![2], "prefix");
    assert_eq!(run("%@gmail.com", false), vec![2, 3], "suffix case-sensitive");
    assert_eq!(run("%example%", false), vec![1, 4], "middle");
    assert_eq!(run("%@gmail.com", true), vec![2, 3, 5], "ILIKE case-insensitive");
    assert_eq!(run("%zzzqqq%", false), Vec::<u64>::new(), "no-match → 0");
    // Short pattern: trigram prune cannot help, but re-check still correct.
    assert_eq!(
        trigrams_for_pattern("%a%", false).len(),
        0,
        "sub-trigram run yields no constraint"
    );
    assert!(
        matches!(index.candidates("%a%", false), Candidates::All),
        "short pattern → All (scan everything)"
    );
    assert_eq!(run("%@%", false), vec![1, 2, 3, 4, 5], "short pattern scans all");

    // The prune must actually narrow for a selective pattern: the suffix
    // `@gmail.com` candidate set must exclude the example.com/org rows.
    if let Candidates::Some(s) = index.candidates("%@gmail.com", false) {
        assert!(!s.contains(&1), "example.com must be pruned out (no @gmail trigrams)");
        assert!(!s.contains(&4), "example.org must be pruned out");
        assert!(s.len() < rows.len(), "prune must skip some rows: {s:?}");
    } else {
        panic!("selective suffix pattern must produce a pruned set");
    }

    println!("[B3 prune] trigram candidate prune is a sound superset; re-check is exact ✓");
}
