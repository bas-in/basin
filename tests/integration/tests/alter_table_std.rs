//! Integration test for the standard ALTER TABLE forms wired up by
//! task #29:
//!
//!   * DROP COLUMN
//!   * RENAME COLUMN
//!   * RENAME TO
//!   * ALTER COLUMN TYPE
//!   * ADD CONSTRAINT CHECK
//!   * ADD CONSTRAINT UNIQUE (registered + enforced + backfill-validated)
//!   * DROP CONSTRAINT
//!   * VALIDATE CONSTRAINT (textual matcher)
//!   * ATTACH PARTITION / DETACH PARTITION (no-op accept)
//!   * SET cold_after = '7d' (quoted-duration parse)
//!
//! Each test creates a fresh engine + project + table, exercises one
//! ALTER form, and verifies the post-state via the catalog or a
//! follow-up DML. The intent is to lock in the catalog mutations so
//! the SQL support matrix can rely on them flipping to ✅.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn engine() -> (TempDir, Engine, ProjectId) {
    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = Storage::new(StorageConfig {
        object_store: fs,
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let eng = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    let project = ProjectId::new();
    (dir, eng, project)
}

#[tokio::test]
async fn drop_column_removes_field_from_schema() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, payload TEXT)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t DROP COLUMN payload")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(meta.schema.field_with_name("payload").is_err());
    assert!(meta.schema.field_with_name("id").is_ok());
}

#[tokio::test]
async fn rename_column_renames_field_in_schema() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, c TEXT)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t RENAME COLUMN c TO d")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(meta.schema.field_with_name("c").is_err());
    assert!(meta.schema.field_with_name("d").is_ok());
}

#[tokio::test]
async fn rename_table_changes_catalog_key() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t RENAME TO u").await.unwrap();
    let cat = &eng.config().catalog;
    // v0.1 keeps the old key alive as a synonym so the engine's
    // post-ALTER session refresh can still find the table.
    assert!(cat
        .load_table(&project, &TableName::new("u").unwrap())
        .await
        .is_ok());
}

#[tokio::test]
async fn alter_column_type_swaps_arrow_data_type() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, c INT)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t ALTER COLUMN c TYPE BIGINT")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    use arrow_schema::DataType;
    assert_eq!(
        meta.schema.field_with_name("c").unwrap().data_type(),
        &DataType::Int64
    );
}

#[tokio::test]
async fn add_check_constraint_persists_predicate() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0)")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.check_constraints.len(), 1);
    assert_eq!(meta.check_constraints[0].name, "ck");
}

/// `ALTER TABLE … ADD CONSTRAINT <n> UNIQUE (a, b)` — the exact shape
/// Django's schema editor emits for `unique_together` (it ALTERs the
/// constraint on AFTER the bare CREATE TABLE). The constraint must land in
/// the catalog's `unique_constraints` and be enforced on subsequent INSERTs
/// by the existing `enforce_unique_on_insert` machinery.
#[tokio::test]
async fn add_unique_constraint_registers_and_enforces() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute(
        "CREATE TABLE django_content_type (id BIGINT NOT NULL PRIMARY KEY, \
         app_label TEXT NOT NULL, model TEXT NOT NULL)",
    )
    .await
    .unwrap();
    // Pre-existing distinct rows must pass the backfill validation.
    sess.execute(
        "INSERT INTO django_content_type (id, app_label, model) VALUES \
         (1, 'myapp', 'question'), (2, 'myapp', 'choice')",
    )
    .await
    .unwrap();
    sess.execute(
        "ALTER TABLE django_content_type ADD CONSTRAINT \
         django_content_type_app_label_model_76bd3d3b_uniq UNIQUE (app_label, model)",
    )
    .await
    .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("django_content_type").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.unique_constraints.len(), 1);
    assert_eq!(
        meta.unique_constraints[0].name,
        "django_content_type_app_label_model_76bd3d3b_uniq"
    );
    assert_eq!(
        meta.unique_constraints[0].columns,
        vec!["app_label".to_string(), "model".to_string()]
    );
    // Enforcement rides the existing INSERT machinery: a duplicate tuple
    // must now be rejected with the unique-violation error…
    let err = sess
        .execute(
            "INSERT INTO django_content_type (id, app_label, model) VALUES \
             (3, 'myapp', 'question')",
        )
        .await
        .expect_err("duplicate (app_label, model) must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("django_content_type_app_label_model_76bd3d3b_uniq"),
        "violation must name the constraint, got: {msg}"
    );
    // …while a tuple differing in either column still inserts.
    sess.execute(
        "INSERT INTO django_content_type (id, app_label, model) VALUES \
         (4, 'otherapp', 'question')",
    )
    .await
    .unwrap();
}

/// ADD CONSTRAINT UNIQUE must validate the EXISTING rows first: if the
/// table already contains a duplicate tuple the ALTER is rejected (PG
/// raises 23505 `could not create unique index`) and nothing is registered.
#[tokio::test]
async fn add_unique_constraint_rejects_existing_duplicates() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t (id, email) VALUES (1, 'a@x.com'), (2, 'a@x.com'), (3, NULL), (4, NULL)",
    )
    .await
    .unwrap();
    let err = sess
        .execute("ALTER TABLE t ADD CONSTRAINT t_email_uniq UNIQUE (email)")
        .await
        .expect_err("existing duplicate emails must fail the backfill validation");
    let msg = err.to_string();
    assert!(
        msg.contains("t_email_uniq") && msg.contains("duplicated"),
        "error must name the constraint and the duplication, got: {msg}"
    );
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(
        meta.unique_constraints.is_empty(),
        "failed ALTER must not register the constraint"
    );
}

/// Unnamed `ADD UNIQUE (col)` synthesises the PG-convention
/// `<table>_<col>_key` name (same derivation as CREATE TABLE's
/// table-level UNIQUE). NULLs stay exempt (PG NULLS-distinct default):
/// multiple NULL rows pass both the backfill scan and later INSERTs.
#[tokio::test]
async fn add_unique_constraint_unnamed_synthesises_pg_name() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, email) VALUES (1, NULL), (2, NULL)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t ADD UNIQUE (email)").await.unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.unique_constraints.len(), 1);
    assert_eq!(meta.unique_constraints[0].name, "t_email_key");
    // A third NULL still inserts (NULLs are distinct under PG's default).
    sess.execute("INSERT INTO t (id, email) VALUES (3, NULL)")
        .await
        .unwrap();
}

/// A UNIQUE constraint added via ALTER must reference real columns and
/// must be DROPpable through the existing DROP CONSTRAINT path.
#[tokio::test]
async fn add_unique_constraint_unknown_column_errors_and_drop_removes() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, email TEXT)")
        .await
        .unwrap();
    let err = sess
        .execute("ALTER TABLE t ADD CONSTRAINT u UNIQUE (ghost)")
        .await
        .expect_err("UNIQUE over a missing column must fail");
    assert!(err.to_string().contains("ghost"), "got: {err}");

    sess.execute("ALTER TABLE t ADD CONSTRAINT u UNIQUE (email)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t DROP CONSTRAINT u").await.unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(meta.unique_constraints.is_empty());
}

#[tokio::test]
async fn drop_check_constraint_removes_predicate() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, CONSTRAINT ck CHECK (id > 0))")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t DROP CONSTRAINT ck")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert!(meta.check_constraints.is_empty());
}

#[tokio::test]
async fn validate_constraint_known_succeeds_unknown_errors() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, CONSTRAINT ck CHECK (id > 0))")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t VALIDATE CONSTRAINT ck")
        .await
        .unwrap();
    let err = sess
        .execute("ALTER TABLE t VALIDATE CONSTRAINT does_not_exist")
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn attach_and_detach_partition_are_accepted() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (region TEXT) PARTITION BY LIST (region)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE p (region TEXT)").await.unwrap();
    // ATTACH PARTITION is accepted (parse-level no-op).
    sess.execute("ALTER TABLE t ATTACH PARTITION p FOR VALUES IN ('us')")
        .await
        .unwrap();
    // DETACH PARTITION: engine now explicitly errors (Phase 6.X PG-faithfulness
    // change — Basin uses flat storage without declarative partitioning).
    let err = sess
        .execute("ALTER TABLE t DETACH PARTITION p")
        .await
        .unwrap_err();
    let msg = format!("{err}").to_ascii_lowercase();
    assert!(
        msg.contains("partition") || msg.contains("not supported"),
        "expected unsupported-partition error, got: {err}"
    );
}

#[tokio::test]
async fn set_cold_after_accepts_quoted_duration() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("ALTER TABLE t SET cold_after = '7d'")
        .await
        .unwrap();
    let meta = eng
        .config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
    assert_eq!(meta.cold_after_seconds, Some(7 * 24 * 3600));
}

#[tokio::test]
async fn fk_implicit_pk_reference_is_accepted_without_column_list() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        .await
        .unwrap();
    // `REFERENCES u` (no col list) — the FK is dropped from the
    // catalog payload but the CREATE TABLE succeeds.
    sess.execute("CREATE TABLE t (id INT REFERENCES u DEFERRABLE INITIALLY DEFERRED)")
        .await
        .unwrap();
}

#[tokio::test]
async fn fk_set_null_action_is_accepted() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        .await
        .unwrap();
    // `ON UPDATE CASCADE ON DELETE SET NULL` — the SET NULL action
    // is mapped to NoAction silently; the CREATE TABLE succeeds.
    sess.execute("CREATE TABLE t (a INT REFERENCES u ON UPDATE CASCADE ON DELETE SET NULL)")
        .await
        .unwrap();
}

#[tokio::test]
async fn unlogged_keyword_is_stripped_pre_parse() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE UNLOGGED TABLE t (id INT)")
        .await
        .unwrap();
    eng.config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn inherits_clause_is_dropped_pre_parse() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE u (id INT)").await.unwrap();
    sess.execute("CREATE TABLE t () INHERITS (u)")
        .await
        .unwrap();
    eng.config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn like_including_all_is_stripped_pre_parse() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE u (id INT NOT NULL)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (LIKE u INCLUDING ALL)")
        .await
        .unwrap();
    eng.config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn unique_include_clause_is_stripped_pre_parse() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id INT NOT NULL, name TEXT, UNIQUE (id, name) INCLUDE (name))")
        .await
        .unwrap();
    eng.config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn fk_match_full_is_stripped_pre_parse() {
    let (_dir, eng, project) = engine();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE u (id INT PRIMARY KEY)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES u(id) MATCH FULL)")
        .await
        .unwrap();
    eng.config()
        .catalog
        .load_table(&project, &TableName::new("t").unwrap())
        .await
        .unwrap();
}
